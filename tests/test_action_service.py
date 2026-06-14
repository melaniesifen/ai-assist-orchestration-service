from __future__ import annotations

import asyncio
import unittest

from ai_assist_orchestration import ACTION_STATUS, InMemoryActionStore, OrchestrationError

from common import (
    IDENTITY,
    BlockingConnector,
    BlockingValidationConnector,
    ConsentService,
    FailingPublisher,
    FailingPayloadVault,
    FailingValidationConnector,
    RecordingConnector,
    StaleApproveRaceStore,
    TokenService,
    action_apply_input,
    action_decision_input,
    base_action_input,
    create_test_action_service,
)


class ActionServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_creates_approves_applies_and_idempotently_replays_proposed_action(self) -> None:
        events = []
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "google_op_001"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, events=events, connector=connector)

        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        self.assertEqual(proposed["status"], ACTION_STATUS.PROPOSED.value)
        self.assertEqual(proposed["actionId"], "action_001")

        approved = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        self.assertEqual(approved["status"], ACTION_STATUS.APPROVED.value)

        first_apply = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_001"))
        replayed_apply = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_001"))
        terminal_apply = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_002"))

        self.assertEqual(connector.write_count, 1)
        self.assertEqual(replayed_apply, first_apply)
        self.assertEqual(terminal_apply["status"], ACTION_STATUS.APPLIED.value)
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.APPLIED.value)
        self.assertTrue(any(event["type"] == "action.status_changed" and event["status"] == ACTION_STATUS.APPLIED.value for event in events))

    async def test_approve_and_reject_lifecycle_decisions_are_deterministic(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )

        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        first_approve = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        second_approve = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        first_reject = await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"], reasonCode="USER_REJECTED"))
        second_reject = await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"], reasonCode="USER_REJECTED"))

        self.assertEqual(first_approve["status"], ACTION_STATUS.APPROVED.value)
        self.assertEqual(second_approve, first_approve)
        self.assertEqual(first_reject["status"], ACTION_STATUS.REJECTED.value)
        self.assertEqual(second_reject, first_reject)
        approved_events = [event for event in events if event.get("status") == ACTION_STATUS.APPROVED.value]
        rejected_events = [event for event in events if event.get("status") == ACTION_STATUS.REJECTED.value]
        self.assertEqual(len(approved_events), 1)
        self.assertEqual(len(rejected_events), 1)

    async def test_uses_server_owned_action_ids_and_clamps_caller_supplied_ttls(self) -> None:
        service = create_test_action_service(
            action_store=InMemoryActionStore(),
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )

        proposed = await service.create_proposed_action(
            IDENTITY,
            {**base_action_input(), "actionId": "caller_chosen", "expiresAt": "2099-01-01T00:00:00.000Z", "ttlMs": 99 * 24 * 60 * 60 * 1000},
        )

        self.assertEqual(proposed["actionId"], "action_001")
        self.assertEqual(proposed["expiresAt"], "2026-05-30T00:00:00.000Z")

    async def test_decision_commands_deny_cross_tenant_user_session_and_resource_scope(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())

        denial_cases = [
            ({"tenantId": "tenant_other", "userId": IDENTITY["userId"]}, action_decision_input(proposed["actionId"])),
            ({"tenantId": IDENTITY["tenantId"], "userId": "user_other"}, action_decision_input(proposed["actionId"])),
            (IDENTITY, action_decision_input(proposed["actionId"], sessionId="session_other")),
            (IDENTITY, action_decision_input(proposed["actionId"], resourceId="doc_other")),
        ]
        for denied_identity, denied_input in denial_cases:
            with self.subTest(denied_identity=denied_identity, denied_input=denied_input):
                with self.assertRaises(OrchestrationError) as caught:
                    await service.approve_action(denied_identity, denied_input)
                self.assertEqual(caught.exception.category, "AUTHORIZATION")

        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.PROPOSED.value)

    async def test_decision_commands_require_session_and_resource_scope(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())

        for method, scoped_input in (
            (service.approve_action, {"actionId": proposed["actionId"], "resourceId": "doc_001"}),
            (service.reject_action, {"actionId": proposed["actionId"], "sessionId": "session_001"}),
            (service.apply_action, {"actionId": proposed["actionId"], "idempotencyKey": "idem_missing_scope", "sessionId": "session_001"}),
        ):
            with self.subTest(method=method.__name__):
                with self.assertRaises(OrchestrationError) as caught:
                    await method(IDENTITY, scoped_input)
                self.assertEqual(caught.exception.category, "VALIDATION")

        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.PROPOSED.value)

    async def test_stale_approval_or_rejection_expires_action_without_connector_mutation(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        connector = RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "should_not_happen"})
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=connector,
        )
        proposed = await service.create_proposed_action(IDENTITY, {**base_action_input(), "ttlMs": 1})
        action_store.update(proposed["actionId"], lambda current: {**current, "expiresAt": "2026-05-28T23:59:59.000Z"})

        approved = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        rejected = await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"]))

        self.assertEqual(approved["status"], ACTION_STATUS.EXPIRED.value)
        self.assertEqual(rejected["status"], ACTION_STATUS.EXPIRED.value)
        self.assertEqual(connector.write_count, 0)
        expired_events = [event for event in events if event.get("status") == ACTION_STATUS.EXPIRED.value]
        self.assertEqual(len(expired_events), 1)

    async def test_action_events_and_fake_encryption_boundary_exclude_payload_plaintext(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )

        proposed = await service.create_proposed_action(
            IDENTITY,
            {
                **base_action_input(),
                "payload": {
                    "currentText": "sensitive current document text",
                    "proposedText": "sensitive proposed document text",
                },
            },
        )
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        persisted = action_store.get(proposed["actionId"])
        self.assertEqual(persisted["encryptedPayload"], {"ciphertextRef": "encrypted_payload_1"})
        self.assertNotIn("sensitive current document text", str(persisted))
        self.assertNotIn("sensitive proposed document text", str(persisted))
        self.assertNotIn("sensitive current document text", str(events))
        self.assertNotIn("sensitive proposed document text", str(events))

    async def test_action_events_include_session_event_envelope_fields(self) -> None:
        events = []
        event_ids = iter(["evt_proposed", "evt_approved"])
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        service.event_id_generator = lambda: next(event_ids)

        proposed = await service.create_proposed_action(
            IDENTITY,
            {**base_action_input(), "requestId": "req_create", "correlationId": "corr_action"},
        )
        await service.approve_action(
            IDENTITY,
            action_decision_input(proposed["actionId"], requestId="req_approve", correlationId="corr_action"),
        )

        self.assertEqual([event["eventId"] for event in events], ["evt_proposed", "evt_approved"])
        self.assertEqual([event["sequence"] for event in events], [1, 2])
        self.assertEqual(events[0]["payload"]["actionId"], proposed["actionId"])
        self.assertEqual(events[0]["payload"]["actionType"], "replace_text")
        self.assertEqual(events[1]["payload"]["status"], ACTION_STATUS.APPROVED.value)
        for event in events:
            self.assertEqual(event["tenantId"], IDENTITY["tenantId"])
            self.assertEqual(event["userId"], IDENTITY["userId"])
            self.assertEqual(event["sessionId"], "session_001")
            self.assertEqual(event["correlationId"], "corr_action")
            self.assertIn("createdAt", event)
            self.assertNotIn("tenantId", event["payload"])
            self.assertNotIn("userId", event["payload"])

    async def test_action_create_publisher_outage_preserves_durable_action_state(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=[],
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        service.event_publisher = FailingPublisher(fail_on_type="action.proposed")

        proposed = await service.create_proposed_action(IDENTITY, base_action_input())

        self.assertEqual(proposed["status"], ACTION_STATUS.PROPOSED.value)
        self.assertEqual(proposed["eventPublishFailures"][0]["eventType"], "action.proposed")
        persisted = action_store.get(proposed["actionId"])
        self.assertEqual(persisted["status"], ACTION_STATUS.PROPOSED.value)
        self.assertNotIn("eventPublishFailures", persisted)

    async def test_action_status_publisher_outage_preserves_approve_state(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=[],
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        service.event_publisher = FailingPublisher(fail_on_type="action.status_changed")

        approved = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        self.assertEqual(approved["status"], ACTION_STATUS.APPROVED.value)
        self.assertEqual(approved["eventPublishFailures"][0]["category"], "DEPENDENCY")
        persisted = action_store.get(proposed["actionId"])
        self.assertEqual(persisted["status"], ACTION_STATUS.APPROVED.value)
        self.assertNotIn("eventPublishFailures", persisted)

    async def test_apply_status_publisher_outage_preserves_apply_state_and_result(self) -> None:
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "google_op_001"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, events=[], connector=connector)
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        service.event_publisher = FailingPublisher(fail_on_type="action.status_changed")

        applied = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_publish_outage"))

        self.assertEqual(applied["status"], ACTION_STATUS.APPLIED.value)
        self.assertEqual(applied["providerOperationId"], "google_op_001")
        self.assertEqual(applied["eventPublishFailures"][0]["eventType"], "action.status_changed")
        self.assertEqual(connector.write_count, 1)
        persisted = action_store.get(proposed["actionId"])
        self.assertEqual(persisted["status"], ACTION_STATUS.APPLIED.value)
        self.assertNotIn("eventPublishFailures", persisted)

    async def test_marks_stale_action_state_conflicted_and_performs_no_provider_mutation(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector(
                {
                    "valid": False,
                    "reasonCode": "ORIGINAL_TEXT_HASH_MISMATCH",
                    "conflictDetails": {
                        "resourceId": "doc_001",
                        "expectedRevision": "rev_001",
                        "currentRevision": "rev_002",
                        "currentText": "must not be retained",
                        "expectedText": "must not be retained either",
                    },
                },
                {"providerOperationId": "should_not_happen"},
            ),
        )

        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        result = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_conflict"))

        self.assertEqual(result["status"], ACTION_STATUS.CONFLICTED.value)
        self.assertEqual(result["reasonCode"], "ORIGINAL_TEXT_HASH_MISMATCH")
        self.assertNotIn("must not be retained", str(result))
        self.assertNotIn("must not be retained", str(action_store.get(proposed["actionId"])))
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.CONFLICTED.value)

    async def test_reserves_apply_before_provider_mutation_so_concurrent_applies_do_not_double_write(self) -> None:
        gate = asyncio.Event()
        connector = BlockingConnector(gate)
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, connector=connector)
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        first = asyncio.create_task(service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_concurrent")))
        await asyncio.sleep(0)
        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_concurrent"))
        self.assertEqual(caught.exception.code, "ACTION_APPLY_IN_PROGRESS")
        gate.set()
        await first
        self.assertEqual(connector.write_count, 1)

    async def test_reserves_apply_before_target_validation_so_duplicate_requests_wait(self) -> None:
        validation_gate = asyncio.Event()
        connector = BlockingValidationConnector(validation_gate)
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, connector=connector)
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        first = asyncio.create_task(service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_validation_race")))
        await asyncio.sleep(0)
        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_validation_race"))
        self.assertEqual(caught.exception.code, "ACTION_APPLY_IN_PROGRESS")
        validation_gate.set()
        await first
        self.assertEqual(connector.validation_count, 1)
        self.assertEqual(connector.write_count, 1)

    async def test_revoked_consent_conflicts_and_skips_connector_mutation(self) -> None:
        events = []
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "should_not_happen"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=connector,
            consent_service=ConsentService({"allowed": False, "reasonCode": "CONSENT_REVOKED"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        result = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_consent_revoked"))

        self.assertEqual(result["status"], ACTION_STATUS.CONFLICTED.value)
        self.assertEqual(result["reasonCode"], "CONSENT_REVOKED")
        self.assertEqual(connector.validation_count, 0)
        self.assertEqual(connector.write_count, 0)
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.CONFLICTED.value)
        self.assertTrue(any(event.get("status") == ACTION_STATUS.CONFLICTED.value and event.get("reasonCode") == "CONSENT_REVOKED" for event in events))

    async def test_reconnect_required_token_status_fails_safely_before_target_validation(self) -> None:
        events = []
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "should_not_happen"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=connector,
            token_service=TokenService({"valid": False, "reasonCode": "RECONNECT_REQUIRED"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        result = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_reconnect"))

        self.assertEqual(result["status"], ACTION_STATUS.FAILED.value)
        self.assertEqual(result["reasonCode"], "RECONNECT_REQUIRED")
        self.assertEqual(connector.validation_count, 0)
        self.assertEqual(connector.write_count, 0)
        self.assertEqual(action_store.get(proposed["actionId"])["failureCode"], "RECONNECT_REQUIRED")
        self.assertTrue(any(event.get("status") == ACTION_STATUS.FAILED.value and event.get("reasonCode") == "RECONNECT_REQUIRED" for event in events))

    async def test_payload_decrypt_failure_fails_safely_before_connector_mutation(self) -> None:
        events = []
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "should_not_happen"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=connector,
            payload_vault=FailingPayloadVault(),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_decrypt"))

        self.assertEqual(caught.exception.code, "ACTION_PAYLOAD_DECRYPT_FAILED")
        self.assertEqual(connector.validation_count, 1)
        self.assertEqual(connector.write_count, 0)
        persisted = action_store.get(proposed["actionId"])
        self.assertEqual(persisted["status"], ACTION_STATUS.FAILED.value)
        self.assertEqual(persisted["failureCode"], "ACTION_PAYLOAD_DECRYPT_FAILED")
        self.assertTrue(any(event.get("status") == ACTION_STATUS.FAILED.value and event.get("reasonCode") == "ACTION_PAYLOAD_DECRYPT_FAILED" for event in events))

    async def test_target_validation_dependency_failure_clears_apply_lock_and_publishes_failure(self) -> None:
        events = []
        connector = FailingValidationConnector()
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, events=events, connector=connector)
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_target_dependency"))
        replay = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_target_dependency"))

        self.assertEqual(caught.exception.code, "TARGET_VALIDATION_FAILED")
        self.assertEqual(replay["status"], ACTION_STATUS.FAILED.value)
        self.assertEqual(replay["reasonCode"], "TARGET_VALIDATION_FAILED")
        self.assertEqual(connector.write_count, 0)
        persisted = action_store.get(proposed["actionId"])
        self.assertNotIn("applyLock", persisted)
        self.assertEqual(persisted["failureCode"], "TARGET_VALIDATION_FAILED")
        self.assertTrue(any(event.get("status") == ACTION_STATUS.FAILED.value and event.get("reasonCode") == "TARGET_VALIDATION_FAILED" for event in events))

    async def test_action_methods_return_typed_validation_errors_for_malformed_inputs(self) -> None:
        service = create_test_action_service(
            action_store=InMemoryActionStore(),
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )

        for method, bad_input in (
            (service.approve_action, None),
            (service.reject_action, []),
            (service.apply_action, None),
        ):
            with self.assertRaises(OrchestrationError) as caught:
                await method(IDENTITY, bad_input)
            self.assertEqual(caught.exception.category, "VALIDATION")
            self.assertEqual(caught.exception.code, "INVALID_FIELD")

    async def test_unknown_persisted_action_status_returns_typed_error(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        action_store.update(proposed["actionId"], lambda current: {**current, "status": "UNKNOWN"})

        with self.assertRaises(OrchestrationError) as caught:
            await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"]))
        self.assertEqual(caught.exception.category, "VALIDATION")
        self.assertEqual(caught.exception.code, "INVALID_ACTION_STATUS")
        self.assertEqual(caught.exception.metadata["actionId"], proposed["actionId"])

    async def test_conditional_approve_does_not_overwrite_rejected_state_after_stale_read(self) -> None:
        action_store = StaleApproveRaceStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())

        with self.assertRaises(OrchestrationError) as caught:
            await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        self.assertEqual(caught.exception.code, "ACTION_NOT_APPROVABLE")
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.REJECTED.value)

    async def test_reject_during_apply_in_progress_does_not_overwrite_apply_lock(self) -> None:
        gate = asyncio.Event()
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, connector=BlockingConnector(gate))
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        first = asyncio.create_task(service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_reject_race")))
        await asyncio.sleep(0)
        with self.assertRaises(OrchestrationError) as caught:
            await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"]))
        self.assertEqual(caught.exception.code, "ACTION_APPLY_IN_PROGRESS")
        gate.set()
        await first
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.APPLIED.value)

    async def test_duplicate_conflict_apply_preserves_first_terminal_transition(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector(
                {
                    "valid": False,
                    "reasonCode": "ORIGINAL_TEXT_HASH_MISMATCH",
                    "conflictDetails": {"currentRevision": "rev_002"},
                },
                {"providerOperationId": "should_not_happen"},
            ),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        first = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_conflict_first"))
        second = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_conflict_second"))

        self.assertEqual(first, second)
        conflict_events = [
            event
            for event in events
            if event["type"] == "action.status_changed" and event["status"] == ACTION_STATUS.CONFLICTED.value
        ]
        self.assertEqual(len(conflict_events), 1)

