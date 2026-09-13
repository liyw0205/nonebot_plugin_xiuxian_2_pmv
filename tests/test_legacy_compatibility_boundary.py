from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch
import sqlite3

from nonebot_plugin_xiuxian_2.features._legacy_feature import (
    CompatibilityRepository,
    LegacyFeatureApplication,
)
from nonebot_plugin_xiuxian_2.features._migrated_application import MigratedFeatureApplication
from nonebot_plugin_xiuxian_2.features._service_port import ServicePort


@dataclass(frozen=True)
class _LegacyResult:
    status: str
    message: str


class LegacyCompatibilityBoundaryTests(unittest.TestCase):
    def test_repository_dispatches_and_normalizes_legacy_results(self) -> None:
        calls: list[tuple[str, str, int]] = []

        def apply(*, operation_id: str, user_id: str, amount: int):
            calls.append((operation_id, user_id, amount))
            return _LegacyResult("ok", "applied")

        repository = CompatibilityRepository(
            "fake",
            handlers={
                "apply": apply,
                "tuple": lambda **_: (True, "tuple ok"),
                "bool": lambda **_: True,
            },
        )

        result = repository.execute("op-1", "user-1", {"action": "apply", "amount": 3})
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["message"], "applied")
        self.assertEqual(calls, [("op-1", "user-1", 3)])
        self.assertEqual(repository.execute("op-2", "user-1", {"action": "tuple"})["status"], "applied")
        self.assertEqual(repository.execute("op-3", "user-1", {"action": "bool"})["status"], "applied")

    def test_unknown_action_is_explicitly_unsupported(self) -> None:
        repository = CompatibilityRepository("fake", handlers={})
        result = repository.execute("op-1", "user-1", {"action": "missing"})
        self.assertEqual(result["status"], "unsupported")
        self.assertEqual(result["action"], "missing")
        self.assertTrue(result["compatibility"])

    def test_generic_boundary_warns_and_records_hit(self) -> None:
        repository = CompatibilityRepository("fake", handlers={"apply": lambda **_: True})
        with patch("nonebot_plugin_xiuxian_2.compatibility.commands.record_compatibility_hit") as record:
            with self.assertWarnsRegex(DeprecationWarning, "compatibility execute is deprecated"):
                repository.execute("op-1", "user-1", {"action": "apply"})
        record.assert_called_once_with("feature:fake")

    def test_default_execute_does_not_guess_a_legacy_service(self) -> None:
        service = SimpleNamespace(settle=lambda **_: True)
        with patch(
            "nonebot_plugin_xiuxian_2.features._legacy_feature.importlib.import_module",
            return_value=SimpleNamespace(daily_reward_service=service),
        ):
            repository = CompatibilityRepository("fake", legacy_module="legacy.module")
            result = repository.execute("op-1", "user-1", {})

        self.assertEqual(result["status"], "unsupported")
        self.assertEqual(result["action"], "execute")

    def test_application_replays_the_first_legacy_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = str(Path(directory) / "game.db")
            calls: list[str] = []

            def apply(**_: object):
                calls.append("called")
                return True

            repository = CompatibilityRepository("fake", handlers={"apply": apply})
            application = LegacyFeatureApplication(database, feature="fake", repository=repository)
            first = application.execute(
                operation_id="op-1",
                user_id="user-1",
                payload={"action": "apply"},
            )
            replay = application.execute(
                operation_id="op-1",
                user_id="user-1",
                payload={"action": "apply"},
            )

            self.assertTrue(first.ok)
            self.assertEqual(replay.status, "replayed")
            self.assertEqual(calls, ["called"])

    def test_application_preserves_legacy_rejection_on_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            calls: list[str] = []

            def reject(**_: object):
                calls.append("called")
                return False

            application = LegacyFeatureApplication(
                Path(directory) / "game.db",
                feature="fake",
                repository=CompatibilityRepository("fake", handlers={"reject": reject}),
            )
            first = application.execute(
                operation_id="op-rejected",
                user_id="user-1",
                payload={"action": "reject"},
            )
            replay = application.execute(
                operation_id="op-rejected",
                user_id="user-1",
                payload={"action": "reject"},
            )

            self.assertFalse(first.ok)
            self.assertEqual(first.status, "rejected")
            self.assertEqual(replay.status, "rejected")
            self.assertEqual(calls, ["called"])

    def test_application_records_callback_failure_and_allows_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            calls: list[str] = []

            def flaky(**_: object):
                calls.append("called")
                if len(calls) == 1:
                    raise RuntimeError("legacy failed")
                return True

            application = LegacyFeatureApplication(
                Path(directory) / "game.db",
                feature="fake",
                repository=CompatibilityRepository("fake", handlers={"flaky": flaky}),
            )
            with self.assertRaisesRegex(RuntimeError, "legacy failed"):
                application.execute(
                    operation_id="op-failed",
                    user_id="user-1",
                    payload={"action": "flaky"},
                )
            retried = application.execute(
                operation_id="op-failed",
                user_id="user-1",
                payload={"action": "flaky"},
            )

            self.assertTrue(retried.ok)
            self.assertEqual(calls, ["called", "called"])

    def test_migrated_application_releases_ledger_lock_before_legacy_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"

            class Repository:
                def execute(self, operation_id: str, user_id: str, action: str, payload: dict[str, object]):
                    with sqlite3.connect(database, timeout=0.2) as connection:
                        connection.execute("CREATE TABLE IF NOT EXISTS legacy_events (operation_id TEXT PRIMARY KEY)")
                        connection.execute("INSERT INTO legacy_events(operation_id) VALUES (?)", (operation_id,))
                    return {"status": "applied", "action": action}

                def inspect(self, user_id: str):
                    return {"user_id": user_id}

            application = MigratedFeatureApplication(database, feature="fake", repository=Repository())
            outcome = application.execute(operation_id="op-lock", user_id="user-1")

            self.assertTrue(outcome.ok)
            with sqlite3.connect(database) as connection:
                self.assertEqual(
                    connection.execute("SELECT operation_id FROM legacy_events").fetchone()[0],
                    "op-lock",
                )

    def test_migrated_application_uses_feature_service_port_for_bound_callback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            calls: list[str] = []

            class Repository(ServicePort):
                def __init__(self) -> None:
                    super().__init__("fake", "fake")

                def execute_callback(self, operation_id, user_id, action, payload, callback):
                    calls.append(f"port:{action}:{operation_id}:{user_id}")
                    return super().execute_callback(operation_id, user_id, action, payload, callback)

            application = MigratedFeatureApplication(
                Path(directory) / "game.db",
                feature="fake",
                repository=Repository(),
            )
            outcome = application.execute_legacy_call(
                operation_id="op-port",
                user_id="user-1",
                action="apply",
                payload={"amount": 2},
                call=lambda: {"status": "applied", "amount": 2},
            )

            self.assertTrue(outcome.ok)
            self.assertEqual(calls, ["port:apply:op-port:user-1"])


if __name__ == "__main__":
    unittest.main()
