"""账本结果日志自描述（operation outcome）。

证据：`/root/xiu2/xiu2.log` 11-10 06:40 三行
`[INFO|WARNING] nonebot_plugin_xiuxian_2 | operation outcome`，没有 action、status、
operation_id 或 audit_category。根因是 `emit()` 把字段 `logger.bind(**payload)` 成 loguru
额外字段，而 nonebot 的文件格式器只渲染 message。

本套件钉住：message 自带 action/status/op（可选 category/code），结构化字段仍然 bind，
长度受控，且不泄漏 before/after/granted 的内容。
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import (
    OperationLedger,
    outcome_message,
)


class _Capture:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict]] = []

    def __call__(self, level, message, **fields):
        self.events.append((str(level), str(message), dict(fields)))
        return dict(fields)


class OutcomeMessageTests(unittest.TestCase):
    def test_applied_is_self_describing(self) -> None:
        message = outcome_message(
            OperationOutcome.applied("op-1", "sect.task_claim", data={"x": 1})
        )
        self.assertIn("action=sect.task_claim", message)
        self.assertIn("status=applied", message)
        self.assertIn("op=op-1", message)
        self.assertNotIn("code=", message)

    def test_rejected_carries_the_code_but_not_ok(self) -> None:
        message = outcome_message(
            OperationOutcome.rejected("op-2", "pet.hatch", "未完成", code="state_changed")
        )
        self.assertIn("status=rejected", message)
        self.assertIn("code=state_changed", message)

    def test_empty_category_and_missing_fields_are_omitted(self) -> None:
        message = outcome_message(OperationOutcome.applied("op-3", "trade.purchase"))
        self.assertNotIn("category=", message)
        self.assertNotIn("code=", message)

    def test_long_identifiers_are_clipped(self) -> None:
        message = outcome_message(
            OperationOutcome.applied("o" * 400, "a" * 400, audit_category="c" * 200)
        )
        self.assertLessEqual(len(message), 300)
        self.assertIn("…", message)

    def test_payload_contents_never_leak(self) -> None:
        outcome = OperationOutcome.applied(
            "op-4",
            "boss.refresh",
            data={"secret": "S3CRET-VALUE"},
            before={"before_marker": "BEFORE-VALUE"},
            after={"after_marker": "AFTER-VALUE"},
            granted={"grant_marker": "GRANT-VALUE"},
        )
        message = outcome_message(outcome)
        for marker in ("S3CRET-VALUE", "BEFORE-VALUE", "AFTER-VALUE", "GRANT-VALUE"):
            self.assertNotIn(marker, message)


class LedgerEmitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        self.ledger = OperationLedger()
        with DatabaseUnitOfWork(self.database) as uow:
            self.ledger.ensure_schema(uow)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _finish(self, outcome):
        capture = _Capture()
        with patch(
            "nonebot_plugin_xiuxian_2.infrastructure.database.ledger.emit", capture
        ):
            with DatabaseUnitOfWork(self.database) as uow:
                self.ledger.begin(uow, outcome.operation_id, outcome.action, {})
            with DatabaseUnitOfWork(self.database) as uow:
                self.ledger.finish(uow, outcome)
        return capture

    def test_applied_outcome_logs_once_with_structured_fields(self) -> None:
        capture = self._finish(OperationOutcome.applied("op-a", "sect.upgrade"))

        self.assertEqual(len(capture.events), 1)
        level, message, fields = capture.events[0]
        self.assertEqual(level, "info")
        self.assertIn("action=sect.upgrade", message)
        self.assertIn("status=applied", message)
        self.assertIn("op=op-a", message)
        self.assertEqual(fields["operation_action"], "sect.upgrade")
        self.assertEqual(fields["operation_status"], "applied")
        self.assertEqual(fields["operation_id"], "op-a")

    def test_rejected_outcome_logs_at_warning_with_code(self) -> None:
        capture = self._finish(
            OperationOutcome.rejected("op-b", "trade.purchase", "未完成", code="item_missing")
        )
        level, message, _ = capture.events[0]
        self.assertEqual(level, "warning")
        self.assertIn("status=rejected", message)
        self.assertIn("code=item_missing", message)

    def test_record_failure_uses_the_same_self_describing_line(self) -> None:
        capture = _Capture()
        with patch(
            "nonebot_plugin_xiuxian_2.infrastructure.database.ledger.emit", capture
        ):
            self.ledger.record_failure(
                self.database, "op-c", "pet.hatch", {"user_id": "u-1"}, "boom"
            )

        level, message, _ = capture.events[-1]
        self.assertEqual(level, "warning")
        self.assertIn("action=pet.hatch", message)
        self.assertIn("status=failed", message)
        self.assertIn("code=internal_error", message)
        self.assertIn("category=failure", message)

    def test_audit_projection_compares_the_json_encoded_shape(self) -> None:
        outcome = OperationOutcome.applied(
            "op-json-shape",
            "pet.travel_claim",
            after={"rewards": ((1, 2),)},
            granted={"items": ((1001, 3), (1002, 1))},
        )

        self._finish(outcome)

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT after_json,granted_json FROM operation_audit "
                "WHERE operation_id=? AND action=?",
                (outcome.operation_id, outcome.action),
            )
        self.assertEqual({"rewards": [[1, 2]]}, json.loads(row["after_json"]))
        self.assertEqual({"items": [[1001, 3], [1002, 1]]}, json.loads(row["granted_json"]))

    def test_audit_finish_rejection_rolls_back_ledger_and_does_not_emit_success(self) -> None:
        failures = (
            ("ignored", "BEFORE INSERT ON operation_audit BEGIN SELECT RAISE(IGNORE); END"),
            ("rewritten", "AFTER INSERT ON operation_audit BEGIN "
             "UPDATE operation_audit SET after_json='{}' WHERE audit_id=NEW.audit_id; END"),
        )
        for suffix, trigger_body in failures:
            with self.subTest(failure=suffix):
                outcome = OperationOutcome.applied(
                    f"op-audit-{suffix}", "sect.upgrade", after={"level": 2}
                )
                with DatabaseUnitOfWork(self.database) as uow:
                    self.ledger.begin(uow, outcome.operation_id, outcome.action, {"level": 2})
                    uow.execute(f"CREATE TRIGGER injected_audit_fault {trigger_body}")

                capture = _Capture()
                with patch(
                    "nonebot_plugin_xiuxian_2.infrastructure.database.ledger.emit", capture
                ):
                    with self.assertRaisesRegex(RuntimeError, "operation audit finish write rejected"):
                        with DatabaseUnitOfWork(self.database) as uow:
                            self.ledger.finish(uow, outcome)

                with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                    saved = self.ledger.get(uow, outcome.operation_id, outcome.action)
                    audit_count = uow.query_one(
                        "SELECT COUNT(*) AS n FROM operation_audit WHERE operation_id=? AND action=?",
                        (outcome.operation_id, outcome.action),
                    )["n"]
                self.assertEqual(saved.status, "started")
                self.assertEqual(audit_count, 0)
                self.assertEqual(capture.events, [])
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("DROP TRIGGER injected_audit_fault")


if __name__ == "__main__":
    unittest.main()
