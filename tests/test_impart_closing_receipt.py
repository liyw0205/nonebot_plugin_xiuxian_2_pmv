"""P3 虚神界出关回执补齐祝福消耗与余额。

`ImpartClosingSettlementResult` 一直携带 `blessing_cost` 与 `exp_day_remaining`，
但出关回执只渲染分钟数与修为，玩家看不到本次消耗了多少祝福、还剩多少。
本套件钉住纯渲染函数 `closing_settlement_lines` 的文案与边界，以及 applied /
duplicate（重放）两条回执路径都追加该段、旧回执缺字段时的降级。
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.impart_pk.closing_settlement_repository import (
    ImpartClosingSettlementResult,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.closing_status import (
    closing_settlement_lines,
    closing_status_lines,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.migrations import apply_impart_pk_operations
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE = REPO_ROOT / "nonebot_plugin_xiuxian_2"


def _read(relative: str) -> str:
    return (PACKAGE / relative).read_text(encoding="utf-8")


def _handler_body(source: str, marker: str) -> str:
    start = source.index(f"async def {marker}")
    rest = source[start + len(f"async def {marker}"):]
    match = re.search(r"\n@|\n(?=async def )|\n(?=def )", rest)
    return source[start:] if match is None else source[start: start + len(f"async def {marker}") + match.start()]


class ClosingSettlementLinesTests(unittest.TestCase):
    def test_consumed_blessing_names_cost_and_remaining(self):
        lines = closing_settlement_lines(blessing_cost=30, exp_day_remaining=90)

        self.assertEqual(lines, ("本次消耗虚神界祝福 30 分钟，剩余 90 分钟",))

    def test_minutes_suffix_is_added_only_when_positive(self):
        self.assertEqual(
            closing_settlement_lines(blessing_cost=30, exp_day_remaining=90, minutes=60),
            ("本次消耗虚神界祝福 30 分钟，剩余 90 分钟（本次闭关 60 分钟）",),
        )
        self.assertEqual(
            closing_settlement_lines(blessing_cost=30, exp_day_remaining=90, minutes=0),
            ("本次消耗虚神界祝福 30 分钟，剩余 90 分钟",),
        )

    def test_zero_cost_says_nothing_was_consumed(self):
        self.assertEqual(
            closing_settlement_lines(blessing_cost=0, exp_day_remaining=45),
            ("本次未消耗虚神界祝福，剩余 45 分钟",),
        )

    def test_dirty_values_are_clamped_to_zero(self):
        self.assertEqual(
            closing_settlement_lines(blessing_cost="bad", exp_day_remaining=None),
            ("本次未消耗虚神界祝福，剩余 0 分钟",),
        )
        self.assertEqual(
            closing_settlement_lines(blessing_cost=-5, exp_day_remaining=-9),
            ("本次未消耗虚神界祝福，剩余 0 分钟",),
        )

    def test_status_helper_is_untouched_by_the_new_function(self):
        self.assertEqual(closing_status_lines(None, exp_day=10, current_exp=1, max_exp=2), ())


class SettlementResultCarriesTheReceiptFieldsTests(unittest.TestCase):
    def test_applied_result_exposes_cost_and_remaining(self):
        result = ImpartClosingSettlementResult("applied", 120, 30, 90)

        self.assertTrue(result.succeeded)
        self.assertEqual((result.exp_gain, result.blessing_cost, result.exp_day_remaining), (120, 30, 90))

    def test_replay_result_carries_the_same_fields(self):
        result = ImpartClosingSettlementResult("duplicate", 120, 30, 90)

        self.assertTrue(result.succeeded, "重放也应渲染同一段，故必须视为成功路径")

    def test_legacy_result_defaults_to_zeroes(self):
        result = ImpartClosingSettlementResult("applied", 120)

        lines = closing_settlement_lines(
            blessing_cost=result.blessing_cost,
            exp_day_remaining=result.exp_day_remaining,
        )
        self.assertEqual(result.blessing_cost, 0)
        self.assertEqual(lines, ("本次未消耗虚神界祝福，剩余 0 分钟",))


class ClosingReceiptWiringTests(unittest.TestCase):
    def setUp(self):
        self.source = _read("xiuxian/xiuxian_impart_pk/__init__.py")

    def test_applied_reply_appends_the_blessing_receipt(self):
        body = _handler_body(self.source, "impart_pk_out_closing_")

        self.assertIn("closing_settlement_lines(", body)
        self.assertIn("blessing_cost=settlement.blessing_cost", body)
        self.assertIn("exp_day_remaining=settlement.exp_day_remaining", body)
        self.assertIn('msg += "\\n" + "\\n".join(settlement_lines)', body)

    def test_replay_reply_appends_the_blessing_receipt(self):
        body = _handler_body(self.source, "impart_pk_out_closing_")
        replay = body[body.index('if settlement.status == "duplicate"')::]

        self.assertIn("closing_settlement_lines(", replay)
        self.assertIn("blessing_cost=settlement.blessing_cost", replay)

    def test_receipt_is_rendered_after_settlement_and_before_task_progress(self):
        body = _handler_body(self.source, "impart_pk_out_closing_")

        settle_at = body.index("impart_pk_application.closing_settle(")
        receipt_at = body.index("closing_settlement_lines(", settle_at)
        self.assertLess(settle_at, receipt_at)

    def test_settlement_math_and_storage_are_unchanged(self):
        repository = _read("features/impart_pk/closing_settlement_repository.py")

        self.assertIn(
            "uow.execute('UPDATE impart_data.xiuxian_impart SET exp_day=exp_day-? "
            "WHERE rowid=? AND user_id=? AND xiuxian_int_cmp(exp_day,?)=0 AND xiuxian_int_cmp(exp_day,?)>=0',"
            "(blessing_cost,effective_state_row_id(impart),user_id,expected_exp_day,blessing_cost))",
            repository,
        )
        self.assertIn(
            "saved=[exp_gain,blessing_cost,expected_exp_day-blessing_cost]", repository
        )
        self.assertIn(
            "saved=[exp_gain,blessing_cost,expected_exp_day-blessing_cost]; "
            "uow.execute('INSERT INTO impart_closing_operations",
            repository,
        )
        status = _read("features/impart_pk/closing_status.py")
        self.assertNotIn("exp_day=", status, "渲染层不得改写祝福余额")

    def test_receipt_function_has_no_io_or_nonebot_imports(self):
        source = _read("features/impart_pk/closing_status.py")

        self.assertNotIn("nonebot", source)
        self.assertNotIn("sqlite3", source)
        self.assertNotIn("DatabaseUnitOfWork", source)


class ClosingSettlementStorageTests(unittest.TestCase):
    """旧行重放兼容：result_json 仍写三个整数，新的渲染只读它。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.database = Path(self._tmp.name) / "game.db"

    def test_operation_row_keeps_three_field_payload(self):
        with DatabaseUnitOfWork(self.database) as uow:
            apply_impart_pk_operations(uow)
            uow.execute(
                "INSERT INTO impart_closing_operations(operation_id,payload,result_json) "
                "VALUES(?,?,?)",
                ("legacy-op", '["u","start"]', "[120,30,90]"),
            )
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(
                "SELECT result_json FROM impart_closing_operations WHERE operation_id=?",
                ("legacy-op",),
            )

        import json

        self.assertEqual(json.loads(row["result_json"]), [120, 30, 90])
        result = ImpartClosingSettlementResult("duplicate", *json.loads(row["result_json"]))
        self.assertEqual(
            closing_settlement_lines(
                blessing_cost=result.blessing_cost,
                exp_day_remaining=result.exp_day_remaining,
            ),
            ("本次消耗虚神界祝福 30 分钟，剩余 90 分钟",),
        )


if __name__ == "__main__":
    unittest.main()
