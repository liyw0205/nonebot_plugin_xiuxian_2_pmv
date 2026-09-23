from __future__ import annotations

import tempfile
import unittest
import importlib
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade.transaction_service import (
    GuishiStoneService,
)
from tests.test_db_backend import db_backend


def test_trade_facade_defers_guishi_stone_service_construction():
    trade = importlib.import_module(
        "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_trade"
    )
    assert trade._guishi_stone_service_instance is None


def test_guishi_stone_handlers_use_feature_repositories():
    source = Path(
        "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_trade/__init__.py"
    ).read_text(encoding="utf-8")
    start = source.index("async def guishi_deposit_")
    deposit_handler = source[start : source.index("@guishi_withdraw.handle", start)]
    assert "trade_application.guishi_deposit(" in deposit_handler
    assert "_guishi_stone_service().deposit(" not in deposit_handler
    assert "GuishiDepositSqlRepository.STORED_CAP" in deposit_handler
    assert "GuishiDepositSqlRepository.OP_AMOUNT_CAP" in deposit_handler
    withdraw_start = source.index("async def guishi_withdraw_")
    withdraw_handler = source[
        withdraw_start : source.index("@guishi_qiugou.handle", withdraw_start)
    ]
    assert "trade_application.guishi_withdraw(" in withdraw_handler
    assert "_guishi_stone_service().withdraw(" not in withdraw_handler
    assert "GuishiWithdrawSqlRepository.OP_AMOUNT_CAP" in withdraw_handler
    assert "_guishi_stone_service_instance = None" in source
    assert "def _guishi_stone_service(" in source
    assert "guishi_stone_service.withdraw(" not in source
    qiugou_start = source.index("async def guishi_qiugou_")
    qiugou_handler = source[
        qiugou_start : source.index("@guishi_cancel_qiugou.handle", qiugou_start)
    ]
    assert "trade_application.guishi_qiugou(" in qiugou_handler
    assert "xianshi_repository.create_guishi_qiugou_order(" not in qiugou_handler
    baitan_start = source.index("async def guishi_baitan_")
    baitan_handler = source[
        baitan_start : source.index("@guishi_shoutan.handle", baitan_start)
    ]
    assert "trade_application.guishi_baitan(" in baitan_handler
    assert "xianshi_repository.create_guishi_baitan_order(" not in baitan_handler
    cancel_qiugou_start = source.index("async def guishi_cancel_qiugou_")
    cancel_qiugou_handler = source[
        cancel_qiugou_start : source.index("@guishi_baitan.handle", cancel_qiugou_start)
    ]
    assert "trade_application.guishi_cancel_qiugou(" in cancel_qiugou_handler
    assert "xianshi_repository.clear_guishi_qiugou_order(" not in cancel_qiugou_handler
    assert "result.cancelled" in cancel_qiugou_handler
    assert "result.cleared" not in cancel_qiugou_handler
    shoutan_start = source.index("async def guishi_shoutan_")
    shoutan_handler = source[
        shoutan_start : source.index("@guishi_take_item.handle", shoutan_start)
    ]
    assert "trade_application.guishi_cancel_baitan(" in shoutan_handler
    assert "xianshi_repository.clear_expired_guishi_order(" not in shoutan_handler


class GuishiStoneServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "game.sqlite3"
        self.trade_database = root / "trade.sqlite3"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES (%s, %s)", ("user", 1000))
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "CREATE TABLE guishi_info (user_id TEXT PRIMARY KEY, "
                "stored_stone INTEGER DEFAULT 0, items TEXT DEFAULT '{}')"
            )
        self.service = GuishiStoneService(self.game_database, self.trade_database)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def balances(self) -> tuple[int, int]:
        with db_backend.connection(self.game_database) as conn:
            player = conn.execute(
                "SELECT stone FROM user_xiuxian WHERE user_id=%s", ("user",)
            ).fetchone()[0]
        with db_backend.connection(self.trade_database) as conn:
            stored = conn.execute(
                "SELECT stored_stone FROM guishi_info WHERE user_id=%s", ("user",)
            ).fetchone()
        return int(player), int(stored[0]) if stored else 0

    def operation_count(self) -> int:
        with db_backend.connection(self.game_database) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=%s",
                ("guishi_stone_operations",),
            ).fetchone()
            if exists is None:
                return 0
            return int(
                conn.execute("SELECT COUNT(*) FROM guishi_stone_operations").fetchone()[0]
            )

    def test_deposit_moves_stones_between_databases_atomically(self) -> None:
        result = self.service.deposit("deposit-1", "user", 600)

        self.assertEqual(result.status, "completed")
        self.assertEqual((result.actual_amount, result.stored_balance), (600, 600))
        self.assertEqual(self.balances(), (400, 600))
        self.assertEqual(self.operation_count(), 1)

    def test_withdraw_applies_fee_and_moves_net_amount(self) -> None:
        self.service.deposit("deposit-before-withdraw", "user", 800)
        result = self.service.withdraw("withdraw-1", "user", 500)

        self.assertEqual(result.status, "completed")
        self.assertEqual((result.fee, result.actual_amount), (100, 400))
        self.assertEqual(self.balances(), (600, 300))

    def test_duplicate_does_not_move_stones_twice(self) -> None:
        first = self.service.deposit("deposit-repeat", "user", 600)
        second = self.service.deposit("deposit-repeat", "user", 900)

        self.assertEqual((first.status, second.status), ("completed", "duplicate"))
        self.assertEqual((second.amount, second.stored_balance), (600, 600))
        self.assertEqual(self.balances(), (400, 600))
        self.assertEqual(self.operation_count(), 1)

    def test_insufficient_balances_leave_both_databases_unchanged(self) -> None:
        deposit = self.service.deposit("deposit-poor", "user", 1001)
        withdraw = self.service.withdraw("withdraw-poor", "user", 1)

        self.assertEqual(deposit.status, "stone_insufficient")
        self.assertEqual(withdraw.status, "stored_insufficient")
        self.assertEqual(self.balances(), (1000, 0))
        self.assertEqual(self.operation_count(), 0)

    def test_operation_failure_rolls_back_both_databases(self) -> None:
        self.service.deposit("initialize-schema", "user", 100)
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_guishi_stone BEFORE INSERT "
                "ON guishi_stone_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.deposit("deposit-fail", "user", 500)
        self.assertEqual(self.balances(), (900, 100))
        self.assertEqual(self.operation_count(), 1)

    def test_missing_player_and_invalid_amount_are_rejected(self) -> None:
        result = self.service.deposit("deposit-missing", "missing", 100)
        self.assertEqual(result.status, "player_missing")
        for amount in (0, -1):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                self.service.deposit("deposit-invalid", "user", amount)
        self.assertEqual(self.balances(), (1000, 0))

    def test_amount_and_stored_hard_caps(self) -> None:
        # 单次超顶
        big = self.service.deposit("deposit-op-cap", "user", GuishiStoneService.OP_AMOUNT_CAP + 1)
        self.assertEqual(big.status, "amount_capped")
        self.assertEqual(self.balances(), (1000, 0))

        # 存到顶后再存拒绝
        self.service.deposit("deposit-fill", "user", 1000)
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "UPDATE guishi_info SET stored_stone=%s WHERE user_id=%s",
                (GuishiStoneService.STORED_CAP, "user"),
            )
        with db_backend.transaction(self.game_database) as conn:
            conn.execute("UPDATE user_xiuxian SET stone=%s WHERE user_id=%s", (500, "user"))
        over = self.service.deposit("deposit-over-cap", "user", 1)
        self.assertEqual(over.status, "stored_cap_exceeded")
        player, stored = self.balances()
        self.assertEqual(player, 500)
        self.assertEqual(stored, GuishiStoneService.STORED_CAP)

        # 历史脏余额读取时钳顶
        with db_backend.transaction(self.trade_database) as conn:
            conn.execute(
                "UPDATE guishi_info SET stored_stone=%s WHERE user_id=%s",
                (GuishiStoneService.STORED_CAP * 3, "user"),
            )
        wd = self.service.withdraw("withdraw-dirty-cap", "user", 100)
        self.assertEqual(wd.status, "completed")
        _, stored2 = self.balances()
        self.assertEqual(stored2, GuishiStoneService.STORED_CAP - 100)


if __name__ == "__main__":
    unittest.main()
