from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.bank.account_info_application import (
    BankAccountInfoApplication,
)
from nonebot_plugin_xiuxian_2.features.bank.command_application import (
    BankCommandApplication,
)
from nonebot_plugin_xiuxian_2.features.bank.command_replies import render_bank_reply
from nonebot_plugin_xiuxian_2.features.bank.history_view import (
    BankOperationView,
    render_bank_history,
)
from nonebot_plugin_xiuxian_2.features.bank.migrations import apply_bank_accounts
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class BankHistoryViewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u1',70)")
            apply_bank_accounts(uow)
            uow.execute(
                "INSERT INTO bank_accounts(user_id,saved_stone,bank_level,updated_at) "
                "VALUES('u1',30,'1','2026-10-29T10:00:00')"
            )
        self.application = BankAccountInfoApplication(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def insert(
        self,
        operation_id: str,
        *,
        user_id: str = "u1",
        deposited: int = 10,
        interest: int = 0,
        wallet_after: int = 70,
        saved_after: int = 30,
        created_at: str = "2026-10-29T10:00:00",
        payload: str | None = None,
    ) -> None:
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO bank_account_operations("
                "operation_id,user_id,payload,deposited,interest,wallet_after,saved_after,created_at"
                ") VALUES(?,?,?,?,?,?,?,?)",
                (
                    operation_id,
                    user_id,
                    payload if payload is not None else json.dumps(["u1", deposited, interest, 1000, "1"]),
                    deposited,
                    interest,
                    wallet_after,
                    saved_after,
                    created_at,
                ),
            )

    def test_recent_operations_are_newest_first_limited_and_user_scoped(self) -> None:
        self.insert("old", created_at="2026-10-28T10:00:00", deposited=4)
        self.insert("new", created_at="2026-10-29T12:00:00", deposited=8)
        self.insert("other", user_id="u2", created_at="2026-10-30T10:00:00")

        rows = self.application.recent_operations(user_id="u1", limit=2)

        self.assertEqual([row.amount for row in rows], [8, 4])
        self.assertEqual((), self.application.recent_operations(user_id="u1", limit=0))
        self.assertEqual(rows, self.application.recent_operations(user_id="u1", limit=2))

    def test_kind_inference_covers_deposit_withdrawal_interest_upgrade(self) -> None:
        self.insert("deposit", deposited=10, interest=0, saved_after=30)
        self.insert(
            "withdraw",
            deposited=-5,
            interest=2,
            saved_after=25,
            payload=json.dumps(["withdraw", "u1", 5, 2, "1"]),
        )
        self.insert(
            "interest",
            deposited=0,
            interest=3,
            saved_after=30,
            payload=json.dumps(["interest", "u1", 3, "1"]),
        )
        self.insert(
            "upgrade",
            deposited=0,
            interest=0,
            saved_after=0,
            payload=json.dumps(["upgrade", "u1", "2", 100]),
        )

        kinds = [row.kind for row in self.application.recent_operations(user_id="u1")]

        self.assertEqual(kinds, ["会员升级", "结息", "取出", "存入"])

    def test_bad_payload_and_fields_degrade_without_raising(self) -> None:
        self.insert("bad-json", deposited=7, payload="not-json")
        self.insert("zero", deposited=0, interest=0, payload="[]")

        rows = self.application.recent_operations(user_id="u1")

        self.assertEqual([row.kind for row in rows], ["账户操作", "存入"])

    def test_missing_table_and_missing_file_return_empty_without_ddl(self) -> None:
        empty = Path(self.temp.name) / "empty.db"
        with DatabaseUnitOfWork(empty, immediate=True) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
        missing = Path(self.temp.name) / "missing.db"

        self.assertEqual((), BankAccountInfoApplication(empty).recent_operations(user_id="u1"))
        self.assertEqual((), BankAccountInfoApplication(missing).recent_operations(user_id="u1"))
        self.assertFalse(missing.exists())
        with DatabaseUnitOfWork(empty) as uow:
            self.assertIsNone(
                uow.query_one(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='bank_account_operations'"
                )
            )

    def test_info_result_includes_operations(self) -> None:
        self.insert("deposit", created_at="2026-10-29T10:00:00")
        application = BankCommandApplication(self.database, {})

        result = application.execute(operation_id="info", user_id="u1", mode="信息", argument=None)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(result["operations"]), 1)
        self.assertEqual(result["operations"][0].kind, "存入")

    def test_render_history_empty_and_kinds(self) -> None:
        self.assertIn("暂无记录", render_bank_history(()))
        rows = (
            BankOperationView("存入", 10, 0, 30, 70, "t1"),
            BankOperationView("取出", 5, 2, 25, 75, "t2"),
            BankOperationView("结息", 0, 3, 28, 78, "t3"),
            BankOperationView("会员升级", 0, 0, 28, 78, "t4"),
        )

        text = render_bank_history(rows)

        self.assertIn("t1 存入 10，结息 0，存款 30", text)
        self.assertIn("t2 取出 5，结息 2，存款 25", text)
        self.assertIn("t3 结息 3，存款 28", text)
        self.assertIn("t4 会员升级", text)
        self.assertNotIn("t4 会员升级，存款", text)

    def test_info_reply_appends_history_only_when_present(self) -> None:
        base = {
            "action": "info",
            "status": "ok",
            "saved_stone": 30,
            "updated_at": "now",
            "bank_level": "1",
            "wallet_stone": 70,
        }
        levels = {"1": {"level": "L1", "savemax": 1000}}
        old = render_bank_reply(base, levels)
        new = render_bank_reply(
            {**base, "operations": (BankOperationView("存入", 10, 0, 30, 70, "t"),)},
            levels,
        )

        self.assertNotIn("最近灵庄记录", old)
        self.assertIn("最近灵庄记录", new)
        self.assertIn("t 存入 10", new)

    def test_command_source_reads_history_for_info(self) -> None:
        source = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "features"
            / "bank"
            / "command_application.py"
        ).read_text(encoding="utf-8")
        block = source[source.index('if action == "info":') : source.index("now = (self.clock")]

        self.assertIn("recent_operations", block)
        self.assertIn('"operations"', block)


if __name__ == "__main__":
    unittest.main()
