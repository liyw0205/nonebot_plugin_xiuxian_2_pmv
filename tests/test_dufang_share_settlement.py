from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_dufang.transaction_service import (
    DufangShareSettlementService,
)
from tests.test_db_backend import db_backend


class DufangShareSettlementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian("
                "user_id TEXT PRIMARY KEY,user_name TEXT,stone INTEGER)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s)",
                (
                    ("source", "发起者", 1000),
                    ("u1", "甲", 100),
                    ("u2", "乙", 20),
                    ("u3", "丙", 0),
                ),
            )
        self.service = DufangShareSettlementService(self.game, self.player)

    def tearDown(self):
        self.temp.cleanup()

    def settle(
        self,
        operation="share",
        event_type="profit",
        title="福泽共享",
        amount=30,
        recipients=(("u1", "甲"), ("u2", "乙")),
        **kwargs,
    ):
        return self.service.settle(
            operation,
            "source",
            event_type,
            title,
            "冻结的共享事件",
            amount,
            12,
            recipients,
            "2026-07-14 07:00:00",
            **kwargs,
        )

    def stones(self):
        with db_backend.connection(self.game) as conn:
            return {
                str(row[0]): int(row[1])
                for row in conn.execute(
                    "SELECT user_id,stone FROM user_xiuxian ORDER BY user_id"
                ).fetchall()
            }

    def sharing_stats(self):
        with db_backend.connection(self.player) as conn:
            if not conn.table_exists("unseal_data"):
                return {}
            return {
                str(row[0]): tuple(int(value or 0) for value in row[1:])
                for row in conn.execute(
                    "SELECT user_id,shared_profit,shared_loss,received_profit,received_loss "
                    "FROM unseal_data ORDER BY user_id"
                ).fetchall()
            }

    def economy_rows(self):
        with db_backend.connection(self.game) as conn:
            return [
                (str(row[0]), str(row[1]), int(row[2]), str(row[3]))
                for row in conn.execute(
                    "SELECT user_id,action,stone_delta,trace_id FROM economy_log ORDER BY id"
                ).fetchall()
            ]

    def test_profit_batch_freezes_event_recipients_and_replays(self):
        first = self.settle()
        self.assertEqual(
            (first.status, first.task_status, first.total, first.completed, first.total_amount),
            ("applied", "completed", 2, 2, 60),
        )
        self.assertEqual([item.status for item in first.recipients], ["applied", "applied"])
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 50, "u3": 0})
        self.assertEqual(
            self.sharing_stats(),
            {
                "source": (60, 0, 0, 0),
                "u1": (0, 0, 30, 0),
                "u2": (0, 0, 30, 0),
            },
        )
        self.assertEqual(
            self.economy_rows(),
            [
                ("u1", "dufang_share_profit", 30, "share"),
                ("u2", "dufang_share_profit", 30, "share"),
            ],
        )

        duplicate = self.settle(
            event_type="loss",
            title="新的随机事件",
            amount=999,
            recipients=(("u3", "丙"),),
        )
        conflict = self.service.settle(
            "share",
            "other-source",
            "profit",
            "冲突",
            "冲突",
            1,
            0,
            (("u1", "甲"),),
            "later",
        )
        self.assertEqual((duplicate.status, conflict.status), ("duplicate", "operation_conflict"))
        self.assertEqual((duplicate.event_type, duplicate.event_title), ("profit", "福泽共享"))
        self.assertEqual([item.user_id for item in duplicate.recipients], ["u1", "u2"])
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 50, "u3": 0})
        self.assertEqual(len(self.economy_rows()), 2)

    def test_loss_is_bounded_and_zero_balance_is_completed(self):
        result = self.settle(
            operation="loss",
            event_type="loss",
            title="厄运共享",
            amount=50,
            recipients=(("u1", "甲"), ("u2", "乙"), ("u3", "丙")),
        )
        self.assertEqual((result.task_status, result.completed, result.total_amount), ("completed", 3, 70))
        self.assertEqual(
            [(item.status, item.amount, item.wallet_stone) for item in result.recipients],
            [("applied", 50, 50), ("applied", 20, 0), ("skipped", 0, 0)],
        )
        self.assertEqual(self.stones(), {"source": 1000, "u1": 50, "u2": 0, "u3": 0})
        self.assertEqual(
            self.sharing_stats(),
            {
                "source": (0, 70, 0, 0),
                "u1": (0, 0, 0, 50),
                "u2": (0, 0, 0, 20),
            },
        )
        self.assertEqual(
            self.economy_rows(),
            [
                ("u1", "dufang_share_loss", -50, "loss"),
                ("u2", "dufang_share_loss", -20, "loss"),
            ],
        )

    def test_failure_keeps_completed_progress_and_resume_uses_frozen_batch(self):
        with db_backend.transaction(self.game) as conn:
            self.service._ensure_schema(conn)
            conn.execute(
                "CREATE TRIGGER fail_second_share BEFORE INSERT ON economy_log "
                "WHEN NEW.user_id='u2' BEGIN SELECT RAISE(ABORT,'failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.settle(operation="resume")

        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 20, "u3": 0})
        self.assertEqual(
            self.sharing_stats(),
            {"source": (30, 0, 0, 0), "u1": (0, 0, 30, 0)},
        )
        with db_backend.connection(self.game) as conn:
            task = tuple(
                conn.execute(
                    "SELECT completed,total_amount,status FROM dufang_share_operations "
                    "WHERE operation_id='resume'"
                ).fetchone()
            )
        self.assertEqual(task, (1, 30, "running"))

        with db_backend.transaction(self.game) as conn:
            conn.execute("DROP TRIGGER fail_second_share")
        resumed = self.settle(
            operation="resume",
            event_type="loss",
            title="不应替换冻结事件",
            amount=999,
            recipients=(("u3", "丙"),),
        )
        self.assertEqual(
            (resumed.status, resumed.task_status, resumed.event_type, resumed.total_amount),
            ("applied", "completed", "profit", 60),
        )
        self.assertEqual(
            [(item.user_id, item.status) for item in resumed.recipients],
            [("u1", "duplicate"), ("u2", "applied")],
        )
        self.assertEqual(self.stones(), {"source": 1000, "u1": 130, "u2": 50, "u3": 0})
        self.assertEqual(len(self.economy_rows()), 2)

    def test_production_handler_has_no_split_write_bypass(self):
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_dufang/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source.split("async def handle_shared_event", 1)[1].split(
            "# 鉴石信息", 1
        )[0]
        self.assertIn("dufang_share_service.settle(", handler)
        self.assertNotIn("sql_message.update_ls(", handler)
        self.assertNotIn("save_unseal_data(", handler)
        unseal_share = source.split("# 处理共享事件", 2)[2].split(
            "# 尘封之物类型", 1
        )[0]
        self.assertNotIn('shared_profit"] +=', unseal_share)
        self.assertNotIn('shared_loss"] +=', unseal_share)


if __name__ == "__main__":
    unittest.main()
