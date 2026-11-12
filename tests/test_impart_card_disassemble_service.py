import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import CardDisassembleService
from nonebot_plugin_xiuxian_2.features.impart.migrations import (
    apply_impart_card_operations,
    apply_impart_exact_crystal_balances,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class CardDisassembleServiceTests(unittest.TestCase):
    def test_impart_facade_no_longer_exposes_legacy_card_disassemble_service(self):
        from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_impart

        self.assertFalse(hasattr(xiuxian_impart, "_card_disassemble_service_instance"))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "impart.db"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE impart_cards (user_id TEXT, card_name TEXT, quantity INTEGER, PRIMARY KEY(user_id, card_name))")
            conn.execute("CREATE TABLE xiuxian_impart (user_id TEXT PRIMARY KEY, stone_num INTEGER)")
            conn.execute("INSERT INTO impart_cards VALUES (%s,%s,%s)", ("u", "卡", 4))
            conn.execute("INSERT INTO xiuxian_impart VALUES (%s,%s)", ("u", 10))
        with DatabaseUnitOfWork(self.db) as uow:
            apply_impart_card_operations(uow)
            apply_impart_exact_crystal_balances(uow)
        self.service = CardDisassembleService(self.db)

    def tearDown(self):
        self.temp.cleanup()

    def state(self):
        with db_backend.connection(self.db) as conn:
            card = conn.execute("SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s", ("u", "卡")).fetchone()[0]
            stone_row = conn.execute(
                "SELECT stone_num,stone_num_exact FROM xiuxian_impart WHERE user_id=%s",
                ("u",),
            ).fetchone()
            stone = stone_row[1] if stone_row[1] is not None else stone_row[0]
            return int(card), int(stone)

    def test_success_duplicate_and_conflict(self):
        first = self.service.disassemble("op", "u", "卡", 2, 4, 10)
        duplicate = self.service.disassemble("op", "u", "卡", 2, 4, 10)
        # different expected snapshot on same op must still replay
        conflict = self.service.disassemble("op", "u", "卡", 2, 1, 99)
        self.assertEqual((first.status, duplicate.status, conflict.status), ("applied", "duplicate", "duplicate"))
        self.assertEqual(self.state(), (2, 14))

    def test_rejections_and_stale_snapshot_change_nothing(self):
        self.assertEqual(self.service.disassemble("stale", "u", "卡", 1, 3, 10).status, "state_changed")
        self.assertEqual(self.service.disassemble("keep", "u", "卡", 4, 4, 10).status, "card_missing")
        self.assertEqual(self.service.disassemble("missing", "x", "卡", 1, 0, 0).status, "user_missing")
        self.assertEqual(self.state(), (4, 10))

    def test_operation_failure_rolls_back_card_and_stones(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TRIGGER fail_disassemble BEFORE INSERT ON impart_card_disassemble_operations BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(db_backend.IntegrityError):
            self.service.disassemble("fail", "u", "卡", 2, 4, 10)
        self.assertEqual(self.state(), (4, 10))

    def test_oversized_balance_is_deducted_and_replayed_exactly(self):
        huge = 10**45 + 123
        with DatabaseUnitOfWork(self.db) as uow:
            uow.execute(
                "UPDATE xiuxian_impart SET stone_num=?,stone_num_exact=?,"
                "stone_num_precision_state='exact' WHERE user_id='u'",
                (huge, str(huge)),
            )

        first = self.service.disassemble("large", "u", "卡", 2, 4, huge)
        replay = self.service.disassemble("large", "u", "卡", 2, 4, huge)

        self.assertEqual((first.status, first.stone_quantity), ("applied", huge + 4))
        self.assertEqual((replay.status, replay.stone_quantity), ("duplicate", huge + 4))
        self.assertEqual(self.state(), (2, huge + 4))
        with db_backend.connection(self.db) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT stone_quantity_exact FROM impart_card_disassemble_operations "
                    "WHERE operation_id='large'"
                ).fetchone()[0],
                str(huge + 4),
            )

    def test_unrecoverable_legacy_real_is_rejected_without_card_or_balance_writes(self):
        with DatabaseUnitOfWork(self.db) as uow:
            uow.execute("DROP TRIGGER impart_crystal_exact_write_guard")
            uow.execute(
                "UPDATE xiuxian_impart SET stone_num=CAST(? AS REAL),"
                "stone_num_exact=NULL,stone_num_precision_state='exact' WHERE user_id='u'",
                (float(2**63),),
            )
            apply_impart_exact_crystal_balances(uow)
        before = self.state()

        result = self.service.disassemble("unrecoverable", "u", "卡", 2, 4, 2**63)

        self.assertEqual(
            (result.status, result.reason),
            ("precision_unavailable", "crystal_precision_lost_real"),
        )
        self.assertEqual(self.state(), before)
        with db_backend.connection(self.db) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM impart_card_disassemble_operations").fetchone()[0],
                0,
            )


if __name__ == "__main__":
    unittest.main()
