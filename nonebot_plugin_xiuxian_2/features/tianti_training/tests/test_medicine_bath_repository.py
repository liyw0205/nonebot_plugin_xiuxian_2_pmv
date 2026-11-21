import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....core.errors import DomainError
from ..repository import TiantiMedicineBathSqlRepository


class Profile:
    def default_data(self):
        return {
            "tianti_level": "one", "tianti_hp": 10, "last_settle_time": None,
            "medicine_last_time": None, "medicine_end_time": None,
            "medicine_effect": 0.0, "medicine_name": "", "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [], "qiaoxue_stage_opened": {},
        }

    def levels(self):
        return {"one": {"rank": 1, "hp_gain_per_min": 2, "need_hp": 0}}

    def clean(self, row):
        data = self.default_data()
        data.update({key: value for key, value in row.items() if value is not None})
        return data

    def cap(self, data):
        return 1000


class MedicineBathRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_num INTEGER, bind_num INTEGER, UNIQUE(user_id, goods_id))")
            conn.execute("INSERT INTO back VALUES ('u', 1, 5, 5)")
            conn.execute("CREATE TABLE tianti_medicine_bath_operations (operation_id TEXT PRIMARY KEY, user_id TEXT NOT NULL, request_json TEXT NOT NULL, result_json TEXT NOT NULL)")
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE tianti_info (user_id TEXT PRIMARY KEY, tianti_level TEXT, tianti_hp TEXT, last_settle_time TEXT, medicine_last_time TEXT, medicine_end_time TEXT, medicine_effect TEXT, medicine_name TEXT, opened_qiaoxue TEXT, opened_qiaoxue_detail TEXT, qiaoxue_stage_opened TEXT)")
            conn.execute("INSERT INTO tianti_info (user_id, tianti_level, tianti_hp, opened_qiaoxue, opened_qiaoxue_detail, qiaoxue_stage_opened) VALUES ('u', 'one', '10', '[]', '[]', '{}')")
        self.repo = TiantiMedicineBathSqlRepository(self.game, self.player, profile_reader=Profile())
        self.plan = ({"item_id": 1, "name": "herb", "amount": 2},)
        self.now = datetime(2026, 9, 14, 10)

    def tearDown(self):
        self.tmp.cleanup()

    def stocks(self):
        with sqlite3.connect(self.game) as conn:
            return conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1"
            ).fetchone()[0]

    def test_success_consumes_inventory_and_replays(self):
        first = self.repo.apply_bath("op", "u", self.plan, 1.5, "slot", self.now, 60)
        second = self.repo.apply_bath("op", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual((first.status, first.effect, first.end_time), ("applied", 1.5, "2026-09-14 11:00:00"))
        self.assertEqual(second.status, "duplicate")
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()[0], 3)
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(conn.execute("SELECT medicine_effect FROM tianti_info WHERE user_id='u'").fetchone()[0], "1.5")

    def test_insufficient_inventory_does_not_write(self):
        result = self.repo.apply_bath("short", "u", ({"item_id": 1, "name": "herb", "amount": 6},), 1.5, "slot", self.now, 60)
        self.assertEqual(result.status, "item_insufficient")
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()[0], 5)

    def test_player_write_failure_rolls_back_inventory(self):
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TRIGGER fail_bath BEFORE UPDATE OF medicine_effect ON tianti_info BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.apply_bath("fail", "u", self.plan, 1.5, "slot", self.now, 60)
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=1").fetchone()[0], 5)
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM tianti_medicine_bath_operations").fetchone()[0], 0)

    def test_ignored_receipt_write_rolls_back_inventory_and_profile(self):
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER ignore_bath_receipt BEFORE INSERT "
                "ON tianti_medicine_bath_operations BEGIN SELECT RAISE(IGNORE); END"
            )

        with self.assertRaises(DomainError) as error:
            self.repo.apply_bath("ignored-receipt", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual(error.exception.code, "tianti_bath_receipt_write_rejected")
        self.assertEqual(
            error.exception.details,
            {"reason": "tianti_bath_receipt_write_rejected", "subject": "receipt"},
        )

        with sqlite3.connect(self.game) as conn:
            self.assertEqual(
                conn.execute("SELECT goods_num,bind_num FROM back WHERE user_id='u' AND goods_id=1").fetchone(),
                (5, 5),
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM tianti_medicine_bath_operations").fetchone()[0],
                0,
            )
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT tianti_hp,medicine_effect,medicine_end_time "
                "FROM tianti_info WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(row, ("10", None, None))

    def test_ignored_inventory_write_is_a_specific_atomic_rejection(self):
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER ignore_bath_inventory BEFORE UPDATE OF goods_num ON back "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        with self.assertRaises(DomainError) as error:
            self.repo.apply_bath("ignored-inventory", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual(error.exception.code, "tianti_bath_item_write_rejected")
        self.assertEqual(
            error.exception.details,
            {"reason": "tianti_bath_item_write_rejected", "subject": "source"},
        )
        self.assertEqual(self.stocks(), 5)
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT medicine_effect,medicine_end_time FROM tianti_info WHERE user_id='u'"
                ).fetchone(),
                (None, None),
            )

    def test_ignored_profile_write_is_a_specific_atomic_rejection(self):
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER ignore_bath_profile BEFORE UPDATE OF medicine_effect ON tianti_info "
                "BEGIN SELECT RAISE(IGNORE); END"
            )

        with self.assertRaises(DomainError) as error:
            self.repo.apply_bath("ignored-profile", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual(error.exception.code, "tianti_bath_profile_projection_mismatch")
        self.assertEqual(error.exception.details["subject"], "player_profile")
        self.assertEqual(self.stocks(), 5)
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM tianti_medicine_bath_operations").fetchone()[0],
                0,
            )
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT medicine_effect,medicine_end_time FROM tianti_info WHERE user_id='u'"
                ).fetchone(),
                (None, None),
            )

    def test_rewritten_inventory_projection_rolls_back_all_writes(self):
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_bath_inventory AFTER UPDATE OF goods_num ON back "
                "BEGIN UPDATE back SET goods_num=goods_num+1 "
                "WHERE user_id=NEW.user_id AND goods_id=NEW.goods_id; END"
            )

        with self.assertRaises(DomainError) as error:
            self.repo.apply_bath("rewritten-item", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual(error.exception.code, "tianti_bath_item_projection_mismatch")
        self.assertEqual(error.exception.details["subject"], "source")
        self.assertEqual(self.stocks(), 5)
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT medicine_effect,medicine_end_time FROM tianti_info WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(row, (None, None))

    def test_rewritten_profile_projection_rolls_back_all_writes(self):
        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_bath_profile AFTER UPDATE OF medicine_effect "
                "ON tianti_info BEGIN UPDATE tianti_info SET medicine_effect='9.9' "
                "WHERE user_id=NEW.user_id; END"
            )

        with self.assertRaises(DomainError) as error:
            self.repo.apply_bath("rewritten-profile", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual(error.exception.code, "tianti_bath_profile_projection_mismatch")
        self.assertEqual(error.exception.details["subject"], "player_profile")
        self.assertEqual(self.stocks(), 5)
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT medicine_effect,medicine_end_time FROM tianti_info WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(row, (None, None))

    def test_rewritten_receipt_projection_rolls_back_all_writes(self):
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_bath_receipt AFTER INSERT "
                "ON tianti_medicine_bath_operations BEGIN UPDATE "
                "tianti_medicine_bath_operations SET result_json='{}' "
                "WHERE operation_id=NEW.operation_id; END"
            )

        with self.assertRaises(DomainError) as error:
            self.repo.apply_bath("rewritten-receipt", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual(error.exception.code, "tianti_bath_receipt_projection_mismatch")
        self.assertEqual(error.exception.details["subject"], "receipt")
        self.assertEqual(self.stocks(), 5)
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM tianti_medicine_bath_operations").fetchone()[0],
                0,
            )
        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT medicine_effect,medicine_end_time FROM tianti_info WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(row, (None, None))

    def test_inventory_abort_rolls_back_all_writes(self):
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER abort_bath_inventory BEFORE UPDATE OF goods_num ON back "
                "BEGIN SELECT RAISE(ABORT, 'isolated inventory abort'); END"
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated inventory abort"):
            self.repo.apply_bath("abort-inventory", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual(self.stocks(), 5)
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT medicine_effect,medicine_end_time FROM tianti_info WHERE user_id='u'"
                ).fetchone(),
                (None, None),
            )
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM tianti_medicine_bath_operations").fetchone()[0],
                0,
            )

    def test_receipt_abort_rolls_back_inventory_and_profile(self):
        with sqlite3.connect(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER abort_bath_receipt BEFORE INSERT "
                "ON tianti_medicine_bath_operations "
                "BEGIN SELECT RAISE(ABORT, 'isolated receipt abort'); END"
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "isolated receipt abort"):
            self.repo.apply_bath("abort-receipt", "u", self.plan, 1.5, "slot", self.now, 60)
        self.assertEqual(self.stocks(), 5)
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT medicine_effect,medicine_end_time FROM tianti_info WHERE user_id='u'"
                ).fetchone(),
                (None, None),
            )
        with sqlite3.connect(self.game) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM tianti_medicine_bath_operations").fetchone()[0],
                0,
            )


if __name__ == "__main__":
    unittest.main()
