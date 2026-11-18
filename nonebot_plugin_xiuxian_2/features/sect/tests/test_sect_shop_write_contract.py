import tempfile
import unittest
from pathlib import Path

from ..repository import SectRenameSqlRepository
from tests.test_db_backend import db_backend


class SectShopWriteContractTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database = Path(self.temp_dir.name) / "sect.sqlite3"
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_contribution INTEGER)"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES ('u', 1, 100)")
            conn.execute(
                "CREATE TABLE sects (sect_id INTEGER PRIMARY KEY, sect_materials INTEGER, closed INTEGER)"
            )
            conn.execute("INSERT INTO sects VALUES (1, 100, 0)")
            conn.execute(
                "CREATE TABLE back ("
                "user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, "
                "create_time TEXT, update_time TEXT, bind_num INTEGER, UNIQUE(user_id, goods_id))"
            )
            conn.execute(
                "CREATE TABLE sect_shop_weekly_purchases ("
                "user_id TEXT, week_key TEXT, item_id INTEGER, quantity INTEGER, "
                "PRIMARY KEY(user_id, week_key, item_id))"
            )
            conn.execute(
                "CREATE TABLE sect_shop_purchase_operations ("
                "operation_id TEXT PRIMARY KEY, payload TEXT, quantity INTEGER, cost INTEGER, "
                "contribution INTEGER, materials INTEGER, purchased INTEGER)"
            )
        self.repository = SectRenameSqlRepository(self.database)

    def tearDown(self):
        self.temp_dir.cleanup()

    def rows(self, sql):
        with db_backend.connection(self.database) as conn:
            return [tuple(row) for row in conn.execute(sql)]

    def purchase(self, operation_id="op-1"):
        return self.repository.purchase(
            operation_id, "u", 1, 9, "丹药", "丹药", 2, 10, 5, 0, 99,
            week_key="2026-W47",
        )

    def assert_rolled_back(self, result, reason, subject):
        self.assertEqual(
            (result["status"], result["reason"], result["subject"]),
            ("state_changed", reason, subject),
        )
        self.assertEqual(self.rows("SELECT sect_contribution,sect_materials FROM user_xiuxian JOIN sects ON sects.sect_id=1"), [(100, 100)])
        self.assertEqual(self.rows("SELECT goods_num,bind_num FROM back"), [])
        self.assertEqual(self.rows("SELECT quantity FROM sect_shop_weekly_purchases"), [])
        self.assertEqual(self.rows("SELECT operation_id FROM sect_shop_purchase_operations"), [])

    def test_member_rewrite_is_specific_and_retryable(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_shop_member AFTER UPDATE OF sect_contribution ON user_xiuxian "
                "BEGIN UPDATE user_xiuxian SET sect_contribution=99 WHERE user_id='u'; END"
            )
        result = self.purchase()
        self.assert_rolled_back(result, "sect_shop_member_write_rejected", "u")
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER rewrite_shop_member")
        self.assertEqual(self.purchase()["status"], "applied")

    def test_state_rewrite_is_specific_and_retryable(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_shop_state AFTER UPDATE OF sect_materials ON sects "
                "BEGIN UPDATE sects SET sect_materials=99 WHERE sect_id=1; END"
            )
        result = self.purchase()
        self.assert_rolled_back(result, "sect_shop_state_write_rejected", "1")
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER rewrite_shop_state")
        self.assertEqual(self.purchase()["status"], "applied")

    def test_operation_conflict_keeps_actor_subject(self):
        self.assertEqual(self.purchase()["status"], "applied")
        conflict = self.repository.purchase(
            "op-1", "u", 1, 9, "丹药", "丹药", 3, 10, 5, 0, 99,
            week_key="2026-W47",
        )
        self.assertEqual((conflict["status"], conflict["reason"], conflict["subject"]), ("state_changed", "operation_conflict", "actor"))

    def test_inventory_ignore_is_specific_and_retryable(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER ignore_shop_inventory BEFORE INSERT ON back "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        result = self.purchase()
        self.assert_rolled_back(result, "sect_shop_inventory_write_rejected", "u")

        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER ignore_shop_inventory")
        self.assertEqual(self.purchase()["status"], "applied")

    def test_weekly_rewrite_is_specific_and_retryable(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_shop_weekly AFTER INSERT ON sect_shop_weekly_purchases "
                "BEGIN UPDATE sect_shop_weekly_purchases SET quantity=99; END"
            )
        result = self.purchase()
        self.assert_rolled_back(result, "sect_shop_weekly_write_rejected", "u")
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER rewrite_shop_weekly")
        self.assertEqual(self.purchase()["status"], "applied")

    def test_receipt_ignore_is_specific_and_retryable(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER ignore_shop_receipt BEFORE INSERT ON sect_shop_purchase_operations "
                "BEGIN SELECT RAISE(IGNORE); END"
            )
        result = self.purchase()
        self.assert_rolled_back(result, "sect_shop_receipt_write_rejected", "op-1")
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER ignore_shop_receipt")
        self.assertEqual(self.purchase()["status"], "applied")

    def test_receipt_side_effect_rewrite_is_caught_by_final_projection(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER rewrite_shop_member_after_receipt AFTER INSERT ON sect_shop_purchase_operations "
                "BEGIN UPDATE user_xiuxian SET sect_contribution=99 WHERE user_id='u'; END"
            )
        result = self.purchase()
        self.assert_rolled_back(result, "sect_shop_member_write_rejected", "u")
        with db_backend.transaction(self.database) as conn:
            conn.execute("DROP TRIGGER rewrite_shop_member_after_receipt")
        self.assertEqual(self.purchase()["status"], "applied")

    def test_receipt_abort_is_transparent(self):
        with db_backend.transaction(self.database) as conn:
            conn.execute(
                "CREATE TRIGGER abort_shop_receipt BEFORE INSERT ON sect_shop_purchase_operations "
                "BEGIN SELECT RAISE(ABORT, 'shop receipt failed'); END"
            )
        with self.assertRaises(db_backend.IntegrityError):
            self.purchase("abort")
        self.assertEqual(self.rows("SELECT sect_contribution,sect_materials FROM user_xiuxian JOIN sects ON sects.sect_id=1"), [(100, 100)])


if __name__ == "__main__":
    unittest.main()
