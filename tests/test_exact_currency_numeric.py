from __future__ import annotations

import sqlite3
import unittest

from nonebot_plugin_xiuxian_2.core.errors import DomainError
from nonebot_plugin_xiuxian_2.core.numeric import (
    bind_sqlite_param, exact_crystal_integer, normalize_user_row,
    register_numeric_functions, require_exact_integer, sqlite_int_compare,
)


class ExactCurrencyNumericTests(unittest.TestCase):
    def test_exact_binding_survives_real_integer_affinity(self):
        with sqlite3.connect(":memory:") as conn:
            register_numeric_functions(conn)
            conn.execute("CREATE TABLE wallet(value INTEGER)")
            for value in (2**53 - 1, 2**53, 2**53 + 1, 2**63 - 1, 2**63, 2**63 + 1, 10**45 + 123, -2**63 - 1):
                with self.subTest(value=value):
                    conn.execute("DELETE FROM wallet")
                    conn.execute("INSERT INTO wallet VALUES(?)", (bind_sqlite_param(value, exact=True),))
                    stored, kind = conn.execute("SELECT value,typeof(value) FROM wallet").fetchone()
                    self.assertIn(kind, {"integer", "blob"})
                    self.assertEqual(value, exact_crystal_integer(stored))
                    self.assertEqual(value, normalize_user_row({"stone": stored})["stone"])
                    self.assertEqual(0, conn.execute("SELECT xiuxian_int_cmp(value,?,1) FROM wallet", (str(value),)).fetchone()[0])
                    conn.execute("UPDATE wallet SET value=xiuxian_int_store(xiuxian_int_sub(value,1))")
                    self.assertEqual(value - 1, exact_crystal_integer(conn.execute("SELECT value FROM wallet").fetchone()[0]))

    def test_safe_equivalent_forms_and_real_changes(self):
        for value in ("100", "1e2", "100.000", 100, 100.0, b"1E2"):
            self.assertEqual(100, require_exact_integer(value, field="stone", subject="sender"))
            self.assertEqual(0, sqlite_int_compare(value, "100", True))
            self.assertEqual(-1, sqlite_int_compare(value, 101, True))
        value = 10**45 + 123
        self.assertEqual(-1, sqlite_int_compare(str(value), str(value + 1), True))
        self.assertEqual(1, sqlite_int_compare(str(value + 1).encode("ascii"), str(value), True))

    def test_unrecoverable_and_invalid_values_are_specific(self):
        for value, reason in ((float(2**53), "stone_precision_lost_real"), (float(2**63), "stone_precision_lost_real"), (1.5, "stone_invalid_numeric_value"), ("1e-2", "stone_invalid_numeric_value"), ("not-an-integer", "stone_invalid_numeric_value"), (float("nan"), "stone_invalid_numeric_value")):
            with self.subTest(value=value):
                self.assertIsNone(sqlite_int_compare(value, 1, True))
                with self.assertRaises(DomainError) as caught:
                    require_exact_integer(value, field="stone", subject="sender")
                self.assertEqual((reason, "sender"), (caught.exception.details["reason"], caught.exception.details["subject"]))
        for value in (float('inf'), float('-inf')):
            with self.assertRaises(DomainError) as caught:
                require_exact_integer(value, field="stone", subject="sender")
            self.assertEqual("stone_invalid_numeric_value", caught.exception.details["reason"])

    def test_receipt_precision_refusals_keep_field_and_party_in_reply(self):
        from nonebot_plugin_xiuxian_2.xiuxian.command_guard import failure_reply

        fields = {"admin_receipt_stone": "管理员调账余额回执",
                  "admin_receipt_delta": "管理员调账金额回执",
                  "purchase_receipt_cost": "仙肆购买扣费回执",
                  "training_receipt_points": "历练兑换余额回执"}
        for field, label in fields.items():
            for value, suffix in ((float(2**63), "precision_lost_real"), ("bad", "invalid_numeric_value")):
                for party, party_label in (("buyer", "购买方"), ("seller", "出售方")):
                    with self.subTest(field=field, suffix=suffix, party=party):
                        with self.assertRaises(DomainError) as caught:
                            require_exact_integer(value, field=field, subject=party)
                        self.assertEqual((field + "_" + suffix, party),
                                         (caught.exception.details["reason"], caught.exception.details["subject"]))
                        reply = failure_reply("结算", caught.exception)
                        self.assertIn(label, reply)
                        self.assertIn(party_label, reply)
                        self.assertNotIn("内部异常", reply)


if __name__ == "__main__":
    unittest.main()
