"""Regression: `two_exp_cd.find_user` self-heals a dropped migration table.

The buff.003 migration (`partner_two_exp_usage`) may be recorded in
`schema_migrations` as applied while the table is gone -- `TWO_EXP_CD.re_data()`
(the `two_exp_cd_up` reset command) DROPs the table, and because the migration
marker remains the runner never recreates it.  Previously `find_user` raised
`RuntimeError("required migration table is missing: partner_two_exp_usage")`,
which surfaced as repeated xiuxian_buff.partner crashes after an upgrade.

These tests pin the self-healing read path: missing table must not raise, must
fall back to the legacy JSON ledger, recreate the exact migration-owned schema,
and keep add/remove working afterwards.  schema_migrations is never touched.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.paths import configure_paths, reset_paths_for_test
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_buff.two_exp_cd import TWO_EXP_CD


class TwoExpCdSelfHealTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.data = root / "data" / "xiuxian"
        self.data.mkdir(parents=True)
        self.player = self.data / "player.db"
        reset_paths_for_test()
        configure_paths(self.data)
        # Fresh instance bound to a scratch player.db (no migration tables).
        self.cd = TWO_EXP_CD()
        self.cd.data_path = str(root / "two_exp_cd.json")  # type: ignore[assignment]
        self.cd.data = {"two_exp_cd": {}}

    def tearDown(self) -> None:
        reset_paths_for_test()
        self.temp.cleanup()

    def table_exists(self) -> bool:
        import sqlite3

        with sqlite3.connect(self.player) as conn:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='partner_two_exp_usage'"
            ).fetchone()
            return row is not None

    def usage(self, user_id: str) -> int:
        import sqlite3

        with sqlite3.connect(self.player) as conn:
            return int(
                conn.execute(
                    "SELECT used_count FROM partner_two_exp_usage WHERE user_id=?",
                    (user_id,),
                ).fetchone()[0]
            )

    def test_missing_table_returns_legacy_count_and_creates_schema(self) -> None:
        self.assertFalse(self.table_exists())
        self.cd.data["two_exp_cd"]["123"] = 5
        count = self.cd.find_user("123")
        self.assertEqual(count, 5)
        self.assertTrue(self.table_exists())
        self.assertEqual(self.usage("123"), 5)

    def test_missing_table_with_no_legacy_record_returns_zero(self) -> None:
        self.assertFalse(self.table_exists())
        self.assertEqual(self.cd.find_user("123"), 0)
        self.assertTrue(self.table_exists())
        self.assertEqual(self.usage("123"), 0)

    def test_add_and_remove_work_after_self_heal(self) -> None:
        self.assertFalse(self.table_exists())
        self.assertTrue(self.cd.add_user("123"))
        self.assertEqual(self.cd.find_user("123"), 1)
        self.assertTrue(self.cd.remove_user("123"))
        self.assertEqual(self.cd.find_user("123"), 0)

    def test_existing_table_path_is_unchanged(self) -> None:
        import sqlite3

        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TABLE partner_two_exp_usage(user_id TEXT PRIMARY KEY,"
                "used_count INTEGER NOT NULL DEFAULT 0,updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            )
            conn.execute("INSERT INTO partner_two_exp_usage(user_id,used_count) VALUES('123',7)")
        self.assertEqual(self.cd.find_user("123"), 7)
        self.assertEqual(self.usage("123"), 7)
        self.assertTrue(self.table_exists())

    def test_re_data_reset_then_next_read_self_heals(self) -> None:
        self.assertTrue(self.cd.add_user("123"))
        self.assertEqual(self.cd.find_user("123"), 1)
        self.cd.re_data()
        self.assertFalse(self.table_exists())
        # The next read must not raise and must come back at zero.
        self.assertEqual(self.cd.find_user("123"), 0)
        self.assertTrue(self.table_exists())


    def test_migration_marker_present_but_table_missing_self_heals(self) -> None:
        """Exact production shape: buff.003 recorded as applied, table dropped.

        This is the /root/xiu2 failure mode -- schema_migrations says buff.003
        ran (2026-11-07T22:01:03Z) but the table is gone after a
        `two_exp_cd_up` reset.  The migration runner treats it as applied and
        never recreates it; the read path must heal without touching the
        marker, and the marker must still be there afterwards.
        """
        import sqlite3

        with sqlite3.connect(self.player) as conn:
            conn.execute(
                "CREATE TABLE schema_migrations(version TEXT PRIMARY KEY,name TEXT NOT NULL,"
                "checksum TEXT NOT NULL,applied_at TEXT NOT NULL,duration_ms INTEGER NOT NULL)"
            )
            conn.execute(
                "INSERT INTO schema_migrations VALUES('buff.003','partner_two_exp_usage',"
                "'9b2752e021c5','2026-11-07T22:01:03.435844+00:00',0)"
            )
            self.assertIsNone(
                conn.execute("SELECT name FROM sqlite_master WHERE name='partner_two_exp_usage'").fetchone()
            )
        self.cd.data["two_exp_cd"]["legacy"] = 7
        self.assertEqual(self.cd.find_user("legacy"), 7)
        self.assertEqual(self.usage("legacy"), 7)
        with sqlite3.connect(self.player) as conn:
            self.assertIsNotNone(
                conn.execute("SELECT 1 FROM schema_migrations WHERE version='buff.003'").fetchone()
            )
            self.assertIsNotNone(
                conn.execute("SELECT name FROM sqlite_master WHERE name='partner_two_exp_usage'").fetchone()
            )


if __name__ == "__main__":
    unittest.main()
