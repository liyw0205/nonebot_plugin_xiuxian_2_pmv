"""Scheduled sect materials must survive a >int64 combat power.

Production traceback (xiu2.log 11:00 / 12:00 ``materialsupdate_``):

    UPDATE sects SET sect_materials=...,combat_power=? WHERE sect_id=?
    OverflowError: Python int too large to convert to SQLite INTEGER

High-realm sects sum member ``power`` values that already exceed int64, so the
statement aborted and the whole scheduled grant (materials included) was lost.

These cases drive the production path (``SectApplication`` →
``SectScheduledMaterialSqlRepository``) with the deployed NUMERIC affinities and
pin: int64-range grants stay exact INTEGER, >int64 grants finish and record the
receipt as REAL (the disclosed precision limit, not a clamp), and replays stay
idempotent.
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.numeric import adapt_sqlite_params
from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication

HUGE_POWER = 10**25 + 6


class ScheduledMaterialBigPowerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "sect-bigpower.db"
        with sqlite3.connect(self.database) as connection:
            # Production affinity: NUMERIC, not INTEGER.
            connection.execute(
                "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,"
                "sect_scale NUMERIC,sect_materials NUMERIC,combat_power NUMERIC)"
            )
            connection.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,"
                "sect_id INTEGER,power NUMERIC)"
            )
            connection.execute(
                "CREATE TABLE sect_scheduled_material_grants(grant_key TEXT,"
                "sect_id INTEGER,materials INTEGER,combat_power NUMERIC,"
                "PRIMARY KEY(grant_key,sect_id))"
            )
            connection.execute("INSERT INTO sects VALUES(1,'owner',120,50,1)")
            connection.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?,?)",
                (
                    ("owner", 1, adapt_sqlite_params(10**25)[0]),
                    ("member", 1, 6),
                ),
            )
        self.service = SectApplication(self.database)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def sect_row(self) -> tuple:
        with sqlite3.connect(self.database) as connection:
            return tuple(
                connection.execute(
                    "SELECT typeof(sect_materials),sect_materials,"
                    "typeof(combat_power),combat_power FROM sects WHERE sect_id=1"
                ).fetchone()
            )

    def receipt(self) -> tuple:
        with sqlite3.connect(self.database) as connection:
            return connection.execute(
                "SELECT materials,combat_power FROM sect_scheduled_material_grants"
                " WHERE grant_key='sect-materials:2026-11-08' AND sect_id=1"
            ).fetchone()

    def stored_power_total(self) -> int:
        """What the SQL sum reports for this sect (double -> int), i.e. the applied value."""
        with sqlite3.connect(self.database) as connection:
            total = connection.execute(
                "SELECT COALESCE(SUM(power),0) FROM user_xiuxian WHERE sect_id=1"
            ).fetchone()[0]
        return int(total)

    def test_int64_range_grant_stays_exact_integer(self) -> None:
        with sqlite3.connect(self.database) as connection:
            connection.execute(
                "UPDATE user_xiuxian SET power=300 WHERE user_id='owner'"
            )
            connection.execute(
                "UPDATE user_xiuxian SET power=200 WHERE user_id='member'"
            )

        result = self.service.grant_scheduled_materials("sect-materials:2026-11-08", 1, 2)

        self.assertEqual("granted", result.status)
        self.assertEqual(240, result.materials)
        self.assertEqual(500, result.combat_power)
        self.assertEqual(("integer", 290, "integer", 500), self.sect_row())
        self.assertEqual((240, 500), self.receipt())

    def test_high_power_grant_finishes_and_records_the_receipt(self) -> None:
        expected_power = self.stored_power_total()
        self.assertGreater(expected_power, 2**63 - 1)

        result = self.service.grant_scheduled_materials("sect-materials:2026-11-08", 1, 2)

        self.assertEqual("granted", result.status)
        self.assertEqual(240, result.materials)
        self.assertEqual(expected_power, result.combat_power)
        materials_kind, materials, power_kind, power = self.sect_row()
        self.assertEqual(("integer", 290), (materials_kind, materials))
        # Beyond int64 the column keeps REAL (double) instead of raising.
        self.assertEqual("real", power_kind)
        self.assertEqual(float(expected_power), power)
        self.assertEqual((240, float(expected_power)), self.receipt())

    def test_high_power_replay_is_idempotent(self) -> None:
        expected_power = self.stored_power_total()
        first = self.service.grant_scheduled_materials("sect-materials:2026-11-08", 1, 2)
        self.assertEqual("granted", first.status)
        before = self.sect_row()

        replay = self.service.grant_scheduled_materials("sect-materials:2026-11-08", 1, 9)

        self.assertEqual("duplicate", replay.status)
        self.assertEqual(240, replay.materials)
        # Replay reports the stored value (REAL -> int), never a second grant.
        self.assertEqual(expected_power, replay.combat_power)
        self.assertEqual(before, self.sect_row())

    def test_missing_receipt_table_is_reported_without_writing(self) -> None:
        with sqlite3.connect(self.database) as connection:
            connection.execute("DROP TABLE sect_scheduled_material_grants")

        result = self.service.grant_scheduled_materials("sect-materials:2026-11-08", 1, 2)

        self.assertEqual("schema_missing", result.status)
        self.assertEqual(("integer", 50, "integer", 1), self.sect_row())


if __name__ == "__main__":
    unittest.main()
