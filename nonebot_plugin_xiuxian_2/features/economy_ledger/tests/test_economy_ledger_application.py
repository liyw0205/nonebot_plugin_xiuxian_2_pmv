from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ..application import EconomyLedgerApplication
from ..repository import ECONOMY_LOG_FIELDS
from ....infrastructure.database import DatabaseUnitOfWork


class _Clock:
    def now(self) -> datetime:
        return datetime(2026, 10, 8, 12, 30, tzinfo=timezone.utc)


class EconomyLedgerApplicationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.database = Path(self.directory.name) / "game.db"

    def tearDown(self) -> None:
        self.directory.cleanup()

    def _create_full_schema(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE economy_log ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,user_id TEXT,sect_id INTEGER,"
                "source TEXT,action TEXT,stone_delta INTEGER,exp_delta INTEGER,"
                "sect_contribution_delta INTEGER,sect_scale_delta INTEGER,"
                "sect_materials_delta INTEGER,item_delta TEXT,detail TEXT,trace_id TEXT,created_at TEXT)"
            )
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)")
            uow.execute("CREATE TABLE sects(sect_id INTEGER,sect_name TEXT)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u1','一号'),('u2','二号'),('u3','三号')")
            uow.execute("INSERT INTO sects VALUES(10,'青云宗'),(20,'玄天宗')")

    def _insert(
        self,
        user_id: str,
        sect_id: int,
        source: str,
        action: str,
        stone_delta: int,
        exp_delta: int,
        item_delta: str,
        created_at: str,
    ) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO economy_log(user_id,sect_id,source,action,stone_delta,exp_delta,"
                "sect_contribution_delta,sect_scale_delta,sect_materials_delta,item_delta,detail,"
                "trace_id,created_at) VALUES(?,?,?,?,?,?,0,0,0,?,'{}','t',?)",
                (user_id, sect_id, source, action, stone_delta, exp_delta, item_delta, created_at),
            )

    def test_query_page_filters_clamps_page_and_aggregates_the_filtered_rows(self) -> None:
        self._create_full_schema()
        self._insert("u1", 10, "gift", "grant", 100, 10, "[]", "2026-10-07 08:00:00")
        self._insert("u2", 10, "gift", "grant", -50, 4, '[{"name":"药"}]', "2026-10-07 09:00:00")
        self._insert("u1", 20, "quest", "reward", 200, 3, "[]", "2026-10-08 08:00:00")
        self._insert("u3", 20, "gift", "admin_adjust", -200, 0, "null", "2026-10-08 10:00:00")

        result = EconomyLedgerApplication(self.database, clock=_Clock()).query_page(
            {"source": "gift", "page_size": "1", "page": "99", "anomaly_only": "0"}
        )

        self.assertEqual(3, result["total"])
        self.assertEqual(3, result["total_pages"])
        self.assertEqual(3, result["page"])
        self.assertFalse(result["has_next"])
        self.assertEqual("u1", result["rows"][0]["user_id"])
        self.assertEqual(
            {
                "records": 3,
                "unique_users": 3,
                "stone_in": 100,
                "stone_out": 250,
                "stone_net": -150,
                "exp_total": 14,
                "sect_contribution_total": 0,
                "sect_scale_total": 0,
                "sect_materials_total": 0,
                "item_change_records": 1,
            },
            result["summary"],
        )
        self.assertEqual("玄天宗", result["sect_stats"][0]["sect_name"])
        self.assertEqual("gift", result["source_options"][0])
        self.assertNotIn("event_id", ECONOMY_LOG_FIELDS)
        self.assertEqual({"source": "gift", "page_size": 1, "page": 3}, result["query_args"])
        self.assertEqual(1, result["first_page_args"]["page"])
        self.assertEqual(2, result["prev_page_args"]["page"])
        self.assertEqual(3, result["next_page_args"]["page"])
        self.assertEqual(3, result["last_page_args"]["page"])
        self.assertEqual(result["query_args"], result["export_args"])
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            indexes = uow.query_all(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='economy_log'"
            )
        self.assertEqual([], indexes)

        item_rows = EconomyLedgerApplication(self.database).query_page(
            {"has_item_delta": "1", "min_abs_stone_delta": "50"}
        )
        self.assertEqual(1, item_rows["total"])
        self.assertEqual("u2", item_rows["rows"][0]["user_id"])
        anomaly_rows = EconomyLedgerApplication(self.database).query_page(
            {"anomaly_only": "1"}
        )
        self.assertEqual(1, anomaly_rows["total"])
        self.assertEqual("admin_adjust", anomaly_rows["rows"][0]["action"])

    def test_legacy_schema_ignores_missing_filter_columns_and_keeps_page_dto(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "CREATE TABLE economy_log(id INTEGER PRIMARY KEY,user_id TEXT,source TEXT,"
                "action TEXT,stone_delta INTEGER,item_delta TEXT,created_at TEXT)"
            )
            uow.execute(
                "INSERT INTO economy_log VALUES(1,'u','old','grant',5,'[]','2020-01-01 00:00:00')"
            )

        result = EconomyLedgerApplication(self.database).query_page(
            {"sect_id": "missing-column", "page": "1", "page_size": "25"}
        )

        self.assertIsNone(result["error"])
        self.assertEqual(1, result["total"])
        self.assertEqual("u", result["rows"][0]["user_id"])
        self.assertEqual("delta-positive", result["rows"][0]["stone_delta_class"])
        self.assertEqual([], result["sect_stats"])
        self.assertEqual(["grant"], result["action_options"])

    def test_export_is_ordered_fixed_width_and_streams_across_batches(self) -> None:
        self._create_full_schema()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.executemany(
                "INSERT INTO economy_log(user_id,sect_id,source,action,stone_delta,exp_delta,"
                "sect_contribution_delta,sect_scale_delta,sect_materials_delta,item_delta,detail,"
                "trace_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    (f"u{i % 5}", i % 2, "test", "bulk", i, 0, 0, 0, 0, "[]", "{}", "t", "2026-10-08 00:00:00")
                    for i in range(520)
                ),
            )

        rows = EconomyLedgerApplication(self.database).iter_export_rows({"source": "test"})
        self.assertTrue(iter(rows) is rows)
        first = next(rows)
        self.assertEqual(520, first["id"])
        self.assertEqual(tuple(ECONOMY_LOG_FIELDS), tuple(first))
        self.assertEqual("t", first["trace_id"])
        remainder = list(rows)
        self.assertEqual(520, len(remainder) + 1)
        self.assertEqual(519, remainder[0]["id"])
        self.assertEqual(1, remainder[-1]["id"])
        self.assertEqual("[]", first["item_delta"])

    def test_missing_database_and_missing_table_return_empty_page_and_export(self) -> None:
        missing = self.database.parent / "missing.db"
        page = EconomyLedgerApplication(missing).query_page({})
        self.assertEqual("修仙数据库不存在，暂无经济流水。", page["notice"])
        self.assertEqual([], list(EconomyLedgerApplication(missing).iter_export_rows({})))
        self.assertFalse(missing.exists())

        with DatabaseUnitOfWork(self.database):
            pass
        page = EconomyLedgerApplication(self.database).query_page({})
        self.assertEqual("economy_log 表尚未创建，暂无经济流水。", page["notice"])
        self.assertEqual([], list(EconomyLedgerApplication(self.database).iter_export_rows({})))

    def test_quick_preset_uses_injected_clock(self) -> None:
        self._create_full_schema()
        result = EconomyLedgerApplication(self.database, clock=_Clock()).query_page({"preset": "7d"})
        self.assertEqual("2026-10-02 00:00:00", result["filters"]["start_time"])
        self.assertEqual("2026-10-08 12:30:00", result["filters"]["end_time"])


if __name__ == "__main__":
    unittest.main()
