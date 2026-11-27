"""Live sect-directory projections avoid per-sect financial/member reads."""
from __future__ import annotations

import asyncio
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
from nonebot_plugin_xiuxian_2.features.sect.application import SectApplication
from nonebot_plugin_xiuxian_2.features.sect.repository import SectRenameSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_sect_read_stage import ROOT, Finished, load


class SectDirectoryReadStageTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "sect.db"
        with DatabaseUnitOfWork(self.database) as uow:
            # No primary keys: exercise supported legacy duplicate identities.
            uow.execute("CREATE TABLE sects(sect_id INTEGER,sect_name TEXT,sect_scale,"
                        "sect_owner TEXT,closed,join_open,sect_materials,sect_used_stone,stone)")
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT,"
                        "sect_id INTEGER,stone,sect_position INTEGER)")
            uow.execute("CREATE INDEX member_sect ON user_xiuxian(sect_id)")
            uow.execute("CREATE INDEX member_user ON user_xiuxian(user_id)")
            uow.execute("INSERT INTO sects VALUES(1,'Sect',0,'owner',0,1,'invalid',1.5,1e30)")
            uow.execute("INSERT INTO user_xiuxian VALUES('owner','Owner',1,'invalid',0)")
            uow.execute("CREATE TABLE sect_member_join_operations(operation_id TEXT PRIMARY KEY,"
                        "user_id TEXT,sect_id INTEGER,member_count INTEGER,member_limit INTEGER)")
        self.app = SectApplication(self.database)
        self.helpers = load(ROOT / "sect_member_utils.py", {
            "get_sect_member_limit", "sect_join_display_status", "can_join_sect",
        }, {"sect_application": self.app})

    def snapshot(self):
        with sqlite3.connect(self.database) as connection:
            return tuple(connection.iterdump())

    def status(self, row):
        return self.helpers["sect_join_display_status"](
            row if row["sect_id"] is not None else None, row["member_count"])

    def run_directory(self):
        async def finish():
            raise Finished

        sender = AsyncMock()
        namespace = {
            "assign_bot": AsyncMock(return_value=(SimpleNamespace(self_id="bot"), None)),
            "sect_application": self.app,
            "sect_join_display_status": self.helpers["sect_join_display_status"],
            "number_to": str,
            "send_msg_handler": sender,
            "sect_list": SimpleNamespace(finish=finish),
        }
        load(ROOT / "__init__.py", {"sect_list_"}, namespace)
        with self.assertRaises(Finished):
            asyncio.run(namespace["sect_list_"](SimpleNamespace(self_id="bot"), None))
        self.assertEqual("宗门列表", sender.call_args.args[2])
        return sender.call_args.args[4]

    def test_handler_uses_one_readonly_projection_and_preserves_output_without_writes(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO sects VALUES(2,'No owner',50000000,NULL,0,1,0,0,0)")
        before = self.snapshot()
        statements = []
        enter = DatabaseUnitOfWork.__enter__
        query = DatabaseUnitOfWork.query_all

        def readonly(uow):
            self.assertTrue(uow.read_only)
            return enter(uow)

        def collect(uow, sql, params=()):
            statements.append(sql)
            return query(uow, sql, params)

        with patch.object(DatabaseUnitOfWork, "__enter__", readonly), patch.object(
                DatabaseUnitOfWork, "query_all", collect), patch.object(
                self.app, "get_sect_info", side_effect=AssertionError("per-sect full row")), \
                patch.object(self.app, "list_sect_members", side_effect=AssertionError("member list")), \
                patch.object(self.app, "count_sect_members", side_effect=AssertionError("per-sect count")):
            output = self.run_directory()
        self.assertEqual([
            "编号1：Sect\n宗主：Owner\n宗门状态：可加入 (1/20)\n建设度：0\n",
            "编号2：No owner\n宗主：暂无\n宗门状态：可加入 (0/21)\n建设度：50000000\n",
        ], output)
        self.assertEqual(1, len(statements))
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            plan = uow.query_all("EXPLAIN QUERY PLAN " + statements[0])
        details = "\n".join(row["detail"] for row in plan)
        self.assertIn("USING COVERING INDEX member_sect (sect_id=?)", details)
        self.assertIn("USING INDEX member_user (user_id=?)", details)
        self.assertEqual(before, self.snapshot())
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("DELETE FROM sects")
        self.assertEqual([], self.run_directory())

    def test_projection_counts_null_duplicate_members_and_selects_first_duplicate_rows(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO sects VALUES(1,'Later row',0,'second',1,0,0,0,0)")
            uow.execute("INSERT INTO sects VALUES(NULL,'Null identity',0,NULL,1,0,0,0,0)")
            uow.executemany("INSERT INTO user_xiuxian VALUES(?,?,?,?,?)", [
                ("owner", "Later owner", 1, "invalid", 0),
                ("duplicate", "One", 1, 1.5, 1),
                ("duplicate", "Two", 1, "invalid", 1),
                (None, "Anonymous", 1, 1e30, 1),
                ("unaffiliated", "Outside", None, "invalid", 1),
            ])
        before = self.snapshot()
        rows = self.app.list_sect_join_display()
        self.assertEqual([None, 1], [row["sect_id"] for row in rows])
        self.assertEqual((False, "宗门不存在"), self.status(rows[0]))
        self.assertEqual(0, rows[0]["member_count"])
        self.assertEqual(("Sect", "Owner", 5), (
            rows[1]["sect_name"], rows[1]["owner_name"], rows[1]["member_count"]))
        self.assertEqual((True, "可加入 (5/20)"), self.status(rows[1]))
        self.assertIn("宗门状态：宗门不存在", self.run_directory()[0])
        self.assertEqual(before, self.snapshot())

    def test_status_retains_priority_and_python_truthiness(self):
        status = self.helpers["sect_join_display_status"]
        self.assertEqual((False, "宗门不存在"), status(None, 100))
        for closed, join_open, expected in (
            (1, 0, (False, "宗门已封闭")),
            ("0", 1, (False, "宗门已封闭")),
            (0, 0, (False, "宗门关闭加入")),
            (None, None, (False, "宗门关闭加入")),
            (0, "0", (True, "可加入 (1/20)")),
            (None, 1, (True, "可加入 (1/20)")),
        ):
            with self.subTest(closed=closed, join_open=join_open):
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("UPDATE sects SET closed=?,join_open=?", (closed, join_open))
                self.assertEqual(expected, self.status(self.app.list_sect_join_display()[0]))

    def test_exact_large_scale_and_capacity_boundaries_keep_integer_arithmetic(self):
        for value, members, expected in (
            (0, 19, (True, "可加入 (19/20)")),
            (0, 20, (False, "人数已满 (20/20)")),
            (49999999, 20, (False, "人数已满 (20/20)")),
            (50000000, 20, (True, "可加入 (20/21)")),
            (2**70 + 7, 99, (True, "可加入 (99/100)")),
            (2**70 + 7, 100, (False, "人数已满 (100/100)")),
        ):
            with self.subTest(scale=value, members=members):
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("UPDATE sects SET sect_scale=?", (bind_sqlite_param(value, exact=True),))
                row = self.app.list_sect_join_display()[0]
                self.assertEqual(value, row["sect_scale"])
                self.assertEqual(expected, self.helpers["sect_join_display_status"](row, members))

    def test_projection_avoids_unrelated_fields_and_required_read_denial_recovers(self):
        enter = DatabaseUnitOfWork.__enter__

        def forbid_unused(uow):
            result = enter(uow)
            def authorizer(action, table, column, *_):
                unused = {"sects": {"sect_materials", "sect_used_stone", "stone"},
                          "user_xiuxian": {"stone", "sect_position"}}
                return (sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ
                        and column in unused.get(table, set()) else sqlite3.SQLITE_OK)
            uow.connection.set_authorizer(authorizer)
            return result

        before = self.snapshot()
        with patch.object(DatabaseUnitOfWork, "__enter__", forbid_unused):
            self.assertEqual((True, "可加入 (1/20)"), self.status(self.app.list_sect_join_display()[0]))
        self.assertEqual(before, self.snapshot())

        def deny_required(uow):
            result = enter(uow)
            uow.connection.set_authorizer(lambda action, table, column, *_:
                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ
                and table == "sects" and column == "closed" else sqlite3.SQLITE_OK)
            return result

        with patch.object(DatabaseUnitOfWork, "__enter__", deny_required), \
                self.assertRaises(sqlite3.DatabaseError):
            self.app.list_sect_join_display()
        self.assertEqual((True, "可加入 (1/20)"), self.status(self.app.list_sect_join_display()[0]))
        self.assertEqual(before, self.snapshot())

    def test_next_request_reads_member_and_state_changes_without_reusing_snapshot(self):
        first = self.app.list_sect_join_display()[0]
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO user_xiuxian VALUES(NULL,'New member',1,0,1)")
            uow.execute("UPDATE sects SET sect_scale=50000000,join_open=0,sect_name='Changed'")
        second = self.app.list_sect_join_display()[0]
        self.assertEqual(("Changed", 50000000, 2), (
            second["sect_name"], second["sect_scale"], second["member_count"]))
        self.assertEqual((False, "宗门关闭加入"), self.status(second))
        self.assertEqual((True, "可加入 (1/20)"), self.status(first))

    def test_legacy_live_wrapper_uses_count_and_keeps_closed_short_circuit(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE sects SET sect_materials=0,sect_used_stone=0,stone=0")
        with patch.object(self.app, "list_sect_members", side_effect=AssertionError("full member list")), \
                patch.object(self.app, "count_sect_members", wraps=self.app.count_sect_members) as count:
            self.assertEqual((True, "可加入 (1/20)"), self.helpers["can_join_sect"](1))
            self.assertEqual(1, count.call_count)
            with DatabaseUnitOfWork(self.database) as uow:
                uow.execute("UPDATE sects SET closed=1")
            self.assertEqual((False, "宗门已封闭"), self.helpers["can_join_sect"](1))
            self.assertEqual((False, "宗门不存在"), self.helpers["can_join_sect"](999))
            self.assertEqual(1, count.call_count)

    def test_display_never_authorizes_join_after_live_state_or_capacity_changes(self):
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO user_xiuxian VALUES('new','Applicant',NULL,0,NULL)")
        repository = SectRenameSqlRepository(self.database)
        for name, update, expected in (
            ("closed", "UPDATE sects SET closed=1", "sect_closed"),
            ("join-closed", "UPDATE sects SET join_open=0", "join_closed"),
            ("full", None, "sect_full"),
        ):
            with self.subTest(name=name):
                with DatabaseUnitOfWork(self.database) as uow:
                    uow.execute("UPDATE sects SET closed=0,join_open=1")
                    uow.execute("DELETE FROM user_xiuxian WHERE user_id LIKE 'filler-%'")
                self.assertEqual((True, "可加入 (1/20)"), self.status(self.app.list_sect_join_display()[0]))
                with DatabaseUnitOfWork(self.database) as uow:
                    if update is not None:
                        uow.execute(update)
                    else:
                        uow.executemany("INSERT INTO user_xiuxian VALUES(?,?,1,0,1)",
                                        [(f"filler-{index}", "Member") for index in range(19)])
                result = repository.join(f"directory-{name}", "new", 1)
                self.assertEqual(expected, result["status"])
                with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                    self.assertIsNone(uow.query_one(
                        "SELECT sect_id FROM user_xiuxian WHERE user_id='new'")["sect_id"])
                    self.assertEqual(0, uow.query_one(
                        "SELECT COUNT(*) AS n FROM sect_member_join_operations")["n"])
