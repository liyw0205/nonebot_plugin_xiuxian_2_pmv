"""Boss pre-reads reuse a handle while each real settlement uses live state."""
from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.migrations import apply_activity_state_schema
from nonebot_plugin_xiuxian_2.features.activity import application as activity_application
from nonebot_plugin_xiuxian_2.features.activity.repository import ActivityRepository
from nonebot_plugin_xiuxian_2.core.errors import OperationConflictError
from nonebot_plugin_xiuxian_2.infrastructure.database import OperationLedger
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as uow_module
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


DatabaseUnitOfWork = uow_module.DatabaseUnitOfWork
FIGHT_DATE = "2026-10-06"


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 6, 12, tzinfo=tz)


class ActivityBossLiveReaderTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            OperationLedger().ensure_schema(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,atk INTEGER)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u','Initial',100)")
            uow.execute("INSERT INTO activity_boss_state VALUES('boss',1000,1000,'')")
            uow.execute(
                "INSERT INTO activity_item_inventory(activity_key,user_id,item_id,count) "
                "VALUES('boss','u','flare',2)"
            )
        self.repository = ActivityRepository(self.database)
        self.application = activity_application.ActivityApplication(self.database, repository=self.repository)
        self.owner = uow_module.ReadOnlyConnectionOwner()
        self.writer = uow_module.WriteConnectionOwner()
        self.connections = []
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.addCleanup(self.owner.close)
        self.addCleanup(self.writer.close)
        self.stack.enter_context(patch.object(uow_module, "LIVE_PROJECTION_READ_OWNER", self.owner))
        for module in (uow_module, activity_application):
            self.stack.enter_context(patch.object(module, "STONE_TRAINING_WRITE_OWNER", self.writer))
        self.stack.enter_context(patch.object(activity_config, "load_config", return_value={
            "enabled": True,
            "gameplay_activities": [{
                "key": "boss", "type": "activity_boss", "enabled": True,
                "name": "BossEvent", "boss_name": "Boss", "mode": "both",
                "start_time": "0", "end_time": "\u65e0\u9650", "max_hp": 1000,
                "atk_ratio": 1.0, "hit_hp_cap_ratio": 0.2, "daily_fight_limit": 4,
                "items": [{"id": "flare", "name": "Flare", "cost": 1,
                           "damage_min": 25, "damage_max": 25}],
            }],
        }))
        self.stack.enter_context(patch.object(activity_config, "activity_runtime_state", return_value={
            "ok": True, "features": ["boss"], "multiplier": 1.0,
        }))
        self.stack.enter_context(patch("datetime.datetime", FixedDatetime))

        def connect(*args, **kwargs):
            kwargs["factory"] = ObservedConnection
            connection = REAL_CONNECT(*args, **kwargs)
            self.connections.append(connection)
            return connection

        self.stack.enter_context(patch("sqlite3.connect", side_effect=connect))

    def write_external(self, *statements):
        connection = REAL_CONNECT(self.database)
        try:
            for sql, params in statements:
                connection.execute(sql, params)
            connection.commit()
        finally:
            connection.close()

    def row(self, sql, params=()):
        connection = REAL_CONNECT(self.database)
        try:
            return connection.execute(sql, params).fetchone()
        finally:
            connection.close()

    def receipt(self, operation_id):
        return self.row(
            "SELECT damage,hp_left,fight_count,inventory "
            "FROM activity_boss_settlement_operations WHERE operation_id=?",
            (operation_id,),
        )

    def assert_idle_reader(self, reader, transactions):
        readers = [connection for connection in self.connections
                   if "PRAGMA query_only=ON" in connection.statements]
        self.assertEqual([reader], readers)
        self.assertFalse(self.owner._active)
        self.assertFalse(reader.closed)
        self.assertFalse(reader.in_transaction)
        self.assertEqual(transactions, reader.statements.count("BEGIN"))
        self.assertEqual(transactions, reader.commits)
        self.assertEqual(1, reader.statements.count("PRAGMA busy_timeout=30000"))
        self.assertTrue(all(connection.closed for connection in self.connections if connection is not reader))

    def test_cooperative_reads_external_attack_hp_and_quota_on_each_transaction(self):
        self.assertTrue(self.repository._fight_boss(user_id="u", operation_id="coop-first")[0])
        reader = self.connections[0]
        self.assertEqual((100, 900, 1, None), self.receipt("coop-first"))
        self.assert_idle_reader(reader, 1)

        self.write_external(
            ("UPDATE user_xiuxian SET atk=150,user_name='Changed' WHERE user_id='u'", ()),
            ("UPDATE activity_boss_state SET hp_left=600 WHERE activity_key='boss'", ()),
            ("INSERT INTO activity_boss_fight_log(activity_key,user_id,fight_date,source) "
             "VALUES('boss','u',?,'world_boss')", (FIGHT_DATE,)),
        )
        ok, text = self.repository._fight_boss(user_id="u", operation_id="coop-second")
        self.assertTrue(ok, text)
        self.assertIn("Changed", text)
        self.assertEqual((150, 450, 3, None), self.receipt("coop-second"))

        self.write_external((
            "INSERT INTO activity_boss_fight_log(activity_key,user_id,fight_date,source) "
            "VALUES('boss','u',?,'item')", (FIGHT_DATE,),
        ))
        ok, text = self.repository._fight_boss(user_id="u", operation_id="coop-limit")
        self.assertFalse(ok)
        self.assertIn("\u6b21\u6570\u5df2\u7528\u5b8c", text)
        self.assertIsNone(self.receipt("coop-limit"))
        self.assertEqual((450,), self.row("SELECT hp_left FROM activity_boss_state"))
        self.assert_idle_reader(reader, 3)

    def test_item_inventory_depletion_and_replenishment_are_live_between_operations(self):
        action = self.repository._use_boss_item
        self.assertTrue(action(user_id="u", query="Boss Flare", operation_id="item-first")[0])
        reader = self.connections[0]
        self.assertEqual((25, 975, 1, 1), self.receipt("item-first"))
        self.write_external(
            ("UPDATE activity_item_inventory SET count=0 WHERE user_id='u'", ()),
            ("UPDATE activity_boss_state SET hp_left=700 WHERE activity_key='boss'", ()),
        )
        ok, text = action(user_id="u", query="Boss Flare", operation_id="item-empty")
        self.assertFalse(ok)
        self.assertIn("\u4e0d\u8db3", text)
        self.assertIn("0", text)
        self.assertIsNone(self.receipt("item-empty"))
        self.assertEqual((700,), self.row("SELECT hp_left FROM activity_boss_state"))

        self.write_external(("UPDATE activity_item_inventory SET count=3 WHERE user_id='u'", ()))
        ok, text = action(user_id="u", query="Boss Flare", operation_id="item-replenished")
        self.assertTrue(ok, text)
        self.assertEqual((25, 675, 2, 2), self.receipt("item-replenished"))
        self.assertEqual((2,), self.row("SELECT count FROM activity_item_inventory"))
        self.assert_idle_reader(reader, 3)

    def test_read_error_discards_handle_before_settlement_and_next_operation_recovers(self):
        self.assertTrue(self.repository._fight_boss(user_id="u", operation_id="before-error")[0])
        reader = self.connections[0]
        reader.set_authorizer(
            lambda action, table, *_: sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ and table == "user_xiuxian" else sqlite3.SQLITE_OK
        )
        with self.assertRaises(sqlite3.DatabaseError):
            self.repository._fight_boss(user_id="u", operation_id="read-denied")
        self.assertTrue(reader.closed)
        self.assertEqual(1, reader.rollbacks)
        self.assertFalse(self.owner._active)
        self.assertEqual(2, len(self.connections))
        self.assertIsNone(self.receipt("read-denied"))
        self.assertEqual((900,), self.row("SELECT hp_left FROM activity_boss_state"))
        self.assertEqual((1,), self.row("SELECT COUNT(*) FROM activity_boss_fight_log"))

        self.assertTrue(self.repository._fight_boss(user_id="u", operation_id="after-error")[0])
        replacement = self.connections[-2]
        self.assertIsNot(reader, replacement)
        self.assertEqual((100, 800, 2, None), self.receipt("after-error"))
        self.assertFalse(replacement.closed)
        self.assertFalse(replacement.in_transaction)
        self.assertTrue(self.connections[-1].closed)

    def test_explicit_caller_scope_keeps_its_original_key_and_lifetime(self):
        self.assertTrue(self.repository._fight_boss(user_id="u", operation_id="warm-unscoped")[0])
        retained = self.connections[0]
        original_settle = self.repository.boss_settlement.settle_cooperative
        scoped_readers = []

        def settle(**kwargs):
            self.assertFalse(self.owner._active)
            self.assertFalse(retained.closed)
            self.assertFalse(retained.in_transaction)
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                scoped = uow.connection
                self.assertIs(scoped, self.connections[-1])
                self.assertIsNot(retained, scoped)
                self.assertNotIn("PRAGMA query_only=ON", scoped.statements)
                self.assertEqual(2, scoped.statements.count("BEGIN"))
                scoped_readers.append(scoped)
            return original_settle(**kwargs)

        with DatabaseUnitOfWork.connection_scope():
            self.assertIsNone(self.owner.when_unscoped())
            with patch.object(self.repository.boss_settlement, "settle_cooperative", side_effect=settle):
                ok, text = self.repository._fight_boss(user_id="u", operation_id="scoped-action")
            self.assertTrue(ok, text)
            self.assertEqual(1, len(scoped_readers))
            self.assertTrue(scoped_readers[0].closed)
            scoped_writer = self.connections[-1]
            self.assertFalse(scoped_writer.closed)
            self.assertFalse(scoped_writer.in_transaction)
        self.assertTrue(scoped_writer.closed)
        self.assertFalse(retained.closed)
        self.assertEqual(1, retained.statements.count("BEGIN"))
        self.assertTrue(self.repository._fight_boss(user_id="u", operation_id="after-scope")[0])
        self.assertEqual((100, 700, 3, None), self.receipt("after-scope"))
        self.assert_idle_reader(retained, 2)

    def execute(self, operation_id, *, item=False, **kwargs):
        payload = {
            "action": "activity_boss.use_item_on_boss" if item else "activity_boss.fight_cooperative_boss",
        }
        if item:
            payload["query"] = "Boss Flare"
        return self.application.execute(
            operation_id=operation_id, user_id="u", payload=payload, **kwargs,
        )

    def test_application_reuses_two_idle_handles_and_three_visible_commits_per_attack(self):
        settle = self.repository.boss_settlement.settle_cooperative
        boundaries = []

        def observe(**kwargs):
            operation_id = kwargs["operation_id"]
            writer = self.writer._idle[1]
            self.assertFalse(writer.in_transaction)
            self.assertFalse(self.owner._idle[1].in_transaction)
            self.assertEqual(("started",), self.row(
                "SELECT status FROM operation_ledger WHERE operation_id=?", (operation_id,),
            ))
            self.assertIsNone(self.receipt(operation_id))
            result = settle(**kwargs)
            self.assertFalse(writer.in_transaction)
            self.assertEqual(("started",), self.row(
                "SELECT status FROM operation_ledger WHERE operation_id=?", (operation_id,),
            ))
            self.assertIsNotNone(self.receipt(operation_id))
            boundaries.append(writer)
            return result

        with patch.object(self.repository.boss_settlement, "settle_cooperative", side_effect=observe):
            first = self.execute("app-first")
            self.assertTrue(first.ok)
            self.assertEqual((100, 900, 1, None), self.receipt("app-first"))
            self.write_external(
                ("UPDATE user_xiuxian SET atk=150,user_name='Changed' WHERE user_id='u'", ()),
                ("UPDATE activity_boss_state SET hp_left=600 WHERE activity_key='boss'", ()),
            )
            second = self.execute("app-second")
        self.assertTrue(second.ok)
        self.assertIn("Changed", second.data["message"])
        self.assertEqual((150, 450, 2, None), self.receipt("app-second"))
        self.assertEqual(2, len(self.connections))
        writer, reader = self.connections
        self.assertEqual([writer, writer], boundaries)
        self.assertEqual(6, writer.commits)
        self.assertEqual(6, writer.statements.count("BEGIN IMMEDIATE"))
        self.assertEqual(2, reader.commits)
        self.assertEqual(2, reader.statements.count("BEGIN"))
        self.assertEqual(1, writer.statements.count("PRAGMA journal_mode"))
        self.assertEqual(1, reader.statements.count("PRAGMA query_only=ON"))
        self.assertTrue(all(not c.closed and not c.in_transaction for c in self.connections))
        replay = self.execute("app-first")
        self.assertTrue(replay.replayed)
        self.assertEqual(first.data, replay.data)
        self.assertEqual(7, writer.commits)
        self.assertEqual(2, reader.commits)
        self.assertEqual(2, len(self.connections))
        self.assertEqual((450,), self.row("SELECT hp_left FROM activity_boss_state"))
        self.assertEqual((2,), self.row("SELECT COUNT(*) FROM activity_boss_fight_log"))
        self.assertEqual((2,), self.row("SELECT COUNT(*) FROM operation_ledger WHERE status='applied'"))

    def test_application_item_live_inventory_rejection_and_replay_preserve_ledger_identity(self):
        identity = {"item": "flare", "message": "original"}
        first = self.execute("app-item", item=True, ledger_payload=identity)
        self.assertTrue(first.ok)
        self.assertEqual((25, 975, 1, 1), self.receipt("app-item"))
        self.write_external(
            ("UPDATE activity_item_inventory SET count=0 WHERE user_id='u'", ()),
            ("UPDATE activity_boss_state SET hp_left=700 WHERE activity_key='boss'", ()),
        )
        rejected = self.execute("app-empty", item=True)
        self.assertFalse(rejected.ok)
        self.assertIn("\u4e0d\u8db3", rejected.data["message"])
        self.assertIsNone(self.receipt("app-empty"))
        self.assertEqual((700,), self.row("SELECT hp_left FROM activity_boss_state"))
        self.write_external(("UPDATE activity_item_inventory SET count=3 WHERE user_id='u'", ()))
        self.assertTrue(self.execute("app-refill", item=True).ok)
        self.assertEqual((25, 675, 2, 2), self.receipt("app-refill"))
        replay = self.execute("app-item", item=True, ledger_payload=identity)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.data, replay.data)
        with self.assertRaises(OperationConflictError):
            self.execute("app-item", item=True, ledger_payload={**identity, "message": "changed"})
        self.assertEqual((2,), self.row("SELECT count FROM activity_item_inventory"))
        self.assertEqual((2,), self.row("SELECT COUNT(*) FROM activity_boss_fight_log"))
        self.assertEqual((675,), self.row("SELECT hp_left FROM activity_boss_state"))

    def test_application_settlement_error_rolls_back_and_records_failure_before_recovery(self):
        self.write_external((
            "CREATE TRIGGER deny_damage BEFORE INSERT ON activity_boss_damage "
            "BEGIN SELECT RAISE(ABORT,'injected damage failure'); END", (),
        ))
        with self.assertRaisesRegex(sqlite3.DatabaseError, "injected damage failure"):
            self.execute("app-error", item=True)
        failed_writer, reader = self.connections[:2]
        self.assertTrue(failed_writer.closed)
        self.assertEqual(1, failed_writer.rollbacks)
        self.assertFalse(self.writer._active)
        self.assertFalse(reader.closed)
        self.assertFalse(reader.in_transaction)
        self.assertIsNone(self.receipt("app-error"))
        self.assertEqual((1000,), self.row("SELECT hp_left FROM activity_boss_state"))
        self.assertEqual((2,), self.row("SELECT count FROM activity_item_inventory"))
        self.assertEqual((0,), self.row("SELECT COUNT(*) FROM activity_boss_fight_log"))
        self.assertEqual(("failed",), self.row(
            "SELECT status FROM operation_ledger WHERE operation_id='app-error'",
        ))
        self.write_external(("DROP TRIGGER deny_damage", ()))
        self.assertTrue(self.execute("app-error", item=True).ok)
        self.assertEqual((25, 975, 1, 1), self.receipt("app-error"))
        self.assertIs(reader, self.owner._idle[1])
        self.assertIsNot(failed_writer, self.writer._idle[1])
        self.assertFalse(self.writer._idle[1].in_transaction)

    def test_application_read_error_does_not_settle_and_next_request_sees_external_updates(self):
        self.assertTrue(self.execute("app-warm").ok)
        writer, reader = self.connections
        reader.set_authorizer(lambda action, table, *_: sqlite3.SQLITE_DENY
                              if action == sqlite3.SQLITE_READ and table == "user_xiuxian"
                              else sqlite3.SQLITE_OK)
        with self.assertRaises(sqlite3.DatabaseError):
            self.execute("app-read-error")
        self.assertTrue(reader.closed)
        self.assertTrue(writer.closed)
        self.assertIsNone(self.receipt("app-read-error"))
        self.assertEqual((900,), self.row("SELECT hp_left FROM activity_boss_state"))
        self.write_external(("UPDATE user_xiuxian SET atk=175 WHERE user_id='u'", ()))
        self.assertTrue(self.execute("app-read-recovery").ok)
        self.assertEqual((175, 725, 2, None), self.receipt("app-read-recovery"))
        self.assertFalse(self.owner._idle[1].in_transaction)
        self.assertFalse(self.writer._idle[1].in_transaction)

    def test_application_explicit_scope_preserves_original_lifetime_and_default_reader_key(self):
        self.assertTrue(self.execute("app-unscoped").ok)
        retained_writer, retained_reader = self.connections
        with DatabaseUnitOfWork.connection_scope():
            self.assertTrue(self.execute("app-scoped").ok)
            self.assertIsNone(self.writer.when_unscoped())
            self.assertIsNone(self.owner.when_unscoped())
            scoped = self.connections[2:]
            self.assertEqual(3, len(scoped))
            self.assertTrue(all(c.closed for c in scoped[:-1]))
            self.assertFalse(scoped[-1].closed)
            self.assertTrue(all("PRAGMA query_only=ON" not in c.statements for c in scoped))
        self.assertTrue(all(c.closed for c in scoped))
        self.assertFalse(retained_writer.closed)
        self.assertFalse(retained_reader.closed)
        self.assertEqual(3, retained_writer.commits)
        self.assertEqual(1, retained_reader.commits)
        self.assertTrue(self.execute("app-after-scope").ok)
        self.assertEqual(5, len(self.connections))
        self.assertEqual(6, retained_writer.commits)
        self.assertEqual(2, retained_reader.commits)

    def test_application_other_actions_remain_unscoped_and_operation_alias_is_supported(self):
        self.assertTrue(self.execute("app-warm").ok)
        retained = tuple(self.connections)
        other = self.application.execute(operation_id="app-unsupported", user_id="u")
        self.assertFalse(other.ok)
        self.assertEqual(4, len(self.connections))
        self.assertTrue(all(c.closed for c in self.connections[2:]))
        self.assertTrue(all(not c.closed for c in retained))
        outcome = self.application.execute(
            operation_id="app-operation-alias", user_id="u",
            payload={"operation": "activity_boss.fight_cooperative_boss"},
        )
        self.assertTrue(outcome.ok)
        self.assertEqual(4, len(self.connections))
        self.assertEqual((100, 800, 2, None), self.receipt("app-operation-alias"))

    def test_application_finish_error_recovers_via_receipt_without_repeating_settlement(self):
        self.write_external((
            "CREATE TRIGGER deny_finish BEFORE UPDATE ON operation_ledger "
            "WHEN NEW.status='applied' BEGIN SELECT RAISE(ABORT,'injected finish failure'); END", (),
        ))
        with self.assertRaisesRegex(sqlite3.DatabaseError, "injected finish failure"):
            self.execute("app-finish-error", item=True)
        self.assertEqual((25, 975, 1, 1), self.receipt("app-finish-error"))
        self.assertEqual((975,), self.row("SELECT hp_left FROM activity_boss_state"))
        self.assertEqual((1,), self.row("SELECT count FROM activity_item_inventory"))
        self.assertEqual(("failed",), self.row(
            "SELECT status FROM operation_ledger WHERE operation_id='app-finish-error'",
        ))
        self.assertTrue(self.connections[0].closed)
        self.assertFalse(self.writer._active)
        self.write_external(("DROP TRIGGER deny_finish", ()))
        recovered = self.execute("app-finish-error", item=True)
        self.assertTrue(recovered.ok)
        self.assertFalse(recovered.replayed)
        self.assertEqual((1,), self.row("SELECT COUNT(*) FROM activity_boss_fight_log"))
        self.assertEqual((1,), self.row("SELECT count FROM activity_item_inventory"))
        self.assertEqual(("applied",), self.row(
            "SELECT status FROM operation_ledger WHERE operation_id='app-finish-error'",
        ))
        replay = self.execute("app-finish-error", item=True)
        self.assertTrue(replay.replayed)
        self.assertEqual(recovered.data, replay.data)
        self.assertTrue(self.execute("app-after-finish-error", item=True).ok)
        self.assertEqual((25, 950, 2, 0), self.receipt("app-after-finish-error"))


if __name__ == "__main__":
    unittest.main()
