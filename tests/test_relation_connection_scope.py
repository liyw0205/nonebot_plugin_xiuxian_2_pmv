"""Live relation handlers reuse readers without holding transactions over replies."""
from contextlib import ExitStack
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from nonebot_plugin_xiuxian_2.features.info import profile_repository
from nonebot_plugin_xiuxian_2.features.info.profile_repository import PlayerProfileSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import uow as database
from nonebot_plugin_xiuxian_2.infrastructure.database.uow import _CONNECTION_SCOPE
from tests.test_relationship_display_reads import modules
from tests.test_uow_connection_scope import ObservedConnection, REAL_CONNECT


class RelationConnectionScopeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.database = Path(temporary.name) / "game.db"
        connection = REAL_CONNECT(self.database)
        try:
            connection.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT,"
                               "user_name TEXT,level TEXT,exp,stone)")
            connection.executemany("INSERT INTO user_xiuxian VALUES(?,?,?,?,?,?)", [
                (1, "m", "Self", "R1", 100, 200), (2, "p", "Partner", "R2", b"1208925819614629174706177", 300),
            ])
            connection.commit()
        finally:
            connection.close()
        self.repository = PlayerProfileSqlRepository(self.database)
        self.connections = []
        self.partner, _, _ = modules()
        self.owner = database.ReadOnlyConnectionOwner()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.addCleanup(self.owner.close)
        for module in (database, profile_repository):
            self.stack.enter_context(patch.object(module, "LIVE_PROJECTION_READ_OWNER", self.owner))

    def connect(self, *args, **kwargs):
        connection = REAL_CONNECT(*args, **kwargs, factory=ObservedConnection)
        self.connections.append(connection)
        return connection

    def external_name(self, name):
        connection = REAL_CONNECT(self.database)
        try:
            connection.execute("UPDATE user_xiuxian SET user_name=? WHERE user_id='p'", (name,))
            connection.commit()
        finally:
            connection.close()

    def assert_no_active_transaction(self):
        self.assertFalse(self.owner._active)
        self.assertTrue(all(not connection.in_transaction for connection in self.connections
                            if not connection.closed))

    def boundaries(self, stack, *, registered=True, expected_scope=None):
        from nonebot.exception import FinishedException
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        async def assign(**kwargs):
            self.assertIs(_CONNECTION_SCOPE.get(), expected_scope)
            self.assert_no_active_transaction()
            return kwargs["bot"], "group"

        async def send(*args, **kwargs):
            self.assertIs(_CONNECTION_SCOPE.get(), expected_scope, "handler must preserve the caller's scope")
            self.assert_no_active_transaction()
            return None

        async def finish():
            self.assertIs(_CONNECTION_SCOPE.get(), expected_scope)
            self.assert_no_active_transaction()
            raise FinishedException

        def check(event):
            self.assertIs(_CONNECTION_SCOPE.get(), expected_scope)
            own = self.repository.get_user_profile("m", raise_errors=True)
            return registered, own, "unregistered"

        stack.enter_context(patch("sqlite3.connect", side_effect=self.connect))
        stack.enter_context(patch.object(self.partner, "assign_bot", side_effect=assign))
        stack.enter_context(patch.object(self.partner, "check_user", side_effect=check))
        sender = stack.enter_context(patch.object(self.partner, "handle_send", new=AsyncMock(side_effect=send)))
        failure = stack.enter_context(patch.object(utils, "handle_send", new=AsyncMock(side_effect=send)))
        for matcher in (self.partner.my_partner, self.partner.my_mentor):
            stack.enter_context(patch.object(matcher, "finish", side_effect=finish))
        stack.enter_context(patch.object(self.partner, "get_user_profile", side_effect=self.repository.get_user_profile))
        stack.enter_context(patch.object(self.partner, "get_user_display_profiles_by_ids",
                                        side_effect=self.repository.get_user_display_profiles_by_ids))
        manager = Mock()

        def statistics(*args):
            self.assertIs(_CONNECTION_SCOPE.get(), expected_scope)
            self.assert_no_active_transaction()
            return {}

        def remaining(actor):
            self.assertIs(_CONNECTION_SCOPE.get(), expected_scope)
            self.assert_no_active_transaction()
            return 1

        def cooldown(*args, **kwargs):
            self.assertIs(_CONNECTION_SCOPE.get(), expected_scope)
            self.assert_no_active_transaction()
            return 0

        manager.get_fields.side_effect = statistics
        stack.enter_context(patch.object(self.partner, "_player_data_manager", return_value=manager))
        stack.enter_context(patch.object(self.partner, "_get_remaining_mentor_transmission", side_effect=remaining))
        stack.enter_context(patch.object(self.partner, "_cooldown_remaining", side_effect=cooldown))

        def pending(actor):
            self.assertIs(_CONNECTION_SCOPE.get(), expected_scope)
            self.assert_no_active_transaction()
            return 2

        stack.enter_context(patch.object(self.partner, "_count_pending_mentor_invites", side_effect=pending))
        return sender, failure

    def assert_idle(self, *, expected=1):
        self.assertEqual(expected, len(self.connections))
        self.assertFalse(self.connections[-1].closed)
        self.assert_no_active_transaction()
        self.assertIsNone(_CONNECTION_SCOPE.get())

    async def test_both_views_reuse_warm_reader_and_see_between_transaction_commit(self):
        from nonebot.exception import FinishedException
        own = {"mentor_id": "p", "apprentice_ids": ["p", "p"], "mentor_protect": "off"}
        for command in (None, "我的师徒", "我的徒弟", "我的师父", "我的师傅"):
            with self.subTest(command=command), ExitStack() as stack:
                sender, failure = self.boundaries(stack)
                name = "Fresh" + str(command)

                def relation(actor):
                    self.assertIsNone(_CONNECTION_SCOPE.get())
                    self.assert_no_active_transaction()
                    self.external_name(name)
                    return own if command else {"partner_id": "p", "bind_time": None, "affection": 10}

                stack.enter_context(patch.object(self.partner, "load_partner", side_effect=relation))
                stack.enter_context(patch.object(self.partner, "load_mentor", side_effect=relation))
                stack.enter_context(patch.object(self.partner, "get_display_apprentice_ids", return_value=["p", "p"]))
                with self.assertRaises(FinishedException):
                    if command:
                        await self.partner.my_mentor_("bot", "event", (command,))
                    else:
                        await self.partner.my_partner_("bot", "event")
                self.assertIn(name, sender.call_args.args[2])
                failure.assert_not_called()
                self.assert_idle()
                connection = self.connections[0]
                requests = (None, "我的师徒", "我的徒弟", "我的师父", "我的师傅").index(command) + 1
                self.assertEqual(2 * requests, connection.statements.count("BEGIN"))
                self.assertEqual(2 * requests, connection.commits)
                self.assertEqual(1, connection.statements.count("PRAGMA busy_timeout=30000"))
                self.assertEqual(1, connection.statements.count("PRAGMA foreign_keys=ON"))
                self.assertEqual(1, connection.statements.count("PRAGMA query_only=ON"))

    async def test_missing_registration_relation_or_profile_finishes_transaction_before_reply(self):
        from nonebot.exception import FinishedException
        for registered, data in ((False, {}), (True, {}), (True, {"partner_id": "missing"})):
            with self.subTest(registered=registered, data=data), ExitStack() as stack:
                sender, failure = self.boundaries(stack, registered=registered)
                reader = stack.enter_context(patch.object(self.partner, "load_partner", return_value=data))
                with self.assertRaises(FinishedException):
                    await self.partner.my_partner_("bot", "event")
                self.assert_idle()
                sender.assert_awaited_once()
                failure.assert_not_called()
                self.assertEqual(int(registered), reader.call_count)
        with ExitStack() as stack:
            sender, failure = self.boundaries(stack, registered=False)
            reader = stack.enter_context(patch.object(self.partner, "load_mentor", side_effect=AssertionError("invalid actor")))
            with self.assertRaises(FinishedException):
                await self.partner.my_mentor_("bot", "event", ("我的师徒",))
            reader.assert_not_called()
            sender.assert_awaited_once()
            failure.assert_not_called()
            self.assert_idle()

    async def test_real_related_read_denial_discards_retained_reader_and_next_request_recovers(self):
        from nonebot.exception import FinishedException
        for mentor in (False, True):
            with self.subTest(mentor=mentor), ExitStack() as stack:
                self.owner.close()
                self.connections.clear()
                sender, failure = self.boundaries(stack)
                denied = True

                def relation(actor):
                    self.assert_no_active_transaction()
                    if denied:
                        self.connections[-1].set_authorizer(
                            lambda action, table, *_: sqlite3.SQLITE_DENY
                            if action == sqlite3.SQLITE_READ and table == "user_xiuxian" else sqlite3.SQLITE_OK
                        )
                    return ({"mentor_id": "p", "apprentice_ids": []} if mentor else
                            {"partner_id": "p", "bind_time": None, "affection": 10})

                stack.enter_context(patch.object(self.partner, "load_mentor", side_effect=relation))
                stack.enter_context(patch.object(self.partner, "load_partner", side_effect=relation))
                stack.enter_context(patch.object(self.partner, "get_display_apprentice_ids", return_value=[]))

                async def run():
                    if mentor:
                        await self.partner.my_mentor_("bot", "event", ("我的师徒",))
                    else:
                        await self.partner.my_partner_("bot", "event")

                await run()
                failure.assert_awaited_once()
                self.assertIn("玩家档案读取失败", failure.call_args.args[2])
                sender.assert_not_called()
                self.assertEqual(1, len(self.connections))
                failed = self.connections[0]
                self.assertTrue(failed.closed)
                self.assertIsNone(self.owner._idle)
                self.assertIsNone(_CONNECTION_SCOPE.get())
                denied = False
                failure.reset_mock()
                with self.assertRaises(FinishedException):
                    await run()
                sender.assert_awaited_once()
                failure.assert_not_called()
                self.assert_idle(expected=2)
                self.assertIsNot(failed, self.connections[-1])

    async def test_explicit_caller_scope_keeps_its_key_and_lifetime_for_both_views(self):
        from nonebot.exception import FinishedException
        for mentor in (False, True):
            with self.subTest(mentor=mentor), ExitStack() as stack:
                self.owner.close()
                self.connections.clear()
                stack.enter_context(patch("sqlite3.connect", side_effect=self.connect))
                self.repository.get_user_profile("m", raise_errors=True)
                retained = self.connections[0]
                with database.DatabaseUnitOfWork.connection_scope():
                    scope = _CONNECTION_SCOPE.get()
                    sender, failure = self.boundaries(stack, expected_scope=scope)

                    def relation(actor):
                        self.assertIs(scope, _CONNECTION_SCOPE.get())
                        self.assert_no_active_transaction()
                        self.external_name("ScopedFresh")
                        return ({"mentor_id": "p", "apprentice_ids": []} if mentor else
                                {"partner_id": "p", "bind_time": None, "affection": 10})

                    stack.enter_context(patch.object(self.partner, "load_mentor", side_effect=relation))
                    stack.enter_context(patch.object(self.partner, "load_partner", side_effect=relation))
                    stack.enter_context(patch.object(self.partner, "get_display_apprentice_ids", return_value=[]))
                    with self.assertRaises(FinishedException):
                        if mentor:
                            await self.partner.my_mentor_("bot", "event", ("我的师徒",))
                        else:
                            await self.partner.my_partner_("bot", "event")
                    self.assertIn("ScopedFresh", sender.call_args.args[2])
                    failure.assert_not_called()
                    self.assertEqual(2, len(self.connections))
                    scoped = self.connections[1]
                    self.assertFalse(scoped.closed)
                    self.assertNotIn("PRAGMA query_only=ON", scoped.statements)
                    self.assertEqual(2, scoped.statements.count("BEGIN"))
                    self.assertEqual(2, scoped.commits)
                    self.assertFalse(retained.closed)
                self.assertTrue(scoped.closed)
                self.assertFalse(retained.closed)
                self.assertIsNone(_CONNECTION_SCOPE.get())
