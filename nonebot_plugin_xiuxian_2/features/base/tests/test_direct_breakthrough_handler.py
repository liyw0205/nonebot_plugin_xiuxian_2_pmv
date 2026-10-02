from __future__ import annotations

import ast
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from ....infrastructure.database import DatabaseUnitOfWork, OutboxStore
from ....compatibility.base_breakthrough_effects import LegacyDirectBreakthroughEffects
from ...game_events.migrations import apply_game_event_statistics_player
from ..application import BaseApplication
from ..migrations import (
    apply_base_direct_breakthrough_operations, apply_base_direct_breakthrough_plans,
    apply_base_direct_breakthrough_player,
)


class _Finished(Exception):
    pass


class _Matcher:
    def handle(self, **kwargs):
        def register(handler):
            self.handler = handler
            return handler

        return register

    async def finish(self):
        raise _Finished()


class DirectBreakthroughHandlerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.database = Path(self.directory.name) / "game.db"
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,"
                "hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER,level_up_rate INTEGER,level_up_cd TIMESTAMP)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES('u','before',10000,5000,10000,1000,10000,5,NULL)")
            apply_base_direct_breakthrough_operations(uow)
            OutboxStore().ensure_schema(uow)
            apply_base_direct_breakthrough_plans(uow)
        self.player = Path(self.directory.name) / "player.db"
        with DatabaseUnitOfWork(self.player, immediate=True) as uow:
            apply_base_direct_breakthrough_player(uow)
            apply_game_event_statistics_player(uow)
        self.effects = LegacyDirectBreakthroughEffects(
            self.database, self.player, players_dir=Path(self.directory.name) / "players", refresh_titles=Mock(),
        )
        self.effects.plan_relations = Mock(return_value=[])
        self.profile = dict(
            user_id="u", level="before", exp=10000, hp=5000, mp=10000, level_up_rate=5,
            level_up_cd=None, root_type="root",
        )
        self.matcher = _Matcher()
        self.event = SimpleNamespace(message_id="message-1")
        self.resolve = Mock(return_value=["after"])
        self.statistics = Mock()
        self.relations = Mock(return_value="")
        self.send = AsyncMock()
        self.command = Mock(return_value=self.matcher)
        sql = SimpleNamespace(get_user_info_with_id=lambda _: dict(self.profile), get_root_rate=lambda *_: 1.5)
        self.namespace = dict(
            BaseApplication=BaseApplication,
            direct_breakthrough_root_rate=lambda _: 1.5,
            Bot=object, GroupMessageEvent=object, PrivateMessageEvent=object,
            _direct_breakthrough_application_instance=None,
            on_command=self.command, Cooldown=lambda **_: None,
            assign_bot=AsyncMock(return_value=(object(), None)),
            check_user=lambda _: (True, dict(self.profile), ""),
            _sql_message=lambda: sql, OtherSet=lambda: SimpleNamespace(get_type=self.resolve),
            XiuConfig=lambda: SimpleNamespace(
                level_punishment_floor=10, level_punishment_limit=10, level_up_probability=0.2,
                level=["before", "after"],
            ),
            convert_rank=lambda _: (0, ["before", "after"]),
            UserBuffDate=lambda _: SimpleNamespace(get_user_main_buff_data=lambda: None),
            jsondata=SimpleNamespace(
                level_rate_data=lambda: {"before": 10},
                level_data=lambda: {"after": {"spend": 2, "power": 10000}},
            ),
            random=SimpleNamespace(randint=Mock(return_value=10)), number_to=str,
            record_level_up_result=self.statistics,
            trigger_breakthrough_relation_rewards=self.relations, handle_send=self.send,
        )
        source = Path(__file__).parents[3] / "xiuxian/xiuxian_base/breakthrough_tribulation.py"
        tree = ast.parse(source.read_text(encoding="utf-8"))
        names = {
            "_breakthrough_operation_id", "configure_direct_breakthrough_application",
            "_direct_breakthrough_application", "level_up_zj_",
        }
        # Execute the source registration and handler without booting unrelated legacy plugins.
        selected = [
            node for node in tree.body
            if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names)
            or (isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == "level_up_zj" for target in node.targets
            ))
        ]
        exec(compile(ast.Module(body=selected, type_ignores=[]), str(source), "exec"), self.namespace)
        self.namespace["configure_direct_breakthrough_application"](
            BaseApplication(self.database, self.player, direct_breakthrough_effects=self.effects)
        )

    async def invoke(self):
        with self.assertRaises(_Finished):
            await self.matcher.handler(object(), self.event)

    def row(self):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            return uow.query_one("SELECT * FROM user_xiuxian WHERE user_id='u'")

    async def test_success_and_delayed_same_message_replay_do_not_repeat_effects(self):
        self.assertEqual(self.command.call_args.args, ("直接突破",))
        self.assertEqual(self.command.call_args.kwargs["aliases"], {"破"})
        await self.invoke()
        self.assertEqual(self.row()["level"], "after")
        self.assertEqual(self.row()["power"], 30000)
        await self.invoke()
        self.namespace["random"].randint.assert_called_once_with(0, 100)
        self.statistics.assert_not_called()
        self.relations.assert_not_called()
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(uow.query_one("SELECT * FROM statistics WHERE user_id='u'")["突破成功"], 1)

    async def test_failure_and_delayed_same_message_replay_do_not_repeat_penalty(self):
        self.namespace["random"].randint.side_effect = [100, 10]
        await self.invoke()
        await self.invoke()
        self.assertEqual(self.row()["exp"], 9000)
        self.assertEqual(self.namespace["random"].randint.call_count, 2)
        self.statistics.assert_not_called()
        self.relations.assert_not_called()
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(uow.query_one("SELECT * FROM statistics WHERE user_id='u'")["突破损失修为"], 1000)

    async def test_stale_snapshot_does_not_run_post_commit_effects(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("UPDATE user_xiuxian SET exp=9999")
        await self.invoke()
        self.assertEqual(self.row()["exp"], 9999)
        self.assertEqual(self.row()["level"], "before")
        self.statistics.assert_not_called()
        self.relations.assert_not_called()

    async def test_missing_schema_does_not_create_receipt_or_run_effects(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute("DROP TABLE direct_breakthrough_operations")
        await self.invoke()
        self.assertEqual(self.row()["level"], "before")
        self.statistics.assert_not_called()
        self.relations.assert_not_called()
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertIsNone(uow.query_one(
                "SELECT name FROM sqlite_master WHERE name='direct_breakthrough_operations'"
            ))
