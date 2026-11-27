"""Request-local arena display reuse keeps live state and legacy ordinal rules."""
from __future__ import annotations

import __future__
import ast
import asyncio
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state
from nonebot_plugin_xiuxian_2.features.arena.state_application import ArenaStateApplication
from nonebot_plugin_xiuxian_2.features.arena.state_repository import ArenaStateRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


ROOT = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_arena"


def _load(path, names, namespace):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.AsyncFunctionDef))
             and node.name in names]
    if {node.name for node in nodes} != set(names):
        raise AssertionError("expected arena implementation is missing")
    for node in nodes:
        node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


def limit(application):
    namespace = _load(ROOT / "arena_limit.py", {"ArenaLimit"}, {})
    return namespace["ArenaLimit"](application)


class Finished(Exception):
    pass


def run_handler(name, rules):
    async def finish():
        raise Finished

    sender = AsyncMock()
    history = Mock(return_value=())
    namespace = _load(ROOT / "__init__.py", {name}, {
        "__package__": "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_arena",
        "arena_limit": rules, "assign_bot": AsyncMock(return_value=(None, None)),
        "check_user": lambda event: (True, {"user_id": "u", "user_name": "Viewer"}, ""),
        "handle_send": sender, "number_to": str, "ARENA_CHALLENGE_BUY_COST": 2000000,
        "arena_myinfo": SimpleNamespace(finish=finish), "arena_honor": SimpleNamespace(finish=finish),
        "arena_season_reward_application": SimpleNamespace(recent_settlements=history),
    })
    try:
        asyncio.run(namespace[name](None, None))
    except Finished:
        pass
    else:
        raise AssertionError("handler did not finish")
    return sender.call_args.args[2], history


class ArenaDisplayRulesTests(unittest.TestCase):
    def test_pure_helpers_cover_bonus_boundaries_without_reading_or_mutating_state(self):
        app = Mock()
        rules = limit(app)
        state = {"rank": "黄金", "daily_extra_challenges": "3"}
        before = deepcopy(state)
        self.assertEqual(13, rules.daily_challenge_cap_from_state(state))
        expected = {0: 0, 1: 500, 2: 300, 3: 300, 4: 200, 10: 200,
                    11: 100, 50: 100, 51: 50, 100: 50, 101: 0, 1001: 0}
        for position, bonus in expected.items():
            with self.subTest(position=position):
                self.assertEqual((300 + bonus, 300, bonus), rules.daily_honor_from_state(state, position))
        self.assertEqual((100, 100, 0), rules.daily_honor_from_state({"rank": "unknown"}, 0))
        self.assertEqual(before, state)
        self.assertEqual([], app.mock_calls)

    def test_legacy_wrappers_still_read_fresh_state_and_ranking_every_call(self):
        app = Mock()
        app.get.side_effect = [{"daily_extra_challenges": 2}, {"daily_extra_challenges": 5},
                               {"rank": "黄金"}, {"rank": "王者"}]
        app.ranking.side_effect = [(("u", 1),), (("other", 20), ("u", 10))]
        rules = limit(app)
        self.assertEqual(12, rules.get_daily_challenge_cap("u"))
        self.assertEqual(15, rules.get_daily_challenge_cap("u"))
        self.assertEqual((800, 300, 500), rules.calculate_daily_honor("u"))
        self.assertEqual((1300, 1000, 300), rules.calculate_daily_honor("u"))
        self.assertEqual(4, app.get.call_count)
        self.assertEqual(2, app.ranking.call_count)
        app.ranking.assert_called_with(1000)

    def test_legacy_top1000_ordinal_preserves_ties_and_outside_zero(self):
        app = Mock()
        app.ranking.return_value = tuple((str(index), 10) for index in range(1, 1001))
        rules = limit(app)
        for actor, expected in ((1, 1), (2, 2), (1000, 1000), (1001, 0), ("absent", 0)):
            with self.subTest(actor=actor):
                self.assertEqual(expected, rules.get_user_ranking(actor))
        app.ranking.assert_called_with(1000)


class ArenaDisplayLiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.database = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_arena_state(uow)
        self.now = datetime(2021, 1, 3, 23, 59, tzinfo=timezone.utc)
        self.application = ArenaStateApplication(self.database, clock=SimpleNamespace(now=lambda: self.now))
        self.application.get("u")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("UPDATE arena SET score=1900,rank='黄金',honor_points=20,total_honor_earned=30,"
                        "daily_challenges_used=7,daily_extra_challenges=2,daily_challenge_buys=2,"
                        "weekly_purchases=? WHERE user_id='u'", ('{"_last_reset":"2021-01-03","1":2}',))
        self.rules = limit(self.application)

    def test_personal_handler_uses_one_live_state_and_keeps_daily_weekly_repair(self):
        with patch.object(self.application.repository, "initialize", wraps=self.application.repository.initialize) as reads:
            text, _ = run_handler("arena_myinfo_", self.rules)
            self.assertEqual(1, reads.call_count)
        self.assertIn("今日挑战\n> 5/12次", text)
        self.assertIn("今日购买\n> 2/3次", text)
        self.now = datetime(2021, 1, 4, 0, 1, tzinfo=timezone.utc)
        with patch.object(self.application.repository, "initialize", wraps=self.application.repository.initialize) as reads:
            text, _ = run_handler("arena_myinfo_", self.rules)
            self.assertEqual(1, reads.call_count)
        self.assertIn("今日挑战\n> 3/10次", text)
        self.assertIn("今日购买\n> 0/3次", text)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one("SELECT daily_challenges_used,weekly_purchases FROM arena WHERE user_id='u'")
            operations = uow.query_all("SELECT kind,period_key FROM arena_state_operations ORDER BY kind")
        self.assertEqual(7, row["daily_challenges_used"])
        self.assertEqual('{"_last_reset": "2021-01-04"}', row["weekly_purchases"])
        self.assertEqual([("day", "2021-01-04"), ("initialize", "2021-01-03"), ("week", "2021-W01")],
                         [(row["kind"], row["period_key"]) for row in operations])

    def test_honor_handler_reads_one_live_state_and_one_ordinal_for_preview_and_display(self):
        with patch.object(self.application.repository, "initialize", wraps=self.application.repository.initialize) as states, \
                patch.object(self.application.repository, "ranking", wraps=self.application.repository.ranking) as rankings:
            text, history = run_handler("arena_honor_", self.rules)
            self.assertEqual((1, 1), (states.call_count, rankings.call_count))
        self.assertIn("当前荣誉值\n> 20点", text)
        self.assertIn("当前排名\n> 第1名", text)
        self.assertIn("总计\n> 800点", text)
        self.assertIn("【最近荣誉结算】", text)
        history.assert_called_once_with("u")
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("INSERT INTO arena(user_id,score) VALUES('rival',2000)")
            uow.execute("UPDATE arena SET honor_points=75 WHERE user_id='u'")
        text, _ = run_handler("arena_honor_", self.rules)
        self.assertIn("当前荣誉值\n> 75点", text)
        self.assertIn("当前排名\n> 第2名", text)
        self.assertIn("总计\n> 600点", text)

    def test_ranking_is_read_only_and_missing_database_is_not_created(self):
        original_enter = DatabaseUnitOfWork.__enter__
        def readonly_enter(uow):
            result = original_enter(uow)
            self.assertTrue(uow.read_only)
            return result
        with patch.object(DatabaseUnitOfWork, "__enter__", readonly_enter):
            self.assertEqual((("u", 1900),), self.application.ranking(10))
        missing = self.database.parent / "missing.db"
        self.assertEqual((), ArenaStateRepository(missing).ranking(10))
        self.assertFalse(missing.exists())
