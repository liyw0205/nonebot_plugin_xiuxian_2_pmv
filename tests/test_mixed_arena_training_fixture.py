from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
import ast
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param
from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state
from nonebot_plugin_xiuxian_2.features.arena.state_application import ArenaStateApplication
from nonebot_plugin_xiuxian_2.features.training.migrations import apply_training_state
from tests import mixed_arena_training_fixture as fixture
from tests.test_mixed_player_projection_fixture import manifest as base_manifest


def manifest():
    saved = base_manifest()
    arena = {"score": 1000, "rank": "青铜", "honor_points": 50,
             "total_honor_earned": 70, "position": 0, "base": 100,
             "bonus": 0, "total": 100, "cap": 12, "remaining": 5, "buys": 2}
    training = {"top": [{"user_id": str(index), "name": f"Name{index}", "value": 100 - index}
                         for index in range(50)],
                "actors": {actor: {"value": 20, "total": 8190, "rank": 100}
                           for actor in saved["actors"]}}
    saved["arena_training"] = {"oracle": {
        "arena": {actor: dict(arena) for actor in saved["actors"]},
        "training": {field: deepcopy(training) for field in ("completed", "points")}}}
    return saved


def reply(request):
    expected, key = request["expected_arena_training"], request["key"]
    if key.startswith("training_"):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to
        label, unit = (("历练完成次数", "次") if expected["field"] == "completed" else ("历练积分", "点"))
        return ("\n".join(f"第{index}位 | {row['name']} | {number_to(row['value'])}"
                          for index, row in enumerate(expected["top"], 1))
                + f"\n公共榜单快照截至 2026-11-27\n你的名次：第{expected['rank']}名 / 共{expected['total']}人"
                  f"（{label} {number_to(expected['value'])}{unit}）")
    if key == "arena_info":
        return (f"**竞技场信息**\n当前积分\n> {expected['score']}\n当前段位\n> {expected['rank']}\n"
                f"今日挑战\n> {expected['remaining']}/{expected['cap']}次\n今日购买\n> {expected['buys']}/3次\n")
    return (f"**我的荣誉**\n当前段位\n> {expected['rank']}\n当前排名\n> 第{expected['position']}名\n"
            f"当前荣誉值\n> {expected['honor_points']}点\n累计获得荣誉值\n> {expected['total_honor_earned']}点\n"
            f"基础奖励\n> {expected['base']}点\n排名奖励\n> {expected['bonus']}点\n总计\n> {expected['total']}点\n"
            "【最近荣誉结算】")


class ArenaTrainingPlanTests(unittest.TestCase):
    def test_frozen_phases_keep_live_writer_profiles_and_replace_only_relation_slots(self):
        saved, numbers = manifest(), set()
        for phase, spec in enumerate(fixture.phase_specs()):
            base = fixture.player.request_plan(saved, spec["mode"], 128, phase)
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            self.assertEqual(128, len(plan))
            current = {row["number"] for row in plan}
            self.assertEqual(128, len(current))
            self.assertFalse(numbers & current)
            numbers |= current
            counts = Counter(row["key"] for row in plan)
            self.assertEqual(56, counts["profile_info"])
            self.assertEqual(0 if phase == 0 else 32, counts["stone_training"])
            for key, *_ in fixture.COMMANDS:
                self.assertEqual(18 if phase == 0 else 10, counts[key])
            targets = [row for row in plan if "expected_arena_training" in row]
            self.assertFalse({row["actor"] for row in targets}
                             & set(saved["groups"]["preserved_background"]))
            for old, new in zip(base, plan):
                if old["key"] in {"profile_info", "stone_training"}:
                    self.assertEqual(old, new)
            self.assertGreater(len({row["actor"] for row in plan}), 56)

    def test_target_command_names_are_real_primary_matchers(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        names = []
        for feature in ("xiuxian_arena", "xiuxian_training"):
            tree = ast.parse((root / feature / "__init__.py").read_text(encoding="utf-8"))
            names += [node.args[0].value for node in ast.walk(tree)
                      if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                      and node.func.id == "on_command" and node.args
                      and isinstance(node.args[0], ast.Constant)]
        for _, command, _ in fixture.COMMANDS:
            self.assertEqual(1, names.count(command), command)

    def test_invalid_phase_count_is_rejected_before_dispatch(self):
        for mode, count, phase in (("pure", 160, 0), ("mixed", 128, 0), ("pure", 128, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)

    def test_exact_reply_oracle_rejects_wrong_values_missing_rows_and_normalizes_newlines(self):
        plan = fixture.request_plan(manifest(), "pure", 128, 0)
        for request in [row for row in plan if "expected_arena_training" in row][:4]:
            text = reply(request)
            for newline in ("\n", "\r", "\r\n"):
                self.assertEqual("success", fixture.classify_result(request, {
                    "replies": [text.replace("\n", newline)]})["outcome"])
            damaged = deepcopy(request)
            expected = damaged["expected_arena_training"]
            field = ("value" if request["key"].startswith("training_") else
                     "score" if request["key"] == "arena_info" else "position")
            expected[field] += 1
            self.assertEqual("unclassified", fixture.classify_result(damaged, {"replies": [text]})["outcome"])
            self.assertEqual("exception", fixture.classify_result(request, {
                "errors": ["injected"], "replies": [text]})["outcome"])
            self.assertEqual("boundary", fixture.classify_result(request, {
                "blocked": ["outside"], "replies": [text]})["outcome"])
        request = next(row for row in plan if row["key"] == "training_rank")
        text = reply(request)
        self.assertEqual("unclassified", fixture.classify_result(request, {
            "replies": [text.replace("Name3", "WrongName")]})["outcome"])
        lines = text.splitlines()
        lines[0], lines[1] = lines[1], lines[0]
        self.assertEqual("unclassified", fixture.classify_result(request, {
            "replies": ["\n".join(lines)]})["outcome"])


class ArenaTrainingPopulationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.paths = SimpleNamespace(game_db=root / "game.db", player_db=root / "player.db")
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT)")
            uow.executemany("INSERT INTO user_xiuxian VALUES(?,?)",
                            ((str(index), f"User{index}") for index in range(1, 1005)))
            uow.execute("CREATE TABLE arena_season_reward_operations(operation_id TEXT)")
        with fixture.shared._uow(self.paths.player_db) as uow:
            apply_arena_state(uow)
            apply_training_state(uow)
            uow.executemany("INSERT INTO arena(user_id,score,rank,honor_points,total_honor_earned,"
                            "daily_extra_challenges,daily_challenge_buys,daily_challenges_used) "
                            "VALUES(?,?,'青铜',50,70,2,2,7)",
                            ((str(index), 5000 - index // 2) for index in range(1, 1005)))
            uow.executemany("INSERT INTO training(user_id,completed,points) VALUES(?,?,?)",
                            ((str(index), 1004 - index, bind_sqlite_param(10**40 + index, exact=True))
                             for index in range(1, 1005)))

    def test_oracle_keeps_ordinal_cutoff_ties_and_exact_huge_training_values(self):
        with patch.object(fixture, "POPULATION", 1004):
            oracle = fixture._oracle(self.paths, {"1", "2", "3", "1000", "1001", "1004"})
        self.assertEqual([1, 2, 3, 1000, 0, 0],
                         [oracle["arena"][str(index)]["position"] for index in (1, 2, 3, 1000, 1001, 1004)])
        self.assertEqual([500, 300, 300, 0, 0, 0],
                         [oracle["arena"][str(index)]["bonus"] for index in (1, 2, 3, 1000, 1001, 1004)])
        self.assertEqual(10**40 + 1004, oracle["training"]["points"]["actors"]["1004"]["value"])
        self.assertEqual(1, oracle["training"]["points"]["actors"]["1004"]["rank"])
        self.assertEqual(0, oracle["training"]["completed"]["actors"]["1004"]["rank"])
        self.assertEqual(1003, oracle["training"]["completed"]["actors"]["1004"]["total"])

    def test_full_snapshot_detects_unrequested_background_and_operation_writes(self):
        before = fixture._snapshot(self.paths)
        with fixture.shared._uow(self.paths.player_db) as uow:
            uow.execute("UPDATE training SET points=1 WHERE user_id='1004'")
        self.assertNotEqual(before, fixture._snapshot(self.paths))
        before = fixture._snapshot(self.paths)
        with fixture.shared._uow(self.paths.player_db) as uow:
            uow.execute("INSERT INTO arena_state_operations(operation_id,user_id,kind,period_key,snapshot) "
                        "VALUES('unexpected','1004','initialize','2026-11-27','{}')")
        self.assertNotEqual(before, fixture._snapshot(self.paths))

    def test_seed_uses_existing_owners_and_builds_normalized_nonempty_feature_rows(self):
        saved = base_manifest()
        saved["feature_table_rows"] = {}
        saved["limits"] = []
        actors = saved["actors"]
        with fixture.shared._uow(self.paths.player_db) as uow:
            uow.execute("DELETE FROM arena")
            uow.execute("DELETE FROM training")
            uow.execute("CREATE TABLE avatar(user_id TEXT PRIMARY KEY)")
            uow.executemany("INSERT INTO avatar VALUES(?)", ((actor,) for actor in actors))
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.executemany("INSERT OR IGNORE INTO user_xiuxian VALUES(?,?)",
                            ((actor, f"User{actor}") for actor in actors))
        with patch.object(fixture, "POPULATION", 128), \
                patch.object(fixture.player, "seed", return_value=saved), \
                patch.object(fixture.player.strata, "_feature_snapshot", return_value={"refreshed": True}), \
                patch.object(fixture, "SystemClock", return_value=SimpleNamespace(
                    now=lambda: datetime(2026, 11, 26, 23, 59, tzinfo=timezone.utc))):
            seeded = fixture.seed(SimpleNamespace(paths=self.paths), ())
        self.assertEqual("mixed-arena-training-v18", seeded["fixture_version"])
        self.assertEqual(128, seeded["arena_training"]["snapshot"]["arena"]["rows"])
        self.assertEqual(128, seeded["arena_training"]["snapshot"]["training"]["rows"])
        self.assertEqual(0, seeded["arena_training"]["snapshot"]["arena_state_operations"]["rows"])
        self.assertEqual("2026-11-26", seeded["arena_training"]["date"])
        clock = SimpleNamespace(now=lambda: datetime(2026, 11, 26, 23, 59, tzinfo=timezone.utc))
        before = fixture._snapshot(self.paths)
        state = ArenaStateApplication(self.paths.player_db, clock=clock).get(actors[11])
        self.assertEqual(2, state["daily_extra_challenges"])
        self.assertEqual(before, fixture._snapshot(self.paths))
        with fixture.shared._uow(self.paths.player_db, read_only=True) as uow:
            self.assertEqual({seeded["arena_training"]["date"]},
                             {row["last_buy_date"] for row in uow.query_all("SELECT last_buy_date FROM arena")})
        self.assertEqual({"refreshed": True}, seeded["feature_snapshot"])
