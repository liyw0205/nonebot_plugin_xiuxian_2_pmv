from collections import Counter
from copy import deepcopy
from datetime import datetime
from pathlib import Path
import ast
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_player_schema
from nonebot_plugin_xiuxian_2.infrastructure.database import message_store
from tests import mixed_rank_projection_fixture as fixture
from tests.test_mixed_arena_training_fixture import manifest as arena_manifest
from tests.test_mixed_arena_training_fixture import reply as training_reply


def manifest():
    saved = arena_manifest()
    identities = fixture._identities(datetime(2026, 11, 28))
    season = [{"rank_key": key, "mode": mode, "period": period, "rank_type": kind,
               "value": 10 ** 40 + index, "rank": 7 + index}
              for index, (key, mode, period, kind) in enumerate(identities)]
    boss = {"top": [{"user_id": str(index), "name": f"BossName{index}", "value": 1000 - index}
                    for index in range(50)],
            "actors": {actor: {"value": 200, "rank": 101, "total": 2046}
                       for actor in saved["actors"]}}
    saved["rank_projection"] = {"oracle": {
        "season": {actor: deepcopy(season) for actor in saved["actors"]}, "boss": boss},
        "snapshot": {}, "cache_observation": "phase counters only"}
    return saved


def reply(request):
    if request["key"].startswith("training_"):
        return training_reply(request)
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import number_to

    expected = request["expected_rank_projection"]
    if request["key"] == "my_season_rank":
        lines, last = ["【我的赛季】"], None
        labels = {"weekly": "周榜", "monthly": "月榜", "quarterly": "季度榜"}
        for row in expected:
            if row["mode"] != last:
                last = row["mode"]
                lines.append(f"\n【{row['period']}{labels[last]}】")
            lines.append(f"{row['rank_type']}：积分{number_to(row['value'])}，当前第{row['rank']}名")
        return "\n".join(lines)
    return ("【世界BOSS积分排行榜】\n" + "".join(
        f"第{index}位 | {row['name']} | {number_to(row['value'])}\n"
        for index, row in enumerate(expected["top"], 1))
        + f"你的名次：第{expected['rank']}名 / 共{expected['total']}人"
          f"（世界积分 {number_to(expected['value'])}点）\n公共榜单快照截至 2026-11-28")


class RankProjectionPlanTests(unittest.TestCase):
    def test_frozen_plan_preserves_all_control_requests_and_exact_target_counts(self):
        saved, numbers = manifest(), set()
        expected_counts = ((24, 24, 12, 12), (14, 13, 6, 7), (14, 13, 7, 6))
        for phase, spec in enumerate(fixture.phase_specs()):
            base = fixture.player.request_plan(saved, spec["mode"], 128, phase)
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            self.assertEqual(128, len(plan))
            self.assertEqual(128, len({row["number"] for row in plan}))
            self.assertFalse(numbers & {row["number"] for row in plan})
            numbers.update(row["number"] for row in plan)
            counts = Counter(row["key"] for row in plan)
            self.assertEqual(56, counts["profile_info"])
            self.assertEqual(0 if phase == 0 else 32, counts["stone_training"])
            self.assertEqual(expected_counts[phase], tuple(counts[key] for key, *_ in fixture.COMMANDS))
            for old, new in zip(base, plan):
                if old["key"] in {"profile_info", "stone_training"}:
                    self.assertEqual(old, new)
            targets = [row for row in plan if row["key"] in fixture.TARGET_KEYS]
            self.assertEqual(len(targets), len({row["actor"] for row in targets}))
            self.assertFalse(set(saved["groups"]["preserved_background"])
                             & {row["actor"] for row in targets})
            seasons = [row for row in targets if row["key"] == "my_season_rank"]
            self.assertTrue(all(len(row["expected_rank_projection"]) == 15 for row in seasons))
        self.assertEqual(384, len(numbers))

    def test_registered_primary_names_and_invalid_shapes_fail_before_dispatch(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        names = []
        for module in ("xiuxian_base", "xiuxian_boss", "xiuxian_training"):
            tree = ast.parse((root / module / "__init__.py").read_text(encoding="utf-8"))
            names += [node.args[0].value for node in ast.walk(tree)
                      if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                      and node.func.id == "on_command" and node.args
                      and isinstance(node.args[0], ast.Constant)]
        for _, command, _ in fixture.COMMANDS:
            self.assertEqual(1, names.count(command), command)
        for mode, count, phase in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)

    def test_all_exact_oracles_reject_changed_personal_values_missing_or_reordered_lines(self):
        plan = fixture.request_plan(manifest(), "pure", 128)
        for key, *_ in fixture.COMMANDS:
            request = next(row for row in plan if row["key"] == key)
            text = reply(request)
            for newline in ("\n", "\r", "\r\n"):
                self.assertEqual("success", fixture.classify_result(request, {
                    "replies": [text.replace("\n", newline)]})["outcome"])
            if key == "my_season_rank":
                damaged = text.replace("当前第7名", "当前第8名", 1)
            else:
                damaged = text.replace("你的名次：第", "你的名次：第9", 1)
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [damaged]})["outcome"])
            lines = text.splitlines()
            nonempty = [index for index, line in enumerate(lines) if line.strip()]
            first, second = nonempty[1:3]
            lines[first], lines[second] = lines[second], lines[first]
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": ["\n".join(lines)]})["outcome"])
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [text.replace(text.splitlines()[-1], "")]})["outcome"])
            for fields, outcome in (({"errors": ["failure"]}, "exception"),
                                    ({"blocked": ["outside"]}, "boundary"),
                                    ({"replies": ["慢...慢一..点"]}, "cooldown")):
                self.assertEqual(outcome, fixture.classify_result(request, {
                    "replies": [text], **fields})["outcome"])

    def test_season_rounding_does_not_make_equal_formatted_scores_false_duplicates(self):
        request = next(row for row in fixture.request_plan(manifest(), "pure", 128)
                       if row["key"] == "my_season_rank")
        for row in request["expected_rank_projection"]:
            row.update(value=10 ** 40, rank=7)
        self.assertEqual("success", fixture.classify_result(request, {"replies": [reply(request)]})["outcome"])

    def test_season_transport_mention_preserves_exact_reply_and_recipient_checks(self):
        request = next(row for row in fixture.request_plan(manifest(), "pure", 128)
                       if row["key"] == "my_season_rank")
        text = reply(request)
        for newline in ("\n", "\r", "\r\n"):
            rendered = f"[CQ:at,qq={request['actor']}]" + newline + text.replace("\n", newline)
            self.assertEqual("success", fixture.classify_result(request, {"replies": [rendered]})["outcome"])
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [rendered.replace("当前第7名", "当前第8名", 1)]})["outcome"])
        self.assertEqual("unclassified", fixture.classify_result(request, {
            "replies": ["[CQ:at,qq=123456789]\n" + text]})["outcome"])

    def test_invariants_filter_targets_from_inherited_controls_but_still_check_targets(self):
        saved = manifest()
        plan = fixture.request_plan(saved, "pure", 128)
        rows = [{"request": row, "outcome": "success", "replies": [reply(row)]}
                if row["key"] in fixture.TARGET_KEYS else {"request": row, "outcome": "success"}
                for row in plan]
        inherited = {"ok": True, "errors": [], "invariants": "inherited"}
        context = SimpleNamespace(paths=None)
        with patch.object(fixture.player, "check_invariants", return_value=inherited) as check, \
                patch.object(fixture, "_snapshot", return_value={}):
            result = fixture.check_invariants(context, (), saved, rows)
            self.assertTrue(result["ok"], result["errors"])
            self.assertEqual(56, len(check.call_args.args[-1]))
            self.assertEqual({"profile_info"}, {row["request"]["key"] for row in check.call_args.args[-1]})
            damaged = deepcopy(rows)
            next(row for row in damaged if row["request"]["key"] == "my_season_rank")["replies"] = ["wrong"]
            self.assertFalse(fixture.check_invariants(context, (), saved, damaged)["ok"])
            self.assertFalse(fixture.check_invariants(context, (), saved, rows[:-1])["ok"])


class RankProjectionPopulationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        self.paths = SimpleNamespace(game_db=root / "game.db", player_db=root / "player.db")
        self.owners = [str(1000 + index) for index in range(64)]
        self.actors = self.owners[:4]
        self.now = datetime(2026, 11, 28, 12)
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT)")
            uow.executemany("INSERT INTO user_xiuxian VALUES(?,?)",
                            ((owner, f"User{owner}") for owner in self.owners))
            message_store.apply_season_rank_schema(uow)
            uow.execute("CREATE TABLE arena_season_reward_operations(operation_id TEXT)")
        with fixture.shared._uow(self.paths.player_db) as uow:
            apply_boss_player_schema(uow)
            uow.execute("CREATE TABLE training(user_id TEXT,completed INTEGER,points INTEGER)")
            uow.execute("CREATE TABLE arena(user_id TEXT,score INTEGER)")
            uow.execute("CREATE TABLE arena_state_operations(operation_id TEXT)")
        for name, value in (("SEASON_POPULATION", 16), ("BOSS_POPULATION", 32)):
            active = patch.object(fixture, name, value)
            active.start()
            self.addCleanup(active.stop)

    def test_populated_seed_has_fifteen_complete_keys_ties_and_exact_nonreal_scores(self):
        seeded = fixture._seed_rank_tables(self.paths, self.actors, self.owners, self.now)
        oracle = fixture._oracle(self.paths, self.actors, seeded["identities"])
        self.assertEqual({"integer": 120, "blob": 120}, seeded["season_storage_classes"])
        self.assertEqual(15, len(oracle["season"][self.actors[0]]))
        self.assertEqual([7, 7, 5, 5], [oracle["season"][actor][0]["rank"] for actor in self.actors])
        self.assertEqual([7, 7, 5, 5], [oracle["boss"]["actors"][actor]["rank"] for actor in self.actors])
        self.assertEqual(10 ** 40, oracle["season"][self.actors[0]][0]["value"])
        self.assertEqual(32, len(oracle["boss"]["top"]))
        with fixture.shared._uow(self.paths.player_db, read_only=True) as uow:
            self.assertEqual({"integer", "blob"}, {row["kind"] for row in uow.query_all(
                "SELECT DISTINCT typeof(integral) AS kind FROM boss_limit")})
        before = fixture._snapshot(self.paths)
        fixture._seed_rank_tables(self.paths, self.actors, self.owners, self.now)
        self.assertEqual(before, fixture._snapshot(self.paths))

    def test_full_streamed_hash_detects_background_score_and_operation_writes(self):
        fixture._seed_rank_tables(self.paths, self.actors, self.owners, self.now)
        before = fixture._snapshot(self.paths)
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.execute("UPDATE season_rank SET score=1 WHERE user_id=?", (self.owners[15],))
        self.assertNotEqual(before, fixture._snapshot(self.paths))
        before = fixture._snapshot(self.paths)
        with fixture.shared._uow(self.paths.player_db) as uow:
            uow.execute("INSERT INTO arena_state_operations VALUES('unexpected')")
        self.assertNotEqual(before, fixture._snapshot(self.paths))
        before = fixture._snapshot(self.paths)
        with fixture.shared._uow(self.paths.player_db) as uow:
            uow.execute("UPDATE boss_limit SET integral=1 WHERE user_id=?", (self.owners[31],))
        self.assertNotEqual(before, fixture._snapshot(self.paths))

    def test_unknown_season_or_boss_owners_are_rejected_before_scores_are_seeded(self):
        with fixture.shared._uow(self.paths.player_db) as uow:
            uow.execute("INSERT INTO boss_limit(user_id,integral) VALUES('foreign',17)")
        before = fixture._snapshot(self.paths)
        with self.assertRaisesRegex(ValueError, "boss rows outside"):
            fixture._seed_rank_tables(self.paths, self.actors, self.owners, self.now)
        self.assertEqual(before, fixture._snapshot(self.paths))
        with fixture.shared._uow(self.paths.player_db) as uow:
            uow.execute("DELETE FROM boss_limit WHERE user_id='foreign'")
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.execute("INSERT INTO season_rank VALUES('foreign','weekly','period','x','foreign',0,17,'{}','a')")
        before = fixture._snapshot(self.paths)
        with self.assertRaisesRegex(ValueError, "season rows outside"):
            fixture._seed_rank_tables(self.paths, self.actors, self.owners, self.now)
        self.assertEqual(before, fixture._snapshot(self.paths))

    def test_boss_python_oracle_counts_only_first_identity_and_keeps_stable_huge_ties(self):
        rows = [{"row_id": index, "user_id": owner, "integral": value}
                for index, (owner, value) in enumerate((
                    ("a", b"10000000000000000000000000000000000000000"),
                    ("a", 1), ("b", 10 ** 40), ("c", 0), ("d", -1),
                    (None, 999), (" ", 999)), 1)]
        oracle = fixture._boss_oracle(rows, ["a", "b", "c", "missing"], {"a": "First"})
        self.assertEqual(["a", "b", "c", "d"], [row["user_id"] for row in oracle["top"]])
        self.assertEqual("First", oracle["top"][0]["name"])
        self.assertEqual({"value": 10 ** 40, "total": 2, "rank": 1}, oracle["actors"]["a"])
        self.assertEqual({"value": 0, "total": 2, "rank": 0}, oracle["actors"]["missing"])


if __name__ == "__main__":
    unittest.main()
