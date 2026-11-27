"""Static/small-fixture checks for V29; no timed load or million-row setup."""
from collections import Counter
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import ast
import json
import tempfile
import unittest
from unittest.mock import patch

import nonebot

from scripts import mixed_command_probe as probe
from tests import mixed_overview_stage_fixture as fixture


def manifest():
    actors = [str(900_000 + index) for index in range(128)]
    return {"actors": actors, "overview_stage": {
        "actors": {actor: {"ranks": [index + 2, 8192 - index, 8192 - index, index + 1],
                            "damage": [100 + index, 9000 - index], "boss_hints": [False, True]}
                   for index, actor in enumerate(actors)},
        "bosses": ["First", "Second"], "boss_rows": [8192, 8192],
        "backpack": {"name": "Sword", "level": "Rank", "quantity": 20, "bound": 0}}}


def reply(request):
    if request["key"] == "stone_training":
        return "修炼结束，共增加修为：100,消耗灵石：1000"
    if request["key"] == "activity_rewards":
        return "\n".join(f"我的名次：第 {rank} 名" for rank in request["expected"]["ranks"])
    if request["key"] == "activity_boss_status":
        text = "\n".join("我的累计伤害 " + fixture.bounded._number(value)
                         for value in request["expected"]["damage"])
        text += "\n全服血量 75万 / 100万\n全服血量 75万 / 100万"
        return text + "\n" + "\n".join(f"可用 活动首领领奖 排行 {name} 领取"
                                       for name, hint in zip(request["bosses"], request["expected"]["boss_hints"]) if hint)
    return "Rank-Sword\n拥有数量：20，绑定数量：0\n※已装备※\n第 1/1 页"


class OverviewStageFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            nonebot.get_driver()
        except ValueError:
            nonebot.init()

    def test_small_seed_exercises_real_schema_and_personal_activity_reply_oracles(self):
        from nonebot_plugin_xiuxian_2.features.activity.migrations import apply_activity_state_schema
        from nonebot_plugin_xiuxian_2.features.activity.read_model_application import ActivityReadModelApplication
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.item_json import Items

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = SimpleNamespace(game_db=root / "game.db")
            config_path = root / "activity.json"
            config = activity_config._load_default_config()
            config.update(enabled=True, start_time="0", end_time="无限")
            bosses = [row for row in config["gameplay_activities"] if row["type"] == "activity_boss"]
            self.assertEqual(1, len(bosses))
            for row in config["gameplay_activities"]:
                row.update(enabled=True, start_time="0", end_time="无限")
            first_key = bosses[0]["key"]
            second_key = activity_config._activity_config_key(config)
            config_path.write_text(json.dumps(config), encoding="utf-8")
            population = [str(900000 + index) for index in range(8192)]
            actors = population[:128]
            item_id = min(map(int, Items().get_data_by_item_type(("法器",))))
            with DatabaseUnitOfWork(paths.game_db) as uow:
                apply_activity_state_schema(uow)
                uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,bind_num INTEGER,state INTEGER)")
                uow.execute("CREATE TABLE BuffInfo(user_id TEXT)")
                uow.execute("INSERT INTO back VALUES(?,?,20,0,1)", (actors[0], item_id))
                uow.executemany("INSERT INTO activity_user(user_id,sign_days,total_sign_days,last_sign_date) VALUES(?,3,?,'2026-01-01')",
                                ((actor, index + 10) for index, actor in enumerate(population)))
                for key in (first_key, second_key):
                    uow.executemany("INSERT INTO activity_boss_damage(activity_key,user_id,total_damage) VALUES(?,?,?)",
                                    ((key, actor, index * 1234 + 100) for index, actor in enumerate(population)))
            stub = {"actors": actors, "activity_keys": [second_key, first_key], "limits": []}
            with (
                patch.object(fixture.inventory, "seed", return_value=stub),
                patch.object(fixture.bounded, "_generation", return_value={"generation": 0}),
                patch.object(activity_config, "CONFIG_PATH", config_path),
                patch.object(activity_config, "BASE_DIR", root),
            ):
                saved = fixture.seed(SimpleNamespace(paths=paths), [])
                projected = activity_config._load_config_projection_readonly()
                application = ActivityReadModelApplication(paths.game_db, config_loader=lambda: projected)
                for request in fixture.request_plan(saved, "pure", 128)[:2]:
                    text = (application.reward_catalog_text(request["actor"]) if request["key"] == "activity_rewards"
                            else application.boss_status_text(request["actor"]))
                    self.assertEqual("success", fixture.classify_result(request, {"replies": [text]})["outcome"], text)
                self.assertEqual([8192, 8192], saved["overview_stage"]["boss_rows"])
                self.assertEqual(16384, saved["overview_stage"]["activity_snapshot"]["damage"]["rows"])

    def test_frozen_counts_actors_commands_and_cooldown_boundaries(self):
        saved, seen, numbers = manifest(), set(), set()
        root = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        command_names = []
        for path in (root / "xiuxian_activity/__init__.py", root / "xiuxian_back/__init__.py",
                     root / "xiuxian_buff/__init__.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "on_command" and node.args:
                    command_names.append(ast.literal_eval(node.args[0]))
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            expected = Counter(activity_rewards=43, activity_boss_status=43, main_backpack=42) if phase == 0 else Counter(
                activity_rewards=32, activity_boss_status=32, main_backpack=32, stone_training=32)
            self.assertEqual(expected, Counter(row["key"] for row in plan))
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            for request in plan:
                self.assertEqual(1, command_names.count(request["command"]), request["command"])
                self.assertNotIn(request["number"], numbers)
                numbers.add(request["number"])
                if request["key"] != "stone_training":
                    identity = request["command"], request["actor"]
                    self.assertNotIn(identity, seen)
                    seen.add(identity)
                    self.assertEqual(fixture.classify_result(request, {"replies": [reply(request)]})["outcome"], "success")
            writers = [row["actor"] for row in plan if row["key"] == "stone_training"]
            self.assertEqual((0, 32, 1)[phase], len(set(writers)))
        self.assertEqual(saved, manifest())
        self.assertEqual(384, len(numbers))
        args = probe.parse_args(["--fixture", "/tmp/fixture", "--report", "/tmp/report", "--scenario", "overview-stage-v29"])
        self.assertEqual(128, args.participants)

    def test_wrong_rank_damage_hint_or_quantity_cannot_be_success(self):
        plan = fixture.request_plan(manifest(), "pure", 128)
        for request in plan[:3]:
            text = reply(request)
            if request["key"] == "activity_rewards":
                wrong = text.replace("第 2 名", "第 1 名")
            elif request["key"] == "activity_boss_status":
                wrong = text.replace("我的累计伤害 100", "我的累计伤害 101")
            else:
                wrong = text.replace("拥有数量：20", "拥有数量：19")
            self.assertNotEqual(text, wrong)
            self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [wrong]})["outcome"])
        request = plan[1]
        self.assertEqual("unclassified", fixture.classify_result(request, {
            "replies": [reply(request).replace("可用 活动首领领奖 排行 Second 领取", "")]
        })["outcome"])

    def test_native_markdown_fallback_checks_exact_inventory_fields_and_equipped_state(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back import _build_backpack_fallback_with_sections

        request = fixture.request_plan(manifest(), "pure", 128)[2]
        sections = [("法器", [{"goods_id": 1, "name": "Sword", "count": 20, "bind": 0,
                              "is_equipped": True, "_is_equipped": True}])]
        text = _build_backpack_fallback_with_sections(
            title="Synthetic bag", sections=sections, current_page=1, total_pages=1,
            show_use_btn=True, next_cmd="我的背包 1",
        )
        self.assertEqual("success", fixture.classify_result(request, {"replies": [text]})["outcome"])
        for old, new in (("数量:20", "数量:19"), ("绑定:0", "绑定:1"),
                         ("※已装备※", ""), ("Sword", "Other"), ("第 1/1 页", "第 1/2 页")):
            with self.subTest(field=old):
                self.assertIn(old, text)
                self.assertEqual("unclassified", fixture.classify_result(request, {
                    "replies": [text.replace(old, new)]})["outcome"])

    def test_initial_preflight_is_allowed_but_partial_traffic_is_rejected(self):
        saved = manifest()
        oracle = saved["overview_stage"]
        oracle.update(inventory_snapshot={"back": {"rows": 27264}}, activity_snapshot={"sign": {"rows": 8192}},
                      config_sha256="digest")
        saved["initial_generation"] = {"generation": 7}
        context = SimpleNamespace(paths=SimpleNamespace(game_db="isolated.db"))
        with (
            patch.object(fixture.base, "check_invariants", return_value={"errors": [], "fresh_training": 0, "invariants": "seed"}),
            patch.object(fixture.transport, "_snapshot", return_value=oracle["inventory_snapshot"]),
            patch.object(fixture, "_activity_snapshot", return_value=oracle["activity_snapshot"]),
            patch.object(fixture.bounded, "_generation", return_value={"generation": 7}),
            patch("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config.CONFIG_PATH") as config,
            patch.object(fixture.hashlib, "sha256", return_value=SimpleNamespace(hexdigest=lambda: "digest")),
        ):
            config.read_bytes.return_value = b"isolated"
            self.assertTrue(fixture.check_invariants(context, [], saved, [])["ok"])
            request = fixture.request_plan(saved, "pure", 128)[0]
            row = {"request": request, "replies": [reply(request)],
                   "outcome": "success", "reason": "v29_exact_live_projection"}
            self.assertFalse(fixture.check_invariants(context, [], saved, [row])["ok"])

    def test_sql_counters_use_complete_aggregation_and_do_not_confuse_batch_damage(self):
        recorder = probe.matrix.QueryRecorder()
        recorder.active = True
        for sql in (
            "SELECT sign_days,total_sign_days,last_sign_date FROM activity_user WHERE user_id=?",
            "SELECT sign_days,last_sign_date,total_sign_days FROM activity_user WHERE user_id=?",
            "SELECT total_damage FROM activity_boss_damage WHERE activity_key=? AND user_id=?",
            "SELECT activity_key,total_damage FROM activity_boss_damage WHERE activity_key IN (?,?) AND user_id=?",
            "SELECT COUNT(*) AS better FROM activity_boss_damage WHERE activity_key=?",
            "SELECT * FROM back WHERE user_id=? and goods_num >= 1",
            "SELECT goods_id,goods_name,goods_type,goods_num,bind_num,state FROM back WHERE user_id=? and goods_num >= 1",
        ):
            recorder.record(None, sql, "execute", .1)
            recorder.record(None, sql, "fetch", .2)
        counts = probe.recorder_metrics(recorder)["stage_read_io"]
        self.assertEqual({"sign_own": 2, "boss_own": 1, "boss_better": 1, "inventory_full": 1, "inventory_compact": 1},
                         {key: row["statements"] for key, row in counts.items()})
        self.assertEqual(.6, counts["sign_own"]["total_ms"])

    def test_invalid_phase_size_and_duplicate_actor_fail_before_dispatch(self):
        for mode, count, phase in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)
        saved = deepcopy(manifest())
        saved["actors"][-1] = saved["actors"][0]
        with self.assertRaises(ValueError):
            fixture.request_plan(saved, "pure", 128)
