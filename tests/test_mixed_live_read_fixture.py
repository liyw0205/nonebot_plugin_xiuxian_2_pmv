"""Small V28 frozen-plan/semantic-oracle checks without a load or database."""
from collections import Counter
from copy import deepcopy
import ast
from pathlib import Path
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests import mixed_live_read_fixture as fixture
from scripts import mixed_command_probe as probe


def manifest():
    actors = [str(900_000 + index) for index in range(128)]

    def item(index, label="功法名字："):
        return {"id": str(1000 + index), "name_line": label + f"真实物品{index}",
                "rank_line": "境界：元神境初期", "holding_line": f"全服持有：{2560 + index} 个"}

    return {"actors": actors, "live_read": {
        "detail": {**item(99, "名字："), "name": "真实物品99"},
        "catalog": [item(index) for index in range(15)], "catalog_pages": 7,
        "probabilities": {actor: f"道友下一次突破成功概率为{31 + index % 13}%" for index, actor in enumerate(actors)}}}


def reply(request):
    if request["key"] == "stone_training":
        return "修炼结束，共增加修为：100,消耗灵石：1000"
    if request["key"] == "breakthrough_probability":
        return request["expected_reply"]
    rows = [request["expected_detail"]] if request["key"] == "item_detail" else request["expected_catalog"]
    text = "\n\n".join("\n".join(("ID：" + row["id"], row["name_line"], row["rank_line"],
                                       row["holding_line"], "效果：真实描述")) for row in rows)
    return text + (f"\n第 1/{request['expected_pages']} 页" if request["key"] == "item_catalog" else "")


class LiveReadFixtureTests(unittest.TestCase):
    def test_empty_preflight_checks_seed_without_requiring_traffic_results(self):
        saved = manifest()
        saved.update(initial_generation={"generation": 7})
        saved["live_read"].update(snapshot={"back": {"rows": 27264}}, profiles=[{"user_id": "u"}])
        context = SimpleNamespace(paths=SimpleNamespace(game_db="isolated.db"))
        with (
            patch.object(fixture.base, "check_invariants", return_value={
                "errors": [], "fresh_training": 0, "invariants": "seed"}),
            patch.object(fixture.transport, "_snapshot", return_value=saved["live_read"]["snapshot"]),
            patch.object(fixture.shared, "_rows", return_value=saved["live_read"]["profiles"]),
            patch.object(fixture.bounded, "_generation", return_value={"generation": 7}),
        ):
            self.assertTrue(fixture.check_invariants(context, [], saved, [])["ok"])
            self.assertFalse(fixture.check_invariants(context, [], saved, [{
                "request": {"key": "breakthrough_probability", "actor": "u", "phase_label": "pure",
                            "expected_reply": "exact probability"}, "outcome": "success",
                "reason": "v28_exact_live_fields", "replies": ["exact probability"]}])["ok"])

    def test_scenario_uses_existing_driver_limits_and_frozen_actor_count(self):
        args = probe.parse_args(["--fixture", "/tmp/fixture", "--report", "/tmp/report",
                                 "--scenario", "live-read-v28"])
        self.assertEqual(args.participants, 128)
        self.assertEqual(args.scenario, "live-read-v28")

    def test_io_counts_come_from_complete_sql_aggregation_including_non_top20(self):
        recorder = probe.matrix.QueryRecorder()
        recorder.active = True
        for index in range(25):
            recorder.record(None, f"SELECT {index}", "execute", 100)
        for sql, count, elapsed in (
            ("SELECT * FROM user_xiuxian WHERE user_id=? ORDER BY rowid ASC LIMIT 1", 2, .2),
            ("SELECT SUM(goods_num) FROM back WHERE goods_id=?", 73, .1),
            ("SELECT requested.column1,SUM(back.goods_num) FROM (VALUES (0,?)) AS requested", 1, .3),
        ):
            for _ in range(count):
                recorder.record(None, sql, "execute", elapsed)
                recorder.record(None, sql, "fetch", elapsed)
        metrics = probe.recorder_metrics(recorder)
        self.assertFalse(any("user_xiuxian" in row["sql"] for row in metrics["top_queries"]))
        self.assertEqual(metrics["live_read_io"], {
            "full_profile": {"statements": 2, "total_ms": .8},
            "single_goods_total": {"statements": 73, "total_ms": 14.6},
            "batch_goods_totals": {"statements": 1, "total_ms": .6}})

    def test_three_phases_freeze_read_counts_real_writes_and_no_cooldown_collisions(self):
        saved, cooldowns, numbers = manifest(), set(), set()
        self.assertEqual([(128, 10, 20), (128, 20, 20), (128, 20, 35)], [
            (row["requests"], row["concurrency"], row["arrival_rate"]) for row in fixture.phase_specs()])
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            expected = Counter(item_detail=43, item_catalog=43, breakthrough_probability=42) if phase == 0 else Counter(
                item_detail=32, item_catalog=32, breakthrough_probability=32, stone_training=32)
            self.assertEqual(expected, Counter(row["key"] for row in plan))
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            ids = {row["number"] for row in plan}
            self.assertEqual(128, len(ids))
            self.assertFalse(ids & numbers)
            numbers |= ids
            identities = [(row["key"], row["actor"]) for row in plan if row["key"] != "stone_training"]
            self.assertEqual(len(identities), len(set(identities)))
            self.assertFalse(set(identities) & cooldowns)
            cooldowns |= set(identities)
            writes = [row for row in plan if row["key"] == "stone_training"]
            self.assertEqual((0, 32, 1)[phase], len({row["actor"] for row in writes}))
            self.assertTrue(all(index % 4 == 3 for index, row in enumerate(plan) if row["key"] == "stone_training"))
            for row in plan:
                self.assertNotIn("修炼60", row["input_text"])
                if row["key"] == "item_catalog":
                    self.assertEqual("查看修仙界物品 功法", row["input_text"])
                if row["key"] == "item_detail":
                    self.assertEqual("查看效果 真实物品99", row["input_text"])
        self.assertEqual(384, len(numbers))
        self.assertEqual(saved, manifest())

    def test_plan_rejects_invalid_phase_size_and_duplicate_actors(self):
        for mode, count, phase in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)
        saved = manifest()
        saved["actors"][-1] = saved["actors"][0]
        with self.assertRaises(ValueError):
            fixture.request_plan(saved, "pure", 128)

    def test_four_commands_resolve_to_one_real_matcher_and_handler(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        trees = [ast.parse((root / path).read_text(encoding="utf-8")) for path in (
            "xiuxian_back/__init__.py", "xiuxian_base/breakthrough_tribulation.py", "xiuxian_buff/__init__.py")]
        for command in [name for _, name in fixture.COMMANDS] + ["灵石修炼"]:
            matches = [(tree, node.targets[0].id) for tree in trees for node in ast.walk(tree)
                       if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                       and isinstance(node.value.func, ast.Name) and node.value.func.id == "on_command"
                       and node.value.args and isinstance(node.value.args[0], ast.Constant)
                       and node.value.args[0].value == command]
            self.assertEqual(1, len(matches), command)
            tree, matcher = matches[0]
            handlers = [node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)
                        and any(isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
                                and isinstance(value.func.value, ast.Name) and value.func.value.id == matcher
                                and value.func.attr == "handle" for value in node.decorator_list)]
            self.assertEqual(1, len(handlers), command)

    def test_independent_rank_oracle_handles_legacy_minus5_and_clamps(self):
        ranks = [f"realm{index}" for index in range(76)]
        self.assertEqual("realm55", fixture._rank({"rank": 1}, ranks))
        self.assertEqual("realm33", fixture._rank({"rank": -5}, ranks))
        self.assertEqual("realm0", fixture._rank({"rank": 1000}, ranks))
        self.assertEqual("realm75", fixture._rank({"rank": -1000}, ranks))

    def test_semantic_replies_survive_quoted_text_forward_nodes_and_desc_blank_lines(self):
        for request in fixture.request_plan(manifest(), "mixed", 128, 1)[:4]:
            text = reply(request)
            for newlines in ("\n", "\r\n", "\r"):
                converted = text.replace("\n", newlines)
                self.assertEqual("success", fixture.classify_result(request, {"replies": [converted]})["outcome"])
            quoted = "[CQ:at,qq=900001] " + "\n".join("> " + line for line in text.splitlines())
            self.assertEqual("success", fixture.classify_result(request, {"replies": [quoted]})["outcome"])
            nodes = [{"type": "node", "data": {"name": "fixture", "uin": "10000", "content": text}}]
            self.assertEqual("success", fixture.classify_result(request, {"replies": [repr(nodes)]})["outcome"])
            expanded = text.replace("效果：真实描述", "效果：\n\n\n真实描述\n\n")
            self.assertEqual("success", fixture.classify_result(request, {"replies": [expanded]})["outcome"])

    def test_catalog_requires_all15_ids_names_counts_ranks_order_and_total_pages(self):
        request = next(row for row in fixture.request_plan(manifest(), "pure", 128) if row["key"] == "item_catalog")
        text = reply(request)
        first = request["expected_catalog"][0]
        damaged = [text.replace(first["name_line"], "功法名字：错误物品", 1),
                   text.replace(first["holding_line"], "全服持有：0 个", 1),
                   text.replace(first["rank_line"], "境界：错误境界", 1),
                   text.replace("ID：1000", "ID：9999", 1),
                   text.replace("第 1/7 页", "第 1/8 页"), text.replace("第 1/7 页", ""),
                   text + "\nID：1000\n" + first["name_line"],
                   text.replace("功法名字：真实物品0\n", "", 1)]
        reordered = deepcopy(request)
        reordered["expected_catalog"][:2] = reversed(reordered["expected_catalog"][:2])
        damaged.append(reply(reordered))
        for bad in damaged:
            with self.subTest(bad=bad):
                self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [bad]})["outcome"])

    def test_detail_and_probability_reject_wrong_live_values_duplicate_or_missing_fields(self):
        plan = fixture.request_plan(manifest(), "pure", 128)
        detail = next(row for row in plan if row["key"] == "item_detail")
        text = reply(detail)
        for bad in (text.replace("名字：真实物品99", "名字：错误"), text.replace("全服持有：2659 个", "全服持有：0 个"),
                    text.replace("境界：元神境初期", "境界：江湖好手"), text + "\n" + text, ""):
            self.assertEqual("unclassified", fixture.classify_result(detail, {"replies": [bad]})["outcome"])
        probability = next(row for row in plan if row["key"] == "breakthrough_probability")
        for bad in (reply(probability).replace("31%", "30%"), reply(probability) * 2, "", "wrong"):
            self.assertEqual("unclassified", fixture.classify_result(probability, {"replies": [bad]})["outcome"])

    def test_refusal_cooldown_boundary_exception_timeout_and_invalid_envelope_are_separate(self):
        request = fixture.request_plan(manifest(), "pure", 128)[0]
        for fields, expected in (({"blocked": ["boundary"]}, "boundary"), ({"errors": ["SQL failure"]}, "exception"),
                                 ({"status": "timeout"}, "timeout")):
            self.assertEqual(expected, fixture.classify_result(request, {"replies": [reply(request)], **fields})["outcome"])
        for text, expected in (("物品 不存在 不存在，请检查名称是否正确！", "business_rejection"),
                               ("慢...慢一..点", "cooldown"), ("{'unexpected': 'shape'}", "unclassified")):
            self.assertEqual(expected, fixture.classify_result(request, {"replies": [text]})["outcome"])


if __name__ == "__main__":
    unittest.main()
