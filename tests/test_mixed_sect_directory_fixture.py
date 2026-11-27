"""V22 frozen distributions, catalog validity and exact transport reply oracles."""
from collections import Counter
from copy import deepcopy
import ast
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import mixed_sect_directory_fixture as fixture
from tests import test_mixed_sect_shared_fixture as previous


def manifest():
    saved = previous.manifest()
    nodes = {
        "sect_directory": [f"编号{index}：Sect{index}\n宗主：Owner{index}\n宗门状态：可加入 (64/100)\n"
                           f"建设度：{4_000_000_000 + index}\n" for index in range(128)],
        "sect_main_view": ["人阶下品Main0", "人阶上品Main1"],
        "sect_sec_view": ["人阶下品:Sec0", "人阶上品:Sec1"],
    }
    return {**saved, "display_nodes": nodes,
            "display_oracles": {key: {"sha256": fixture._reply_digest(value), "entries": len(value)}
                                for key, value in nodes.items()}}


def forward(request, nodes, *, merged=True):
    label = fixture.VIEWS[request["key"]][0]
    return str({"group_id": 900100, "messages": [
        {"type": "node", "data": {"name": label, "uin": "10001", "content": content}}
        for content in (["\n".join(nodes)] if merged else nodes)]})


class SectDirectoryFixtureTests(unittest.TestCase):
    def setUp(self):
        formatter = patch.object(fixture.base, "_number", side_effect=lambda value: str(value))
        formatter.start()
        self.addCleanup(formatter.stop)

    def test_frozen_distribution_reuses_exact_writer_and_control_subsets_without_cd_collision(self):
        saved, messages, cooldown = manifest(), set(), set()
        self.assertEqual([(128, 10, 10), (128, 20, 20), (128, 20, 35)], [
            (row["requests"], row["concurrency"], row["arrival_rate"]) for row in fixture.phase_specs()])
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            old = fixture.base.request_plan(saved, spec["mode"], 128, phase)
            counts = Counter(row["key"] for row in plan)
            self.assertEqual(16, counts["sect_directory"])
            self.assertEqual(56 if phase == 0 else 32, counts["sect_main_view"])
            self.assertEqual(56 if phase == 0 else 32, counts["sect_sec_view"])
            self.assertEqual(0 if phase == 0 else 16, counts["asset_overview"])
            self.assertEqual(128, len({row["number"] for row in plan}))
            self.assertFalse(messages & {row["number"] for row in plan})
            messages |= {row["number"] for row in plan}
            writes = [row for row in plan if row["key"] in fixture.PRACTICES]
            without_role = lambda row: {key: value for key, value in row.items() if key != "performance_role"}
            self.assertEqual(list(map(without_role, writes)),
                             [without_role(row) for row in old if row["key"] in fixture.PRACTICES])
            controls = [row for row in plan if row["key"] == "asset_overview"]
            self.assertTrue(all(row in old for row in controls))
            self.assertTrue(all(row["performance_role"] == "unchanged_control" for row in writes + controls))
            keys = {(row["actor"], row["key"]) for row in writes}
            self.assertEqual(len(keys), len(writes))
            self.assertFalse(keys & cooldown)
            cooldown |= keys
            if phase:
                self.assertEqual([10, 11, 11], sorted(Counter(row["key"] for row in writes).values()))
                self.assertEqual(32 if phase == 1 else 11, len({row["actor"] for row in writes}))
                self.assertEqual(4 if phase == 1 else 1, len({row["sect_id"] for row in writes}))
            for row in plan:
                if row["key"] in fixture.VIEWS:
                    self.assertNotIn("expected_sect", row)
                    self.assertLess(len(str(row["expected_reply"])), 130)
            self.assertEqual(saved, manifest())
        self.assertEqual(384, len(messages))

    def test_seven_command_names_resolve_to_exactly_one_real_matcher_and_handler(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        trees = [ast.parse((root / module / "__init__.py").read_text(encoding="utf-8"))
                 for module in ("xiuxian_sect", "xiuxian_base")]
        self.assertEqual(7, len(fixture.COMMANDS))
        for _, command in fixture.COMMANDS:
            matches = [(tree, node.targets[0].id) for tree in trees for node in ast.walk(tree)
                       if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                       and isinstance(node.value.func, ast.Name) and node.value.func.id == "on_command"
                       and node.value.args and isinstance(node.value.args[0], ast.Constant)
                       and node.value.args[0].value == command]
            self.assertEqual(1, len(matches), command)
            tree, matcher = matches[0]
            handlers = [node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)
                        and any(isinstance(item, ast.Call) and isinstance(item.func, ast.Attribute)
                                and isinstance(item.func.value, ast.Name)
                                and item.func.value.id == matcher and item.func.attr == "handle"
                                for item in node.decorator_list)]
            self.assertEqual(1, len(handlers), command)

    def test_invalid_phase_and_group_plan_fail_before_dispatch(self):
        for args in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, 3)):
            with self.subTest(args=args), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), *args)
        saved = manifest()
        saved["hot_groups"][0].pop()
        with self.assertRaises(ValueError):
            fixture.request_plan(saved, "contention", 128, 2)

    def test_directory_oracle_keeps_every_ordered_entry_status_owner_and_large_scale(self):
        sects = {str(index): {"sect_id": index, "sect_name": f"S{index}", "sect_owner": str(index),
                             "sect_scale": 10**30 if index == 4 else 4_000_000_000,
                             "closed": index == 1, "join_open": index != 2}
                 for index in (4, 3, 2, 1)}
        nodes = fixture._directory_nodes(sects, {"1": "A", "2": "", "3": None, "4": "D"},
                                         {1: 64, 2: 64, 3: 100, 4: 64})
        self.assertEqual(4, len(nodes))
        for index, status in ((1, "宗门已封闭"), (2, "宗门关闭加入"),
                              (3, "人数已满 (100/100)"), (4, "可加入 (64/100)")):
            self.assertTrue(nodes[index - 1].startswith(f"编号{index}：S{index}\n"))
            self.assertIn("宗门状态：" + status + "\n", nodes[index - 1])
        self.assertIn("宗主：\n", nodes[1])
        self.assertIn("宗主：暂无\n", nodes[2])
        self.assertIn(f"建设度：{10**30}\n", nodes[3])

    def test_forward_and_text_wrappers_require_complete_exact_directory_and_catalog(self):
        saved = manifest()
        plan = fixture.request_plan(saved, "pure", 128)
        samples = {row["key"]: row for row in plan}
        for key, request in samples.items():
            nodes = saved["display_nodes"][key]
            for merged in (True, False):
                reply = forward(request, nodes, merged=merged)
                self.assertEqual("success", fixture.classify_result(request, {"replies": [reply]})["outcome"])
            title = fixture.VIEWS[key][1] or ""
            for newline in ("\n", "\r", "\r\n"):
                reply = (title + "\n".join(nodes)).replace("\n", newline)
                self.assertEqual("success", fixture.classify_result(request, {"replies": [reply]})["outcome"])
            for changed in (nodes[:-1], nodes + nodes[:1], list(reversed(nodes)),
                            [nodes[0].replace("Main0" if key == "sect_main_view" else
                                              "Sec0" if key == "sect_sec_view" else "Owner0", "wrong"), *nodes[1:]]):
                with self.subTest(key=key, changed=changed[:1]):
                    self.assertEqual("unclassified", fixture.classify_result(request, {
                        "replies": [forward(request, changed)]})["outcome"])
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [forward(request, nodes), forward(request, nodes)]})["outcome"])

    def test_forward_metadata_and_malformed_payloads_cannot_hide_wrong_business_output(self):
        saved = manifest()
        request = fixture.request_plan(saved, "pure", 128)[0]
        good = forward(request, saved["display_nodes"][request["key"]])
        for damaged in (good.replace("'name': '宗门列表'", "'name': 'wrong'"),
                        good.replace("'uin': '10001'", "'uin': 'wrong'"),
                        good.replace("'type': 'node'", "'type': 'image'"),
                        good[:-1], "{'messages': []}", "{'evil': " + good + "}",
                        "prefix" + good):
            self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [damaged]})["outcome"])
        for fields, expected in (({"errors": ["failure"]}, "exception"),
                                 ({"blocked": ["DB boundary"]}, "boundary"),
                                 ({"status": "timeout"}, "timeout")):
            self.assertEqual(expected, fixture.classify_result(request, {"replies": [good], **fields})["outcome"])
        for text, expected in (("慢...慢一..点", "cooldown"),
                               ("本宗尚未获得任何功法", "unclassified")):
            self.assertEqual(expected, fixture.classify_result(request, {"replies": [text]})["outcome"])

    def test_catalog_selection_cross_checks_real_api_definitions_and_config(self):
        definitions = {1: {"name": "M1", "level": "low"}, 2: {"name": "M2", "level": "high"},
                       3: {"name": "S1", "level": "low"}, 4: {"name": "S2", "level": "high"}}
        for item in definitions.values():
            item["rank"] = "55"
        config = {key: {"建设度": 100, "学习资材消耗": 200}
                  for key in ("宗门主功法参数", "宗门神通参数")}
        raw = {key: {"name": row["name"], "rank": row["level"], "level": row["rank"]}
               for key, row in definitions.items()}
        module = SimpleNamespace(
            items=SimpleNamespace(get_data_by_item_type=lambda kinds: (2, 1) if kinds == ("功法",) else (4, 3),
                                  get_data_by_item_id=lambda item: definitions[item]),
            BuffJsonDate=lambda: SimpleNamespace(get_main_buff=lambda item: raw[item],
                                                get_sec_buff=lambda item: raw[item]),
            buffrankkey={"low": 1, "high": 2}, config=config)
        selected = fixture._catalog_selection(module)
        self.assertEqual([1, 2], [row["id"] for row in selected["功法"]])
        self.assertEqual([3, 4], [row["id"] for row in selected["神通"]])
        for kind in ("功法", "神通"):
            stored = fixture._catalog_storage(selected[kind])
            self.assertNotIn(", ", stored)
            self.assertEqual([str(row["id"]) for row in reversed(selected[kind])], stored[1:-1].split(","))
            self.assertTrue(all(definitions[int(item)] for item in stored[1:-1].split(",")))
        self.assertEqual({"sect_main_view": ["lowM1", "highM2"],
                          "sect_sec_view": ["low:S1", "high:S2"]}, fixture._catalog_nodes(selected))
        raw[1]["name"] = "stale different definition"
        with self.assertRaisesRegex(ValueError, "disagree"):
            fixture._catalog_selection(module)
        raw[1]["name"] = definitions[1]["name"]
        config["宗门神通参数"]["建设度"] = 0
        with self.assertRaisesRegex(ValueError, "config"):
            fixture._catalog_selection(module)

    def test_bundled_catalog_preflight_uses_production_normalization_without_database(self):
        from nonebot_plugin_xiuxian_2.features.admin.item_catalog_repository import AdminItemCatalogRepository

        root = Path(__file__).parents[1]
        paths = {kind: root / "data/xiuxian/功法" / filename
                 for kind, filename in (("功法", "主功法.json"), ("神通", "神通.json"))}
        repository = AdminItemCatalogRepository(paths)
        repository.ensure_loaded()
        raw = {kind: json.loads(path.read_text(encoding="utf-8")) for kind, path in paths.items()}
        tree = ast.parse((root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/__init__.py")
                         .read_text(encoding="utf-8"))
        ranks = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id == "buffrankkey"
                             for target in node.targets))
        config_tree = ast.parse((root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_sect/sectconfig.py")
                                .read_text(encoding="utf-8"))
        config = next(ast.literal_eval(node.value) for node in config_tree.body
                      if isinstance(node, ast.Assign) and any(
                          isinstance(target, ast.Name) and target.id == "CONFIG"
                          for target in node.targets))
        module = SimpleNamespace(items=repository, buffrankkey=ranks, config=config,
                                 BuffJsonDate=lambda: SimpleNamespace(
                                     get_main_buff=lambda key: raw["功法"][str(key)],
                                     get_sec_buff=lambda key: raw["神通"][str(key)]))
        selected = fixture._catalog_selection(module)
        for rows in selected.values():
            self.assertEqual(2, len({row["rank"] for row in rows}))
            for item in fixture._catalog_storage(rows)[1:-1].split(","):
                self.assertIsNotNone(repository.get_data_by_item_id(item))

    def test_practice_and_asset_oracles_and_invariants_reuse_previous_contract(self):
        saved = manifest()
        plan = fixture.request_plan(saved, "mixed", 128, 1)
        for request in plan:
            if request["key"] in fixture.VIEWS:
                continue
            result = {"replies": [previous.reply(request)]}
            self.assertEqual(fixture.base.classify_result(request, result), fixture.classify_result(request, result))
        request = plan[0]
        results = [{"request": request, "replies": [forward(request, saved["display_nodes"][request["key"]])]}]
        with patch.object(fixture.base, "check_invariants", return_value={"ok": True}) as check:
            self.assertEqual({"ok": True}, fixture.check_invariants("context", "managers", saved, results))
        self.assertEqual("success", check.call_args.args[-1][0]["outcome"]["outcome"])
        self.assertNotIn("outcome", results[0])


if __name__ == "__main__":
    unittest.main()
