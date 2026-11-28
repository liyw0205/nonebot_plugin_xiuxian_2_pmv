"""Frozen real-command routes, reply oracles and bounded inventory projections."""
from collections import Counter
from copy import deepcopy
import ast
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import mixed_shared_query_fixture as fixture
from tests.test_mixed_player_projection_fixture import manifest as base_manifest


def expectation(index=0):
    return {"skill_lines": [f"{label}：Skill{slot}" for slot, (_, _, label) in enumerate(fixture.SLOTS)],
            "equipment_blocks": [f"名字：Equipment{slot}\n拥有数量: {20 + index % 3} (绑定: {index % 2})"
                                 + ("\n※已装备※" if slot in (0, 4) else "") for slot in range(8)],
            "equipment_names": [f"Equipment{slot}" for slot in range(8)],
            "quantity": 20 + index % 3, "bound": index % 2, "equipped_count": 2}


def manifest():
    saved = base_manifest()
    saved["shared_query"] = {"oracle": {actor: expectation(index)
                                        for index, actor in enumerate(saved["actors"])},
                             "snapshot": {}, "roles": {key: role for key, _, role in fixture.COMMANDS}}
    return saved


def reply(request):
    expected = request["expected_shared_query"]
    if request["key"] == "skill_info":
        return "**我的功法**\n" + "\n".join(expected["skill_lines"]) + "\n"
    return "\n".join(expected["equipment_blocks"]) + "\n第 1/1 页\n"


class SharedQueryPlanTests(unittest.TestCase):
    def test_three_frozen_phases_preserve_live_writes_profiles_and_unique_target_actors(self):
        saved, numbers = manifest(), set()
        self.assertEqual([(128, 10, 20), (128, 20, 35), (128, 20, 50)],
                         [(row["requests"], row["concurrency"], row["arrival_rate"])
                          for row in fixture.phase_specs()])
        for phase, spec in enumerate(fixture.phase_specs()):
            original = fixture.player.request_plan(saved, spec["mode"], 128, phase)
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            counts = Counter(row["key"] for row in plan)
            self.assertEqual(56, counts["profile_info"])
            self.assertEqual(0 if phase == 0 else 32, counts["stone_training"])
            for key, _, role in fixture.COMMANDS:
                targets = [row for row in plan if row["key"] == key]
                self.assertEqual(36 if phase == 0 else 20, len(targets))
                self.assertEqual(len(targets), len({row["actor"] for row in targets}))
                self.assertEqual({role}, {row["performance_role"] for row in targets})
            self.assertEqual(128, len(plan))
            current = {row["number"] for row in plan}
            self.assertEqual(128, len(current))
            self.assertFalse(current & numbers)
            numbers |= current
            for old, new in zip(original, plan):
                if old["key"] in {"profile_info", "stone_training"}:
                    self.assertEqual(old, new)
            self.assertFalse(set(saved["groups"]["preserved_background"]) &
                             {row["actor"] for row in plan if "expected_shared_query" in row})
        self.assertEqual(384, len(numbers))

    def test_each_target_has_one_real_primary_matcher_and_one_handler(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        for _, command, _ in fixture.COMMANDS:
            matches = []
            for module in ("xiuxian_buff", "xiuxian_back"):
                tree = ast.parse((root / module / "__init__.py").read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                            and isinstance(node.value.func, ast.Name) and node.value.func.id == "on_command"
                            and node.value.args and isinstance(node.value.args[0], ast.Constant)
                            and node.value.args[0].value == command):
                        matches.append((tree, node.targets[0].id))
            self.assertEqual(1, len(matches), command)
            tree, name = matches[0]
            handlers = [node for node in ast.walk(tree) if isinstance(node, ast.AsyncFunctionDef)
                        and any(isinstance(decorator, ast.Call)
                                and isinstance(decorator.func, ast.Attribute)
                                and isinstance(decorator.func.value, ast.Name)
                                and decorator.func.value.id == name and decorator.func.attr == "handle"
                                for decorator in node.decorator_list)]
            self.assertEqual(1, len(handlers), command)

    def test_invalid_counts_and_actor_groups_fail_before_dispatch(self):
        for mode, count, phase in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)
        saved = manifest()
        saved["groups"]["exact_blob"] = saved["groups"]["exact_blob"][:-1]
        with self.assertRaises(ValueError):
            fixture.request_plan(saved, "pure", 128)

    def test_exact_nonempty_slots_counts_equipped_state_and_failures_are_required(self):
        targets = [row for row in fixture.request_plan(manifest(), "pure", 128)
                   if "expected_shared_query" in row][:2]
        for request in targets:
            text = reply(request)
            for newline in ("\n", "\r", "\r\n"):
                self.assertEqual("success", fixture.classify_result(request, {
                    "replies": [text.replace("\n", newline)]})["outcome"])
            for fields, outcome in (({"errors": ["injected"]}, "exception"),
                                    ({"blocked": ["outside root"]}, "boundary"),
                                    ({"status": "timeout"}, "timeout"),
                                    ({"replies": ["慢...慢一..点"]}, "cooldown")):
                self.assertEqual(outcome, fixture.classify_result(request, {
                    "replies": [text], **fields})["outcome"])
            damaged = deepcopy(request)
            field = "skill_lines" if request["key"] == "skill_info" else "equipment_blocks"
            damaged["expected_shared_query"][field][0] += "wrong"
            self.assertEqual("unclassified", fixture.classify_result(damaged, {"replies": [text]})["outcome"])
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [text + text]})["outcome"])
        equipment = targets[1]
        text = reply(equipment)
        for broken in (text.replace("※已装备※", "", 1), text.replace("拥有数量:", "数量:", 1),
                       text.replace("第 1/1 页", "第 1/2 页")):
            self.assertEqual("unclassified", fixture.classify_result(equipment, {"replies": [broken]})["outcome"])

    def test_real_before_r01_forward_envelope_extracts_content_without_rewriting_literal_escapes(self):
        # First equipment response captured in shared-query-before-v19-20261127-r01.json.
        captured = (
            r"{'group_id': 900100, 'messages': [{'type': 'node', 'data': {'name': '我的装备', 'uin': '900000', 'content': '\r☆------法器------☆"
            r"\r名字：精铁符剑\r品阶：下品符器\r效果：精铁藏锋，符意附刃，御之可破浊煞邪祟，提升8%攻击力！\r拥有数量: 21 (绑定: 1)\r※已装备※"
            r"\r名字：火铜符剑\r品阶：上品符器\r效果：火铜蕴炁，符锋一振，便有燎原烈意随剑而行，提升12%攻击力！\r拥有数量: 21 (绑定: 1)"
            r"\r名字：桃木符剑\r品阶：下品符器\r效果：桃符裁煞，木剑引雷，一念惊破幽祟心胆，提升8%会心率！\r拥有数量: 21 (绑定: 1)"
            r"\r名字：精元符剑\r品阶：下品符器\r效果：符纹藏锋，精元暗涌，一剑出鞘便惊灵台，提升4%攻击力！提升4%会心率！\r拥有数量: 21 (绑定: 1)"
            r"\r☆------防具------☆\r名字：修士道袍\r品阶：下品符器\r效果：随处可见的长袍，能略微抵挡真元冲击，提升8%减伤率！\r拥有数量: 21 (绑定: 1)\r※已装备※"
            r"\r名字：御灵盾\r品阶：上品玄器\r效果：灵纹潜照，御气成垣，可拒外邪侵体而守心台，提升14%减伤率！\r拥有数量: 21 (绑定: 1)"
            r"\r名字：阴磷甲\r品阶：下品玄器\r效果：幽磷潜甲，似自阴渊剥鳞而成，寒意侵骨却能镇身护命，提升12%减伤率！\r拥有数量: 21 (绑定: 1)"
            r"\r名字：化尘道袍\r品阶：上品符器\r效果：披此袍如身隐尘外，符意流转，自护清虚道骨，提升10%减伤率！\r拥有数量: 21 (绑定: 1)\r\r第 1/1 页'}}]}"
        )
        definitions = [
            ("精铁符剑", "下品符器", "精铁藏锋，符意附刃，御之可破浊煞邪祟，提升8%攻击力！"),
            ("火铜符剑", "上品符器", "火铜蕴炁，符锋一振，便有燎原烈意随剑而行，提升12%攻击力！"),
            ("桃木符剑", "下品符器", "桃符裁煞，木剑引雷，一念惊破幽祟心胆，提升8%会心率！"),
            ("精元符剑", "下品符器", "符纹藏锋，精元暗涌，一剑出鞘便惊灵台，提升4%攻击力！提升4%会心率！"),
            ("修士道袍", "下品符器", "随处可见的长袍，能略微抵挡真元冲击，提升8%减伤率！"),
            ("御灵盾", "上品玄器", "灵纹潜照，御气成垣，可拒外邪侵体而守心台，提升14%减伤率！"),
            ("阴磷甲", "下品玄器", "幽磷潜甲，似自阴渊剥鳞而成，寒意侵骨却能镇身护命，提升12%减伤率！"),
            ("化尘道袍", "上品符器", "披此袍如身隐尘外，符意流转，自护清虚道骨，提升10%减伤率！"),
        ]
        blocks = [f"名字：{name}\n品阶：{rank}\n效果：{description}\n拥有数量: 21 (绑定: 1)"
                  + ("\n※已装备※" if index in (0, 4) else "")
                  for index, (name, rank, description) in enumerate(definitions)]
        request = {"key": "equipment_info", "expected_shared_query": {
            "equipment_blocks": blocks, "equipped_count": 2}}
        self.assertEqual("success", fixture.classify_result(request, {"replies": [captured]})["outcome"])
        self.assertNotIn("group_id", fixture._reply_text({"replies": [captured]}))
        self.assertEqual("unclassified", fixture.classify_result(request, {
            "replies": [captured.replace("绑定: 1", "绑定: 0", 1)]})["outcome"])
        literal = r"商品描述包含\r与\n，不能替换"
        node = {"type": "node", "data": {"name": "我的装备", "uin": "900000", "content": literal}}
        self.assertEqual(literal, fixture._reply_text({"replies": [str([node])]}))
        self.assertEqual(literal, fixture._reply_text({"replies": [literal]}))
        envelope = {"group_id": 900100, "messages": [node]}
        self.assertEqual(literal, fixture._reply_text({"replies": [str(envelope)]}))

    def test_cq_mention_prefixed_skill_reply_remains_plain_text(self):
        request = next(row for row in fixture.request_plan(manifest(), "pure", 128)
                       if row["key"] == "skill_info")
        text = "[CQ:at,qq=900001]\r" + reply(request).replace("\n", "\r")
        self.assertEqual("success", fixture.classify_result(request, {"replies": [text]})["outcome"])
        self.assertEqual(text.replace("\r", "\n"), fixture._reply_text({"replies": [text]}))
        missing = text.replace(request["expected_shared_query"]["skill_lines"][0], "different", 1)
        self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [missing]})["outcome"])

    def test_unknown_large_or_executable_transport_envelopes_fail_closed(self):
        request = next(row for row in fixture.request_plan(manifest(), "pure", 128)
                       if row["key"] == "equipment_info")
        node = {"type": "node", "data": {"name": "我的装备", "uin": "900000", "content": reply(request)}}
        invalid = ["{'messages': []}", "[__import__('os').getcwd()]",
                   str({"group_id": 900100, "messages": [node], "unexpected": "metadata"}),
                   str([node] * (fixture.MAX_FORWARD_NODES + 1)),
                   str([{**node, "type": "image"}]),
                   str([{"type": "node", "data": {**node["data"], "content": {"text": "nested"}}}]),
                   "x" * (fixture.MAX_REPLY_BYTES + 1)]
        for text in invalid:
            with self.subTest(prefix=text[:60]):
                classified = fixture.classify_result(request, {"replies": [text]})
                self.assertEqual("unclassified", classified["outcome"])
                self.assertEqual("shared_query_invalid_transport_envelope", classified["reason"])


class SharedQuerySeedTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.paths = SimpleNamespace(game_db=Path(directory.name) / "game.db")
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                        "goods_num INTEGER,bind_num INTEGER,state INTEGER,create_time TEXT,update_time TEXT,all_num INTEGER)")
            uow.execute("CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,main_buff INTEGER,sub_buff INTEGER,"
                        "sec_buff INTEGER,effect1_buff INTEGER,effect2_buff INTEGER,faqi_buff INTEGER,armor_buff INTEGER)")

    def test_snapshot_includes_nonparticipant_columns_and_bounds_rows(self):
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.execute("INSERT INTO BuffInfo(user_id,main_buff) VALUES('background',1)")
        before = fixture._snapshot(self.paths)
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.execute("UPDATE BuffInfo SET sub_buff=2 WHERE user_id='background'")
        self.assertNotEqual(before, fixture._snapshot(self.paths))
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.executemany("INSERT INTO back(user_id,goods_id) VALUES('background',?)", [(1,), (2,)])
        with patch.object(fixture, "MAX_TABLE_ROWS", 1), self.assertRaisesRegex(ValueError, "row bound"):
            fixture._snapshot(self.paths)

    def test_seed_populates_all_slots_and_equipment_without_changing_scores(self):
        saved = manifest()
        saved["feature_table_rows"], saved["limits"] = {}, []
        catalog, next_id = {}, 100
        for category, count in [(category, 1) for _, category, _ in fixture.SLOTS] + [("法器", 4), ("防具", 4)]:
            catalog[category] = []
            for index in range(count):
                catalog[category].append({"id": next_id, "definition": {
                    "name": f"{category}{index}", "item_type": category,
                    "type": "装备" if category in {"法器", "防具"} else "技能"}})
                next_id += 1
        with fixture.shared._uow(self.paths.game_db) as uow:
            uow.executemany("INSERT INTO BuffInfo(user_id) VALUES(?)", [(actor,) for actor in saved["actors"]])
        original_scores = deepcopy(saved["score_evidence"])
        with patch.object(fixture.player, "seed", return_value=saved), \
                patch.object(fixture, "_catalog", return_value=catalog), \
                patch.object(fixture, "_equipment_blocks", side_effect=lambda _, amount, bound: [
                    f"equipment {index} {amount} {bound}" for index in range(8)]):
            seeded = fixture.seed(SimpleNamespace(paths=self.paths), ())
        self.assertEqual(original_scores, seeded["score_evidence"])
        self.assertEqual("mixed-shared-query-v19", seeded["fixture_version"])
        self.assertEqual(128 * 13, seeded["shared_query"]["snapshot"]["back"]["rows"])
        self.assertEqual(128, seeded["shared_query"]["snapshot"]["BuffInfo"]["rows"])
        self.assertEqual(seeded["inventory"], fixture.shared._inventory(self.paths.game_db, seeded["actors"]))
        with fixture.shared._uow(self.paths.game_db, read_only=True) as uow:
            rows = uow.query_all("SELECT * FROM BuffInfo")
            self.assertTrue(all(all(row[field] > 0 for field, _, _ in fixture.SLOTS) for row in rows))
            self.assertEqual({2}, {row["n"] for row in uow.query_all(
                "SELECT user_id,SUM(state) AS n FROM back GROUP BY user_id")})
        self.assertEqual(fixture._snapshot(self.paths), seeded["shared_query"]["snapshot"])
        self.assertTrue(any("not exercised" in line for line in seeded["limits"]))

    def test_check_invariants_preserves_parent_failures_and_detects_complete_table_writes(self):
        saved = manifest()
        saved["shared_query"]["snapshot"] = fixture._snapshot(self.paths)
        parent = {"ok": False, "errors": ["parent exact balance failed"], "invariants": "parent"}
        with patch.object(fixture.player, "check_invariants", return_value=parent):
            checked = fixture.check_invariants(SimpleNamespace(paths=self.paths), (), saved, [])
            self.assertFalse(checked["ok"])
            self.assertEqual(parent["errors"], checked["errors"])
            with fixture.shared._uow(self.paths.game_db) as uow:
                uow.execute("INSERT INTO BuffInfo(user_id,main_buff) VALUES('unrequested',7)")
            changed = fixture.check_invariants(SimpleNamespace(paths=self.paths), (), saved, [])
        self.assertIn("shared-query traffic changed complete inventory or BuffInfo tables", changed["errors"])


if __name__ == "__main__":
    unittest.main()
