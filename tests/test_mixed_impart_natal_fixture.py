"""Frozen V24 routing, independent reply/state oracles and drift failures."""
from collections import Counter
from copy import deepcopy
import ast
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import mixed_impart_natal_fixture as fixture


def manifest():
    actors = [str(900_000 + index) for index in range(fixture.PARTICIPANTS)]
    initial, users, impart, daily, natal = {}, {}, {}, {}, {}
    for index, actor in enumerate(actors):
        initial[actor] = {"exp": 832_000 + index * 256, "stone": 500_000_000 + index * 1_000_037}
        initial[actor]["power"] = initial[actor]["exp"] * 3
        users[actor] = {"id": index + 1, "user_id": actor, **initial[actor],
                        "root_type": "unchanged", "opaque": {"sqlite_blob_hex": "00ff"}}
        impart[actor] = {"id": index + 1, "user_id": actor, "impart_lv": 6, "exp_day": 600 + index,
                         "stone_num": 1e30, "stone_num_exact": str(10**30 + index),
                         "stone_num_precision_state": "exact", "impart_exp_up": 0}
        daily[actor] = {"user_id": actor, **fixture.DAILY_DEFAULTS}
        natal[actor] = {"user_id": actor, **fixture.NATAL_DEFAULTS, "name": f"SyntheticNatalV24{actor}",
                        "level": 1 + index % 3, "effect1_level": 1 + index % 3}
    saved = {"actors": actors, "initial": initial, "initial_users": users, "initial_impart": impart,
             "initial_daily": daily, "initial_natal": natal, "power_multiplier": 3., "exp_cap": 2_000_000,
             "training_rules": {"closing_exp": 100, "root_rate": 1.5, "realm_rate": 2., "main_rate": .2,
                                "main_closing": .1, "card_bonus": 0., "lv_bonus": 6 * .05},
             "natal_rules": {"max_level": 10, "max_effect": 10, "bleed_growth": .005,
                             "bleed_name": "流血", "periodic_base": .01, "periodic_growth": .005}}
    saved["training_oracles"] = {actor: fixture._training_oracle(saved, actor) for actor in actors}
    return saved


def reply(request):
    key = request["key"]
    if key in {"natal_info", "natal_exists"}:
        return request["expected_text"]
    if key == "world_info":
        return request["expected_texts"][0]
    if key == "world_training":
        return request["training"]["reply"]
    if key == "stone_training":
        return "修炼结束，共增加修为：100,消耗灵石：1000"
    return "【我的资产总览】\n灵石：" + request["wallet_choices"][0]


def samples(saved, mode="mixed", phase=1):
    results, world, stone, ledgers = [], [], [], []
    for request in fixture.request_plan(saved, mode, 128, phase):
        result = {"request": request, "replies": [reply(request)]}
        result["outcome"] = fixture.classify_result(request, result)["outcome"]
        results.append(result)
        if request["key"] not in {"world_training", "stone_training"}:
            continue
        operation, actor = fixture._operation(request), request["actor"]
        if request["key"] == "world_training":
            world.append({"operation_id": operation, "payload": json.dumps([actor, 1]),
                          "result_json": json.dumps(request["training"]["saved"])})
        else:
            data = {"exp_gain": 100, "stone_cost": 1000, "power": (saved["initial"][actor]["exp"] + 100) * 3}
            stone.append({"operation_id": operation,
                          "payload": json.dumps([actor, 1000, saved["exp_cap"], saved["power_multiplier"]]),
                          "result_json": json.dumps(data)})
            ledgers.append({"operation_id": operation, "status": "applied", "result_json": json.dumps({
                "operation_id": operation, "status": "applied", "action": "buff.stone_training", "data": data})})
    return results, world, stone, ledgers


class ImpartNatalFixtureTests(unittest.TestCase):
    def setUp(self):
        formatter = patch.object(fixture.bounded, "_number", side_effect=str)
        formatter.start()
        self.addCleanup(formatter.stop)

    def test_frozen_384_plan_has_304_success_80_refusals_and_64_real_writes(self):
        saved, numbers, cooldown_keys, writer_ids = manifest(), set(), set(), set()
        outcomes = Counter()
        self.assertEqual([(128, 10, 20), (128, 20, 20), (128, 20, 35)], [
            (row["requests"], row["concurrency"], row["arrival_rate"]) for row in fixture.phase_specs()])
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], 128, phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, phase))
            self.assertEqual(128, len(plan))
            ids = {request["number"] for request in plan}
            self.assertEqual(128, len(ids))
            self.assertFalse(ids & numbers)
            numbers |= ids
            counts = Counter(row["key"] for row in plan)
            self.assertEqual({"natal_info": 32, "natal_exists": 32, "world_info": 32, "asset_overview": 32}
                             if phase == 0 else {"natal_info": 24, "natal_exists": 24, "world_info": 32,
                                                "world_training": 16, "stone_training": 16, "asset_overview": 16}, counts)
            keys = {(row["actor"], row["key"]) for row in plan if row["key"].startswith("natal_")}
            self.assertEqual(counts["natal_info"] + counts["natal_exists"], len(keys))
            self.assertFalse(keys & cooldown_keys)
            cooldown_keys |= keys
            writes = [row for row in plan if row["key"] in {"world_training", "stone_training"}]
            writers = {row["actor"] for row in writes}
            self.assertEqual(0 if phase == 0 else 32, len(writers))
            self.assertEqual(len(writers), len(writes))
            self.assertFalse(writers & writer_ids)
            writer_ids |= writers
            if phase == 2:
                world_readers = Counter(row["actor"] for row in plan if row["key"] == "world_info")
                self.assertEqual({row["actor"] for row in writes if row["key"] == "world_training"}, set(world_readers))
                self.assertEqual({2}, set(world_readers.values()))
            phase_outcomes = Counter(fixture.classify_result(row, {"replies": [reply(row)]})["outcome"] for row in plan)
            self.assertEqual({"success": 96 if phase == 0 else 104,
                              "business_rejection": 32 if phase == 0 else 24}, phase_outcomes)
            outcomes.update(phase_outcomes)
        self.assertEqual(384, len(numbers))
        self.assertEqual(64, len(writer_ids))
        self.assertEqual({"success": 304, "business_rejection": 80}, outcomes)
        self.assertEqual(saved, manifest())

    def test_six_routes_resolve_to_one_real_matcher_and_handler(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        trees = [ast.parse((root / module / "__init__.py").read_text(encoding="utf-8")) for module in (
            "xiuxian_natal_treasure", "xiuxian_impart_pk", "xiuxian_base", "xiuxian_buff")]
        for _, command in fixture.COMMANDS:
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

    def test_invalid_plan_or_duplicate_actor_fails_before_dispatch(self):
        for mode, count, phase in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, 3), ("pure", 128, -1)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)
        for actors in (manifest()["actors"][:-1], ["same"] * 128):
            saved = manifest()
            saved["actors"] = actors
            with self.assertRaises(ValueError):
                fixture.request_plan(saved, "mixed", 128, 1)

    def test_independent_one_minute_training_oracle_keeps_real_gain_and_exact_time(self):
        saved = manifest()
        actor = saved["actors"][0]
        expected = fixture._training_oracle(saved, actor)
        self.assertEqual(514, expected["gain"])
        self.assertEqual((832_000 + 514) * 3, expected["power"])
        self.assertEqual([599, 832_514, 1, 1, 5, 514], expected["saved"])
        self.assertEqual("虚神界修炼结束，共修炼1分钟，本次增加修为：514（修炼效率：210%）\n"
                         "神魂承载：0% -> 5%（本次修炼+5%，实际+5%）\n今日虚神界修炼收益：514\n今日修炼次数：1次",
                         expected["reply"])
        saved["exp_cap"] = 832_003
        self.assertEqual(3, fixture._training_oracle(saved, actor)["gain"])
        saved["exp_cap"] = 832_000
        with self.assertRaises(ValueError):
            fixture._training_oracle(saved, actor)
        saved = manifest()
        saved["initial_impart"][actor]["exp_day"] = 0
        with self.assertRaises(ValueError):
            fixture._training_oracle(saved, actor)
        saved = manifest()
        saved["initial_daily"][actor]["exp_load"] = 5
        with self.assertRaises(ValueError):
            fixture._training_oracle(saved, actor)

    def test_natal_effect_and_virtual_crystals_have_complete_precise_reply_oracles(self):
        saved = manifest()
        actor = saved["actors"][1]
        self.assertEqual("【SyntheticNatalV24900001】  总等级：2/10\n  └─ 养成进度：17/100\n"
                         "  └─ 流血 Lv.2/10：3.0%\n\n  ◎ 道韵：每4回合对所有敌方造成当前生命 2.0% 的真实伤害。",
                         fixture._natal_text(saved["initial_natal"][actor], saved["natal_rules"]))
        text = fixture._world_info_text(saved["initial_impart"][actor], saved["initial_daily"][actor], saved["training_rules"])
        self.assertEqual("现位于：元神幻境（LV 6）\n虚神界修炼时间：601 分钟\n修炼效率：30% \n"
                         "今日神魂承载：0%/100%\n今日可探索次数：10\n今日可对决次数：7\n"
                         "思恋结晶：1000000000000000000000000000001", text)

    def test_full_replies_and_natal_refusal_reject_wrong_or_missing_fields(self):
        plan = fixture.request_plan(manifest(), "mixed", 128, 1)
        for key in dict(fixture.COMMANDS):
            request = next(row for row in plan if row["key"] == key)
            expected = "business_rejection" if key == "natal_exists" else "success"
            for newline in ("\n", "\r", "\r\n"):
                text = reply(request).replace("\n", newline)
                self.assertEqual(expected, fixture.classify_result(request, {"replies": [text]})["outcome"])
                self.assertEqual(expected, fixture.classify_result(request, {"replies": ["[CQ:at,qq=900001] " + text]})["outcome"])
            if key == "natal_exists":
                self.assertEqual("already_awakened", fixture.classify_result(request, {"replies": [reply(request)]})["reason"])
            for replies in ([], [""], ["wrong reply"], [reply(request), reply(request)]):
                self.assertEqual("unclassified", fixture.classify_result(request, {"replies": replies})["outcome"])
            if key != "stone_training":
                for text in (reply(request) + "\n" + reply(request), reply(request).replace(
                        "灵石：" if key == "asset_overview" else "100" if key.startswith("natal_") else "分钟", "wrong", 1)):
                    self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [text]})["outcome"])

    def test_contention_allows_only_complete_pre_or_post_training_views(self):
        request = next(row for row in fixture.request_plan(manifest(), "contention", 128, 2) if row["key"] == "world_info")
        self.assertEqual(2, len(request["expected_texts"]))
        for text in request["expected_texts"]:
            self.assertEqual("success", fixture.classify_result(request, {"replies": [text]})["outcome"])
        old, new = request["expected_texts"]
        invalid = old.replace("今日神魂承载：0%", "今日神魂承载：5%")
        self.assertNotEqual(invalid, new)
        for text in (invalid, old.replace("思恋结晶：", "思恋结晶：-1"), new.replace("今日可对决次数：7", "今日可对决次数：6")):
            self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [text]})["outcome"])

    def test_asset_display_accepts_equal_rounded_candidates_but_not_duplicate_lines(self):
        request = next(row for row in fixture.request_plan(manifest(), "contention", 128, 2)
                       if row["key"] == "asset_overview")
        for choices in (["6.12亿", "6.12亿"], ["6.12亿", "6.11亿"]):
            request["wallet_choices"] = choices
            for value in set(choices):
                text = "【我的资产总览】\n灵石：" + value + "\n背包：20 种 / 共 400 个"
                self.assertEqual("success", fixture.classify_result(request, {"replies": [text]})["outcome"])
                self.assertEqual("unclassified", fixture.classify_result(
                    request, {"replies": [text + "\n灵石：" + value]})["outcome"])
            both = "【我的资产总览】\n" + "\n".join("灵石：" + value for value in choices)
            self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [both]})["outcome"])
            self.assertEqual("unclassified", fixture.classify_result(
                request, {"replies": ["【我的资产总览】\n灵石：6.13亿"]})["outcome"])

    def test_boundary_errors_cooldown_and_replay_never_count_as_success(self):
        request = next(row for row in fixture.request_plan(manifest(), "mixed", 128, 1) if row["key"] == "world_training")
        for fields, expected in (({"blocked": ["boundary"]}, "boundary"), ({"errors": ["failure"]}, "exception"),
                                 ({"status": "timeout"}, "timeout"), ({"timed_out": True}, "timeout")):
            self.assertEqual(expected, fixture.classify_result(request, {"replies": [reply(request)], **fields})["outcome"])
        for text, expected in (("慢...慢一..点", "cooldown"), ("虚神界修炼结束（重放），无需重复提交。", "unclassified"),
                               ("累计时间不足，修炼失败!", "unclassified")):
            self.assertEqual(expected, fixture.classify_result(request, {"replies": [text]})["outcome"])

    def test_exact_receipts_change_only_declared_rows_and_preserve_other_values(self):
        saved = manifest()
        results, world, stone, ledgers = samples(saved)
        users, impart, daily, virtual_count, stone_count, errors = fixture._expected_changes(saved, results, world, stone, ledgers)
        self.assertEqual([], errors)
        self.assertEqual((16, 16), (virtual_count, stone_count))
        world_writers = {row["request"]["actor"] for row in results if row["request"]["key"] == "world_training"}
        stone_writers = {row["request"]["actor"] for row in results if row["request"]["key"] == "stone_training"}
        for actor in saved["actors"]:
            expected_user, expected_impart, expected_daily = (deepcopy(saved[key][actor]) for key in (
                "initial_users", "initial_impart", "initial_daily"))
            if actor in world_writers:
                expected_user.update(exp=expected_user["exp"] + 514, power=(expected_user["exp"] + 514) * 3)
                expected_impart["exp_day"] -= 1
                expected_daily.update(exp_used=1, exp_count=1, exp_load=5, exp_gain=514)
            elif actor in stone_writers:
                expected_user.update(exp=expected_user["exp"] + 100, stone=expected_user["stone"] - 1000,
                                     power=(expected_user["exp"] + 100) * 3)
            self.assertEqual(expected_user, users[actor])
            self.assertEqual(expected_impart, impart[actor])
            self.assertEqual(expected_daily, daily[actor])
        self.assertEqual(saved, manifest())

    def test_missing_extra_duplicate_or_wrong_receipt_and_forged_outcome_are_fatal(self):
        saved = manifest()
        mutations = ("missing_world", "extra_stone", "duplicate_world", "wrong_payload", "wrong_gain",
                     "wrong_ledger", "forged_outcome", "duplicate_request", "missing_ledger")
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                results, world, stone, ledgers = samples(saved)
                if mutation == "missing_world":
                    world.pop()
                elif mutation == "extra_stone":
                    stone.append({**stone[0], "operation_id": "training:extra"})
                elif mutation == "duplicate_world":
                    world.append(deepcopy(world[0]))
                elif mutation == "wrong_payload":
                    world[0]["payload"] = json.dumps([saved["actors"][0], 1])
                elif mutation == "wrong_gain":
                    data = json.loads(world[0]["result_json"])
                    data[-1] += 1
                    world[0]["result_json"] = json.dumps(data)
                elif mutation == "wrong_ledger":
                    data = json.loads(ledgers[0]["result_json"])
                    data["data"]["stone_cost"] = 0
                    ledgers[0]["result_json"] = json.dumps(data)
                elif mutation == "forged_outcome":
                    next(row for row in results if row["request"]["key"] == "natal_exists")["outcome"] = "success"
                elif mutation == "duplicate_request":
                    results.append(deepcopy(next(row for row in results if row["request"]["key"] == "world_training")))
                elif mutation == "missing_ledger":
                    ledgers.pop()
                self.assertTrue(fixture._expected_changes(saved, results, world, stone, ledgers)[-1])

    def test_full_row_snapshot_rejects_missing_and_duplicate_selected_rows(self):
        for rows in ([{"user_id": "one"}], [{"user_id": "one"}, {"user_id": "one"}]):
            with patch.object(fixture.shared, "_rows", return_value=rows), self.assertRaises(ValueError):
                fixture._full_rows("unused", "natal_treasure", ["one", "two"])
        rows = [{"user_id": "one", "opaque": b"\x00\xff"}, {"user_id": "two", "exact": "100000000000000000001"}]
        with patch.object(fixture.shared, "_rows", return_value=rows):
            self.assertEqual({"one": {"user_id": "one", "opaque": {"sqlite_blob_hex": "00ff"}}, "two": rows[1]},
                             fixture._full_rows("unused", "natal_treasure", ["one", "two"]))


if __name__ == "__main__":
    unittest.main()
