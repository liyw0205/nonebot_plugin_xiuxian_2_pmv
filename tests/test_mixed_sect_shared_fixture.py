"""Frozen sect load plans, reply oracles and exact allowed-change checks."""
from collections import Counter
from copy import deepcopy
import ast
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import mixed_sect_shared_fixture as fixture


def manifest():
    actors = [str(900_000 + index) for index in range(fixture.PARTICIPANTS)]
    users, sects, oracle = {}, {}, {}
    for index, actor in enumerate(actors):
        sect_id = 8_950_001 + index % 4
        users[actor] = {"id": index + 1, "user_id": actor, "user_name": f"Actor{index}",
                        "stone": fixture.WALLET, "atkpractice": 0, "hppractice": 0, "mppractice": 0,
                        "sect_id": sect_id, "sect_position": 0 if index < 4 else fixture.MEMBER_POSITION,
                        "sect_contribution": 500_000_000, "exp": 800_000 + index,
                        "root_type": "unchanged", "opaque": b"raw unchanged data"}
        sects[str(sect_id)] = {"sect_id": sect_id, "sect_materials": fixture.MATERIALS,
                               "sect_name": f"Sect{index % 4}", "sect_scale": 4_000_000_000,
                               "opaque": "unchanged treasury metadata"}
        oracle[actor] = {
            "sect_id": sect_id, "sect_name": f"Sect{index % 4}", "user_name": f"Actor{index}",
            "owner_name": f"Actor{index % 4}", "owner": index < 4,
            "position": "Owner" if index < 4 else "Member", "rank": index % 4 + 1,
            "members": 64, "max_members": 100, "scale": "4000000000", "scale_level": 800,
            "contribution": "500000000", "treasury": "2000000000", "power": "100000",
            "task": "ActualTask", "task_desc": "Actual task description",
        }
    return {"actors": actors, "wallet": fixture.WALLET, "materials": fixture.MATERIALS,
            "rules": {"stone_cost": 5_000_000, "materials_cost": 50_000_000},
            "hot_groups": [actors[index::4] for index in range(4)], "oracle": oracle,
            "initial_users": {"actors": users, "background": {"rows": 8064, "sha256": "background"}},
            "initial_sects": sects, "initial_generation": {
                "generation": 10, "exp_generation": 20, "stone_generation": 30},
            "users": 1_000_000, "population_ids": list(range(1, 8193)), "period": "2026-11-27",
            "frozen": {"tables": {"game_db:sect_task_state": {"rows": 8192, "sha256": "tasks"}},
                       "exclusions": {"game_db:user_xiuxian": "explicit oracle"}}}


def reply(request):
    expected, key = request["expected_sect"], request["key"]
    if key in fixture.PRACTICES:
        label = fixture.PRACTICES[key][3]
        return (f"升级成功！\n道友当前{label}修炼等级\n> 1\n消耗灵石\n> {request['stone_cost']}枚\n"
                f"消耗宗门资材\n> {request['materials_cost']}")
    if key == "my_sect":
        fields = (("宗门名讳", expected["sect_name"]), ("宗门编号", expected["sect_id"]),
                  ("宗主", expected["owner_name"]), ("道友职位", expected["position"]),
                  ("宗门状态", "开放加入"),
                  ("宗门人数", f'{expected["members"]}/{expected["max_members"]}'),
                  ("宗门建设度", expected["scale"]), ("宗门排名", expected["rank"]),
                  ("宗门拥有资材", expected["materials_choices"][0]),
                  ("宗门贡献度", expected["contribution"]), ("宗门战力", expected["power"]),
                  ("炼体堂", "暂无"), ("宗门丹房", "暂无"))
        text = f'**{expected["user_name"]}所在宗门**\n' + "\n".join(f"{label}\n> {value}" for label, value in fields)
        return text + (f'\n宗门储备：{expected["treasury"]}枚灵石' if expected["owner"] else "")
    if key == "sect_buildings":
        return "\n".join(("【宗门建设】",
            f'宗门：{expected["sect_name"]}（{expected["members"]}/{expected["max_members"]}人）',
            f'建设度：{expected["scale"]}', f'宗门储备：{expected["treasury"]}灵石',
            f'宗门资材：{expected["materials_choices"][0]}', f'个人贡献：{expected["contribution"]}',
            "丹房：未建设", "炼体堂：未建设", f'修炼上限：{expected["scale_level"]}级',
            f'{expected["task"]}：{expected["task_desc"]}'))
    return "\n".join(("【我的资产总览】", f'灵石：{expected["wallet_choices"][0]}',
                      f'宗门：{expected["sect_name"]}（贡献 {expected["contribution"]}）'))


def receipt(request):
    return {"operation_id": fixture._operation(request), "user_id": request["actor"],
            "sect_id": request["sect_id"], "practice_type": fixture.PRACTICES[request["key"]][0],
            "from_level": 0, "to_level": 1, "stone_cost": request["stone_cost"],
            "materials_cost": request["materials_cost"]}


class SectSharedFixtureTests(unittest.TestCase):
    def setUp(self):
        # Parser and plan tests isolate the shared display formatter; real
        # matcher load uses the unpatched production number_to function.
        formatter = patch.object(fixture, "_number", side_effect=lambda value: str(value))
        formatter.start()
        self.addCleanup(formatter.stop)

    def test_frozen_phases_have_real_write_distribution_and_no_cross_phase_cd_collision(self):
        saved, numbers, cooldown_keys = manifest(), set(), set()
        self.assertEqual([(128, 10, 20), (128, 20, 35), (128, 20, 50)], [
            (row["requests"], row["concurrency"], row["arrival_rate"]) for row in fixture.phase_specs()])
        for index, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], 128, index)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 128, index))
            self.assertEqual(128, len(plan))
            self.assertEqual(128, len({row["number"] for row in plan}))
            self.assertFalse(numbers & {row["number"] for row in plan})
            numbers |= {row["number"] for row in plan}
            counts = Counter(row["key"] for row in plan)
            self.assertEqual(64 if index == 0 else 32, counts["my_sect"])
            self.assertEqual(64 if index == 0 else 32, counts["sect_buildings"])
            self.assertEqual(0 if index == 0 else 32, counts["asset_overview"])
            writes = [row for row in plan if row["key"] in fixture.PRACTICES]
            self.assertEqual(0 if index == 0 else 32, len(writes))
            keys = {(row["actor"], row["key"]) for row in writes}
            self.assertEqual(len(writes), len(keys))
            self.assertFalse(keys & cooldown_keys)
            cooldown_keys |= keys
            if index == 1:
                self.assertEqual(32, len({row["actor"] for row in writes}))
                self.assertEqual(4, len({row["sect_id"] for row in writes}))
            if index == 2:
                self.assertEqual(11, len({row["actor"] for row in writes}))
                self.assertEqual(1, len({row["sect_id"] for row in writes}))
                self.assertEqual([10, 11, 11], sorted(Counter(row["key"] for row in writes).values()))
            self.assertEqual(saved, manifest())
        self.assertEqual(384, len(numbers))

    def test_all_command_names_resolve_to_single_real_primary_matcher_and_handler(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        trees = [ast.parse((root / module / "__init__.py").read_text(encoding="utf-8"))
                 for module in ("xiuxian_sect", "xiuxian_base")]
        for _, command in fixture.COMMANDS:
            matches = []
            for tree in trees:
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

    def test_invalid_frozen_plan_and_populations_fail_before_dispatch(self):
        for mode, count, phase in (("pure", 127, 0), ("mixed", 128, 0), ("pure", 128, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)
        saved = manifest()
        saved["hot_groups"][0] = saved["hot_groups"][0][:-1]
        with self.assertRaises(ValueError):
            fixture.request_plan(saved, "contention", 128, 2)
        actors = [str(index) for index in range(1, 9)]
        population = actors + [str(index) for index in range(9, 17)]
        members = fixture._assign_members(actors, population, (1, 2, 3, 4), members_per_sect=4)
        self.assertEqual({actor for group in members.values() for actor in group}, set(population))
        self.assertEqual([4] * 4, list(map(len, members.values())))
        for args in ((actors, population[:-1], (1, 2, 3, 4)),
                     (actors, population, (1, 1, 3, 4)),
                     (actors, population[1:] + population[:1], (1, 2, 3, 4))):
            with self.assertRaises(ValueError):
                fixture._assign_members(*args, members_per_sect=4)

    def test_member_position_obeys_real_role_capacity_and_practice_eligibility(self):
        rules = json.loads((Path(__file__).parents[1] / "data/xiuxian/宗门玩法配置.json").read_text())
        self.assertEqual(1, rules["0"]["max_count"])
        self.assertEqual(0, rules[str(fixture.MEMBER_POSITION)]["max_count"])
        self.assertNotIn(fixture.MEMBER_POSITION, (12, 14, 15))
        self.assertTrue(all(row["sect_position"] in (0, fixture.MEMBER_POSITION)
                            for row in manifest()["initial_users"]["actors"].values()))

    def test_exact_reply_oracle_covers_six_commands_and_rejects_missing_or_wrong_values(self):
        plan = fixture.request_plan(manifest(), "mixed", 128, 1)
        samples = {row["key"]: row for row in plan}
        self.assertEqual(set(dict(fixture.COMMANDS)), set(samples))
        for key, request in samples.items():
            text = reply(request)
            for newline in ("\n", "\r", "\r\n"):
                normalized = text.replace("\n", newline)
                self.assertEqual("success", fixture.classify_result(request, {"replies": [normalized]})["outcome"])
            self.assertEqual("success", fixture.classify_result(request, {
                "replies": ["[CQ:at,qq=900001] " + text]})["outcome"])
            for damaged in (text + "\n" + text, text.replace(
                    "升级成功！" if key in fixture.PRACTICES else request["expected_sect"]["sect_name"], "wrong", 1)):
                self.assertEqual("unclassified", fixture.classify_result(request, {
                    "replies": [damaged]})["outcome"])
        request = samples["sect_buildings"]
        for old, new in (("64/100", "63/100"), ("ActualTask", "unknown task"),
                         ("修炼上限：800", "修炼上限：100")):
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [reply(request).replace(old, new)]})["outcome"])

    def test_permitted_balance_prefixes_do_not_require_atomic_cross_read_snapshots(self):
        plan = fixture.request_plan(manifest(), "contention", 128, 2)
        for key, label, choices_key in (("my_sect", "宗门拥有资材\n> ", "materials_choices"),
                                       ("sect_buildings", "宗门资材：", "materials_choices"),
                                       ("asset_overview", "灵石：", "wallet_choices")):
            request = next(row for row in plan if row["key"] == key)
            if key == "my_sect":
                # Choose a reader in the contended sect so every prefix exists.
                request = next(row for row in plan if row["key"] == key
                               and len(row["expected_sect"][choices_key]) > 1)
            choices = request["expected_sect"][choices_key]
            for value in choices:
                text = reply(request).replace(label + choices[0], label + value)
                self.assertEqual("success", fixture.classify_result(request, {"replies": [text]})["outcome"])
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [reply(request).replace(label + choices[0], label + "-1")]})["outcome"])

    def test_rejection_cooldown_exception_and_replay_never_count_as_success(self):
        request = next(row for row in fixture.request_plan(manifest(), "mixed", 128, 1)
                       if row["key"] in fixture.PRACTICES)
        for text, outcome in (("宗门资材不足，请补足宗门资材后操作。", "business_rejection"),
                              ("灵石余额不足，请补足灵石后再操作。", "business_rejection"),
                              ("所选修炼等级与升级前不一致", "business_rejection"),
                              ("慢...慢一..点", "cooldown"),
                              ("本次攻击修炼升级已经完成，请刷新状态。", "unclassified")):
            self.assertEqual(outcome, fixture.classify_result(request, {"replies": [text]})["outcome"])
        for fields, outcome in (({"errors": ["failure"]}, "exception"),
                                ({"blocked": ["boundary"]}, "boundary"), ({"status": "timeout"}, "timeout")):
            self.assertEqual(outcome, fixture.classify_result(request, {
                "replies": [reply(request)], **fields})["outcome"])

    def test_exact_receipts_account_for_shared_materials_and_multiple_fields_per_actor(self):
        saved = manifest()
        requests = [row for row in fixture.request_plan(saved, "contention", 128, 2)
                    if row["key"] in fixture.PRACTICES]
        results = [{"request": row, "outcome": "success"} for row in requests]
        users, sects, successes, errors = fixture._expected_changes(saved, results, list(map(receipt, requests)))
        self.assertEqual([], errors)
        self.assertEqual(32, successes)
        self.assertEqual(fixture.MATERIALS - 32 * 50_000_000,
                         sects[str(requests[0]["sect_id"])]["sect_materials"])
        actor = requests[0]["actor"]
        self.assertEqual(fixture.WALLET - 3 * 5_000_000, users[actor]["stone"])
        self.assertEqual([1, 1, 1], [users[actor][field] for field in fixture.MUTABLE_USER_FIELDS[1:]])
        self.assertEqual(saved, manifest())
        for change in ("missing", "extra", "cost", "user", "level", "rejected_committed"):
            rows, responses = list(map(receipt, requests)), deepcopy(results)
            if change == "missing":
                rows.pop()
            elif change == "extra":
                rows.append({**rows[0], "operation_id": "sect:895999999:unexpected:1"})
            elif change == "cost":
                rows[0]["stone_cost"] += 1
            elif change == "user":
                rows[0]["user_id"] = requests[1]["actor"] + "wrong"
            elif change == "level":
                rows[0]["to_level"] = 2
            else:
                responses[0]["outcome"] = "business_rejection"
            with self.subTest(change=change):
                self.assertTrue(fixture._expected_changes(saved, responses, rows)[3])

    def test_check_detects_lost_balance_updates_background_tasks_generation_and_period(self):
        saved = manifest()
        request = next(row for row in fixture.request_plan(saved, "mixed", 128, 1)
                       if row["key"] in fixture.PRACTICES)
        results, receipts = [{"request": request, "outcome": "success"}], [receipt(request)]
        users, sects, _, errors = fixture._expected_changes(saved, results, receipts)
        self.assertEqual([], errors)
        state = {"selected": {"actors": users, "background": saved["initial_users"]["background"]},
                 "sects": sects, "frozen": saved["frozen"], "generation": {
                     "generation": 11, "exp_generation": 20, "stone_generation": 31},
                 "period": saved["period"], "receipts": receipts, "count": 1_000_000}

        def check(current):
            def rows(database, sql, params=()):
                return [{"n": current["count"]}] if "COUNT(*)" in sql else current["receipts"]
            with patch.object(fixture.shared, "_rows", side_effect=rows), \
                    patch.object(fixture, "_selected_users", return_value=current["selected"]), \
                    patch.object(fixture, "_sects", return_value=current["sects"]), \
                    patch.object(fixture, "_frozen_tables", return_value=current["frozen"]), \
                    patch.object(fixture, "_generation", return_value=current["generation"]), \
                    patch.object(fixture, "_period", return_value=current["period"]):
                return fixture.check_invariants(SimpleNamespace(paths=SimpleNamespace(game_db="game")), (), saved, results)

        self.assertTrue(check(state)["ok"])
        for change in ("wallet", "nonmutable_actor", "background", "materials", "task", "generation", "period", "count"):
            current = deepcopy(state)
            if change == "wallet":
                current["selected"]["actors"][request["actor"]]["stone"] += 1
            elif change == "nonmutable_actor":
                current["selected"]["actors"][request["actor"]]["exp"] += 1
            elif change == "background":
                current["selected"]["background"]["sha256"] = "changed"
            elif change == "materials":
                current["sects"][str(request["sect_id"])]["sect_materials"] -= 1
            elif change == "task":
                current["frozen"]["tables"]["game_db:sect_task_state"]["sha256"] = "changed"
            elif change == "generation":
                current["generation"]["exp_generation"] += 1
            elif change == "period":
                current["period"] = "2026-11-28"
            else:
                current["count"] += 1
            with self.subTest(change=change):
                self.assertFalse(check(current)["ok"])

    def test_stream_hash_preserves_raw_types_columns_and_enforces_bound(self):
        class Cursor:
            description = (("value",),)
            def __init__(self, rows):
                self.rows, self.closed = list(rows), False
            def fetchmany(self, amount):
                result, self.rows = self.rows[:amount], self.rows[amount:]
                return result
            def close(self):
                self.closed = True

        signatures = []
        for value in (1, "1", b"1", 1.0):
            cursor = Cursor([{"value": value}])
            signatures.append(fixture._stream_signature(cursor)["sha256"])
            self.assertTrue(cursor.closed)
        self.assertEqual(4, len(set(signatures)))
        cursor = Cursor([{"value": 1}, {"value": 2}])
        self.assertEqual({"excluded": "row_bound", "rows_at_least": 2},
                         fixture._stream_signature(cursor, max_rows=1))
        self.assertTrue(cursor.closed)

    def test_selected_rows_preflight_rejects_ambiguous_background_money_then_accepts_exact_large_wallet(self):
        from nonebot_plugin_xiuxian_2.core.errors import DomainError
        from nonebot_plugin_xiuxian_2.core.numeric import bind_sqlite_param, require_exact_integer

        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "selected.db"
            with fixture.shared._uow(database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT,"
                            "stone INTEGER,sect_contribution INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES(1,'actor',1000000000,500000000)")
                uow.execute("INSERT INTO user_xiuxian VALUES(2,'background',?,500000000)", (1e30,))
            with self.assertRaises(DomainError) as failure:
                fixture._selected_users(database, [1, 2], ["actor"])
            self.assertEqual("stone_precision_lost_real", failure.exception.details["reason"])
            with fixture.shared._uow(database) as uow:
                uow.execute("UPDATE user_xiuxian SET stone=? WHERE id=2",
                            (bind_sqlite_param(fixture.BACKGROUND_WALLET + 1, exact=True),))
            snapshot = fixture._selected_users(database, [1, 2], ["actor"])
            self.assertEqual(1, snapshot["background"]["rows"])
            row = fixture.shared._rows(database, "SELECT stone FROM user_xiuxian WHERE id=2")[0]
            self.assertEqual(fixture.BACKGROUND_WALLET + 1,
                             require_exact_integer(row["stone"], field="stone", subject="test"))

    def test_manifest_blob_values_are_explicit_json_data_and_keep_exact_bytes(self):
        row = {"integer": 10**50, "text": "123", "blob": b"123\x00\xff", "real": 1.25, "null": None}
        encoded = fixture._json_row(row)
        self.assertEqual(encoded, json.loads(json.dumps(encoded)))
        self.assertEqual(row, {key: fixture._raw_value(value) for key, value in encoded.items()})
        self.assertNotEqual(encoded["text"], encoded["blob"])


if __name__ == "__main__":
    unittest.main()
