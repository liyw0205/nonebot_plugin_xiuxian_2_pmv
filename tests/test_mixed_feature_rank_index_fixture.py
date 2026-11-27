from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import ast
import json
import tempfile
import unittest

from tests import mixed_feature_rank_index_fixture as fixture


def manifest():
    actors = [str(100_000 + index) for index in range(128)]
    identities = fixture.ranks._identities(datetime.now())
    seasons = {actor: [{"rank_key": key, "mode": mode, "period": period, "rank_type": kind,
        "value": 10 ** 40 + index if index < 8 else 20_000_000 + index * 10_000,
        "rank": index + 1} for key, mode, period, kind in identities]
        for index, actor in enumerate(actors)}
    boss = {"top": [{"user_id": str(index), "name": f"Boss{index}", "value": 10 ** 40 + index}
                    for index in range(50)],
            "actors": {actor: {"value": 30_000_000 + index * 10_000, "rank": index + 51, "total": 2048}
                       for index, actor in enumerate(actors)}}
    initial = {name + ":" + table: [] for name, tables in fixture.CHECKED_TABLES.items() for table in tables}
    rules = {"cost": 1000, "weekly_limit": 10, "item_name": "渡厄丹", "item_type": "丹药",
             "max_goods_num": 1000, "materials_rate": 1, "materials": 1000,
             "weekly_goal": "sect_supply", "weekly_target": 5_000_000,
             "purchase_day": datetime.now(timezone.utc).date().isoformat()}
    periods = {mode: period for _, mode, period, _ in identities}
    return {"actors": actors, "initial_users": {"actors": {
        actor: {"sect_id": 8950001 + index % 4} for index, actor in enumerate(actors)}},
        "feature_rank_index": {"rules": rules, "donors": actors[8:32], "buyers": actors[32:56],
            "oracle": {"season": seasons, "boss": boss}, "initial": initial,
            "population": {"identities": identities}, "weekly_table": "boss_weekly_purchases",
            "public": {"periods": periods, "top": [{"name": f"Power{index}", "value": 10**40 + index}
                                                       for index in range(10)]},
            "started_at": datetime.now().replace(microsecond=0).isoformat(sep=" ")}}


def reply(request, *, step=0):
    key, rules = request["key"], request["feature_rules"]
    if key == "sect_donate":
        return (f"道友捐献灵石1000枚，宗门建设度增加：1000，宗门资材增加：{rules['materials']}，"
                "宗门贡献度增加：1000点，蒸蒸日上！")
    if key == "boss_purchase":
        return "道友成功兑换获得：渡厄丹1个"
    if key == "my_season_rank":
        lines, last = ["【我的赛季】"], None
        labels = {"weekly": "周榜", "monthly": "月榜", "quarterly": "季度榜"}
        for row in request["expected_rank_projection"]:
            if row["mode"] != last:
                last = row["mode"]
                lines.append(f"【{row['period']}{labels[last]}】")
            value = row["values"][min(step, len(row["values"]) - 1)]
            lines.append(f"{row['rank_type']}：积分{fixture.sect._number(value)}，当前第{row['rank']}名")
        return "\n".join(lines)
    if key == "boss_integral_rank":
        expected = request["expected_rank_projection"]
        value = expected["values"][min(step, len(expected["values"]) - 1)]
        return "\n".join(["【世界BOSS积分排行榜】"] + [
            f"第{index}位 | {row['name']} | {fixture.sect._number(row['value'])}"
            for index, row in enumerate(expected["top"], 1)] + [
            f"你的名次：第{expected['rank']}名 / 共{expected['total']}人（世界积分 {fixture.sect._number(value)}点）",
            "公共榜单快照截至 2026-11-28 00:00:00 UTC（最长缓存 60 分钟）；个人名次与资格按实时数据计算。"])
    expected = request["expected_public"]
    periods = expected["periods"]
    return "\n".join(["【赛季榜】", f"本期榜单：{periods['monthly']}月榜", f"周榜：{periods['weekly']}",
        f"月榜：{periods['monthly']}", f"季度榜：{periods['quarterly']}",
        f"【{periods['monthly']}月榜战力榜 前十】"] + [
        f"{index}. {row['name']} 积分{fixture.sect._number(row['value'])}"
        for index, row in enumerate(expected["top"], 1)] + [
        "可发送：赛季榜、赛季榜 周榜、赛季榜 月榜、赛季榜 季度榜、赛季榜 周榜 战力、我的赛季",
        "可看榜单：交易活跃、讨伐、宗门贡献、试炼、战力。"])


def settled_donation(saved, request):
    from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
    from nonebot_plugin_xiuxian_2.infrastructure.database.ledger import request_hash

    current, rules = deepcopy(saved["feature_rank_index"]["initial"]), request["feature_rules"]
    operation = fixture._operation(request)
    payload = {"user_id": request["actor"], "sect_id": request["sect_id"], "stone": 1000,
               "materials": rules["materials"]}
    outcome = OperationOutcome.applied(operation, "sect.donate", data={"status": "donated", **payload},
                                       audit_category="sect").to_dict()
    current["game_db:sect_donation_operations"].append({"operation_id": operation, **payload})
    current["game_db:operation_ledger"].append({"operation_id": operation, "action": "sect.donate",
        "status": "applied", "request_hash": request_hash(payload), "result_json": json.dumps(outcome)})
    current["game_db:operation_audit"].append({"audit_id": 1, "operation_id": operation,
        "action": "sect.donate", "status": "applied", "category": "sect", **{
        key + "_json": "{}" for key in ("before", "after", "consumed", "granted")}})
    current["game_db:economy_log"].append({"id": 1, "user_id": request["actor"],
        "sect_id": request["sect_id"], "source": "sect", "action": "donate", "stone_delta": -1000,
        "exp_delta": 0, "sect_contribution_delta": 1000, "sect_scale_delta": 1000,
        "sect_materials_delta": rules["materials"], "trace_id": None, "event_id": None,
        "item_delta": "[]", "detail": json.dumps({"donate_stone": 1000, "materials_rate": 1}),
        "created_at": saved["feature_rank_index"]["started_at"]})
    return current


class FeatureRankPlanTests(unittest.TestCase):
    def test_frozen_plan_counts_disjoint_writers_and_reachable_read_candidates(self):
        saved, numbers = manifest(), set()
        for phase, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], 96, phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 96, phase))
            self.assertEqual(96, len(plan))
            counts = Counter(row["key"] for row in plan)
            self.assertEqual([32, 32, 32] if phase == 0 else [16, 16, 16],
                             [counts[key] for key, _ in fixture.COMMANDS[:3]])
            self.assertEqual([0, 0] if phase == 0 else [24, 24],
                             [counts[key] for key, _ in fixture.COMMANDS[3:]])
            writes = Counter((row["key"], row["actor"]) for row in plan if row["key"] in fixture.WRITE_KEYS)
            self.assertEqual({1} if phase == 1 else {4} if phase == 2 else set(), set(writes.values()))
            current = {row["number"] for row in plan}
            self.assertEqual(96, len(current))
            self.assertFalse(numbers & current)
            numbers |= current
            for request in plan:
                if request["key"] == "my_season_rank":
                    self.assertEqual(15, len(request["expected_rank_projection"]))
                    for row in request["expected_rank_projection"]:
                        count = writes[("sect_donate", request["actor"])] if row["rank_type"] == "宗门贡献" else 0
                        self.assertEqual([row["value"] + n * 1000 for n in range(count + 1)], row["values"])
                elif request["key"] == "boss_integral_rank":
                    expected = request["expected_rank_projection"]
                    self.assertEqual([expected["value"] - n * 1000
                        for n in range(writes[("boss_purchase", request["actor"])] + 1)], expected["values"])
        self.assertEqual(288, len(numbers))

    def test_registered_names_and_invalid_shapes_fail_before_dispatch(self):
        names = []
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        for module in ("xiuxian_base", "xiuxian_boss", "xiuxian_sect"):
            tree = ast.parse((root / module / "__init__.py").read_text(encoding="utf-8"))
            names += [node.args[0].value for node in ast.walk(tree) if isinstance(node, ast.Call)
                      and isinstance(node.func, ast.Name) and node.func.id == "on_command"
                      and node.args and isinstance(node.args[0], ast.Constant)]
        for _, command in fixture.COMMANDS:
            self.assertEqual(1, names.count(command), command)
        for mode, count, phase in (("pure", 95, 0), ("mixed", 96, 0), ("pure", 96, 3)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)
        saved = manifest()
        saved["feature_rank_index"]["buyers"][0] = saved["feature_rank_index"]["donors"][0]
        with self.assertRaises(ValueError):
            fixture.request_plan(saved, "pure", 96)

    def test_all_five_exact_replies_handle_transport_prefix_and_reject_corruption(self):
        plan = fixture.request_plan(manifest(), "contention", 96, 2)
        for key, _ in fixture.COMMANDS:
            request = next(row for row in plan if row["key"] == key)
            text = reply(request, step=4)
            for newline in ("\n", "\r\n", "\r"):
                payload = {"replies": [f"[CQ:at,qq={request['actor']}] " + text.replace("\n", newline)]}
                self.assertEqual("success", fixture.classify_result(request, payload)["outcome"], key)
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [text + "\nextra"]})["outcome"], key)
            self.assertEqual("unclassified", fixture.classify_result(request, {
                "replies": [text[:-2]]})["outcome"], key)

    def test_personal_rank_candidates_never_accept_changed_ranks_or_out_of_range_scores(self):
        plan = fixture.request_plan(manifest(), "contention", 96, 2)
        for key in ("my_season_rank", "boss_integral_rank"):
            request = next(row for row in plan if row["key"] == key)
            text = reply(request)
            expected = request["expected_rank_projection"]
            rank = expected[0]["rank"] if isinstance(expected, list) else expected["rank"]
            marker = f"当前第{rank}名" if isinstance(expected, list) else f"你的名次：第{rank}名"
            corrupt = text.replace(marker, marker.replace(str(rank), str(rank + 1)), 1)
            self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [corrupt]})["outcome"])
            bad = deepcopy(request)
            if isinstance(expected, list):
                bad["expected_rank_projection"][0]["values"] = [-1]
            else:
                bad["expected_rank_projection"]["values"] = [-1]
            self.assertEqual("unclassified", fixture.classify_result(bad, {"replies": [text]})["outcome"])

    def test_expected_cas_refusals_and_boundary_failures_stay_separate(self):
        request = next(row for row in fixture.request_plan(manifest(), "mixed", 96, 1)
                       if row["key"] == "boss_purchase")
        result = {"replies": ["兑换未完成：活动进度已更新，请重新兑换。"]}
        self.assertEqual({"outcome": "business_rejection", "reason": "purchase_state_changed"},
                         fixture.classify_result(request, result))
        for extra, outcome in (({"blocked": ["sqlite"]}, "boundary"), ({"status": "timeout"}, "timeout"),
                               ({"errors": ["failure"]}, "exception")):
            self.assertEqual(outcome, fixture.classify_result(request, {**result, **extra})["outcome"])
        for text in ("道友成功兑换获得：渡厄丹1个\n该兑换请求已经处理，无需重复提交。",
                     "该商品已到限购数量，无法继续兑换。", "兑换失败：世界积分不足。"):
            self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [text]})["outcome"])


class FeatureRankOracleTests(unittest.TestCase):
    def test_donation_event_keeps_user_subject_and_zero_sect_rank_identity(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import season_rank_service

        items = season_rank_service._build_event_score_items(
            "sect_donate", 1000, {"sect_id": 8950001, "source": "sect", "action": "donate"})
        self.assertEqual([{"rank_type": "宗门贡献", "score": 1000}], items)
        self.assertNotIn("sect_id", items[0])
        self.assertEqual(("100008", 0), season_rank_service._normalize_subject(user_id="100008", sect_id=None))

    def test_rank_interval_guards_reject_crossing_or_top_writers(self):
        saved = manifest()["feature_rank_index"]
        fixture._intervals(saved["oracle"], saved["donors"], saved["buyers"], saved["rules"])
        oracle = deepcopy(saved["oracle"])
        donor = saved["donors"][0]
        own = next(row for row in oracle["season"][donor] if row["rank_type"] == "宗门贡献")
        target = next(row for row in oracle["season"][saved["donors"][1]] if row["rank_key"] == own["rank_key"])
        target["value"] = own["value"] + 2000
        with self.assertRaisesRegex(ValueError, "donation"):
            fixture._intervals(oracle, saved["donors"], saved["buyers"], saved["rules"])
        oracle = deepcopy(saved["oracle"])
        oracle["boss"]["top"][0]["user_id"] = saved["buyers"][0]
        with self.assertRaisesRegex(ValueError, "TOP50"):
            fixture._intervals(oracle, saved["donors"], saved["buyers"], saved["rules"])

    def test_full_row_reader_is_bounded_and_preserves_blob_storage(self):
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / "small.db"
            with fixture.shared._uow(database) as uow:
                uow.execute("CREATE TABLE scores(owner TEXT PRIMARY KEY,value)")
                uow.execute("INSERT INTO scores VALUES('u',?)", (b"1000000000000000000000000",))
            self.assertEqual([{"owner": "u", "value": {"sqlite_blob_hex": b"1000000000000000000000000".hex()}}],
                             fixture._rows(database, "scores"))
            from unittest.mock import patch

            with patch.object(fixture.sect, "MAX_TABLE_ROWS", 0), self.assertRaisesRegex(ValueError, "bounded"):
                fixture._rows(database, "scores")
            with self.assertRaisesRegex(ValueError, "absent"):
                fixture._rows(database, "missing")

    def test_receipt_derived_donation_delta_keeps_ledger_audit_and_economy_independent(self):
        saved = manifest()
        request = next(row for row in fixture.request_plan(saved, "mixed", 96, 1)
                       if row["key"] == "sect_donate")
        current = settled_donation(saved, request)
        results = [{"request": request, "outcome": "success", "replies": [reply(request)]}]
        donors, buyers, sects, errors = fixture._write_oracle(saved, results, current)
        self.assertEqual([], errors)
        self.assertEqual(Counter({request["actor"]: 1}), donors)
        self.assertEqual(Counter(), buyers)
        self.assertEqual(Counter({str(request["sect_id"]): 1}), sects)
        corruptions = (("game_db:sect_donation_operations", "stone", 999),
                       ("game_db:operation_ledger", "request_hash", "wrong"),
                       ("game_db:operation_audit", "granted_json", '{"stone":1000}'),
                       ("game_db:economy_log", "stone_delta", -999))
        for table, column, value in corruptions:
            corrupted = deepcopy(current)
            corrupted[table][0][column] = value
            with self.subTest(table=table):
                self.assertTrue(fixture._write_oracle(saved, results, corrupted)[3])
        duplicate = deepcopy(current)
        duplicate["game_db:sect_donation_operations"].append(deepcopy(duplicate["game_db:sect_donation_operations"][0]))
        self.assertTrue(fixture._write_oracle(saved, results, duplicate)[3])

    def test_initial_empty_traffic_passes_exact_mutation_oracles(self):
        saved = manifest()
        initial = saved["feature_rank_index"]["initial"]
        self.assertEqual((Counter(), Counter(), Counter(), []), fixture._write_oracle(saved, [], initial))
        self.assertEqual([], fixture._expected_tables(saved, initial, Counter(), Counter(), Counter()))

    def test_prior_receipts_cannot_disappear_or_change(self):
        old = [{"operation_id": "old", "value": 1}]
        self.assertEqual([{"operation_id": "new", "value": 2}], fixture._additions(
            old, old + [{"operation_id": "new", "value": 2}], lambda row: row["operation_id"]))
        for rows in ([], [{"operation_id": "old", "value": 2}], old + old):
            with self.assertRaises(ValueError):
                fixture._additions(old, rows, lambda row: row["operation_id"])
