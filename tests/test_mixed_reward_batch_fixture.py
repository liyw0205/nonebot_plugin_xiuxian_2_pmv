"""Small plan/oracle/schema contracts; no retained fixture or load execution."""
from collections import Counter
import ast
import json
from pathlib import Path
import random
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from scripts import mixed_command_probe as probe
from tests import mixed_reward_batch_fixture as fixture


def manifest():
    ordinary = [{"item_id": item, "name": f"Item{item}", "item_type": "丹药", "quantity": amount}
                for item, amount in ((15103, 1), (15006, 2), (1998, 2))]
    definitions = {str(item): {"name": f"Item{item}", "type": "丹药"}
                   for item in (1999, 15103, 15006, 1998, 18050, 18126)}
    accessories = [{"id": 19001 + index, "name": f"Accessory{index}", "part": part, "set_type": "烈阳"}
                   for index, part in enumerate(("手镯", "戒指", "手环", "项链"))]
    return {"actors": [str(900_000 + index) for index in range(128)], "population_ids": [1, 2],
            "reward_batch": {"definitions": definitions, "ordinary_rewards": ordinary,
                "accessory_definitions": accessories,
                "package_names": {"ordinary_package": "感谢大礼包", "accessory_package": "一阶烈阳套装礼包"},
                "claim": {"candidates": ["15103", "15006", "1998"], "give_num": 6,
                          "contribution_required": 100, "materials_required": 200, "sect_ids": [1, 2, 3, 4]},
                "shop": [["编号：1", "名称：A", "价格：100贡献度", "每周限购：1/1个"]], "shop_pages": 1}}


def reply(request):
    key = request["key"]
    if key == "shop_view":
        return f"【宗门商店】第1/{request['expected_pages']}页\n" + "\n".join(
            line for row in request["expected_shop"] for line in row)
    if key == "elixir_claim":
        return request["expected_reply"]
    prefix = f"道友打开了 1 个 {request['package_name']}：\n"
    if key == "ordinary_package":
        return prefix + "\n".join(f"获得 {row['name']} x{row['quantity']}" for row in request["expected_rewards"])
    return prefix + "\n".join(f"获得饰品 {row['name']} x1（一阶）" for row in request["expected_rewards"])


class RewardBatchFixtureTests(unittest.TestCase):
    def test_frozen_three_phases_and_real_contention_have_sufficient_inventory(self):
        saved, numbers = manifest(), set()
        self.assertEqual([(96, 1, 10), (96, 10, 20), (96, 20, 35)], [
            (row["requests"], row["concurrency"], row["arrival_rate"]) for row in fixture.phase_specs()])
        for index, spec in enumerate(fixture.phase_specs()):
            plan = fixture.request_plan(saved, spec["mode"], 96, index)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], 96, index))
            identities = {row["number"] for row in plan}
            self.assertEqual(96, len(identities))
            self.assertFalse(numbers & identities)
            numbers |= identities
            self.assertEqual(Counter(shop_view=96) if index == 0 else Counter({key: 24 for key, _ in fixture.COMMANDS}),
                             Counter(row["key"] for row in plan))
            if index:
                claims = [row for row in plan if row["key"] == "elixir_claim"]
                self.assertEqual(24, len({row["actor"] for row in claims}))
                for key in fixture.PACKAGE_IDS:
                    counts = Counter(row["actor"] for row in plan if row["key"] == key)
                    self.assertEqual([4] * 6 if index == 2 else [1] * 24, sorted(counts.values()))
                    self.assertLessEqual(max(counts.values()), 8)
        self.assertEqual(288, len(numbers))
        for args in (("pure", 95, 0), ("mixed", 96, 0), ("pure", 96, 3)):
            with self.assertRaises(ValueError):
                fixture.request_plan(saved, *args)

    def test_matcher_names_resolve_statically_and_cli_selects_frozen_scenario(self):
        root = Path(__file__).parents[1] / "nonebot_plugin_xiuxian_2/xiuxian"
        trees = [ast.parse((root / name / "__init__.py").read_text(encoding="utf-8"))
                 for name in ("xiuxian_sect", "xiuxian_back")]
        for command in {value for _, value in fixture.COMMANDS}:
            assignments = [node for tree in trees for node in ast.walk(tree)
                           if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                           and isinstance(node.value.func, ast.Name) and node.value.func.id == "on_command"
                           and node.value.args and isinstance(node.value.args[0], ast.Constant)
                           and node.value.args[0].value == command]
            self.assertEqual(1, len(assignments), command)
        args = probe.parse_args(["--fixture", "/tmp/fixture", "--report", "/tmp/report",
                                 "--scenario", "reward-batch-v30", "--preflight-only"])
        self.assertEqual(128, args.participants)

    def test_reply_oracles_reject_missing_extra_or_wrong_rewards_and_preserve_failures(self):
        requests = {row["key"]: row for row in fixture.request_plan(manifest(), "mixed", 96, 1)}
        for request in requests.values():
            text = reply(request)
            self.assertEqual("success", fixture.classify_result(request, {"replies": [text]})["outcome"])
            self.assertEqual("success", fixture.classify_result(request, {"replies": ["[CQ:at,qq=900000] " + text]})["outcome"])
            forwarded = repr({"group_id": 10001, "messages": [
                {"type": "node", "data": {"name": "Bot", "uin": "10001", "content": text}}]})
            self.assertEqual("success", fixture.classify_result(request, {"replies": [forwarded]})["outcome"])
            for wrong in ("\n".join(text.splitlines()[:-1]), text + "\n" + text, text.replace("获得", "错误", 1)
                          if request["key"] != "shop_view" and request["key"] != "elixir_claim" else text + "\n编号：999"):
                self.assertEqual("unclassified", fixture.classify_result(request, {"replies": [wrong]})["outcome"])
        request = requests["elixir_claim"]
        for extra, expected in (({"errors": ["bad"]}, "exception"), ({"blocked": ["DB"]}, "boundary"),
                                ({"status": "timeout"}, "timeout")):
            self.assertEqual(expected, fixture.classify_result(request, {"replies": [reply(request)], **extra})["outcome"])
        for text, expected in (("道友已经领取过了", "business_rejection"), ("慢...慢一..点", "cooldown")):
            self.assertEqual(expected, fixture.classify_result(request, {"replies": [text]})["outcome"])

    def test_claim_oracle_preserves_candidate_order_and_request_random_draws(self):
        self.assertEqual(["3", "2"], fixture._claim_candidates({
            "1999": {"rank": 16}, "3": {"rank": "20"}, "broken": {}, "bad": {"rank": "x"},
            "2": {"rank": 56}, "low": {"rank": 15}, "high": {"rank": 57}}, 16))
        saved, number = manifest(), fixture.MESSAGE_BASE + 100_001
        rng = random.Random(f"{fixture.SEED_TAG}:{number}")
        expected = Counter(rng.choice(saved["reward_batch"]["claim"]["candidates"]) for _ in range(5))
        rewards = fixture._claim_rewards(saved, number)
        self.assertEqual(1, rewards[0][3])
        self.assertEqual(list(expected.items()), [(str(row[0]), row[3]) for row in rewards[1:]])
        self.assertEqual(6, sum(row[3] for row in rewards))
        self.assertEqual(rewards, fixture._claim_rewards(saved, number))

    def test_accessory_validation_keeps_real_random_shapes_and_ranges(self):
        definition = manifest()["reward_batch"]["accessory_definitions"][0]
        valid = fixture._initial_accessory("900000", 1, definition)
        self.assertTrue(fixture._valid_accessory(valid, definition))
        for delta in ({"uid": ""}, {"quality": 2}, {"item_id": 1}, {"locked_affixes": [1]},
                      {"affixes": [{"type": "速度", "value": 4.5}, {"type": "防御", "value": .01}]},
                      {"affixes": [{"type": "气血", "value": .02}] * 2}):
            self.assertFalse(fixture._valid_accessory({**valid, **delta}, definition))
        self.assertFalse(fixture._valid_accessory(None, definition))

    def test_small_sqlite_seed_initial_invariants_and_unexpected_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = SimpleNamespace(**{name: Path(directory) / (name + ".db")
                                       for name in ("game_db", "player_db", "impart_db")})
            saved = manifest()
            saved["actors"] = actors = saved["actors"][:2]
            with fixture.shared._uow(paths.game_db) as uow:
                uow.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT UNIQUE,level TEXT,"
                            "sect_id INTEGER,sect_elixir_get INTEGER,stone INTEGER,exp INTEGER)")
                for index, actor in enumerate(actors):
                    uow.execute("INSERT INTO user_xiuxian VALUES(?,?,?,1,0,100,200)", (index + 1, actor, "old"))
                uow.execute("CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,last_check_info_time TEXT DEFAULT '')")
                uow.execute("CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,elixir_room_level INTEGER)")
                uow.executemany("INSERT INTO sects VALUES(?,0)", ((value,) for value in range(1, 5)))
                uow.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                            "goods_num INTEGER,bind_num INTEGER,state INTEGER,all_num INTEGER,create_time TEXT,"
                            "update_time TEXT,UNIQUE(user_id,goods_id))")
                uow.execute("CREATE TABLE xiuxian_numeric_rank_generation(singleton INTEGER,generation INTEGER,"
                            "exp_generation INTEGER,stone_generation INTEGER)")
                uow.execute("INSERT INTO xiuxian_numeric_rank_generation VALUES(1,1,2,3)")
            with fixture.shared._uow(paths.impart_db):
                pass
            fixture._seed_rows(paths, actors, saved["reward_batch"])
            batch = saved["reward_batch"]
            batch.update(initial_back=fixture._all_rows(paths.game_db, "back"),
                initial_accessories=fixture._all_rows(paths.player_db, "player_accessory"),
                initial_users=fixture.sect._selected_users(paths.game_db, saved["population_ids"], actors),
                initial_sects=fixture.sect._sects(paths.game_db), initial_generation=fixture.sect._generation(paths.game_db),
                initial_cd=fixture.shared._rows(paths.game_db, "SELECT * FROM user_cd ORDER BY user_id"),
                frozen=fixture._frozen(paths))
            for database, table in (("game_db", "sect_elixir_claim_operations"), ("game_db", "package_reward_operations"),
                                    ("game_db", "accessory_package_operations"), ("player_db", "accessory_package_operations"),
                                    ("game_db", "operation_ledger"), ("game_db", "operation_audit"), ("game_db", "domain_outbox")):
                self.assertEqual([], fixture._receipt_rows(paths, database, table))
            with patch.object(fixture, "EXPECTED_USERS", 2):
                self.assertTrue(fixture.check_invariants(SimpleNamespace(paths=paths), [], saved, [])["ok"])
                with fixture.shared._uow(paths.game_db) as uow:
                    uow.execute("UPDATE back SET goods_num=goods_num+1 WHERE user_id=? AND goods_id=18050", (actors[0],))
                checked = fixture.check_invariants(SimpleNamespace(paths=paths), [], saved, [])
                self.assertFalse(checked["ok"])
                self.assertTrue(any("complete inventory" in error for error in checked["errors"]))

    def test_reward_sql_counters_include_entries_outside_top_twenty_and_exclude_other_work(self):
        statements = ["SELECT stone FROM user_xiuxian WHERE user_id = ?",
            "SELECT goods_num FROM back WHERE user_id = ? AND goods_id = ?",
            "SELECT COALESCE(goods_num,0) AS n,COALESCE(bind_num,0) AS b FROM back WHERE user_id=? AND goods_id=?",
            "SELECT b.goods_name,b.goods_type,b.goods_num,b.bind_num FROM back AS b WHERE user_id=? AND goods_id=?",
            "SELECT b.rowid AS _found,b.goods_num,b.bind_num FROM wanted LEFT JOIN back AS b ON b.rowid="
            "(SELECT rowid FROM back WHERE user_id=? AND goods_id=wanted.goods_id LIMIT 1)",
            "SELECT name FROM sqlite_master WHERE type='table'", "PRAGMA table_info(back)"]
        categories = ["wallet", "inventory_point", "inventory_point", "inventory_point", "inventory_batch", "snapshot_tables", "back_columns"]
        by_sql = {(str(index), f"SELECT {index}"): {"count": 1, "execute_ms": 1., "fetch_ms": 0.}
                  for index in range(25)}
        for sql, category in zip(statements, categories):
            self.assertEqual(category, probe.reward_batch_sql_category(sql))
            by_sql[("game", sql)] = {"count": 2, "execute_ms": 3., "fetch_ms": 1.}
        for sql in ("UPDATE back SET goods_num=goods_num-1 WHERE user_id=? AND goods_id=?",
                    "SELECT * FROM user_xiuxian WHERE user_id=?", "SELECT * FROM back WHERE user_id=?",
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='player_accessory'"):
            self.assertIsNone(probe.reward_batch_sql_category(sql))
            by_sql[("game", sql)] = {"count": 10, "execute_ms": 10., "fetch_ms": 0.}
        recorder = SimpleNamespace(by_sql=by_sql, snapshot=lambda: {"top_sql": list(by_sql)[:20]})
        metrics = probe.recorder_metrics(recorder)
        self.assertEqual({"statements": 6, "total_ms": 12.}, metrics["reward_batch_io"]["inventory_point"])
        for category in ("wallet", "inventory_batch", "snapshot_tables", "back_columns"):
            self.assertEqual({"statements": 2, "total_ms": 4.}, metrics["reward_batch_io"][category])
        self.assertEqual(10, metrics["live_read_io"]["full_profile"]["statements"])
