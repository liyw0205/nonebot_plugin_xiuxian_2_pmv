import copy
import json
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import mixed_relation_fixture as fixture


def manifest():
    actors = [str(900_000 + index) for index in range(128)]
    groups = {"training_low": actors[:64], "preserved_background": actors[64:96],
              "exact_blob": actors[96:]}
    return {"actors": actors, "groups": groups, "relations": fixture._relation_layout(groups),
            "relation_profiles": {actor: {"user_id": actor, "user_name": f"Relation{index:03d}",
                                            "level": f"Realm{index % 4}"}
                                  for index, actor in enumerate(actors)},
            "relation_limits": {"max_apprentices": 5, "transmission_limit": 3},
            "expected_ranks": {actor: {"exp": index + 1, "stone": 128 - index}
                               for index, actor in enumerate(actors)}}


def reply_for(request):
    expected = request["expected_relation"]
    if request["key"] == "profile_info":
        children = expected["apprentices"]
        names = "、".join(child["user_name"] for child in children[:3]) or "无"
        if len(children) > 3:
            names += f"等{len(children)}人"
        mentor = expected["mentor"]["user_name"] if expected["mentor"] else "无"
        ranks = request["expected_ranks"]
        return (f"道号: {expected['actor']['user_name']}\n境界: {expected['actor']['level']}\n"
                f"修为排行: 第{ranks['exp']}位\n灵石排行: 第{ranks['stone']}位\n"
                f"关系：道侣：{expected['partner']['user_name']} (初识情愫)；师父：{mentor}；徒弟：{names}")
    if request["key"] == "my_partner":
        spouse = expected["partner"]
        return (f"**我的道侣**\n---\n道侣\n> {spouse['user_name']}\n境界\n> {spouse['level']}\n"
                f"修为\n> changing-live-exp\n**结契信息**\n- 绑定时间\n> {spouse['bind_time']}\n"
                f"- 亲密度\n> {spouse['affection']}（初识情愫）")
    master = expected["mentor"]
    mentor_line = (f"{master['user_name']}（{master['level']}，拜师时间：{fixture.strata.shared.ANCHOR}）"
                   if master else "无")
    children = "\n".join(f"- {child['user_name']}（{child['level']}）" for child in expected["apprentices"]) or "无"
    count = f"徒弟（{len(expected['apprentices'])}/{expected['max_apprentices']}）"
    limit = expected["transmission_limit"]
    if request["command"] in {"我的师父", "我的师傅"}:
        return f"【我的师父】\n师父：{mentor_line}\n\n今日剩余传功次数：{limit}/{limit}\n接受传功：0"
    if request["command"] == "我的徒弟":
        return (f"【我的徒弟】\n{count}：\n{children}\n待处理拜师申请：{expected['pending_count']}条\n"
                f"今日剩余传功次数：{limit}/{limit}")
    return (f"**我的师徒**\n师父\n> {mentor_line}\n\n{count}\n> {children}\n"
            f"待处理拜师申请\n> {expected['pending_count']}条\n今日剩余传功次数\n> {limit}/{limit}")


class RelationPlanTests(unittest.TestCase):
    def test_frozen_608_requests_cover_all_high_profiles_and_real_write_ratios(self):
        saved, numbers = manifest(), set()
        specs = fixture.phase_specs()
        self.assertEqual([(160, 10, 20), (256, 20, 35), (192, 20, 50)],
                         [(row["requests"], row["concurrency"], row["arrival_rate"]) for row in specs])
        self.assertEqual(608, sum(row["requests"] for row in specs))
        background = set(saved["groups"]["preserved_background"])
        for phase, spec in enumerate(specs):
            plan = fixture.request_plan(saved, spec["mode"], spec["requests"], phase)
            self.assertEqual(plan, fixture.request_plan(saved, spec["mode"], spec["requests"], phase))
            self.assertFalse(numbers & {row["number"] for row in plan})
            self.assertEqual(len(plan), len({row["number"] for row in plan}))
            numbers.update(row["number"] for row in plan)
            self.assertFalse(background & {row["actor"] for row in plan})
            writes = [row for row in plan if row["key"] == "stone_training"]
            self.assertEqual((0, 64, 96)[phase], len(writes))
            if phase == 1:
                self.assertEqual(set(saved["groups"]["training_low"]),
                                 {row["actor"] for row in writes})
                self.assertTrue(all((row["key"] == "stone_training") == (index % 4 == 3)
                                    for index, row in enumerate(plan)))
            if phase == 2:
                self.assertEqual({saved["groups"]["training_low"][0]}, {row["actor"] for row in writes})
                self.assertTrue(all((row["key"] == "stone_training") == (index % 2 == 1)
                                    for index, row in enumerate(plan)))
            profiles = [row for row in plan if row["key"] == "profile_info"]
            self.assertEqual(set(saved["groups"]["exact_blob"]), {row["actor"] for row in profiles})
            for row in profiles:
                self.assertEqual(saved["expected_ranks"][row["actor"]], row["expected_ranks"])
            if phase == 0:
                self.assertEqual(32, len(profiles))
                self.assertEqual({"我的师徒", "我的徒弟", "我的师父", "我的师傅"},
                                 {row["command"] for row in plan if row["key"] == "my_mentor"})
                mentors = [row for row in plan if row["key"] == "my_mentor"]
                self.assertTrue(any(row["expected_relation"]["mentor"] for row in mentors))
                self.assertTrue(any(len(row["expected_relation"]["apprentices"]) == 7 for row in mentors))
                self.assertEqual(set(saved["groups"]["exact_blob"]),
                                 {row["actor"] for row in mentors if row["expected_relation"]["mentor"] is None})

    def test_relation_layout_is_reciprocal_and_preserves_child_order(self):
        saved = manifest()
        layout, groups = saved["relations"], saved["groups"]
        high, low, background = (groups[name] for name in ("exact_blob", "training_low", "preserved_background"))
        counts = [len(layout["mentor"][actor]["apprentice_ids"]) for actor in high]
        self.assertEqual([7, 3] + [5] * 6 + [1] * 24, counts)
        children = [child for actor in high for child in layout["mentor"][actor]["apprentice_ids"]]
        self.assertEqual(low, children)
        self.assertEqual(64, len(set(children)))
        for master in high:
            for child in layout["mentor"][master]["apprentice_ids"]:
                self.assertEqual(master, layout["mentor"][child]["mentor_id"])
        for actor, row in layout["partner"].items():
            self.assertNotEqual(actor, row["partner_id"])
            self.assertEqual(actor, layout["partner"][row["partner_id"]]["partner_id"])
        for master, pupil in zip(high, low[:32]):
            self.assertEqual(pupil, layout["partner"][master]["partner_id"])
        for actor in background:
            self.assertIsNone(layout["mentor"][actor]["mentor_id"])
            self.assertEqual([], layout["mentor"][actor]["apprentice_ids"])
            self.assertIn(layout["partner"][actor]["partner_id"], background)
        damaged = copy.deepcopy(groups)
        damaged["exact_blob"][0] = low[0]
        with self.assertRaisesRegex(ValueError, "disjoint"):
            fixture._relation_layout(damaged)

    def test_invalid_plan_is_rejected(self):
        for mode, count, phase in (("pure", 159, 0), ("mixed", 256, 0), ("contention", 192, 3), ("pure", 160, -1)):
            with self.subTest(mode=mode, count=count, phase=phase), self.assertRaises(ValueError):
                fixture.request_plan(manifest(), mode, count, phase)

    def test_every_alias_and_role_has_exact_reply_checks(self):
        plan = fixture.request_plan(manifest(), "pure", 160)
        for row in plan:
            with self.subTest(key=row["key"], command=row["command"], actor=row["actor"]):
                reply = reply_for(row)
                self.assertEqual("success", fixture.classify_result(row, {"replies": [reply]})["outcome"])
                self.assertEqual("success", fixture.classify_result(row, {"replies": [reply.replace("\n", "\r")]})["outcome"])
                self.assertEqual("exception", fixture.classify_result(row, {"replies": [reply], "errors": ["failure"]})["outcome"])
                self.assertEqual("cooldown", fixture.classify_result(row, {"replies": ["慢...慢一..点"]})["outcome"])
                self.assertEqual("boundary", fixture.classify_result(row, {"blocked": ["outside"], "replies": [reply]})["outcome"])
        spouse = next(row for row in plan if row["key"] == "my_partner")
        text = reply_for(spouse)
        expectation = spouse["expected_relation"]["partner"]
        for altered in (text.replace(expectation["user_name"], "WrongPartner"),
                        text.replace(expectation["level"], "WrongRealm"),
                        text.replace(str(expectation["affection"]), "1"), text + "\n道侣\n> WrongPartner"):
            self.assertEqual("unclassified", fixture.classify_result(spouse, {"replies": [altered]})["outcome"])
        master = next(row for row in plan if row["key"] == "my_mentor" and len(row["expected_relation"]["apprentices"]) == 7)
        text = reply_for(master)
        first, second = master["expected_relation"]["apprentices"][:2]
        swapped = text.replace(first["user_name"], "SWAP").replace(second["user_name"], first["user_name"]).replace("SWAP", second["user_name"])
        for altered in (text.replace("徒弟（7/5）", "徒弟（5/5）"), swapped,
                        text.replace("128条", "0条"), text.replace(first["level"], "WrongRealm")):
            self.assertEqual("unclassified", fixture.classify_result(master, {"replies": [altered]})["outcome"])
        profile = next(row for row in plan if row["key"] == "profile_info")
        text = reply_for(profile)
        self.assertEqual("profile_relation_mismatch", fixture.classify_result(profile, {
            "replies": [text.replace("；师父：无；", "；师父：WrongMaster；")]})["reason"])
        self.assertEqual("exact_exp_rank_mismatch", fixture.classify_result(profile, {
            "replies": [text.replace(f"修为排行: 第{profile['expected_ranks']['exp']}位", "修为排行: 第999位")]})["reason"])
        write = next(row for row in fixture.request_plan(manifest(), "mixed", 256, 1) if row["key"] == "stone_training")
        self.assertEqual("business_rejection", fixture.classify_result(write, {"replies": ["你的灵石还不够呢"]})["outcome"])


class RelationIsolatedSeedTests(unittest.TestCase):
    def test_small_real_sqlite_seed_has_4096_existing_unique_pending_applicants(self):
        import nonebot
        nonebot.init()
        saved = manifest()
        with tempfile.TemporaryDirectory() as directory:
            paths = SimpleNamespace(game_db=Path(directory) / "game.db", player_db=Path(directory) / "player.db")
            applicants = [str(700_000 + index) for index in range(fixture.PENDING_APPLICATIONS)]
            with sqlite3.connect(paths.game_db) as connection:
                connection.execute("CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT UNIQUE)")
                connection.executemany("INSERT INTO user_xiuxian(user_id) VALUES(?)", [(actor,) for actor in saved["actors"] + applicants])
            fixture._write_relation_tables(paths, saved["relations"], saved["groups"]["exact_blob"])
            snapshot = fixture._relation_snapshot(paths)
            self.assertEqual(128, len(snapshot["mentor"]))
            self.assertEqual(128, len(snapshot["partner"]))
            self.assertEqual(4096, len(snapshot["mentor_applications"]))
            pending = snapshot["mentor_applications"]
            self.assertEqual(set(applicants), {row["apprentice_id"] for row in pending})
            self.assertEqual({"pending"}, {row["status"] for row in pending})
            self.assertEqual({fixture.PENDING_EXPIRES_AT}, {row["expires_at"] for row in pending})
            self.assertEqual(4096, len({row["created_at"] for row in pending}))
            for actor in saved["groups"]["exact_blob"]:
                self.assertEqual(128, sum(row["mentor_id"] == actor for row in pending))
            first = next(row for row in snapshot["mentor"] if row["user_id"] == saved["groups"]["exact_blob"][0])
            self.assertEqual(saved["groups"]["training_low"][:7], json.loads(first["apprentice_ids"]))
            with self.assertRaisesRegex(ValueError, "original application table"):
                fixture._write_relation_tables(paths, saved["relations"], saved["groups"]["exact_blob"])

    def test_json_seed_requires_probe_legacy_boundary_and_resets_singleton_after_restore(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "data"
            target = root / "xiuxian" / "diagnostic-legacy" / "mentor_exp_cd.json"
            target.parent.mkdir(parents=True)
            paths = SimpleNamespace(game_db=root / "xiuxian" / "game.db")
            store = SimpleNamespace(data_path=str(target), data={"old": 9})
            data = fixture._seed_mentor_cd(paths, ["a", "b"], store)
            self.assertEqual({"mentor_exp_cd": {"a": 0, "b": 0}}, data)
            self.assertEqual(data, json.loads(target.read_text(encoding="utf-8")))
            snapshot = fixture._json_snapshot(paths)
            store.data["mentor_exp_cd"]["a"] = 1
            target.write_text("{}", encoding="utf-8")
            self.assertNotEqual(snapshot, fixture._json_snapshot(paths))
            fixture._seed_mentor_cd(paths, ["a", "b"], store)
            self.assertEqual(data, store.data)
            self.assertEqual(snapshot, fixture._json_snapshot(paths))
            escaped = SimpleNamespace(data_path=str(root / "outside.json"))
            with self.assertRaisesRegex(ValueError, "outside isolated"):
                fixture._seed_mentor_cd(paths, ["a"], escaped)
            self.assertFalse((root / "outside.json").exists())


if __name__ == "__main__":
    unittest.main()
