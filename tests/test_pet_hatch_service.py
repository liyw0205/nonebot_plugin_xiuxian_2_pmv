import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_pet.transaction_service import PetHatchService
from tests.test_db_backend import db_backend


class PetHatchServiceTests(unittest.TestCase):
    def setUp(self):
        self.t = tempfile.TemporaryDirectory()
        r = Path(self.t.name)
        self.g = r / "g"
        self.p = r / "p"
        with db_backend.transaction(self.g) as c:
            c.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            c.execute("INSERT INTO user_xiuxian VALUES('u',100)")
        with db_backend.transaction(self.p) as c:
            c.execute(
                "CREATE TABLE player_pet("
                "user_id TEXT PRIMARY KEY,active_uid TEXT,"
                "egg_pity_count INTEGER,egg_pity_no_mythic_count INTEGER,travel TEXT)"
            )
            c.execute("INSERT INTO player_pet VALUES('u','',0,0,NULL)")
            c.execute(
                "CREATE TABLE player_pet_item("
                "id TEXT,user_id TEXT,uid TEXT,is_active INTEGER,pet_id TEXT,"
                "stars INTEGER,exp INTEGER,total_exp INTEGER,skill_id TEXT,"
                "created_at INTEGER,updated_at INTEGER)"
            )
        self.s = PetHatchService(self.g, self.p)
        self.pet = {
            "uid": "x",
            "pet_id": "1",
            "stars": 1,
            "exp": 0,
            "total_exp": 0,
            "skill": {},
        }

    def tearDown(self):
        self.t.cleanup()

    def call(self, op="o", stone=100, pet=None, meta=None):
        pet = pet or self.pet
        meta = meta if meta is not None else ["", 0, 0, None]
        return self.s.hatch(op, "u", stone, 10, meta, [(pet, True)], ["x", 1, 0], 10)

    def test_idempotent(self):
        first = self.call()
        self.assertEqual(first.status, "applied")
        other = {"uid": "y", "pet_id": "2", "stars": 1, "exp": 0, "total_exp": 0, "skill": {}}
        second = self.call(stone=90, pet=other)
        self.assertEqual(second.status, "duplicate")
        self.assertEqual(second.pets[0][0]["uid"], "x")

    def test_snapshot(self):
        self.assertEqual(self.call(op="s", stone=99).status, "state_changed")

    def test_travel_dict_vs_json_string_not_state_changed(self):
        """get_pet_doc 给 dict，DB 存 JSON 字符串时不应误报 state_changed。"""
        travel_json = (
            '{"pet_uid":"pet_1","pet_name":"天刑麟皇","pet_rarity":"神话",'
            '"pet_stars":10,"scene":"forage","scene_name":"觅食",'
            '"start_at":1,"end_at":2,"duration_hours":1}'
        )
        with db_backend.transaction(self.p) as c:
            c.execute(
                "UPDATE player_pet SET active_uid=%s,egg_pity_count=%s,"
                "egg_pity_no_mythic_count=%s,travel=%s WHERE user_id=%s",
                ("pet_1", 3, 1, travel_json, "u"),
            )
        import json

        travel_dict = json.loads(travel_json)
        result = self.call(
            op="travel-ok",
            meta=["pet_1", 3, 1, travel_dict],
        )
        self.assertEqual(result.status, "applied", result)

    def test_identity_conflict(self):
        self.assertEqual(self.call(op="c").status, "applied")
        r = self.s.hatch(
            "c",
            "u",
            100,
            20,
            ["", 0, 0, None],
            [(self.pet, True), ({**self.pet, "uid": "z"}, False)],
            ["x", 1, 0],
            10,
        )
        self.assertEqual(r.status, "state_changed")

    def test_rollback(self):
        with db_backend.transaction(self.g) as c:
            c.execute(
                "CREATE TABLE pet_hatch_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,created_at TEXT)"
            )
            c.execute(
                "CREATE TRIGGER f BEFORE INSERT ON pet_hatch_operations "
                "BEGIN SELECT RAISE(ABORT,'x');END"
            )
        with self.assertRaises(Exception):
            self.call()
        with db_backend.connection(self.g) as c:
            self.assertEqual(c.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 100)


if __name__ == "__main__":
    unittest.main()
