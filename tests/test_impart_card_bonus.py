import tempfile
import unittest
from pathlib import Path

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.card_compose_service import CardComposeService
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.card_disassemble_service import CardDisassembleService
from tests.test_db_backend import db_backend


class CardBonusRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "impart.db"
        fields = "impart_two_exp REAL,impart_exp_up REAL,impart_atk_per REAL,impart_hp_per REAL,impart_mp_per REAL,boss_atk REAL,impart_know_per REAL,impart_burst_per REAL,impart_mix_per REAL,impart_reap_per REAL"
        with db_backend.transaction(self.db) as conn:
            conn.execute("CREATE TABLE impart_cards (user_id TEXT,card_name TEXT,quantity INTEGER,PRIMARY KEY(user_id,card_name))")
            conn.execute(f"CREATE TABLE xiuxian_impart (user_id TEXT PRIMARY KEY,stone_num INTEGER,{fields})")
            conn.execute("INSERT INTO impart_cards VALUES (%s,%s,%s)", ("u", "攻击卡", 9))
            conn.execute("INSERT INTO impart_cards VALUES (%s,%s,%s)", ("u", "气血卡", 4))
            conn.execute("INSERT INTO xiuxian_impart VALUES (%s,%s,0,0,99,99,0,0,0,0,0,0)", ("u", 0))
        self.defs = {"攻击卡": {"type": "impart_atk_per", "vale": 0.1}, "气血卡": {"type": "impart_hp_per", "vale": 0.2}}

    def tearDown(self):
        self.temp.cleanup()

    def bonuses(self):
        with db_backend.connection(self.db) as conn:
            return tuple(map(float, conn.execute("SELECT impart_atk_per,impart_hp_per FROM xiuxian_impart WHERE user_id=%s", ("u",)).fetchone()))

    def test_compose_refreshes_bonus_in_same_transaction(self):
        result = CardComposeService(self.db).compose("compose", "u", "攻击卡", "气血卡", 9, 4, 5, self.defs)
        self.assertTrue(result.succeeded)
        self.assertEqual(self.bonuses(), (0.1, 0.4))

    def test_disassemble_refreshes_bonus_in_same_transaction(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("UPDATE impart_cards SET quantity=6 WHERE card_name=%s", ("攻击卡",))
        result = CardDisassembleService(self.db).disassemble("split", "u", "攻击卡", 1, 6, 0, 2, self.defs)
        self.assertTrue(result.succeeded)
        self.assertEqual(self.bonuses(), (0.2, 0.2))

    def test_bonus_failure_rolls_back_card_change(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("DELETE FROM xiuxian_impart WHERE user_id=%s", ("u",))
        with self.assertRaises(ValueError):
            CardComposeService(self.db).compose("fail", "u", "攻击卡", "气血卡", 9, 4, 5, self.defs)
        with db_backend.connection(self.db) as conn:
            self.assertEqual(dict(conn.execute("SELECT card_name,quantity FROM impart_cards").fetchall()), {"攻击卡": 9, "气血卡": 4})


if __name__ == "__main__":
    unittest.main()
