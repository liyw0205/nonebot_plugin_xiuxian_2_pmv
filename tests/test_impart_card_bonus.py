import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import nonebot
nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import CardComposeService
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.transaction_service import CardDisassembleService
from nonebot_plugin_xiuxian_2.features.impart.card_bonus import (
    BONUS_FIELDS,
    calculate_card_bonuses,
    refresh_card_bonuses,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.card_bonus import (
    calculate_card_bonuses as calculate_legacy_card_bonuses,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.impart_uitls import (
    format_impart_percent,
    get_impart_card_description,
    re_impart_data,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.impart_data import IMPART_DATA
from nonebot_plugin_xiuxian_2.features.impart.migrations import (
    apply_impart_card_operations,
    apply_impart_exact_crystal_balances,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
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
        with DatabaseUnitOfWork(self.db) as uow:
            apply_impart_card_operations(uow)
            apply_impart_exact_crystal_balances(uow)
        self.defs = {"攻击卡": {"type": "impart_atk_per", "vale": 0.1}, "气血卡": {"type": "impart_hp_per", "vale": 0.2}}

    def tearDown(self):
        self.temp.cleanup()

    def bonuses(self):
        with db_backend.connection(self.db) as conn:
            return tuple(map(float, conn.execute("SELECT impart_atk_per,impart_hp_per FROM xiuxian_impart WHERE user_id=%s", ("u",)).fetchone()))

    def test_compose_refreshes_bonus_in_same_transaction(self):
        result = CardComposeService(self.db).compose("compose", "u", "攻击卡", "气血卡", 9, 4, 5, self.defs)
        self.assertTrue(result.succeeded)
        self.assertAlmostEqual(self.bonuses()[0], 0.25)
        self.assertAlmostEqual(self.bonuses()[1], 0.6)

    def test_disassemble_refreshes_bonus_in_same_transaction(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("UPDATE impart_cards SET quantity=6 WHERE card_name=%s", ("攻击卡",))
        result = CardDisassembleService(self.db).disassemble("split", "u", "攻击卡", 1, 6, 0, 2, self.defs)
        self.assertTrue(result.succeeded)
        self.assertAlmostEqual(self.bonuses()[0], 0.3)
        self.assertAlmostEqual(self.bonuses()[1], 0.5)

    def test_first_copy_is_full_and_second_copy_is_half_in_persisted_state(self):
        for quantity, expected in ((1, 0.1), (2, 0.15)):
            with self.subTest(quantity=quantity):
                with db_backend.transaction(self.db) as conn:
                    conn.execute(
                        "UPDATE impart_cards SET quantity=%s WHERE card_name=%s",
                        (quantity, "攻击卡"),
                    )
                    bonuses = refresh_card_bonuses(
                        conn, "u", self.defs, schema="main"
                    )
                self.assertAlmostEqual(bonuses["impart_atk_per"], expected)
                self.assertAlmostEqual(self.bonuses()[0], expected)

    def test_persisted_half_bonus_reaches_final_combat_attributes(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute(
                "UPDATE impart_cards SET quantity=%s WHERE card_name=%s",
                (2, "攻击卡"),
            )
            refresh_card_bonuses(conn, "u", self.defs, schema="main")

        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import (
            _int_after_float_math,
            get_final_attributes,
        )

        self.assertEqual(_int_after_float_math(114.99999999999999), 115)
        self.assertEqual(_int_after_float_math(114.9), 114)

        base = {
            "user_id": "u",
            "nickname": "测试玩家",
            "level": "江湖好手",
            "exp": 1000,
            "base_hp": 500,
            "base_mp": 500,
            "base_atk": 100,
            "atkpractice": 0,
            "hppractice": 0,
            "mppractice": 0,
        }
        attributes = get_final_attributes(
            "u",
            base_provider=lambda _user_id: base,
            buff_info_provider=lambda _user_id: {},
            item_provider=lambda _item_id: {},
            impart_provider=lambda _user_id: {
                "impart_atk_per": self.bonuses()[0]
            },
            accessory_provider=lambda _user_id: {"equipped": {}},
            tianti_provider=lambda _user_id: {},
        )

        self.assertEqual(attributes["final_atk"], 115)

    def test_new_and_legacy_calculators_keep_the_25_card_cap(self):
        definition = {"攻击卡": {"type": "impart_atk_per", "vale": 0.1}}
        for quantity in (25, 26):
            with self.subTest(quantity=quantity):
                cards = {"攻击卡": quantity}
                new_value = calculate_card_bonuses(cards, definition)["impart_atk_per"]
                legacy_value = calculate_legacy_card_bonuses(cards, definition)[
                    "impart_atk_per"
                ]
                self.assertAlmostEqual(new_value, 1.3)
                self.assertAlmostEqual(legacy_value, 1.3)

    def test_persisted_bonus_caps_at_25_without_clamping_card_inventory(self):
        for quantity in (25, 26):
            with self.subTest(quantity=quantity):
                with db_backend.transaction(self.db) as conn:
                    conn.execute(
                        "UPDATE impart_cards SET quantity=%s WHERE card_name=%s",
                        (quantity, "攻击卡"),
                    )
                    bonuses = refresh_card_bonuses(
                        conn, "u", self.defs, schema="main"
                    )
                with db_backend.connection(self.db) as conn:
                    stored_quantity = conn.execute(
                        "SELECT quantity FROM impart_cards WHERE user_id=%s AND card_name=%s",
                        ("u", "攻击卡"),
                    ).fetchone()[0]

                self.assertEqual(stored_quantity, quantity)
                self.assertAlmostEqual(bonuses["impart_atk_per"], 1.3)
                self.assertAlmostEqual(self.bonuses()[0], 1.3)

    def test_legacy_recalculation_uses_half_weight_for_duplicate_cards(self):
        data_manager = Mock()
        data_manager.data_person_list.return_value = {"攻击卡": 2}
        data_manager.data_all_.return_value = self.defs
        manager = Mock()
        with (
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.impart_uitls._impart_data_manager",
                return_value=data_manager,
            ),
            patch(
                "nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart.impart_uitls._impart_manager",
                return_value=manager,
            ),
        ):
            self.assertTrue(asyncio.run(re_impart_data("u")))
        self.assertAlmostEqual(
            manager.update_impart_atk_per.call_args.args[0], 0.15
        )
        self.assertEqual(manager.update_impart_atk_per.call_args.args[1], "u")

    def test_legacy_direct_card_add_refreshes_persisted_bonus(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("DELETE FROM impart_cards WHERE user_id=%s", ("u",))
            conn.execute(
                "UPDATE xiuxian_impart SET impart_atk_per=0 WHERE user_id=%s", ("u",)
            )

        manager = IMPART_DATA.__new__(IMPART_DATA)
        manager.conn = db_backend.connect(self.db)
        manager.cursor = manager.conn.cursor()
        manager.data_all = self.defs
        try:
            self.assertEqual(manager.data_person_add("u", "攻击卡"), (True, 1))
            self.assertAlmostEqual(self.bonuses()[0], 0.1)
            self.assertEqual(manager.data_person_add("u", "攻击卡"), (False, 2))
            self.assertAlmostEqual(self.bonuses()[0], 0.15)
        finally:
            manager.conn.close()

    def test_legacy_json_import_refreshes_existing_role_bonus(self):
        with tempfile.TemporaryDirectory() as directory:
            data_path = Path(directory) / "impart_person.json"
            data_path.write_text(
                json.dumps({"u": {"攻击卡": 2}}, ensure_ascii=False),
                encoding="utf-8",
            )
            with db_backend.transaction(self.db) as conn:
                conn.execute("DELETE FROM impart_cards WHERE user_id=%s", ("u",))
                conn.execute(
                    "UPDATE xiuxian_impart SET impart_atk_per=%s WHERE user_id=%s",
                    (0.2, "u"),
                )

            manager = IMPART_DATA.__new__(IMPART_DATA)
            manager.dir_path_person = Path(directory)
            manager.data_path_person = data_path
            manager.data_all = self.defs
            manager.conn = db_backend.connect(self.db)
            manager.cursor = manager.conn.cursor()
            try:
                manager._migrate_existing_data()
                self.assertAlmostEqual(self.bonuses()[0], 0.15)
                self.assertTrue(data_path.with_suffix(".json.backup").exists())
                with db_backend.connection(self.db) as conn:
                    self.assertEqual(
                        conn.execute(
                            "SELECT quantity FROM impart_cards "
                            "WHERE user_id=%s AND card_name=%s",
                            ("u", "攻击卡"),
                        ).fetchone()[0],
                        2,
                    )
            finally:
                manager.conn.close()

    def test_half_card_bonus_is_visible_in_percentage_and_card_description(self):
        self.assertEqual(format_impart_percent(0.025), "2.5%")
        self.assertEqual(format_impart_percent(0.1), "10%")
        self.assertEqual(
            get_impart_card_description("连翘"),
            "闭关经验提升：首张+5%，重复卡每张+2.5%",
        )

    def test_legacy_duplicate_player_rows_refresh_only_effective_row(self):
        bonus_columns = ",".join(f'"{field}" REAL' for field in BONUS_FIELDS)
        with db_backend.transaction(self.db) as conn:
            conn.execute("DROP TABLE xiuxian_impart")
            conn.execute(
                f"CREATE TABLE xiuxian_impart(user_id TEXT,stone_num INTEGER,{bonus_columns})"
            )
            conn.execute("INSERT INTO xiuxian_impart(user_id,stone_num) VALUES(%s,%s)", ("u", 0))
            conn.execute(
                "INSERT INTO xiuxian_impart(user_id,stone_num,impart_atk_per,impart_hp_per) VALUES(%s,%s,%s,%s)",
                ("u", 1, 0.7, 0.9),
            )
            refresh_card_bonuses(conn, "u", self.defs, schema="main")
            rows = conn.execute(
                "SELECT impart_atk_per,impart_hp_per FROM xiuxian_impart WHERE user_id=%s ORDER BY rowid",
                ("u",),
            ).fetchall()

        self.assertEqual(
            [tuple(map(float, row)) for row in rows], [(0.5, 0.5), (0.7, 0.9)]
        )

    def test_bonus_failure_rolls_back_card_change(self):
        with db_backend.transaction(self.db) as conn:
            conn.execute("DELETE FROM xiuxian_impart WHERE user_id=%s", ("u",))
        with self.assertRaises(ValueError):
            CardComposeService(self.db).compose("fail", "u", "攻击卡", "气血卡", 9, 4, 5, self.defs)
        with db_backend.connection(self.db) as conn:
            self.assertEqual(dict(conn.execute("SELECT card_name,quantity FROM impart_cards").fetchall()), {"攻击卡": 9, "气血卡": 4})


if __name__ == "__main__":
    unittest.main()
