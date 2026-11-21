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

    def test_persisted_first_and_half_bonus_reach_final_combat_attributes(self):
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
        for quantity, expected_bonus, expected_attack in (
            (1, 0.1, 110),
            (2, 0.15, 115),
        ):
            with self.subTest(quantity=quantity):
                with db_backend.transaction(self.db) as conn:
                    conn.execute(
                        "UPDATE impart_cards SET quantity=%s WHERE card_name=%s",
                        (quantity, "攻击卡"),
                    )
                    bonuses = refresh_card_bonuses(
                        conn, "u", self.defs, schema="main"
                    )

                self.assertAlmostEqual(bonuses["impart_atk_per"], expected_bonus)
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
                self.assertEqual(attributes["final_atk"], expected_attack)

    def test_duplicate_bonus_accumulates_across_paid_draw_and_prayer(self):
        from nonebot_plugin_xiuxian_2.features.impart.application import ImpartApplication
        from nonebot_plugin_xiuxian_2.features.impart.migrations import (
            apply_impart_base_schema,
            apply_impart_draw_operations,
            apply_impart_draw_player_statistics,
            apply_impart_prayer_operations,
            apply_impart_prayer_player_statistics,
        )
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.xiuxian2_handle import (
            get_final_attributes,
        )

        root = Path(self.temp.name) / "cross-activity"
        root.mkdir()
        game, impart, player = (root / name for name in ("game.db", "impart.db", "player.db"))
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES(?,?)", ("u", 20_000_000))
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER,"
                "bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
            uow.execute("INSERT INTO back VALUES(?,?,?,?)", ("u", 20005, 1, 1))
            apply_impart_draw_operations(uow)
            apply_impart_prayer_operations(uow)
        with DatabaseUnitOfWork(impart) as uow:
            apply_impart_base_schema(uow)
            uow.execute(
                "INSERT INTO xiuxian_impart(user_id,stone_num,impart_num,wish) "
                "VALUES(?,?,?,?)",
                ("u", 0, 0, 0),
            )
        with DatabaseUnitOfWork(player) as uow:
            apply_impart_draw_player_statistics(uow)
            apply_impart_prayer_player_statistics(uow)

        definitions = {"攻击卡": {"type": "impart_atk_per", "vale": 0.1}}
        app = ImpartApplication(game, impart_database=impart, player_database=player)
        paid_draw = app.draw(
            operation_id="paid-draw",
            user_id="u",
            expected_stone=20_000_000,
            expected_wish=0,
            expected_count=0,
            cost=10_000_000,
            new_wish=10,
            pulls=1,
            cards=("攻击卡",),
            card_definitions=definitions,
            requested_pulls=1,
        )
        self.assertEqual(paid_draw.status, "applied")
        with DatabaseUnitOfWork(impart, read_only=True) as uow:
            first = uow.query_one(
                "SELECT c.quantity,i.impart_atk_per FROM impart_cards c "
                "JOIN xiuxian_impart i ON i.user_id=c.user_id "
                "WHERE c.user_id=? AND c.card_name=?",
                ("u", "攻击卡"),
            )
        self.assertEqual(int(first["quantity"]), 1)
        self.assertAlmostEqual(float(first["impart_atk_per"]), 0.1)

        prayer = app.prayer_settle(
            operation_id="prayer-draw",
            user_id="u",
            game_database=game,
            player_database=player,
            item_id=20005,
            quantity=1,
            cards=("攻击卡",),
            card_definitions=definitions,
        )
        self.assertEqual(prayer.status, "applied")
        with DatabaseUnitOfWork(impart, read_only=True) as uow:
            second = uow.query_one(
                "SELECT c.quantity,i.impart_atk_per FROM impart_cards c "
                "JOIN xiuxian_impart i ON i.user_id=c.user_id "
                "WHERE c.user_id=? AND c.card_name=?",
                ("u", "攻击卡"),
            )
        self.assertEqual(int(second["quantity"]), 2)
        self.assertAlmostEqual(float(second["impart_atk_per"]), 0.15)

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

        def read_impart(_user_id):
            with DatabaseUnitOfWork(impart, read_only=True) as uow:
                return uow.query_one(
                    "SELECT * FROM xiuxian_impart WHERE user_id=?", ("u",)
                )

        attributes = get_final_attributes(
            "u",
            base_provider=lambda _user_id: base,
            buff_info_provider=lambda _user_id: {},
            item_provider=lambda _item_id: {},
            impart_provider=read_impart,
            accessory_provider=lambda _user_id: {"equipped": {}},
            tianti_provider=lambda _user_id: {},
        )
        self.assertEqual(attributes["final_atk"], 115)
        for database in (game, impart, player):
            with DatabaseUnitOfWork(database, read_only=True) as uow:
                self.assertEqual(uow.query_one("PRAGMA integrity_check")["integrity_check"], "ok")

    def test_new_and_legacy_calculators_keep_the_25_card_cap(self):
        definition = {"攻击卡": {"type": "impart_atk_per", "vale": 0.1}}
        for quantity, expected in (
            (1, 0.1),
            (2, 0.15),
            (5, 0.3),
            (25, 1.3),
            (26, 1.3),
        ):
            with self.subTest(quantity=quantity):
                cards = {"攻击卡": quantity}
                new_value = calculate_card_bonuses(cards, definition)["impart_atk_per"]
                legacy_value = calculate_legacy_card_bonuses(cards, definition)[
                    "impart_atk_per"
                ]
                self.assertAlmostEqual(new_value, expected)
                self.assertAlmostEqual(legacy_value, expected)

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

    def test_all_bonus_fields_keep_half_values_in_the_actual_integer_affinity_schema(self):
        from nonebot_plugin_xiuxian_2.features.impart.migrations import (
            apply_impart_base_schema,
        )

        database = Path(self.temp.name) / "all-bonuses.db"
        definitions = {
            field: {"type": field, "vale": 1 if field in (
                "impart_two_exp", "impart_mix_per", "impart_reap_per"
            ) else 0.1}
            for field in BONUS_FIELDS
        }
        with DatabaseUnitOfWork(database) as uow:
            apply_impart_base_schema(uow)
            uow.execute("INSERT INTO xiuxian_impart(user_id) VALUES(?)", ("u",))
            for card_name in definitions:
                uow.execute(
                    "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES(?,?,?)",
                    ("u", card_name, 1),
                )

        for quantity in (1, 2):
            with self.subTest(quantity=quantity):
                with DatabaseUnitOfWork(database) as uow:
                    uow.execute("UPDATE impart_cards SET quantity=?", (quantity,))
                    refresh_card_bonuses(uow.connection, "u", definitions, schema="main")
                with DatabaseUnitOfWork(database, read_only=True) as uow:
                    state = uow.query_one("SELECT * FROM xiuxian_impart WHERE user_id=?", ("u",))
                for field in BONUS_FIELDS:
                    with self.subTest(field=field):
                        if field in ("impart_two_exp", "impart_mix_per", "impart_reap_per"):
                            expected = 1 if quantity == 1 else 1.5
                        else:
                            expected = 0.1 if quantity == 1 else 0.15
                        self.assertAlmostEqual(float(state[field]), expected)

        # A different card sharing the same attribute still has its own full first copy.
        definitions["second-attack-card"] = {"type": "impart_atk_per", "vale": 0.1}
        with DatabaseUnitOfWork(database) as uow:
            uow.execute(
                "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES(?,?,?)",
                ("u", "second-attack-card", 1),
            )
            refresh_card_bonuses(uow.connection, "u", definitions, schema="main")
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            state = uow.query_one("SELECT * FROM xiuxian_impart WHERE user_id=?", ("u",))
            self.assertAlmostEqual(float(state["impart_atk_per"]), 0.25)
            self.assertEqual(uow.query_one("PRAGMA integrity_check")["integrity_check"], "ok")

    def test_crystal_prayer_retains_the_26th_card_without_increasing_capped_bonus(self):
        from nonebot_plugin_xiuxian_2.features.impart.application import ImpartApplication
        from nonebot_plugin_xiuxian_2.features.impart.migrations import (
            apply_impart_base_schema,
            apply_impart_crystal_draw_operations,
            apply_impart_draw_player_statistics,
        )

        database = Path(self.temp.name) / "crystal-cap.db"
        player = Path(self.temp.name) / "crystal-cap-player.db"
        with DatabaseUnitOfWork(database) as uow:
            apply_impart_base_schema(uow)
            apply_impart_crystal_draw_operations(uow)
            uow.execute(
                "INSERT INTO xiuxian_impart(user_id,stone_num) VALUES(?,?)",
                ("u", 30),
            )
            uow.execute(
                "INSERT INTO impart_cards(user_id,card_name,quantity) VALUES(?,?,?)",
                ("u", "攻击卡", 24),
            )
            apply_impart_exact_crystal_balances(uow)
            refresh_card_bonuses(uow.connection, "u", self.defs, schema="main")
        with DatabaseUnitOfWork(player) as uow:
            apply_impart_draw_player_statistics(uow)
        app = ImpartApplication(database, impart_database=database, player_database=player)
        self.assertAlmostEqual(float(app.state("u")["impart_atk_per"]), 1.25)

        for quantity, balance in ((25, 30), (26, 20)):
            with self.subTest(quantity=quantity):
                request = dict(
                    operation_id=f"crystal-cap-{quantity}",
                    user_id="u",
                    expected_stone=balance,
                    expected_wish=0,
                    cost=10,
                    new_wish=0,
                    exp_minutes=120,
                    cards=("攻击卡",),
                    card_definitions=self.defs,
                    requested_pulls=1,
                )
                self.assertEqual(app.crystal_draw(**request).status, "applied")
                self.assertEqual(app.cards("u"), {"攻击卡": quantity})
                state = app.state("u")
                self.assertAlmostEqual(float(state["impart_atk_per"]), 1.3)
                self.assertEqual(state["stone_num"], balance - 10)
                self.assertEqual(int(state["exp_day"]), (quantity - 24) * 120)
                self.assertEqual(app.crystal_draw(**request).status, "duplicate")
                self.assertEqual(app.cards("u"), {"攻击卡": quantity})
                self.assertEqual(app.state("u"), state)

        for path in (database, player):
            with DatabaseUnitOfWork(path, read_only=True) as uow:
                self.assertEqual(uow.query_one("PRAGMA integrity_check")["integrity_check"], "ok")

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
        for cards in ({"攻击卡": 2}, ["攻击卡", "攻击卡"]):
            with self.subTest(cards=cards):
                with tempfile.TemporaryDirectory() as directory:
                    data_path = Path(directory) / "impart_person.json"
                    data_path.write_text(
                        json.dumps({"u": cards}, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    with db_backend.transaction(self.db) as conn:
                        conn.execute(
                            "DELETE FROM impart_cards WHERE user_id=%s", ("u",)
                        )
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
