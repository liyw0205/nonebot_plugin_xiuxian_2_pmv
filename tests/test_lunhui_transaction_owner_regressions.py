from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_lunhui.transaction_service import (
    CultivationResetService,
    LunhuiRecallService,
    LunhuiSettlementService,
)
from tests.test_db_backend import db_backend


RECALL_TYPES = (
    ("main_buff", "retrieved_main", "main_buff", "主功法"),
    ("sub_buff", "retrieved_sub", "sub_buff", "辅修"),
    ("sec_buff", "retrieved_sec", "sec_buff", "神通"),
    ("effect1_buff", "retrieved_effect1", "effect1_buff", "身法"),
    ("effect2_buff", "retrieved_effect2", "effect2_buff", "瞳术"),
)


class LunhuiTransactionOwnerRegressions(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="lunhui-owner-")
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        self.impart = root / "impart.db"

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _create_statistics(database: Path, user_id: str, values: dict[str, int]) -> None:
        columns = ",".join(
            f"{db_backend.quote_ident(name)} INTEGER NOT NULL DEFAULT 0" for name in values
        )
        names = list(values)
        quoted_names = ",".join(db_backend.quote_ident(name) for name in names)
        placeholders = ",".join("%s" for _ in names)
        with db_backend.transaction(database) as conn:
            conn.execute(
                f"CREATE TABLE statistics(user_id TEXT PRIMARY KEY,{columns})"
            )
            conn.execute(
                f"INSERT INTO statistics(user_id,{quoted_names}) VALUES(%s,{placeholders})",
                (user_id, *values.values()),
            )

    @staticmethod
    def _statistics(database: Path, user_id: str, fields: tuple[str, ...]) -> tuple[int, ...]:
        projection = ",".join(db_backend.quote_ident(field) for field in fields)
        with db_backend.connection(database) as conn:
            row = conn.execute(
                f"SELECT {projection} FROM statistics WHERE user_id=%s", (user_id,)
            ).fetchone()
        return tuple(int(value or 0) for value in row)

    def _create_reset_state(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,level TEXT,exp INTEGER,"
                "level_up_rate INTEGER,power INTEGER,hp INTEGER,mp INTEGER,atk INTEGER)"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", "感气境中期", 900, 7, 99, 300, 500, 80),
            )

    def test_reset_increments_player_stat_once_across_operation_replay(self) -> None:
        self._create_reset_state()
        self._create_statistics(self.player, "u", {"自废修为次数": 4})
        service = CultivationResetService(self.game, player_database=self.player)

        first = service.reset("reset-1", "u", "感气境中期", 900)
        replay = service.reset("reset-1", "u", "感气境中期", 900)

        self.assertEqual(("applied", "duplicate"), (first.status, replay.status))
        self.assertEqual((5,), self._statistics(self.player, "u", ("自废修为次数",)))

    def test_reset_failure_rolls_back_game_and_player_stat(self) -> None:
        self._create_reset_state()
        self._create_statistics(self.player, "u", {"自废修为次数": 4})
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE cultivation_reset_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,reset_exp INTEGER)"
            )
            conn.execute(
                "CREATE TRIGGER reject_reset BEFORE INSERT ON cultivation_reset_operations "
                "BEGIN SELECT RAISE(ABORT,'injected reset failure'); END"
            )
        service = CultivationResetService(self.game, player_database=self.player)

        with self.assertRaises(Exception):
            service.reset("reset-fail", "u", "感气境中期", 900)

        self.assertEqual((4,), self._statistics(self.player, "u", ("自废修为次数",)))
        with db_backend.connection(self.game) as conn:
            row = conn.execute(
                "SELECT level,exp FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
        self.assertEqual(("感气境中期", 900), tuple(row))

    def _create_recall_state(self) -> None:
        game_columns = "user_id TEXT PRIMARY KEY," + ",".join(
            f"{db_backend.quote_ident(field)} INTEGER NOT NULL DEFAULT 0"
            for field in ("main_buff", "sub_buff", "sec_buff", "effect1_buff", "effect2_buff")
        )
        player_columns = "user_id TEXT PRIMARY KEY," + ",".join(
            f"{db_backend.quote_ident(field)} INTEGER NOT NULL DEFAULT 0"
            for field in (
                "main_buff", "sub_buff", "sec_buff", "effect1_buff", "effect2_buff",
                "retrieved_main", "retrieved_sub", "retrieved_sec", "retrieved_effect1",
                "retrieved_effect2",
            )
        )
        with db_backend.transaction(self.game) as conn:
            conn.execute(f"CREATE TABLE BuffInfo({game_columns})")
        with db_backend.transaction(self.player) as conn:
            conn.execute(f"CREATE TABLE reincarnation_memory({player_columns})")
        for index, (skill_type, retrieved_field, buff_field, _) in enumerate(RECALL_TYPES, 1):
            user_id = f"u-{skill_type}"
            skill_id = 100 + index
            game_fields = ("main_buff", "sub_buff", "sec_buff", "effect1_buff", "effect2_buff")
            game_values = [0] * len(game_fields)
            game_values[game_fields.index(buff_field)] = 0
            with db_backend.transaction(self.game) as conn:
                conn.execute(
                    f"INSERT INTO BuffInfo(user_id,{','.join(game_fields)}) "
                    f"VALUES(%s,{','.join('%s' for _ in game_fields)})",
                    (user_id, *game_values),
                )
            player_fields = (
                "main_buff", "sub_buff", "sec_buff", "effect1_buff", "effect2_buff",
                "retrieved_main", "retrieved_sub", "retrieved_sec", "retrieved_effect1",
                "retrieved_effect2",
            )
            player_values = [0] * len(player_fields)
            player_values[player_fields.index(skill_type)] = skill_id
            player_values[player_fields.index(retrieved_field)] = 0
            with db_backend.transaction(self.player) as conn:
                conn.execute(
                    f"INSERT INTO reincarnation_memory(user_id,{','.join(player_fields)}) "
                    f"VALUES(%s,{','.join('%s' for _ in player_fields)})",
                    (user_id, *player_values),
                )
            if index == 1:
                counters = {"回忆前世次数": 7}
                counters.update({f"回忆前世{label}": index for _, _, _, label in RECALL_TYPES})
                self._create_statistics(self.player, user_id, counters)
            else:
                with db_backend.transaction(self.player) as conn:
                    fields = list(f"回忆前世{label}" for _, _, _, label in RECALL_TYPES)
                    quoted = ",".join(db_backend.quote_ident(field) for field in fields)
                    conn.execute(
                        f"INSERT INTO statistics(user_id,{quoted}) "
                        f"VALUES(%s,{','.join('%s' for _ in fields)})",
                        (user_id, *([index] * len(fields))),
                    )
                with db_backend.transaction(self.player) as conn:
                    conn.execute(
                        "UPDATE statistics SET \"回忆前世次数\"=%s WHERE user_id=%s",
                        (7, user_id),
                    )

    def test_recall_increments_total_and_each_type_once_across_replay(self) -> None:
        self._create_recall_state()
        service = LunhuiRecallService(self.game, self.player)

        for index, (skill_type, retrieved_field, buff_field, label) in enumerate(RECALL_TYPES, 1):
            with self.subTest(skill_type=skill_type):
                user_id = f"u-{skill_type}"
                skill_id = 100 + index
                operation_id = f"recall-{skill_type}"
                first = service.recall(operation_id, user_id, skill_type, skill_id)
                replay = service.recall(operation_id, user_id, skill_type, skill_id + 99)

                self.assertEqual(("applied", "duplicate"), (first.status, replay.status))
                self.assertEqual(
                    (8, index + 1),
                    self._statistics(
                        self.player, user_id, ("回忆前世次数", f"回忆前世{label}")
                    ),
                )
                with db_backend.connection(self.player) as conn:
                    memory = conn.execute(
                        f"SELECT {db_backend.quote_ident(retrieved_field)} "
                        "FROM reincarnation_memory WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                with db_backend.connection(self.game) as conn:
                    buff = conn.execute(
                        f"SELECT {db_backend.quote_ident(buff_field)} FROM BuffInfo WHERE user_id=%s",
                        (user_id,),
                    ).fetchone()
                self.assertEqual((1,), tuple(memory))
                self.assertEqual((skill_id,), tuple(buff))

    def test_recall_failure_rolls_back_player_stats_and_game_effect(self) -> None:
        self._create_recall_state()
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE lunhui_recall_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,skill_id INTEGER)"
            )
            conn.execute(
                "CREATE TRIGGER reject_recall BEFORE INSERT ON lunhui_recall_operations "
                "BEGIN SELECT RAISE(ABORT,'injected recall failure'); END"
            )
        service = LunhuiRecallService(self.game, self.player)
        skill_type, retrieved_field, buff_field, label = RECALL_TYPES[0]
        user_id = f"u-{skill_type}"

        with self.assertRaises(Exception):
            service.recall("recall-fail", user_id, skill_type, 101)

        self.assertEqual(
            (7, 1),
            self._statistics(self.player, user_id, ("回忆前世次数", f"回忆前世{label}")),
        )
        with db_backend.connection(self.player) as conn:
            memory = conn.execute(
                f"SELECT {db_backend.quote_ident(retrieved_field)} "
                "FROM reincarnation_memory WHERE user_id=%s",
                (user_id,),
            ).fetchone()
        with db_backend.connection(self.game) as conn:
            buff = conn.execute(
                f"SELECT {db_backend.quote_ident(buff_field)} FROM BuffInfo WHERE user_id=%s",
                (user_id,),
            ).fetchone()
        self.assertEqual((0,), tuple(memory))
        self.assertEqual((0,), tuple(buff))

    def _create_settlement_state(self) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,level TEXT,"
                "exp INTEGER,stone INTEGER,level_up_rate INTEGER,root TEXT,root_type TEXT,"
                "root_level INTEGER,power INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,"
                "atkpractice INTEGER,hppractice INTEGER,mppractice INTEGER)"
            )
            conn.execute(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", "道友", "渡劫", 999, 200_000_000, 5, "旧根", "旧", 1, 9, 8, 7, 6, 5, 4, 3),
            )
            conn.execute(
                "CREATE TABLE BuffInfo(user_id TEXT PRIMARY KEY,main_buff INTEGER,sub_buff INTEGER,"
                "sec_buff INTEGER,effect1_buff INTEGER,effect2_buff INTEGER)"
            )
            conn.execute("INSERT INTO BuffInfo VALUES('u',1,2,3,4,5)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,bind_num INTEGER,all_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            conn.execute("INSERT INTO back VALUES('u',10,'丹','丹药',1,0,9)")
        with db_backend.transaction(self.impart) as conn:
            conn.execute(
                "CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER,stone_num INTEGER)"
            )
            conn.execute("INSERT INTO xiuxian_impart VALUES('u',12,250)")
        memory_fields = (
            "main_buff", "sub_buff", "sec_buff", "effect1_buff", "effect2_buff",
            "memory_level", "retrieved_main", "retrieved_sub", "retrieved_sec",
            "retrieved_effect1", "retrieved_effect2",
        )
        memory_columns = ",".join(
            f"{db_backend.quote_ident(field)} {'TEXT' if field == 'memory_level' else 'INTEGER'}"
            for field in memory_fields
        )
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                f"CREATE TABLE reincarnation_memory(user_id TEXT PRIMARY KEY,{memory_columns})"
            )
            conn.execute(
                "INSERT INTO reincarnation_memory VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                ("u", 9, 8, 7, 6, 5, "旧境界", 1, 1, 1, 1, 1),
            )
        self._create_statistics(
            self.player,
            "u",
            {"轮回次数": 2, "无限轮回次数": 4, "普通轮回次数": 5},
        )

    def _settlement_service(self) -> LunhuiSettlementService:
        return LunhuiSettlementService(self.game, self.player, self.impart)

    def _settle(self, service: LunhuiSettlementService, operation_id: str, *, stone: int = 200_000_000):
        return service.settle(
            operation_id,
            "u",
            "渡劫",
            9,
            "旧",
            20025,
            "灵根改名卡",
            expected_exp=999,
            expected_stone=stone,
            expected_root_level=1,
            expected_buffs={
                "main_buff": 1,
                "sub_buff": 2,
                "sec_buff": 3,
                "effect1_buff": 4,
                "effect2_buff": 5,
            },
            expected_impart_exp_day=12,
            expected_impart_stone=250,
            user_name="道友",
        )

    def test_settlement_atomically_saves_memory_and_increments_reincarnation_stats(self) -> None:
        self._create_settlement_state()
        service = self._settlement_service()

        first = self._settle(service, "settle-1")
        replay = self._settle(service, "settle-1", stone=1)

        self.assertEqual(("applied", "duplicate"), (first.status, replay.status))
        with db_backend.connection(self.player) as conn:
            memory = conn.execute(
                "SELECT main_buff,sub_buff,sec_buff,effect1_buff,effect2_buff,memory_level,"
                "retrieved_main,retrieved_sub,retrieved_sec,retrieved_effect1,retrieved_effect2 "
                "FROM reincarnation_memory WHERE user_id='u'"
            ).fetchone()
        self.assertEqual((1, 2, 3, 4, 5, "渡劫", 0, 0, 0, 0, 0), tuple(memory))
        self.assertEqual(
            (3, 5, 5),
            self._statistics(
                self.player, "u", ("轮回次数", "无限轮回次数", "普通轮回次数")
            ),
        )

    def test_settlement_failure_rolls_back_player_memory_and_statistics(self) -> None:
        self._create_settlement_state()
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE lunhui_settlement_operations("
                "operation_id TEXT PRIMARY KEY,payload TEXT,stone INTEGER,root_level INTEGER,"
                "wishing_stones INTEGER)"
            )
            conn.execute(
                "CREATE TRIGGER reject_settlement BEFORE INSERT ON lunhui_settlement_operations "
                "BEGIN SELECT RAISE(ABORT,'injected settlement failure'); END"
            )
        service = self._settlement_service()

        with self.assertRaises(Exception):
            self._settle(service, "settle-fail")

        with db_backend.connection(self.player) as conn:
            memory = conn.execute(
                "SELECT main_buff,sub_buff,sec_buff,effect1_buff,effect2_buff,memory_level,"
                "retrieved_main,retrieved_sub,retrieved_sec,retrieved_effect1,retrieved_effect2 "
                "FROM reincarnation_memory WHERE user_id='u'"
            ).fetchone()
        self.assertEqual((9, 8, 7, 6, 5, "旧境界", 1, 1, 1, 1, 1), tuple(memory))
        self.assertEqual(
            (2, 4, 5),
            self._statistics(
                self.player, "u", ("轮回次数", "无限轮回次数", "普通轮回次数")
            ),
        )
        with db_backend.connection(self.game) as conn:
            self.assertEqual(
                ("渡劫", 999, 200_000_000),
                tuple(conn.execute("SELECT level,exp,stone FROM user_xiuxian WHERE user_id='u'").fetchone()),
            )
        with db_backend.connection(self.impart) as conn:
            self.assertEqual(
                (12, 250),
                tuple(conn.execute("SELECT exp_day,stone_num FROM xiuxian_impart WHERE user_id='u'").fetchone()),
            )


if __name__ == "__main__":
    unittest.main()
