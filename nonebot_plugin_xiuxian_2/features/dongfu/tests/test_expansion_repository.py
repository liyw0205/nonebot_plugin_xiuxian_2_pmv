import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ...map.migrations import apply_map_dongfu_status_schema
from ..expansion_repository import DongfuExpansionSqlRepository
from . import install_operation_schema
from tests.test_db_backend import db_backend


class DongfuExpansionRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',100)")
            conn.execute("CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_num INTEGER)")
            conn.execute("INSERT INTO back VALUES('u',1,2)")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plot_count INTEGER)"
            )
            uow.execute("INSERT INTO dongfu_status VALUES('u',1,3)")
            apply_map_dongfu_status_schema(uow)
        install_operation_schema(self.game)
        self.repo = DongfuExpansionSqlRepository(self.game, self.player)

    def tearDown(self):
        self.temp_dir.cleanup()

    def player_row(self):
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            return uow.query_one(
                "SELECT plot_count,plant_slots,planting,plant_seed_id,plant_start,plant_finish "
                "FROM dongfu_status WHERE user_id='u'"
            )

    def game_balances(self):
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            return (
                uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"],
                uow.query_one("SELECT goods_num FROM back WHERE user_id='u'")["goods_num"],
            )

    def expand(self, operation_id):
        return self.repo.expand(
            operation_id, "u", 1, 3, 6, 20, seed_names={21001: "青灵草种"}
        )

    def test_expansion_updates_assets_and_slots_in_one_operation(self):
        first = self.expand("e")
        duplicate = self.expand("e")

        self.assertEqual((first.status, first.current_count), ("expanded", 4))
        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(self.game_balances(), (80, 1))
        row = self.player_row()
        slots = json.loads(row["plant_slots"])
        self.assertEqual(int(row["plot_count"]), 4)
        self.assertEqual(len(slots), 4)
        self.assertEqual(slots[-1], {
            "fertilizer": 0,
            "plant_finish": "",
            "plant_start": "",
            "seed_id": 0,
            "seed_name": "",
            "slot": 4,
        })

    def test_expansion_migrates_legacy_plant_into_first_slot(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "UPDATE dongfu_status SET planting=1,plant_seed_id=21001,"
                "plant_start='2026-01-01 00:00:00',plant_finish='2026-01-01 01:00:00' "
                "WHERE user_id='u'"
            )

        result = self.expand("legacy")
        row = self.player_row()
        slots = json.loads(row["plant_slots"])

        self.assertEqual(result.status, "expanded")
        self.assertEqual(len(slots), 4)
        self.assertEqual(
            (
                slots[0]["seed_id"],
                slots[0]["seed_name"],
                slots[0]["plant_start"],
                slots[0]["plant_finish"],
            ),
            (21001, "青灵草种", "2026-01-01 00:00:00", "2026-01-01 01:00:00"),
        )
        self.assertEqual(
            (row["planting"], row["plant_seed_id"], row["plant_start"], row["plant_finish"]),
            (1, 21001, "2026-01-01 00:00:00", "2026-01-01 01:00:00"),
        )

    def test_duplicate_repairs_slots_left_short_by_legacy_handler(self):
        self.expand("e")
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "UPDATE dongfu_status SET plant_slots='[]' WHERE user_id='u'"
            )

        duplicate = self.expand("e")

        self.assertEqual(duplicate.status, "duplicate")
        self.assertEqual(self.game_balances(), (80, 1))
        row = self.player_row()
        self.assertEqual(len(json.loads(row["plant_slots"])), 4)

    def test_slot_update_failure_rolls_back_assets_and_receipt(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "CREATE TRIGGER fail_slot_update BEFORE UPDATE OF plant_slots ON dongfu_status "
                "BEGIN SELECT RAISE(ABORT, 'slot write rejected'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.expand("e")

        self.assertEqual(self.game_balances(), (100, 2))
        self.assertEqual(int(self.player_row()["plot_count"]), 3)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(
                uow.query_one("SELECT COUNT(*) AS count FROM dongfu_expansion_operations")["count"],
                0,
            )

    def test_insufficient_assets_do_not_change_state(self):
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE back SET goods_num=0 WHERE user_id='u'")
        self.assertEqual(self.expand("deed").status, "deed_insufficient")

        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("UPDATE back SET goods_num=2 WHERE user_id='u'")
            uow.execute("UPDATE user_xiuxian SET stone=0 WHERE user_id='u'")
        self.assertEqual(self.expand("stone").status, "stone_insufficient")
        self.assertEqual(self.game_balances(), (0, 2))
        self.assertEqual(int(self.player_row()["plot_count"]), 3)


if __name__ == "__main__":
    unittest.main()
