import json
import tempfile
import unittest
from pathlib import Path
from ....infrastructure.database import DatabaseUnitOfWork
from ...map.migrations import apply_map_dongfu_status_schema
from ..harvest_repository import DongfuHarvestSqlRepository
from ..plant_slots import canonical_plant_slots, empty_plant_slot
from . import install_operation_schema
from tests.test_db_backend import db_backend

class DongfuHarvestRepositoryTests(unittest.TestCase):
    def test_harvest_replay_and_maturity_guard(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'; slots=[{'slot':1,'seed_id':1,'plant_finish':'2026-01-01 00:00:00'}]; expected=json.dumps(slots)
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)'); c.execute("INSERT INTO user_xiuxian VALUES('u')"); c.execute('CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))')
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plant_slots TEXT,planting INTEGER,plant_seed_id INTEGER,plant_start TEXT,plant_finish TEXT,harvest_settlement TEXT)'); c.execute("INSERT INTO dongfu_status VALUES('u',1,?,?,?,?,?,?)",(expected,1,1,'','2025-12-31 00:00:00',''))
            install_operation_schema(game)
            repo=DongfuHarvestSqlRepository(game,player); items=[{'id':2,'name':'果','type':'特殊物品','amount':1}]; first=repo.harvest('h','u',slots,[1],items,99,'2026-01-02 00:00:00'); dup=repo.harvest('h','u',slots,[1],items,99,'2026-01-02 00:00:00'); self.assertEqual((first.status,dup.status),('harvested','duplicate'))


class DongfuHarvestSnapshotRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game, self.player = root / "game.db", root / "player.db"
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u')")
        self.slots = [empty_plant_slot(index) for index in range(1, 4)]
        self.slots[0].update(
            {
                "seed_id": 21001,
                "seed_name": "青灵草种",
                "plant_start": "2026-01-01 00:00:00",
                "plant_finish": "2026-01-01 01:00:00",
            }
        )
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE dongfu_status(user_id TEXT PRIMARY KEY,built INTEGER,plot_count INTEGER)")
            uow.execute("INSERT INTO dongfu_status VALUES('u',1,3)")
            apply_map_dongfu_status_schema(uow)
            uow.execute(
                "UPDATE dongfu_status SET plant_slots=?,planting=1,plant_seed_id=21001,"
                "plant_start='2026-01-01 00:00:00',plant_finish='2026-01-01 01:00:00' "
                "WHERE user_id='u'",
                (canonical_plant_slots(self.slots),),
            )
        install_operation_schema(self.game)
        self.repository = DongfuHarvestSqlRepository(self.game, self.player)
        self.snapshot = {
            "expected_slots": self.slots,
            "failed_slots": [],
            "items": [{"id": 1, "name": "材料", "type": "材料", "amount": 2}],
            "slot_numbers": [1],
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def prepare(self, snapshot=None, expected_slots=None):
        return self.repository.prepare_snapshot(
            "u",
            self.slots if expected_slots is None else expected_slots,
            self.snapshot if snapshot is None else snapshot,
            base_plot_count=3,
            max_plot_count=6,
            fertilizer_max=3,
            seed_names={21001: "青灵草种"},
        )

    def read_status(self):
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            return uow.query_one(
                "SELECT plant_slots,planting,plant_seed_id,harvest_settlement "
                "FROM dongfu_status WHERE user_id='u'"
            )

    def test_prepare_reuses_first_snapshot_without_overwrite(self):
        first = self.prepare()
        replacement = dict(self.snapshot, items=[{"id": 2, "name": "other", "type": "材料", "amount": 9}])
        replay = self.prepare(replacement)

        self.assertEqual((first.status, replay.status), ("prepared", "existing"))
        self.assertEqual(replay.snapshot, self.snapshot)
        self.assertEqual(json.loads(self.read_status()["harvest_settlement"]), self.snapshot)

    def test_prepare_normalizes_legacy_plant_fields_before_freezing(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "UPDATE dongfu_status SET plant_slots='',planting=1,plant_seed_id=21001,"
                "plant_start='2026-01-01 00:00:00',plant_finish='2026-01-01 01:00:00' "
                "WHERE user_id='u'"
            )

        result = self.prepare()
        state = self.read_status()

        self.assertEqual(result.status, "prepared")
        self.assertEqual(json.loads(state["plant_slots"]), self.slots)
        self.assertEqual(state["planting"], 1)
        self.assertEqual(state["plant_seed_id"], 21001)

    def test_changed_slots_reject_snapshot_without_persisting_it(self):
        changed = list(self.slots)
        changed[1] = empty_plant_slot(2)
        changed[1]["seed_id"] = 21002
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(
                "UPDATE dongfu_status SET plant_slots=? WHERE user_id='u'",
                (canonical_plant_slots(changed),),
            )

        result = self.prepare()

        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.read_status()["harvest_settlement"], "")

    def test_invalid_existing_snapshot_is_not_replaced(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("UPDATE dongfu_status SET harvest_settlement='broken' WHERE user_id='u'")

        result = self.prepare()

        self.assertEqual(result.status, "snapshot_invalid")
        self.assertEqual(self.read_status()["harvest_settlement"], "broken")
