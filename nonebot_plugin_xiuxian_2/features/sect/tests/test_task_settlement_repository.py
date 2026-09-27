import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..application import SectApplication
from ..migrations import apply_sect_task_settlement_operations
from ..task_settlement_repository import SectTaskSettlementSqlRepository


class _Clock:
    def now(self):
        return datetime(2026, 9, 27, 12, 13, 14)


def _prepare_database(database: Path, *, apply_migration: bool = True) -> None:
    with DatabaseUnitOfWork(database, immediate=True) as uow:
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,"
            "stone INTEGER,hp INTEGER,exp INTEGER,sect_task INTEGER,sect_contribution INTEGER)"
        )
        uow.execute("INSERT INTO user_xiuxian VALUES('user',1,1000,500,2000,0,20)")
        uow.execute(
            "CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_used_stone INTEGER,"
            "sect_scale INTEGER,sect_materials INTEGER)"
        )
        uow.execute("INSERT INTO sects VALUES(1,50,100,200)")
        uow.execute(
            "CREATE TABLE sect_task_state(user_id TEXT,sect_id INTEGER,task_key TEXT,"
            "task_data TEXT,period TEXT,status TEXT,progress INTEGER,target INTEGER,"
            "updated_at TEXT,completed_at TEXT,PRIMARY KEY(user_id,period))"
        )
        uow.execute(
            "INSERT INTO sect_task_state VALUES('user',1,'trial','{}','p','accepted',0,1,'now',NULL)"
        )
        if apply_migration:
            apply_sect_task_settlement_operations(uow)


class SectTaskSettlementRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "sect.db"
        _prepare_database(self.database)

    def tearDown(self):
        self.temp.cleanup()

    def row(self, sql, params=()):
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            row = uow.query_one(sql, params)
            return tuple(row.values()) if row is not None else None

    def test_hp_and_stone_settlement_replay_atomically_with_injected_time(self):
        repository = SectTaskSettlementSqlRepository(self.database, clock=_Clock())
        first = repository.settle("op", "user", 1, "p", "hp", 100, 300, 25)
        duplicate = repository.settle("op", "user", 1, "p", "hp", 999, 999, 999)
        self.assertEqual((first["status"], duplicate["status"]), ("settled", "duplicate"))
        self.assertEqual(
            ("2026-09-27 12:13:14",),
            self.row("SELECT completed_at FROM sect_task_state WHERE period='p'"),
        )
        self.assertEqual((400, 2300, 45), self.row(
            "SELECT hp,exp,sect_contribution FROM user_xiuxian WHERE user_id='user'"
        ))

        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO sect_task_state VALUES('user',1,'trial','{}','p2','accepted',0,1,'now',NULL)"
            )
        stone = repository.settle("stone-op", "user", 1, "p2", "stone", 150, 50, 5)
        self.assertEqual("settled", stone["status"])
        self.assertEqual((850, 2350, 50), self.row(
            "SELECT stone,exp,sect_contribution FROM user_xiuxian WHERE user_id='user'"
        ))

    def test_missing_migration_returns_schema_missing_without_request_ddl(self):
        database = Path(self.temp.name) / "unmigrated.db"
        _prepare_database(database, apply_migration=False)

        result = SectTaskSettlementSqlRepository(database).settle(
            "op", "user", 1, "p", "hp", 100, 300, 25
        )

        self.assertEqual("schema_missing", result["status"])
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            table = uow.query_one(
                "SELECT 1 AS present FROM sqlite_master "
                "WHERE type='table' AND name='sect_task_settlement_operations'"
            )
        self.assertIsNone(table)

    def test_migration_is_idempotent_and_preserves_existing_receipts(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO sect_task_settlement_operations "
                "(operation_id,user_id,sect_id,period,cost_type,cost,exp_reward,"
                "sect_reward,materials_reward) VALUES('old','user',1,'p','hp',10,20,3,30)"
            )
            apply_sect_task_settlement_operations(uow)
        self.assertEqual(
            ("user", 1, "p", "hp", 10, 20, 3, 30),
            self.row(
                "SELECT user_id,sect_id,period,cost_type,cost,exp_reward,sect_reward,"
                "materials_reward FROM sect_task_settlement_operations WHERE operation_id='old'"
            ),
        )

    def test_receipt_failure_rolls_back_asset_and_task_updates(self):
        with DatabaseUnitOfWork(self.database, immediate=True) as uow:
            uow.execute(
                "CREATE TRIGGER fail_task_settlement BEFORE INSERT "
                "ON sect_task_settlement_operations "
                "BEGIN SELECT RAISE(ABORT,'settlement failed'); END"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            SectTaskSettlementSqlRepository(self.database, clock=_Clock()).settle(
                "fail", "user", 1, "p", "hp", 100, 300, 25
            )
        self.assertEqual((500, 2000, 0, 20), self.row(
            "SELECT hp,exp,sect_task,sect_contribution FROM user_xiuxian WHERE user_id='user'"
        ))
        self.assertEqual(("accepted", 0, None), self.row(
            "SELECT status,progress,completed_at FROM sect_task_state WHERE period='p'"
        ))

    def test_application_passes_its_clock_to_the_repository(self):
        result = SectApplication(self.database, clock=_Clock()).settle_task(
            "app-op", "user", 1, "p", "hp", 100, 300, 25
        )
        self.assertEqual("settled", result.status)
        self.assertEqual(
            ("2026-09-27 12:13:14",),
            self.row("SELECT completed_at FROM sect_task_state WHERE period='p'"),
        )

    def test_migration_is_game_database_only(self):
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        player = {item.version for item in migrations_for_database(migrations, "player_db")}
        self.assertIn("sect.018", game)
        self.assertNotIn("sect.018", player)
