import sqlite3
import tempfile, unittest
from pathlib import Path
from ..repository import DungeonSessionSqlRepository
from tests.test_db_backend import db_backend


class DungeonSessionRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.t=tempfile.TemporaryDirectory();self.db=Path(self.t.name)/'p.db'
        with db_backend.transaction(self.db) as c:
            c.execute('CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_status TEXT,current_layer INTEGER,total_layers INTEGER,last_reset_date TEXT,reset_generation INTEGER,reset_operation_id TEXT)');c.execute("INSERT INTO player_dungeon_status VALUES('u','d1','not_started',2,5,'2026-09-15',3,'r3')");c.execute('CREATE TABLE dungeon_session_operations(operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,dungeon_status TEXT)')
        self.r=DungeonSessionSqlRepository(self.db,self.db);self.expected={'dungeon_id':'d1','dungeon_status':'not_started','current_layer':2,'total_layers':5,'last_reset_date':'2026-09-15','reset_generation':3,'reset_operation_id':'r3'};self.dungeon={'dungeon_id':'d1','date':'2026-09-15'}
    def tearDown(self):self.t.cleanup()
    def test_enter_exit_and_replay(self):
        self.assertEqual('applied',self.r.session_transition('enter','u',self.expected,self.dungeon,'enter')['status']);exploring=dict(self.expected,dungeon_status='exploring');self.assertEqual('applied',self.r.session_transition('exit','u',exploring,self.dungeon,'exit')['status']);self.assertEqual('duplicate',self.r.operation_session_result('exit','u','exit')['status'])
    def test_aba_snapshot_rejected(self):
        stale=dict(self.expected,reset_generation=2);self.assertEqual('state_changed',self.r.session_transition('aba','u',stale,self.dungeon,'enter')['status'])

    def test_existing_receipt_replays_before_missing_live_schema(self):
        self.assertEqual(self.r.session_transition("enter", "u", self.expected, self.dungeon, "enter")["status"], "applied")
        with db_backend.transaction(self.db) as connection:
            connection.execute("DROP TABLE player_dungeon_status")
        self.assertEqual(self.r.operation_session_result("enter", "u", "enter")["status"], "duplicate")
        self.assertEqual(self.r.session_transition("enter", "u", {}, self.dungeon, "enter")["status"], "duplicate")
        self.assertEqual(self.r.session_transition("new", "u", {}, self.dungeon, "enter")["status"], "schema_missing")

    def test_omitted_generation_snapshot_cannot_bypass_zero_generation_guard(self):
        with db_backend.transaction(self.db) as connection:
            connection.execute("UPDATE player_dungeon_status SET reset_generation=0,reset_operation_id=''")
        expected = {key: value for key, value in self.expected.items() if key not in {"reset_generation", "reset_operation_id"}}
        self.assertEqual(self.r.session_transition("no-generation", "u", expected, self.dungeon, "enter")["status"], "state_changed")

    def test_missing_session_schema_fails_closed_without_ddl(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing.db"
            repository = DungeonSessionSqlRepository(database, database)
            result = repository.session_transition(
                "missing", "u", {}, {"dungeon_id": "d1", "date": "today"}, "exit"
            )
            replay = repository.operation_session_result("missing", "u", "exit")
            self.assertEqual(result["status"], "schema_missing")
            self.assertEqual(replay["status"], "schema_missing")
            self.assertFalse(database.exists())

    def test_partial_session_schema_fails_closed_without_creating_status_table(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "partial.db"
            with db_backend.transaction(database) as connection:
                connection.execute(
                    "CREATE TABLE dungeon_session_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,dungeon_status TEXT)"
                )
            result = DungeonSessionSqlRepository(database, database).session_transition(
                "partial", "u", {}, {"dungeon_id": "d1", "date": "today"}, "exit"
            )
            with db_backend.connection(database) as connection:
                status_table = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='player_dungeon_status'"
                ).fetchone()
            self.assertEqual(result["status"], "schema_missing")
            self.assertIsNone(status_table)

    def test_missing_status_column_fails_closed_without_alter(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "missing-column.db"
            with db_backend.transaction(database) as connection:
                connection.execute(
                    "CREATE TABLE player_dungeon_status("
                    "user_id TEXT PRIMARY KEY,dungeon_id TEXT,dungeon_status TEXT,"
                    "current_layer INTEGER,total_layers INTEGER)"
                )
                connection.execute(
                    "CREATE TABLE dungeon_session_operations("
                    "operation_id TEXT PRIMARY KEY,payload TEXT,result_status TEXT,dungeon_status TEXT)"
                )
            result = DungeonSessionSqlRepository(database, database).session_transition(
                "missing-column", "u", {}, {"dungeon_id": "d1", "date": "today"}, "exit"
            )
            with db_backend.connection(database) as connection:
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(player_dungeon_status)"
                    )
                }
            self.assertEqual(result["status"], "schema_missing")
            self.assertNotIn("last_reset_date", columns)

    def test_missing_generation_schema_is_read_only_and_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "legacy.db"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    "CREATE TABLE player_dungeon_status(user_id TEXT PRIMARY KEY,"
                    "dungeon_id TEXT,dungeon_status TEXT,current_layer INTEGER,"
                    "total_layers INTEGER,last_reset_date TEXT)"
                )
                connection.execute(
                    "CREATE TABLE dungeon_session_operations(operation_id TEXT PRIMARY KEY,"
                    "payload TEXT,result_status TEXT,dungeon_status TEXT)"
                )
            before = database.read_bytes()
            repository = DungeonSessionSqlRepository(database, database)
            self.assertEqual(repository.operation_session_result("op", "u", "exit")["status"], "schema_missing")
            self.assertEqual(repository.session_transition("op", "u", {}, self.dungeon, "exit")["status"], "schema_missing")
            self.assertEqual(database.read_bytes(), before)
            self.assertEqual({path.name for path in database.parent.iterdir()}, {"legacy.db"})
            with sqlite3.connect(database) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM dungeon_session_operations").fetchone()[0], 0)

if __name__=='__main__':unittest.main()
