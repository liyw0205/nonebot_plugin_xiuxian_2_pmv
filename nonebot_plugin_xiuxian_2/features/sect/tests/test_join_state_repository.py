import tempfile
import unittest
from pathlib import Path
from ..join_state_repository import SectJoinStateSqlRepository
from tests.test_db_backend import db_backend
from .fixtures import migrate_sect

class SectJoinStateRepositoryTests(unittest.TestCase):
    def test_open_close_join_replay(self):
        with tempfile.TemporaryDirectory() as temp:
            db = Path(temp) / 'sect.db'
            migrate_sect(db)
            with db_backend.transaction(db) as c:
                c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,sect_id INTEGER,sect_position INTEGER)')
                c.execute("INSERT INTO user_xiuxian VALUES('owner',1,0)")
                c.execute('CREATE TABLE sects(sect_id INTEGER PRIMARY KEY,sect_owner TEXT,sect_name TEXT,join_open INTEGER,closed INTEGER)')
                c.execute("INSERT INTO sects VALUES(1,'owner','青云',0,0)")
            repo = SectJoinStateSqlRepository(db)
            opened = repo.open('open','owner',expected_sect_id=1)
            closed = repo.close('close','owner',expected_sect_id=1)
            self.assertEqual((opened['status'],closed['status']),('opened','closed'))
            self.assertEqual(repo.close('close','owner')['status'],'duplicate')

    def _database(self, join_open: int):
        temp = tempfile.TemporaryDirectory()
        db = Path(temp.name) / "sect.sqlite3"
        migrate_sect(db)
        with db_backend.transaction(db) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian ("
                "user_id TEXT PRIMARY KEY, sect_id INTEGER, sect_position INTEGER)"
            )
            conn.execute(
                "CREATE TABLE sects ("
                "sect_id INTEGER PRIMARY KEY, sect_owner TEXT, sect_name TEXT, "
                "join_open INTEGER, closed INTEGER)"
            )
            conn.execute("INSERT INTO sects VALUES (1, 'owner', '青云', ?, 0)", (join_open,))
            conn.execute("INSERT INTO user_xiuxian VALUES ('owner', 1, 0)")
        return temp, db

    def test_state_rewrite_is_rejected_and_same_operation_recovers(self):
        for action, initial, desired in (("open", 0, 1), ("close", 1, 0)):
            with self.subTest(action=action):
                temp, db = self._database(initial)
                try:
                    with db_backend.transaction(db) as conn:
                        conn.execute(
                            f"CREATE TRIGGER rewrite_{action}_state AFTER UPDATE ON sects "
                            f"BEGIN UPDATE sects SET join_open={initial} WHERE sect_id=1; END"
                        )
                    result = getattr(SectJoinStateSqlRepository(db), action)(
                        f"rewrite-{action}", "owner", expected_sect_id=1
                    )
                    self.assertEqual(
                        (result["status"], result["reason"], result["subject"]),
                        ("state_changed", f"sect_{action}_join_state_write_rejected", "sect"),
                    )
                    with db_backend.connection(db) as conn:
                        self.assertEqual(
                            conn.execute("SELECT join_open FROM sects WHERE sect_id=1").fetchone()[0],
                            initial,
                        )
                        self.assertEqual(
                            conn.execute(f"SELECT COUNT(*) FROM sect_{action}_join_operations").fetchone()[0],
                            0,
                        )
                    with db_backend.transaction(db) as conn:
                        conn.execute(f"DROP TRIGGER rewrite_{action}_state")
                    recovered = getattr(SectJoinStateSqlRepository(db), action)(
                        f"rewrite-{action}", "owner", expected_sect_id=1
                    )
                    self.assertEqual(recovered["status"], "opened" if desired else "closed")
                finally:
                    temp.cleanup()

    def test_receipt_ignore_is_rejected_and_same_operation_recovers(self):
        for action, initial, desired in (("open", 0, 1), ("close", 1, 0)):
            with self.subTest(action=action):
                temp, db = self._database(initial)
                try:
                    with db_backend.transaction(db) as conn:
                        conn.execute(
                            f"CREATE TRIGGER ignore_{action}_receipt BEFORE INSERT "
                            f"ON sect_{action}_join_operations BEGIN SELECT RAISE(IGNORE); END"
                        )
                    result = getattr(SectJoinStateSqlRepository(db), action)(
                        f"ignore-{action}", "owner", expected_sect_id=1
                    )
                    self.assertEqual(
                        (result["status"], result["reason"], result["subject"]),
                        ("state_changed", f"sect_{action}_join_receipt_write_rejected", f"ignore-{action}"),
                    )
                    with db_backend.connection(db) as conn:
                        self.assertEqual(
                            conn.execute("SELECT join_open FROM sects WHERE sect_id=1").fetchone()[0],
                            initial,
                        )
                        self.assertEqual(
                            conn.execute(f"SELECT COUNT(*) FROM sect_{action}_join_operations").fetchone()[0],
                            0,
                        )
                    with db_backend.transaction(db) as conn:
                        conn.execute(f"DROP TRIGGER ignore_{action}_receipt")
                    recovered = getattr(SectJoinStateSqlRepository(db), action)(
                        f"ignore-{action}", "owner", expected_sect_id=1
                    )
                    self.assertEqual(recovered["status"], "opened" if desired else "closed")
                finally:
                    temp.cleanup()

    def test_receipt_rewrite_is_rejected_and_aborts_are_transparent(self):
        temp, db = self._database(0)
        try:
            with db_backend.transaction(db) as conn:
                conn.execute(
                    "CREATE TRIGGER rewrite_open_receipt AFTER INSERT "
                    "ON sect_open_join_operations BEGIN "
                    "UPDATE sect_open_join_operations SET actor_id='tampered' "
                    "WHERE operation_id='rewrite-receipt'; END"
                )
            result = SectJoinStateSqlRepository(db).open("rewrite-receipt", "owner", expected_sect_id=1)
            self.assertEqual(
                (result["status"], result["reason"], result["subject"]),
                ("state_changed", "sect_open_join_receipt_write_rejected", "rewrite-receipt"),
            )
            with db_backend.connection(db) as conn:
                self.assertEqual(conn.execute("SELECT join_open FROM sects WHERE sect_id=1").fetchone()[0], 0)
            with db_backend.transaction(db) as conn:
                conn.execute("DROP TRIGGER rewrite_open_receipt")
                conn.execute(
                    "CREATE TRIGGER abort_open_receipt BEFORE INSERT "
                    "ON sect_open_join_operations BEGIN SELECT RAISE(ABORT, 'join receipt failed'); END"
                )
            with self.assertRaises(db_backend.IntegrityError):
                SectJoinStateSqlRepository(db).open("abort-receipt", "owner", expected_sect_id=1)
            with db_backend.connection(db) as conn:
                self.assertEqual(conn.execute("SELECT join_open FROM sects WHERE sect_id=1").fetchone()[0], 0)
        finally:
            temp.cleanup()

    def test_same_operation_with_another_actor_is_a_conflict(self):
        temp, db = self._database(0)
        try:
            repo = SectJoinStateSqlRepository(db)
            self.assertEqual(repo.open("same-op", "owner", expected_sect_id=1)["status"], "opened")
            conflict = repo.open("same-op", "other", expected_sect_id=1)
            self.assertEqual(
                (conflict["status"], conflict["reason"], conflict["subject"]),
                ("operation_conflict", "operation_conflict", "actor"),
            )
        finally:
            temp.cleanup()
