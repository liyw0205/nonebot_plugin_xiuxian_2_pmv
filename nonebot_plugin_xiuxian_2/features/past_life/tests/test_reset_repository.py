import tempfile
import unittest
from pathlib import Path
from ..reset_repository import PastLifeResetSqlRepository
from tests.test_db_backend import db_backend

class PastLifeResetRepositoryTests(unittest.TestCase):
    def test_reset_one_replay_and_clear_history(self):
        with tempfile.TemporaryDirectory() as temp:
            game,player=Path(temp)/'game.db',Path(temp)/'player.db'
            with db_backend.transaction(game) as c: c.execute('CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY)'); c.execute("INSERT INTO user_xiuxian VALUES('u')")
            with db_backend.transaction(player) as c: c.execute('CREATE TABLE past_life(user_id TEXT PRIMARY KEY,state INTEGER,stage INTEGER,revision INTEGER,total_runs INTEGER,best_ending TEXT,best_score INTEGER,endings_log TEXT,achievement_points INTEGER)'); c.execute("INSERT INTO past_life VALUES('u',2,3,5,2,'end',88,'[]',9)")
            repo=PastLifeResetSqlRepository(game,player); first=repo.reset_one('r','u',False); dup=repo.reset_one('r','u',False); self.assertEqual((first.status,dup.status),('applied','duplicate'))

    def test_reset_all_freezes_targets_and_replays_after_chunked_completion(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp) / "game.db", Path(temp) / "player.db"
            with db_backend.transaction(player) as conn:
                conn.execute(
                    "CREATE TABLE past_life(user_id TEXT PRIMARY KEY,state INTEGER,"
                    "revision INTEGER,total_runs INTEGER,best_ending TEXT,best_score INTEGER,"
                    "endings_log TEXT,achievement_points INTEGER)"
                )
                conn.executemany(
                    "INSERT INTO past_life(user_id,state,revision,total_runs,best_ending,best_score,endings_log,achievement_points) VALUES(?,?,?,?,?,?,?,?)",
                    [("u", 2, 1, 2, "end", 80, "[]", 4), ("v", 2, 3, 5, "best", 90, "[]", 8)],
                )
            repo = PastLifeResetSqlRepository(game, player)
            created = repo.reset_all_create("all-1", False)
            self.assertEqual((created.status, created.total), ("created", 2))
            with db_backend.transaction(player) as conn:
                conn.execute("INSERT INTO past_life(user_id,state,revision) VALUES('w',2,9)")
                conn.execute("UPDATE past_life SET revision=99 WHERE user_id='v'")
            first = repo.reset_all_batch("all-1", batch_size=1)
            self.assertFalse(first.complete)
            final = repo.reset_all_batch("all-1", batch_size=10)
            self.assertTrue(final.complete)
            self.assertEqual((final.total, final.applied, final.conflicted, final.missing), (2, 1, 1, 0))
            self.assertEqual("duplicate", repo.reset_all_create("all-1", False).status)
            with db_backend.connection(player) as conn:
                self.assertEqual(2, conn.execute("SELECT revision FROM past_life WHERE user_id='u'").fetchone()[0])
                self.assertEqual(99, conn.execute("SELECT revision FROM past_life WHERE user_id='v'").fetchone()[0])
                self.assertEqual(9, conn.execute("SELECT revision FROM past_life WHERE user_id='w'").fetchone()[0])

    def test_reset_all_batch_records_error_and_can_resume_after_rollback(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp) / "game.db", Path(temp) / "player.db"
            with db_backend.transaction(player) as conn:
                conn.execute("CREATE TABLE past_life(user_id TEXT PRIMARY KEY,state INTEGER,revision INTEGER)")
                conn.executemany("INSERT INTO past_life(user_id,state,revision) VALUES(?,?,?)", [("u", 2, 1), ("v", 2, 2)])
            repo = PastLifeResetSqlRepository(game, player)
            repo.reset_all_create("all-error")
            repo.reset_all_batch("all-error", batch_size=1)
            with db_backend.transaction(player) as conn:
                conn.execute(
                    "CREATE TRIGGER reject_v_reset BEFORE UPDATE "
                    + "ON past_life WHEN OLD.user_id='v' BEGIN SELECT RAISE(ABORT,'reject v'); END"
                )
            with self.assertRaises(db_backend.IntegrityError):
                repo.reset_all_batch("all-error", batch_size=1)
            pending = repo.find_pending_all()
            self.assertIn("reject v", pending.last_error)
            with db_backend.transaction(player) as conn:
                conn.execute("DROP TRIGGER reject_v_reset")
            self.assertTrue(repo.reset_all_batch("all-error", batch_size=1).complete)
