from pathlib import Path
import tempfile
import unittest

from ..repository import StoneTrainingSqlRepository, TiantiProfileReader


class StoneTrainingSchemaTests(unittest.TestCase):
    def test_missing_migration_is_explicit_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "炼体").mkdir()
            (root / "炼体" / "炼体境界.json").write_text('{"初境":{"rank":1,"need_hp":0}}', encoding="utf-8")
            import sqlite3
            game = root / "game.db"
            player = root / "player.db"
            with sqlite3.connect(game) as conn:
                conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER)")
                conn.execute("INSERT INTO user_xiuxian VALUES ('u', 1000)")
            repo = StoneTrainingSqlRepository(game, player, profile_reader=TiantiProfileReader(root))
            with self.assertRaisesRegex(RuntimeError, "run migrations"):
                repo.train("op", "u", 100)
            with sqlite3.connect(game) as conn:
                self.assertEqual(conn.execute("SELECT stone FROM user_xiuxian").fetchone()[0], 1000)


if __name__ == "__main__":
    unittest.main()
