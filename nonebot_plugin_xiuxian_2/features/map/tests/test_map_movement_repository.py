import sqlite3
import json
import tempfile
import unittest
from pathlib import Path

from ..repository import MapMovementSqlRepository


class MapMovementRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with sqlite3.connect(self.game) as conn:
            conn.execute("CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, user_stamina INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES ('u', 20)")
            conn.execute("CREATE TABLE map_movement_operations (operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL, stamina INTEGER NOT NULL)")
        with sqlite3.connect(self.player) as conn:
            conn.execute("CREATE TABLE map_status (user_id TEXT PRIMARY KEY, realm TEXT, heaven TEXT, node_id TEXT, visited_nodes TEXT)")
            conn.execute("INSERT INTO map_status VALUES ('u', '凡界', '一重天', 'n1', '[\"n1\"]')")

    def tearDown(self):
        self.tmp.cleanup()

    def test_move_and_replay(self):
        repo = MapMovementSqlRepository(self.game, self.player)
        kwargs: dict[str, object] = dict(operation_id="move-1", user_id="u", expected_position={"realm": "凡界", "heaven": "一重天", "node_id": "n1"}, target_position={"realm": "凡界", "heaven": "一重天", "node_id": "n2"}, expected_stamina=20, cost=5)
        first = repo.move(**kwargs)  # type: ignore[arg-type]
        second = repo.move(**kwargs)  # type: ignore[arg-type]
        self.assertEqual((first["status"], first["stamina"], second["status"]), ("applied", 15, "duplicate"))
        with sqlite3.connect(self.player) as conn:
            self.assertEqual(conn.execute("SELECT node_id,visited_nodes FROM map_status").fetchone(), ("n2", '["n1", "n2"]'))

    def test_stale_snapshot_does_not_write(self):
        repo = MapMovementSqlRepository(self.game, self.player)
        result = repo.move("move-2", "u", {"realm": "凡界", "heaven": "一重天", "node_id": "n2"}, {"realm": "凡界", "heaven": "一重天", "node_id": "n3"}, 20, 5)
        self.assertEqual(result["status"], "state_changed")

    def test_fractional_historical_receipt_is_not_replayed(self):
        payload = json.dumps([
            "u",
            {"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
            {"realm": "凡界", "heaven": "一重天", "node_id": "n2"},
            20,
            5,
        ], sort_keys=True)
        with sqlite3.connect(self.game) as connection:
            connection.execute(
                "INSERT INTO map_movement_operations VALUES ('move-corrupt', ?, 0.5)",
                (payload,),
            )

        result = MapMovementSqlRepository(self.game, self.player).move(
            "move-corrupt",
            "u",
            {"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
            {"realm": "凡界", "heaven": "一重天", "node_id": "n2"},
            20,
            5,
        )
        self.assertEqual(result["reason"], "map_move_receipt_invalid")
        self.assertEqual(result["subject"], "receipt")
        self.assertEqual(self._projection("move-corrupt")[0], (20,))

    def _projection(self, operation_id):
        with sqlite3.connect(self.game) as connection:
            stamina = connection.execute(
                "SELECT user_stamina FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
            receipt = connection.execute(
                "SELECT payload,stamina FROM map_movement_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchall()
        with sqlite3.connect(self.player) as connection:
            position = connection.execute(
                "SELECT realm,heaven,node_id,visited_nodes FROM map_status WHERE user_id='u'"
            ).fetchone()
        return stamina, position, receipt

    def test_trigger_write_rejections_roll_back_and_retry_original_operation(self):
        scenarios = (
            (
                "position_ignore", self.player,
                "CREATE TRIGGER position_ignore BEFORE UPDATE OF realm ON map_status "
                "BEGIN SELECT RAISE(IGNORE); END",
                "map_move_position_write_rejected",
            ),
            (
                "position_rewrite", self.player,
                "CREATE TRIGGER position_rewrite AFTER UPDATE OF node_id ON map_status "
                "BEGIN UPDATE map_status SET node_id='rewritten' WHERE user_id=NEW.user_id; END",
                "map_move_position_projection_mismatch",
            ),
            (
                "stamina_ignore", self.game,
                "CREATE TRIGGER stamina_ignore BEFORE UPDATE OF user_stamina ON user_xiuxian "
                "BEGIN SELECT RAISE(IGNORE); END",
                "map_move_stamina_write_rejected",
            ),
            (
                "stamina_rewrite", self.game,
                "CREATE TRIGGER stamina_rewrite AFTER UPDATE OF user_stamina ON user_xiuxian "
                "BEGIN UPDATE user_xiuxian SET user_stamina=NEW.user_stamina+1 WHERE user_id=NEW.user_id; END",
                "map_move_stamina_projection_mismatch",
            ),
            (
                "receipt_ignore", self.game,
                "CREATE TRIGGER receipt_ignore BEFORE INSERT ON map_movement_operations "
                "BEGIN SELECT RAISE(IGNORE); END",
                "map_move_receipt_write_rejected",
            ),
            (
                "receipt_rewrite", self.game,
                "CREATE TRIGGER receipt_rewrite AFTER INSERT ON map_movement_operations "
                "BEGIN UPDATE map_movement_operations SET payload='rewritten' WHERE operation_id=NEW.operation_id; END",
                "map_move_receipt_projection_mismatch",
            ),
        )
        for trigger, database, create_sql, reason in scenarios:
            with self.subTest(trigger=trigger):
                operation_id = "move-" + trigger
                before = self._projection(operation_id)
                with sqlite3.connect(database) as connection:
                    connection.execute(create_sql)

                result = MapMovementSqlRepository(self.game, self.player).move(
                    operation_id,
                    "u",
                    {"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
                    {"realm": "凡界", "heaven": "一重天", "node_id": "n2"},
                    20,
                    5,
                )
                self.assertEqual(result["reason"], reason)
                self.assertEqual(result["subject"], {
                    "position_ignore": "map_status",
                    "position_rewrite": "map_status",
                    "stamina_ignore": "stamina",
                    "stamina_rewrite": "stamina",
                    "receipt_ignore": "receipt",
                    "receipt_rewrite": "receipt",
                }[trigger])
                self.assertEqual(self._projection(operation_id), before)

                with sqlite3.connect(database) as connection:
                    connection.execute(f"DROP TRIGGER {trigger}")
                recovered = MapMovementSqlRepository(self.game, self.player).move(
                    operation_id,
                    "u",
                    {"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
                    {"realm": "凡界", "heaven": "一重天", "node_id": "n2"},
                    20,
                    5,
                )
                self.assertEqual(recovered["status"], "applied")
                self.assertEqual(self._projection(operation_id)[0], (15,))
                with sqlite3.connect(self.game) as connection:
                    connection.execute("UPDATE user_xiuxian SET user_stamina=20 WHERE user_id='u'")
                    connection.execute("DELETE FROM map_movement_operations")
                with sqlite3.connect(self.player) as connection:
                    connection.execute(
                        "UPDATE map_status SET realm='凡界',heaven='一重天',node_id='n1',visited_nodes='[\"n1\"]' WHERE user_id='u'"
                    )

    def test_abort_remains_original_exception_and_rolls_back(self):
        operation_id = "move-abort"
        with sqlite3.connect(self.game) as connection:
            connection.execute(
                "CREATE TRIGGER move_abort BEFORE INSERT ON map_movement_operations "
                "BEGIN SELECT RAISE(ABORT,'movement receipt blocked'); END"
            )
        before = self._projection(operation_id)
        with self.assertRaisesRegex(sqlite3.IntegrityError, "movement receipt blocked"):
            MapMovementSqlRepository(self.game, self.player).move(
                operation_id,
                "u",
                {"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
                {"realm": "凡界", "heaven": "一重天", "node_id": "n2"},
                20,
                5,
            )
        self.assertEqual(self._projection(operation_id), before)
        with sqlite3.connect(self.game) as connection:
            connection.execute("DROP TRIGGER move_abort")
        result = MapMovementSqlRepository(self.game, self.player).move(
            operation_id,
            "u",
            {"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
            {"realm": "凡界", "heaven": "一重天", "node_id": "n2"},
            20,
            5,
        )
        self.assertEqual(result["status"], "applied")


if __name__ == "__main__":
    unittest.main()
