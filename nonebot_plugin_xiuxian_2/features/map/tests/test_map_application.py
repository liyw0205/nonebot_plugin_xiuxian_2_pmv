import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import MapApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class MapApplicationTest(unittest.TestCase):
    def test_move_uses_operation_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
            app = MapApplication(game, player, repository=Repo())
            self.assertTrue(app.move(operation_id="map-1", user_id="u").ok)

    def test_interactive_finish_uses_feature_settlement_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
            with DatabaseUnitOfWork(player) as uow:
                uow.execute(
                    "CREATE TABLE map_interactive_actions("
                    "user_id TEXT PRIMARY KEY, action_id TEXT, status TEXT, "
                    "state_json TEXT, settlement_json TEXT, ready_at TEXT, "
                    "expires_at TEXT, cooldown_seconds INTEGER, updated_at TEXT)"
                )
                uow.execute(
                    "INSERT INTO map_interactive_actions VALUES (?, ?, 'active', ?, '', ?, ?, ?, ?)",
                    (
                        "u",
                        "action-1",
                        json.dumps({"action_id": "action-1", "action": "采集"}),
                        "2026-09-21 12:00:00",
                        "2026-09-21 12:01:00",
                        20,
                        "2026-09-21 11:59:00",
                    ),
                )

            application = MapApplication(game, player)
            settlement = {"daily": {"date": "2026-09-21", "gather_count": 1}}
            first = application.interactive_finish(
                operation_id="finish-1",
                user_id="u",
                action_id="action-1",
                settlement=settlement,
            )
            replay = application.interactive_finish(
                operation_id="finish-1",
                user_id="u",
                action_id="action-1",
                settlement=settlement,
            )

            self.assertTrue(first.ok)
            self.assertEqual(replay.status, "replayed")
            with DatabaseUnitOfWork(player) as uow:
                row = uow.query_one(
                    "SELECT settlement_json FROM map_interactive_actions WHERE action_id = ?",
                    ("action-1",),
                )
            self.assertEqual(json.loads(row["settlement_json"]), settlement)


if __name__ == "__main__": unittest.main()
