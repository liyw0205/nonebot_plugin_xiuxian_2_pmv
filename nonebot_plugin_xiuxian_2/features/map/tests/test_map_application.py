import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema
from ..application import MapApplication


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class CombatApplication:
    def __init__(self):
        self.kwargs = None

    def settle(self, **kwargs):
        self.kwargs = kwargs
        return "combat-settlement"


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

    def test_combat_settle_delegates_to_combat_feature_application(self):
        with tempfile.TemporaryDirectory() as directory:
            application = MapApplication(Path(directory) / "game.db", Path(directory) / "player.db")
            combat = CombatApplication()
            application._combat_settlement_application = combat
            values = {
                "operation_id": "combat-1",
                "user_id": "u",
                "expected_daily": {"date": "2026-09-21", "combat_count": 1},
                "snapshot": '{"task_id":"combat-1"}',
                "daily_limit": 4,
                "stone": 10,
                "items": ({"id": 1, "name": "材料", "type": "材料", "amount": 2},),
                "max_goods_num": 99,
            }

            self.assertEqual(application.combat_settle(**values), "combat-settlement")
            self.assertEqual(combat.kwargs, values)


if __name__ == "__main__": unittest.main()
