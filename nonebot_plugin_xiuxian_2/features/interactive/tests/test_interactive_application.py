from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from ..application import InteractiveApplication
from ....infrastructure.database import DatabaseUnitOfWork


class InteractiveApplicationTest(unittest.TestCase):
    @staticmethod
    def _database(directory: str) -> Path:
        database = Path(directory) / "game.db"
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, exp INTEGER, level TEXT, stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES ('u', 100000, '练气境初期', 500)")
        return database

    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = InteractiveApplication(f"{directory}/game.db")
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)

    def test_reward_action_replays_and_records_asset_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = InteractiveApplication(self._database(directory))
            request = {
                "action": "stone_settle",
                "expected_stone": 500,
                "business_date": date(2026, 9, 12),
            }
            first = app.execute(operation_id="stone-op", user_id="u", payload=request)
            second = app.execute(operation_id="stone-op", user_id="u", payload=request)
            self.assertEqual(first.status, "applied")
            self.assertEqual(second.status, "replayed")
            with DatabaseUnitOfWork(Path(directory) / "game.db") as uow:
                row = uow.query_one("SELECT status FROM operation_ledger WHERE operation_id = 'stone-op'")
            self.assertEqual(row["status"], "applied")

    def test_greeting_and_fortune_actions_use_one_application_boundary(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            app = InteractiveApplication(self._database(directory))
            greeting = app.execute(
                operation_id="greeting-op",
                user_id="u",
                payload={"action": "greeting_claim", "kind": "morning", "business_date": "2026-09-12"},
            )
            fortune = app.execute(
                operation_id="fortune-op",
                user_id="u",
                payload={
                    "action": "fortune_resolve",
                    "business_date": "2026-09-12",
                    "create_fortune": lambda: {"type": "吉", "description": "签文", "stars": "*****"},
                },
            )
            self.assertEqual((greeting.data["claimed"], greeting.data["position"]), (True, 1))
            self.assertEqual((fortune.data["fortune_type"], fortune.data["stars"]), ("吉", "*****"))


if __name__ == "__main__":
    unittest.main()
