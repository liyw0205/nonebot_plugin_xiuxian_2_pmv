from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import EntertainmentApplication
from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import apply_platform_schema


class EntertainmentApplicationTest(unittest.TestCase):
    def test_list_account_summaries_is_read_only_and_does_not_open_game_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            state = Path(directory) / "accounts.json"
            state.write_text('[{"api_user_id":"42","secret":"hidden"}]', encoding="utf-8")
            app = EntertainmentApplication(database)

            result = app.list_account_summaries(state_path=state)

            self.assertEqual(result.status, "ok")
            self.assertEqual(result.accounts[0].api_user_id, "42")
            self.assertFalse(database.exists())

    def test_resolve_checkin_targets_is_read_only_and_does_not_open_game_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            state = Path(directory) / "accounts.json"
            state.write_text(
                '[{"api_user_id":"42","secret":"hidden","base_url":"https://api.test"}]',
                encoding="utf-8",
            )
            app = EntertainmentApplication(database)

            result = app.resolve_checkin_targets(state_path=state, selector="")

            self.assertEqual(result.status, "ok")
            self.assertEqual(result.targets[0].api_user_id, "42")
            self.assertNotIn("hidden", repr(result))
            self.assertFalse(database.exists())

    def test_resolve_info_targets_uses_feature_owned_bounded_query(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            state = Path(directory) / "accounts.json"
            state.write_text(
                '[{"api_user_id":"42","secret":"hidden","base_url":"https://api.test"}]',
                encoding="utf-8",
            )
            app = EntertainmentApplication(database)

            result = app.resolve_info_targets(state_path=state, selector="")

            self.assertEqual(result.status, "ok")
            self.assertEqual(result.targets[0].api_user_id, "42")
            self.assertNotIn("hidden", repr(result))
            self.assertFalse(database.exists())

    def test_execute_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "game.db"
            with DatabaseUnitOfWork(database) as uow:
                apply_platform_schema(uow)
            app = EntertainmentApplication(database)
            first = app.execute(operation_id="op-1", user_id="u")
            second = app.execute(operation_id="op-1", user_id="u")
            self.assertEqual(first.operation_id, second.operation_id)
            self.assertTrue(second.replayed)


if __name__ == "__main__":
    unittest.main()
