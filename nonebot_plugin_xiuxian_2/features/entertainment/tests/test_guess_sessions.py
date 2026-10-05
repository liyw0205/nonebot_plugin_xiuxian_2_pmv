from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from ....infrastructure.database import DatabaseUnitOfWork
from ....plugin import build_migrations, migrations_for_database
from ..guess_application import EntertainmentGuessSessionApplication
from ..guess_repository import EntertainmentGuessSessionSqlRepository
from ..migrations import apply_entertainment_guess_sessions


class EntertainmentGuessSessionsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.database = Path(self.temp.name) / "game.db"
        with DatabaseUnitOfWork(self.database) as uow:
            apply_entertainment_guess_sessions(uow)
        self.repository = EntertainmentGuessSessionSqlRepository(self.database)
        self.application = EntertainmentGuessSessionApplication(self.repository)

    def tearDown(self):
        self.temp.cleanup()

    def start_number(self, user_id="u1", answer=50, now=1000):
        return self.application.start_number(
            user_id=user_id,
            user_name="Player",
            answer=answer,
            create_time="2026-10-06 00:00:00",
            last_action_time="2026-10-06 00:00:00",
            now_epoch=now,
        )

    def start_puzzle(self, user_id="u1", answer="1234", now=1000):
        return self.application.start_puzzle(
            user_id=user_id,
            user_name="Player",
            answer=answer,
            difficulty="简单",
            digits=4,
            create_time="2026-10-06 00:00:00",
            last_action_time="2026-10-06 00:00:00",
            now_epoch=now,
        )

    def test_game_types_are_isolated_and_sessions_survive_application_recreation(self):
        number = self.start_number()
        puzzle = self.start_puzzle()
        restarted = EntertainmentGuessSessionApplication(
            EntertainmentGuessSessionSqlRepository(self.database)
        )

        self.assertEqual("started", number["status"])
        self.assertEqual("started", puzzle["status"])
        self.assertEqual("active", restarted.read("number", "u1", now_epoch=1001)["status"])
        self.assertEqual("active", restarted.read("puzzle", "u1", now_epoch=1001)["status"])
        self.assertEqual(50, restarted.read("number", "u1", now_epoch=1001)["session"]["answer"])
        self.assertEqual("1234", restarted.read("puzzle", "u1", now_epoch=1001)["session"]["answer"])

    def test_duplicate_start_keeps_the_existing_answer_and_deadline(self):
        first = self.start_number(answer=50)
        duplicate = self.start_number(answer=12, now=1100)

        self.assertEqual("existing", duplicate["status"])
        self.assertEqual(50, duplicate["session"]["answer"])
        self.assertEqual(first["session_token"], duplicate["session_token"])
        self.assertEqual(first["expires_at"], duplicate["expires_at"])

    def test_number_guess_progress_win_and_duplicate_delivery_semantics(self):
        self.start_number(answer=50)
        first = self.application.guess_number("u1", 20, last_action_time="t1", now_epoch=1010)
        duplicate = self.application.guess_number("u1", 20, last_action_time="t2", now_epoch=1011)

        self.assertEqual(("updated", "too_low"), (first["status"], first["outcome"]))
        self.assertEqual((21, 1), (first["session"]["low"], first["session"]["tries"]))
        self.assertEqual((21, 2), (duplicate["session"]["low"], duplicate["session"]["tries"]))

        won = self.application.guess_number("u1", 50, last_action_time="t3", now_epoch=1012)
        self.assertEqual("finished", won["status"])
        self.assertEqual(3, won["session"]["tries"])
        self.assertEqual("missing", self.application.read("number", "u1", now_epoch=1013)["status"])

    def test_puzzle_guess_counts_positions_and_finishes_atomically(self):
        self.start_puzzle()
        progress = self.application.guess_puzzle(
            "u1", "1299", last_action_time="t1", now_epoch=1010
        )
        won = self.application.guess_puzzle(
            "u1", "1234", last_action_time="t2", now_epoch=1011
        )

        self.assertEqual("progress:2", progress["outcome"])
        self.assertEqual(1, progress["session"]["tries"])
        self.assertEqual("correct:4", won["outcome"])
        self.assertEqual("finished", won["status"])
        self.assertEqual(2, won["session"]["tries"])

    def test_timeout_is_lazy_after_restart_and_old_token_cannot_expire_a_renewed_game(self):
        started = self.start_number(answer=50)
        updated = self.application.guess_number(
            "u1", 1, last_action_time="t1", now_epoch=1010
        )
        old_timer = self.application.expire(
            "number", "u1", started["session_token"], now_epoch=1300
        )

        self.assertEqual("stale", old_timer["status"])
        self.assertNotEqual(started["session_token"], updated["session_token"])
        self.assertEqual("active", self.application.read("number", "u1", now_epoch=1300)["status"])
        expired = self.application.read("number", "u1", now_epoch=1310)
        self.assertEqual("expired", expired["status"])
        self.assertEqual(1, expired["session"]["tries"])
        self.assertEqual("missing", self.application.read("number", "u1", now_epoch=1310)["status"])

    def test_end_returns_the_final_state_and_removes_only_that_game(self):
        self.start_number()
        self.start_puzzle()

        ended = self.application.end("number", "u1", now_epoch=1001)

        self.assertEqual("finished", ended["status"])
        self.assertEqual(50, ended["session"]["answer"])
        self.assertEqual("missing", self.application.read("number", "u1", now_epoch=1002)["status"])
        self.assertEqual("active", self.application.read("puzzle", "u1", now_epoch=1002)["status"])

    def test_concurrent_guesses_do_not_lose_updates(self):
        self.start_number(answer=100)
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(
                pool.map(
                    lambda index: self.application.guess_number(
                        "u1", 1, last_action_time=f"t{index}", now_epoch=1010
                    ),
                    range(12),
                )
            )

        self.assertTrue(all(result["status"] == "updated" for result in results))
        current = self.application.read("number", "u1", now_epoch=1011)
        self.assertEqual(12, current["session"]["tries"])

    def test_late_failure_rolls_back_the_session_update(self):
        self.start_number(answer=50)
        with patch.object(self.repository, "_new_token", side_effect=RuntimeError("late failure")):
            with self.assertRaisesRegex(RuntimeError, "late failure"):
                self.application.guess_number(
                    "u1", 10, last_action_time="failed", now_epoch=1010
                )

        current = self.application.read("number", "u1", now_epoch=1011)
        self.assertEqual(0, current["session"]["tries"])
        self.assertEqual(1, current["session"]["low"])

    def test_missing_schema_fails_closed_without_creating_database(self):
        missing = Path(self.temp.name) / "not-created.db"
        repository = EntertainmentGuessSessionSqlRepository(missing)

        with self.assertRaisesRegex(RuntimeError, "schema_missing"):
            repository.get("number", "u1", now_epoch=1000)
        self.assertFalse(missing.exists())

    def test_schema_migration_is_registered_for_game_database_only(self):
        migrations = build_migrations()
        game = {item.version for item in migrations_for_database(migrations, "game_db")}
        player = {item.version for item in migrations_for_database(migrations, "player_db")}

        self.assertIn("legacy.entertainment.004", game)
        self.assertNotIn("legacy.entertainment.004", player)


if __name__ == "__main__":
    unittest.main()
