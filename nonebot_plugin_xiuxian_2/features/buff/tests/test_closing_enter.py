from __future__ import annotations

import concurrent.futures
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger
from ..application import BuffApplication
from ..migrations import apply_closing_enter_game, apply_closing_enter_player
from tests.test_db_backend import db_backend


class FixedClock:
    def now(self):
        return datetime(2026, 1, 5, 12, 0, 0)


class ClosingEnterApplicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,root_type TEXT NOT NULL)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?)",
                [("u", "天灵根"), ("mortal", "伪灵根"), ("busy", "天灵根")],
            )
            conn.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
            )
            conn.executemany(
                "INSERT INTO user_cd VALUES(?,?,?,NULL)",
                [("u", 0, "0"), ("mortal", 0, "0"), ("busy", 5, "old")],
            )
        with db_backend.transaction(self.player) as conn:
            conn.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY)")
        with DatabaseUnitOfWork(self.game) as uow:
            OperationLedger(clock=FixedClock()).ensure_schema(uow)
            apply_closing_enter_game(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            apply_closing_enter_player(uow)
        self.application = BuffApplication(self.game, self.player, clock=FixedClock())

    def tearDown(self):
        self.temp.cleanup()

    def state(self, user_id="u"):
        with db_backend.connection(self.game) as conn:
            cd = tuple(
                conn.execute(
                    "SELECT type,create_time,scheduled_time FROM user_cd WHERE user_id=?",
                    (user_id,),
                ).fetchone()
            )
        with db_backend.connection(self.player) as conn:
            row = conn.execute(
                'SELECT "闭关次数" FROM statistics WHERE user_id=?', (user_id,)
            ).fetchone()
        return cd, None if row is None else int(row[0])

    def test_success_replay_and_payload_conflict_are_idempotent(self):
        first = self.application.closing_enter(
            operation_id="enter-1", user_id="u", started_at="2026-01-05 12:00:00.000000"
        )
        replay = self.application.closing_enter(
            operation_id="enter-1", user_id="u", started_at="2026-01-05 12:01:00.000000"
        )
        conflict = self.application.closing_enter(
            operation_id="enter-1", user_id="mortal", started_at="2026-01-05 12:00:00.000000"
        )

        self.assertTrue(first.ok)
        self.assertTrue(replay.replayed)
        self.assertEqual(first.data, replay.data)
        self.assertEqual("operation_conflict", conflict.code)
        self.assertEqual(((1, "2026-01-05 12:00:00.000000", None), 1), self.state())
        self.assertEqual(((0, "0", None), None), self.state("mortal"))

    def test_ineligible_and_busy_do_not_write(self):
        ineligible = self.application.closing_enter(
            operation_id="mortal-enter", user_id="mortal", started_at="now"
        )
        busy = self.application.closing_enter(
            operation_id="busy-enter", user_id="busy", started_at="now"
        )

        self.assertEqual(("ineligible", "busy"), (ineligible.code, busy.code))
        self.assertEqual(((0, "0", None), None), self.state("mortal"))
        self.assertEqual(((5, "old", None), None), self.state("busy"))

    def test_two_operation_ids_only_one_wins_the_cas(self):
        def enter(operation_id):
            app = BuffApplication(self.game, self.player, clock=FixedClock())
            return app.closing_enter(
                operation_id=operation_id, user_id="u", started_at=operation_id
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(enter, ("race-a", "race-b")))

        self.assertEqual(1, sum(result.ok for result in results))
        self.assertEqual(1, sum(result.code in {"busy", "state_changed"} for result in results))
        self.assertEqual(1, self.state()[1])

    def test_receipt_failure_rolls_back_and_same_operation_can_retry(self):
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TRIGGER reject_closing_enter BEFORE INSERT ON "
                "closing_enter_operations BEGIN SELECT RAISE(ABORT,'reject'); END"
            )

        with self.assertRaises(Exception):
            self.application.closing_enter(
                operation_id="rollback", user_id="u", started_at="rollback-time"
            )
        self.assertEqual(((0, "0", None), None), self.state())

        with db_backend.transaction(self.game) as conn:
            conn.execute("DROP TRIGGER reject_closing_enter")
        retried = self.application.closing_enter(
            operation_id="rollback", user_id="u", started_at="retry-time"
        )
        self.assertTrue(retried.ok)
        self.assertEqual(((1, "retry-time", None), 1), self.state())

    def test_missing_feature_schema_fails_closed_without_request_ddl(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp) / "game.db", Path(temp) / "player.db"
            with db_backend.transaction(game) as conn:
                conn.execute(
                    "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,root_type TEXT)"
                )
                conn.execute("INSERT INTO user_xiuxian VALUES('u','天灵根')")
                conn.execute(
                    "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
                )
                conn.execute("INSERT INTO user_cd VALUES('u',0,'0',NULL)")
            with db_backend.transaction(player) as conn:
                conn.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY)")
            result = BuffApplication(game, player, clock=FixedClock()).closing_enter(
                operation_id="missing", user_id="u", started_at="now"
            )
            self.assertEqual("schema_missing", result.code)
            with db_backend.connection(game) as conn:
                self.assertIsNone(
                    conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='closing_enter_operations'"
                    ).fetchone()
                )
            with db_backend.connection(player) as conn:
                columns = {
                    row[1]
                    for row in conn.execute("PRAGMA table_info(statistics)").fetchall()
                }
                self.assertNotIn("闭关次数", columns)

    def test_migration_routing_is_split_between_game_and_player(self):
        # Importing the composition root starts the complete NoneBot legacy
        # plugin graph and exposes an unrelated Activity import cycle.  The
        # migration routing contract is source-bound and checked without that
        # startup side effect; runtime routing is covered by the progress gate.
        plugin = Path(__file__).resolve().parents[3] / "plugin.py"
        source = plugin.read_text(encoding="utf-8")
        self.assertIn(
            'Migration("buff.012", "closing_enter_operations", apply_closing_enter_game)',
            source,
        )
        self.assertIn(
            'Migration("buff.013", "closing_enter_player_statistics", apply_closing_enter_player)',
            source,
        )
        game_exclusions = source[
            source.index("_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS") : source.index(
                "_PLAYER_DATABASE_MIGRATION_VERSIONS"
            )
        ]
        player_versions = source[
            source.index("_PLAYER_DATABASE_MIGRATION_VERSIONS") : source.index(
                "_TRADE_DATABASE_MIGRATION_VERSIONS"
            )
        ]
        self.assertIn('"buff.013"', game_exclusions)
        self.assertIn('"buff.013"', player_versions)


if __name__ == "__main__":
    unittest.main()
