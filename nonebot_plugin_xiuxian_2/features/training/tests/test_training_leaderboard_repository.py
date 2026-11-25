from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ....infrastructure.database import DatabaseUnitOfWork
from ....xiuxian.xiuxian_utils import db_backend
from ....xiuxian.xiuxian_utils.shared_read_cache import process_read_cache
from ...base.ranking_position import PersonalRank
from ..application import TrainingApplication
from ..leaderboard_repository import TrainingLeaderboardSqlRepository


class TrainingLeaderboardRepositoryTests(unittest.TestCase):
    def setUp(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.game_database = root / "game.db"
        self.player_database = root / "player.db"
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)"
            )
            conn.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name) VALUES(?,?)",
                (("u1", "甲"), ("u2", "乙"), ("viewer", "道友")),
            )
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "CREATE TABLE training(user_id TEXT,completed INTEGER,points INTEGER)"
            )
            conn.executemany(
                "INSERT INTO training(user_id,completed,points) VALUES(?,?,?)",
                (("u1", 10, 100), ("u2", 9, 90), ("viewer", 8, 80)),
            )
        self.repository = TrainingLeaderboardSqlRepository(
            self.game_database, self.player_database
        )

    def test_cold_display_snapshot_shares_live_rank_read_and_hot_rank_stays_fresh(self):
        original_attach = DatabaseUnitOfWork.attach_database
        with patch.object(
            DatabaseUnitOfWork,
            "attach_database",
            autospec=True,
            side_effect=lambda uow, *args, **kwargs: original_attach(uow, *args, **kwargs),
        ) as attach:
            snapshot, rank = self.repository.top_snapshot_with_rank(
                "completed", "viewer", limit=50
            )
        self.assertEqual(1, attach.call_count)
        self.assertEqual(PersonalRank(3, 3, 8), rank)
        self.assertEqual(["u1", "u2", "viewer"], [row["user_id"] for row in snapshot["rows"]])

        with db_backend.transaction(self.player_database) as conn:
            conn.execute("UPDATE training SET completed=12 WHERE user_id='viewer'")

        with patch.object(
            DatabaseUnitOfWork,
            "attach_database",
            autospec=True,
            side_effect=lambda uow, *args, **kwargs: original_attach(uow, *args, **kwargs),
        ) as attach:
            cached_snapshot, live_rank = self.repository.top_snapshot_with_rank(
                "completed", "viewer", limit=50
            )
        self.assertEqual(1, attach.call_count)
        self.assertEqual(snapshot["rows"], cached_snapshot["rows"])
        self.assertEqual(snapshot["as_of"], cached_snapshot["as_of"])
        self.assertEqual(PersonalRank(1, 3, 12), live_rank)

    def test_default_application_keeps_both_viewer_ranks_live_outside_top_slice(self):
        application = TrainingApplication(self.game_database, self.player_database)
        for field, old_value, new_value in (
            ("completed", 8, 12),
            ("points", 80, 120),
        ):
            with self.subTest(field=field):
                snapshot, rank = application.leaderboard_snapshot_with_rank(
                    field, "viewer", limit=2
                )
                self.assertEqual(["u1", "u2"], [
                    row["user_id"] for row in snapshot["rows"]
                ])
                self.assertEqual(PersonalRank(3, 3, old_value), rank)
                with db_backend.transaction(self.player_database) as conn:
                    conn.execute(
                        f"UPDATE training SET {field}=? WHERE user_id='viewer'",
                        (new_value,),
                    )
                cached_snapshot, live_rank = application.leaderboard_snapshot_with_rank(
                    field, "viewer", limit=2
                )
                self.assertEqual(snapshot, cached_snapshot)
                self.assertEqual(PersonalRank(1, 3, new_value), live_rank)


if __name__ == "__main__":
    unittest.main()
