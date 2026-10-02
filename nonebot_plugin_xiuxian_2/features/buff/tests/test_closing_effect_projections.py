import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..closing_statistics import ClosingStatisticsRepository
from ..migrations import apply_closing_effects_player
from ....compatibility.buff_closing_effects import log_closing_event_once


class ClosingEffectProjectionTests(unittest.TestCase):
    def test_statistics_and_json_log_replay_are_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            player_db = root / "player.db"
            with DatabaseUnitOfWork(player_db) as uow:
                apply_closing_effects_player(uow)

            stats = ClosingStatisticsRepository(player_db)
            increments = {"闭关时长": 45, "闭关修为": 30, "闭关灵石消耗": 5}
            self.assertTrue(stats.record(
                event_id="closing:stats",
                user_id="u",
                increments=increments,
                occurred_at="2026-10-02T10:00:00+08:00",
            ))
            self.assertFalse(stats.record(
                event_id="closing:stats",
                user_id="u",
                increments=increments,
                occurred_at="2026-10-02T10:00:00+08:00",
            ))

            first_log = log_closing_event_once(
                user_id="u",
                message="[出关] 闭关45分钟",
                event_id="closing:log",
                occurred_at="2026-10-02T10:00:00+08:00",
                player_database=player_db,
                players_dir=root / "players",
            )
            second_log = log_closing_event_once(
                user_id="u",
                message="[出关] 闭关45分钟",
                event_id="closing:log",
                occurred_at="2026-10-02T10:00:00+08:00",
                player_database=player_db,
                players_dir=root / "players",
            )

            self.assertTrue(first_log)
            self.assertFalse(second_log)
            log_path = root / "players" / "u" / "logs" / "261002.log"
            self.assertEqual(1, len(json.loads(log_path.read_text(encoding="utf-8"))))
            with DatabaseUnitOfWork(player_db, read_only=True) as uow:
                row = uow.query_one(
                    'SELECT "闭关时长","闭关修为","闭关灵石消耗" FROM statistics WHERE user_id=?',
                    ("u",),
                )
                self.assertEqual((45, 30, 5), tuple(row.values()))
                self.assertEqual(3, uow.query_one("SELECT COUNT(*) AS n FROM closing_statistics_events")["n"])


if __name__ == "__main__":
    unittest.main()
