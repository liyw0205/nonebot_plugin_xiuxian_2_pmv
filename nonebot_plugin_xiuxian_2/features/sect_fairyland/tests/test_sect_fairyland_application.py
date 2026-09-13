from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import SectFairylandApplication


class _Repository:
    def __init__(self, status="claimed"):
        self.status = status
        self.calls = 0

    def claim(self, operation_id, user_id, sect_id, day, level, minutes):
        self.calls += 1
        return {
            "status": self.status,
            "user_id": user_id,
            "sect_id": sect_id,
            "detail": {"real_gain": 20, "new_hp": 120, "sect_bonus": 0.1},
        }


class SectFairylandApplicationTests(unittest.TestCase):
    def test_claim_replays_without_second_repository_call(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            app = SectFairylandApplication(Path(directory) / "player.db", repository=repository)
            kwargs = {"operation_id": "fairy-1", "user_id": "u", "sect_id": "1", "day": "2026-09-12", "level": 2, "minutes": 30}
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 1)
            self.assertEqual(first.granted["tianti_hp"], 20)

    def test_already_claimed_is_a_stable_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository("already_claimed")
            app = SectFairylandApplication(Path(directory) / "player.db", repository=repository)
            kwargs = {"operation_id": "fairy-2", "user_id": "u", "sect_id": "1", "day": "2026-09-12", "level": 2, "minutes": 30}
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertFalse(first.ok)
            self.assertEqual(first.code, "already_claimed")
            self.assertEqual(second.code, "already_claimed")
            self.assertEqual(repository.calls, 1)


if __name__ == "__main__":
    unittest.main()
