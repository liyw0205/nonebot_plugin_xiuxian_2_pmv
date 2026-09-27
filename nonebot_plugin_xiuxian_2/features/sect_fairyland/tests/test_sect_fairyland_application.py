from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ..application import SectFairylandApplication


class _Repository:
    def __init__(self, status="claimed"):
        self.status = status
        self.calls = 0
        self.last_claim_day = "2026-09-12"

    def claim(self, operation_id, user_id, sect_id, day, level, minutes):
        self.calls += 1
        status = "duplicate" if self.calls > 1 and self.status == "claimed" else self.status
        return {
            "status": status,
            "user_id": user_id,
            "sect_id": sect_id,
            "detail": {"real_gain": 20, "new_hp": 120, "sect_bonus": 0.1},
        }

    def get_last_claim_day(self, user_id, sect_id):
        self.status_query = (user_id, sect_id)
        return self.last_claim_day


class SectFairylandApplicationTests(unittest.TestCase):
    def test_repository_receipt_marks_duplicate_as_replayed_without_outer_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            repository = _Repository()
            database = Path(directory) / "player.db"
            app = SectFairylandApplication(database, repository=repository)
            kwargs = {"operation_id": "fairy-1", "user_id": "u", "sect_id": "1", "day": "2026-09-12", "level": 2, "minutes": 30}
            first = app.claim(**kwargs)
            second = app.claim(**kwargs)
            self.assertTrue(first.ok)
            self.assertTrue(second.replayed)
            self.assertEqual(repository.calls, 2)
            self.assertEqual(first.granted["tianti_hp"], 20)
            self.assertFalse(database.exists())

    def test_default_repository_is_feature_owned(self):
        with tempfile.TemporaryDirectory() as directory:
            app = SectFairylandApplication(Path(directory) / "player.db")
            self.assertEqual(type(app._repository()).__name__, "SectFairylandSqlRepository")

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
            self.assertEqual(repository.calls, 2)

    def test_status_read_is_delegated_with_normalized_ids(self):
        repository = _Repository()
        app = SectFairylandApplication(Path("unused.db"), repository=repository)

        self.assertEqual(app.get_last_claim_day(" u ", " 1 "), "2026-09-12")
        self.assertEqual(repository.status_query, ("u", "1"))


if __name__ == "__main__":
    unittest.main()
