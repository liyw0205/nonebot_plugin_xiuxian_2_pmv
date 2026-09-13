from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from ..application import TiantiTrainingApplication


class _Repository:
    def __init__(self):
        self.calls = []

    def train(self, operation_id, user_id, requested_stone):
        self.calls.append(("train", operation_id))
        return {"status": "trained", "user_id": user_id, "stone_cost": requested_stone, "hp_gain": requested_stone // 10, "new_hp": 20}

    def apply_bath(self, operation_id, user_id, consume_plan, effect, slot_name, started_at, duration_minutes, *, sect_fairyland_level=0):
        self.calls.append(("bath", operation_id))
        return {"status": "applied", "user_id": user_id, "consumed": list(consume_plan), "effect": effect, "bath_name": slot_name, "end_time": "2026-09-12 16:00:00", "settlement": {"status": "ok"}}

    def breakthrough(self, operation_id, user_id, *, cultivation_rank, roll_success):
        self.calls.append(("breakthrough", operation_id))
        return {"status": "completed", "user_id": user_id, "old_level": "初境", "new_level": "次境" if roll_success else "初境", "hp_cost": 10, "new_hp": 90, "success": roll_success}

    def open_qiaoxue(self, operation_id, user_id, roll):
        self.calls.append(("qiaoxue", operation_id))
        return {"status": "opened", "user_id": user_id, "qiaoxue": {"name": "窍一", "effect_type": "hp_gain_pct", "effect_value": 0.01}, "hp_cost": 10, "new_hp": 90, "opened_count": 1, "unlock_limit": 3}


class TiantiTrainingApplicationTests(unittest.TestCase):
    def test_all_actions_are_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repository = _Repository()
            app = TiantiTrainingApplication(root / "game.db", root / "player.db", repository=repository)
            first = app.train(operation_id="train-1", user_id="u", requested_stone=100)
            replay = app.train(operation_id="train-1", user_id="u", requested_stone=100)
            self.assertTrue(first.ok)
            self.assertTrue(replay.replayed)
            self.assertEqual(repository.calls, [("train", "train-1")])

            bath = app.apply_bath(
                operation_id="bath-1", user_id="u", consume_plan=({"item_id": 1, "name": "药材", "amount": 2},),
                effect=1.5, slot_name="午酉血火", started_at=datetime(2026, 9, 12, 10), duration_minutes=360,
            )
            breakthrough = app.breakthrough(operation_id="break-1", user_id="u", cultivation_rank=1, roll_success=True)
            qiaoxue = app.open_qiaoxue(operation_id="q-1", user_id="u", roll=0)
            self.assertTrue(all(item.ok for item in (bath, breakthrough, qiaoxue)))

    def test_rejection_is_recorded_without_calling_again_on_replay(self):
        class Rejecting(_Repository):
            def train(self, operation_id, user_id, requested_stone):
                self.calls.append(("train", operation_id))
                return {"status": "stone_insufficient", "user_id": user_id}

        with tempfile.TemporaryDirectory() as directory:
            repository = Rejecting()
            app = TiantiTrainingApplication(Path(directory) / "game.db", Path(directory) / "player.db", repository=repository)
            first = app.train(operation_id="reject-1", user_id="u", requested_stone=100)
            second = app.train(operation_id="reject-1", user_id="u", requested_stone=100)
            self.assertFalse(first.ok)
            self.assertEqual(second.code, "stone_insufficient")
            self.assertEqual(repository.calls, [("train", "reject-1")])


if __name__ == "__main__":
    unittest.main()
