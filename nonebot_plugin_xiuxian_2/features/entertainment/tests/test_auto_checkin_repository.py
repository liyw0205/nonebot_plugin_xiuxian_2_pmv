import json
import tempfile
import unittest
from pathlib import Path

from ..repository import EntertainmentRepository


class EntertainmentAutoCheckinRepositoryTests(unittest.TestCase):
    def test_toggle_auto_checkin_updates_only_selected_account(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            state.write_text(json.dumps([
                {"api_user_id": "11", "auto_checkin": False, "secret": "hidden"},
                {"api_user_id": "22", "auto_checkin": True, "secret": "hidden2"},
            ]), encoding="utf-8")
            repo = EntertainmentRepository(Path(temp) / "game.db")
            result = repo.toggle_auto_checkin(str(state), 1)
            self.assertEqual("applied", result["status"])
            rows = json.loads(state.read_text(encoding="utf-8"))
            self.assertTrue(rows[0]["auto_checkin"])
            self.assertTrue(rows[1]["auto_checkin"])
            self.assertEqual("hidden", rows[0]["secret"])

    def test_toggle_auto_checkin_rejects_out_of_range_without_write(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "accounts.json"
            original = [{"api_user_id": "11", "auto_checkin": False}]
            state.write_text(json.dumps(original), encoding="utf-8")
            repo = EntertainmentRepository(Path(temp) / "game.db")
            result = repo.toggle_auto_checkin(str(state), 2)
            self.assertEqual("rejected", result["status"])
            self.assertEqual(original, json.loads(state.read_text(encoding="utf-8")))
