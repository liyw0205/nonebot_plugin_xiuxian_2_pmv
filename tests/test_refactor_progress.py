from __future__ import annotations

import unittest

from scripts.check_full_refactor_progress import _slice_status


class RefactorProgressTests(unittest.TestCase):
    def test_progress_report_tracks_arena_cutovers_and_active_blockers(self) -> None:
        slices = _slice_status()
        arena = slices["arena"]
        self.assertTrue(arena["state_application_owned"])
        self.assertTrue(arena["legacy_state_owner_disabled"])
        self.assertTrue(arena["weekly_rank_application_owned"])
        self.assertTrue(arena["legacy_scheduler_disabled"])
        self.assertTrue(arena["daily_reward_application_owned"])
        self.assertTrue(arena["legacy_daily_reward_disabled"])
        self.assertFalse(slices["sign_in"]["lottery_compatibility_fallback"])
        tower = slices["tower"]
        self.assertTrue(tower["state_application_owned"])
        self.assertTrue(tower["legacy_state_owner_disabled"])
        training = slices["training"]
        self.assertTrue(training["state_application_owned"])
        self.assertTrue(training["legacy_state_owner_disabled"])
        work = slices["work"]
        self.assertTrue(work["daily_refresh_application_owned"])
        self.assertTrue(work["legacy_daily_refresh_disabled"])


if __name__ == "__main__":
    unittest.main()
