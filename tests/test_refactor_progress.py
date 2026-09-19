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
        activity = slices["activity_reward"]
        self.assertTrue(activity["claim_all_application_owned"])
        self.assertTrue(activity["legacy_claim_all_disabled"])
        dungeon_team = slices["dungeon_team"]
        self.assertTrue(dungeon_team["create_invite_application_owned"])
        self.assertTrue(dungeon_team["legacy_create_invite_disabled"])
        bank = slices["bank"]
        self.assertTrue(bank["deposit_application_owned"])
        self.assertTrue(bank["withdrawal_application_owned"])
        self.assertTrue(bank["upgrade_application_owned"])
        self.assertTrue(bank["interest_application_owned"])
        self.assertTrue(bank["legacy_deposit_disabled"])
        self.assertTrue(bank["legacy_withdrawal_disabled"])
        self.assertTrue(bank["legacy_upgrade_disabled"])
        self.assertTrue(bank["legacy_interest_disabled"])
        map_slice = slices["map"]
        self.assertTrue(map_slice["interactive_application_owned"])
        self.assertTrue(map_slice["resource_application_owned"])
        self.assertTrue(map_slice["legacy_interactive_disabled"])
        self.assertTrue(map_slice["legacy_resource_disabled"])
        sect = slices["sect"]
        self.assertTrue(sect["membership_application_owned"])
        self.assertTrue(sect["economy_application_owned"])
        self.assertTrue(sect["legacy_membership_disabled"])
        natal = slices["natal_treasure"]
        self.assertTrue(natal["awaken_application_owned"])
        self.assertTrue(natal["legacy_awaken_disabled"])


if __name__ == "__main__":
    unittest.main()
