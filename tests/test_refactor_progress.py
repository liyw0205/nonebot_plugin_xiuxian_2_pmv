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
        world_events = slices["world_events"]
        self.assertTrue(world_events["claim_application_owned"])
        self.assertTrue(world_events["legacy_claim_disabled"])
        rift = slices["rift"]
        self.assertTrue(rift["entry_application_owned"])
        self.assertTrue(rift["legacy_entry_disabled"])
        back = slices["back"]
        self.assertTrue(back["repair_application_owned"])
        self.assertTrue(back["legacy_repair_disabled"])
        past_life = slices["past_life"]
        self.assertTrue(past_life["reset_one_application_owned"])
        self.assertTrue(past_life["legacy_reset_one_disabled"])
        dufang = slices["dufang"]
        self.assertTrue(dufang["share_application_owned"])
        self.assertTrue(dufang["legacy_share_disabled"])
        fusion = slices["fusion"]
        self.assertTrue(fusion["single_application_owned"])
        self.assertTrue(fusion["legacy_single_disabled"])
        self.assertTrue(fusion["batch_application_owned"])
        self.assertTrue(fusion["legacy_batch_disabled"])
        title = slices["title"]
        self.assertTrue(title["equip_replay_application_owned"])
        self.assertTrue(title["legacy_equip_replay_disabled"])
        self.assertTrue(title["legacy_unequip_replay_disabled"])
        self.assertTrue(title["unlock_batch_application_owned"])
        self.assertTrue(title["legacy_unlock_batch_disabled"])
        base = slices["base"]
        self.assertTrue(base["rename_application_owned"])
        self.assertTrue(base["legacy_rename_disabled"])
        puppet = slices["puppet"]
        self.assertTrue(puppet["harvest_application_owned"])
        self.assertTrue(puppet["legacy_harvest_disabled"])
        pet = slices["pet"]
        self.assertTrue(pet["active_switch_application_owned"])
        self.assertTrue(pet["legacy_active_switch_disabled"])
        trade = slices["trade"]
        self.assertTrue(trade["purchase_application_owned"])
        self.assertTrue(trade["legacy_purchase_disabled"])
        boss = slices["boss"]
        self.assertTrue(boss["manual_spawn_application_owned"])
        self.assertTrue(boss["legacy_manual_spawn_disabled"])
        buff = slices["buff"]
        self.assertTrue(buff["blessed_open_application_owned"])
        self.assertTrue(buff["legacy_blessed_open_disabled"])
        self.assertTrue(buff["blessed_rename_application_owned"])
        self.assertTrue(buff["legacy_blessed_rename_disabled"])
        self.assertTrue(buff["blessed_upgrade_application_owned"])
        self.assertTrue(buff["legacy_blessed_upgrade_disabled"])
        impart = slices["impart"]
        self.assertTrue(impart["love_sand_application_owned"])
        self.assertTrue(impart["card_compose_application_owned"])
        self.assertTrue(impart["card_disassemble_application_owned"])
        mixelixir = slices["mixelixir"]
        self.assertTrue(mixelixir["harvest_level_application_owned"])
        self.assertTrue(mixelixir["legacy_harvest_level_disabled"])
        self.assertTrue(slices["dongfu"]["plant_application_owned"])
        self.assertTrue(slices["dongfu"]["harvest_application_owned"])
        self.assertTrue(slices["dongfu"]["fertilize_application_owned"])
        self.assertTrue(slices["dongfu"]["accelerate_application_owned"])
        self.assertTrue(slices["dongfu"]["patrol_application_owned"])
        self.assertTrue(slices["impart_pk"]["training_replay_application_owned"])
        self.assertTrue(slices["impart_pk"]["legacy_training_replay_disabled"])
        self.assertTrue(slices["impart_pk"]["closing_enter_replay_application_owned"])
        self.assertTrue(slices["impart_pk"]["legacy_closing_enter_replay_disabled"])
        self.assertTrue(slices["impart_pk"]["closing_settlement_replay_application_owned"])
        self.assertTrue(slices["impart_pk"]["legacy_closing_settlement_replay_disabled"])
        self.assertTrue(slices["impart_pk"]["explore_replay_application_owned"])
        self.assertTrue(slices["impart_pk"]["legacy_explore_replay_disabled"])
        self.assertTrue(slices["impart_pk"]["battle_replay_application_owned"])
        self.assertTrue(slices["impart_pk"]["legacy_battle_replay_disabled"])
        self.assertTrue(slices["admin"]["item_destroy_application_owned"])
        self.assertTrue(slices["admin"]["exp_adjust_application_owned"])
        self.assertTrue(slices["admin"]["level_change_application_owned"])
        self.assertTrue(slices["admin"]["root_change_application_owned"])
        self.assertTrue(slices["admin"]["impart_stone_application_owned"])
        self.assertTrue(slices["admin"]["accessory_application_owned"])
        self.assertTrue(slices["admin"]["player_status_batch_application_owned"])
        self.assertTrue(slices["admin"]["item_batch_application_owned"])
        self.assertTrue(slices["admin"]["accessory_batch_application_owned"])
        self.assertTrue(slices["admin"]["impart_stone_batch_application_owned"])
        self.assertTrue(slices["admin"]["blackhouse_application_owned"])
        self.assertTrue(slices["admin"]["player_status_application_owned"])
        self.assertTrue(slices["impart"]["prayer_application_owned"])
        self.assertTrue(slices["rift"]["speedup_application_owned"])
        self.assertTrue(slices["rift"]["settlement_application_owned"])
        self.assertTrue(slices["boss"]["daily_limit_application_owned"])
        self.assertTrue(slices["boss"]["punishment_application_owned"])
        self.assertTrue(slices["buff"]["stone_training_application_owned"])
        self.assertTrue(slices["buff"]["training_lifecycle_application_owned"])
        self.assertTrue(slices["buff"]["closing_settlement_application_owned"])
        self.assertTrue(slices["buff"]["pvp_application_owned"])
        self.assertTrue(slices["back"]["equipment_unequip_application_owned"])
        self.assertTrue(slices["back"]["equipment_equip_application_owned"])
        self.assertTrue(slices["back"]["pet_egg_application_owned"])
        self.assertTrue(slices["back"]["package_application_owned"])
        self.assertTrue(slices["back"]["accessory_package_application_owned"])
        self.assertTrue(slices["natal_treasure"]["effect_upgrade_application_owned"])
        self.assertTrue(slices["world_events"]["attack_application_owned"])
        self.assertTrue(slices["sect"]["daily_maintenance_application_owned"])
        self.assertTrue(slices["rift"]["key_event_application_owned"])
        self.assertTrue(slices["dongfu"]["array_upgrade_application_owned"])
if __name__ == "__main__":
    unittest.main()
