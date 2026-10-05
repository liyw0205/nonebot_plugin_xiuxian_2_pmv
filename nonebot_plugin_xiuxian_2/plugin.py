"""Composition root for the refactored runtime.

The historical package initializer still loads legacy feature packages for
backward compatibility.  New code should use this module as the only place to
assemble the new application boundary; it deliberately contains no gameplay
rules or SQL.
"""

from __future__ import annotations

from pathlib import Path
import os
from typing import Any

from .bootstrap import FeatureRegistry, Lifecycle, LifecyclePhase, Readiness, RuntimeContext, build_runtime_context
from .bootstrap.legacy import run_legacy_shutdown, run_legacy_startup
from .bootstrap.platform_manifest import FEATURE as PLATFORM_WEB_FEATURE
from .features.daily_fortune.manifest import FEATURE as DAILY_FORTUNE_FEATURE
from .features.daily_fortune.migrations import apply_daily_fortune
from .features.illusion.manifest import FEATURE as ILLUSION_FEATURE
from .features.illusion.migrations import apply_illusion
from .features.impart.migrations import (
    apply_impart_love_sand_operations,
    apply_impart_love_sand_player_statistics,
    apply_impart_prayer_operations,
    apply_impart_prayer_player_statistics,
)
from .features.interactive.manifest import FEATURE as INTERACTIVE_FEATURE
from .features.interactive.migrations import apply_interactive
from .features.info.migrations import apply_avatar_identity_player, apply_avatar_initialization_player
from .features.beg.manifest import FEATURE as BEG_FEATURE
from .features.beg.migrations import apply_beg
from .features.beg.application import BegApplication
from .features.title.manifest import FEATURE as TITLE_FEATURE
from .features.title.migrations import apply_title, apply_title_schema
from .features.title.application import TitleApplication
from .features.sign_in.manifest import FEATURE as SIGN_IN_FEATURE
from .features.sign_in.migrations import apply_lottery, apply_lottery_audit, apply_sign_in, apply_sign_in_statistics, apply_sign_in_tasks
from .features.tasks.application import TaskClaimApplication
from .features.tasks.migrations import (
    apply_task_claim,
    apply_task_claim_player,
    apply_task_claim_recovery,
    apply_task_progress,
)
from .features.stone_gift.manifest import FEATURE as STONE_GIFT_FEATURE
from .features.stone_gift.migrations import apply_stone_gift, apply_stone_gift_limits
from .features.package_reward.manifest import FEATURE as PACKAGE_REWARD_FEATURE
from .features.package_reward.migrations import apply_package_reward
from .features.pet.manifest import FEATURE as PET_FEATURE
from .features.pet.migrations import apply_pet, apply_pet_hatch, apply_pet_skill_replace, apply_pet_travel_claim
from .features.game_events.migrations import apply_game_event_statistics_player
from .features.sect.manifest import FEATURE as SECT_FEATURE
from .features.sect.migrations import apply_sect, apply_sect_rename, apply_sect_join, apply_sect_removal, apply_sect_position, apply_sect_donation, apply_sect_shop, apply_sect_mainbuff, apply_sect_secbuff, apply_sect_elixir, apply_sect_weekly, apply_sect_weekly_player, apply_sect_manual_disband
from .features.sect.migrations import apply_sect_fairyland_upgrade
from .features.sect.migrations import apply_sect_scheduled_materials
from .features.sect.migrations import (
    apply_sect_task_claim_operations,
    apply_sect_task_settlement_operations,
    apply_sect_task_state,
)
from .features.natal_treasure.manifest import FEATURE as NATAL_TREASURE_FEATURE
from .features.natal_treasure.migrations import apply_natal_treasure
from .features.buff.manifest import FEATURE as BUFF_FEATURE
from .features.buff.migrations import (
    apply_buff,
    apply_partner_cultivation_operations,
    apply_partner_cultivation_player_schema,
    apply_partner_token_operations,
    apply_partner_token_usage,
    apply_normal_pvp_operations,
    apply_normal_pvp_player_statistics,
    apply_closing_settlement_game,
    apply_closing_effects_player,
    apply_normal_training_game,
    apply_normal_training_player,
)
from .features.base.manifest import FEATURE as BASE_FEATURE
from .features.base.migrations import (
    apply_base,
    apply_base_direct_breakthrough_operations,
    apply_base_direct_breakthrough_plans,
    apply_base_direct_breakthrough_player,
    apply_base_player_rename_operations,
    apply_base_root_reroll_operations,
    apply_base_stone_contest_operations,
    apply_base_stone_robbery_operations,
    apply_base_stone_robbery_player_statistics,
    apply_base_xiangyuan,
    apply_base_xiangyuan_player,
)
from .features.back.manifest import FEATURE as BACK_FEATURE
from .features.back.migrations import apply_accessory_affix_operations, apply_alchemy, apply_back, apply_backpack_repair, apply_blessed_flag_replace, apply_breakthrough_rate_item, apply_cultivation_item, apply_equipment, apply_item_use, apply_lottery_talisman, apply_permanent_atk_item, apply_pet_egg_use, apply_recovery_item, apply_skill_learning, apply_stone_reward, apply_three_cultivation_pill, apply_unbind
from .features.trade.manifest import FEATURE as TRADE_FEATURE
from .features.trade.migrations import (
    apply_trade,
    apply_trade_guishi_deposit,
    apply_trade_guishi_schema,
    apply_trade_guishi_withdraw,
    apply_trade_guishi_qiugou,
    apply_trade_guishi_order_cancel,
    apply_trade_guishi_match,
    apply_trade_guishi_expired_cleanup,
    apply_trade_guishi_take_item,
    apply_trade_xianshi_listing,
    apply_trade_xianshi_plan_listing,
    apply_trade_xianshi_removal,
    apply_trade_xianshi_purchase,
)
from .features.map.manifest import FEATURE as MAP_FEATURE
from .features.map.migrations import apply_map, apply_map_combat_plan, apply_map_combat_player, apply_map_combat_start, apply_map_dongfu_build, apply_map_dongfu_player, apply_map_dongfu_status_schema, apply_map_explore_player, apply_map_explore_settlement, apply_map_explore_start, apply_map_home_return, apply_map_interactive_player, apply_map_interactive_start, apply_map_mission_claim, apply_map_movement, apply_map_resource_reward, apply_map_seed_purchase
from .features.rift.manifest import FEATURE as RIFT_FEATURE
from .features.rift.migrations import apply_rift, apply_rift_demon_token_operations, apply_rift_demon_token_player_schema, apply_rift_speedup_operations, apply_rift_world_generation, apply_rift_termination_operations, apply_rift_key_event_operations, apply_rift_settlement_operations, apply_rift_entry_schema
from .features.accessory_package.manifest import FEATURE as ACCESSORY_PACKAGE_FEATURE
from .features.accessory_package.migrations import apply_accessory_package
from .features.arena.manifest import FEATURE as ARENA_FEATURE
from .features.arena.migrations import apply_arena, apply_arena_challenge_purchase, apply_arena_challenge_ticket, apply_arena_daily_reward_player, apply_arena_purchase, apply_arena_season_reward, apply_arena_settlement, apply_arena_state, apply_arena_weekly_rank_reduction
from .features.auction.manifest import FEATURE as AUCTION_FEATURE
from .features.auction.jobs import settle as auction_settle_job
from .features.bank.manifest import FEATURE as BANK_FEATURE
from .features.bank.migrations import apply_bank, apply_bank_accounts, apply_bank_legacy_accounts
from .features.activity_reward.manifest import FEATURE as ACTIVITY_REWARD_FEATURE
from .features.activity_reward.migrations import (
    apply_activity_claim_all,
    apply_activity_claim_all_legacy_receipts,
    apply_activity_reward,
    apply_activity_task_claim,
    apply_activity_task_claim_legacy_receipts,
    apply_activity_pass_claim,
    apply_activity_pass_claim_legacy_receipts,
    apply_activity_boss_milestone_claim,
    apply_activity_boss_milestone_legacy_receipts,
    apply_activity_boss_rank_claim,
    apply_activity_boss_rank_legacy_receipts,
)
from .features.activity.migrations import (
    apply_activity_state_schema,
    apply_activity_state_legacy,
    apply_activity_event_receipts,
)
from .features.combat_settlement.manifest import FEATURE as COMBAT_SETTLEMENT_FEATURE
from .features.combat_settlement.migrations import apply_combat_settlement, apply_combat_settlement_operations, apply_dao_battle_operations, apply_dao_battle_record
from .features.admin_asset.manifest import FEATURE as ADMIN_ASSET_FEATURE
from .features.admin_asset.migrations import apply_admin_accessory_batch, apply_admin_accessory_operations, apply_admin_asset, apply_admin_exp_adjustment, apply_admin_impart_stone_batch, apply_admin_impart_stone_operations, apply_admin_item_batch, apply_admin_item_destroy, apply_admin_item_grant, apply_admin_realm_changes, apply_admin_stone_adjustment, apply_admin_stone_batch
from .features.tianti_settlement.manifest import FEATURE as TIANTI_SETTLEMENT_FEATURE
from .features.tianti_settlement.migrations import apply_tianti_settlement, apply_tianti_settlement_operations
from .features.tianti_training.manifest import FEATURE as TIANTI_TRAINING_FEATURE
from .features.tianti_training.migrations import apply_tianti_breakthrough_operations, apply_tianti_item_reward_operations, apply_tianti_medicine_bath_operations, apply_tianti_player_info, apply_tianti_qiaoxue_operations, apply_tianti_training, apply_tianti_training_operations, apply_training_state
from .features.training.migrations import apply_training_event_operations, apply_training_event_player, apply_training_purchase_operations, apply_training_reset_operations
from .features.tower.manifest import FEATURE as TOWER_FEATURE
from .features.tower.migrations import apply_tower, apply_tower_purchase, apply_tower_settlement, apply_tower_state
from .features.sect_fairyland.manifest import FEATURE as SECT_FAIRYLAND_FEATURE
from .features.sect_fairyland.migrations import apply_sect_fairyland, apply_sect_fairyland_player
from .features.world_events.manifest import FEATURE as WORLD_EVENTS_FEATURE
from .features.world_events.migrations import (
    apply_world_events,
    apply_world_events_claim,
    apply_world_events_lifecycle,
    apply_world_events_player,
    apply_world_events_spirit_vein_lifecycle,
    apply_world_events_wave_refresh,
)
from .features.work.manifest import FEATURE as WORK_FEATURE
from .features.work.migrations import (
    apply_work,
    apply_work_daily_refresh_reset,
    apply_work_item_use,
    apply_work_offer_snapshots,
    apply_work_refresh_operations,
    apply_work_abort_cleanup,
    apply_work_claim_operations,
    apply_work_settlement_operations,
)
from .features.mixelixir.manifest import FEATURE as MIXELIXIR_FEATURE
from .features.mixelixir.migrations import (
    apply_mixelixir,
    apply_mixelixir_refine_claim,
    apply_mixelixir_refine_claim_player,
)
from .features.puppet.manifest import FEATURE as PUPPET_FEATURE
from .features.puppet.migrations import apply_puppet, apply_puppet_status
from .features.boss.manifest import FEATURE as BOSS_FEATURE
from .features.boss.migrations import apply_boss, apply_boss_battle_player_schema, apply_boss_full_refresh_player_schema, apply_boss_player_schema, apply_boss_purchase, apply_boss_settlement
from .features.dungeon.manifest import FEATURE as DUNGEON_FEATURE
from .features.dungeon.migrations import apply_dungeon, apply_dungeon_explore, apply_dungeon_explore_player_schema, apply_dungeon_explore_resolution_intent, apply_dungeon_purchase, apply_dungeon_session, apply_dungeon_team, apply_dungeon_team_invite_expiry, apply_dungeon_team_members_index, apply_dungeon_team_schema
from .features.fusion.migrations import apply_fusion_operations
from .features.dongfu.migrations import (
    apply_dongfu_infiltrate_failure,
    apply_dongfu_infiltrate_success,
    apply_dongfu_operations,
)
from .features.auction.migrations import (
    apply_auction,
    apply_auction_settlement,
    apply_auction_queue_operations,
    apply_auction_player_queue,
    apply_auction_bid_operations,
    apply_auction_bid_statistics,
    apply_auction_settlement_game_effects,
    apply_auction_settlement_statistics,
)
from .features._legacy_migrated import (
    APPLICATIONS as LEGACY_MIGRATED_APPLICATIONS,
    FEATURES as LEGACY_MIGRATED_FEATURES,
    MIGRATIONS as LEGACY_MIGRATIONS,
)
from .compatibility.legacy_manifest import FEATURE as LEGACY_SCHEDULER_FEATURE
from .compatibility.legacy_jobs import legacy_job_handler
from .compatibility import compatibility_hits
from .compatibility.feature_inventory import FEATURES as LEGACY_FEATURES
from .infrastructure.database import DatabaseUnitOfWork, Migration, MigrationRunner, OperationLedger, OutboxStore
from .infrastructure.scheduler import JobExecutor, JobRegistry


def apply_platform_schema(uow: DatabaseUnitOfWork) -> None:
    """Create shared operation/outbox tables during startup migration."""
    OperationLedger().ensure_schema(uow)
    OutboxStore().ensure_schema(uow)
    uow.execute(
        "CREATE INDEX IF NOT EXISTS direct_breakthrough_pending_outbox "
        "ON domain_outbox(attempts,created_at,event_id) "
        "WHERE event_type='base.direct_breakthrough.effects' AND status='pending'"
    )


def build_migrations() -> tuple[Migration, ...]:
    """Return the one migration catalog used by every runtime entry point.

    Keeping this list in the composition root prevents the maintenance CLI,
    startup lifecycle and recovery rehearsal from drifting apart.
    """
    return (
        Migration("accessory_package.001", "accessory_package_operations", apply_accessory_package),
        Migration("activity_reward.001", "activity_reward_feature_migrations", apply_activity_reward),
        Migration("activity_reward.002", "activity_claim_all_operations", apply_activity_claim_all),
        Migration("activity_reward.003", "activity_claim_all_legacy_receipts", apply_activity_claim_all_legacy_receipts),
        Migration("activity_reward.004", "activity_task_reward_claims", apply_activity_task_claim),
        Migration("activity_reward.005", "activity_task_reward_legacy_receipts", apply_activity_task_claim_legacy_receipts),
        Migration("activity_reward.006", "activity_pass_reward_claims", apply_activity_pass_claim),
        Migration("activity_reward.007", "activity_pass_reward_legacy_receipts", apply_activity_pass_claim_legacy_receipts),
        Migration("activity_reward.008", "activity_boss_milestone_reward_claims", apply_activity_boss_milestone_claim),
        Migration("activity_reward.009", "activity_boss_milestone_legacy_receipts", apply_activity_boss_milestone_legacy_receipts),
        Migration("activity_reward.010", "activity_boss_rank_reward_claims", apply_activity_boss_rank_claim),
        Migration("activity_reward.011", "activity_boss_rank_legacy_receipts", apply_activity_boss_rank_legacy_receipts),
        Migration("activity_state.001", "activity_state_schema", apply_activity_state_schema),
        Migration("activity_state.002", "activity_state_legacy_backfill", apply_activity_state_legacy),
        Migration("activity_state.003", "activity_event_receipts", apply_activity_event_receipts),
        Migration("admin_asset.001", "admin_asset_feature_migrations", apply_admin_asset),
        Migration("admin_asset.002", "admin_stone_adjustment_operations", apply_admin_stone_adjustment),
        Migration("admin_asset.003", "admin_stone_batch_adjustment_operations", apply_admin_stone_batch),
        Migration("admin_asset.004", "admin_exp_adjustment_operations", apply_admin_exp_adjustment),
        Migration("admin_asset.005", "admin_item_destroy_operations", apply_admin_item_destroy),
        Migration("admin_asset.006", "admin_item_grant_operations", apply_admin_item_grant),
        Migration("admin_asset.007", "admin_level_root_change_operations", apply_admin_realm_changes),
        Migration("admin_asset.008", "admin_impart_stone_operations", apply_admin_impart_stone_operations),
        Migration("admin_asset.009", "admin_accessory_operations", apply_admin_accessory_operations),
        Migration("admin_asset.010", "admin_accessory_batch_operations", apply_admin_accessory_batch),
        Migration("admin_asset.011", "admin_impart_stone_batch_operations", apply_admin_impart_stone_batch),
        Migration("admin_asset.012", "admin_item_batch_operations", apply_admin_item_batch),
        Migration("arena.001", "arena_feature_migrations", apply_arena),
        Migration("arena.002", "arena_challenge_purchase_operations", apply_arena_challenge_purchase),
        Migration("arena.003", "arena_purchase_operations", apply_arena_purchase),
        Migration("arena.004", "arena_challenge_ticket_operations", apply_arena_challenge_ticket),
        Migration("arena.005", "arena_challenge_settlement_operations", apply_arena_settlement),
        Migration("arena.006", "arena_weekly_rank_reduction_operations", apply_arena_weekly_rank_reduction),
        Migration("arena.007", "arena_season_reward_operations", apply_arena_season_reward),
        Migration("arena.008", "arena_daily_reward_player_schema", apply_arena_daily_reward_player),
        Migration("arena.009", "arena_state_operations", apply_arena_state),
        Migration("auction.001", "auction_feature_migrations", apply_auction),
        Migration("auction.002", "auction_session_settlement_schema", apply_auction_settlement),
        Migration("auction.003", "auction_queue_operations", apply_auction_queue_operations),
        Migration("auction.004", "auction_player_upload", apply_auction_player_queue),
        Migration("auction.005", "auction_bid_operations", apply_auction_bid_operations),
        Migration("auction.006", "auction_bid_statistics_projection", apply_auction_bid_statistics),
        Migration("auction.007", "auction_settlement_game_effect_receipts", apply_auction_settlement_game_effects),
        Migration("auction.008", "auction_settlement_statistics_projection", apply_auction_settlement_statistics),
        Migration("back.001", "back_feature_migrations", apply_back),
        Migration("back.002", "alchemy_operations", apply_alchemy),
        Migration("back.003", "unbind_item_operations", apply_unbind),
        Migration("back.004", "cultivation_item_operations", apply_cultivation_item),
        Migration("back.005", "skill_learning_operations", apply_skill_learning),
        Migration("back.006", "lottery_talisman_operations", apply_lottery_talisman),
        Migration("back.007", "stone_item_reward_operations", apply_stone_reward),
        Migration("back.008", "three_cultivation_pill_operations", apply_three_cultivation_pill),
        Migration("back.009", "breakthrough_rate_item_operations", apply_breakthrough_rate_item),
        Migration("back.010", "recovery_item_operations", apply_recovery_item),
        Migration("back.011", "permanent_atk_item_operations", apply_permanent_atk_item),
        Migration("back.012", "blessed_flag_replace_operations", apply_blessed_flag_replace),
        Migration("back.013", "equipment_operations", apply_equipment),
        Migration("back.014", "backpack_repair_tasks", apply_backpack_repair),
        Migration("back.015", "batch_pet_egg_use_operations", apply_pet_egg_use),
        Migration("back.016", "back_item_use_operations", apply_item_use),
        Migration("back.017", "accessory_transaction_operations", apply_accessory_affix_operations),
        Migration("bank.001", "bank_feature_migrations", apply_bank),
        Migration("bank.002", "bank_accounts", apply_bank_accounts),
        Migration("bank.003", "bank_legacy_account_backfill", apply_bank_legacy_accounts),
        Migration("base.001", "base_feature_migrations", apply_base),
        Migration("base.002", "player_rename_operations", apply_base_player_rename_operations),
        Migration("base.003", "stone_contest_operations", apply_base_stone_contest_operations),
        Migration("base.004", "stone_robbery_operations", apply_base_stone_robbery_operations),
        Migration("base.005", "stone_robbery_player_statistics", apply_base_stone_robbery_player_statistics),
        Migration("base.006", "xiangyuan_projection", apply_base_xiangyuan),
        Migration("base.007", "xiangyuan_player_limits", apply_base_xiangyuan_player),
        Migration("base.008", "player_root_reroll_operations", apply_base_root_reroll_operations),
        Migration("base.009", "direct_breakthrough_operations", apply_base_direct_breakthrough_operations),
        Migration("base.010", "direct_breakthrough_plans", apply_base_direct_breakthrough_plans),
        Migration("base.011", "direct_breakthrough_player", apply_base_direct_breakthrough_player),
        Migration("beg.001", "beg_feature_migrations", apply_beg),
        Migration("boss.001", "boss_feature_migrations", apply_boss),
        Migration("boss.002", "boss_purchase_operations", apply_boss_purchase),
        Migration("boss.003", "world_boss_battle_operations", apply_boss_settlement),
        Migration("boss.004", "world_boss_player_schema", apply_boss_player_schema),
        Migration("boss.005", "world_boss_full_refresh_player_schema", apply_boss_full_refresh_player_schema),
        Migration("boss.006", "world_boss_battle_player_schema", apply_boss_battle_player_schema),
        Migration("buff.001", "buff_feature_migrations", apply_buff),
        Migration("buff.002", "partner_token_operations", apply_partner_token_operations),
        Migration("buff.003", "partner_two_exp_usage", apply_partner_token_usage),
        Migration("buff.004", "partner_cultivation_operations", apply_partner_cultivation_operations),
        Migration("buff.005", "partner_cultivation_player_schema", apply_partner_cultivation_player_schema),
        Migration("buff.006", "normal_pvp_operations", apply_normal_pvp_operations),
        Migration("buff.007", "normal_pvp_player_statistics", apply_normal_pvp_player_statistics),
        Migration("buff.008", "closing_settlement_outbox", apply_closing_settlement_game),
        Migration("buff.009", "closing_effects_player_statistics", apply_closing_effects_player),
        Migration("buff.010", "normal_training_operations", apply_normal_training_game),
        Migration("buff.011", "normal_training_player_statistics", apply_normal_training_player),
        Migration("combat_settlement.001", "combat_settlement_feature_migrations", apply_combat_settlement),
        Migration("combat_settlement.002", "map_combat_settlement_operations", apply_combat_settlement_operations),
        Migration("combat_settlement.003", "map_dao_battle_operations", apply_dao_battle_operations),
        Migration("combat_settlement.004", "dao_battle_record", apply_dao_battle_record),
        Migration("daily_fortune.001", "daily_fortune_claims", apply_daily_fortune),
        Migration("dongfu.002", "dongfu_infiltrate_success_operations", apply_dongfu_infiltrate_success),
        Migration("dongfu.003", "dongfu_infiltrate_failure_operations", apply_dongfu_infiltrate_failure),
        Migration("dongfu.004", "dongfu_action_operations", apply_dongfu_operations),
        Migration("dungeon.001", "dungeon_feature_migrations", apply_dungeon),
        Migration("dungeon.002", "dungeon_purchase_operations", apply_dungeon_purchase),
        Migration("dungeon.003", "dungeon_session_operations", apply_dungeon_session),
        Migration("dungeon.004", "dungeon_explore_operations", apply_dungeon_explore),
        Migration("dungeon.005", "dungeon_team_operations", apply_dungeon_team),
        Migration("dungeon.006", "dungeon_team_state_schema", apply_dungeon_team_schema),
        Migration("dungeon.007", "dungeon_explore_player_state_schema", apply_dungeon_explore_player_schema),
        Migration("dungeon.008", "dungeon_explore_resolution_intent", apply_dungeon_explore_resolution_intent),
        Migration("dungeon.009", "dungeon_team_members_index", apply_dungeon_team_members_index),
        Migration("dungeon.010", "dungeon_team_invite_expiry", apply_dungeon_team_invite_expiry),
        Migration("fusion.002", "fusion_operation_tables", apply_fusion_operations),
        Migration("game_events.001", "game_event_statistics_projection", apply_game_event_statistics_player),
        Migration("illusion.001", "illusion_feature_migrations", apply_illusion),
        Migration("impart.002", "impart_prayer_operations", apply_impart_prayer_operations),
        Migration("impart.003", "impart_prayer_player_statistics", apply_impart_prayer_player_statistics),
        Migration("impart.004", "love_sand_operations", apply_impart_love_sand_operations),
        Migration("impart.005", "love_sand_player_statistics", apply_impart_love_sand_player_statistics),
        Migration("info.avatar.001", "avatar_player_identity_and_receipts", apply_avatar_identity_player),
        Migration("info.avatar.002", "avatar_initialization_plans", apply_avatar_initialization_player),
        Migration("interactive.001", "interactive_feature_migrations", apply_interactive),
        *(Migration(version, f"{version.replace('.', '_')}_migrations", migration) for version, migration in LEGACY_MIGRATIONS),
        Migration("lottery.001", "lottery_feature_migrations", apply_lottery),
        Migration("lottery.002", "lottery_audit", apply_lottery_audit),
        Migration("map.001", "map_feature_migrations", apply_map),
        Migration("map.002", "map_movement_operations", apply_map_movement),
        Migration("map.003", "map_home_return_operations", apply_map_home_return),
        Migration("map.004", "map_interactive_start_operations", apply_map_interactive_start),
        Migration("map.005", "map_interactive_player", apply_map_interactive_player),
        Migration("map.006", "map_resource_reward_operations", apply_map_resource_reward),
        Migration("map.007", "map_explore_start_operations", apply_map_explore_start),
        Migration("map.008", "map_explore_player", apply_map_explore_player),
        Migration("map.009", "map_explore_settlement_operations", apply_map_explore_settlement),
        Migration("map.010", "map_mission_claim_operations", apply_map_mission_claim),
        Migration("map.011", "map_seed_purchase_operations", apply_map_seed_purchase),
        Migration("map.012", "map_dongfu_build_operations", apply_map_dongfu_build),
        Migration("map.013", "map_dongfu_player", apply_map_dongfu_player),
        Migration("map.014", "map_combat_start_operations", apply_map_combat_start),
        Migration("map.015", "map_combat_player", apply_map_combat_player),
        Migration("map.016", "map_combat_plan_operations", apply_map_combat_plan),
        Migration("map.017", "map_dongfu_status_schema", apply_map_dongfu_status_schema),
        Migration("mixelixir.001", "mixelixir_feature_migrations", apply_mixelixir),
        Migration("mixelixir.002", "mixelixir_refine_claim", apply_mixelixir_refine_claim),
        Migration("mixelixir.003", "mixelixir_refine_claim_player", apply_mixelixir_refine_claim_player),
        Migration("natal_treasure.001", "natal_treasure_feature_migrations", apply_natal_treasure),
        Migration("package_reward.001", "package_reward_operations", apply_package_reward),
        Migration("pet.001", "pet_feature_migrations", apply_pet),
        Migration("pet.002", "pet_hatch_operations", apply_pet_hatch),
        Migration("pet.003", "pet_skill_replace_operations", apply_pet_skill_replace),
        Migration("pet.004", "pet_travel_claim_operations", apply_pet_travel_claim),
        Migration("platform.001", "operation_ledger_outbox", apply_platform_schema),
        Migration("puppet.001", "puppet_feature_migrations", apply_puppet),
        Migration("puppet.002", "puppet_status_column", apply_puppet_status),
        Migration("rift.001", "rift_feature_migrations", apply_rift),
        Migration("rift.002", "rift_demon_token_battle_operations", apply_rift_demon_token_operations),
        Migration("rift.003", "rift_demon_token_player_schema", apply_rift_demon_token_player_schema),
        Migration("rift.004", "rift_speedup_operations", apply_rift_speedup_operations),
        Migration("rift.005", "rift_world_generation", apply_rift_world_generation),
        Migration("rift.006", "rift_termination_operations", apply_rift_termination_operations),
        Migration("rift.007", "rift_key_event_operations", apply_rift_key_event_operations),
        Migration("rift.008", "rift_settlement_operations", apply_rift_settlement_operations),
        Migration("rift.009", "rift_entry_schema", apply_rift_entry_schema),
        Migration("sect.001", "sect_feature_migrations", apply_sect),
        Migration("sect.002", "sect_rename_operations", apply_sect_rename),
        Migration("sect.003", "sect_member_join_operations", apply_sect_join),
        Migration("sect.004", "sect_member_removal_operations", apply_sect_removal),
        Migration("sect.005", "sect_position_change_operations", apply_sect_position),
        Migration("sect.006", "sect_donation_operations", apply_sect_donation),
        Migration("sect.007", "sect_shop_purchase_operations", apply_sect_shop),
        Migration("sect.008", "sect_mainbuff_learn_operations", apply_sect_mainbuff),
        Migration("sect.009", "sect_secbuff_learn_operations", apply_sect_secbuff),
        Migration("sect.010", "sect_elixir_claim_operations", apply_sect_elixir),
        Migration("sect.011", "sect_weekly_reward_operations", apply_sect_weekly),
        Migration("sect.012", "sect_weekly_reward_player_schema", apply_sect_weekly_player),
        Migration("sect.013", "sect_manual_disband_operations", apply_sect_manual_disband),
        Migration("sect.014", "sect_fairyland_upgrade_operations", apply_sect_fairyland_upgrade),
        Migration("sect.015", "sect_scheduled_material_grants", apply_sect_scheduled_materials),
        Migration("sect.016", "sect_task_state", apply_sect_task_state),
        Migration("sect.017", "sect_task_claim_operations", apply_sect_task_claim_operations),
        Migration("sect.018", "sect_task_settlement_operations", apply_sect_task_settlement_operations),
        Migration("sect_fairyland.001", "sect_fairyland_feature_migrations", apply_sect_fairyland),
        Migration("sect_fairyland.002", "sect_fairyland_claim_player_schema", apply_sect_fairyland_player),
        Migration("sign_in.001", "sign_in_operations", apply_sign_in),
        Migration("sign_in.002", "sign_in_statistics_events", apply_sign_in_statistics),
        Migration("sign_in.003", "sign_in_task_events", apply_sign_in_tasks),
        Migration("stone_gift.001", "stone_gift_operations", apply_stone_gift),
        Migration("stone_gift.002", "stone_gift_limits", apply_stone_gift_limits),
        Migration("tasks.001", "task_progress_schema", apply_task_progress),
        Migration("tasks.002", "task_reward_claim_schema", apply_task_claim),
        Migration("tasks.003", "task_reward_claim_recovery_schema", apply_task_claim_recovery),
        Migration("tasks.004", "task_reward_claim_player_schema", apply_task_claim_player),
        Migration("tianti_settlement.001", "tianti_settlement_feature_migrations", apply_tianti_settlement),
        Migration("tianti_settlement.002", "tianti_settlement_operations", apply_tianti_settlement_operations),
        Migration("tianti_training.001", "tianti_training_feature_migrations", apply_tianti_training),
        Migration("tianti_training.002", "tianti_stone_training_operations", apply_tianti_training_operations),
        Migration("tianti_training.003", "tianti_player_info", apply_tianti_player_info),
        Migration("tianti_training.004", "tianti_breakthrough_operations", apply_tianti_breakthrough_operations),
        Migration("tianti_training.005", "tianti_qiaoxue_operations", apply_tianti_qiaoxue_operations),
        Migration("tianti_training.006", "tianti_medicine_bath_operations", apply_tianti_medicine_bath_operations),
        Migration("tianti_training.007", "tianti_item_reward_operations", apply_tianti_item_reward_operations),
        Migration("tianti_training.008", "training_state_operations", apply_training_state),
        Migration("title.001", "title_feature_migrations", apply_title),
        Migration("title.002", "title_schema", apply_title_schema),
        Migration("tower.001", "tower_feature_migrations", apply_tower),
        Migration("tower.002", "tower_purchase_operations", apply_tower_purchase),
        Migration("tower.003", "tower_settlement_operations", apply_tower_settlement),
        Migration("tower.004", "tower_state_operations", apply_tower_state),
        Migration("trade.001", "trade_feature_migrations", apply_trade),
        Migration("trade.002", "trade_guishi_deposit_operations", apply_trade_guishi_deposit),
        Migration("trade.003", "guishi_info", apply_trade_guishi_schema),
        Migration("trade.004", "trade_guishi_withdraw_operations", apply_trade_guishi_withdraw),
        Migration("trade.005", "guishi_order_create_operations", apply_trade_guishi_qiugou),
        Migration("trade.006", "guishi_order_cancel_operations", apply_trade_guishi_order_cancel),
        Migration("trade.007", "guishi_match_operations", apply_trade_guishi_match),
        Migration("trade.008", "guishi_expired_order_operations", apply_trade_guishi_expired_cleanup),
        Migration("trade.009", "guishi_take_item_operations", apply_trade_guishi_take_item),
        Migration("trade.010", "xianshi_listing_operations", apply_trade_xianshi_listing),
        Migration("trade.011", "xianshi_plan_listing_operations", apply_trade_xianshi_plan_listing),
        Migration("trade.012", "xianshi_removal_operations", apply_trade_xianshi_removal),
        Migration("trade.013", "xianshi_operations", apply_trade_xianshi_purchase),
        Migration("training.001", "training_event_operations", apply_training_event_operations),
        Migration("training.002", "training_event_player_schema", apply_training_event_player),
        Migration("training.003", "training_purchase_operations", apply_training_purchase_operations),
        Migration("training.004", "training_reset_operations", apply_training_reset_operations),
        Migration("work.001", "work_feature_migrations", apply_work),
        Migration("work.002", "work_daily_refresh_reset_operations", apply_work_daily_refresh_reset),
        Migration("work.003", "work_item_use_operations", apply_work_item_use),
        Migration("work.004", "work_offer_snapshots", apply_work_offer_snapshots),
        Migration("work.005", "work_refresh_operations", apply_work_refresh_operations),
        Migration("work.006", "work_abort_cleanup_operations", apply_work_abort_cleanup),
        Migration("work.007", "work_claim_operations", apply_work_claim_operations),
        Migration("work.008", "work_settlement_operations", apply_work_settlement_operations),
        Migration("world_events.001", "world_events_feature_migrations", apply_world_events),
        Migration("world_events.002", "world_events_player_attack_settlement", apply_world_events_player),
        Migration("world_events.003", "demon_claim_operations", apply_world_events_claim),
        Migration("world_events.004", "demon_event_lifecycle_operations", apply_world_events_lifecycle),
        Migration("world_events.005", "demon_wave_refresh_operations", apply_world_events_wave_refresh),
        Migration("world_events.006", "spirit_vein_lifecycle_operations", apply_world_events_spirit_vein_lifecycle),
    )


_GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS = frozenset(
    {
        "arena.006",
        "arena.008",
        "arena.009",
        "tower.004",
        "title.002",
        "combat_settlement.003",
        "combat_settlement.004",
        "dungeon.003",
        "dungeon.005",
        "dungeon.006",
        "dungeon.007",
        "dungeon.009",
        "dungeon.010",
        "map.003",
        "map.005",
        "map.008",
        "map.013",
        "map.015",
        "map.016",
        "map.017",
        "tianti_settlement.002",
        "tianti_training.003",
        "tianti_training.004",
        "tianti_training.005",
        "tianti_training.008",
        "trade.003",
        "trade.005",
        "trade.006",
        "trade.007",
        "trade.008",
        "auction.004",
        "auction.006",
        "auction.008",
        "info.avatar.001",
        "info.avatar.002",
        "pet.003",
        "buff.003",
        "buff.005",
        "buff.007",
        "mixelixir.003",
        "impart.003",
        "impart.005",
        "rift.003",
        "sect.012",
        "sect_fairyland.002",
        "tasks.001",
        "tasks.004",
        "training.002",
        "world_events.002",
        "world_events.004",
        "world_events.005",
        "world_events.006",
        "legacy.dufang.003",
        "legacy.dufang.006",
        "base.005",
        "base.011",
        "base.007",
        "boss.004",
        "boss.006",
        "buff.009",
        "buff.011",
        "game_events.001",
    }
)
_PLAYER_DATABASE_MIGRATION_VERSIONS = frozenset(
    {
        "legacy.admin.003",
        "legacy.admin.005",
        "arena.006",
        "arena.008",
        "arena.009",
        "auction.006",
        "auction.008",
        "info.avatar.001",
        "info.avatar.002",
        "tower.004",
        "buff.009",
        "buff.011",
        "game_events.001",
        "platform.001",
        "title.001",
        "title.002",
        "combat_settlement.003",
        "combat_settlement.004",
        "dungeon.003",
        "dungeon.005",
        "dungeon.006",
        "dungeon.007",
        "dungeon.009",
        "dungeon.010",
        "map.003",
        "map.005",
        "map.008",
        "map.013",
        "map.015",
        "map.016",
        "map.017",
        "tianti_settlement.002",
        "tianti_training.003",
        "tianti_training.004",
        "tianti_training.005",
        "tianti_training.008",
        "pet.003",
        "buff.003",
        "buff.005",
        "buff.007",
        "mixelixir.003",
        "impart.003",
        "impart.005",
        "rift.003",
        "sect.012",
        "sect_fairyland.002",
        "tasks.001",
        "tasks.004",
        "training.002",
        "world_events.002",
        "world_events.004",
        "world_events.005",
        "world_events.006",
        "legacy.dufang.003",
        "legacy.dufang.006",
        "base.007",
        "base.005",
        "base.011",
        "boss.004",
        "boss.006",
    }
)
_TRADE_DATABASE_MIGRATION_VERSIONS = frozenset({"legacy.admin.003", "legacy.admin.005", "platform.001", "trade.003", "trade.005", "trade.006", "trade.007", "trade.008", "auction.004"})
_IMPART_DATABASE_MIGRATION_VERSIONS = frozenset({"legacy.admin.003", "legacy.admin.005", "platform.001"})


def migrations_for_database(
    migrations: tuple[Migration, ...], database_key: str
) -> tuple[Migration, ...]:
    """Select the shared migration catalog for one configured database."""
    if database_key == "game_db":
        return tuple(
            migration
            for migration in migrations
            if migration.version not in _GAME_DATABASE_EXCLUDED_MIGRATION_VERSIONS
        )
    if database_key == "player_db":
        return tuple(
            migration
            for migration in migrations
            if migration.version in _PLAYER_DATABASE_MIGRATION_VERSIONS
        )
    if database_key == "trade_db":
        return tuple(
            migration
            for migration in migrations
            if migration.version in _TRADE_DATABASE_MIGRATION_VERSIONS
        )
    if database_key == "impart_db":
        return tuple(
            migration
            for migration in migrations
            if migration.version in _IMPART_DATABASE_MIGRATION_VERSIONS
        )
    return tuple(migration for migration in migrations if migration.version == "platform.001")


def build_registry(*, disabled: set[str] | frozenset[str] | tuple[str, ...] = ()) -> FeatureRegistry:
    # Normalize the public input contract before applying derived feature
    # switches.  Callers may pass an immutable collection when constructing a
    # read-only manifest view.
    disabled = set(disabled)
    registry = FeatureRegistry()
    registry.register(PLATFORM_WEB_FEATURE)
    if ACCESSORY_PACKAGE_FEATURE.key not in disabled:
        registry.register(ACCESSORY_PACKAGE_FEATURE)
    if ACTIVITY_REWARD_FEATURE.key not in disabled:
        registry.register(ACTIVITY_REWARD_FEATURE)
    if ADMIN_ASSET_FEATURE.key not in disabled:
        registry.register(ADMIN_ASSET_FEATURE)
    if ARENA_FEATURE.key not in disabled:
        registry.register(ARENA_FEATURE)
    if AUCTION_FEATURE.key not in disabled:
        registry.register(AUCTION_FEATURE)
    if BACK_FEATURE.key not in disabled:
        registry.register(BACK_FEATURE)
    if BANK_FEATURE.key not in disabled:
        registry.register(BANK_FEATURE)
    if BASE_FEATURE.key not in disabled:
        registry.register(BASE_FEATURE)
    if BEG_FEATURE.key not in disabled:
        registry.register(BEG_FEATURE)
    if BOSS_FEATURE.key not in disabled:
        registry.register(BOSS_FEATURE)
    if BUFF_FEATURE.key not in disabled:
        registry.register(BUFF_FEATURE)
    if COMBAT_SETTLEMENT_FEATURE.key not in disabled:
        registry.register(COMBAT_SETTLEMENT_FEATURE)
    if DAILY_FORTUNE_FEATURE.key not in disabled:
        registry.register(DAILY_FORTUNE_FEATURE)
    if DUNGEON_FEATURE.key not in disabled:
        registry.register(DUNGEON_FEATURE)
    if ILLUSION_FEATURE.key not in disabled:
        registry.register(ILLUSION_FEATURE)
    if INTERACTIVE_FEATURE.key not in disabled:
        registry.register(INTERACTIVE_FEATURE)
    for feature in LEGACY_MIGRATED_FEATURES:
        if feature.key not in disabled:
            registry.register(feature)
    if MAP_FEATURE.key not in disabled:
        registry.register(MAP_FEATURE)
    if MIXELIXIR_FEATURE.key not in disabled:
        registry.register(MIXELIXIR_FEATURE)
    if NATAL_TREASURE_FEATURE.key not in disabled:
        registry.register(NATAL_TREASURE_FEATURE)
    if PACKAGE_REWARD_FEATURE.key not in disabled:
        registry.register(PACKAGE_REWARD_FEATURE)
    if PET_FEATURE.key not in disabled:
        registry.register(PET_FEATURE)
    if PUPPET_FEATURE.key not in disabled:
        registry.register(PUPPET_FEATURE)
    if RIFT_FEATURE.key not in disabled:
        registry.register(RIFT_FEATURE)
    if SECT_FEATURE.key not in disabled:
        registry.register(SECT_FEATURE)
    if SECT_FAIRYLAND_FEATURE.key not in disabled:
        registry.register(SECT_FAIRYLAND_FEATURE)
    if SIGN_IN_FEATURE.key not in disabled:
        registry.register(SIGN_IN_FEATURE)
    if STONE_GIFT_FEATURE.key not in disabled:
        registry.register(STONE_GIFT_FEATURE)
    if TIANTI_SETTLEMENT_FEATURE.key not in disabled:
        registry.register(TIANTI_SETTLEMENT_FEATURE)
    if TIANTI_TRAINING_FEATURE.key not in disabled:
        registry.register(TIANTI_TRAINING_FEATURE)
    if TITLE_FEATURE.key not in disabled:
        registry.register(TITLE_FEATURE)
    if TOWER_FEATURE.key not in disabled:
        registry.register(TOWER_FEATURE)
    if TRADE_FEATURE.key not in disabled:
        registry.register(TRADE_FEATURE)
    if WORK_FEATURE.key not in disabled:
        registry.register(WORK_FEATURE)
    if WORLD_EVENTS_FEATURE.key not in disabled:
        registry.register(WORLD_EVENTS_FEATURE)
    registry.register(LEGACY_SCHEDULER_FEATURE)
    registry.register_many(LEGACY_FEATURES)
    return registry


def build_lifecycle(context: RuntimeContext | None = None) -> tuple[Lifecycle, Readiness, RuntimeContext]:
    context = context or build_runtime_context()
    lifecycle = Lifecycle()
    readiness = Readiness()
    phase_state: dict[str, bool] = {}
    disabled = set()
    if context.settings is not None and not context.settings.get("daily_fortune_enabled", True):
        disabled.add(DAILY_FORTUNE_FEATURE.key)
    if context.settings is not None and not context.settings.get("illusion_enabled", True):
        disabled.add(ILLUSION_FEATURE.key)
    if context.settings is not None and not context.settings.get("interactive_enabled", True):
        disabled.add(INTERACTIVE_FEATURE.key)
    if context.settings is not None and not context.settings.get("beg_enabled", True):
        disabled.add(BEG_FEATURE.key)
    if context.settings is not None and not context.settings.get("title_enabled", True):
        disabled.add(TITLE_FEATURE.key)
    if context.settings is not None and not context.settings.get("sign_in_enabled", True):
        disabled.add(SIGN_IN_FEATURE.key)
    if context.settings is not None and not context.settings.get("stone_gift_enabled", True):
        disabled.add(STONE_GIFT_FEATURE.key)
    if context.settings is not None and not context.settings.get("accessory_package_enabled", True):
        disabled.add(ACCESSORY_PACKAGE_FEATURE.key)
    if context.settings is not None and not context.settings.get("arena_enabled", True):
        disabled.add(ARENA_FEATURE.key)
    if context.settings is not None and not context.settings.get("auction_enabled", True):
        disabled.add(AUCTION_FEATURE.key)
    if context.settings is not None and not context.settings.get("bank_enabled", True):
        disabled.add(BANK_FEATURE.key)
    if context.settings is not None and not context.settings.get("activity_reward_enabled", True):
        disabled.add(ACTIVITY_REWARD_FEATURE.key)
    if context.settings is not None and not context.settings.get("combat_settlement_enabled", True):
        disabled.add(COMBAT_SETTLEMENT_FEATURE.key)
    if context.settings is not None and not context.settings.get("admin_asset_enabled", True):
        disabled.add(ADMIN_ASSET_FEATURE.key)
    if context.settings is not None and not context.settings.get("tianti_settlement_enabled", True):
        disabled.add(TIANTI_SETTLEMENT_FEATURE.key)
    if context.settings is not None and not context.settings.get("tianti_training_enabled", True):
        disabled.add(TIANTI_TRAINING_FEATURE.key)
    if context.settings is not None and not context.settings.get("tower_enabled", True):
        disabled.add(TOWER_FEATURE.key)
    if context.settings is not None and not context.settings.get("sect_fairyland_enabled", True):
        disabled.add(SECT_FAIRYLAND_FEATURE.key)
    if context.settings is not None and not context.settings.get("world_events_enabled", True):
        disabled.add(WORLD_EVENTS_FEATURE.key)
    if context.settings is not None and not context.settings.get("work_claim_enabled", True):
        disabled.add(WORK_FEATURE.key)
    if context.settings is not None and not context.settings.get("mixelixir_enabled", True):
        disabled.add(MIXELIXIR_FEATURE.key)
    if context.settings is not None and not context.settings.get("puppet_enabled", True):
        disabled.add(PUPPET_FEATURE.key)
    if context.settings is not None and not context.settings.get("boss_enabled", True):
        disabled.add(BOSS_FEATURE.key)
    if context.settings is not None and not context.settings.get("dungeon_enabled", True):
        disabled.add(DUNGEON_FEATURE.key)
    if context.settings is not None and not context.settings.get("pet_enabled", True):
        disabled.add(PET_FEATURE.key)
    if context.settings is not None and not context.settings.get("sect_enabled", True):
        disabled.add(SECT_FEATURE.key)
    for feature in (BASE_FEATURE, BACK_FEATURE, BUFF_FEATURE, MAP_FEATURE, NATAL_TREASURE_FEATURE, RIFT_FEATURE, TRADE_FEATURE):
        if context.settings is not None and not context.settings.get(f"{feature.key}_enabled", True):
            disabled.add(feature.key)
    for feature in LEGACY_MIGRATED_FEATURES:
        if context.settings is not None and not context.settings.get(f"{feature.key}_enabled", True):
            disabled.add(feature.key)
    registry = context.registry or build_registry(disabled=disabled)
    context.registry = registry
    context.compatibility = compatibility_hits
    context.jobs = JobRegistry()
    context.readiness = readiness

    migration_runner = MigrationRunner(build_migrations(), clock=context.clock)
    context.migrations = migration_runner

    def ensure_filesystem() -> None:
        context.paths.data.mkdir(parents=True, exist_ok=True)
        context.paths.backups.mkdir(parents=True, exist_ok=True)
        context.paths.logs.mkdir(parents=True, exist_ok=True)
        phase_state["filesystem"] = True

    def ensure_database() -> None:
        for spec in context.database.specs():
            if not str(spec.path):
                raise RuntimeError("database path is empty")
            spec.path.parent.mkdir(parents=True, exist_ok=True)
        primary = context.database.path("game_db")
        phase_state["database"] = True

    def ensure_migrations() -> None:
        for spec in context.database.specs():
            with DatabaseUnitOfWork(spec.path) as uow:
                runner = MigrationRunner(
                    migrations_for_database(migration_runner.migrations, spec.key),
                    clock=context.clock,
                )
                runner.apply(uow)
            if spec.key == "game_db":
                from .features.accessory_package.attached_migrations import (
                    apply_attached_player_accessory,
                    apply_attached_player_accessory_operations,
                    apply_attached_player_accessory_presets,
                )
                from .infrastructure.database.attached_uow import AttachedDatabaseUnitOfWork

                player_database = context.database.path("player_db")
                player_database.parent.mkdir(parents=True, exist_ok=True)
                if not player_database.exists():
                    player_database.touch()
                with AttachedDatabaseUnitOfWork(
                    spec.path,
                    attachments={"player_data": player_database},
                    immediate=True,
                ) as attached_uow:
                    apply_attached_player_accessory(attached_uow, clock=context.clock)
                    apply_attached_player_accessory_operations(attached_uow, clock=context.clock)
                    apply_attached_player_accessory_presets(attached_uow, clock=context.clock)
        phase_state["migrations"] = True

    def ensure_repositories() -> None:
        from .features.daily_fortune.application import DailyFortuneApplication
        from .features.illusion.application import IllusionApplication
        from .features.interactive.application import InteractiveApplication
        from .features.sign_in.application import SignInApplication
        from .features.stone_gift.application import StoneGiftApplication
        from .features.package_reward.application import PackageRewardApplication
        from .features.accessory_package.application import AccessoryPackageApplication
        from .features.arena.application import ArenaApplication
        from .features.auction.application import AuctionBidApplication
        from .compatibility.auction_bid_effects import LegacyAuctionBidEffects
        from .compatibility.auction_settlement_effects import LegacyAuctionSettlementEffects
        from .features.auction.settlement import AuctionSettlementApplication
        from .features.bank.application import BankApplication
        from .features.activity_reward.application import ActivityRewardApplication
        from .features.activity_reward.task_claim_application import ActivityTaskClaimApplication
        from .features.activity_reward.pass_claim_application import ActivityPassClaimApplication
        from .features.activity_reward.boss_milestone_claim_application import ActivityBossMilestoneClaimApplication
        from .features.activity_reward.boss_rank_claim_application import ActivityBossRankClaimApplication
        from .features.combat_settlement.application import CombatSettlementApplication
        from .features.admin_asset.application import AdminAssetApplication
        from .features.tianti_settlement.application import TiantiSettlementApplication
        from .features.tianti_training.application import TiantiTrainingApplication
        from .features.training.application import TrainingApplication
        from .features.tower.application import TowerApplication
        from .features.sect_fairyland.application import SectFairylandApplication
        from .features.world_events.application import DemonClaimApplication
        from .features.world_events.repository import WorldEventClaimSqlRepository
        from .features.work.application import WorkClaimApplication
        from .features.mixelixir.application import MixelixirApplication
        from .features.puppet.application import PuppetApplication
        from .features.boss.application import BossApplication
        from .features.dungeon.application import DungeonApplication
        from .features.pet.application import PetApplication
        from .features.sect.application import SectApplication
        from .features.natal_treasure.application import NatalTreasureApplication
        from .features.buff.application import BuffApplication
        from .compatibility.buff_closing_effects import LegacyBuffClosingEffects
        from .compatibility.base_breakthrough_effects import LegacyDirectBreakthroughEffects
        from .compatibility.game_event_effects import LegacyGameEventEffects
        from .features.base.application import BaseApplication
        from .features.back.application import BackApplication
        from .features.trade.application import TradeApplication
        from .features.map.application import MapApplication
        from .features.rift.application import RiftApplication
        from .features.info.profile_application import PlayerProfileApplication
        from .features.info.avatar_application import PlayerAvatarApplication
        from .features.info.activity_application import PlayerActivityApplication
        from .features.info.attribute_application import PlayerAttributeApplication
        from .features.player_state.application import PlayerStateApplication
        from .features.base.stamina_application import PlayerStaminaApplication

        settings = context.settings
        from .features.bank.feature_flag import bank_first_use_enabled
        lower = int(settings.get("sign_in_lower_limit", 100000)) if settings is not None else 100000
        upper = int(settings.get("sign_in_upper_limit", 500000)) if settings is not None else 500000
        fee_rate = float(settings.get("stone_gift_fee_rate", 0.1)) if settings is not None else 0.1
        sign_in_effects = None
        game_event_effects = LegacyGameEventEffects(
            context.database.path("game_db"),
            context.database.path("player_db"),
            clock=context.clock,
        )
        lottery_service = None
        lottery_application_type = None
        if context.legacy_startup:
            # Keep historical lottery/statistics/task behavior at the adapter
            # boundary while the side effects are migrated independently.
            try:
                from nonebot import get_driver

                get_driver()
            except ValueError:
                # CLI/serve maintenance contexts do not own a NoneBot driver.
                # Do not import legacy adapters there; the real driver process
                # wires them after plugin loading.
                sign_in_effects = None
            else:
                from .features.sign_in.application_effects import SignInApplicationEffects
                from .features.sign_in.statistics import SignInStatisticsRepository
                from .features.sign_in.tasks import SignInTaskRepository
                from .features.sign_in.task_effects import ApplicationSignInTaskEffects
                from .features.sign_in.lottery_application import LotteryApplication
                from .features.sign_in.lottery_repository import LotteryRepository
                lottery_application_type = LotteryApplication

                lottery_service = LotteryApplication(
                    str(context.database.path("game_db")), repository=LotteryRepository(), clock=context.clock, random_source=context.random,
                )
                sign_in_effects = SignInApplicationEffects(
                    context.database.path("game_db"),
                    lottery=lottery_service,
                    clock=context.clock,
                    statistics=SignInStatisticsRepository(str(context.database.path("game_db"))),
                    tasks=ApplicationSignInTaskEffects(SignInTaskRepository(str(context.database.path("game_db"))), context.clock),
                )

        def spirit_vein_tianti_multiplier() -> float:
            from .xiuxian.xiuxian_world_events import get_spirit_vein_tianti_multiplier

            return get_spirit_vein_tianti_multiplier()

        context.services = {
            "player_profile": PlayerProfileApplication(str(context.database.path("game_db"))),
            "player_avatar": PlayerAvatarApplication(str(context.database.path("player_db"))),
            "player_activity": PlayerActivityApplication(
                str(context.database.path("game_db")), clock=context.clock
            ),
            "player_attributes": PlayerAttributeApplication(),
            "player_state": PlayerStateApplication(
                str(context.database.path("game_db"))
            ),
            "player_stamina": PlayerStaminaApplication(
                str(context.database.path("game_db"))
            ),
            "task_claim": TaskClaimApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                clock=context.clock,
            ),
            "daily_fortune": DailyFortuneApplication(str(context.database.path("game_db")), clock=context.clock, random_source=context.random),
            "illusion": IllusionApplication(str(context.database.path("game_db")), clock=context.clock),
            "interactive": InteractiveApplication(str(context.database.path("game_db"))),
            "beg": BegApplication(str(context.database.path("game_db"))),
            "title": TitleApplication(str(context.database.path("player_db"))),
            "sign_in": SignInApplication(
                str(context.database.path("game_db")),
                random_source=context.random,
                clock=context.clock,
                lower_limit=lower,
                upper_limit=upper,
                effects=sign_in_effects,
            ),
            "stone_gift": StoneGiftApplication(
                str(context.database.path("game_db")),
                fee_rate=fee_rate,
                clock=context.clock,
            ),
            "package_reward": PackageRewardApplication(str(context.database.path("game_db"))),
            "accessory_package": AccessoryPackageApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
            ),
            "arena": ArenaApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                clock=context.clock,
            ),
            "auction": AuctionBidApplication(
                str(context.database.path("game_db")),
                effects=LegacyAuctionBidEffects(str(context.database.path("player_db"))),
            ),
            "auction_settlement": AuctionSettlementApplication(
                str(context.database.path("game_db")),
                effects=LegacyAuctionSettlementEffects(str(context.database.path("player_db"))),
            ),
            "bank": BankApplication(
                str(context.database.path("game_db")),
            ),
            "activity_reward": ActivityRewardApplication(
                str(context.database.path("game_db")),
            ),
            "activity_task_claim": ActivityTaskClaimApplication(
                str(context.database.path("game_db")),
                str(context.database.path("game_db")),
                clock=context.clock,
            ),
            "activity_pass_claim": ActivityPassClaimApplication(
                str(context.database.path("game_db")),
                str(context.database.path("game_db")),
                clock=context.clock,
            ),
            "activity_boss_milestone_claim": ActivityBossMilestoneClaimApplication(
                str(context.database.path("game_db")),
                str(context.database.path("game_db")),
                clock=context.clock,
            ),
            "activity_boss_rank_claim": ActivityBossRankClaimApplication(
                str(context.database.path("game_db")),
                str(context.database.path("game_db")),
                clock=context.clock,
            ),
            "combat_settlement": CombatSettlementApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
            ),
            "admin_asset": AdminAssetApplication(str(context.database.path("game_db"))),
            "tianti_settlement": TiantiSettlementApplication(
                str(context.database.path("player_db")),
                spirit_vein_multiplier=spirit_vein_tianti_multiplier,
            ),
            "tianti_training": TiantiTrainingApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                clock=context.clock,
            ),
            "tower": TowerApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                clock=context.clock,
            ),
            "sect_fairyland": SectFairylandApplication(
                str(context.database.path("player_db")),
                clock=context.clock,
                spirit_vein_multiplier=spirit_vein_tianti_multiplier,
            ),
            "world_events": DemonClaimApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                repository=WorldEventClaimSqlRepository(
                    str(context.database.path("game_db")),
                    str(context.database.path("player_db")),
                ),
            ),
            "work": WorkClaimApplication(
                str(context.database.path("game_db")),
            ),
            "mixelixir": MixelixirApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
            ),
            "puppet": PuppetApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
            ),
            "boss": BossApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                activity_database=str(context.database.path("game_db")),
                clock=context.clock,
            ),
            "dungeon": DungeonApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
            ),
            "pet": PetApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                clock=context.clock,
                game_event_effects=game_event_effects,
            ),
            "sect": SectApplication(
                str(context.database.path("game_db")),
                player_database=str(context.database.path("player_db")),
                clock=context.clock,
                random_source=context.random,
            ),
        }
        context.services.update({
            "natal_treasure": NatalTreasureApplication(str(context.database.path("player_db")), str(context.database.path("game_db"))),
            "buff": BuffApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                closing_effects=LegacyBuffClosingEffects(context.database.path("player_db")),
                clock=context.clock,
            ),
            "base": BaseApplication(
                context.database.path("game_db"), context.database.path("player_db"), clock=context.clock,
                direct_breakthrough_effects=LegacyDirectBreakthroughEffects(
                    context.database.path("game_db"), context.database.path("player_db"), players_dir=context.paths.players,
                ),
            ),
            "back": BackApplication(str(context.database.path("game_db")), str(context.database.path("player_db"))),
            "trade": TradeApplication(
                str(context.database.path("game_db")),
                str(context.database.path("trade_db")),
                clock=context.clock,
                ids=context.ids,
                random_source=context.random,
                auction_settlement=context.services["auction_settlement"],
            ),
            "map": MapApplication(
                str(context.database.path("game_db")),
                str(context.database.path("player_db")),
                game_event_effects=game_event_effects,
            ),
            "rift": RiftApplication(str(context.database.path("game_db")), str(context.database.path("player_db"))),
        })
        if bank_first_use_enabled(context.settings):
            from .features.bank.account_application import BankDepositApplication

            context.services["bank_first_use"] = BankDepositApplication(str(context.database.path("game_db")))
            if context.settings.get("bank_first_use_upgrade_enabled", False):
                from .features.bank.account_upgrade_application import BankUpgradeApplication

                context.services["bank_first_use_upgrade"] = BankUpgradeApplication(str(context.database.path("game_db")))
            if context.settings.get("bank_first_use_interest_enabled", False):
                from .features.bank.account_interest_application import BankInterestApplication

                context.services["bank_first_use_interest"] = BankInterestApplication(str(context.database.path("game_db")))
            if context.settings.get("bank_first_use_withdrawal_enabled", False):
                from .features.bank.account_withdrawal_application import BankWithdrawalApplication

                context.services["bank_first_use_withdrawal"] = BankWithdrawalApplication(str(context.database.path("game_db")))
            if context.settings.get("bank_first_use_info_enabled", False):
                from .features.bank.account_info_application import BankAccountInfoApplication

                context.services["bank_first_use_info"] = BankAccountInfoApplication(str(context.database.path("game_db")))
        for feature_key, application_type in LEGACY_MIGRATED_APPLICATIONS.items():
            if feature_key == "training":
                continue
            context.services[feature_key] = application_type(str(context.database.path("game_db")))
        context.services["training"] = TrainingApplication(
            str(context.database.path("game_db")),
            str(context.database.path("player_db")),
            clock=context.clock,
        )
        try:
            from nonebot import get_driver

            get_driver()
        except ValueError:
            pass
        else:
            from .xiuxian.xiuxian_base import (
                configure_direct_breakthrough_application,
                configure_lottery_application,
                configure_sign_in_application,
            )
            from .xiuxian.xiuxian_utils.utils import configure_player_avatar_application, configure_player_profile_application
            from .xiuxian.xiuxian_utils.utils import configure_player_activity_application
            from .xiuxian.xiuxian_utils.utils import configure_player_attribute_application
            from .xiuxian.xiuxian_utils.utils import configure_player_stamina_application
            from .xiuxian.xiuxian_utils.player_fight import configure_player_state_application
            from .xiuxian.xiuxian_buff import configure_buff_application
            from .xiuxian.xiuxian_map import configure_map_application
            from .xiuxian.xiuxian_pet import configure_pet_application
            from .xiuxian.xiuxian_back import configure_back_application, configure_package_reward_application
            from .xiuxian.xiuxian_tasks.task_data import configure_task_claim_application
            from .xiuxian.xiuxian_training import configure_training_application
            from .xiuxian.xiuxian_activity.service import (
                configure_activity_pass_claim_application,
                configure_activity_task_claim_application,
            )
            from .xiuxian.xiuxian_activity.activity_boss import (
                configure_activity_boss_milestone_claim_application,
                configure_activity_boss_rank_claim_application,
            )

            configure_sign_in_application(context.services["sign_in"])
            configure_direct_breakthrough_application(context.services["base"])
            configure_player_profile_application(context.services["player_profile"])
            configure_player_avatar_application(context.services["player_avatar"])
            configure_player_activity_application(context.services["player_activity"])
            configure_player_attribute_application(context.services["player_attributes"])
            configure_player_state_application(context.services["player_state"])
            configure_buff_application(context.services["buff"])
            configure_map_application(context.services["map"])
            configure_pet_application(context.services["pet"])
            configure_player_stamina_application(context.services["player_stamina"])
            configure_back_application(context.services["back"])
            configure_package_reward_application(context.services["package_reward"])
            configure_task_claim_application(context.services["task_claim"])
            configure_activity_task_claim_application(context.services["activity_task_claim"])
            configure_activity_pass_claim_application(context.services["activity_pass_claim"])
            configure_activity_boss_milestone_claim_application(context.services["activity_boss_milestone_claim"])
            configure_activity_boss_rank_claim_application(context.services["activity_boss_rank_claim"])
            configure_training_application(context.services["training"])
            if lottery_application_type is not None and lottery_service is not None and isinstance(lottery_service, lottery_application_type):
                configure_lottery_application(lottery_service)
        context.reconcile_handlers = {
            "accessory_package.open": context.services["accessory_package"].reconcile,
            "map.mission_claim": context.services["map"].reconcile_mission_claim_operation,
            "pet.travel_claim": context.services["pet"].reconcile_travel_claim_operation,
            "tasks.claim_rewards": context.services["task_claim"].reconcile,
            "activity_reward.tasks.claim": context.services["activity_task_claim"].reconcile,
            "activity_reward.pass.claim": context.services["activity_pass_claim"].reconcile,
            "activity_reward.boss_milestone.claim": context.services["activity_boss_milestone_claim"].reconcile,
            "activity_reward.boss_rank.claim": context.services["activity_boss_rank_claim"].reconcile,
        }
        context.outbox_handlers = {
            "accessory_package.open": context.services["accessory_package"].reconcile,
            "buff.closing.effects": context.services["buff"].reconcile_outbox_event,
            "base.direct_breakthrough.effects": context.services["base"].reconcile_direct_breakthrough_event,
            "game_event.projection": game_event_effects.on_outbox_event,
            "sign_in.effects": context.services["sign_in"].reconcile_outbox_event,
            "auction.bid.effects": context.services["auction"].reconcile_outbox_event,
            "auction.settlement.effects": context.services["auction_settlement"].reconcile_outbox_event,
        }
        phase_state["repositories"] = True

    async def ensure_jobs() -> None:
        from .compatibility.scheduler import activate_scheduler_bridge
        from .features.sign_in.clock import set_sign_in_clock

        context.services["sign_in_clock_token"] = set_sign_in_clock(context.clock)
        activate_scheduler_bridge()
        # Legacy APScheduler keeps its decorators for the compatibility
        # release, but registration is activated here rather than during
        # module import. The registry points at the same stable job IDs so
        # CLI/Web manual runs do not execute an unwired placeholder.
        for feature in registry.features:
            for job in feature.jobs:
                if feature.key == LEGACY_SCHEDULER_FEATURE.key:
                    handler = legacy_job_handler(job.id)
                elif feature.key == AUCTION_FEATURE.key and job.id == "auction.settle":
                    handler = lambda scheduled_at=None, _app=context.services["auction_settlement"]: auction_settle_job(
                        _app,
                        scheduled_at=scheduled_at,
                        clock=context.clock,
                        ids=context.ids,
                    )
                else:
                    handler = None
                context.jobs.register_manifest(job, handler=handler)
        context.job_executor = JobExecutor(context.jobs)
        # Compatibility modules register callbacks explicitly with the
        # bootstrap bridge.  They are executed once after all new runtime
        # resources are ready, never as import-time driver hooks.
        if context.legacy_startup:
            await run_legacy_startup()
        phase_state["jobs"] = True

    async def shutdown_jobs() -> None:
        from .features.sign_in.clock import reset_sign_in_clock

        if context.legacy_startup:
            await run_legacy_shutdown()
        token = (context.services or {}).pop("sign_in_clock_token", None)
        if token is not None:
            reset_sign_in_clock(token)
        phase_state["jobs"] = False

    def ensure_web() -> None:
        if not context.legacy_startup:
            # Maintenance commands check storage and repository readiness but
            # do not own a NoneBot transport or the legacy Web matcher graph.
            # Keeping this phase successful preserves the six-check health
            # contract without importing driver-bound legacy modules.
            context.web_app = None
            phase_state["web"] = True
            return
        from .adapters.web.app import create_app

        context.web_app = create_app(context=context, registry=registry, readiness=readiness)
        phase_state["web"] = True

    def shutdown_web() -> None:
        context.web_app = None
        phase_state["web"] = False

    lifecycle.register(LifecyclePhase.FILESYSTEM, ensure_filesystem)
    lifecycle.register(LifecyclePhase.DATABASE, ensure_database)
    lifecycle.register(LifecyclePhase.MIGRATIONS, ensure_migrations)
    lifecycle.register(LifecyclePhase.REPOSITORIES, ensure_repositories)
    lifecycle.register(LifecyclePhase.JOBS, ensure_jobs, shutdown=shutdown_jobs)
    lifecycle.register(LifecyclePhase.WEB, ensure_web, shutdown=shutdown_web)
    for name in ("filesystem", "database", "migrations", "repositories", "jobs", "web"):
        readiness.register(name, lambda name=name: phase_state.get(name, False))
    return lifecycle, readiness, context


def manifest_json() -> dict[str, Any]:
    return build_registry().export()


async def startup(context: RuntimeContext | None = None):
    context = context or build_runtime_context()
    disabled = set()
    if context.settings is not None and not context.settings.get("daily_fortune_enabled", True):
        disabled.add(DAILY_FORTUNE_FEATURE.key)
    if context.settings is not None and not context.settings.get("illusion_enabled", True):
        disabled.add(ILLUSION_FEATURE.key)
    if context.settings is not None and not context.settings.get("sign_in_enabled", True):
        disabled.add(SIGN_IN_FEATURE.key)
    if context.settings is not None and not context.settings.get("stone_gift_enabled", True):
        disabled.add(STONE_GIFT_FEATURE.key)
    if context.settings is not None and not context.settings.get("accessory_package_enabled", True):
        disabled.add(ACCESSORY_PACKAGE_FEATURE.key)
    if context.settings is not None and not context.settings.get("arena_enabled", True):
        disabled.add(ARENA_FEATURE.key)
    if context.settings is not None and not context.settings.get("auction_enabled", True):
        disabled.add(AUCTION_FEATURE.key)
    if context.settings is not None and not context.settings.get("bank_enabled", True):
        disabled.add(BANK_FEATURE.key)
    if context.settings is not None and not context.settings.get("activity_reward_enabled", True):
        disabled.add(ACTIVITY_REWARD_FEATURE.key)
    if context.settings is not None and not context.settings.get("combat_settlement_enabled", True):
        disabled.add(COMBAT_SETTLEMENT_FEATURE.key)
    if context.settings is not None and not context.settings.get("admin_asset_enabled", True):
        disabled.add(ADMIN_ASSET_FEATURE.key)
    if context.settings is not None and not context.settings.get("tianti_settlement_enabled", True):
        disabled.add(TIANTI_SETTLEMENT_FEATURE.key)
    if context.settings is not None and not context.settings.get("tianti_training_enabled", True):
        disabled.add(TIANTI_TRAINING_FEATURE.key)
    if context.settings is not None and not context.settings.get("tower_enabled", True):
        disabled.add(TOWER_FEATURE.key)
    if context.settings is not None and not context.settings.get("sect_fairyland_enabled", True):
        disabled.add(SECT_FAIRYLAND_FEATURE.key)
    if context.settings is not None and not context.settings.get("world_events_enabled", True):
        disabled.add(WORLD_EVENTS_FEATURE.key)
    if context.settings is not None and not context.settings.get("work_claim_enabled", True):
        disabled.add(WORK_FEATURE.key)
    if context.settings is not None and not context.settings.get("mixelixir_enabled", True):
        disabled.add(MIXELIXIR_FEATURE.key)
    if context.settings is not None and not context.settings.get("puppet_enabled", True):
        disabled.add(PUPPET_FEATURE.key)
    if context.settings is not None and not context.settings.get("boss_enabled", True):
        disabled.add(BOSS_FEATURE.key)
    if context.settings is not None and not context.settings.get("dungeon_enabled", True):
        disabled.add(DUNGEON_FEATURE.key)
    if context.settings is not None and not context.settings.get("pet_enabled", True):
        disabled.add(PET_FEATURE.key)
    if context.settings is not None and not context.settings.get("sect_enabled", True):
        disabled.add(SECT_FEATURE.key)
    for feature in (BASE_FEATURE, BACK_FEATURE, BUFF_FEATURE, MAP_FEATURE, NATAL_TREASURE_FEATURE, RIFT_FEATURE, TRADE_FEATURE):
        if context.settings is not None and not context.settings.get(f"{feature.key}_enabled", True):
            disabled.add(feature.key)
    for feature in LEGACY_MIGRATED_FEATURES:
        if context.settings is not None and not context.settings.get(f"{feature.key}_enabled", True):
            disabled.add(feature.key)
    registry = build_registry(disabled=disabled)
    context.registry = registry
    lifecycle, readiness, context = build_lifecycle(context)
    context.lifecycle = lifecycle
    state = await lifecycle.start()
    return state, readiness, context, lifecycle


async def shutdown(lifecycle: Lifecycle):
    return await lifecycle.shutdown()


def install_driver_hooks(driver: Any) -> tuple[Any, Any]:
    """Install the single new-runtime hook pair on a NoneBot driver.

    The marker is stored on the driver rather than in a module global so test
    drivers and hot-reload drivers remain independent. Legacy modules still
    own their deprecated hooks during the compatibility release; this pair is
    the authoritative readiness/database boundary.
    """
    marker = "_xiuxian_refactored_lifecycle_hooks"
    existing = getattr(driver, marker, None)
    if existing is not None:
        return existing
    holder: dict[str, Any] = {"context": None, "lifecycle": None}

    # Matchers are wired once, before startup, but resolve applications only
    # after the lifecycle has assembled repositories and migrations.
    from .adapters.nonebot import register_migrated_matchers, register_bank_first_use_extended_matchers, register_bank_first_use_matcher

    register_migrated_matchers(driver, holder)

    async def on_startup() -> None:
        data_dir = getattr(getattr(driver, "config", None), "xiuxian_data_dir", None)
        context = build_runtime_context(data_dir=data_dir, legacy_startup=True)
        if context.message_gateway is None or getattr(context.message_gateway, "sender", None) is None:
            from .adapters.nonebot import NoneBotMessageGateway
            from .xiuxian.messaging.delivery import delivery_service

            context.message_gateway = NoneBotMessageGateway(delivery_service)
        state, readiness, context, lifecycle = await startup(context)
        holder.update(context=context, lifecycle=lifecycle, readiness=readiness)
        from .features.bank.clock import set_bank_clock
        holder["bank_clock_token"] = set_bank_clock(context.clock)
        if context.services.get("bank_first_use") is not None:
            register_bank_first_use_matcher(
                driver,
                holder,
                limit=int(context.settings.get("bank_first_use_limit", 1000000000)),
            )
        if any(key in context.services for key in ("bank_first_use_upgrade", "bank_first_use_interest", "bank_first_use_withdrawal")):
            register_bank_first_use_extended_matchers(driver, holder)
        if state.phase is LifecyclePhase.NOT_READY:
            raise RuntimeError(state.error or "xiuxian runtime is not ready")

    async def on_shutdown() -> None:
        lifecycle = holder.get("lifecycle")
        if lifecycle is not None:
            await shutdown(lifecycle)
            holder["lifecycle"] = None
        token = holder.pop("bank_clock_token", None)
        if token is not None:
            from .features.bank.clock import reset_bank_clock
            reset_bank_clock(token)

    driver.on_startup(on_startup)
    driver.on_shutdown(on_shutdown)
    hooks = (on_startup, on_shutdown)
    setattr(driver, marker, hooks)
    return hooks


__all__ = ["build_lifecycle", "build_migrations", "build_registry", "install_driver_hooks", "manifest_json", "startup", "shutdown"]
