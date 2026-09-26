#!/usr/bin/env python3
"""Emit quantitative evidence for the second-stage full refactor.

This report intentionally distinguishes a real entry-point cutover from removal
of the old implementation.  It is a progress instrument, not a completion gate.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "nonebot_plugin_xiuxian_2"

PATTERNS = {
    "db_backend_connect_files": "db_backend.connect",
    "sqlite3_connect_files": "sqlite3.connect",
    "legacy_service_import_files": "transaction_service",
    "legacy_handle_import_files": "xiuxian2_handle",
    "direct_random_files": "random.",
    "datetime_now_files": "datetime.now",
    "time_now_files": "time.time",
}


def _py_files() -> list[Path]:
    return sorted(PACKAGE.rglob("*.py"))


def _counts() -> dict[str, int]:
    files = _py_files()
    counts = {"python_files": len(files)}
    for name, token in PATTERNS.items():
        counts[name] = sum(token in path.read_text(encoding="utf-8", errors="ignore") for path in files)
    transaction_files = list(PACKAGE.rglob("*transaction_service.py"))
    counts["transaction_service_files"] = len(transaction_files)
    counts["transaction_service_lines"] = sum(
        len(path.read_text(encoding="utf-8", errors="ignore").splitlines()) for path in transaction_files
    )
    handle = PACKAGE / "xiuxian" / "xiuxian_utils" / "xiuxian2_handle.py"
    counts["xiuxian2_handle_bytes"] = handle.stat().st_size if handle.is_file() else 0
    return counts


def _slice_status() -> dict[str, dict[str, object]]:
    base = (PACKAGE / "xiuxian" / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
    adapter = (PACKAGE / "adapters" / "nonebot" / "commands.py").read_text(encoding="utf-8")
    web = (PACKAGE / "adapters" / "web" / "api.py").read_text(encoding="utf-8")
    legacy_transaction = (PACKAGE / "xiuxian" / "xiuxian_base" / "transaction_service.py").read_text(encoding="utf-8")
    sign_effects = (PACKAGE / "features" / "sign_in" / "application_effects.py").read_text(encoding="utf-8")
    sign_application = (PACKAGE / "features" / "sign_in" / "application.py").read_text(encoding="utf-8")
    plugin = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    arena = (PACKAGE / "xiuxian" / "xiuxian_arena" / "__init__.py").read_text(encoding="utf-8")
    arena_limit = (PACKAGE / "xiuxian" / "xiuxian_arena" / "arena_limit.py").read_text(encoding="utf-8")
    tower_limit = (PACKAGE / "xiuxian" / "xiuxian_tower" / "tower_limit.py").read_text(encoding="utf-8")
    training_limit = (PACKAGE / "xiuxian" / "xiuxian_training" / "training_limit.py").read_text(encoding="utf-8")
    work_facade = (PACKAGE / "xiuxian" / "xiuxian_work" / "__init__.py").read_text(encoding="utf-8")
    work_accelerate_handler = work_facade[
        work_facade.index("async def use_work_order") : work_facade.index(
            "async def use_work_capture_order", work_facade.index("async def use_work_order")
        )
    ]
    work_capture_handler = work_facade[
        work_facade.index("async def use_work_capture_order") :
    ]
    activity_service = (PACKAGE / "xiuxian" / "xiuxian_activity" / "service.py").read_text(encoding="utf-8")
    dungeon_facade = (PACKAGE / "xiuxian" / "xiuxian_dungeon" / "__init__.py").read_text(encoding="utf-8")
    dungeon_manager = (PACKAGE / "xiuxian" / "xiuxian_dungeon" / "dungeon_manager.py").read_text(encoding="utf-8")
    bank_facade = (PACKAGE / "xiuxian" / "xiuxian_bank" / "__init__.py").read_text(encoding="utf-8")
    map_facade = (PACKAGE / "xiuxian" / "xiuxian_map" / "__init__.py").read_text(encoding="utf-8")
    sect_facade = (PACKAGE / "xiuxian" / "xiuxian_sect" / "__init__.py").read_text(encoding="utf-8")
    entertainment_facade = (PACKAGE / "xiuxian" / "xiuxian_entertainment" / "mod" / "newapi_store.py").read_text(encoding="utf-8")
    partner_facade = (PACKAGE / "xiuxian" / "xiuxian_buff" / "partner.py").read_text(encoding="utf-8")
    partner_cultivation_application = (PACKAGE / "features" / "buff" / "partner_cultivation_application.py").read_text(encoding="utf-8")
    partner_cultivation_repository = (PACKAGE / "features" / "buff" / "partner_cultivation_repository.py").read_text(encoding="utf-8")
    buff_migrations = (PACKAGE / "features" / "buff" / "migrations.py").read_text(encoding="utf-8")
    natal_facade = (PACKAGE / "xiuxian" / "xiuxian_natal_treasure" / "__init__.py").read_text(encoding="utf-8")
    world_events_facade = (PACKAGE / "xiuxian" / "xiuxian_world_events" / "__init__.py").read_text(encoding="utf-8")
    rift_facade = (PACKAGE / "xiuxian" / "xiuxian_rift" / "__init__.py").read_text(encoding="utf-8")
    rift_event_handler = rift_facade[
        rift_facade.index("async def _roll_rift_event") : rift_facade.index(
            "async def _roll_rift_boss_event", rift_facade.index("async def _roll_rift_event")
        )
    ]
    rift_boss_handler = rift_facade[
        rift_facade.index("async def _roll_rift_boss_event") : rift_facade.index(
            "@complete_rift.handle", rift_facade.index("async def _roll_rift_boss_event")
        )
    ]
    rift_jsondata = (PACKAGE / "xiuxian" / "xiuxian_rift" / "jsondata.py").read_text(encoding="utf-8")
    rift_application = (PACKAGE / "features" / "rift" / "application.py").read_text(encoding="utf-8")
    rift_domain = (PACKAGE / "features" / "rift" / "domain.py").read_text(encoding="utf-8")
    rift_make = (PACKAGE / "xiuxian" / "xiuxian_rift" / "riftmake.py").read_text(encoding="utf-8")
    rift_player_fight = (PACKAGE / "xiuxian" / "xiuxian_utils" / "player_fight.py").read_text(encoding="utf-8")
    rift_attributes = (PACKAGE / "xiuxian" / "xiuxian_utils" / "xiuxian2_handle.py").read_text(encoding="utf-8")
    rift_cooldown_repository = (PACKAGE / "features" / "rift" / "cooldown_repository.py").read_text(encoding="utf-8")
    rift_generation_repository = (PACKAGE / "features" / "rift" / "generation_repository.py").read_text(encoding="utf-8")
    rift_key_event_repository = (PACKAGE / "features" / "rift" / "key_event_repository.py").read_text(encoding="utf-8")
    rift_settlement_repository = (PACKAGE / "features" / "rift" / "settlement_repository.py").read_text(encoding="utf-8")
    rift_entry_repository = (PACKAGE / "features" / "rift" / "entry_repository.py").read_text(encoding="utf-8")
    rift_termination_repository = (PACKAGE / "features" / "rift" / "termination_repository.py").read_text(encoding="utf-8")
    rift_speedup_repository = (PACKAGE / "features" / "rift" / "speedup_repository.py").read_text(encoding="utf-8")
    rift_migrations = (PACKAGE / "features" / "rift" / "migrations.py").read_text(encoding="utf-8")
    back_facade = (PACKAGE / "xiuxian" / "xiuxian_back" / "__init__.py").read_text(encoding="utf-8")
    back_accessory_facade = (PACKAGE / "xiuxian" / "xiuxian_back" / "accessory.py").read_text(encoding="utf-8")
    back_util_facade = (PACKAGE / "xiuxian" / "xiuxian_back" / "back_util.py").read_text(encoding="utf-8")
    back_application_source = (PACKAGE / "features" / "back" / "application.py").read_text(encoding="utf-8")
    past_life_events_facade = (PACKAGE / "xiuxian" / "xiuxian_past_life" / "past_life_events.py").read_text(encoding="utf-8")
    past_life_command_facade = (PACKAGE / "xiuxian" / "xiuxian_past_life" / "__init__.py").read_text(encoding="utf-8")
    dufang_facade = (PACKAGE / "xiuxian" / "xiuxian_dufang" / "__init__.py").read_text(encoding="utf-8")
    fusion_facade = (PACKAGE / "xiuxian" / "xiuxian_fusion" / "__init__.py").read_text(encoding="utf-8")
    title_facade = (PACKAGE / "xiuxian" / "xiuxian_title" / "__init__.py").read_text(encoding="utf-8")
    base_facade = (PACKAGE / "xiuxian" / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
    puppet_facade = (PACKAGE / "xiuxian" / "xiuxian_puppet" / "__init__.py").read_text(encoding="utf-8")
    pet_facade = (PACKAGE / "xiuxian" / "xiuxian_pet" / "__init__.py").read_text(encoding="utf-8")
    trade_facade = (PACKAGE / "xiuxian" / "xiuxian_trade" / "__init__.py").read_text(encoding="utf-8")
    trade_application_source = (PACKAGE / "features" / "trade" / "application.py").read_text(encoding="utf-8")
    trade_manifest_source = (PACKAGE / "features" / "trade" / "manifest.py").read_text(encoding="utf-8")
    trade_web_source = (PACKAGE / "features" / "trade" / "web.py").read_text(encoding="utf-8")
    trade_web_test_source = (ROOT / "tests" / "test_trade_auction_web.py").read_text(encoding="utf-8")
    trade_xianshi_transactions = (PACKAGE / "features" / "trade" / "xianshi_listing_repository.py").read_text(encoding="utf-8")
    trade_plan_xianshi_transactions = (PACKAGE / "features" / "trade" / "xianshi_plan_listing_repository.py").read_text(encoding="utf-8")
    trade_xianshi_removal_transactions = (PACKAGE / "features" / "trade" / "xianshi_removal_repository.py").read_text(encoding="utf-8")
    trade_xianshi_purchase_transactions = (PACKAGE / "features" / "trade" / "xianshi_purchase_repository.py").read_text(encoding="utf-8")
    trade_xianshi_query_repository = (PACKAGE / "features" / "trade" / "xianshi_query_repository.py").read_text(encoding="utf-8")
    trade_xianshi_schema_adapter = (PACKAGE / "compatibility" / "legacy_xianshi_schema.py").read_text(encoding="utf-8")
    trade_feature_repository = (PACKAGE / "features" / "trade" / "repository.py").read_text(encoding="utf-8")
    legacy_trade_auction_compatibility = (PACKAGE / "compatibility" / "legacy_trade_auction_sessions.py").read_text(encoding="utf-8")
    trade_auction_transactions = (PACKAGE / "xiuxian" / "xiuxian_trade" / "transaction_service.py").read_text(encoding="utf-8")
    auction_settlement_source = (PACKAGE / "features" / "auction" / "settlement.py").read_text(encoding="utf-8")
    trade_legacy_guishi_compatibility = (PACKAGE / "compatibility" / "legacy_guishi_stone.py").read_text(encoding="utf-8")
    trade_deposit_handler = trade_facade[
        trade_facade.index("async def guishi_deposit_") : trade_facade.index(
            "@guishi_withdraw.handle", trade_facade.index("async def guishi_deposit_")
        )
    ]
    trade_withdraw_handler = trade_facade[
        trade_facade.index("async def guishi_withdraw_") : trade_facade.index(
            "@guishi_qiugou.handle", trade_facade.index("async def guishi_withdraw_")
        )
    ]
    trade_qiugou_handler = trade_facade[
        trade_facade.index("async def guishi_qiugou_") : trade_facade.index(
            "@guishi_cancel_qiugou.handle", trade_facade.index("async def guishi_qiugou_")
        )
    ]
    trade_baitan_handler = trade_facade[
        trade_facade.index("async def guishi_baitan_") : trade_facade.index(
            "@guishi_shoutan.handle", trade_facade.index("async def guishi_baitan_")
        )
    ]
    trade_cancel_qiugou_handler = trade_facade[
        trade_facade.index("async def guishi_cancel_qiugou_") : trade_facade.index(
            "@guishi_baitan.handle", trade_facade.index("async def guishi_cancel_qiugou_")
        )
    ]
    trade_cancel_baitan_handler = trade_facade[
        trade_facade.index("async def guishi_shoutan_") : trade_facade.index(
            "@guishi_take_item.handle", trade_facade.index("async def guishi_shoutan_")
        )
    ]
    trade_expired_job = trade_facade[
        trade_facade.index("async def clear_expired_baitan_orders_job") : trade_facade.index(
            "@auction_view.handle", trade_facade.index("async def clear_expired_baitan_orders_job")
        )
    ]
    trade_take_handler = trade_facade[
        trade_facade.index("async def guishi_take_item_(") : trade_facade.index(
            "@guishi_info.handle", trade_facade.index("async def guishi_take_item_(")
        )
    ]
    xianshi_listing_handler = trade_facade[
        trade_facade.index("async def xian_shop_add_(") : trade_facade.index(
            "@xianshi_auto_add.handle", trade_facade.index("async def xian_shop_add_(")
        )
    ]
    xianshi_auto_listing_handler = trade_facade[
        trade_facade.index("async def xianshi_auto_add_(") : trade_facade.index(
            "@xianshi_fast_add.handle", trade_facade.index("async def xianshi_auto_add_(")
        )
    ]
    xianshi_fast_listing_handler = trade_facade[
        trade_facade.index("async def xianshi_fast_add_(") : trade_facade.index(
            "@xiuxian_shop_view.handle", trade_facade.index("async def xianshi_fast_add_(")
        )
    ]
    xianshi_system_listing_handler = trade_facade[
        trade_facade.index("async def xian_shop_added_by_admin_(") : trade_facade.index(
            "@xian_shop_remove_by_admin.handle",
            trade_facade.index("async def xian_shop_added_by_admin_("),
        )
    ]
    xianshi_name_removal_handler = trade_facade[
        trade_facade.index("async def xian_shop_remove_(") : trade_facade.index(
            "@xian_buy.handle", trade_facade.index("async def xian_shop_remove_(")
        )
    ]
    xianshi_clear_handler = trade_facade[
        trade_facade.index("async def xian_shop_off_all_(") : trade_facade.index(
            "@xian_shop_added_by_admin.handle", trade_facade.index("async def xian_shop_off_all_(")
        )
    ]
    xianshi_admin_removal_handler = trade_facade[
        trade_facade.index("async def xian_shop_remove_by_admin_(") : trade_facade.index(
            "# --- 鬼市命令处理 ---", trade_facade.index("async def xian_shop_remove_by_admin_(")
        )
    ]
    auction_settlement = (PACKAGE / "features" / "auction" / "settlement.py").read_text(encoding="utf-8")
    auction_settlement_statistics = (PACKAGE / "features" / "auction" / "settlement_statistics.py").read_text(encoding="utf-8")
    auction_settlement_compat = (PACKAGE / "compatibility" / "auction_settlement_effects.py").read_text(encoding="utf-8")
    auction_bid = (PACKAGE / "features" / "auction" / "bid_repository.py").read_text(encoding="utf-8")
    auction_bid_application = (PACKAGE / "features" / "auction" / "application.py").read_text(encoding="utf-8")
    auction_bid_effects = (PACKAGE / "features" / "auction" / "bid_effects.py").read_text(encoding="utf-8")
    auction_bid_statistics = (PACKAGE / "features" / "auction" / "bid_statistics.py").read_text(encoding="utf-8")
    auction_compat_effects = (PACKAGE / "compatibility" / "auction_bid_effects.py").read_text(encoding="utf-8")
    web_app_source = (PACKAGE / "adapters" / "web" / "app.py").read_text(encoding="utf-8")
    cli_source = (PACKAGE / "cli.py").read_text(encoding="utf-8")
    auction_queue = (PACKAGE / "features" / "auction" / "queue_application.py").read_text(encoding="utf-8")
    auction_start = (PACKAGE / "features" / "auction" / "session_start_application.py").read_text(encoding="utf-8")
    auction_query = (PACKAGE / "features" / "auction" / "query_application.py").read_text(encoding="utf-8")
    auction_query_repository = (PACKAGE / "features" / "auction" / "query_repository.py").read_text(encoding="utf-8")
    auction_queue_handlers = trade_facade[
        trade_facade.index("async def auction_add_(") : trade_facade.index(
            "@my_auction.handle", trade_facade.index("async def auction_add_(")
        )
    ]
    boss_facade = (PACKAGE / "xiuxian" / "xiuxian_boss" / "__init__.py").read_text(encoding="utf-8")
    buff_facade = (PACKAGE / "xiuxian" / "xiuxian_buff" / "__init__.py").read_text(encoding="utf-8")
    impart_facade = (PACKAGE / "xiuxian" / "xiuxian_impart" / "__init__.py").read_text(encoding="utf-8")
    impart_prayer_repository = (PACKAGE / "features" / "impart" / "prayer_repository.py").read_text(encoding="utf-8")
    impart_migrations = (PACKAGE / "features" / "impart" / "migrations.py").read_text(encoding="utf-8")
    impart_prayer_handler = impart_facade[
        impart_facade.index("async def use_wishing_stone") : impart_facade.index(
            "async def use_love_sand", impart_facade.index("async def use_wishing_stone")
        )
    ]
    mixelixir_facade = (PACKAGE / "xiuxian" / "xiuxian_mixelixir" / "__init__.py").read_text(encoding="utf-8")
    dongfu_facade = (PACKAGE / "xiuxian" / "xiuxian_dongfu" / "__init__.py").read_text(encoding="utf-8")
    dongfu_success_handler = dongfu_facade[
        dongfu_facade.index('operation_id = f"dongfu-infiltrate-success:') : dongfu_facade.index(
            'if result.status == "inventory_full":',
            dongfu_facade.index('operation_id = f"dongfu-infiltrate-success:'),
        )
    ]
    dongfu_failure_handler = dongfu_facade[
        dongfu_facade.index('operation_id = f"dongfu-infiltrate-failure:') : dongfu_facade.index(
            "    stealth_penalty =", dongfu_facade.index('operation_id = f"dongfu-infiltrate-failure:')
        )
    ]
    impart_pk_facade = (PACKAGE / "xiuxian" / "xiuxian_impart_pk" / "__init__.py").read_text(encoding="utf-8")
    admin_facade = (PACKAGE / "xiuxian" / "xiuxian_admin" / "__init__.py").read_text(encoding="utf-8")
    return {
        "stone_gift": {
            "default_legacy_handler_disabled": '"送灵石" if _legacy_stone_gift_enabled' in base,
            "nonebot_application_path": "_build_stone" in adapter and "application.read_limits" in adapter and "handle_stone_gift" in adapter,
            "web_application_path": "create_stone_gift_blueprint" in web and "application.transfer" in web,
            "old_service_removed": "class StoneGiftService" not in legacy_transaction and (PACKAGE / "compatibility" / "legacy_stone_gift.py").is_file(),
            "status": "cutover_with_compatibility_rollback_isolated",
        },
        "sign_in": {
            "default_legacy_handler_disabled": '"修仙签到" if _legacy_sign_in_enabled' in base,
            "nonebot_application_path": "_build_sign" in adapter and "application.read_limits" in adapter,
            "web_application_path": "create_sign_in_blueprint" in web and "application.claim" in web,
            "old_service_removed": "class SignInService" not in legacy_transaction and (PACKAGE / "compatibility" / "legacy_sign_in.py").is_file(),
            "effects_application_owned": "SignInApplicationEffects" in sign_effects and "SignInApplicationEffects(" in plugin,
            "effects_outbox_reconcile_owned": '"sign_in.effects"' in plugin and "reconcile_outbox_event" in sign_application,
            "task_core_legacy": "SignInTaskEffects(record_task_progress)" in plugin,
            "lottery_core_default_legacy": "LotteryApplication(" not in plugin or "LotterySettlementService" in plugin,
            "lottery_compatibility_fallback": "XIUXIAN_SIGN_IN_LEGACY_LOTTERY" in plugin and "LotterySettlementService" in plugin,
            "status": "cutover_with_compatibility_rollback_side_effects_retained",
        },
        "entertainment": {
            "account_delete_application_owned": "entertainment_application.delete_accounts(" in entertainment_facade and "_run_entertainment_write(" not in entertainment_facade[entertainment_facade.index("def delete_accounts("):entertainment_facade.index("def resolve_targets(")],
            "status": "auto-checkin and account-delete local JSON paths cut over; bind remains credential compatibility",
        },
        "arena": {
            "state_application_owned": "ArenaStateApplication" in arena_limit,
            "legacy_state_owner_disabled": "ArenaStateService" not in arena_limit,
            "weekly_rank_application_owned": "arena_weekly_rank_application.reduce(" in arena,
            "legacy_scheduler_disabled": "ArenaWeeklyRankReductionService" not in arena and "_arena_weekly_rank_reduction_service" not in arena,
            "daily_reward_application_owned": "arena_season_reward_application.reset_daily()" in arena,
            "legacy_daily_reward_disabled": "ArenaSeasonRewardService" not in arena and "_arena_season_reward_service" not in arena,
            "status": "state_weekly_rank_and_daily_reward_cutover_with_legacy_service_retained_for_compatibility",
        },
        "tower": {
            "state_application_owned": "TowerStateApplication" in tower_limit,
            "legacy_state_owner_disabled": "TowerStateService" not in tower_limit,
            "status": "state_cutover_with_legacy_service_retained_for_compatibility",
        },
        "training": {
            "state_application_owned": "TrainingStateApplication" in training_limit,
            "legacy_state_owner_disabled": "TrainingStateService" not in training_limit,
            "status": "state_cutover_with_legacy_service_retained_for_compatibility",
        },
        "work": {
            "daily_refresh_application_owned": "work_daily_refresh_application.reset(" in work_facade,
            "legacy_daily_refresh_disabled": "_work_daily_refresh_reset_service" not in work_facade,
            "item_accelerate_application_owned": "work_item_use_application.accelerate(" in work_accelerate_handler,
            "legacy_item_accelerate_disabled": "_work_item_use_service().accelerate(" not in work_accelerate_handler,
            "capture_application_owned": "work_item_use_application.capture(" in work_capture_handler,
            "legacy_item_capture_disabled": "_work_item_use_service().capture(" not in work_capture_handler,
            "capture_json_projection_only": "savef(user_id, work_data, sync_snapshot=False)" in work_capture_handler,
            "item_use_migrations_registered": (
                'Migration("work.003", "work_item_use_operations", apply_work_item_use)' in plugin
                and 'Migration("work.004", "work_offer_snapshots", apply_work_offer_snapshots)' in plugin
            ),
            "status": "daily_refresh_accelerate_and_capture_cutover_with_other_work_compatibility",
        },
        "activity_reward": {
            "claim_all_application_owned": "activity_claim_all_application.run(" in activity_service,
            "legacy_claim_all_disabled": "_activity_claim_all_service().run(" not in activity_service,
            "status": "claim_all_cutover_with_legacy_service_retained_for_compatibility",
        },
        "dungeon_team": {
            "create_invite_application_owned": "dungeon_team_application.create(" in dungeon_facade and "dungeon_team_application.invite(" in dungeon_facade,
            "legacy_create_invite_disabled": "_dungeon_team_transaction_service().create(" not in dungeon_facade and "_dungeon_team_transaction_service().invite(" not in dungeon_facade,
            "explore_settlement_application_owned": "dungeon_application.settle(" in dungeon_facade and "_dungeon_explore_operation_service().settle(" not in dungeon_facade,
            "reset_application_owned": "self.dungeon_application = DungeonApplication(" in dungeon_manager and "self._reset_application().reset(" in dungeon_manager and "self.reset_service.reset(" not in dungeon_manager,
            "status": "create_invite_cutover_with_legacy_join_exit_compatibility",
        },
        "bank": {
            "deposit_application_owned": "BankDepositApplication" in bank_facade and "bank_application.deposit(" not in bank_facade,
            "withdrawal_application_owned": "BankWithdrawalApplication" in bank_facade and "bank_application.withdraw(" not in bank_facade,
            "upgrade_application_owned": "BankUpgradeApplication" in bank_facade and "bank_application.upgrade(" not in bank_facade,
            "interest_application_owned": "BankInterestApplication" in bank_facade and "bank_application.settle_interest(" not in bank_facade,
            "legacy_deposit_disabled": "bank_deposit_service.deposit(" not in bank_facade,
            "legacy_withdrawal_disabled": "bank_withdrawal_service.withdraw(" not in bank_facade,
            "legacy_upgrade_disabled": "bank_upgrade_service.upgrade(" not in bank_facade,
            "legacy_interest_disabled": "bank_interest_service.settle(" not in bank_facade,
            "status": "deposit_withdrawal_upgrade_interest_cutover",
        },
        "map": {
            "interactive_application_owned": "map_application.interactive_settlement(" in map_facade and "map_application.interactive_start(" in map_facade,
            "resource_application_owned": "map_application.resource_reward(" in map_facade,
            "legacy_interactive_disabled": "map_interactive_action_service.save_settlement(" not in map_facade and "map_interactive_action_service.start(" not in map_facade,
            "legacy_resource_disabled": "map_resource_reward_service.settle(" not in map_facade,
            "status": "interactive_resource_cutover_with_legacy_services_retained_for_compatibility",
        },
        "sect": {
            "membership_application_owned": all(f"sect_application.{name}(" in sect_facade for name in ("join", "leave", "kick", "change_position")),
            "economy_application_owned": all(f"sect_application.{name}(" in sect_facade for name in ("rename", "donate", "purchase")),
            "daily_maintenance_application_owned": "sect_application.reset_daily_maintenance(" in sect_facade,
            "close_mountain_application_owned": sect_facade.count("sect_application.close_mountain(") >= 2,
            "owner_inherit_application_owned": "sect_application.inherit_owner(" in sect_facade,
            "join_state_application_owned": "sect_application.open_join(" in sect_facade and "sect_application.close_join(" in sect_facade,
            "disband_application_owned": sect_facade.count("sect_application.disband_inactive(") >= 3,
            "owner_transfer_application_owned": "sect_application.transfer_owner(" in sect_facade,
            "scheduled_grant_application_owned": "sect_application.grant_scheduled_materials(" in sect_facade,
            "fairyland_upgrade_application_owned": "sect_application.upgrade_fairyland(" in sect_facade,
            "elixir_room_upgrade_application_owned": "sect_application.upgrade_elixir_room(" in sect_facade,
            "buff_search_application_owned": sect_facade.count("sect_application.apply_buff_search(") >= 2,
            "practice_application_owned": sect_facade.count("sect_application.upgrade_practice(") >= 3,
            "task_settlement_application_owned": sect_facade.count("sect_application.settle_task(") >= 2,
            "creation_application_owned": sect_facade.count("sect_application.create_sect(") >= 2,
            "name_refresh_application_owned": "sect_application.charge_name_refresh(" in sect_facade,
            "legacy_membership_disabled": all(token not in sect_facade for token in ("sect_membership_service.join", "sect_membership_service.leave_sect", "sect_membership_service.kick_member", "sect_membership_service.change_position")),
            "status": "membership_economy_daily_maintenance_cutover_with_other_sect_compatibility_paths",
        },
        "natal_treasure": {
            "awaken_application_owned": "natal_treasure_application.awaken(" in natal_facade,
            "effect_upgrade_application_owned": "natal_treasure_application.upgrade(" in natal_facade,
            "legacy_awaken_disabled": "_natal_awaken_service().awaken(" not in natal_facade,
            "status": "awaken_effect_upgrade_cutover_with_other_natal_mutations_compatibility",
        },
        "world_events": {
            "claim_application_owned": "demon_claim_application.claim(" in world_events_facade,
            "attack_application_owned": "demon_attack_application.settle(" in world_events_facade,
            "legacy_claim_disabled": "_demon_claim_service().claim(" not in world_events_facade,
            "status": "demon_claim_attack_settlement_cutover_with_other_world_event_compatibility",
        },
        "rift": {
            "entry_application_owned": "rift_application.enter(" in rift_facade,
            "speedup_application_owned": "rift_application.speedup(" in rift_facade and "rift_application.execute_legacy_call(" not in rift_facade,
            "settlement_application_owned": "rift_application.settle(" in rift_facade,
            "key_event_application_owned": "rift_application.event_settle(" in rift_facade,
            "demon_token_application_owned": "rift_application.replay_demon_token_battle(" in rift_facade and "rift_application.settle_demon_token_battle(" in rift_facade,
            "legacy_demon_token_disabled": "_rift_demon_token_battle_settlement_service" not in rift_facade,
            "demon_token_migrations_registered": all(token in plugin for token in ("rift.002", "rift.003", "apply_rift_demon_token_operations", "apply_rift_demon_token_player_schema")) and "rift_demon_token_battle_operations" in rift_migrations,
            "speedup_default_repository_owned": "repository=repository" in rift_application and "if self.repository is None" in rift_application and "RiftSpeedupSqlRepository(self.database).apply" in rift_application,
            "speedup_migrations_registered": "rift.004" in plugin and "apply_rift_speedup_operations" in plugin and "rift_speedup_operations" in rift_migrations,
            "speedup_request_path_has_no_ddl": "CREATE TABLE" not in rift_speedup_repository and "ALTER TABLE" not in rift_speedup_repository and "schema_missing" in rift_speedup_repository,
            "legacy_speedup_getter_disabled": "RiftSpeedupService" not in rift_facade and "_rift_speedup_service" not in rift_facade,
            "generation_application_owned": all(token in rift_facade for token in ("rift_application.generate(", "rift_application.current_world(", "rift_application.bootstrap_world(")),
            "legacy_generation_disabled": "_rift_entry_service()" not in rift_facade and "RiftEntryService" not in rift_facade,
            "generation_migrations_registered": "rift.005" in plugin and "apply_rift_world_generation" in plugin and "rift_world_state" in rift_migrations and "rift_generation_operations" in rift_migrations,
            "generation_request_path_has_no_ddl": "CREATE TABLE" not in rift_generation_repository and "ALTER TABLE" not in rift_generation_repository and "schema_missing" in rift_generation_repository,
            "termination_application_owned": all(token in rift_facade for token in ("rift_application.terminate(", "rift_application.replay_termination(")),
            "legacy_termination_disabled": "_rift_termination_service()" not in rift_facade and "RiftTerminationService" not in rift_facade,
            "termination_migrations_registered": "rift.006" in plugin and "apply_rift_termination_operations" in plugin and "rift_termination_operations" in rift_migrations,
            "termination_request_path_has_no_ddl": "CREATE TABLE" not in rift_termination_repository and "ALTER TABLE" not in rift_termination_repository and "schema_missing" in rift_termination_repository,
            "key_event_default_repository_owned": "RiftKeyEventSqlRepository" in rift_application and "self.key_event_repository.settle" in rift_application,
            "legacy_key_event_disabled": "_rift_key_event_settlement_service()" not in rift_facade and "RiftKeyEventSettlementService" not in rift_facade,
            "key_event_migrations_registered": "rift.007" in plugin and "apply_rift_key_event_operations" in plugin and "rift_key_event_operations" in rift_migrations,
            "key_event_request_path_has_no_ddl": "CREATE TABLE" not in rift_key_event_repository and "ALTER TABLE" not in rift_key_event_repository,
            "settlement_default_repository_owned": "RiftSettlementSqlRepository" in rift_application and "self.settlement_repository.settle" in rift_application,
            "legacy_settlement_disabled": "_rift_settlement_service()" not in rift_facade and "RiftSettlementService" not in rift_facade,
            "settlement_migrations_registered": "rift.008" in plugin and "apply_rift_settlement_operations" in plugin and "rift_settlement_operations" in rift_migrations,
            "settlement_request_path_has_no_ddl": "CREATE TABLE" not in rift_settlement_repository and "ALTER TABLE" not in rift_settlement_repository,
            "entry_default_repository_owned": "RiftEntrySqlRepository" in rift_application and "self.entry_repository.enter" in rift_application,
            "entry_migrations_registered": "rift.009" in plugin and "apply_rift_entry_schema" in plugin and all(token in rift_migrations for token in ("rift_entries", "rift_entry_counts", "rift_entry_operations")),
            "entry_request_path_has_no_ddl": "CREATE TABLE" not in rift_entry_repository and "ALTER TABLE" not in rift_entry_repository,
            "legacy_entry_disabled": "_rift_entry_service().enter(" not in rift_facade,
            "entry_read_projection_repository_owned": "RiftEntrySqlRepository" in rift_jsondata and "read_entry" in rift_jsondata,
            "entry_read_projection_has_no_ddl": "CREATE TABLE" not in rift_jsondata and "ALTER TABLE" not in rift_jsondata,
            "legacy_entry_read_disabled": "RiftEntryService" not in rift_jsondata,
            "cooldown_read_projection_repository_owned": "RiftCooldownSqlRepository" in rift_application and "self.cooldown_repository.read" in rift_application,
            "cooldown_read_request_path_has_no_ddl": "CREATE TABLE" not in rift_cooldown_repository and "ALTER TABLE" not in rift_cooldown_repository,
            "legacy_cooldown_read_disabled": "get_user_cd(" not in rift_facade and "XiuxianDateManage" not in rift_facade,
            "damage_event_application_owned": "rift_application.roll_damage_event(" in rift_event_handler,
            "damage_event_resolver_owned": "class RiftDamageEventResolver" in rift_domain and "RiftDamageEventResolver" in rift_application,
            "damage_event_is_persistence_free": "update_exp" not in rift_domain and "update_ls" not in rift_domain,
            "legacy_damage_event_disabled": "get_dxsj_info(" not in rift_event_handler,
            "boss_battle_application_owned": "rift_application.roll_boss_battle(" in rift_event_handler and "rift_application.roll_boss_battle(" in rift_boss_handler,
            "boss_battle_resolver_owned": "class RiftBossBattleResolver" in rift_domain and "RiftBossBattleResolver" in rift_application,
            "boss_battle_is_persistence_free": "update_exp" not in rift_domain and "update_ls" not in rift_domain and "battle_mode=0" in rift_application,
            "legacy_boss_battle_disabled": all("get_boss_battle_info(" not in handler for handler in (rift_event_handler, rift_boss_handler)),
            "boss_battle_asset_provider_wired": "player_asset_provider=get_rift_battle_player_assets" in rift_facade and "player_asset_provider=get_rift_battle_player_assets" in rift_make,
            "boss_battle_asset_snapshot_boundary": "class RiftBossBattleAssetProvider" in rift_domain and "runner_kwargs[\"player_data\"] = player_data" in rift_domain and "player1_data = player_data if" in rift_player_fight,
            "boss_battle_engine_provider_wired": all(
                token in rift_make and token in rift_facade
                for token in (
                    "boss_attribute_provider=get_boss_attributes",
                    "boss_buff_provider=generate_boss_buff",
                    "boss_skill_provider=get_rift_battle_boss_skill_provider",
                    "boss_status_updater=ignore_rift_battle_boss_status_update",
                )
            ) and all(
                token in rift_domain and token in rift_player_fight
                for token in (
                    "boss_attribute_provider",
                    "boss_buff_provider",
                    "boss_skill_provider",
                    "boss_status_updater",
                )
            ) and all(
                token in rift_domain for token in (
                    'runner_kwargs["boss_attribute_provider"] = self.boss_attribute_provider',
                    'runner_kwargs["boss_buff_provider"] = self.boss_buff_provider',
                    'runner_kwargs["boss_skill_provider"] = self.boss_skill_provider',
                    'runner_kwargs["boss_status_updater"] = self.boss_status_updater',
                )
            ),
            "boss_battle_buff_random_source_wired": 'runner_kwargs["boss_buff_random_source"] = random_source' in rift_domain and "boss_buff_random_source=None" in rift_player_fight and "random_source=boss_buff_random_source" in rift_player_fight and "rng = random_source or random" in rift_player_fight,
            "boss_battle_status_writeback_isolated": "def ignore_rift_battle_boss_status_update" in rift_make and "return None" in rift_make[rift_make.index("def ignore_rift_battle_boss_status_update"):rift_make.index("async def get_boss_battle_info", rift_make.index("def ignore_rift_battle_boss_status_update"))] and "boss_status_updater=ignore_rift_battle_boss_status_update" in rift_make,
            "boss_battle_skill_provider_read_only": "def get_rift_battle_boss_skill_data" in rift_make and "skill_path.open" in rift_make and "skill_data_cache" not in rift_make[rift_make.index("def get_rift_battle_boss_skill_data"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_boss_skill_data"))],
            "boss_battle_legacy_asset_provider_explicit": "def get_rift_battle_player_assets" in rift_make and "get_players_attributes(" in rift_make and "item_provider=get_rift_battle_item_data" in rift_make,
            "boss_battle_item_provider_wired": "item_provider=get_rift_battle_item_data" in rift_make and "item_data = item_provider(item_id)" in rift_player_fight,
            "boss_battle_item_provider_read_only": "def get_rift_battle_item_data" in rift_make and "item_path.open" in rift_make and "ITEMS_CACHE" not in rift_make[rift_make.index("def get_rift_battle_item_data"):rift_make.index("def get_rift_battle_boss_skill_data", rift_make.index("def get_rift_battle_item_data"))],
            "boss_battle_items_lazy": all("\nitems = Items()\n" not in source and "_items_instance" in source for source in (rift_make, rift_player_fight, rift_attributes)),
            "boss_battle_pet_provider_wired": "pet_provider=get_user_pet_for_battle" in rift_make and "buffs[\"宠物\"] = pet_provider(user_id)" in rift_player_fight,
            "boss_battle_attribute_provider_wired": "attribute_provider=get_rift_battle_final_attributes" in rift_make and "final_attr = attribute_provider(user_id, ratio=ratio, include_current=True)" in rift_player_fight,
            "boss_battle_natal_provider_wired": "natal_provider=get_rift_battle_natal_data" in rift_make and "natal_data = natal_provider(user_id)" in rift_player_fight,
            "boss_battle_natal_provider_read_only": "def get_rift_battle_natal_data" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_natal_data"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_natal_data"))],
            "boss_battle_impart_provider_wired": "impart_provider=get_rift_battle_impart_data" in rift_make and "impart_provider=None" in rift_attributes and "impart = impart_provider(user_id) or {}" in rift_attributes,
            "boss_battle_impart_provider_read_only": "def get_rift_battle_impart_data" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_impart_data"):rift_make.index("def get_rift_battle_final_attributes", rift_make.index("def get_rift_battle_impart_data"))],
            "boss_battle_buff_info_provider_wired": "buff_info_provider=get_rift_battle_buff_info" in rift_make and "buff_info_provider=None" in rift_attributes and "buff_info = buff_info_provider(user_id) or {}" in rift_attributes,
            "boss_battle_buff_info_provider_read_only": "def get_rift_battle_buff_info" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_buff_info"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_buff_info"))],
            "boss_battle_accessory_provider_wired": "accessory_provider=get_rift_battle_accessory_data" in rift_make and "accessory_provider=None" in rift_attributes and "calc_accessory_effects(user_id, accessory_provider=accessory_provider)" in rift_attributes,
            "boss_battle_accessory_provider_read_only": "def get_rift_battle_accessory_data" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_accessory_data"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_accessory_data"))],
            "boss_battle_tianti_provider_wired": "tianti_provider=get_rift_battle_tianti_data" in rift_make and "tianti_provider=None" in rift_attributes and "_tdata = tianti_provider(user_id) or {}" in rift_attributes,
            "boss_battle_tianti_provider_read_only": "def get_rift_battle_tianti_data" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_tianti_data"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_tianti_data"))],
            "boss_battle_base_provider_wired": "base_provider=get_rift_battle_base_attributes" in rift_make and "base_provider=None" in rift_attributes and "base_provider = base_provider or get_base_attributes" in rift_attributes,
            "boss_battle_base_provider_read_only": "def get_rift_battle_base_attributes" in rift_make and "DatabaseUnitOfWork(database, read_only=True)" in rift_make and "CREATE TABLE" not in rift_make[rift_make.index("def get_rift_battle_base_attributes"):rift_make.index("async def get_boss_battle_info", rift_make.index("def get_rift_battle_base_attributes"))],
            "treasure_application_owned": "rift_application.roll_treasure(" in rift_event_handler,
            "treasure_resolver_owned": "class RiftTreasureResolver" in rift_domain and "RiftTreasureResolver" in rift_application,
            "treasure_is_persistence_free": "update_exp" not in rift_domain and "update_ls" not in rift_domain and "update_ls" not in rift_application,
            "legacy_treasure_disabled": "get_treasure_info(" not in rift_event_handler,
            "status": "world_generation_termination_key_event_settlement_entry_speedup_demon_token_damage_event_boss_battle_asset_boundary_engine_provider_boundary_skill_provider_boundary_buff_random_source_boundary_status_writeback_isolation_item_provider_boundary_lazy_items_boundary_treasure_cutover_with_natal_impart_buff_info_accessory_tianti_and_base_provider_boundaries_and_remaining_rift_compatibility",
        },
        "back": {
            "cultivation_item_application_owned": "back_application.cultivation_item(" in back_facade and "_cultivation_item_application().apply(" in back_util_facade,
            "legacy_cultivation_item_disabled": "_cultivation_item_service().apply(" not in back_facade and "_cultivation_item_service().apply(" not in back_util_facade,
            "skill_learning_application_owned": "back_application.learn_skill(" in back_facade,
            "legacy_skill_learning_disabled": "_skill_learning_service().learn(" not in back_facade,
            "lottery_talisman_application_owned": "back_application.lottery_talisman(" in back_facade,
            "legacy_lottery_talisman_disabled": "_lottery_talisman_service().apply(" not in back_facade,
            "stone_reward_application_owned": back_facade.count("back_application.stone_reward(") >= 2,
            "legacy_stone_reward_disabled": "_stone_reward_service().apply(" not in back_facade,
            "three_cultivation_pill_application_owned": "back_application.three_cultivation_pill(" in back_facade,
            "legacy_three_cultivation_pill_disabled": "_three_cultivation_pill_service().apply(" not in back_facade,
            "breakthrough_rate_item_application_owned": "_breakthrough_rate_item_application().apply(" in back_util_facade,
            "legacy_breakthrough_rate_item_disabled": "_breakthrough_rate_item_service().apply(" not in back_util_facade,
            "recovery_item_application_owned": "_recovery_item_application().apply(" in back_util_facade,
            "legacy_recovery_item_disabled": "_recovery_item_service().apply(" not in back_util_facade,
            "permanent_atk_item_application_owned": "_permanent_atk_item_application().apply(" in back_util_facade,
            "legacy_permanent_atk_item_disabled": "_permanent_atk_item_service().apply(" not in back_util_facade,
            "alchemy_application_owned": back_facade.count("back_application.alchemy(") >= 3,
            "legacy_alchemy_disabled": "_alchemy_service().apply(" not in back_facade,
            "unbind_application_owned": "back_application.unbind(" in back_facade,
            "legacy_unbind_disabled": "_unbind_item_service().apply(" not in back_facade,
            "repair_application_owned": "back_application.repair(" in back_facade,
            "equipment_unequip_application_owned": "back_application.change_equipment(" in back_facade,
            "equipment_equip_application_owned": back_facade.count("back_application.change_equipment(") >= 2,
            "pet_egg_application_owned": "back_application.use_pet_eggs(" in back_facade,
            "generic_item_use_application_owned": "self.item_use_application.apply(" in back_application_source,
            "legacy_generic_item_use_default_disabled": (
                "if self._explicit_repository is None:\n            if \"item_id\"" in back_application_source
            ),
            "package_application_owned": (
                "package_reward_application.open_package(" in back_facade
                and "back_application.open_package(" not in back_facade
            ),
            "accessory_package_application_owned": "back_application.accessory_package(" in back_facade,
            "accessory_affix_application_owned": (
                back_accessory_facade.count("_affix_application().set_locks") >= 2
                and "_affix_application().replay" in back_accessory_facade
            ),
            "legacy_accessory_affix_disabled": "_accessory_transaction_service().set_affix_locks" not in back_accessory_facade,
            "accessory_decompose_application_owned": (
                "_decompose_application().decompose" in back_accessory_facade
                and "_decompose_application().replay" in back_accessory_facade
            ),
            "legacy_accessory_decompose_disabled": "_accessory_transaction_service().decompose" not in back_accessory_facade,
            "accessory_batch_decompose_application_owned": "_decompose_application().batch_decompose" in back_accessory_facade,
            "legacy_accessory_batch_decompose_disabled": "_accessory_transaction_service().batch_decompose" not in back_accessory_facade,
            "accessory_wash_application_owned": "_wash_application().wash" in back_accessory_facade and "_wash_application().replay" in back_accessory_facade,
            "legacy_accessory_wash_disabled": "_accessory_transaction_service().wash" not in back_accessory_facade,
            "accessory_upgrade_application_owned": "_upgrade_application().upgrade" in back_accessory_facade and "_upgrade_application().replay" in back_accessory_facade,
            "legacy_accessory_upgrade_disabled": "_accessory_transaction_service().upgrade" not in back_accessory_facade,
            "accessory_preset_application_owned": "_preset_application().save" in back_accessory_facade and "_preset_application().replay" in back_accessory_facade,
            "legacy_accessory_preset_disabled": "_accessory_transaction_service().save_preset" not in back_accessory_facade,
            "accessory_quick_equip_application_owned": "_quick_equip_application().equip" in back_accessory_facade and "_quick_equip_application().replay" in back_accessory_facade,
            "legacy_accessory_quick_equip_disabled": "_accessory_transaction_service().quick_equip_preset" not in back_accessory_facade,
            "legacy_repair_disabled": "_backpack_repair_service().run(" not in back_facade,
            "status": "cultivation_item_skill_learning_lottery_talisman_stone_reward_three_cultivation_pill_alchemy_unbind_repair_equipment_equip_unequip_pet_egg_package_accessory_package_affix_lock_unlock_decompose_batch_decompose_wash_upgrade_preset_quick_equip_generic_item_use_cutover_with_other_back_compatibility",
        },
        "past_life": {
            "final_settlement_application_owned": "_past_life_application.final_settle(" in past_life_events_facade,
            "choice_application_owned": "_past_life_application.choice(" in past_life_events_facade,
            "start_application_owned": "_past_life_application.start(" in past_life_events_facade,
            "reset_one_application_owned": "past_life_application.reset_one(" in past_life_command_facade,
            "legacy_reset_one_disabled": "_past_life_reset_service().reset_one(" not in past_life_command_facade,
            "reset_all_application_owned": (
                "past_life_application.reset_all_create(" in past_life_command_facade
                and "past_life_application.reset_all_batch(" in past_life_command_facade
                and "past_life_application.reset_all_pending(" in past_life_command_facade
            ),
            "legacy_reset_all_disabled": (
                "_past_life_reset_service().create_all(" not in past_life_command_facade
                and "_past_life_reset_service().run_batch(" not in past_life_command_facade
                and "_past_life_reset_service().find_pending_all(" not in past_life_command_facade
            ),
            "status": "reset_one_and_reset_all_cutover_with_legacy_service_retained_for_compatibility",
        },
        "dufang": {
            "share_application_owned": "dufang_application.share_settle(" in dufang_facade,
            "legacy_share_disabled": "_dufang_share_service().settle(" not in dufang_facade,
            "status": "share_settlement_cutover_with_bet_payout_compatibility",
        },
        "fusion": {
            "single_application_owned": "fusion_application.apply(" in fusion_facade,
            "legacy_single_disabled": "_fusion_service().apply(" not in fusion_facade,
            "batch_application_owned": "fusion_application.apply_batch(" in fusion_facade,
            "legacy_batch_disabled": "_fusion_service().apply_batch(" not in fusion_facade,
            "status": "single_batch_fusion_cutover",
        },
        "title": {
            "equip_replay_application_owned": "title_application.get_result(" in title_facade,
            "legacy_equip_replay_disabled": "# 先回放：成功后 equipped 变化会挡住同事件幂等。\n    prior = _title_transaction_service().get_result(" not in title_facade,
            "legacy_unequip_replay_disabled": "operation_id = _title_operation_id(event, \"unequip\", str(user_id))\n    prior = _title_transaction_service().get_result(" not in title_facade,
            "unlock_batch_application_owned": "title_application.execute(" in (PACKAGE / "xiuxian" / "xiuxian_title" / "title_data.py").read_text(encoding="utf-8"),
            "legacy_unlock_batch_disabled": "_title_transaction_service().unlock_batch(" not in (PACKAGE / "xiuxian" / "xiuxian_title" / "title_data.py").read_text(encoding="utf-8"),
            "status": "equip_unequip_unlock_cutover",
        },
        "base": {
            "rename_application_owned": "base_application.rename(" in base_facade,
            "legacy_rename_disabled": "_player_rename_service().rename_user(" not in base_facade and "_player_rename_service().rename_root(" not in base_facade,
            "status": "rename_cutover_with_replay_compatibility",
        },
        "puppet": {
            "harvest_application_owned": "puppet_application.harvest(" in puppet_facade,
            "legacy_harvest_disabled": "_puppet_harvest_service().harvest(" not in puppet_facade,
            "status": "harvest_cutover_with_purchase_upgrade_compatibility",
        },
        "pet": {
            "active_switch_application_owned": "pet_application.switch(" in pet_facade,
            "legacy_active_switch_disabled": "_pet_active_switch_service().switch(" not in pet_facade,
            "skill_replace_application_owned": "pet_application.skill_replace(" in pet_facade,
            "legacy_skill_replace_disabled": "_pet_skill_replace_service().replace(" not in pet_facade,
            "status": "active_switch_and_skill_replace_cutover_with_other_pet_compatibility",
        },
        "trade": {
            "xianshi_listing_application_owned": "trade_application.xianshi_list_items(" in xianshi_listing_handler and "class XianshiListingSqlRepository" in trade_xianshi_transactions,
            "xianshi_auto_listing_application_owned": "trade_application.xianshi_list_plan(" in xianshi_auto_listing_handler and "class XianshiPlanListingSqlRepository" in trade_plan_xianshi_transactions,
            "legacy_xianshi_auto_listing_disabled": "xianshi_repository.add_xianshi_plan_items(" not in xianshi_auto_listing_handler,
            "xianshi_fast_listing_application_owned": "trade_application.xianshi_list_items(" in xianshi_fast_listing_handler and "class XianshiListingSqlRepository" in trade_xianshi_transactions,
            "legacy_xianshi_fast_listing_disabled": "xianshi_repository.add_xianshi_items(" not in xianshi_fast_listing_handler,
            "xianshi_system_listing_application_owned": "trade_application.xianshi_list_system_item(" in xianshi_system_listing_handler and "def list_system_item(" in trade_xianshi_transactions,
            "legacy_xianshi_system_listing_disabled": "xianshi_repository.add_xianshi_item(" not in xianshi_system_listing_handler,
            "xianshi_name_removal_application_owned": "trade_application.xianshi_remove_by_name(" in xianshi_name_removal_handler and "class XianshiRemovalSqlRepository" in trade_xianshi_removal_transactions,
            "legacy_xianshi_name_removal_disabled": "xianshi_repository.remove_xianshi_by_name(" not in xianshi_name_removal_handler,
            "xianshi_admin_removal_application_owned": "trade_application.xianshi_remove_listing(" in xianshi_admin_removal_handler,
            "legacy_xianshi_admin_removal_disabled": "xianshi_repository.remove_xianshi_listing(" not in xianshi_admin_removal_handler,
            "xianshi_clear_application_owned": "trade_application.xianshi_clear_all(" in xianshi_clear_handler,
            "legacy_xianshi_clear_disabled": "xianshi_repository.clear_all_xianshi_listings(" not in xianshi_clear_handler,
            "purchase_application_owned": "trade_application.purchase(" in trade_facade,
            "legacy_purchase_disabled": "_xianshi_purchase_service().purchase(" not in trade_facade,
            "purchase_default_repository_direct": "self.xianshi_purchase_repository.purchase(" in trade_application_source and "xiuxian.xiuxian_trade.repository" not in trade_application_source,
            "purchase_compatibility_repository_direct": "XianshiPurchaseSqlRepository" in trade_feature_repository and ".purchase(" in trade_feature_repository and "XianshiPurchaseService" not in trade_feature_repository,
            "purchase_repository_feature_owned": "class XianshiPurchaseSqlRepository" in trade_xianshi_purchase_transactions and "DatabaseUnitOfWork(self.database, immediate=True)" in trade_xianshi_purchase_transactions,
            "xianshi_query_application_owned": "trade_application.xianshi_get_items(" in trade_facade and "XianshiQuerySqlRepository" in trade_application_source,
            "xianshi_query_repository_read_only": "DatabaseUnitOfWork(self.database, read_only=True)" in trade_xianshi_query_repository and "CREATE TABLE" not in trade_xianshi_query_repository,
            "legacy_xianshi_query_disabled": "xianshi_repository.get_xianshi_items(" not in trade_facade,
            "legacy_trade_repository_runtime_disabled": "from .repository import TradeRepository" not in trade_facade and "xianshi_repository = TradeRepository(" not in trade_facade,
            "legacy_xianshi_schema_adapter_explicit": "LegacyXianshiSchemaAdapter" in trade_facade and "from ..xiuxian.xiuxian_trade.repository import TradeRepository" in trade_xianshi_schema_adapter,
            "auction_compatibility_binding_feature_owned": "bind_auction_repository(_auction_bid_repository, _auction_session_service)" in trade_facade,
            "guishi_compatibility_repository_direct": "GuishiDepositSqlRepository(" in trade_feature_repository and "GuishiWithdrawSqlRepository(" in trade_feature_repository and "GuishiStoneService" not in trade_feature_repository,
            "guishi_legacy_service_isolated": "class GuishiStoneService" not in trade_auction_transactions and "class LegacyGuishiStoneService" in trade_legacy_guishi_compatibility,
            "guishi_legacy_getter_removed": "_guishi_stone_service" not in trade_facade and "GuishiStoneService" not in trade_facade,
            "guishi_deposit_application_owned": "trade_application.guishi_deposit(" in trade_deposit_handler,
            "legacy_guishi_deposit_disabled": "_guishi_stone_service().deposit(" not in trade_deposit_handler,
            "guishi_withdraw_application_owned": "trade_application.guishi_withdraw(" in trade_withdraw_handler,
            "legacy_guishi_withdraw_disabled": "_guishi_stone_service().withdraw(" not in trade_withdraw_handler,
            "guishi_qiugou_application_owned": "trade_application.guishi_qiugou(" in trade_qiugou_handler,
            "legacy_guishi_qiugou_disabled": "xianshi_repository.create_guishi_qiugou_order(" not in trade_qiugou_handler,
            "guishi_baitan_application_owned": "trade_application.guishi_baitan(" in trade_baitan_handler,
            "legacy_guishi_baitan_disabled": "xianshi_repository.create_guishi_baitan_order(" not in trade_baitan_handler,
            "guishi_cancel_qiugou_application_owned": "trade_application.guishi_cancel_qiugou(" in trade_cancel_qiugou_handler,
            "legacy_guishi_cancel_qiugou_disabled": "xianshi_repository.clear_guishi_qiugou_order(" not in trade_cancel_qiugou_handler,
            "guishi_cancel_baitan_application_owned": "trade_application.guishi_cancel_baitan(" in trade_cancel_baitan_handler,
            "legacy_guishi_cancel_baitan_disabled": "xianshi_repository.clear_expired_guishi_order(" not in trade_cancel_baitan_handler,
            "guishi_matching_application_owned": "trade_application.guishi_match(" in trade_facade and "xianshi_repository.match_guishi_orders(" not in trade_facade[trade_facade.index("async def process_guishi_transactions"):trade_facade.index("@scheduler.scheduled_job", trade_facade.index("async def process_guishi_transactions"))],
            "legacy_guishi_matching_disabled": "xianshi_repository.match_guishi_orders(" not in trade_facade[trade_facade.index("async def process_guishi_transactions"):trade_facade.index("@scheduler.scheduled_job", trade_facade.index("async def process_guishi_transactions"))],
            "guishi_expired_cleanup_application_owned": "trade_application.guishi_clear_expired_baitan(" in trade_expired_job,
            "legacy_guishi_expired_cleanup_disabled": "xianshi_repository.clear_expired_guishi_order(" not in trade_expired_job,
            "guishi_take_application_owned": "trade_application.guishi_take_stored_item(" in trade_take_handler,
            "legacy_guishi_take_disabled": "xianshi_repository.take_guishi_stored_item(" not in trade_take_handler,
            "status": "xianshi_query_purchase_listing_removal_legacy_schema_and_guishi_stone_cutover_with_other_trade_compatibility",
        },
        "auction": {
            "bid_application_owned": "AuctionBidSqlRepository" in auction_bid and "bid_application.place_bid(" in trade_auction_transactions and "auction_bid_application=_auction_bid_application" in trade_facade,
            "bid_effects_owned": "AuctionBidEffects" in auction_bid_application and "self.effects.on_bid(" in auction_bid_application and "self.outbox.append(" in auction_bid_application and "auction_bid_statistics_events" in auction_bid_statistics and "class LegacyAuctionBidEffects" in auction_compat_effects and "log_auction_bid_once" in auction_compat_effects and "LegacyAuctionBidEffects(get_paths().player_db)" in trade_facade and "LegacyAuctionBidEffects(str(context.database.path(\"player_db\")))" in web_app_source and "if bid_application is None and not bid_replayed:" in trade_auction_transactions,
            "bid_outbox_reconcile_owned": '"auction.bid.effects": context.services["auction"].reconcile_outbox_event' in plugin and 'handlers=getattr(context, "outbox_handlers", None)' in (PACKAGE / "adapters" / "web" / "blueprints" / "database.py").read_text(encoding="utf-8"),
            "bid_started_operation_recoverable": 'elif existing.status != "started"' in auction_bid_application and 'self.repository.place_auction_bid(' in auction_bid_application,
            "queue_application_owned": "AuctionQueueSqlRepository" in auction_queue,
            "queue_display_queries_application_owned": "get_player_items(" in auction_queue and "count_player_items(" in auction_queue and "read_only=True" in (PACKAGE / "features" / "auction" / "queue_repository.py").read_text(encoding="utf-8") and "_trade_manager().get_player_auction_items(" not in trade_facade,
            "legacy_queue_disabled": "_auction_queue_service().enqueue(" not in auction_queue_handlers and "_auction_queue_service().dequeue(" not in auction_queue_handlers,
            "session_start_application_owned": "AuctionSessionStartSqlRepository" in auction_start and "auction_session_start_application=_auction_session_start_application" in trade_facade and "start_application.start(" in trade_auction_transactions,
            "session_start_uses_settlement_application": "settlement_application.settle_active(" in trade_auction_transactions and "auction_settlement_application=_auction_settlement_application" in trade_facade,
            "settlement_application_owned": "AuctionSettlementSqlRepository" in auction_settlement,
            "settlement_effects_owned": "AuctionSettlementEffects" in auction_settlement and "self.effects.on_settlement(" in auction_settlement and "self.outbox.append(" in auction_settlement and "recover_started_operation" in auction_settlement and "auction_settlement_statistics_events" in auction_settlement_statistics and "safe_record_game_event" in auction_settlement_compat,
            "settlement_outbox_reconcile_owned": '"auction.settlement.effects": context.services["auction_settlement"].reconcile_outbox_event' in plugin and '"auction.settlement.effects": settlement.reconcile_outbox_event' in cli_source and 'outbox_handlers.setdefault("auction.settlement.effects", settlement.reconcile_outbox_event)' in web_app_source,
            "settlement_projection_ids_idempotent": "log_auction_event_once" in auction_settlement_compat and "skip_statistics\": True" in auction_settlement_compat and "season_rank_event_receipts" in (PACKAGE / "xiuxian" / "xiuxian_utils" / "season_rank_service.py").read_text(encoding="utf-8") and "event_id" in (PACKAGE / "xiuxian" / "xiuxian_utils" / "economy_log.py").read_text(encoding="utf-8"),
            "settlement_started_operation_recoverable": "recover_started_operation = True" in auction_settlement and 'status != "duplicate" or recover_started_operation' in auction_settlement,
            "settlement_compat_effects_disabled": "if settlement_application is not None:\n        logger.info(\"拍卖已结束，结算及副作用事件已提交！\")\n        return auction_results" in trade_auction_transactions,
            "legacy_settlement_disabled": "repository=LegacyAuctionSettlementRepository(" not in plugin,
            "trade_web_actions_application_owned": all(
                f"self.auction_{application}" in trade_application_source
                for application in ("queue", "session_start", "settlement")
            ) and all(
                f"self.auction_{application}." in trade_application_source
                for application in ("queue", "session_start", "settlement")
            ),
            "trade_web_finish_uses_settlement_ledger_directly": "if action == \"session_finish\" and self.repository is None:" in trade_application_source and "return self.auction_settlement.settle_active(" in trade_application_source,
            "trade_web_session_routes_admin": '"session_start": "admin"' in trade_web_source and '"session_finish": "admin"' in trade_web_source and '"admin" if action in {"session_start", "session_finish"}' in trade_manifest_source,
            "trade_web_real_route_replay_and_effects_covered": all(
                token in trade_web_test_source
                for token in (
                    "/api/v1/trade/enqueue",
                    "/api/v1/trade/dequeue",
                    "/api/v1/trade/session_start",
                    "/api/v1/trade/session_finish",
                    "finish_replay",
                    "self.effects.events",
                )
            ),
            "legacy_trade_auction_session_isolated": "AuctionSessionService" not in trade_feature_repository and "LegacyTradeAuctionSessionAdapter" in trade_feature_repository and "AuctionSessionService" in legacy_trade_auction_compatibility,
            "legacy_trade_auction_session_feature_owned": "AuctionSessionStartSqlRepository" in legacy_trade_auction_compatibility and "AuctionSettlementSqlRepository" in legacy_trade_auction_compatibility and "transaction_service" not in legacy_trade_auction_compatibility and "class AuctionSessionService" not in trade_auction_transactions,
            "legacy_settlement_adapter_feature_owned": "AuctionSettlementSqlRepository" in auction_settlement_source and "_auction_dependencies" not in auction_settlement_source and "transaction_service" not in auction_settlement_source,
            "display_queries_application_owned": "AuctionQuerySqlRepository" in auction_query and "AuctionQuerySqlRepository" in auction_query_repository and all(
                token in trade_facade for token in (
                    "_auction_query_application().get_current_auction(auction_id)",
                    "_auction_query_application().get_auction_history(auction_id)",
                    "_auction_query_application().get_current_auction()",
                )
            ) and "_auction_query_application().count_auction_history()" in trade_facade and "xianshi_repository.get_current_auction(auction_id)" not in trade_facade and "xianshi_repository.get_auction_history(auction_id)" not in trade_facade,
            "display_query_repository_does_not_own_ddl": "CREATE TABLE" not in auction_query_repository and "ensure_schema" not in auction_query_repository and "read_only=True" in auction_query_repository and "read_only: bool = False" in (PACKAGE / "infrastructure" / "database" / "uow.py").read_text(encoding="utf-8"),
            "scheduler_query_application_owned": "_auction_query_application().count_current_auctions()" in trade_facade and "xianshi_repository.get_current_auction()" not in trade_facade,
            "status": "bid_and_settlement_effects_owned_with_outbox_reconcile; trade_web_auction_actions_application_owned; display_queue_and_scheduler_queries_application_owned; explicit_auction_rollback_adapters_feature_owned",
        },
        "boss": {
            "manual_spawn_application_owned": "boss_application.spawn(" in boss_facade,
            "daily_limit_application_owned": "boss_application.reset_daily_limit(" in boss_facade,
            "punishment_application_owned": "boss_application.punish(" in boss_facade and "boss_application.punishment_snapshot(" in boss_facade,
            "legacy_manual_spawn_disabled": "_spawn_world_boss(" not in boss_facade or "boss_application.spawn(" in boss_facade,
            "status": "manual_spawn_daily_limit_punishment_cutover_with_other_boss_compatibility",
        },
        "buff": {
            "blessed_open_application_owned": "buff_application.open(" in buff_facade,
            "legacy_blessed_open_disabled": "_blessed_spot_service().open(" not in buff_facade,
            "blessed_rename_application_owned": "buff_application.rename(" in buff_facade,
            "legacy_blessed_rename_disabled": "_blessed_spot_service().rename(" not in buff_facade,
            "blessed_upgrade_application_owned": "buff_application.upgrade_field(" in buff_facade,
            "stone_training_application_owned": "buff_application.stone_training(" in buff_facade,
            "training_lifecycle_application_owned": "buff_application.training_start(" in buff_facade and "buff_application.training_complete(" in buff_facade,
            "closing_settlement_application_owned": "buff_application.closing_settle(" in buff_facade,
            "pvp_application_owned": "buff_application.pvp_settle(" in buff_facade,
            "partner_token_application_owned": (
                "_partner_token_application().apply(" in partner_facade
                and "PartnerTokenUseApplication" in partner_facade
            ),
            "legacy_partner_token_disabled": "_partner_token_service().apply(" not in partner_facade,
            "legacy_blessed_upgrade_disabled": "_blessed_spot_service().upgrade_field(" not in buff_facade,
            "status": "blessed_spot_open_rename_upgrade_stone_training_lifecycle_closing_settlement_pvp_partner_token_partner_cultivation_cutover_with_other_buff_compatibility",
        },
        "partner_cultivation": {
            "application_owned": (
                "_partner_cultivation_application().apply(" in partner_facade
                and "class PartnerCultivationApplication" in partner_cultivation_application
            ),
            "repository_owned": "class PartnerCultivationSqlRepository" in partner_cultivation_repository,
            "legacy_default_disabled": "PartnerCultivationService" not in partner_facade,
            "usage_settled_atomically": (
                "expected_used_count_1=limt_1" in partner_facade
                and "used_count=used_count+?" in partner_cultivation_repository
                and "two_exp_cd.add_user(" not in partner_facade
            ),
            "migration_owned": (
                '"buff.004"' in plugin
                and '"buff.005"' in plugin
                and "def apply_partner_cultivation_operations(" in buff_migrations
                and "def apply_partner_cultivation_player_schema(" in buff_migrations
            ),
            "request_path_has_no_ddl": "CREATE TABLE" not in partner_cultivation_repository,
            "status": "cultivation_settlement_cutover_with_legacy_transaction_service_retained_as_compatibility_reference",
        },
        "impart": {
            "love_sand_application_owned": "impart_application.love_sand(" in impart_facade,
            "card_compose_application_owned": "impart_application.compose(" in impart_facade,
            "card_disassemble_application_owned": "impart_application.disassemble(" in impart_facade,
            "prayer_application_owned": "impart_application.prayer_settle(" in impart_facade,
            "prayer_stats_transaction_owned": (
                "impart_database=get_paths().impart_db" in impart_facade
                and "player_database=get_paths().player_db" in impart_prayer_handler
                and 'update_statistics_value(user_id, "祈愿石使用"' not in impart_prayer_handler
                and 'invalidate_player_data_cache("statistics"' in impart_prayer_handler
                and all(f'"{field}"' in impart_migrations for field in ("祈愿石使用", "传承新卡", "传承重复卡"))
            ),
            "prayer_request_path_has_no_ddl": "CREATE TABLE" not in impart_prayer_repository,
            "prayer_schema_migrations_owned": (
                'Migration("impart.002", "impart_prayer_operations"' in plugin
                and 'Migration("impart.003", "impart_prayer_player_statistics"' in plugin
                and "def apply_impart_prayer_operations(" in impart_migrations
                and "def apply_impart_prayer_player_statistics(" in impart_migrations
            ),
            "status": "love_sand_compose_disassemble_prayer_cutover_with_other_impart_compatibility",
        },
        "mixelixir": {
            "harvest_level_application_owned": "mixelixir_application.harvest_level_upgrade(" in mixelixir_facade,
            "legacy_harvest_level_disabled": "_mixelixir_harvest_level_upgrade_service().upgrade(" not in mixelixir_facade,
            "status": "harvest_level_upgrade_cutover_with_other_mixelixir_compatibility",
        },
        "dongfu": {
            "expansion_application_owned": "dongfu_application.expand(" in dongfu_facade,
            "plant_application_owned": "dongfu_application.plant(" in dongfu_facade,
            "harvest_application_owned": "dongfu_application.harvest(" in dongfu_facade,
            "fertilize_application_owned": "dongfu_application.fertilize(" in dongfu_facade,
            "accelerate_application_owned": "dongfu_application.accelerate(" in dongfu_facade,
            "patrol_application_owned": "dongfu_application.patrol(" in dongfu_facade,
            "array_upgrade_application_owned": "dongfu_application.array_upgrade(" in dongfu_facade,
            "infiltrate_success_application_owned": "dongfu_application.infiltrate_success(" in dongfu_success_handler,
            "legacy_infiltrate_success_disabled": "_dongfu_infiltrate_success_service().settle(" not in dongfu_success_handler and "_run_dongfu_action(" not in dongfu_success_handler,
            "infiltrate_failure_application_owned": "dongfu_application.infiltrate_failure(" in dongfu_failure_handler,
            "legacy_infiltrate_failure_disabled": "_dongfu_infiltrate_failure_service().settle(" not in dongfu_failure_handler and "_run_dongfu_action(" not in dongfu_failure_handler,
            "status": "plant_harvest_fertilize_accelerate_patrol_array_upgrade_infiltrate_success_infiltrate_failure_cutover_with_other_dongfu_compatibility",
        },
        "impart_pk": {
            "project_join_application_owned": "impart_pk_application.project_join(" in impart_pk_facade,
            "training_replay_application_owned": "impart_pk_application.training_settle(" in impart_pk_facade,
            "legacy_training_replay_disabled": "_impart_training_settlement_service().get_result(" not in impart_pk_facade,
            "closing_enter_replay_application_owned": "impart_pk_application.closing_enter(" in impart_pk_facade,
            "legacy_closing_enter_replay_disabled": "_impart_closing_enter_service().get_result(" not in impart_pk_facade,
            "closing_settlement_replay_application_owned": "impart_pk_application.closing_settle(" in impart_pk_facade,
            "legacy_closing_settlement_replay_disabled": "_impart_closing_settlement_service().get_result(" not in impart_pk_facade,
            "explore_replay_application_owned": "impart_pk_application.explore_settle(" in impart_pk_facade,
            "legacy_explore_replay_disabled": "_impart_explore_settlement_service().get_result(" not in impart_pk_facade,
            "battle_replay_application_owned": "impart_pk_application.battle_settle(" in impart_pk_facade,
            "legacy_battle_replay_disabled": "_impart_battle_batch_service().get_result(" not in impart_pk_facade,
            "status": "training_closing_enter_settlement_explore_battle_replay_cutover_with_other_impart_pk_compatibility",
        },
        "admin": {
            "item_destroy_application_owned": "admin_asset_application.destroy_item(" in admin_facade,
            "exp_adjust_application_owned": "admin_asset_application.adjust_exp(" in admin_facade,
            "level_change_application_owned": "admin_asset_application.change_level(" in admin_facade,
            "root_change_application_owned": "admin_asset_application.change_root(" in admin_facade,
            "impart_stone_application_owned": "admin_asset_application.adjust_impart_stone(" in admin_facade,
            "accessory_application_owned": "admin_asset_application.adjust_accessory(" in admin_facade,
            "player_status_batch_application_owned": "admin_application.reset_player_status_batch(" in admin_facade,
            "item_batch_application_owned": "admin_application.grant_item_batch(" in admin_facade,
            "accessory_batch_application_owned": "admin_application.grant_accessory_batch(" in admin_facade,
            "impart_stone_batch_application_owned": "admin_application.adjust_impart_stone_batch(" in admin_facade,
            "blackhouse_application_owned": "admin_application.set_blackhouse_status(" in admin_facade,
            "player_status_application_owned": "admin_application.reset_player_status(" in admin_facade,
            "status": "item_destroy_exp_adjust_level_root_impart_stone_accessory_player_status_player_status_batch_item_batch_accessory_batch_impart_stone_batch_blackhouse_cutover_with_admin_batch_compatibility",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    slices = _slice_status()
    blockers = [
        "legacy transaction services remain",
        "xiuxian2_handle remains in legacy execution paths",
    ]
    if slices["sign_in"]["lottery_compatibility_fallback"]:
        blockers.append("sign_in explicit lottery compatibility fallback remains")
    report = {
        "schema": 1,
        "scope": "full_refactor_phase2",
        "counts": _counts(),
        "slices": slices,
        "exit_ready": False,
        "exit_blockers": blockers,
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=None if args.json else 2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
