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
    plugin = (PACKAGE / "plugin.py").read_text(encoding="utf-8")
    arena = (PACKAGE / "xiuxian" / "xiuxian_arena" / "__init__.py").read_text(encoding="utf-8")
    arena_limit = (PACKAGE / "xiuxian" / "xiuxian_arena" / "arena_limit.py").read_text(encoding="utf-8")
    tower_limit = (PACKAGE / "xiuxian" / "xiuxian_tower" / "tower_limit.py").read_text(encoding="utf-8")
    training_limit = (PACKAGE / "xiuxian" / "xiuxian_training" / "training_limit.py").read_text(encoding="utf-8")
    work_facade = (PACKAGE / "xiuxian" / "xiuxian_work" / "__init__.py").read_text(encoding="utf-8")
    activity_service = (PACKAGE / "xiuxian" / "xiuxian_activity" / "service.py").read_text(encoding="utf-8")
    dungeon_facade = (PACKAGE / "xiuxian" / "xiuxian_dungeon" / "__init__.py").read_text(encoding="utf-8")
    bank_facade = (PACKAGE / "xiuxian" / "xiuxian_bank" / "__init__.py").read_text(encoding="utf-8")
    map_facade = (PACKAGE / "xiuxian" / "xiuxian_map" / "__init__.py").read_text(encoding="utf-8")
    sect_facade = (PACKAGE / "xiuxian" / "xiuxian_sect" / "__init__.py").read_text(encoding="utf-8")
    natal_facade = (PACKAGE / "xiuxian" / "xiuxian_natal_treasure" / "__init__.py").read_text(encoding="utf-8")
    world_events_facade = (PACKAGE / "xiuxian" / "xiuxian_world_events" / "__init__.py").read_text(encoding="utf-8")
    rift_facade = (PACKAGE / "xiuxian" / "xiuxian_rift" / "__init__.py").read_text(encoding="utf-8")
    back_facade = (PACKAGE / "xiuxian" / "xiuxian_back" / "__init__.py").read_text(encoding="utf-8")
    past_life_facade = (PACKAGE / "xiuxian" / "xiuxian_past_life" / "__init__.py").read_text(encoding="utf-8")
    dufang_facade = (PACKAGE / "xiuxian" / "xiuxian_dufang" / "__init__.py").read_text(encoding="utf-8")
    fusion_facade = (PACKAGE / "xiuxian" / "xiuxian_fusion" / "__init__.py").read_text(encoding="utf-8")
    title_facade = (PACKAGE / "xiuxian" / "xiuxian_title" / "__init__.py").read_text(encoding="utf-8")
    base_facade = (PACKAGE / "xiuxian" / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
    puppet_facade = (PACKAGE / "xiuxian" / "xiuxian_puppet" / "__init__.py").read_text(encoding="utf-8")
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
            "task_core_legacy": "SignInTaskEffects(record_task_progress)" in plugin,
            "lottery_core_default_legacy": "if legacy_lottery else LotteryApplication(" not in plugin,
            "lottery_compatibility_fallback": "XIUXIAN_SIGN_IN_LEGACY_LOTTERY" in plugin and "LotterySettlementService" in plugin,
            "status": "cutover_with_compatibility_rollback_side_effects_retained",
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
            "status": "daily_refresh_cutover_with_legacy_service_retained_for_compatibility",
        },
        "activity_reward": {
            "claim_all_application_owned": "activity_claim_all_application.run(" in activity_service,
            "legacy_claim_all_disabled": "_activity_claim_all_service().run(" not in activity_service,
            "status": "claim_all_cutover_with_legacy_service_retained_for_compatibility",
        },
        "dungeon_team": {
            "create_invite_application_owned": "dungeon_team_application.create(" in dungeon_facade and "dungeon_team_application.invite(" in dungeon_facade,
            "legacy_create_invite_disabled": "_dungeon_team_transaction_service().create(" not in dungeon_facade and "_dungeon_team_transaction_service().invite(" not in dungeon_facade,
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
            "legacy_membership_disabled": all(token not in sect_facade for token in ("sect_membership_service.join", "sect_membership_service.leave_sect", "sect_membership_service.kick_member", "sect_membership_service.change_position")),
            "status": "membership_economy_cutover_with_other_sect_compatibility_paths",
        },
        "natal_treasure": {
            "awaken_application_owned": "natal_treasure_application.awaken(" in natal_facade,
            "legacy_awaken_disabled": "_natal_awaken_service().awaken(" not in natal_facade,
            "status": "awaken_cutover_with_other_natal_mutations_compatibility",
        },
        "world_events": {
            "claim_application_owned": "demon_claim_application.claim(" in world_events_facade,
            "legacy_claim_disabled": "_demon_claim_service().claim(" not in world_events_facade,
            "status": "demon_claim_cutover_with_other_world_event_compatibility",
        },
        "rift": {
            "entry_application_owned": "rift_application.enter(" in rift_facade,
            "legacy_entry_disabled": "_rift_entry_service().enter(" not in rift_facade,
            "status": "entry_cutover_with_other_rift_compatibility",
        },
        "back": {
            "repair_application_owned": "back_application.repair(" in back_facade,
            "legacy_repair_disabled": "_backpack_repair_service().run(" not in back_facade,
            "status": "backpack_repair_cutover_with_other_back_compatibility",
        },
        "past_life": {
            "reset_one_application_owned": "past_life_application.reset_one(" in past_life_facade,
            "legacy_reset_one_disabled": "_past_life_reset_service().reset_one(" not in past_life_facade,
            "status": "reset_one_cutover_with_reset_all_compatibility",
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
