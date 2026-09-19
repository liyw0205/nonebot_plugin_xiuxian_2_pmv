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
