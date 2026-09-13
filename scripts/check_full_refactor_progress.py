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
    return {
        "stone_gift": {
            "default_legacy_handler_disabled": '"送灵石" if _legacy_stone_gift_enabled' in base,
            "nonebot_application_path": "_build_stone" in adapter and "application.read_limits" in adapter and "application.reply" in adapter,
            "web_application_path": "create_stone_gift_blueprint" in web and "application.transfer" in web,
            "old_service_removed": False,
            "status": "cutover_with_rollback_old_service_retained",
        },
        "sign_in": {
            "default_legacy_handler_disabled": '"修仙签到" if _legacy_sign_in_enabled' in base,
            "nonebot_application_path": "handle_sign_in" in adapter,
            "web_application_path": "create_sign_in_blueprint" in web and "application.claim" in web,
            "old_service_removed": False,
            "status": "cutover_with_rollback_old_service_and_side_effects_retained",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    report = {
        "schema": 1,
        "scope": "full_refactor_phase2",
        "counts": _counts(),
        "slices": _slice_status(),
        "exit_ready": False,
        "exit_blockers": [
            "legacy transaction services remain",
            "xiuxian2_handle remains in legacy execution paths",
            "sign_in side effects are not yet application-owned",
            "full NoneBot registration duplicate-prefix snapshot is not clean",
        ],
    }
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=None if args.json else 2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
