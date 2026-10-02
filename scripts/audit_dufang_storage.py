#!/usr/bin/env python3
"""Read-only storage and recovery inventory for SQLite data directories."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from nonebot_plugin_xiuxian_2.infrastructure.database.backup_capacity import backup_reserve_bytes


DATABASE_FILES = {
    "game_db": "xiuxian.db",
    "player_db": "player.db",
    "trade_db": "trade.db",
    "impart_db": "xiuxian_impart.db",
    "message_db": "message.db",
}
MAX_BACKUP_SCAN_ENTRIES = 50_000

AUDITED_TABLES = {
    "game_db": (
        "dufang_bets",
        "dufang_bet_operations",
        "dufang_payout_operations",
        "dufang_bet_resolutions",
        "dufang_player_outbox",
        "dufang_share_operations",
        "dufang_share_progress",
        "operation_ledger",
        "operation_audit",
        "domain_outbox",
        "economy_log",
    ),
    "player_db": (
        "dufang_player_operation_receipts",
        "dufang_share_player_receipts",
        "unseal_data",
    ),
}


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _readonly_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=2.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    return connection


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row["name"])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _table_inventory(connection: sqlite3.Connection, table: str) -> dict[str, Any]:
    quoted_table = _quote_identifier(table)
    columns = list(connection.execute(f"PRAGMA table_info({quoted_table})"))
    text_columns = [
        str(row["name"])
        for row in columns
        if any(marker in str(row["type"]).upper() for marker in ("TEXT", "CHAR", "CLOB"))
    ]
    text_sum = " + ".join(
        f"COALESCE(length(CAST({_quote_identifier(name)} AS BLOB)),0)"
        for name in text_columns
    ) or "0"
    row = connection.execute(
        f"SELECT COUNT(*) AS row_count, COALESCE(SUM({text_sum}),0) AS text_bytes "
        f"FROM {quoted_table}"
    ).fetchone()
    result: dict[str, Any] = {
        "row_count": int(row["row_count"]),
        "text_bytes_estimate": int(row["text_bytes"]),
    }
    if any(str(column["name"]).casefold() == "status" for column in columns):
        status_rows = connection.execute(
            f"SELECT status,COUNT(*) AS count FROM {quoted_table} GROUP BY status ORDER BY status"
        )
        result["status_counts"] = {
            str(status_row["status"]): int(status_row["count"])
            for status_row in status_rows
        }
    return result


def _pending_inventory(connection: sqlite3.Connection, tables: set[str]) -> dict[str, int]:
    pending: dict[str, int] = {}
    checks = {
        "dufang_bets_pending": ("dufang_bets", "status='pending'"),
        "dufang_player_outbox_pending": ("dufang_player_outbox", "status='pending'"),
        "dufang_share_progress_pending": ("dufang_share_progress", "status='pending'"),
        "domain_outbox_pending_or_dead": ("domain_outbox", "status IN ('pending','dead')"),
        "ledger_nonterminal": (
            "operation_ledger",
            "status IN ('started','failed','needs_reconcile')",
        ),
    }
    for label, (table, condition) in checks.items():
        if table not in tables:
            continue
        pending[label] = int(
            connection.execute(f'SELECT COUNT(*) FROM "{table}" WHERE {condition}').fetchone()[0]
        )
    if "dufang_share_operations" in tables:
        columns = {
            str(row["name"]).casefold()
            for row in connection.execute('PRAGMA table_info("dufang_share_operations")')
        }
        if {"completed", "total"}.issubset(columns):
            pending["dufang_share_operations_incomplete"] = int(
                connection.execute(
                    "SELECT COUNT(*) FROM dufang_share_operations WHERE completed < total"
                ).fetchone()[0]
            )
    return pending


def _sidecar_inventory(path: Path) -> dict[str, int]:
    sidecars: dict[str, int] = {}
    for suffix in ("-wal", "-shm", "-journal"):
        candidate = Path(f"{path}{suffix}")
        if candidate.is_file():
            sidecars[suffix[1:]] = candidate.stat().st_size
    return sidecars


def _nearest_existing_directory(path: Path) -> Path:
    candidate = path.expanduser().resolve(strict=False)
    while not candidate.exists():
        candidate = candidate.parent
    return candidate if candidate.is_dir() else candidate.parent


def _directory_inventory(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        return {
            "exists": True,
            "file_count": 0,
            "bytes": 0,
            "scan_complete": False,
            "reason": "symlink directory was not traversed",
        }
    if not path.is_dir():
        return {"exists": False, "file_count": 0, "bytes": 0, "scan_complete": True}
    pending = [path]
    scanned_entries = 0
    file_count = 0
    total_bytes = 0
    complete = True
    while pending and complete:
        directory = pending.pop()
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if scanned_entries >= MAX_BACKUP_SCAN_ENTRIES:
                        complete = False
                        break
                    scanned_entries += 1
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            pending.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            file_count += 1
                            total_bytes += entry.stat(follow_symlinks=False).st_size
                    except OSError:
                        complete = False
                        break
        except OSError:
            complete = False
    return {
        "exists": True,
        "file_count": file_count,
        "bytes": total_bytes,
        "scan_complete": complete,
        "scanned_entries": scanned_entries,
        "entry_limit": MAX_BACKUP_SCAN_ENTRIES,
    }


def audit_data_dir(
    data_dir: str | Path,
    *,
    backup_dir: str | Path | None = None,
    reserve_bytes: int = 0,
) -> dict[str, Any]:
    """Inventory persistent SQLite state without creating or changing files."""
    if reserve_bytes < 0:
        raise ValueError("reserve_bytes must not be negative")
    root = Path(data_dir).expanduser().resolve(strict=False)
    file_specs = [(key, root / filename, key) for key, filename in DATABASE_FILES.items()]
    activity_path = root / "activity" / "activity.db"
    file_specs.append(("legacy_activity", activity_path, "legacy_activity"))

    files: list[dict[str, Any]] = []
    pending_work: dict[str, int] = {}
    backup_estimate_bytes = 0
    extra_file_estimate_bytes = 0
    estimate_complete = True
    for key, path, database_key in file_specs:
        entry: dict[str, Any] = {
            "key": key,
            "path": str(path),
            "exists": path.is_file(),
            "physical_bytes": path.stat().st_size if path.is_file() else 0,
            "sidecars": _sidecar_inventory(path),
        }
        entry["sidecar_bytes"] = sum(entry["sidecars"].values())
        if not path.is_file():
            entry["status"] = "missing"
            files.append(entry)
            if key in {"game_db", "player_db"}:
                estimate_complete = False
            continue

        connection: sqlite3.Connection | None = None
        try:
            connection = _readonly_connection(path)
            page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
            page_bytes = page_count * page_size
            backup_estimate_bytes += page_bytes
            tables = _table_names(connection)
            table_stats = {
                table: _table_inventory(connection, table)
                for table in AUDITED_TABLES.get(database_key, ())
                if table in tables
            }
            if database_key in {"game_db", "player_db"}:
                pending_work.update(_pending_inventory(connection, tables))
            entry.update(
                {
                    "status": "ok",
                    "page_count": page_count,
                    "page_size": page_size,
                    "freelist_count": freelist_count,
                    "logical_page_bytes": page_bytes,
                    "reusable_page_bytes": freelist_count * page_size,
                    "tables": table_stats,
                }
            )
        except sqlite3.DatabaseError as exc:
            entry.update({"status": "unreadable", "error": str(exc)})
            estimate_complete = False
        finally:
            if connection is not None:
                connection.close()
        files.append(entry)

    config_path = root / "config.json"
    if config_path.is_file():
        extra_file_estimate_bytes = config_path.stat().st_size

    target = Path(backup_dir).expanduser() if backup_dir is not None else root / "backups"
    capacity_path = _nearest_existing_directory(target)
    existing_backup_inventory = _directory_inventory(target)
    free_bytes = shutil.disk_usage(capacity_path).free
    backup_payload_estimate_bytes = backup_estimate_bytes + extra_file_estimate_bytes
    effective_reserve_bytes = backup_reserve_bytes(
        backup_payload_estimate_bytes, additional_bytes=reserve_bytes
    )
    required_bytes = backup_payload_estimate_bytes + effective_reserve_bytes
    return {
        "schema": 1,
        "scope": "dufang_receipts_and_sqlite_backup_capacity",
        "read_only": True,
        "data_dir": str(root),
        "files": files,
        "pending_work": pending_work,
        "backup_capacity": {
            "target": str(target.resolve(strict=False)),
            "capacity_checked_at": str(capacity_path),
            "available_bytes": free_bytes,
            "minimum_sqlite_backup_estimate_bytes": backup_estimate_bytes,
            "extra_file_estimate_bytes": extra_file_estimate_bytes,
            "minimum_backup_payload_estimate_bytes": backup_payload_estimate_bytes,
            "reserve_bytes": effective_reserve_bytes,
            "estimate_complete": estimate_complete,
            "capacity_estimate_sufficient": estimate_complete and free_bytes >= required_bytes,
            "estimate_is_lower_bound": True,
            "existing_backup_inventory": existing_backup_inventory,
        },
        "archive_ready": False,
        "archive_blockers": [
            "This inventory does not prove a clean reconcile or backup restore.",
            "No replay window or approved archive format is defined; persistent receipts are retained.",
            "Pending and nonterminal records must not be removed.",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", required=True, help="existing runtime data directory; never created")
    parser.add_argument("--backup-dir", help="backup destination used only for a free-space estimate")
    parser.add_argument("--reserve-bytes", type=int, default=0, help="additional reserve beyond the shared minimum policy")
    args = parser.parse_args(argv)
    try:
        report = audit_data_dir(args.data_dir, backup_dir=args.backup_dir, reserve_bytes=args.reserve_bytes)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
