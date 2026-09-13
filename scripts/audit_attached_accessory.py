#!/usr/bin/env python3
"""Read-only audit for the attached accessory namespace."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

with redirect_stdout(StringIO()):
    from nonebot_plugin_xiuxian_2.features.accessory_package.attached_migrations import (
        ATTACHED_SCHEMA_NAME,
        ATTACHED_SCHEMA_VERSION,
        _checksum,
    )


def audit(game: Path, player: Path) -> dict[str, object]:
    connection = sqlite3.connect(game)
    try:
        connection.execute("ATTACH DATABASE ? AS player_data", (str(player),))
        rows = connection.execute(
            "SELECT name FROM player_data.sqlite_master WHERE type='table' AND name IN (?, ?)",
            ("attached_schema_migrations", ATTACHED_SCHEMA_NAME),
        ).fetchall()
        tables = {str(row[0]) for row in rows}
        ledger = connection.execute(
            "SELECT version,name,checksum,applied_at FROM player_data.attached_schema_migrations WHERE version=?",
            (ATTACHED_SCHEMA_VERSION,),
        ).fetchone() if "attached_schema_migrations" in tables else None
        accessories = connection.execute("SELECT COUNT(*) FROM player_data.player_accessory").fetchone()[0] if ATTACHED_SCHEMA_NAME in tables else 0
        return {
            "game_database": str(game),
            "player_database": str(player),
            "tables": sorted(tables),
            "accessory_rows": int(accessories),
            "migration": {
                "version": ATTACHED_SCHEMA_VERSION,
                "name": ATTACHED_SCHEMA_NAME,
                "expected_checksum": _checksum(),
                "applied": ledger is not None,
                "ledger": None if ledger is None else {"version": ledger[0], "name": ledger[1], "checksum": ledger[2], "applied_at": ledger[3]},
            },
            "read_only": True,
        }
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--game", required=True, type=Path)
    parser.add_argument("--player", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(audit(args.game, args.player), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
