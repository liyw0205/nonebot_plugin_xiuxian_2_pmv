#!/usr/bin/env python3
"""Read-only audit for the bank attached namespace and ledgers."""
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


def audit(game: Path, player: Path) -> dict[str, object]:
    conn = sqlite3.connect(game)
    try:
        conn.execute("ATTACH DATABASE ? AS player_data", (str(player),))
        names = {row[0] for row in conn.execute("SELECT name FROM player_data.sqlite_master WHERE type='table'")}
        bankinfo = "bankinfo" in names
        rows = int(conn.execute("SELECT COUNT(*) FROM player_data.bankinfo").fetchone()[0]) if bankinfo else 0
        ledger_names = [name for name in names if name.startswith("bank_") and name.endswith("_operations")]
        ledgers = {name: int(conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]) for name in sorted(ledger_names)}
        return {"game_database": str(game), "player_database": str(player), "bankinfo": bankinfo, "bankinfo_rows": rows, "operation_ledgers": ledgers, "read_only": True}
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--game", required=True, type=Path)
    parser.add_argument("--player", required=True, type=Path)
    args = parser.parse_args()
    with redirect_stdout(StringIO()):
        payload = audit(args.game, args.player)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
