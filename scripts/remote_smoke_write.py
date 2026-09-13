"""Perform the harmless, reversible write used by remote_smoke.sh.

The hook deliberately writes only a marker under ``XIUXIAN_DATA_DIR``.  It
exercises the deployment's write and rollback hooks without touching a player,
trade, or production database row.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation-id", default=os.environ.get("SMOKE_OPERATION_ID", ""))
    args = parser.parse_args(argv)
    operation_id = str(args.operation_id).strip()
    data_dir = Path(os.environ.get("XIUXIAN_DATA_DIR", "")).expanduser()
    if not operation_id:
        parser.error("--operation-id or SMOKE_OPERATION_ID is required")
    if not data_dir.is_absolute():
        parser.error("XIUXIAN_DATA_DIR must be an absolute path")
    data_dir.mkdir(parents=True, exist_ok=True)
    marker = data_dir / "remote-smoke-marker.json"
    payload = {
        "operation_id": operation_id,
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    temporary = marker.with_suffix(".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(marker)
    print(json.dumps({"ok": True, "marker": str(marker), "operation_id": operation_id}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
