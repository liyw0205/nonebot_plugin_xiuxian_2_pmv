from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from ...infrastructure.database import DatabaseUnitOfWork
from ...infrastructure.filesystem import atomic_write


def log_closing_event_once(
    *,
    user_id: str,
    message: str,
    event_id: str,
    occurred_at: str,
    player_database: str | Path,
    players_dir: str | Path,
    resolve_impersonation: bool = True,
) -> bool:
    """Append one closing log record without duplicating a projection replay."""
    if not Path(player_database).is_file():
        raise RuntimeError("player database is unavailable for closing log projection")
    try:
        when = datetime.fromisoformat(str(occurred_at)).astimezone()
    except ValueError as exc:
        raise ValueError("closing event has an invalid occurred_at") from exc

    legacy_utils = sys.modules.get("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils")
    impersonating = getattr(legacy_utils, "_impersonating_users", {})
    target_user = str(impersonating.get(str(user_id), user_id) if resolve_impersonation else user_id)
    root = Path(players_dir)
    logs_dir = root / target_user / "logs"
    log_file = logs_dir / f"{when.strftime('%y%m%d')}.log"
    record = {
        "timestamp": when.strftime("%Y-%m-%d %H:%M:%S"),
        "message": str(message),
        "event_id": str(event_id),
    }
    with DatabaseUnitOfWork(player_database, immediate=True):
        logs_dir.mkdir(parents=True, exist_ok=True)
        existing = json.loads(log_file.read_text(encoding="utf-8")) if log_file.exists() else []
        if not isinstance(existing, list):
            raise ValueError("closing log must contain a JSON list")
        if any(isinstance(item, dict) and str(item.get("event_id", "")) == str(event_id) for item in existing):
            return False
        existing.insert(0, record)
        atomic_write(log_file, json.dumps(existing, ensure_ascii=False, indent=4).encode("utf-8"))
    return True


__all__ = ["log_closing_event_once"]
