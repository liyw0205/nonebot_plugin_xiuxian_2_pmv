from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from ...xiuxian.xiuxian_utils.shared_read_cache import (
    process_read_cache,
    shared_snapshot_key,
)


DEFAULT_DISPLAY_TTL_SECONDS = 60 * 60


def shared_display_snapshot(
    database_path: str | Path,
    view: str,
    *parts: Any,
    loader: Callable[[], Any],
    dependency_databases: tuple[str | Path, ...] = (),
    ttl: int = DEFAULT_DISPLAY_TTL_SECONDS,
) -> dict[str, Any]:
    dependencies = tuple(Path(path) for path in dependency_databases)
    dependency_keys = tuple(
        shared_snapshot_key(path, "database-identity") for path in dependencies
    )
    key = shared_snapshot_key(
        database_path, view, *parts, ("dependencies", *dependency_keys)
    )
    return process_read_cache.get_or_load(
        key,
        database_path=database_path,
        dependency_databases=dependencies,
        ttl=ttl,
        snapshot=True,
        loader=lambda: {
            "rows": loader(),
            "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
    )


def display_snapshot_freshness(snapshot: Mapping[str, Any]) -> str:
    raw = str(snapshot.get("as_of") or "")
    try:
        captured = datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
        stamp = captured.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (TypeError, ValueError, OverflowError):
        stamp = "时间未知"
    return (
        f"公共榜单快照截至 {stamp}（最长缓存 60 分钟）；"
        "个人名次与资格按实时数据计算。"
    )


__all__ = [
    "DEFAULT_DISPLAY_TTL_SECONDS",
    "display_snapshot_freshness",
    "shared_display_snapshot",
]
