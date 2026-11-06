"""玩家自己的副本探索记录（只读）。

`dungeon_explore_operations` 在每次探索结算时落行：`request_identity` 是
``{"action": "explore", "user_id": ...}`` 的规范 JSON，成功结算写
`phase='completed'`、`result_status='applied'`、`result_json`（含 message 与战斗消息）、
`current_layer`、`dungeon_status`、`updated_at`。玩家只有 `我的副本状态` 的当前进度，
看不到近几天探索过哪些层、结果与奖励摘要；本模块补齐这段个人历史。

只读；缺表/缺列/坏 JSON 或读失败返回空态，不建表、不补列。时间窗只用下界。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

DAYS = 7
RECENT = 10
EMPTY_TEXT = "近7天没有副本探索记录。"

_COLUMNS = frozenset(
    {
        "request_identity",
        "phase",
        "result_status",
        "result_json",
        "current_layer",
        "dungeon_status",
        "updated_at",
    }
)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _stamp(value: Any) -> str:
    text = str(value or "").strip()
    return text[:19] if text else ""


def _columns(uow: DatabaseUnitOfWork, table: str) -> frozenset[str]:
    rows = uow.query_all(f'PRAGMA table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def _identity(user_id: str) -> str:
    return json.dumps(
        {"action": "explore", "user_id": user_id}, ensure_ascii=True, sort_keys=True
    )


class DungeonExploreHistoryApplication:
    """近 N 天本人的副本探索结算记录，只读。"""

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = str(database)
        self._clock = clock

    def _now(self) -> datetime:
        return self._clock.now() if self._clock is not None else datetime.now()

    @staticmethod
    def _empty(notice: str = EMPTY_TEXT) -> dict[str, Any]:
        return {
            "ok": False,
            "notice": notice,
            "total": 0,
            "cleared": 0,
            "best_layer": 0,
            "battles": 0,
            "recent": [],
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询副本记录。")
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not _columns(uow, "dungeon_explore_operations").issuperset(_COLUMNS):
                    return self._empty()
                rows = uow.query_all(
                    "SELECT result_json,current_layer,dungeon_status,updated_at "
                    "FROM dungeon_explore_operations "
                    "WHERE request_identity = ? AND phase = 'completed' "
                    "AND result_status = 'applied' AND updated_at >= ? "
                    "ORDER BY updated_at DESC, rowid DESC",
                    (_identity(user_id), since),
                )
        except Exception:
            return self._empty()

        runs: list[dict[str, Any]] = []
        for row in rows:
            try:
                response = json.loads(str(row["result_json"] or "{}"))
            except (TypeError, ValueError):
                continue
            if not isinstance(response, dict):
                continue
            battle_messages = response.get("battle_messages")
            runs.append(
                {
                    "message": str(response.get("message") or "").strip(),
                    "battles": len(battle_messages) if isinstance(battle_messages, list) else 0,
                    "current_layer": _int(row["current_layer"]),
                    "dungeon_status": str(row["dungeon_status"] or ""),
                    "updated_at": _stamp(row["updated_at"]),
                }
            )
        if not runs:
            return self._empty()
        return {
            "ok": True,
            "notice": None,
            "total": len(runs),
            "cleared": sum(1 for item in runs if item["dungeon_status"] == "completed"),
            "best_layer": max(item["current_layer"] for item in runs),
            "battles": sum(item["battles"] for item in runs),
            "recent": runs[:RECENT],
        }


def _one_line(text: str, limit: int = 120) -> str:
    compact = " / ".join(part.strip() for part in text.splitlines() if part.strip())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1] + "…"


def format_dungeon_explore_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    total = _int(summary.get("total"))
    if total <= 0:
        return EMPTY_TEXT
    lines = [
        f"近{DAYS}天副本探索记录",
        f"探索：{total} 次（通关 {_int(summary.get('cleared'))}）",
        f"最高层：{_int(summary.get('best_layer'))} 层 · 战斗 {_int(summary.get('battles'))} 场",
    ]
    recent = summary.get("recent") or []
    if recent:
        lines.append("最近：")
        for item in recent:
            state = "通关" if item["dungeon_status"] == "completed" else "探索中"
            text = _one_line(str(item.get("message") or ""))
            suffix = f"：{text}" if text else ""
            lines.append(
                f"- {item['updated_at'] or '时间未知'} "
                f"第{_int(item['current_layer'])}层 {state}{suffix}"
            )
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "RECENT",
    "DungeonExploreHistoryApplication",
    "format_dungeon_explore_history",
]
