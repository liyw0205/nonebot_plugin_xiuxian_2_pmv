"""玩家自己的洞府收获记录（只读）。

`dongfu_harvest_operations` 在每次成功收获时由真实写路径落行：`payload` 是规范列表
``[user_id, expected_slots, slot_numbers, reward_rows, failed_slots]``，其中
`reward_rows` 为 ``[物品ID, 名称, 类型, 数量]``；`rewards` 列另有按物品聚合的
``[ID, 数量]``。`我的洞府` 只展示当前灵田与作物，玩家看不到最近收获了哪些物品。
本模块补齐这段个人历史。

只读；缺库、缺表、缺列、坏 JSON 或读失败返回空态，不建表、不补列。时间窗只用下界。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

DAYS = 7
RECENT = 10
EMPTY_TEXT = "近7天没有洞府收获记录。"

_RECEIPT_COLUMNS = frozenset({"payload", "rewards", "created_at"})


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


def _decode(row: Any) -> dict[str, Any] | None:
    try:
        payload = json.loads(str(row["payload"] or "[]"))
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, list) or len(payload) < 4:
        return None
    raw_rewards = payload[3]
    if not isinstance(raw_rewards, list):
        return None
    items: list[dict[str, Any]] = []
    for item in raw_rewards:
        if not isinstance(item, (list, tuple)) or len(item) < 4:
            continue
        amount = _int(item[3])
        if amount > 0:
            items.append(
                {
                    "id": str(item[0]),
                    "name": str(item[1]),
                    "type": str(item[2]),
                    "amount": amount,
                }
            )
    if not items:
        return None
    failed = payload[4] if len(payload) > 4 and isinstance(payload[4], list) else []
    return {
        "items": items,
        "failed_slots": len(failed),
        "created_at": _stamp(row["created_at"]),
    }


class DongfuHarvestHistoryApplication:
    """近 N 天本人的洞府收获记录，只读。"""

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
            "total_items": 0,
            "distinct_items": 0,
            "recent": [],
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询洞府收获记录。")
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not _columns(uow, "dongfu_harvest_operations").issuperset(
                    _RECEIPT_COLUMNS
                ):
                    return self._empty()
                rows = uow.query_all(
                    "SELECT payload, created_at FROM dongfu_harvest_operations "
                    "WHERE json_valid(payload) "
                    "AND json_extract(payload, '$[0]') = ? AND created_at >= ? "
                    "ORDER BY created_at DESC, rowid DESC",
                    (user_id, since),
                )
        except Exception:
            return self._empty()

        runs: list[dict[str, Any]] = []
        for row in rows:
            item = _decode(row)
            if item is not None:
                runs.append(item)
        if not runs:
            return self._empty()
        all_items = [item for run in runs for item in run["items"]]
        return {
            "ok": True,
            "notice": None,
            "total": len(runs),
            "total_items": sum(item["amount"] for item in all_items),
            "distinct_items": len({item["id"] for item in all_items}),
            "recent": runs[:RECENT],
        }


def _items_text(items: list[dict[str, Any]]) -> str:
    parts = [f"{item['name']}×{_int(item['amount'])}" for item in items[:4]]
    if len(items) > 4:
        parts.append(f"等{len(items)}种")
    return "、".join(parts)


def format_dongfu_harvest_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    total = _int(summary.get("total"))
    if total <= 0:
        return EMPTY_TEXT
    lines = [
        f"近{DAYS}天洞府收获记录",
        f"收获：{total} 次（共 {_int(summary.get('total_items'))} 件，"
        f"{_int(summary.get('distinct_items'))} 种物品）",
    ]
    recent = summary.get("recent") or []
    if recent:
        lines.append("最近：")
        for item in recent:
            suffix = (
                f"（{_int(item['failed_slots'])} 块未成熟）"
                if _int(item.get("failed_slots"))
                else ""
            )
            lines.append(
                f"- {item['created_at'] or '时间未知'} "
                f"{_items_text(item.get('items') or [])}{suffix}"
            )
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "RECENT",
    "DongfuHarvestHistoryApplication",
    "format_dongfu_harvest_history",
]
