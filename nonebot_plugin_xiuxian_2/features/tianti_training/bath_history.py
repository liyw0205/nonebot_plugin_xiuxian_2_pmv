"""玩家自己的炼体药浴记录（只读）。

`tianti_medicine_bath_operations` 在每次药浴开启时由真实写路径落行：
`request_json` 为 ``{"plan": [...], "effect": float}``，`result_json` 保存
``{"consumed": [...], "effect": float, "bath_name": str, "end_time": str,
"settlement": {...}}``，`created_at` 可排序。该表此前只被写入与重放查询使用，
没有任何玩家面读取它——药浴结束（或被后续结算清理）后玩家无法回看自己开过什么药浴、
消耗了哪些药材。本模块补齐这段个人历史。

只读；缺库、缺表、缺列、坏 JSON 或读失败返回空态，不建表、不补列。
时间窗只用下界（`created_at` 由写入方提供，UTC 与本地混用，加上界会丢弃「刚刚发生」的行）。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

DAYS = 7
RECENT = 10
EMPTY_TEXT = "近7天没有药浴记录。"
MISSING_TEXT = "缺少玩家身份，无法查询药浴记录。"

_RECEIPT_COLUMNS = frozenset({"operation_id", "user_id", "result_json", "created_at"})


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0.0


def _stamp(value: Any) -> str:
    text = str(value or "").strip()
    return text[:19] if text else ""


def _load(raw: Any) -> Any:
    if not isinstance(raw, (str, bytes, bytearray)):
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _columns(uow: DatabaseUnitOfWork, table: str) -> frozenset[str]:
    rows = uow.query_all(f'PRAGMA table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def _consumed(raw: Any) -> list[dict[str, Any]]:
    """Normalize a payload's consumption list; dirty entries are skipped."""
    items: list[dict[str, Any]] = []
    if not isinstance(raw, list):
        return items
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        amount = _int(entry.get("amount"))
        if not name or amount <= 0:
            continue
        items.append({"name": name, "amount": amount})
    return items


def _decode(row: Any) -> dict[str, Any] | None:
    payload = _load(row["request_json"])
    result = _load(row["result_json"])
    if not isinstance(result, dict):
        return None
    consumed = _consumed(result.get("consumed"))
    if not consumed and isinstance(payload, dict):
        consumed = _consumed(payload.get("plan"))
    if not consumed:
        # A receipt without a readable consumption list carries no player value.
        return None
    effect = _float(result.get("effect"))
    if effect <= 0 and isinstance(payload, dict):
        effect = _float(payload.get("effect"))
    return {
        "bath_name": str(result.get("bath_name") or "").strip(),
        "consumed": consumed,
        "units": sum(item["amount"] for item in consumed),
        "effect": effect,
        "end_time": _stamp(result.get("end_time")),
        "created_at": _stamp(row["created_at"]),
    }


class TiantiBathHistoryApplication:
    """近 N 天本人的炼体药浴记录，只读。"""

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
            "total_units": 0,
            "total_kinds": 0,
            "best_effect": 0.0,
            "recent": [],
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty(MISSING_TEXT)
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not _columns(uow, "tianti_medicine_bath_operations").issuperset(
                    _RECEIPT_COLUMNS
                ):
                    return self._empty()
                rows = uow.query_all(
                    "SELECT operation_id, user_id, request_json, result_json, created_at "
                    "FROM tianti_medicine_bath_operations "
                    "WHERE user_id = ? AND created_at >= ? "
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
        kinds: set[str] = set()
        for run in runs:
            kinds.update(entry["name"] for entry in run["consumed"])
        return {
            "ok": True,
            "notice": None,
            "total": len(runs),
            "total_units": sum(run["units"] for run in runs),
            "total_kinds": len(kinds),
            "best_effect": max(run["effect"] for run in runs),
            "recent": runs[:RECENT],
        }


def _effect_text(effect: float) -> str:
    text = f"{effect * 100:.2f}".rstrip("0").rstrip(".")
    return f"{text}%"


def _plan_text(consumed: list[dict[str, Any]], limit: int = 4) -> str:
    names = [f"{entry['name']}×{entry['amount']}" for entry in consumed]
    if len(names) > limit:
        return "、".join(names[:limit]) + f"等{len(names)}种"
    return "、".join(names)


def format_tianti_bath_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    total = _int(summary.get("total"))
    if total <= 0:
        return EMPTY_TEXT
    lines = [
        f"近{DAYS}天药浴记录",
        f"开启次数：{total} 次",
        f"累计消耗药材：{_int(summary.get('total_units'))} 份（{_int(summary.get('total_kinds'))} 种）",
        f"最高结算效果：{_effect_text(_float(summary.get('best_effect')))}",
    ]
    recent = summary.get("recent") or []
    if recent:
        lines.append("最近：")
        for item in recent:
            name = str(item.get("bath_name") or "").strip() or "炼体药浴"
            tail = (
                f"效果 {_effect_text(_float(item.get('effect')))}"
                f" · 有效至 {item.get('end_time') or '未知'}"
            )
            lines.append(
                f"- {item.get('created_at') or '时间未知'} {name}"
                f"\n  消耗：{_plan_text(item.get('consumed') or [])}"
                f"（共{_int(item.get('units'))}份）"
                f"\n  {tail}"
            )
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "MISSING_TEXT",
    "RECENT",
    "TiantiBathHistoryApplication",
    "format_tianti_bath_history",
]
