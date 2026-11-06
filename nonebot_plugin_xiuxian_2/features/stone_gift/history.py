"""玩家自己的灵石赠送收支（只读）。

`stone_gift_operations` 在每次成功赠送时由真实写路径落行（发送方、接收方、
总额、净额、手续费、时间）。此前玩家只能发起 `送灵石`，看不到自己送出/收到过
哪些灵石；本模块给出近 7 天的只读聚合。

时间窗**只用下界**：`created_at` 由 SQLite `CURRENT_TIMESTAMP`（UTC）写入，
但历史行可能来自不同时区写法，加上界会把「刚刚发生」的行误判为未来而丢弃。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork
from .schema import OPERATIONS_COLUMNS, table_ready

DAYS = 7
TOP_N = 3
RECENT = 5
EMPTY_TEXT = "近7天没有赠送记录。"


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


class StoneGiftHistoryApplication:
    """近 N 天赠送收支，只读。"""

    def __init__(self, database: str | Path, *, clock: Any | None = None) -> None:
        self.database = str(database)
        self._clock = clock

    def _now(self):
        return self._clock.now() if self._clock is not None else datetime.now()

    @staticmethod
    def _empty(notice: str = EMPTY_TEXT) -> dict[str, Any]:
        return {
            "ok": False,
            "notice": notice,
            "sent_count": 0,
            "sent_gross": 0,
            "sent_fee": 0,
            "sent_net": 0,
            "received_count": 0,
            "received_net": 0,
            "top_sent": [],
            "top_received": [],
            "recent": [],
            "names": {},
        }

    @staticmethod
    def _top(rows: list[Any]) -> list[dict[str, Any]]:
        totals: dict[str, dict[str, Any]] = {}
        for row in rows:
            other = str(row["other"])
            entry = totals.setdefault(other, {"user_id": other, "amount": 0, "count": 0})
            entry["amount"] += _int(row["net_amount"])
            entry["count"] += 1
        ordered = sorted(
            totals.values(),
            key=lambda item: (-item["amount"], -item["count"], item["user_id"]),
        )
        return ordered[:TOP_N]

    @staticmethod
    def _names(uow: DatabaseUnitOfWork, user_ids: set[str]) -> dict[str, str]:
        if not user_ids:
            return {}
        placeholders = ",".join("?" for _ in user_ids)
        try:
            rows = uow.query_all(
                f"SELECT user_id, user_name FROM user_xiuxian WHERE user_id IN ({placeholders})",
                tuple(sorted(user_ids)),
            )
        except Exception:
            return {}
        return {
            str(row["user_id"]): str(row["user_name"])
            for row in rows
            if str(row["user_name"] or "").strip()
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询赠送流水。")
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not table_ready(uow, "stone_gift_operations", OPERATIONS_COLUMNS):
                    return self._empty()
                sent = uow.query_all(
                    "SELECT recipient_id AS other, gross_amount, net_amount, fee_amount, "
                    "created_at FROM stone_gift_operations "
                    "WHERE sender_id = ? AND created_at >= ? "
                    "ORDER BY created_at DESC, rowid DESC",
                    (user_id, since),
                )
                received = uow.query_all(
                    "SELECT sender_id AS other, net_amount, created_at "
                    "FROM stone_gift_operations "
                    "WHERE recipient_id = ? AND created_at >= ? "
                    "ORDER BY created_at DESC, rowid DESC",
                    (user_id, since),
                )
                names = self._names(
                    uow,
                    {str(row["other"]) for row in sent}
                    | {str(row["other"]) for row in received},
                )
        except Exception:
            return self._empty()
        recent: list[dict[str, Any]] = []
        for row in sent:
            recent.append(
                {
                    "direction": "sent",
                    "other": str(row["other"]),
                    "net_amount": _int(row["net_amount"]),
                    "gross_amount": _int(row["gross_amount"]),
                    "fee_amount": _int(row["fee_amount"]),
                    "created_at": str(row["created_at"] or ""),
                }
            )
        for row in received:
            recent.append(
                {
                    "direction": "received",
                    "other": str(row["other"]),
                    "net_amount": _int(row["net_amount"]),
                    "gross_amount": _int(row["net_amount"]),
                    "fee_amount": 0,
                    "created_at": str(row["created_at"] or ""),
                }
            )
        recent.sort(
            key=lambda item: (item["created_at"], 1 if item["direction"] == "sent" else 0),
            reverse=True,
        )
        return {
            "ok": True,
            "notice": None,
            "sent_count": len(sent),
            "sent_gross": sum(_int(row["gross_amount"]) for row in sent),
            "sent_fee": sum(_int(row["fee_amount"]) for row in sent),
            "sent_net": sum(_int(row["net_amount"]) for row in sent),
            "received_count": len(received),
            "received_net": sum(_int(row["net_amount"]) for row in received),
            "top_sent": self._top(sent),
            "top_received": self._top(received),
            "recent": recent[:RECENT],
            "names": names,
        }


def _label(user_id: str, names: dict[str, str]) -> str:
    return names.get(user_id) or user_id


def _top_line(title: str, rows: list[dict[str, Any]], names: dict[str, str]) -> str | None:
    if not rows:
        return None
    parts = [
        f"{_label(str(row['user_id']), names)} {_int(row['amount'])}灵石/{_int(row['count'])}笔"
        for row in rows
    ]
    return f"{title}：" + "、".join(parts)


def format_stone_gift_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    if summary["sent_count"] == 0 and summary["received_count"] == 0:
        return EMPTY_TEXT
    names = summary.get("names") or {}
    lines = [
        f"近{DAYS}天赠送流水",
        f"送出：{summary['sent_count']} 笔，共 {summary['sent_gross']} 灵石"
        f"（手续费 {summary['sent_fee']}，实到 {summary['sent_net']}）",
        f"收到：{summary['received_count']} 笔，共 {summary['received_net']} 灵石",
    ]
    sent_line = _top_line("主要送出", summary.get("top_sent") or [], names)
    received_line = _top_line("主要收到", summary.get("top_received") or [], names)
    if sent_line:
        lines.append(sent_line)
    if received_line:
        lines.append(received_line)
    recent = summary.get("recent") or []
    if recent:
        lines.append("最近：")
        for row in recent:
            verb = "送出给" if row["direction"] == "sent" else "收到来自"
            lines.append(
                f"- {verb} {_label(str(row['other']), names)} "
                f"{_int(row['net_amount'])}灵石（{row['created_at']}）"
            )
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "RECENT",
    "TOP_N",
    "StoneGiftHistoryApplication",
    "format_stone_gift_history",
]
