"""玩家自己的炼体成长记录（只读）。

三张炼体回执行记录着玩家的每次成长，但此前只被写入与幂等重放查询使用，没有任何玩家
可见面——玩家错过一次性回执后，无法回看突破成败、冲窍开出了哪个窍穴、灵石炼体花了多少：

- `tianti_breakthrough_operations`（player DB）：`old_level`/`new_level`/`hp_cost`/`success`。
- `tianti_qiaoxue_operations`（player DB）：`qiaoxue_json`/`hp_cost`/`opened_count`/`unlock_limit`。
- `tianti_stone_training_operations`（game DB）：`requested_stone`/`stone_cost`/`hp_gain`。

只读；表/列缺失、坏 JSON 或读失败只让对应段消失，不建表、不补列。
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
EMPTY_TEXT = "近7天没有炼体记录。"
MISSING_TEXT = "缺少玩家身份，无法查询炼体记录。"

BREAKTHROUGH_TABLE = "tianti_breakthrough_operations"
QIAOXUE_TABLE = "tianti_qiaoxue_operations"
STONE_TABLE = "tianti_stone_training_operations"

_QUERY = "SELECT {columns} FROM {table} WHERE user_id = ? AND created_at >= ? ORDER BY created_at DESC, rowid DESC"


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


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


def _columns(uow: DatabaseUnitOfWork, schema: str, table: str) -> frozenset[str]:
    rows = uow.query_all(f'PRAGMA {schema}.table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def _has_table(uow: DatabaseUnitOfWork, schema: str, table: str) -> bool:
    row = uow.query_one(
        f"SELECT 1 AS present FROM {schema}.sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return row is not None


def _rows(
    uow: DatabaseUnitOfWork,
    schema: str,
    table: str,
    columns: tuple[str, ...],
    user_id: str,
    since: str,
) -> list[Any]:
    """Read one receipt table defensively; a missing table/column yields no rows."""
    if not _has_table(uow, schema, table):
        return []
    available = _columns(uow, schema, table)
    if not {"user_id", "created_at"}.issubset(available):
        return []
    selected = [name for name in columns if name in available]
    if not {name for name in columns}.issubset(available):
        # A half-migrated receipt table cannot be read as a whole; skip the section.
        return []
    return uow.query_all(
        _QUERY.format(columns=", ".join(selected), table=f"{schema}.{table}"),
        (user_id, since),
    )


class TiantiProgressHistoryApplication:
    """近 N 天本人的炼体突破/冲窍/灵石炼体记录，只读。"""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        clock: Any | None = None,
    ) -> None:
        self.game_database = str(game_database)
        self.player_database = str(player_database)
        self._clock = clock

    def _now(self) -> datetime:
        return self._clock.now() if self._clock is not None else datetime.now()

    @staticmethod
    def _empty(notice: str = EMPTY_TEXT) -> dict[str, Any]:
        return {
            "ok": False,
            "notice": notice,
            "breakthrough": None,
            "qiaoxue": None,
            "stone": None,
        }

    # -- sections ---------------------------------------------------------
    @staticmethod
    def _breakthrough(uow: DatabaseUnitOfWork, user_id: str, since: str) -> dict[str, Any] | None:
        rows = _rows(
            uow,
            "main",
            BREAKTHROUGH_TABLE,
            ("old_level", "new_level", "hp_cost", "success", "created_at"),
            user_id,
            since,
        )
        if not rows:
            return None
        recent = [
            {
                "old_level": str(row["old_level"] or ""),
                "new_level": str(row["new_level"] or ""),
                "hp_cost": _int(row["hp_cost"]),
                "success": bool(_int(row["success"])),
                "created_at": _stamp(row["created_at"]),
            }
            for row in rows
        ]
        return {
            "total": len(recent),
            "success": sum(1 for item in recent if item["success"]),
            "failed": sum(1 for item in recent if not item["success"]),
            "hp_cost": sum(item["hp_cost"] for item in recent),
            "recent": recent[:RECENT],
        }

    @staticmethod
    def _qiaoxue(uow: DatabaseUnitOfWork, user_id: str, since: str) -> dict[str, Any] | None:
        rows = _rows(
            uow,
            "main",
            QIAOXUE_TABLE,
            ("qiaoxue_json", "hp_cost", "opened_count", "created_at"),
            user_id,
            since,
        )
        if not rows:
            return None
        recent: list[dict[str, Any]] = []
        for row in rows:
            detail = _load(row["qiaoxue_json"])
            if not isinstance(detail, dict):
                detail = {}
            recent.append(
                {
                    "name": str(detail.get("name") or ""),
                    "group": str(detail.get("group") or ""),
                    "hp_cost": _int(row["hp_cost"]),
                    "opened_count": _int(row["opened_count"]),
                    "created_at": _stamp(row["created_at"]),
                }
            )
        return {
            "total": len(recent),
            "hp_cost": sum(item["hp_cost"] for item in recent),
            "opened_count": max((item["opened_count"] for item in recent), default=0),
            "recent": recent[:RECENT],
        }

    @staticmethod
    def _stone(uow: DatabaseUnitOfWork, user_id: str, since: str) -> dict[str, Any] | None:
        rows = _rows(
            uow,
            "main",
            STONE_TABLE,
            ("requested_stone", "stone_cost", "hp_gain", "created_at"),
            user_id,
            since,
        )
        if not rows:
            return None
        recent = [
            {
                "requested_stone": _int(row["requested_stone"]),
                "stone_cost": _int(row["stone_cost"]),
                "hp_gain": _int(row["hp_gain"]),
                "created_at": _stamp(row["created_at"]),
            }
            for row in rows
        ]
        return {
            "total": len(recent),
            "stone_cost": sum(item["stone_cost"] for item in recent),
            "hp_gain": sum(item["hp_gain"] for item in recent),
            "recent": recent[:RECENT],
        }

    # -- entry point ------------------------------------------------------
    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty(MISSING_TEXT)
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        payload = self._empty()
        # 突破与冲窍回执在玩家库；灵石炼体回执在主库并附玩家库。
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                payload["breakthrough"] = self._breakthrough(uow, user_id, since)
                payload["qiaoxue"] = self._qiaoxue(uow, user_id, since)
        except Exception:
            pass
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                try:
                    uow.attach_database(self.player_database, "player_data")
                except Exception:
                    pass
                payload["stone"] = self._stone(uow, user_id, since)
        except Exception:
            pass
        payload["ok"] = any(payload[key] is not None for key in ("breakthrough", "qiaoxue", "stone"))
        payload["notice"] = None if payload["ok"] else EMPTY_TEXT
        return payload


def _one_line(text: str, limit: int = 90) -> str:
    compact = " / ".join(part.strip() for part in str(text or "").splitlines() if part.strip())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1] + "…"


def format_tianti_progress_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    lines = [f"近{DAYS}天炼体记录"]
    breakthrough = summary.get("breakthrough")
    if breakthrough:
        lines.append(
            f"突破：{_int(breakthrough.get('total'))} 次"
            f"（成功 {_int(breakthrough.get('success'))} · 失败 {_int(breakthrough.get('failed'))}）"
            f"，累计消耗气血 {_int(breakthrough.get('hp_cost'))}"
        )
        for item in breakthrough.get("recent") or []:
            outcome = "成功" if item.get("success") else "失败"
            lines.append(
                f"- {item.get('created_at') or '时间未知'} 突破{outcome}"
                f"（{item.get('old_level') or '?'} → {item.get('new_level') or '?'}"
                f"，消耗气血 {_int(item.get('hp_cost'))}）"
            )
    qiaoxue = summary.get("qiaoxue")
    if qiaoxue:
        lines.append(
            f"冲窍：{_int(qiaoxue.get('total'))} 次，累计消耗气血 {_int(qiaoxue.get('hp_cost'))}"
            f"，已开窍 {_int(qiaoxue.get('opened_count'))}/108"
        )
        for item in qiaoxue.get("recent") or []:
            lines.append(
                f"- {item.get('created_at') or '时间未知'} 开启【{item.get('name') or '?'}】"
                f"（{item.get('group') or '未知组'}，消耗气血 {_int(item.get('hp_cost'))}）"
            )
    stone = summary.get("stone")
    if stone:
        lines.append(
            f"灵石炼体：{_int(stone.get('total'))} 次，"
            f"累计消耗灵石 {_int(stone.get('stone_cost'))}，累计获得气血 {_int(stone.get('hp_gain'))}"
        )
        for item in stone.get("recent") or []:
            lines.append(
                f"- {item.get('created_at') or '时间未知'} 灵石炼体"
                f"（消耗灵石 {_int(item.get('stone_cost'))}，获得气血 {_int(item.get('hp_gain'))}）"
            )
    if len(lines) == 1:
        return EMPTY_TEXT
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "MISSING_TEXT",
    "RECENT",
    "TiantiProgressHistoryApplication",
    "format_tianti_progress_history",
]
