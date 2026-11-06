"""玩家自己的宗门任务结算记录（只读）。

`sect_task_settlement_operations` 在每次宗门任务成功提交时由 `SectApplication.settle_task`
落行：任务周期、消耗类型/数量、修为/宗门贡献/宗门资材奖励与完成时间。玩家侧只能看到
当前接取的任务与一次完成提示，无法回看自己最近做过哪些任务、付出了什么、拿到了什么。
本模块补齐这段个人历史。

只查询 `user_id = 当前会话` 的已结算行；请求路径不建表、不补列，缺 schema 时降级为空态。
时间窗只用下界，理由与其它个人流水一致。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

DAYS = 7
RECENT = 10
EMPTY_TEXT = "近7天没有宗门任务完成记录。"

_RECEIPT_COLUMNS = frozenset(
    {
        "user_id",
        "period",
        "cost_type",
        "cost",
        "exp_reward",
        "sect_reward",
        "materials_reward",
        "created_at",
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


class SectTaskHistoryApplication:
    """近 N 天本人的宗门任务结算记录，只读。"""

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
            "total_exp": 0,
            "total_sect": 0,
            "total_materials": 0,
            "recent": [],
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询宗门任务记录。")
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not _columns(
                    uow, "sect_task_settlement_operations"
                ).issuperset(_RECEIPT_COLUMNS):
                    return self._empty()
                rows = uow.query_all(
                    "SELECT period,cost_type,cost,exp_reward,sect_reward,"
                    "materials_reward,created_at "
                    "FROM sect_task_settlement_operations "
                    "WHERE user_id = ? AND created_at >= ? "
                    "ORDER BY created_at DESC, rowid DESC",
                    (user_id, since),
                )
        except Exception:
            return self._empty()

        runs = [
            {
                "period": str(row["period"] or ""),
                "cost_type": str(row["cost_type"] or ""),
                "cost": _int(row["cost"]),
                "exp_reward": _int(row["exp_reward"]),
                "sect_reward": _int(row["sect_reward"]),
                "materials_reward": _int(row["materials_reward"]),
                "created_at": _stamp(row["created_at"]),
            }
            for row in rows
        ]
        if not runs:
            return self._empty()
        return {
            "ok": True,
            "notice": None,
            "total": len(runs),
            "total_exp": sum(item["exp_reward"] for item in runs),
            "total_sect": sum(item["sect_reward"] for item in runs),
            "total_materials": sum(item["materials_reward"] for item in runs),
            "recent": runs[:RECENT],
        }


def _cost_text(cost_type: str, cost: int) -> str:
    label = {"hp": "气血", "stone": "灵石"}.get(cost_type, "资源")
    return f"消耗{label} {cost}"


def format_sect_task_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    total = _int(summary.get("total"))
    if total <= 0:
        return EMPTY_TEXT
    lines = [
        f"近{DAYS}天宗门任务记录",
        f"完成：{total} 次",
        f"累计奖励：修为 {_int(summary.get('total_exp'))} · "
        f"宗门贡献 {_int(summary.get('total_sect'))} · "
        f"宗门资材 {_int(summary.get('total_materials'))}",
    ]
    recent = summary.get("recent") or []
    if recent:
        lines.append("最近：")
        for item in recent:
            lines.append(
                f"- {item['created_at'] or '时间未知'} "
                f"{_cost_text(str(item['cost_type']), _int(item['cost']))} → "
                f"修为 {_int(item['exp_reward'])} · "
                f"贡献 {_int(item['sect_reward'])} · "
                f"资材 {_int(item['materials_reward'])}"
            )
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "RECENT",
    "SectTaskHistoryApplication",
    "format_sect_task_history",
]
