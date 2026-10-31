"""玩家自己的签到累计（只读）。

每次 `修仙签到` 成功，`SignInApplicationEffects` 都会把统计写进投影：

- `features/sign_in/application_effects.py:54`
  `statistics.record(user_id=..., operation_id=f"statistics:{operation_id}",
  event_key="修仙签到", occurred_at=now)`
- 写入方 `SignInStatisticsRepository` 由 `plugin.py` 以 game_db 装配

于是 `sign_in_statistics_events` 逐条累积、`sign_in_statistics_projection.value`
逐次加一。但玩家侧从没有任何命令读它——`SignInStatisticsRepository.value()`
本就是为这个视图准备的读接口，却只被测试和写入侧使用。

本模块补上玩家视图：累计签到次数、本月签到天数、最近签到时间。
累计次数与 `SignInStatisticsRepository.value()` 用同一 SQL、同一 event_key，
测试用同一夹具断言两者一致，保证不会与写入侧口径分歧。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork

# 与写入侧完全一致的投影键，不猜测其他键名。
EVENT_KEY = "修仙签到"


class SignInStatisticsViewApplication:
    """累计签到、本月签到天数、最近签到时间（只读）。"""

    def __init__(self, game_database: str | Path, *, clock=None) -> None:
        self.game_database = str(game_database)
        self._clock = clock

    def _now(self) -> datetime:
        return self._clock.now() if self._clock is not None else datetime.now()

    @staticmethod
    def _table_exists(uow: DatabaseUnitOfWork, table: str) -> bool:
        return (
            uow.query_one(
                "SELECT 1 AS present FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            is not None
        )

    @staticmethod
    def _columns(uow: DatabaseUnitOfWork, table: str) -> set[str]:
        return {str(row["name"]) for row in uow.query_all(f'PRAGMA table_info("{table}")')}

    def _empty(self, notice: str | None = None) -> dict[str, Any]:
        return {
            "ok": notice is None,
            "notice": notice,
            "total": None,
            "month_days": None,
            "last_at": None,
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询签到统计。")
        month_prefix = self._now().strftime("%Y-%m")
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                payload = self._empty()
                has_projection = self._table_exists(uow, "sign_in_statistics_projection")
                has_events = self._table_exists(uow, "sign_in_statistics_events")
                if not has_projection and not has_events:
                    return self._empty("暂无签到统计数据。")

                if has_projection:
                    columns = self._columns(uow, "sign_in_statistics_projection")
                    if {"user_id", "event_key", "value"} <= columns:
                        row = uow.query_one(
                            "SELECT value FROM sign_in_statistics_projection "
                            "WHERE user_id=? AND event_key=?",
                            (user_id, EVENT_KEY),
                        )
                        if row is not None:
                            payload["total"] = int(row["value"] or 0)

                if has_events:
                    columns = self._columns(uow, "sign_in_statistics_events")
                    if {"user_id", "event_key", "created_at"} <= columns:
                        stats = uow.query_one(
                            "SELECT COUNT(*) AS rows_count,"
                            "COUNT(DISTINCT substr(created_at,1,7)||'-'||"
                            "substr(created_at,9,2)) AS month_days,"
                            "MAX(created_at) AS last_at "
                            "FROM sign_in_statistics_events "
                            "WHERE user_id=? AND event_key=? AND "
                            "substr(created_at,1,7)=?",
                            (user_id, EVENT_KEY, month_prefix),
                        )
                        if stats is not None:
                            payload["month_days"] = int(stats["month_days"] or 0)
                        last = uow.query_one(
                            "SELECT MAX(created_at) AS last_at FROM sign_in_statistics_events "
                            "WHERE user_id=? AND event_key=?",
                            (user_id, EVENT_KEY),
                        )
                        if last is not None and last["last_at"]:
                            payload["last_at"] = str(last["last_at"])
                        if payload["total"] is None:
                            # 没有投影行时用事件行数兜底，避免显示 0 次却有时间。
                            counted = uow.query_one(
                                "SELECT COUNT(*) AS total FROM sign_in_statistics_events "
                                "WHERE user_id=? AND event_key=?",
                                (user_id, EVENT_KEY),
                            )
                            payload["total"] = int((counted or {}).get("total") or 0)
                return payload
        except Exception:
            # 只读视图绝不向玩家抛异常。
            return self._empty("暂时无法读取签到统计，请稍后再试。")


def _format_time(raw: Any) -> str:
    text = str(raw or "").strip()
    if len(text) >= 16 and text[4] == "-" and text[7] == "-":
        return text[5:16]
    return text or "未知时间"


def format_sign_in_statistics(payload: Mapping[str, Any], *, format_number=None) -> str:
    """渲染签到统计。纯函数：无 I/O、无 NoneBot。"""
    fmt = format_number or (lambda value: f"{int(value):,}")
    if not payload.get("ok"):
        return str(payload.get("notice") or "暂无签到统计数据。")

    total = payload.get("total")
    month_days = payload.get("month_days")
    last_at = payload.get("last_at")
    if not total:
        return "【我的签到统计】\n还没有签到记录，发送【修仙签到】开始吧。"

    lines = ["【我的签到统计】", f"累计签到：{fmt(int(total))} 次"]
    if month_days is not None:
        lines.append(f"本月签到：{int(month_days)} 天")
    if last_at:
        lines.append(f"最近签到：{_format_time(last_at)}")
    return "\n".join(lines)


__all__ = ["EVENT_KEY", "SignInStatisticsViewApplication", "format_sign_in_statistics"]
