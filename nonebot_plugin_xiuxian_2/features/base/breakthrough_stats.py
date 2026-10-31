"""玩家的突破派生统计（只读）。

突破是玩家最在意的成长节点，四个原始计数一直在 `player_data.statistics` 里累积
（该表实有 59 列，含 `突破次数`/`突破成功`/`突破失败`/`突破损失修为`）。但玩家侧
唯一的入口是 `修仙数据`，而它只是把 55 个字段原样裸罗列
（`xiuxian_base/__init__.py:1824`），没有成功率、没有累计损失的汇总——玩家要自己
在 55 行里挑 4 个数字做除法。

本模块把四个计数变成一个可读结论：次数、成功、失败、成功率、累计损失修为。
只读 `statistics` 单行，不建表、不补列、不改任何写路径。

口径上刻意**不臆造**：成功率的分母永远是 `突破次数`；若
`突破成功 + 突破失败 != 突破次数`（历史脏数据或旧版本只记了部分计数），
输出会追加一行如实说明，而不是悄悄改用"成功+失败"当分母。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork

TOTAL_FIELD = "突破次数"
SUCCESS_FIELD = "突破成功"
FAILURE_FIELD = "突破失败"
EXP_LOST_FIELD = "突破损失修为"
FIELDS = (TOTAL_FIELD, SUCCESS_FIELD, FAILURE_FIELD, EXP_LOST_FIELD)


def _int(value: Any) -> int:
    """把统计值转成非负整数：负数与非法值一律按 0。"""
    try:
        number = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


class BreakthroughStatsApplication:
    """突破次数/成功/失败/成功率/累计损失修为（只读）。"""

    def __init__(self, player_database: str | Path, *, clock=None) -> None:
        self.player_database = str(player_database)
        self._clock = clock

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
            "success": None,
            "failure": None,
            "exp_lost": None,
            "rate": None,
            "inconsistent": False,
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询突破统计。")
        try:
            with DatabaseUnitOfWork(self.player_database, read_only=True) as uow:
                if not self._table_exists(uow, "statistics"):
                    return self._empty("暂无突破统计数据。")
                columns = self._columns(uow, "statistics")
                if "user_id" not in columns:
                    return self._empty("暂无突破统计数据。")
                present = [field for field in FIELDS if field in columns]
                if not present:
                    return self._empty("暂无突破统计数据。")
                selected = ",".join(
                    f'"{field}"' if field in columns else f"NULL AS \"{field}\""
                    for field in FIELDS
                )
                row = uow.query_one(
                    f'SELECT {selected} FROM statistics WHERE user_id=?', (user_id,)
                )
                if row is None:
                    return self._empty()

                total = _int(row[TOTAL_FIELD])
                success = _int(row[SUCCESS_FIELD])
                failure = _int(row[FAILURE_FIELD])
                exp_lost = _int(row[EXP_LOST_FIELD])

                payload = self._empty()
                payload["total"] = total
                payload["success"] = success
                payload["failure"] = failure
                payload["exp_lost"] = exp_lost
                if total > 0:
                    payload["rate"] = round(success * 100.0 / total, 1)
                    payload["inconsistent"] = success + failure != total
                return payload
        except Exception:
            # 只读视图绝不向玩家抛异常。
            return self._empty("暂时无法读取突破统计，请稍后再试。")


def format_breakthrough_stats(payload: Mapping[str, Any], *, format_number=None) -> str:
    """渲染突破统计。纯函数：无 I/O、无 NoneBot。"""
    fmt = format_number or (lambda value: f"{int(value):,}")
    if not payload.get("ok"):
        return str(payload.get("notice") or "暂无突破统计数据。")

    total = int(payload.get("total") or 0)
    if total <= 0:
        return "【我的突破统计】\n还没有突破记录，发送【突破】试试。"

    success = int(payload.get("success") or 0)
    failure = int(payload.get("failure") or 0)
    exp_lost = int(payload.get("exp_lost") or 0)
    rate = payload.get("rate")

    lines = [
        "【我的突破统计】",
        f"突破次数：{fmt(total)} 次",
        f"成功：{fmt(success)} 次",
        f"失败：{fmt(failure)} 次",
    ]
    if rate is not None:
        lines.append(f"成功率：{float(rate):.1f}%")
    lines.append(f"累计损失修为：{fmt(exp_lost)}")
    if payload.get("inconsistent"):
        lines.append("（成功+失败 与 突破次数 不一致，成功率按突破次数计）")
    return "\n".join(lines)


__all__ = ["BreakthroughStatsApplication", "format_breakthrough_stats"]
