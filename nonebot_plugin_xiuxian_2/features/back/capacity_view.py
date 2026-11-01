"""背包容量体检（只读）。

背包有硬上限 `XiuConfig().max_goods_num`，而各玩法在**写入前**就按「单件物品件数」判定：

    SELECT COALESCE(goods_num,0) FROM back WHERE user_id=? AND goods_id=?
    当前件数 + 本次发放 > max_goods_num  →  inventory_full

见 `features/tasks/claim_repository.py`（任务领奖）、
`features/dongfu/harvest_repository.py`（洞府收获）、
`features/dongfu/patrol_repository.py`（巡逻）、
`features/dongfu/infiltrate_success_repository.py`（潜入）、
`features/combat_settlement/repository.py`（战斗结算）、
`features/pet/repository.py`（宠物出行）。

但 `我的背包` 只按分页列物品，不显示上限、也不显示哪件快满：玩家往往是在领奖被拒之后
才知道装不下了。本模块按**同一口径**（单件物品件数，而非总件数、也非条目数）给出容量体检：
最接近上限的几件、剩余可容纳件数、以及达到警示线的条目。

只读 `back` 表，不建表、不补列、不写库，也不改任何 `inventory_full` 判定。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

TABLE = "back"
WARN_RATIO = 0.8
TOP_N = 3


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


class BackpackCapacityApplication:
    """按单件物品上限给出背包容量与风险条目（只读）。"""

    def __init__(self, game_database: str | Path, max_goods_num: int) -> None:
        self.game_database = str(game_database)
        self.max_goods_num = max(1, _int(max_goods_num))

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
            "limit": self.max_goods_num,
            "entries": 0,
            "total_items": 0,
            "top": [],
            "warnings": [],
            "has_warning": False,
            "rule": "上限按单件物品件数判定（与领奖/收获/结算前的检查一致）",
        }

    def capacity(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法检查背包容量。")

        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                if not self._table_exists(uow, TABLE):
                    return self._empty("暂无背包数据：背包表尚未创建。")
                columns = self._columns(uow, TABLE)
                if not {"user_id", "goods_num"} <= columns:
                    return self._empty("暂无背包数据：背包表结构不完整。")
                name_sql = "goods_name" if "goods_name" in columns else "''"
                id_sql = "goods_id" if "goods_id" in columns else "0"
                rows = uow.query_all(
                    f'SELECT {id_sql} AS goods_id,{name_sql} AS goods_name,'
                    'COALESCE(goods_num,0) AS goods_num FROM "back" '
                    "WHERE user_id=? AND COALESCE(goods_num,0)>0 "
                    "ORDER BY COALESCE(goods_num,0) DESC,goods_name ASC",
                    (user_id,),
                )
        except Exception:
            return self._empty("暂时无法读取背包数据，请稍后再试。")

        payload = self._empty()
        payload["entries"] = len(rows)
        if not rows:
            payload["notice"] = "背包空空如也，暂时不用担心容量。"
            return payload

        items = [
            {
                "goods_id": _int(row["goods_id"]),
                "name": str(row["goods_name"] or "").strip() or f"物品#{_int(row['goods_id'])}",
                "count": _int(row["goods_num"]),
            }
            for row in rows
        ]
        payload["total_items"] = sum(item["count"] for item in items)

        limit = self.max_goods_num
        for item in items:
            item["remaining"] = max(0, limit - item["count"])
            item["ratio"] = round(item["count"] / limit, 4)

        payload["top"] = items[:TOP_N]
        payload["warnings"] = [item for item in items if item["ratio"] >= WARN_RATIO]
        payload["has_warning"] = bool(payload["warnings"])
        if payload["has_warning"]:
            worst = payload["warnings"][0]
            payload["notice"] = (
                f"「{worst['name']}」已占 {worst['count']}/{limit}，"
                f"该物品再获得最多 {worst['remaining']} 件就会被判定背包已满。"
            )
        return payload


def format_backpack_capacity(payload: dict[str, Any], *, format_number=None) -> str:
    """渲染背包容量体检。纯函数：无 I/O、不依赖 NoneBot。"""
    notice = str(payload.get("notice") or "").strip()
    if not payload.get("ok"):
        return notice or "暂时无法检查背包容量。"

    fmt = format_number or (lambda value: f"{int(value):,}")
    limit = int(payload.get("limit") or 0)
    entries = int(payload.get("entries") or 0)
    total = int(payload.get("total_items") or 0)

    lines = ["【我的背包容量】"]
    if entries == 0:
        lines.append(f"上限：单件 {fmt(limit)} 件")
        lines.append(notice or "背包空空如也，暂时不用担心容量。")
        return "\n".join(lines)

    lines.append(f"上限：单件 {fmt(limit)} 件（{payload.get('rule', '')}）")
    lines.append(f"当前：{fmt(entries)} 种物品，合计 {fmt(total)} 件")

    top = list(payload.get("top") or [])
    if top:
        lines.append("")
        lines.append("最接近上限：")
        for item in top:
            lines.append(
                f"· {item.get('name')}：{fmt(item.get('count') or 0)}/{fmt(limit)}"
                f"（还剩 {fmt(item.get('remaining') or 0)} 件，占 "
                f"{round(float(item.get('ratio') or 0) * 100)}%）"
            )

    warnings = list(payload.get("warnings") or [])
    if warnings:
        lines.append("")
        lines.append(f"注意：{len(warnings)} 种物品已达 {round(WARN_RATIO * 100)}% 以上")
        for item in warnings[:TOP_N]:
            lines.append(f"· {item.get('name')}（{fmt(item.get('count') or 0)} 件）")

    if notice and not warnings:
        lines.append("")
        lines.append(notice)
    return "\n".join(lines)


__all__ = ["BackpackCapacityApplication", "WARN_RATIO", "format_backpack_capacity"]
