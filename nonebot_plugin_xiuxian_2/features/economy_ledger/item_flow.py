"""玩家自己的物品收支（只读）。

物品增减已经全量记进共享 `economy_log.item_delta`：背包加减
（`xiuxian2_handle` 的 `item_add`/`item_cost`）、奖励统一发放（`reward_service`）、
宗门商店兑换、仙肆购买。元素形状统一为 `{"id","name","type","amount",...}`，
其中加物品 `amount` 为正、扣物品 `amount` 为负。

但玩家此前看不到这些记录，只能翻 `我的背包` 看当前持有，无法回答
"我今天的丹药/材料是怎么来的、花到哪去了"。本模块给出近 7 天的只读聚合。

时间窗**只用下界**：`economy_log.created_at` 由各写入方提供，UTC 与本地时区混用，
加上界会把「刚刚发生」的本地时间行误判为未来而丢弃（灵石流水切片已实测该陷阱）。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .schemas import ITEM_FLOW_DAYS, ITEM_FLOW_RECENT, ITEM_FLOW_TOP_N

# 视为"没有物品变动"的字面量，与 repository.query_page 的 item_change 口径一致。
_EMPTY_ITEM_DELTA = {"", "[]", "{}", "null", "none"}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _parse_items(raw: Any) -> list[dict[str, Any]]:
    """把一行 `item_delta` 解析成 `[{"name","type","amount"}]`。

    脏数据只让这一行降级，绝不让整个视图失败：非法 JSON / 非 list / 元素非 dict /
    amount 非法 全部跳过。
    """
    text = "" if raw is None else str(raw).strip()
    if text.casefold() in _EMPTY_ITEM_DELTA:
        return []
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        return []
    if isinstance(data, dict):
        # 兼容早期把单个物品直接写成对象的行。
        data = [data]
    if not isinstance(data, list):
        return []
    items: list[dict[str, Any]] = []
    for entry in data:
        if not isinstance(entry, Mapping):
            continue
        amount = _int(entry.get("amount"))
        if amount == 0:
            continue
        name = str(entry.get("name") or "").strip()
        if not name:
            item_id = entry.get("id")
            name = f"物品#{item_id}" if item_id not in (None, "") else "未知物品"
        kind = str(entry.get("type") or "").strip() or "其他"
        items.append({"name": name, "type": kind, "amount": amount})
    return items


class ItemFlowApplication:
    """近 N 天物品收支，只读。"""

    def __init__(self, game_database: str | Path, *, clock=None) -> None:
        self.game_database = str(game_database)
        self._clock = clock

    def _now(self):
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
            "records": 0,
            "gained_total": 0,
            "gained_kinds": 0,
            "spent_total": 0,
            "spent_kinds": 0,
            "top_gained": [],
            "top_spent": [],
            "recent": [],
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询物品流水。")
        since = (self._now() - timedelta(days=ITEM_FLOW_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                if not self._table_exists(uow, "economy_log"):
                    return self._empty("暂无物品流水记录。")
                columns = self._columns(uow, "economy_log")
                if not {"user_id", "item_delta"} <= columns:
                    return self._empty("暂无物品流水记录。")
                time_sql = "created_at" if "created_at" in columns else "''"
                source_sql = "source" if "source" in columns else "''"
                action_sql = "action" if "action" in columns else "''"
                id_sql = "id" if "id" in columns else "0"
                rows = uow.query_all(
                    f'SELECT {id_sql} AS id,{source_sql} AS source,{action_sql} AS action,'
                    f'item_delta,{time_sql} AS created_at FROM "economy_log" '
                    'WHERE user_id=? AND item_delta IS NOT NULL '
                    "AND TRIM(CAST(item_delta AS TEXT))<>'' "
                    "AND LOWER(TRIM(CAST(item_delta AS TEXT))) NOT IN ('[]','{}','null','none') "
                    "AND LOWER(TRIM(CAST(item_delta AS TEXT)))<>'null' "
                    f"AND {time_sql}>=? "
                    f"ORDER BY {time_sql} DESC,{id_sql} DESC",
                    (user_id, since),
                )
        except Exception:
            return self._empty("暂时无法读取物品流水，请稍后再试。")

        gained: dict[str, dict[str, Any]] = {}
        spent: dict[str, dict[str, Any]] = {}
        recent: list[dict[str, Any]] = []
        records = 0
        for row in rows:
            items = _parse_items(row["item_delta"])
            if not items:
                continue
            records += 1
            for item in items:
                bucket = gained if item["amount"] > 0 else spent
                entry = bucket.setdefault(
                    item["name"],
                    {"name": item["name"], "type": item["type"], "amount": 0, "records": 0},
                )
                entry["amount"] += abs(int(item["amount"]))
                entry["records"] += 1
            if len(recent) < ITEM_FLOW_RECENT:
                recent.append(
                    {
                        "created_at": str(row["created_at"] or ""),
                        "source": str(row["source"] or ""),
                        "action": str(row["action"] or ""),
                        "items": items,
                    }
                )

        payload = self._empty()
        payload["records"] = records
        payload["gained_total"] = sum(entry["amount"] for entry in gained.values())
        payload["spent_total"] = sum(entry["amount"] for entry in spent.values())
        payload["gained_kinds"] = len(gained)
        payload["spent_kinds"] = len(spent)
        payload["top_gained"] = sorted(
            gained.values(), key=lambda e: (-e["amount"], -e["records"])
        )[:ITEM_FLOW_TOP_N]
        payload["top_spent"] = sorted(
            spent.values(), key=lambda e: (-e["amount"], -e["records"])
        )[:ITEM_FLOW_TOP_N]
        payload["recent"] = recent
        return payload


# 已知动作 → 玩家可读文案；未知动作回退 `source·action`。
ACTION_LABELS = {
    "donate": "宗门捐献",
    "task_complete": "宗门任务",
    "shop_exchange": "宗门商店兑换",
    "xianshi_buy_item": "仙肆购买",
    "item_add": "物品获得",
    "item_cost": "物品消耗",
}


def _action_label(row: Mapping[str, Any]) -> str:
    action = str(row.get("action") or "").strip()
    label = ACTION_LABELS.get(action)
    if label:
        return label
    source = str(row.get("source") or "").strip()
    if action:
        return f"{source}·{action}" if source else action
    return source or "未知来源"


def _time(row: Mapping[str, Any]) -> str:
    raw = str(row.get("created_at") or "").strip()
    if len(raw) >= 16 and raw[4] == "-" and raw[7] == "-":
        return raw[5:16]
    return raw or "未知时间"


def format_item_flow(payload: Mapping[str, Any], *, format_number=None) -> str:
    """渲染物品流水。纯函数：无 I/O、无 NoneBot。"""
    fmt = format_number or (lambda value: f"{int(value):,}")
    if not payload.get("ok"):
        return str(payload.get("notice") or "暂无物品流水记录。")

    lines = [f"【我的物品流水】近{ITEM_FLOW_DAYS}天"]
    records = int(payload.get("records") or 0)
    if records <= 0:
        lines.append("近7天没有物品收支记录。")
        return "\n".join(lines)

    lines.append(
        f"获得：{fmt(int(payload.get('gained_total') or 0))} 件"
        f"（{int(payload.get('gained_kinds') or 0)} 种）"
    )
    lines.append(
        f"消耗：{fmt(int(payload.get('spent_total') or 0))} 件"
        f"（{int(payload.get('spent_kinds') or 0)} 种）"
    )
    lines.append(f"变动记录：{records} 笔")

    top_gained = list(payload.get("top_gained") or [])
    if top_gained:
        lines.append("")
        lines.append("主要获得：")
        for entry in top_gained:
            lines.append(
                f"· {entry.get('name')} ×{fmt(int(entry.get('amount') or 0))}"
                f"（{int(entry.get('records') or 0)}笔）"
            )

    top_spent = list(payload.get("top_spent") or [])
    if top_spent:
        lines.append("")
        lines.append("主要消耗：")
        for entry in top_spent:
            lines.append(
                f"· {entry.get('name')} ×{fmt(int(entry.get('amount') or 0))}"
                f"（{int(entry.get('records') or 0)}笔）"
            )

    recent = list(payload.get("recent") or [])
    if recent:
        lines.append("")
        lines.append(f"最近 {len(recent)} 笔：")
        for row in recent:
            parts = []
            for item in row.get("items") or []:
                amount = _int(item.get("amount"))
                verb = "获得" if amount > 0 else "失去"
                parts.append(f"{verb} {item.get('name')} ×{fmt(abs(amount))}")
            detail = "、".join(parts) or "无明细"
            lines.append(f"· {_time(row)} {_action_label(row)} {detail}")
    return "\n".join(lines)


__all__ = ["ItemFlowApplication", "format_item_flow"]
