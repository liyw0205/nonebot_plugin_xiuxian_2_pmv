"""玩家自己的修行周报（只读，跨来源汇总近 7 天）。

玩家现有视图都是单一口径：`我的灵石流水`、`我的物品流水`、`我的宗门贡献流水`、
`我的资产总览` 各看一面，`我的签到统计`/`我的突破统计` 是累计计数。
没有任何命令回答「我这周整体练得怎么样」：修为涨了多少、灵石是净赚还是净亏、
哪几类玩法在产生收益、这周活跃了几天。

本模块只读 `economy_log`（修为/灵石/贡献/物品的统一账本），一次查询完成汇总，
不建表、不补列、不写库。

时间窗**只用下界**：`created_at` 由各写入方提供，UTC 与本地时区混用，加上界会把
「刚刚发生」的本地时间行误判为未来而丢弃（灵石流水切片已实测该陷阱）。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .schemas import (
    WEEKLY_REPORT_DAYS,
    WEEKLY_REPORT_TOP_ACTIONS,
    WEEKLY_REPORT_TOP_ITEMS,
)

_EMPTY_ITEM_DELTA = {"", "[]", "{}", "null", "none"}

# 已知动作 → 玩家可读文案；未知动作回退 `source·action`。
ACTION_LABELS = {
    "donate": "宗门捐献",
    "task_complete": "宗门任务",
    "shop_exchange": "宗门商店兑换",
    "xianshi_buy_item": "仙肆购买",
    "item_add": "物品获得",
    "item_cost": "物品消耗",
    "sign_in": "修仙签到",
    "closing": "闭关修炼",
    "training_event": "历练事件",
    "tower_reward": "爬塔奖励",
    "breakthrough": "突破",
    "boss_reward": "世界BOSS",
    "rift_settlement": "秘境探索",
}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _day(value: Any) -> str:
    """取 `created_at` 的日期部分；解析失败返回空串（该行不计入活跃天数）。"""
    text = str(value or "").strip()
    if len(text) >= 10:
        head = text[:10]
        if head[4:5] == "-" and head[7:8] == "-" and head[:4].isdigit():
            return head
    return ""


def _parse_items(raw: Any) -> list[dict[str, Any]]:
    """把一行 `item_delta` 解析成 `[{"name","type","amount"}]`。

    脏数据只让这一行降级，绝不让整个视图失败：非法 JSON / 非 list / 元素非 dict /
    amount 非法全部跳过。
    """
    text = "" if raw is None else str(raw).strip()
    if text.casefold() in _EMPTY_ITEM_DELTA:
        return []
    try:
        data = json.loads(text)
    except (TypeError, ValueError):
        return []
    if isinstance(data, dict):
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
        items.append(
            {
                "name": str(entry.get("name") or f"物品#{entry.get('id') or '?'}"),
                "type": str(entry.get("type") or ""),
                "amount": amount,
            }
        )
    return items


def _action_label(source: Any, action: Any) -> str:
    key = str(action or "").strip()
    label = ACTION_LABELS.get(key)
    if label:
        return label
    src = str(source or "").strip()
    if key:
        return f"{src}·{key}" if src else key
    return src or "未知来源"


class WeeklyReportApplication:
    """近 7 天修行汇总（只读）。"""

    def __init__(self, game_database: Any, now_fn: Any = None) -> None:
        self.game_database = game_database
        self._now = now_fn or datetime.now

    # ---------------------------------------------------------------- helpers
    @staticmethod
    def _table_exists(uow: Any, name: str) -> bool:
        try:
            row = uow.query_one(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)
            )
        except Exception:
            return False
        return bool(row)

    @staticmethod
    def _columns(uow: Any, name: str) -> set[str]:
        try:
            rows = uow.query_all(f'PRAGMA table_info("{name}")')
        except Exception:
            return set()
        return {str(row["name"]) for row in rows}

    def _empty(self, notice: str = "") -> dict[str, Any]:
        return {
            "days": WEEKLY_REPORT_DAYS,
            "start_time": "",
            "notice": notice,
            "records": 0,
            "exp": {"gained": 0, "spent": 0, "net": 0, "records": 0},
            "stone": {"gained": 0, "spent": 0, "net": 0, "records": 0},
            "contribution": {"gained": 0, "spent": 0, "net": 0, "records": 0},
            "items": {"gained_total": 0, "spent_total": 0, "top_gained": [], "top_spent": []},
            "activity": {"active_days": 0, "busiest_day": "", "busiest_records": 0},
            "top_actions": [],
        }

    # --------------------------------------------------------------- main API
    def weekly_report(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法生成修行周报。")

        now = self._now()
        start_time = (now - timedelta(days=WEEKLY_REPORT_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                if not self._table_exists(uow, "economy_log"):
                    return self._empty("暂无修行记录：账本尚未初始化。")
                columns = self._columns(uow, "economy_log")
                if not {"user_id", "created_at"} <= columns:
                    return self._empty("暂无修行记录：账本结构不完整。")

                exp_sql = "exp_delta" if "exp_delta" in columns else "0"
                stone_sql = "stone_delta" if "stone_delta" in columns else "0"
                contribution_sql = (
                    "sect_contribution_delta" if "sect_contribution_delta" in columns else "0"
                )
                item_sql = "item_delta" if "item_delta" in columns else "''"
                action_sql = "action" if "action" in columns else "''"
                source_sql = "source" if "source" in columns else "''"

                rows = uow.query_all(
                    f"SELECT {exp_sql} AS exp_delta,{stone_sql} AS stone_delta,"
                    f"{contribution_sql} AS sect_contribution_delta,"
                    f"{item_sql} AS item_delta,{action_sql} AS action,"
                    f"{source_sql} AS source,created_at FROM \"economy_log\" "
                    "WHERE user_id=? AND created_at>=? "
                    "ORDER BY created_at ASC",
                    (user_id, start_time),
                )
        except Exception:
            return self._empty("暂时无法读取修行账本，请稍后再试。")

        payload = self._empty()
        payload["start_time"] = start_time

        exp = payload["exp"]
        stone = payload["stone"]
        contribution = payload["contribution"]
        items = payload["items"]
        gained_items: dict[str, dict[str, Any]] = {}
        spent_items: dict[str, dict[str, Any]] = {}
        action_counts: dict[str, int] = {}
        day_counts: dict[str, int] = {}

        for row in rows:
            payload["records"] += 1

            exp_delta = _int(row["exp_delta"])
            if exp_delta:
                exp["records"] += 1
                if exp_delta > 0:
                    exp["gained"] += exp_delta
                else:
                    exp["spent"] += -exp_delta

            stone_delta = _int(row["stone_delta"])
            if stone_delta:
                stone["records"] += 1
                if stone_delta > 0:
                    stone["gained"] += stone_delta
                else:
                    stone["spent"] += -stone_delta

            contribution_delta = _int(row["sect_contribution_delta"])
            if contribution_delta:
                contribution["records"] += 1
                if contribution_delta > 0:
                    contribution["gained"] += contribution_delta
                else:
                    contribution["spent"] += -contribution_delta

            for item in _parse_items(row["item_delta"]):
                bucket = gained_items if item["amount"] > 0 else spent_items
                entry = bucket.setdefault(
                    item["name"],
                    {"name": item["name"], "type": item["type"], "amount": 0},
                )
                entry["amount"] += abs(int(item["amount"]))

            label = _action_label(row["source"], row["action"])
            action_counts[label] = action_counts.get(label, 0) + 1

            day = _day(row["created_at"])
            if day:
                day_counts[day] = day_counts.get(day, 0) + 1

        exp["net"] = exp["gained"] - exp["spent"]
        stone["net"] = stone["gained"] - stone["spent"]
        contribution["net"] = contribution["gained"] - contribution["spent"]

        items["gained_total"] = sum(e["amount"] for e in gained_items.values())
        items["spent_total"] = sum(e["amount"] for e in spent_items.values())
        items["top_gained"] = sorted(
            gained_items.values(), key=lambda e: (-e["amount"], e["name"])
        )[:WEEKLY_REPORT_TOP_ITEMS]
        items["top_spent"] = sorted(
            spent_items.values(), key=lambda e: (-e["amount"], e["name"])
        )[:WEEKLY_REPORT_TOP_ITEMS]

        payload["activity"]["active_days"] = len(day_counts)
        if day_counts:
            # 记录数最多的一天；并列取较早那天（排序键第二项即日期字符串）。
            busiest = sorted(day_counts.items(), key=lambda kv: (-kv[1], kv[0]))[0]
            payload["activity"]["busiest_day"] = busiest[0]
            payload["activity"]["busiest_records"] = busiest[1]

        payload["top_actions"] = [
            {"label": label, "records": count}
            for label, count in sorted(
                action_counts.items(), key=lambda kv: (-kv[1], kv[0])
            )[:WEEKLY_REPORT_TOP_ACTIONS]
        ]

        if not rows:
            payload["notice"] = "本周暂无修行记录。"
        return payload


__all__ = ["ACTION_LABELS", "WeeklyReportApplication", "format_weekly_report"]


def format_weekly_report(payload: Mapping[str, Any], *, format_number=None) -> str:
    """渲染修行周报。纯函数：无 I/O、不依赖 NoneBot。"""
    notice = str(payload.get("notice") or "").strip()
    records = int(payload.get("records") or 0)
    if records <= 0:
        return notice or "本周暂无修行记录。"

    fmt = format_number or (lambda value: f"{int(value):,}")
    days = int(payload.get("days") or WEEKLY_REPORT_DAYS)
    lines = [f"【我的修行周报】近{days}天"]

    def delta_line(title: str, block: Mapping[str, Any], unit: str) -> str:
        gained = int(block.get("gained") or 0)
        spent = int(block.get("spent") or 0)
        net = int(block.get("net") or 0)
        return (
            f"{title}：+{fmt(gained)} / -{fmt(spent)}，"
            f"净{'+' if net >= 0 else '-'}{fmt(abs(net))}{unit}"
            f"（{int(block.get('records') or 0)}笔）"
        )

    exp = payload.get("exp") or {}
    stone = payload.get("stone") or {}
    contribution = payload.get("contribution") or {}
    if int(exp.get("records") or 0):
        lines.append(delta_line("修为", exp, ""))
    if int(stone.get("records") or 0):
        lines.append(delta_line("灵石", stone, ""))
    if int(contribution.get("records") or 0):
        lines.append(delta_line("宗门贡献", contribution, ""))

    items = payload.get("items") or {}
    gained_total = int(items.get("gained_total") or 0)
    spent_total = int(items.get("spent_total") or 0)
    if gained_total or spent_total:
        lines.append(f"物品：获得 {fmt(gained_total)} 件 / 消耗 {fmt(spent_total)} 件")
        tops = list(items.get("top_gained") or [])
        if tops:
            detail = "、".join(
                f"{row.get('name', '')}×{fmt(int(row.get('amount') or 0))}" for row in tops
            )
            lines.append(f"· 主要获得：{detail}")
        tops = list(items.get("top_spent") or [])
        if tops:
            detail = "、".join(
                f"{row.get('name', '')}×{fmt(int(row.get('amount') or 0))}" for row in tops
            )
            lines.append(f"· 主要消耗：{detail}")

    activity = payload.get("activity") or {}
    active_days = int(activity.get("active_days") or 0)
    if active_days:
        line = f"活跃：{active_days} 天，共 {records} 笔记录"
        busiest = str(activity.get("busiest_day") or "").strip()
        if busiest:
            line += f"，最活跃 {busiest}（{int(activity.get('busiest_records') or 0)} 笔）"
        lines.append(line)

    actions = list(payload.get("top_actions") or [])
    if actions:
        detail = "、".join(
            f"{row.get('label', '')}（{int(row.get('records') or 0)}笔）" for row in actions
        )
        lines.append(f"主要玩法：{detail}")

    if notice:
        lines.append("")
        lines.append(notice)
    return "\n".join(lines)
