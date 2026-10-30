"""玩家自己的宗门贡献收支（只读）。

宗门贡献是玩家实际付出灵石换来的个人额度：`宗门捐献` 加、`宗门商店兑换` 减、
宗门设施升级等也走同一列。`economy_log.sect_contribution_delta` 由这些真实写路径
持续记录（`xiuxian/xiuxian_sect/__init__.py` 的 `donate` / `shop_exchange` /
`fairyland_upgrade` / `elixir_room_upgrade`），但玩家此前只能看到
`user_xiuxian.sect_contribution` 这一个当前余额，看不到"最近怎么变的"。

本模块给出一条只读聚合视图：近 7 天获得/消耗/净额、主要来源与去向各前 3、
最近 5 笔明细，以及当前贡献余额与所属宗门。

时间窗**只用下界**：`economy_log.created_at` 由各写入方提供，UTC 与本地时区混用，
加上界会把「刚刚发生」的本地时间行误判为未来而丢弃（灵石流水切片已实测过该陷阱）。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.database import DatabaseUnitOfWork
from .schemas import (
    CONTRIBUTION_FLOW_DAYS,
    CONTRIBUTION_FLOW_RECENT,
    CONTRIBUTION_FLOW_TOP_N,
)

FIELD = "sect_contribution_delta"

# 已知的宗门贡献动作 → 玩家可读文案；未知动作回退为 `source·action`。
ACTION_LABELS = {
    "donate": "宗门捐献",
    "task_complete": "宗门任务",
    "shop_exchange": "宗门商店兑换",
    "fairyland_upgrade": "宗门洞天升级",
    "elixir_room_upgrade": "宗门炼丹房升级",
}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


class ContributionFlowApplication:
    """近 N 天宗门贡献收支，只读。"""

    def __init__(self, game_database: str | Path, *, clock=None) -> None:
        self.game_database = str(game_database)
        self._clock = clock

    # -- helpers ---------------------------------------------------------
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
            "balance": None,
            "sect_name": None,
            "gained": 0,
            "spent": 0,
            "net": 0,
            "records": 0,
            "sources": [],
            "uses": [],
            "recent": [],
        }

    def _balance(self, uow: DatabaseUnitOfWork, user_id: str) -> tuple[int | None, str | None]:
        if not self._table_exists(uow, "user_xiuxian"):
            return None, None
        columns = self._columns(uow, "user_xiuxian")
        if "user_id" not in columns:
            return None, None
        balance_sql = (
            '"sect_contribution"' if "sect_contribution" in columns else "NULL"
        )
        row = uow.query_one(
            f"SELECT {balance_sql} AS balance,sect_id FROM user_xiuxian WHERE user_id=?",
            (user_id,),
        ) if "sect_id" in columns else uow.query_one(
            f"SELECT {balance_sql} AS balance,NULL AS sect_id FROM user_xiuxian WHERE user_id=?",
            (user_id,),
        )
        if row is None:
            return None, None
        balance = _int(row["balance"]) if "sect_contribution" in columns else None
        sect_id = row["sect_id"]
        sect_name = None
        if sect_id not in (None, "", 0) and self._table_exists(uow, "sects"):
            if {"sect_id", "sect_name"} <= self._columns(uow, "sects"):
                sect = uow.query_one(
                    "SELECT sect_name FROM sects WHERE sect_id=?", (_int(sect_id),)
                )
                if sect is not None and str(sect["sect_name"] or "").strip():
                    sect_name = str(sect["sect_name"]).strip()
        return balance, sect_name

    # -- public ----------------------------------------------------------
    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询宗门贡献流水。")
        since = (self._now() - timedelta(days=CONTRIBUTION_FLOW_DAYS)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        try:
            with DatabaseUnitOfWork(self.game_database, read_only=True) as uow:
                if not self._table_exists(uow, "economy_log"):
                    return self._empty("暂无宗门贡献流水记录。")
                columns = self._columns(uow, "economy_log")
                if not {"user_id", FIELD} <= columns:
                    # 旧库没有该列：没有可聚合的数据，但不报错。
                    balance, sect_name = self._balance(uow, user_id)
                    payload = self._empty("暂无宗门贡献流水记录。")
                    payload["balance"], payload["sect_name"] = balance, sect_name
                    return payload

                balance, sect_name = self._balance(uow, user_id)
                where = f'WHERE user_id=? AND created_at>=?'
                params = (user_id, since)
                totals = uow.query_one(
                    f'SELECT COUNT(*) AS records,'
                    f'COALESCE(SUM(CASE WHEN "{FIELD}">0 THEN "{FIELD}" ELSE 0 END),0) AS gained,'
                    f'COALESCE(SUM(CASE WHEN "{FIELD}"<0 THEN -"{FIELD}" ELSE 0 END),0) AS spent,'
                    f'COALESCE(SUM("{FIELD}"),0) AS net '
                    f'FROM "economy_log" {where} AND COALESCE("{FIELD}",0)<>0',
                    params,
                )
                payload = self._empty()
                payload["balance"], payload["sect_name"] = balance, sect_name
                payload["records"] = _int((totals or {}).get("records"))
                payload["gained"] = _int((totals or {}).get("gained"))
                payload["spent"] = _int((totals or {}).get("spent"))
                payload["net"] = _int((totals or {}).get("net"))
                if payload["records"] <= 0:
                    return payload

                groups = uow.query_all(
                    f'SELECT COALESCE(source,\'\') AS source,COALESCE(action,\'\') AS action,'
                    f'COUNT(*) AS records,'
                    f'COALESCE(SUM(CASE WHEN "{FIELD}">0 THEN "{FIELD}" ELSE 0 END),0) AS gained,'
                    f'COALESCE(SUM(CASE WHEN "{FIELD}"<0 THEN -"{FIELD}" ELSE 0 END),0) AS spent '
                    f'FROM "economy_log" {where} AND COALESCE("{FIELD}",0)<>0 '
                    f'GROUP BY COALESCE(source,\'\'),COALESCE(action,\'\')',
                    params,
                )
                payload["sources"] = sorted(
                    (dict(row) for row in groups if _int(row["gained"]) > 0),
                    key=lambda row: (-_int(row["gained"]), -_int(row["records"])),
                )[:CONTRIBUTION_FLOW_TOP_N]
                payload["uses"] = sorted(
                    (dict(row) for row in groups if _int(row["spent"]) > 0),
                    key=lambda row: (-_int(row["spent"]), -_int(row["records"])),
                )[:CONTRIBUTION_FLOW_TOP_N]

                recent = uow.query_all(
                    f'SELECT source,action,"{FIELD}" AS delta,created_at,detail '
                    f'FROM "economy_log" {where} AND COALESCE("{FIELD}",0)<>0 '
                    f"ORDER BY created_at DESC,id DESC LIMIT ?",
                    (*params, CONTRIBUTION_FLOW_RECENT),
                )
                payload["recent"] = [dict(row) for row in recent]
                return payload
        except Exception:
            # 只读视图绝不向玩家抛异常；库不可读时按"暂无记录"降级。
            return self._empty("暂时无法读取宗门贡献流水，请稍后再试。")


def _label(row: Mapping[str, Any]) -> str:
    source = str(row.get("source") or "").strip()
    action = str(row.get("action") or "").strip()
    label = ACTION_LABELS.get(action)
    if label:
        return label
    if action:
        return f"{source}·{action}" if source else action
    return source or "未知来源"


def _time(row: Mapping[str, Any]) -> str:
    raw = str(row.get("created_at") or "").strip()
    if len(raw) >= 16 and raw[4] == "-" and raw[7] == "-":
        return raw[5:16]
    return raw or "未知时间"


def format_contribution_flow(payload: Mapping[str, Any], *, format_number=None) -> str:
    """渲染宗门贡献流水。纯函数：无 I/O、无 NoneBot。"""
    fmt = format_number or (lambda value: f"{int(value):,}")
    if not payload.get("ok") and not payload.get("records"):
        base = str(payload.get("notice") or "暂无宗门贡献流水记录。")
        balance = payload.get("balance")
        if balance is not None:
            base += f"\n当前宗门贡献：{fmt(int(balance))}"
        return base

    balance = payload.get("balance")
    sect_name = payload.get("sect_name")
    header = f"【我的宗门贡献流水】近{CONTRIBUTION_FLOW_DAYS}天"
    lines = [header]
    if sect_name:
        lines.append(
            f"宗门：{sect_name}"
            + (f"（贡献 {fmt(int(balance))}）" if balance is not None else "")
        )
    elif balance is not None:
        lines.append(f"当前宗门贡献：{fmt(int(balance))}")

    if int(payload.get("records") or 0) <= 0:
        lines.append("近7天没有宗门贡献收支记录。")
        return "\n".join(lines)

    gained = int(payload.get("gained") or 0)
    spent = int(payload.get("spent") or 0)
    net = int(payload.get("net") or 0)
    lines.append(f"获得：+{fmt(gained)}")
    lines.append(f"消耗：-{fmt(spent)}")
    lines.append(f"净变化：{'+' if net >= 0 else '-'}{fmt(abs(net))}")
    lines.append(f"记录：{int(payload.get('records') or 0)} 笔")

    sources = list(payload.get("sources") or [])
    if gained > 0 and sources:
        lines.append("")
        lines.append("主要来源：")
        for row in sources:
            lines.append(
                f"· {_label(row)} +{fmt(_int(row.get('gained')))}"
                f"（{_int(row.get('records'))}笔）"
            )

    uses = list(payload.get("uses") or [])
    if spent > 0 and uses:
        lines.append("")
        lines.append("主要去向：")
        for row in uses:
            lines.append(
                f"· {_label(row)} -{fmt(_int(row.get('spent')))}"
                f"（{_int(row.get('records'))}笔）"
            )

    recent = list(payload.get("recent") or [])
    if recent:
        lines.append("")
        lines.append(f"最近 {len(recent)} 笔：")
        for row in recent:
            delta = _int(row.get("delta"))
            lines.append(
                f"· {_time(row)} {_label(row)} "
                f"{'+' if delta >= 0 else '-'}{fmt(abs(delta))}"
            )
    return "\n".join(lines)


__all__ = ["ContributionFlowApplication", "format_contribution_flow"]
