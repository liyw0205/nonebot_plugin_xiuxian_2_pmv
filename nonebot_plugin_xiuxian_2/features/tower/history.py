"""玩家自己的通天塔爬塔记录（只读）。

`tower_settlement_operations` 在每次爬塔结算时由真实写路径落行：
`payload` 是 ``[user_id, floor, stamina_cost]``，`result_json` 保存本次积分、灵石、
修为、是否战斗胜利与掉落列表。`通天塔信息` 只展示当前层数、历史最高和累计积分，
玩家看不到自己最近爬了哪些层、成功还是失败、拿到了什么；本模块补齐这段个人历史。

`result_json` 没有 status 字段，但被拒绝的结算会把 floor 写成 0，因此查询与解析
都要求 ``floor > 0``，只保留真实结算行。时间窗只用下界，理由与其它个人流水一致。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

DAYS = 7
RECENT = 10
EMPTY_TEXT = "近7天没有通天塔挑战记录。"

_RECEIPT_COLUMNS = frozenset({"payload", "result_json", "created_at"})


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _load(raw: Any) -> Any:
    if not isinstance(raw, (str, bytes, bytearray)):
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _stamp(value: Any) -> str:
    text = str(value or "").strip()
    return text[:19] if text else ""


def _columns(uow: DatabaseUnitOfWork, table: str) -> frozenset[str]:
    rows = uow.query_all(f'PRAGMA table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


class TowerHistoryApplication:
    """近 N 天本人的通天塔结算记录，只读。"""

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
            "successes": 0,
            "failures": 0,
            "best_floor": 0,
            "total_score": 0,
            "total_stone": 0,
            "total_exp": 0,
            "recent": [],
        }

    @staticmethod
    def _decode(row: Any) -> dict[str, Any] | None:
        payload = _load(row["payload"])
        result = _load(row["result_json"])
        if not isinstance(payload, list) or len(payload) < 2 or not isinstance(result, dict):
            return None
        floor = _int(payload[1])
        if floor <= 0:
            return None
        rewards: list[dict[str, Any]] = []
        raw_rewards = result.get("rewards")
        if isinstance(raw_rewards, list):
            for item in raw_rewards:
                if isinstance(item, (list, tuple)) and len(item) >= 4:
                    amount = _int(item[3])
                    if amount > 0:
                        rewards.append(
                            {
                                "id": str(item[0]),
                                "name": str(item[1]),
                                "type": str(item[2]),
                                "amount": amount,
                            }
                        )
        stamina = result.get("stamina_cost")
        if stamina is None and len(payload) > 2:
            stamina = payload[2]
        return {
            "floor": floor,
            "challenge_succeeded": bool(result.get("challenge_succeeded", True)),
            "score": _int(result.get("score")),
            "stone": _int(result.get("stone")),
            "exp": _int(result.get("exp")),
            "stamina_cost": _int(stamina),
            "rewards": rewards,
            "created_at": _stamp(row["created_at"]),
        }

    def _rows(self, uow: DatabaseUnitOfWork, user_id: str, since: str) -> list[Any]:
        try:
            return uow.query_all(
                "SELECT payload, result_json, created_at "
                "FROM tower_settlement_operations "
                "WHERE json_extract(payload, '$[0]') = ? "
                "AND CAST(json_extract(payload, '$[1]') AS INTEGER) > 0 "
                "AND created_at >= ? "
                "ORDER BY created_at DESC, rowid DESC",
                (user_id, since),
            )
        except Exception:
            # Very old SQLite builds may not expose JSON1; fall back to a
            # bounded scan and filter from the parsed payload.
            rows = uow.query_all(
                "SELECT payload, result_json, created_at "
                "FROM tower_settlement_operations "
                "WHERE created_at >= ? "
                "ORDER BY created_at DESC, rowid DESC LIMIT 2000",
                (since,),
            )
            filtered = []
            for row in rows:
                payload = _load(row["payload"])
                if (
                    isinstance(payload, list)
                    and len(payload) >= 2
                    and str(payload[0]) == user_id
                    and _int(payload[1]) > 0
                ):
                    filtered.append(row)
            return filtered

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询通天塔记录。")
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not _columns(uow, "tower_settlement_operations").issuperset(
                    _RECEIPT_COLUMNS
                ):
                    return self._empty()
                rows = self._rows(uow, user_id, since)
        except Exception:
            return self._empty()

        runs: list[dict[str, Any]] = []
        for row in rows:
            item = self._decode(row)
            if item is not None:
                runs.append(item)
        if not runs:
            return self._empty()
        return {
            "ok": True,
            "notice": None,
            "total": len(runs),
            "successes": sum(1 for item in runs if item["challenge_succeeded"]),
            "failures": sum(1 for item in runs if not item["challenge_succeeded"]),
            "best_floor": max(item["floor"] for item in runs),
            "total_score": sum(item["score"] for item in runs),
            "total_stone": sum(item["stone"] for item in runs),
            "total_exp": sum(item["exp"] for item in runs),
            "recent": runs[:RECENT],
        }


def _reward_text(rewards: list[dict[str, Any]]) -> str:
    if not rewards:
        return ""
    parts = [f"{item['name']}×{item['amount']}" for item in rewards[:3]]
    if len(rewards) > 3:
        parts.append(f"等{len(rewards)}种")
    return "，奖励 " + "、".join(parts)


def format_tower_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    total = _int(summary.get("total"))
    if total <= 0:
        return EMPTY_TEXT
    lines = [
        f"近{DAYS}天通天塔记录",
        f"挑战：{total} 次（成功 {_int(summary.get('successes'))} / "
        f"失败 {_int(summary.get('failures'))}）",
        f"最高层：{_int(summary.get('best_floor'))} 层",
        f"累计：积分 {_int(summary.get('total_score'))} · "
        f"灵石 {_int(summary.get('total_stone'))} · "
        f"修为 {_int(summary.get('total_exp'))}",
    ]
    recent = summary.get("recent") or []
    if recent:
        lines.append("最近：")
        for item in recent:
            result = "成功" if item["challenge_succeeded"] else "失败"
            detail = (
                f"+{_int(item['score'])}积分"
                if item["challenge_succeeded"]
                else f"消耗体力 {_int(item['stamina_cost'])}"
            )
            lines.append(
                f"- {item['created_at'] or '时间未知'} "
                f"第{_int(item['floor'])}层 {result} {detail}"
                f"{_reward_text(item.get('rewards') or [])}"
            )
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "RECENT",
    "TowerHistoryApplication",
    "format_tower_history",
]
