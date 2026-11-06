"""玩家自己的历练事件记录（只读）。

`training_event_operations` 在每次历练事件结算时由真实写路径落行：
`payload` 是规范列表 ``[user_id, expected_state, state, expected_user, stone_delta,
exp_delta, hp_delta, rewards, max_goods_num]``，`result_json` 保存
``{"status": "applied", "message": 事件文本}``。`历练状态` 只展示当前进度，玩家看不到
最近触发过哪些事件、资源怎么变化、拿到了什么；本模块补齐这段个人历史。

只读；缺库、缺表、缺列、坏 JSON 或读失败返回空态，不建表、不补列。时间窗只用下界。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ...infrastructure.database import DatabaseUnitOfWork

DAYS = 7
RECENT = 10
EMPTY_TEXT = "近7天没有历练记录。"

_RECEIPT_COLUMNS = frozenset({"payload", "result_json", "created_at"})


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
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


def _columns(uow: DatabaseUnitOfWork, table: str) -> frozenset[str]:
    rows = uow.query_all(f'PRAGMA table_info("{table}")')
    return frozenset(str(row["name"]).casefold() for row in rows)


def _decode(row: Any) -> dict[str, Any] | None:
    payload = _load(row["payload"])
    result = _load(row["result_json"])
    if not isinstance(payload, list) or len(payload) < 8 or not isinstance(result, dict):
        return None
    if str(result.get("status") or "") != "applied":
        return None
    raw_rewards = payload[7]
    rewards: list[dict[str, Any]] = []
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
    return {
        "message": str(result.get("message") or "").strip(),
        "stone": _int(payload[4]),
        "exp": _int(payload[5]),
        "hp": _int(payload[6]),
        "rewards": rewards,
        "created_at": _stamp(row["created_at"]),
    }


class TrainingEventHistoryApplication:
    """近 N 天本人的历练事件记录，只读。"""

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
            "total_stone": 0,
            "total_exp": 0,
            "total_hp": 0,
            "total_items": 0,
            "recent": [],
        }

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询历练记录。")
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not _columns(uow, "training_event_operations").issuperset(
                    _RECEIPT_COLUMNS
                ):
                    return self._empty()
                rows = uow.query_all(
                    "SELECT payload, result_json, created_at "
                    "FROM training_event_operations "
                    "WHERE json_valid(payload) "
                    "AND json_extract(payload, '$[0]') = ? AND created_at >= ? "
                    "ORDER BY created_at DESC, rowid DESC",
                    (user_id, since),
                )
        except Exception:
            return self._empty()

        runs: list[dict[str, Any]] = []
        for row in rows:
            item = _decode(row)
            if item is not None:
                runs.append(item)
        if not runs:
            return self._empty()
        return {
            "ok": True,
            "notice": None,
            "total": len(runs),
            "total_stone": sum(item["stone"] for item in runs),
            "total_exp": sum(item["exp"] for item in runs),
            "total_hp": sum(item["hp"] for item in runs),
            "total_items": sum(
                reward["amount"] for item in runs for reward in item["rewards"]
            ),
            "recent": runs[:RECENT],
        }


def _delta_text(value: int) -> str:
    return f"+{value}" if value > 0 else str(value)


def _reward_text(rewards: list[dict[str, Any]]) -> str:
    if not rewards:
        return ""
    return "；奖励 " + "、".join(
        f"{item['name']}×{_int(item['amount'])}" for item in rewards[:3]
    ) + ("等" if len(rewards) > 3 else "")


def _one_line(text: str, limit: int = 100) -> str:
    compact = " / ".join(part.strip() for part in text.splitlines() if part.strip())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1] + "…"


def format_training_event_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    total = _int(summary.get("total"))
    if total <= 0:
        return EMPTY_TEXT
    lines = [
        f"近{DAYS}天历练记录",
        f"事件：{total} 次",
        f"累计变化：灵石 {_delta_text(_int(summary.get('total_stone')))} · "
        f"修为 {_delta_text(_int(summary.get('total_exp')))} · "
        f"气血 {_delta_text(_int(summary.get('total_hp')))}",
    ]
    recent = summary.get("recent") or []
    if recent:
        lines.append("最近：")
        for item in recent:
            message = _one_line(str(item.get("message") or "")) or "历练事件"
            changes = (
                f"灵石 {_delta_text(_int(item['stone']))} · "
                f"修为 {_delta_text(_int(item['exp']))} · "
                f"气血 {_delta_text(_int(item['hp']))}"
            )
            lines.append(
                f"- {item['created_at'] or '时间未知'} {message}"
                f"（{changes}{_reward_text(item.get('rewards') or [])}）"
            )
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "RECENT",
    "TrainingEventHistoryApplication",
    "format_training_event_history",
]
