"""玩家自己的竞技场挑战战绩（只读）。

`arena_challenge_settlement_operations` 在每次挑战结算时由真实写路径落行：
`payload` 保存挑战者/对手/结果/挑战时间，`result_json` 保存结算状态、积分变化与
结算后积分段位。`我的竞技场` 只展示累计场次与胜率，`我的荣誉` 只展示荣誉结算，
玩家看不到自己每一场挑战的结果；本模块补齐这段个人历史。

只统计 `result_json.status == "applied"` 的真实结算行；被拒绝的请求（次数用尽、
体力不足、状态冲突等）同样会落回执，但不会出现在战绩里。时间窗只用下界，
理由与其它个人流水一致：`created_at` 由 SQLite 以 UTC 写入，历史行可能混用
时区写法，加上界会误丢「刚刚发生」的记录。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from ...infrastructure.database import DatabaseUnitOfWork
from .schema import table_ready

DAYS = 7
RECENT = 5
MAX_ROWS = 200
EMPTY_TEXT = "近7天没有竞技场挑战记录。"

_RECEIPT_COLUMNS = frozenset({"challenger_id", "payload", "result_json"})
_OUTCOMES = {"win", "loss", "draw", "no_match"}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _load(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, (str, bytes, bytearray)):
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _stamp(value: Any) -> str:
    text = str(value or "").strip()
    return text[:19] if text else ""


class ArenaMatchHistoryApplication:
    """近 N 天本人作为挑战者的竞技场战绩，只读。"""

    def __init__(
        self,
        database: str | Path,
        *,
        clock: Any | None = None,
        name_resolver: Callable[[str], str | None] | None = None,
    ) -> None:
        self.database = str(database)
        self._clock = clock
        self._name_resolver = name_resolver

    def _now(self) -> datetime:
        return self._clock.now() if self._clock is not None else datetime.now()

    @staticmethod
    def _empty(notice: str = EMPTY_TEXT) -> dict[str, Any]:
        return {
            "ok": False,
            "notice": notice,
            "total": 0,
            "wins": 0,
            "losses": 0,
            "draws": 0,
            "no_match": 0,
            "net_delta": 0,
            "recent": [],
            "names": {},
        }

    def _names(self, user_ids: set[str]) -> dict[str, str]:
        if not user_ids or self._name_resolver is None:
            return {}
        names: dict[str, str] = {}
        for user_id in sorted(user_ids):
            try:
                name = self._name_resolver(user_id)
            except Exception:
                continue
            text = str(name or "").strip()
            if text:
                names[user_id] = text
        return names

    def summary(self, user_id: str) -> dict[str, Any]:
        user_id = str(user_id or "").strip()
        if not user_id:
            return self._empty("缺少玩家身份，无法查询竞技场战绩。")
        since = (self._now() - timedelta(days=DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with DatabaseUnitOfWork(self.database, read_only=True) as uow:
                if not table_ready(
                    uow, "arena_challenge_settlement_operations", _RECEIPT_COLUMNS
                ):
                    return self._empty()
                rows = uow.query_all(
                    "SELECT payload, result_json, created_at "
                    "FROM arena_challenge_settlement_operations "
                    "WHERE challenger_id = ? AND created_at >= ? "
                    "ORDER BY created_at DESC, rowid DESC LIMIT ?",
                    (user_id, since, MAX_ROWS),
                )
        except Exception:
            return self._empty()

        matches: list[dict[str, Any]] = []
        for row in rows:
            payload = _load(row["payload"])
            result = _load(row["result_json"])
            if payload is None or result is None:
                continue
            status = str(result.get("status") or "").strip()
            if status and status != "applied":
                continue
            outcome = str(payload.get("outcome") or result.get("outcome") or "").strip()
            if outcome not in _OUTCOMES:
                continue
            opponent = payload.get("opponent_id")
            matches.append(
                {
                    "challenged_at": _stamp(
                        payload.get("challenged_at")
                        or result.get("challenged_at")
                        or row["created_at"]
                    ),
                    "outcome": outcome,
                    "opponent_id": "" if opponent in (None, "") else str(opponent),
                    "score_delta": _int(result.get("score_delta")),
                    "challenger_score": _int(result.get("challenger_score")),
                    "challenger_rank": str(result.get("challenger_rank") or ""),
                }
            )

        if not matches:
            return self._empty()
        recent = matches[:RECENT]
        return {
            "ok": True,
            "notice": None,
            "total": len(matches),
            "wins": sum(1 for item in matches if item["outcome"] == "win"),
            "losses": sum(1 for item in matches if item["outcome"] == "loss"),
            "draws": sum(1 for item in matches if item["outcome"] == "draw"),
            "no_match": sum(1 for item in matches if item["outcome"] == "no_match"),
            "net_delta": sum(item["score_delta"] for item in matches),
            "recent": recent,
            "names": self._names(
                {item["opponent_id"] for item in recent if item["opponent_id"]}
            ),
        }


def _delta_text(value: int) -> str:
    return f"+{value}" if value > 0 else str(value)


def _outcome_text(outcome: str) -> str:
    return {
        "win": "胜",
        "loss": "负",
        "draw": "平",
        "no_match": "未匹配",
    }.get(outcome, "未知")


def format_arena_match_history(summary: dict[str, Any]) -> str:
    """纯渲染，无 I/O、无 NoneBot 依赖。"""
    if not summary.get("ok"):
        return str(summary.get("notice") or EMPTY_TEXT)
    total = _int(summary.get("total"))
    if total <= 0:
        return EMPTY_TEXT
    parts = [
        f"胜 {_int(summary.get('wins'))}",
        f"负 {_int(summary.get('losses'))}",
    ]
    draws = _int(summary.get("draws"))
    no_match = _int(summary.get("no_match"))
    if draws:
        parts.append(f"平 {draws}")
    if no_match:
        parts.append(f"未匹配 {no_match}")
    lines = [
        f"近{DAYS}天竞技场战绩",
        f"挑战：{total} 场（{' / '.join(parts)}）",
        f"积分净变化：{_delta_text(_int(summary.get('net_delta')))}",
    ]
    recent = summary.get("recent") or []
    names = summary.get("names") or {}
    if recent:
        lines.append("最近：")
        for item in recent:
            opponent = str(item.get("opponent_id") or "")
            label = names.get(opponent) or opponent or "无匹配"
            score = _int(item.get("challenger_score"))
            rank = str(item.get("challenger_rank") or "").strip()
            suffix = f"（当前 {score} · {rank}）" if rank else f"（当前 {score}）"
            lines.append(
                f"- {item.get('challenged_at') or '时间未知'} "
                f"{_outcome_text(str(item.get('outcome') or ''))} {label} "
                f"{_delta_text(_int(item.get('score_delta')))}{suffix}"
            )
    return "\n".join(lines)


__all__ = [
    "DAYS",
    "EMPTY_TEXT",
    "RECENT",
    "ArenaMatchHistoryApplication",
    "format_arena_match_history",
]
