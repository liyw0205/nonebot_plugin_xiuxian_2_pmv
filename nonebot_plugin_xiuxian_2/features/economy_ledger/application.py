from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from .repository import EconomyLedgerSqlRepository
from .schemas import (
    DEFAULT_ANOMALY_STONE_DELTA,
    DEFAULT_PAGE_SIZE,
    FILTER_FIELDS,
    MAX_PAGE_SIZE,
    QUICK_PRESETS,
    STONE_FLOW_DAYS,
    STONE_FLOW_RECENT,
    STONE_FLOW_TOP_N,
    TIME_FILTER_FIELDS,
)


def _parse_positive_int(raw_value: Any, default: int, min_value: int = 1, max_value: int | None = None) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    value = max(value, min_value)
    return min(value, max_value) if max_value is not None else value


def _parse_non_negative_int(raw_value: Any, default: int = 0, max_value: int | None = None) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    value = max(value, 0)
    return min(value, max_value) if max_value is not None else value


def _parse_page_args(args: Mapping[str, Any]) -> tuple[int, int]:
    page = _parse_positive_int(args.get("page", 1), 1)
    page_size = _parse_positive_int(
        args.get("page_size", args.get("limit", DEFAULT_PAGE_SIZE)),
        DEFAULT_PAGE_SIZE,
        1,
        MAX_PAGE_SIZE,
    )
    return page, page_size


class EconomyLedgerApplication:
    """Normalize admin query arguments for the shared ledger read model."""

    def __init__(
        self,
        database: str | Path,
        repository: EconomyLedgerSqlRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or EconomyLedgerSqlRepository(database)
        self.clock = clock or SystemClock()

    def _filters(self, args: Mapping[str, Any]) -> dict[str, str]:
        filters: dict[str, str] = {}
        for field in (*FILTER_FIELDS, *TIME_FILTER_FIELDS):
            value = str(args.get(field, "")).strip()
            if value:
                filters[field] = value[:100] if field in FILTER_FIELDS else value[:32]

        preset = str(args.get("preset", "")).strip()
        if preset in QUICK_PRESETS:
            filters["preset"] = preset
            if not filters.get("start_time") and not filters.get("end_time"):
                now = self.clock.now()
                _, days_back = QUICK_PRESETS[preset]
                start = (now - timedelta(days=days_back)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                filters["start_time"] = start.strftime("%Y-%m-%d %H:%M:%S")
                filters["end_time"] = now.strftime("%Y-%m-%d %H:%M:%S")

        min_abs_stone_delta = _parse_non_negative_int(args.get("min_abs_stone_delta"), 0)
        if min_abs_stone_delta > 0:
            filters["min_abs_stone_delta"] = str(min_abs_stone_delta)

        if str(args.get("anomaly_stone_delta", "")).strip():
            filters["anomaly_stone_delta"] = str(
                _parse_non_negative_int(
                    args.get("anomaly_stone_delta"), DEFAULT_ANOMALY_STONE_DELTA
                )
            )
        if str(args.get("has_item_delta", "")).strip() in {"1", "true", "on"}:
            filters["has_item_delta"] = "1"
        if str(args.get("anomaly_only", "")).strip() in {"1", "true", "on"}:
            filters["anomaly_only"] = "1"
        return filters

    def query_page(self, args: Mapping[str, Any]) -> dict[str, Any]:
        page, page_size = _parse_page_args(args)
        result = self.repository.query_page(self._filters(args), page, page_size)
        query_args = self._build_query_args(
            result["filters"], result["page_size"], result["page"]
        )
        result.update(
            {
                "query_args": query_args,
                "first_page_args": self._build_query_args(
                    result["filters"], result["page_size"], 1
                ),
                "prev_page_args": self._build_query_args(
                    result["filters"], result["page_size"], max(result["page"] - 1, 1)
                ),
                "next_page_args": self._build_query_args(
                    result["filters"],
                    result["page_size"],
                    min(result["page"] + 1, result["total_pages"]),
                ),
                "last_page_args": self._build_query_args(
                    result["filters"], result["page_size"], result["total_pages"]
                ),
                "export_args": self._build_query_args(
                    result["filters"], result["page_size"], result["page"]
                ),
            }
        )
        return result

    @staticmethod
    def _build_query_args(
        filters: Mapping[str, str], page_size: int, page: int | None = None
    ) -> dict[str, Any]:
        args = {field: value for field, value in filters.items() if value}
        args["page_size"] = page_size
        if page is not None:
            args["page"] = page
        return args

    def iter_export_rows(self, args: Mapping[str, Any]):
        return self.repository.iter_export_rows(self._filters(args))

    # ---- 玩家个人灵石流水（只读） --------------------------------------
    def stone_flow_summary(self, user_id: str) -> dict[str, Any]:
        """Return one player's own stone flow for the last 7 days.

        Reuses :meth:`query_page` (same repository, same SQL) so the player view
        and the administrative ledger can never disagree.  The caller's
        ``user_id`` is always injected here; it is never taken from command
        arguments.
        """
        user_id = str(user_id or "").strip()
        if not user_id:
            return {
                "ok": False,
                "notice": "缺少玩家身份，无法查询灵石流水。",
                "summary": {},
                "income": [],
                "expense": [],
                "recent": [],
            }
        page = self.query_page(
            {
                "user_id": user_id,
                # 只用下界：economy_log 的时间戳由各写入方提供，UTC 与本地时区混用，
                # 加上界会把「刚刚发生」的本地时间行误判为未来而丢弃。
                "start_time": (
                    self.clock.now() - timedelta(days=STONE_FLOW_DAYS)
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "page_size": STONE_FLOW_RECENT,
            }
        )
        if page.get("error"):
            return {
                "ok": False,
                "notice": str(page["error"]),
                "summary": {},
                "income": [],
                "expense": [],
                "recent": [],
            }
        if page.get("notice"):
            return {
                "ok": False,
                "notice": str(page["notice"]),
                "summary": {},
                "income": [],
                "expense": [],
                "recent": [],
            }

        summary = dict(page.get("summary") or {})
        sources = list(page.get("source_stats") or [])
        income = sorted(
            (row for row in sources if int(row.get("stone_in") or 0) > 0),
            key=lambda row: (-int(row.get("stone_in") or 0), -int(row.get("records") or 0)),
        )[:STONE_FLOW_TOP_N]
        expense = sorted(
            (row for row in sources if int(row.get("stone_out") or 0) > 0),
            key=lambda row: (-int(row.get("stone_out") or 0), -int(row.get("records") or 0)),
        )[:STONE_FLOW_TOP_N]
        recent = [row for row in (page.get("rows") or []) if int(row.get("stone_delta") or 0) != 0]
        return {
            "ok": True,
            "notice": None,
            "summary": summary,
            "income": income,
            "expense": expense,
            "recent": recent[:STONE_FLOW_RECENT],
        }


def _flow_label(row: Mapping[str, Any]) -> str:
    source = str(row.get("source") or "").strip() or "未知来源"
    action = str(row.get("action") or "").strip()
    return f"{source}·{action}" if action else source


def _flow_time(row: Mapping[str, Any]) -> str:
    raw = str(row.get("created_at") or "").strip()
    # created_at is stored as "YYYY-MM-DD HH:MM:SS"; show MM-DD HH:MM.
    if len(raw) >= 16 and raw[4] == "-" and raw[7] == "-":
        return raw[5:16]
    return raw or "未知时间"


def format_stone_flow(payload: Mapping[str, Any], *, format_number=None) -> str:
    """Render the player stone-flow view. Pure: no I/O, no NoneBot."""
    if not payload.get("ok"):
        return str(payload.get("notice") or "暂无灵石流水记录。")

    summary = payload.get("summary") or {}
    stone_in = int(summary.get("stone_in") or 0)
    stone_out = int(summary.get("stone_out") or 0)
    stone_net = int(summary.get("stone_net") or 0)
    records = int(summary.get("records") or 0)
    if records <= 0:
        return "近7天没有灵石收支记录。"

    fmt = format_number or (lambda value: f"{int(value):,}")
    lines = [
        "【我的灵石流水】近7天",
        f"灵石收入：+{fmt(stone_in)}",
        f"灵石支出：-{fmt(stone_out)}",
        f"净收支：{'+' if stone_net >= 0 else '-'}{fmt(abs(stone_net))}",
        f"记录：{records} 笔",
    ]

    income = list(payload.get("income") or [])
    if stone_in > 0 and income:
        lines.append("")
        lines.append("主要收入来源：")
        for row in income:
            lines.append(
                f"· {_flow_label(row)} +{fmt(int(row.get('stone_in') or 0))}"
                f"（{int(row.get('records') or 0)}笔）"
            )

    expense = list(payload.get("expense") or [])
    if stone_out > 0 and expense:
        lines.append("")
        lines.append("主要支出方向：")
        for row in expense:
            lines.append(
                f"· {_flow_label(row)} -{fmt(int(row.get('stone_out') or 0))}"
                f"（{int(row.get('records') or 0)}笔）"
            )

    recent = list(payload.get("recent") or [])
    if recent:
        lines.append("")
        lines.append(f"最近 {len(recent)} 笔：")
        for row in recent:
            delta = int(row.get("stone_delta") or 0)
            lines.append(
                f"· {_flow_time(row)} {_flow_label(row)} "
                f"{'+' if delta >= 0 else '-'}{fmt(abs(delta))}"
            )
    return "\n".join(lines)


__all__ = ["EconomyLedgerApplication", "format_stone_flow"]
