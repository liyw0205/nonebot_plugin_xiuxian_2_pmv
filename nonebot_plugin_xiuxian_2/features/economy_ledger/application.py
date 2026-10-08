from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, Mapping

from ...infrastructure.clock import SystemClock
from .repository import EconomyLedgerSqlRepository, QUICK_PRESETS

FILTER_FIELDS = ("user_id", "sect_id", "source", "action", "trace_id")
TIME_FILTER_FIELDS = ("start_time", "end_time")
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500
DEFAULT_ANOMALY_STONE_DELTA = 100000000


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


__all__ = ["EconomyLedgerApplication"]
