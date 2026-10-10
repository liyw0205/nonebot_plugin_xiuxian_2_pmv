"""Stable query bounds and field names for the economy-ledger read model."""

from __future__ import annotations


ECONOMY_LOG_FIELDS = (
    "id",
    "user_id",
    "sect_id",
    "source",
    "action",
    "stone_delta",
    "exp_delta",
    "sect_contribution_delta",
    "sect_scale_delta",
    "sect_materials_delta",
    "item_delta",
    "detail",
    "trace_id",
    "created_at",
)
FILTER_FIELDS = ("user_id", "sect_id", "source", "action", "trace_id")
TIME_FILTER_FIELDS = ("start_time", "end_time")
DELTA_FIELDS = (
    "stone_delta",
    "exp_delta",
    "sect_contribution_delta",
    "sect_scale_delta",
    "sect_materials_delta",
)
QUICK_PRESETS = {"today": ("今天", 0), "7d": ("近7天", 6), "30d": ("近30天", 29)}
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500
DEFAULT_ANOMALY_STONE_DELTA = 100000000

__all__ = [
    "DEFAULT_ANOMALY_STONE_DELTA",
    "DEFAULT_PAGE_SIZE",
    "DELTA_FIELDS",
    "ECONOMY_LOG_FIELDS",
    "FILTER_FIELDS",
    "MAX_PAGE_SIZE",
    "QUICK_PRESETS",
    "TIME_FILTER_FIELDS",
]
