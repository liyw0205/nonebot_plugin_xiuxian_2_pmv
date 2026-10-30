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
# 玩家个人灵石流水视图（只读）：近 7 天窗口、来源/去向各前 3、最近 5 笔。
STONE_FLOW_DAYS = 7
STONE_FLOW_TOP_N = 3
STONE_FLOW_RECENT = 5
# 玩家个人宗门贡献流水视图（只读）：同一套窗口/数量口径，字段是 sect_contribution_delta。
CONTRIBUTION_FLOW_DAYS = 7
CONTRIBUTION_FLOW_TOP_N = 3
CONTRIBUTION_FLOW_RECENT = 5

__all__ = [
    "CONTRIBUTION_FLOW_DAYS",
    "CONTRIBUTION_FLOW_RECENT",
    "CONTRIBUTION_FLOW_TOP_N",
    "DEFAULT_ANOMALY_STONE_DELTA",
    "DEFAULT_PAGE_SIZE",
    "DELTA_FIELDS",
    "ECONOMY_LOG_FIELDS",
    "FILTER_FIELDS",
    "MAX_PAGE_SIZE",
    "QUICK_PRESETS",
    "STONE_FLOW_DAYS",
    "STONE_FLOW_RECENT",
    "STONE_FLOW_TOP_N",
    "TIME_FILTER_FIELDS",
]
