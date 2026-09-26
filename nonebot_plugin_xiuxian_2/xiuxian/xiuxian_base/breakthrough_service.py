"""Stable facade for breakthrough and tribulation transactions."""

from ...compatibility.legacy_base_breakthrough import BreakthroughService

# BEGIN IMMEDIATE protects these operation ledgers.
OPERATION_TABLES = (
    "direct_breakthrough_operations",
    "tribulation_breakthrough_operations",
    "continuous_breakthrough_operations",
    "continuous_tribulation_operations",
)

__all__ = ["BreakthroughService", "OPERATION_TABLES"]
