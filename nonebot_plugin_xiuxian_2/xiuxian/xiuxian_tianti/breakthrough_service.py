"""Stable facade for Tianti breakthrough transactions."""

from .transaction_service import TiantiBreakthroughService

# BEGIN IMMEDIATE protects tianti_breakthrough_operations.
OPERATION_TABLE = "tianti_breakthrough_operations"

__all__ = ["TiantiBreakthroughService", "OPERATION_TABLE"]
