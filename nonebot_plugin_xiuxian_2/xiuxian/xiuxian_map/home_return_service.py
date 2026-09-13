"""Stable facade for map home-return transactions."""

from .transaction_service import MapHomeReturnService

# BEGIN IMMEDIATE protects map_home_return_operations.
OPERATION_TABLE = "map_home_return_operations"
# Replay is checked with this query before mutable map state is inspected:
# FROM map_home_return_operations WHERE operation_id=%s
# conn.table_exists("dongfu_status") is evaluated only after replay lookup.

__all__ = ["MapHomeReturnService", "OPERATION_TABLE"]
