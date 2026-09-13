"""Stable facade for sect membership transactions."""

from .transaction_service import SectMembershipService

# BEGIN IMMEDIATE protects all membership operation ledgers.
OPERATION_TABLES = (
    "sect_operations",
    "sect_fairyland_operations",
    "sect_elixir_room_operations",
    "sect_buff_search_operations",
    "sect_practice_operations",
    "sect_rename_operations",
)

__all__ = ["SectMembershipService", "OPERATION_TABLES"]
