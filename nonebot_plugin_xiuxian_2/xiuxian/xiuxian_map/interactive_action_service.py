"""Stable facade for persistent map interactive actions."""

from ...compatibility.legacy_map_transactions import MapInteractiveActionService

# BEGIN IMMEDIATE protects map_interactive_start_operations and
# map_interactive_terminal_operations.
OPERATION_TABLES = ("map_interactive_start_operations", "map_interactive_terminal_operations")

__all__ = ["MapInteractiveActionService", "OPERATION_TABLES"]
