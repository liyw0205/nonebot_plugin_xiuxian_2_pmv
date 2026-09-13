"""Stable facade for Tianti acupoint opening."""

from .transaction_service import QiaoxueService

# BEGIN IMMEDIATE protects tianti_qiaoxue_operations.
OPERATION_TABLE = "tianti_qiaoxue_operations"

__all__ = ["QiaoxueService", "OPERATION_TABLE"]
