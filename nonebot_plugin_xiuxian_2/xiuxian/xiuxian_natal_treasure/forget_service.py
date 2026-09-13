"""Stable facade for natal treasure effect removal."""

from .transaction_service import ForgetEffectService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "natal_forget_operations"

__all__ = ["ForgetEffectService", "OPERATION_TABLE"]
