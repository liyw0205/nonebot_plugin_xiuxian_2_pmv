"""Stable facade for Tianti medicine baths."""

from .transaction_service import MedicineBathService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "tianti_medicine_bath_operations"

__all__ = ["MedicineBathService", "OPERATION_TABLE"]
