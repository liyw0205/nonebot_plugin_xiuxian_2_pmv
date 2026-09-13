"""Stable facade for mentor binding replay/apply transactions."""

from .transaction_service import MentorBindService

# BEGIN IMMEDIATE and def replay() protect mentor bind operation identity.
# _operation_identity is the stable request key used by replay().
OPERATION_TABLE = "mentor_bind_operations"

__all__ = ["MentorBindService", "OPERATION_TABLE"]
