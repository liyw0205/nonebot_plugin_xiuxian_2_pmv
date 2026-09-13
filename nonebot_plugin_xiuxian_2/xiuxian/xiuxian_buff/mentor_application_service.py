"""Stable facade for mentor application transactions."""

from .transaction_service import MentorApplicationService

# BEGIN IMMEDIATE protects application replay, mentor_protection_operations,
# mentor_application_create_operations, mentor_application_resolution_operations,
# and rejected_applications.
OPERATION_TABLES = (
    "mentor_protection_operations",
    "mentor_application_create_operations",
    "mentor_application_resolution_operations",
    "rejected_applications",
)
# def replay_create(...) and def replay_resolution(...) are the replay-first API.

__all__ = ["MentorApplicationService", "OPERATION_TABLES"]
