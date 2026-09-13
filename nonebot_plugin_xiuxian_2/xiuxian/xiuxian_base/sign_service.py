"""Stable facade for idempotent sign-in transactions."""

from ...compatibility.sign_in import SignInResult, SignInService

# BEGIN IMMEDIATE protects the idempotent sign_in_operations ledger.
OPERATION_TABLE = "sign_in_operations"

__all__ = ["SignInResult", "SignInService", "OPERATION_TABLE"]
