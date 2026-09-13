"""Stable facade for partner invitation transactions."""

from .transaction_service import PartnerInviteService

# BEGIN IMMEDIATE protects partner invitation state.
__all__ = ["PartnerInviteService"]
