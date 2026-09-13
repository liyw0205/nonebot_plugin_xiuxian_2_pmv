"""Stable facade for database-authoritative auction sessions."""

from .transaction_service import AuctionSessionService

# BEGIN IMMEDIATE protects auction session state and replay operations.
OPERATION_TABLE = "auction_session_operations"
# Database authority is the auction_sessions table.

__all__ = ["AuctionSessionService", "OPERATION_TABLE"]
