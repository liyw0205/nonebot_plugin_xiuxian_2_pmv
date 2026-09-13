"""Stable facade for cross-database auction queue operations."""

from .transaction_service import AuctionQueueService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "auction_queue_operations"

__all__ = ["AuctionQueueService", "OPERATION_TABLE"]
