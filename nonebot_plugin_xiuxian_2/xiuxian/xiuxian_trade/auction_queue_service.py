"""Stable facade for cross-database auction queue operations."""

from ...compatibility.legacy_trade_auction_queue import AuctionQueueService

# The implementation uses ATTACH DATABASE and BEGIN IMMEDIATE.
OPERATION_TABLE = "auction_queue_operations"

__all__ = ["AuctionQueueService", "OPERATION_TABLE"]
