"""Stable facade for persistent map resource rewards."""

from .transaction_service import MapResourceRewardService

# BEGIN IMMEDIATE protects resource reward settlement and its operation ledger.
OPERATION_TABLE = "map_resource_reward_operations"
# The terminal repository update is SET status='completed' and
# gather_cd_until=EXCLUDED.gather_cd_until.

__all__ = ["MapResourceRewardService", "OPERATION_TABLE"]
