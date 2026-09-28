"""Compatibility facade for legacy task transaction services."""

from ...compatibility.legacy_task_transactions import (
    LegacyTaskProgressEventService,
    TaskRewardClaimResult,
    TaskRewardClaimService,
)
from ...features.tasks.progress import TaskProgressEventResult, TasksProgressRepository

TaskProgressEventService = TasksProgressRepository

__all__ = [
    "TaskRewardClaimResult",
    "TaskRewardClaimService",
    "LegacyTaskProgressEventService",
    "TaskProgressEventResult",
    "TaskProgressEventService",
]
