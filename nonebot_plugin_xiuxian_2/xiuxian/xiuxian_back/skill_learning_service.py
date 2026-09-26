"""Stable facade for skill learning transactions."""

from ...compatibility.legacy_back_skill_learning import SkillLearningService

# BEGIN IMMEDIATE protects the idempotent skill_learning_operations ledger.
OPERATION_TABLE = "skill_learning_operations"

__all__ = ["SkillLearningService", "OPERATION_TABLE"]
