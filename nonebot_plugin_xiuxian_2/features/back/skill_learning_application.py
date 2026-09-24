from __future__ import annotations

from pathlib import Path

from .skill_learning_repository import SkillLearningResult, SkillLearningSqlRepository


class SkillLearningApplication:
    """Feature-owned use case for learning a skill book."""

    def __init__(self, database: str | Path, *, repository: SkillLearningSqlRepository | None = None) -> None:
        self.repository = repository or SkillLearningSqlRepository(database)

    def learn(self, operation_id: str, user_id: str, skill_item_id: int, skill_type: str) -> SkillLearningResult:
        return self.repository.learn(operation_id, user_id, skill_item_id, skill_type)


__all__ = ["SkillLearningApplication", "SkillLearningResult"]
