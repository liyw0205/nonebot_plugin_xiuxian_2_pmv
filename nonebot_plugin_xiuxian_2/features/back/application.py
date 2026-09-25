from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from .repository import BackRepository, LegacyBackRepository
from ..package_reward.application import PackageRewardApplication
from .alchemy_application import AlchemyApplication
from .cultivation_item_application import CultivationItemApplication
from .skill_learning_application import SkillLearningApplication
from .lottery_talisman_application import LotteryTalismanApplication
from .stone_reward_application import StoneRewardApplication
from .three_cultivation_pill_application import ThreeCultivationPillApplication
from .breakthrough_rate_item_application import BreakthroughRateItemApplication
from .recovery_item_application import RecoveryItemApplication
from .permanent_atk_item_application import PermanentAtkItemApplication
from .unbind_application import UnbindApplication


class BackApplication(LegacyApplication):
    def __init__(self, database: str | Path, player_database: str | Path | None = None, *, repository: BackRepository | None = None) -> None:
        self._explicit_repository = repository
        self.alchemy_application = AlchemyApplication(database)
        self.cultivation_item_application = CultivationItemApplication(database)
        self.skill_learning_application = SkillLearningApplication(database)
        self.lottery_talisman_application = LotteryTalismanApplication(database)
        self.stone_reward_application = StoneRewardApplication(database)
        self.three_cultivation_pill_application = ThreeCultivationPillApplication(database)
        self.breakthrough_rate_item_application = BreakthroughRateItemApplication(database)
        self.recovery_item_application = RecoveryItemApplication(database)
        self.permanent_atk_item_application = PermanentAtkItemApplication(database)
        self.unbind_application = UnbindApplication(database)
        super().__init__(database, repository=repository or LegacyBackRepository(database, player_database), feature="back")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"back.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def open_package(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return PackageRewardApplication(self.database).open_package(operation_id=operation_id, user_id=user_id, **kwargs)
        return self._action("open_package", operation_id=operation_id, user_id=user_id, **kwargs)
    def use_item(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("use_item", operation_id=operation_id, user_id=user_id, **kwargs)
    def change_equipment(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("change_equipment", operation_id=operation_id, user_id=user_id, **kwargs)
    def learn_skill(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.skill_learning_application.learn(operation_id, user_id, **kwargs)
        return self._action("learn_skill", operation_id=operation_id, user_id=user_id, **kwargs)
    def repair(self, *, operation_id: str, user_id: str = "system", **kwargs: Any): return self._action("repair", operation_id=operation_id, user_id=user_id, **kwargs)
    def use_pet_eggs(self, *, operation_id: str, user_id: str, **kwargs: Any): return self._action("use_pet_eggs", operation_id=operation_id, user_id=user_id, **kwargs)
    def cultivation_item(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.cultivation_item_application.apply(operation_id, user_id, **kwargs)
        return self._action("cultivation_item", operation_id=operation_id, user_id=user_id, **kwargs)
    def lottery_talisman(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.lottery_talisman_application.apply(operation_id, user_id, **kwargs)
        return self._action("lottery_talisman", operation_id=operation_id, user_id=user_id, **kwargs)
    def stone_reward(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.stone_reward_application.apply(operation_id, user_id, **kwargs)
        return self._action("stone_reward", operation_id=operation_id, user_id=user_id, **kwargs)
    def three_cultivation_pill(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.three_cultivation_pill_application.apply(operation_id, user_id, **kwargs)
        return self._action("three_cultivation_pill", operation_id=operation_id, user_id=user_id, **kwargs)
    def breakthrough_rate_item(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.breakthrough_rate_item_application.apply(operation_id, user_id, **kwargs)
        return self._action("breakthrough_rate_item", operation_id=operation_id, user_id=user_id, **kwargs)
    def recovery_item(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.recovery_item_application.apply(operation_id, user_id, **kwargs)
        return self._action("recovery_item", operation_id=operation_id, user_id=user_id, **kwargs)
    def permanent_atk_item(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.permanent_atk_item_application.apply(operation_id, user_id, **kwargs)
        return self._action("permanent_atk_item", operation_id=operation_id, user_id=user_id, **kwargs)
    def alchemy(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.alchemy_application.apply(operation_id, user_id, **kwargs)
        return self._action("alchemy", operation_id=operation_id, user_id=user_id, **kwargs)
    def unbind(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.unbind_application.apply(operation_id, user_id, **kwargs)
        return self._action("unbind", operation_id=operation_id, user_id=user_id, **kwargs)



__all__ = ["BackApplication"]
