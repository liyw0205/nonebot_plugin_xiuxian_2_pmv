from __future__ import annotations

from pathlib import Path
from typing import Any

from .._legacy_application import LegacyApplication
from ...core.errors import ValidationError
from .repository import BackRepository
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
from .blessed_flag_replace_application import BlessedFlagReplaceApplication
from .equipment_application import EquipmentApplication
from .repair_application import BackpackRepairApplication
from .pet_egg_application import PetEggApplication
from .item_use_application import ItemUseApplication
from ..accessory_package.application import AccessoryPackageApplication, AccessoryPackageResult
from .accessory_affix_application import AccessoryAffixApplication
from .accessory_decompose_application import AccessoryDecomposeApplication
from .accessory_wash_application import AccessoryWashApplication


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
        self.blessed_flag_replace_application = BlessedFlagReplaceApplication(database, player_database or database)
        self.equipment_application = EquipmentApplication(database)
        self.repair_application = BackpackRepairApplication(database)
        self.pet_egg_application = PetEggApplication(database, player_database or database)
        self.item_use_application = ItemUseApplication(database)
        self.accessory_package_application = AccessoryPackageApplication(
            database,
            player_database or database,
        )
        self.accessory_affix_application = AccessoryAffixApplication(
            database,
            player_database or database,
        )
        self.accessory_decompose_application = AccessoryDecomposeApplication(
            database,
            player_database or database,
        )
        self.accessory_wash_application = AccessoryWashApplication(
            database,
            player_database or database,
        )
        super().__init__(database, repository=repository, feature="back")

    def _action(self, action: str, *, operation_id: str, user_id: str, **kwargs: Any):
        return self._execute(operation_id=operation_id, user_id=user_id, action=f"back.{action}", payload={"user_id": user_id, **kwargs}, call=lambda: self.repository.invoke(action, operation_id, user_id, **kwargs))

    def open_package(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return PackageRewardApplication(self.database).open_package(operation_id=operation_id, user_id=user_id, **kwargs)
        return self._action("open_package", operation_id=operation_id, user_id=user_id, **kwargs)
    def use_item(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            if "item_id" not in kwargs or "quantity" not in kwargs:
                raise ValidationError("item_id and quantity are required")
            return self.item_use_application.apply(
                operation_id,
                user_id,
                item_id=kwargs["item_id"],
                quantity=kwargs["quantity"],
                expected_item_count=kwargs.get("expected_item_count"),
            )
        return self._action("use_item", operation_id=operation_id, user_id=user_id, **kwargs)
    def change_equipment(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.equipment_application.change(operation_id, user_id, **kwargs)
        return self._action("change_equipment", operation_id=operation_id, user_id=user_id, **kwargs)
    def learn_skill(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.skill_learning_application.learn(operation_id, user_id, **kwargs)
        return self._action("learn_skill", operation_id=operation_id, user_id=user_id, **kwargs)
    def repair(self, *, operation_id: str, user_id: str = "system", **kwargs: Any):
        if self._explicit_repository is None:
            return self.repair_application.run(operation_id, **kwargs)
        return self._action("repair", operation_id=operation_id, user_id=user_id, **kwargs)
    def use_pet_eggs(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.pet_egg_application.use(operation_id, user_id, **kwargs)
        return self._action("use_pet_eggs", operation_id=operation_id, user_id=user_id, **kwargs)
    def accessory_package(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            # Keep the legacy handler's result shape while routing the actual
            # state transition through the cross-database application.
            request = dict(kwargs)
            package_value = request.pop("item_id", None)
            if package_value is None:
                package_value = request.pop("package_id")
            package_id = int(package_value)
            rewards = tuple(request.get("rewards", ()))
            accessories = tuple(request.get("accessories", ()))
            request["rewards"] = rewards
            request["accessories"] = accessories
            outcome = self.accessory_package_application.open_package(
                operation_id=operation_id,
                user_id=user_id,
                package_id=package_id,
                **request,
            )
            data = outcome.data if isinstance(outcome.data, dict) else {}
            status = "duplicate" if outcome.status == "replayed" else (
                outcome.code if outcome.status in {"rejected", "failed"} else outcome.status
            )
            return AccessoryPackageResult(
                status=status,
                user_id=str(data.get("user_id", user_id)),
                package_id=int(data.get("package_id", package_id)),
                quantity=int(data.get("quantity", request.get("quantity", 0))),
                rewards=rewards,
                accessories=tuple(data.get("accessories", accessories)),
            )
        return self._action("accessory_package", operation_id=operation_id, user_id=user_id, **kwargs)
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
    def blessed_flag_replace(self, *, operation_id: str, user_id: str, **kwargs: Any):
        if self._explicit_repository is None:
            return self.blessed_flag_replace_application.replace(operation_id, user_id, **kwargs)
        return self._action("blessed_flag_replace", operation_id=operation_id, user_id=user_id, **kwargs)



__all__ = ["BackApplication"]
