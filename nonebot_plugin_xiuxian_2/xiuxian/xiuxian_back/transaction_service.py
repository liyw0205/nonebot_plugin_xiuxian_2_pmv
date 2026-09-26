from __future__ import annotations

from ...compatibility.legacy_back_accessory_transaction import (
    AccessoryTransactionResult,
    AccessoryTransactionService,
)
from ...compatibility.legacy_back_skill_learning import (
    SKILL_COLUMNS,
    SkillLearningResult,
    SkillLearningService,
)
from ...compatibility.legacy_back_lottery_talisman import (
    LotteryReward,
    LotteryTalismanUse,
    LotteryTalismanService,
)
from ...compatibility.legacy_back_stone_reward import (
    StoneItemReward,
    StoneItemRewardService,
)
from ...compatibility.legacy_back_three_cultivation_pill import (
    ThreeCultivationPillUse,
    ThreeCultivationPillService,
)
from ...compatibility.legacy_back_unbind import (
    UnbindItemResult,
    UnbindItemService,
)
from ...compatibility.legacy_back_cultivation_item import (
    CultivationItemUse,
    CultivationItemService,
)
from ...compatibility.legacy_back_breakthrough_rate_item import (
    BreakthroughRateItemUse,
    BreakthroughRateItemService,
)
from ...compatibility.legacy_back_permanent_atk_item import (
    PermanentAtkItemUse,
    PermanentAtkItemService,
)
from ...compatibility.legacy_back_recovery_item import (
    RecoveryItemUse,
    RecoveryItemService,
)
from ...compatibility.legacy_back_blessed_flag_replace import (
    BlessedFlagReplaceResult,
    BlessedFlagReplaceService,
)

from ...features.back.pet_egg_repository import BatchPetEggUseResult, PetEggUseSqlRepository
from ...compatibility.legacy_back_repair import BackpackRepairResult, BackpackRepairService
from ...compatibility.legacy_back_alchemy import AlchemyResult, AlchemyService
from ...compatibility.legacy_back_package_reward import (
    PackageOpenResult,
    PackageReward,
    PackageRewardService,
)
from ...compatibility.legacy_back_accessory_package import (
    AccessoryPackageResult,
    AccessoryPackageService,
)
from ...compatibility.legacy_back_pet_egg import BatchItemUseService
from ...compatibility.legacy_back_equipment import EquipmentChange, EquipmentService


__all__ = [
    "PackageReward",
    "PackageOpenResult",
    "PackageRewardService",
    "AccessoryPackageResult",
    "AccessoryPackageService",
    "AccessoryTransactionResult",
    "AccessoryTransactionService",
    "AlchemyResult",
    "AlchemyService",
    "BackpackRepairResult",
    "BackpackRepairService",
    "BatchPetEggUseResult",
    "BatchItemUseService",
    "BlessedFlagReplaceResult",
    "BlessedFlagReplaceService",
    "BreakthroughRateItemUse",
    "BreakthroughRateItemService",
    "CultivationItemUse",
    "CultivationItemService",
    "EquipmentChange",
    "EquipmentService",
    "LotteryReward",
    "LotteryTalismanUse",
    "LotteryTalismanService",
    "PermanentAtkItemUse",
    "PermanentAtkItemService",
    "RecoveryItemUse",
    "RecoveryItemService",
    "SkillLearningResult",
    "SkillLearningService",
    "StoneItemReward",
    "StoneItemRewardService",
    "ThreeCultivationPillUse",
    "ThreeCultivationPillService",
    "UnbindItemResult",
    "UnbindItemService",
    "SKILL_COLUMNS",
]
