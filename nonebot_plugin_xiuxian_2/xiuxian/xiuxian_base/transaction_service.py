from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
import random
from typing import Callable
from ..xiuxian_utils import db_backend
from ..xiuxian_buff.relation_transaction_utils import increment_stat
from ...compatibility.legacy_base_lottery import (
    LotteryPoolSnapshot,
    LotterySettlementResult,
    LotterySettlementService,
    LotteryWinner,
)
from ...compatibility.legacy_base_player_rename import (
    PlayerRenameResult,
    PlayerRenameService,
)
from ...compatibility.legacy_base_stone_contest import (
    StoneContestResult,
    StoneContestService,
    StoneTheftResult,
)
from ...compatibility.legacy_base_stone_robbery import (
    StoneRobberyResult,
    StoneRobberySettlementService,
)
from ...compatibility.legacy_base_xiangyuan import (
    XiangyuanClaimResult,
    XiangyuanCreateResult,
    XiangyuanSettlementService,
)
from ...compatibility.legacy_base_breakthrough import (
    BreakthroughService,
    ContinuousBreakthroughResult,
    DirectBreakthroughResult,
)
from ...compatibility.legacy_base_ordinary_tribulation import (
    OrdinaryTribulationResult,
    OrdinaryTribulationService,
)
from ...compatibility.legacy_base_destiny_tribulation import (
    DestinyTribulationResult,
    DestinyTribulationService,
)
from ...compatibility.legacy_base_heart_devil_tribulation import (
    HeartDevilTribulationResult,
    HeartDevilTribulationService,
)
from ...compatibility.legacy_base_pill_fusion import PillFusionResult, PillFusionService
from ...compatibility.legacy_base_tribulation_state_migration import (
    TribulationStateMigrationResult,
    TribulationStateMigrationService,
)
from datetime import date, datetime
from datetime import datetime


__all__ = [
    "SignInResult",
    "SignInService",
    "PlayerRenameResult",
    "PlayerRenameService",
    "StoneGiftResult",
    "StoneGiftService",
    "StoneContestResult",
    "StoneTheftResult",
    "StoneContestService",
    "StoneRobberyResult",
    "StoneRobberySettlementService",
    "LotteryWinner",
    "LotteryPoolSnapshot",
    "LotterySettlementResult",
    "LotterySettlementService",
    "XiangyuanCreateResult",
    "XiangyuanClaimResult",
    "XiangyuanSettlementService",
    "DirectBreakthroughResult",
    "ContinuousBreakthroughResult",
    "BreakthroughService",
    "OrdinaryTribulationResult",
    "OrdinaryTribulationService",
    "DestinyTribulationResult",
    "DestinyTribulationService",
    "HeartDevilTribulationResult",
    "HeartDevilTribulationService",
    "PillFusionResult",
    "PillFusionService",
    "TribulationStateMigrationResult",
    "TribulationStateMigrationService",
]
