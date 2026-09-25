from __future__ import annotations

from pathlib import Path
from typing import Any

from ...infrastructure.clock import SystemClock
from .partner_cultivation_repository import (
    PartnerCultivationResult,
    PartnerCultivationSqlRepository,
)


class PartnerCultivationApplication:
    """Application boundary for settling a dual-cultivation operation."""

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: PartnerCultivationSqlRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or PartnerCultivationSqlRepository(
            game_database, player_database
        )
        self.clock = clock or SystemClock()

    def apply(
        self,
        operation_id: str,
        user_id_1: str,
        user_id_2: str,
        *,
        expected_exp_1: int,
        expected_exp_2: int,
        exp_1: int,
        exp_2: int,
        used_count: int,
        power_1: int,
        power_2: int,
        hp_1: int,
        mp_1: int,
        atk_1: int,
        hp_2: int,
        mp_2: int,
        atk_2: int,
        level_rate_1: int = 0,
        level_rate_2: int = 0,
        expected_affection_1: int | None = None,
        expected_affection_2: int | None = None,
        affection_1: int = 0,
        affection_2: int = 0,
        invite_id: str | None = None,
        expected_used_count_1: int | None = None,
        expected_used_count_2: int | None = None,
        expected_target_protection: str | None = None,
    ) -> PartnerCultivationResult:
        return self.repository.apply(
            operation_id,
            user_id_1,
            user_id_2,
            now_timestamp=self.clock.now().timestamp(),
            expected_exp_1=expected_exp_1,
            expected_exp_2=expected_exp_2,
            exp_1=exp_1,
            exp_2=exp_2,
            used_count=used_count,
            power_1=power_1,
            power_2=power_2,
            hp_1=hp_1,
            mp_1=mp_1,
            atk_1=atk_1,
            hp_2=hp_2,
            mp_2=mp_2,
            atk_2=atk_2,
            level_rate_1=level_rate_1,
            level_rate_2=level_rate_2,
            expected_affection_1=expected_affection_1,
            expected_affection_2=expected_affection_2,
            affection_1=affection_1,
            affection_2=affection_2,
            invite_id=invite_id,
            expected_used_count_1=expected_used_count_1,
            expected_used_count_2=expected_used_count_2,
            expected_target_protection=expected_target_protection,
        )


__all__ = ["PartnerCultivationApplication", "PartnerCultivationResult"]
