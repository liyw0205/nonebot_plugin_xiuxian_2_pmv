from __future__ import annotations

from .boss_settlement_common import ActivityBossSettlementResult, ActivityBossSettlementService


class ActivityBossCoopSettlementService(ActivityBossSettlementService):
    def settle(
        self, operation_id, user_id, activity_key, expected_hp, expected_max_hp,
        expected_fight_count, daily_limit, fixed_damage, fight_date, timestamp,
        milestones=(),
    ) -> ActivityBossSettlementResult:
        return self._settle(
            operation_id=operation_id,
            user_id=user_id,
            activity_key=activity_key,
            expected_hp=expected_hp,
            expected_max_hp=expected_max_hp,
            expected_fight_count=expected_fight_count,
            daily_limit=daily_limit,
            fixed_damage=fixed_damage,
            fight_date=fight_date,
            source="coop",
            timestamp=timestamp,
            milestones=milestones,
        )
