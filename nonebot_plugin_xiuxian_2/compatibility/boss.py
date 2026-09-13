"""Compatibility facades for world-boss asset operations."""

from __future__ import annotations

import os
import warnings
from pathlib import Path
from typing import Any

from ..features.boss.application import BossApplication


def _warn(name: str) -> None:
    warnings.warn(f"{name} is a compatibility facade; use BossApplication", DeprecationWarning, stacklevel=3)
    from .commands import record_compatibility_hit

    record_compatibility_hit("boss")


class BossPurchaseService:
    def __init__(self, game_database: str | Path, player_database: str | Path) -> None:
        self.application = BossApplication(game_database, player_database)
        self._legacy = None
        if os.environ.get("XIUXIAN_BOSS_ENABLED", "true").strip().lower() in {"0", "false", "no", "off"}:
            from ..xiuxian.xiuxian_boss.transaction_service import BossPurchaseService as Legacy

            self._legacy = Legacy(game_database, player_database)

    def purchase(self, *args: Any, **kwargs: Any):
        _warn("BossPurchaseService")
        if self._legacy is not None:
            return self._legacy.purchase(*args, **kwargs)
        from ..xiuxian.xiuxian_boss.transaction_service import BossPurchaseResult

        names = ("operation_id", "user_id", "item_id", "item_name", "item_type", "quantity", "unit_cost", "weekly_limit", "expected_integral", "expected_weekly_purchases", "max_goods_num", "today")
        values = dict(zip(names, args))
        values.update(kwargs)
        outcome = self.application.purchase(**values)
        data = outcome.data or {}
        return BossPurchaseResult(str(data.get("status", outcome.status)), int(data.get("quantity", 0) or 0), int(data.get("cost", 0) or 0), int(data.get("integral", values.get("expected_integral", 0)) or 0), int(data.get("purchased", 0) or 0), int(data.get("inventory", 0) or 0))


class WorldBossBattleSettlementService:
    def __init__(self, game_database: str | Path, player_database: str | Path, activity_database: str | Path | None = None) -> None:
        self.application = BossApplication(game_database, player_database, activity_database=activity_database)
        self._game_database = str(game_database)
        self._player_database = str(player_database)
        self._activity_database = str(activity_database) if activity_database else None

    def get_result(self, operation_id: str):
        _warn("WorldBossBattleSettlementService")
        from ..xiuxian.xiuxian_boss.transaction_service import WorldBossBattleSettlementService as Legacy

        return Legacy(self._game_database, self._player_database, self._activity_database).get_result(operation_id)

    def settle(self, *args: Any, **kwargs: Any):
        _warn("WorldBossBattleSettlementService")
        from ..xiuxian.xiuxian_boss.transaction_service import WorldBossBattleSettlementResult

        names = ("operation_id", "user_id", "expected_bosses", "settled_bosses", "boss_index", "expected_stamina", "stamina_cost", "expected_hp", "expected_mp", "final_hp", "final_mp", "expected_exp", "exp_reward", "expected_stone", "stone_reward", "expected_daily_stone", "expected_daily_integral", "expected_total_integral", "integral_reward", "expected_battle_count", "battle_limit", "expected_checked_at", "checked_at", "item", "max_goods_num", "actual_damage", "killed", "daily_period", "weekly_period", "activity_bosses")
        values = dict(zip(names, args))
        values.update(kwargs)
        outcome = self.application.settle(**values)
        data = outcome.data or {}
        return WorldBossBattleSettlementResult(str(data.get("status", outcome.status)), int(data.get("boss_hp", 0) or 0), int(data.get("stamina", values.get("expected_stamina", 0)) or 0), int(data.get("battle_count", values.get("expected_battle_count", 0)) or 0), int(data.get("stone", values.get("expected_stone", 0)) or 0), int(data.get("exp", values.get("expected_exp", 0)) or 0), int(data.get("integral", values.get("expected_total_integral", 0)) or 0), tuple(data.get("activity_lines", ()) or ()))


__all__ = ["BossPurchaseService", "WorldBossBattleSettlementService"]
