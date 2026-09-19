from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from ...infrastructure.clock import SystemClock
from .season_reward_repository import ArenaSeasonRewardRepository


class ArenaSeasonRewardApplication:
    """Feature boundary for daily arena honor rewards and counter reset."""

    _RANK_HONOR_REWARDS = {
        "青铜": 100,
        "白银": 200,
        "黄金": 300,
        "铂金": 400,
        "钻石": 600,
        "王者": 1000,
    }

    def __init__(
        self,
        game_database: str | Path,
        player_database: str | Path,
        *,
        repository: ArenaSeasonRewardRepository | None = None,
        clock: Any | None = None,
    ) -> None:
        self.repository = repository or ArenaSeasonRewardRepository(game_database, player_database)
        self.clock = clock or SystemClock()

    @classmethod
    def _reward(cls, rank: str, position: int) -> tuple[int, int, int]:
        base = cls._RANK_HONOR_REWARDS.get(str(rank), 100)
        if position == 1:
            bonus = 500
        elif 2 <= position <= 3:
            bonus = 300
        elif 4 <= position <= 10:
            bonus = 200
        elif 11 <= position <= 50:
            bonus = 100
        elif 51 <= position <= 100:
            bonus = 50
        else:
            bonus = 0
        return base + bonus, base, bonus

    def claim(
        self,
        operation_id: str,
        user_id: str,
        season_key: str,
        expected_score: int,
        expected_rank: str,
        expected_position: int,
        expected_honor: int,
        expected_total_honor: int,
        base_honor: int,
        ranking_bonus: int,
        items: Sequence[Mapping[str, Any]] = (),
        max_goods_num: int = 999999999,
        *,
        expected_reset: Mapping[str, Any] | None = None,
    ):
        return self.repository.claim(
            operation_id,
            user_id,
            season_key,
            expected_score,
            expected_rank,
            expected_position,
            expected_honor,
            expected_total_honor,
            base_honor,
            ranking_bonus,
            items,
            max_goods_num,
            expected_reset=expected_reset,
        )

    def reset_daily(self) -> tuple[dict[str, Any], ...]:
        now = self.clock.now()
        season_key = now.date().isoformat()
        settled: list[dict[str, Any]] = []
        for candidate in self.repository.daily_candidates():
            user_id = str(candidate["user_id"])
            position = int(candidate["position"])
            total, base, bonus = self._reward(str(candidate["rank"]), position)
            result = self.claim(
                f"arena-season-reward:{season_key}:{user_id}",
                user_id,
                season_key,
                int(candidate["score"]),
                str(candidate["rank"]),
                position,
                int(candidate["honor_points"]),
                int(candidate["total_honor_earned"]),
                base,
                bonus,
                expected_reset=candidate,
            )
            if result.succeeded and total > 0:
                settled.append(
                    {
                        "user_id": user_id,
                        "total": total,
                        "base": base,
                        "bonus": bonus,
                        "status": result.status,
                    }
                )
        return tuple(settled)


__all__ = ["ArenaSeasonRewardApplication"]
