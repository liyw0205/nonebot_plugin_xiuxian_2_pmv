from __future__ import annotations

from ...paths import get_paths
from ...features.arena.state_application import ArenaStateApplication
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager


_player_data_manager_instance = None


class ArenaLimit:
    """Arena rules plus a compatibility facade for transactional state reads."""

    def __init__(self, state_application: ArenaStateApplication | None = None) -> None:
        self.table_name = "arena"
        self.initial_score = 1000
        self.win_points = 20
        self.lose_points = 10
        self.no_match_points = 10
        self.daily_challenges = 10
        self.daily_buy_limit = 3
        self.rank_honor_rewards = {
            "青铜": 100,
            "白银": 200,
            "黄金": 300,
            "铂金": 400,
            "钻石": 600,
            "王者": 1000,
        }
        self.ranking_honor_bonus = {
            "1": 500,
            "2-3": 300,
            "4-10": 200,
            "11-50": 100,
            "51-100": 50,
        }
        self._state_application_override = state_application
        self._state_application_instance = None

    def _player_data_manager(self):
        global _player_data_manager_instance
        if _player_data_manager_instance is None:
            _player_data_manager_instance = PlayerDataManager()
        return _player_data_manager_instance

    def _state_application(self):
        if self._state_application_override is not None:
            return self._state_application_override
        if self._state_application_instance is None:
            self._state_application_instance = ArenaStateApplication(
                get_paths().player_db,
                lock=self._player_data_manager().lock,
            )
        return self._state_application_instance

    def get_user_arena_info(self, user_id):
        state_application = self._state_application()
        return state_application.get(user_id)

    def get_daily_challenge_cap(self, user_id):
        arena_info = self.get_user_arena_info(user_id)
        return self.daily_challenges + int(arena_info.get("daily_extra_challenges", 0))

    def calculate_daily_honor(self, user_id):
        arena_info = self.get_user_arena_info(user_id)
        base_honor = self.rank_honor_rewards.get(arena_info["rank"], 100)
        ranking_bonus = 0
        user_ranking = self.get_user_ranking(user_id)
        if user_ranking == 1:
            ranking_bonus = self.ranking_honor_bonus["1"]
        elif 2 <= user_ranking <= 3:
            ranking_bonus = self.ranking_honor_bonus["2-3"]
        elif 4 <= user_ranking <= 10:
            ranking_bonus = self.ranking_honor_bonus["4-10"]
        elif 11 <= user_ranking <= 50:
            ranking_bonus = self.ranking_honor_bonus["11-50"]
        elif 51 <= user_ranking <= 100:
            ranking_bonus = self.ranking_honor_bonus["51-100"]
        return base_honor + ranking_bonus, base_honor, ranking_bonus

    def get_user_ranking(self, user_id):
        for index, (candidate_id, _) in enumerate(self.get_arena_ranking(limit=1000), 1):
            if str(candidate_id) == str(user_id):
                return index
        return 0

    def get_weekly_purchases(self, user_id, item_id):
        weekly = self.get_user_arena_info(user_id)["weekly_purchases"]
        return int(weekly.get(str(item_id), 0))

    def can_challenge_today(self, user_id):
        arena_info = self.get_user_arena_info(user_id)
        return int(arena_info["daily_challenges_used"]) < self.get_daily_challenge_cap(user_id)

    @staticmethod
    def calculate_rank(score):
        if score >= 3200:
            return "王者"
        if score >= 2700:
            return "钻石"
        if score >= 2300:
            return "铂金"
        if score >= 1900:
            return "黄金"
        if score >= 1500:
            return "白银"
        return "青铜"

    def get_arena_ranking(self, limit=50):
        state_application = self._state_application()
        return state_application.ranking(limit)

    @staticmethod
    def get_rank_order():
        return ["青铜", "白银", "黄金", "铂金", "钻石", "王者"]


arena_limit = ArenaLimit()


__all__ = ["ArenaLimit", "arena_limit"]
