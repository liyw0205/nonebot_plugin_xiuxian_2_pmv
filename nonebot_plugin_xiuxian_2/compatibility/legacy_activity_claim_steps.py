from __future__ import annotations

from collections.abc import Callable


def _stage_claim(user_id: str) -> tuple[bool, str]:
    """Stage rewards are owned by the activity feature claim application."""
    from ..features.activity.stage_claim_application import ActivityStageClaimApplication
    from ..paths import get_paths

    return ActivityStageClaimApplication(get_paths().game_db).claim(user_id)


def _rank_claim(user_id: str) -> tuple[bool, str]:
    """Rank rewards are owned by the activity feature claim application."""
    from ..features.activity.rank_claim_application import ActivityRankClaimApplication
    from ..paths import get_paths

    return ActivityRankClaimApplication(get_paths().game_db).claim(user_id)


def build_legacy_activity_claim_runners(user_id: str) -> dict[str, Callable[[str], tuple[bool, str]]]:
    from ..xiuxian.xiuxian_activity.activity_boss import claim_boss_milestone_reward, claim_boss_rank_reward
    from ..xiuxian.xiuxian_activity.service import claim_activity_pass_rewards, claim_activity_tasks

    uid = str(user_id)
    return {
        "tasks": lambda child_id: claim_activity_tasks(uid, operation_id=child_id),
        "pass": lambda child_id: claim_activity_pass_rewards(uid, operation_id=child_id),
        "boss_milestone": lambda child_id: claim_boss_milestone_reward(uid, operation_id=child_id),
        "boss_rank": lambda child_id: claim_boss_rank_reward(uid, operation_id=child_id),
        "stage": lambda child_id: _stage_claim(uid),
        "rank": lambda child_id: _rank_claim(uid),
    }


__all__ = ["build_legacy_activity_claim_runners"]
