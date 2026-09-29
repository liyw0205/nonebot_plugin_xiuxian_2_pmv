from __future__ import annotations

from collections.abc import Callable


def build_legacy_activity_claim_runners(user_id: str) -> dict[str, Callable[[str], tuple[bool, str]]]:
    from ..xiuxian.xiuxian_activity.activity_boss import claim_boss_milestone_reward, claim_boss_rank_reward
    from ..xiuxian.xiuxian_activity.service import claim_activity_pass_rewards, claim_activity_tasks

    uid = str(user_id)
    return {
        "tasks": lambda child_id: claim_activity_tasks(uid, operation_id=child_id),
        "pass": lambda child_id: claim_activity_pass_rewards(uid, operation_id=child_id),
        "boss_milestone": lambda child_id: claim_boss_milestone_reward(uid, operation_id=child_id),
        "boss_rank": lambda child_id: claim_boss_rank_reward(uid, operation_id=child_id),
    }


__all__ = ["build_legacy_activity_claim_runners"]
