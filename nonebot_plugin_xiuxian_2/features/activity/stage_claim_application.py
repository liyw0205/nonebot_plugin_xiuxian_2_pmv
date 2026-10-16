"""Stage reward settlement: claim ended stages the player took part in.

The flow covers the whole player path for stage rewards: eligibility comes from
the player's own participation evidence, rewards come from the versioned
config, delivery reuses the existing reward path, and the claim row makes a
repeat call a no-op with an explicit message.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ...infrastructure.clock import SystemClock
from .stage import claimable_stage_rows, stage_timeline, stage_window_bounds
from .stage_claim_repository import ActivityStageClaimRepository
from .stage_participation import ActivityStageParticipationRepository

TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"


class ActivityStageClaimApplication:
    """Deliver configured stage rewards once per player per ended stage."""

    action = "activity.stage.claim"

    def __init__(
        self,
        database: str | Path,
        *,
        clock: Any | None = None,
        config_loader: Callable[[], dict[str, Any]] | None = None,
        claim_repository: ActivityStageClaimRepository | None = None,
        participation_repository: ActivityStageParticipationRepository | None = None,
        reward_sender: Callable[[str, str], list[str]] | None = None,
    ) -> None:
        self.database = Path(database)
        self.clock = clock or SystemClock()
        self.config_loader = config_loader
        self.claims = claim_repository or ActivityStageClaimRepository(database)
        self.participation = (
            participation_repository
            or ActivityStageParticipationRepository(database)
        )
        self._reward_sender = reward_sender

    def _config(self) -> dict[str, Any]:
        if self.config_loader is not None:
            return self.config_loader()
        from ...xiuxian.xiuxian_activity.activity_config import load_config

        return load_config()

    def _send_reward(self, user_id: str, reward_text: str) -> list[str]:
        """Deliver one reward row through the existing reward path."""
        if self._reward_sender is not None:
            return list(self._reward_sender(user_id, reward_text) or [])
        from ...xiuxian.xiuxian_activity.service import parse_reward
        from ...xiuxian.xiuxian_compensation.common import send_reward_to_user

        return list(send_reward_to_user(user_id, parse_reward(reward_text)) or [])

    def claim(self, user_id: str, *, query: str = "") -> tuple[bool, str]:
        from ...xiuxian.xiuxian_activity.activity_config import (
            _activity_config_key,
            _runtime_feature_refusal,
            activity_state,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import _clean_text

        uid = str(user_id)
        config = self._config()
        ok, reason = activity_state(config)
        if not ok:
            return False, reason or "活动未开放"
        refusal = _runtime_feature_refusal(config, "claim", "活动领奖")
        if refusal:
            return False, refusal

        now = self.clock.now().astimezone().replace(tzinfo=None)
        timeline = stage_timeline(config, now)
        ended = timeline["ended"]
        if not ended:
            return False, "当前没有已结束的活动阶段"

        activity_key = _activity_config_key(config)
        windows = [(stage["key"], *stage_window_bounds(stage)) for stage in ended]
        participated = self.participation.participated_stages(uid, activity_key, windows)
        claimed = self.claims.claimed_stages(uid, activity_key)
        rows = claimable_stage_rows(config, now, participated, claimed)

        target = _clean_text(query)
        if target:
            rows = [
                row
                for row in rows
                if target in row["key"] or target in row["name"]
            ]
            if not rows:
                return False, f"没有可领取的阶段奖励：{target}"

        if not rows:
            if not any(stage["reward_rows"] for stage in ended):
                return False, "已结束阶段没有配置奖励"
            if not participated:
                return False, "你在已结束阶段没有参与记录"
            return False, "阶段奖励已领取"

        lines = ["活动阶段奖励领取成功："]
        recorded: list[tuple[str, str]] = []
        for row in rows:
            rewards = row["rewards"]
            recorded.append(
                (row["key"], "、".join(item["reward"] for item in rewards))
            )
            for item in rewards:
                texts = self._send_reward(uid, item["reward"])
                name = item["name"] or row["name"]
                detail = "；".join(texts) if texts else item["reward"]
                lines.append(f"- {row['name']}｜{name}：{detail}")

        self.claims.record_claims(
            uid, activity_key, recorded, timestamp=now.strftime(TIMESTAMP_FMT)
        )
        return True, "\n".join(lines)


__all__ = ["ActivityStageClaimApplication"]
