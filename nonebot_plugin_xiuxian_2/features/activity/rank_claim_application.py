"""Rank reward settlement: claim the configured tiers the player reached.

The flow covers the whole player path for rank rewards: the board list and the
reward tiers come from the versioned config, the player's position comes from
the existing activity tables, delivery reuses the existing reward path, and the
claim row makes a repeat call a no-op with an explicit message.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ...infrastructure.clock import SystemClock
from .rank import board_by_query, board_labels, rewards_for_board, tier_for_position
from .rank_claim_repository import ActivityRankClaimRepository
from .read_model_repository import ActivityReadModelSqlRepository

TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"


class ActivityRankClaimApplication:
    """Deliver configured rank rewards once per player per board tier."""

    action = "activity.rank.claim"

    def __init__(
        self,
        database: str | Path,
        *,
        clock: Any | None = None,
        config_loader: Callable[[], dict[str, Any]] | None = None,
        claim_repository: ActivityRankClaimRepository | None = None,
        read_model_repository: ActivityReadModelSqlRepository | None = None,
        reward_sender: Callable[[str, str], list[str]] | None = None,
    ) -> None:
        self.database = Path(database)
        self.clock = clock or SystemClock()
        self.config_loader = config_loader
        self.claims = claim_repository or ActivityRankClaimRepository(database)
        self.read_model = (
            read_model_repository or ActivityReadModelSqlRepository(database)
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
            activity_rank_config,
            activity_state,
        )
        from ...xiuxian.xiuxian_activity.activity_utils import _clean_text

        uid = str(user_id)
        config = self._config()
        ok, reason = activity_state(config)
        if not ok:
            return False, reason or "活动未开放"

        rank_config = activity_rank_config(config)
        boards = rank_config["boards"]
        rewards = rank_config["rewards"]
        if not rewards:
            return False, "当前活动未配置排行奖励"

        target = _clean_text(query)
        if target:
            board = board_by_query(boards, target)
            if board is None:
                return False, f"未找到榜单【{target}】\n可用榜单：{board_labels(boards)}"
            boards = [board]

        activity_key = _activity_config_key(config)
        claimed = self.claims.claimed_tiers(uid, activity_key)
        now = self.clock.now().astimezone().replace(tzinfo=None)

        lines: list[str] = []
        recorded: list[tuple[str, str, str]] = []
        already: list[str] = []
        not_reached: list[str] = []
        unranked: list[str] = []
        without_tier: list[str] = []
        for board in boards:
            label = str(
                board.get("label") or board.get("key") or board.get("source") or ""
            )
            tiers = rewards_for_board(rewards, board)
            if not tiers:
                without_tier.append(label)
                continue
            position, _value = self.read_model.board_position(
                str(board.get("source") or ""), activity_key, uid
            )
            tier = tier_for_position(tiers, position)
            if tier is None:
                if position <= 0:
                    unranked.append(label)
                else:
                    not_reached.append(label)
                continue
            board_key = str(board.get("key") or board.get("source") or "")
            tier_key = str(tier.get("key") or "")
            if (board_key, tier_key) in claimed:
                already.append(f"{label}｜{tier.get('name')}")
                continue
            reward_text = str(tier.get("reward") or "")
            texts = self._send_reward(uid, reward_text)
            detail = "；".join(texts) if texts else reward_text
            lines.append(f"- {label}｜{tier.get('name')}：{detail}")
            recorded.append((board_key, tier_key, reward_text))

        if not lines:
            if already:
                return False, "排行奖励已领取：" + "、".join(already)
            if not_reached:
                return False, "未达到排行奖励名次：" + "、".join(not_reached)
            if unranked:
                return False, "你尚未上榜，无法领取排行奖励：" + "、".join(unranked)
            if without_tier:
                return False, "该榜单未配置排行奖励：" + "、".join(without_tier)
            return False, "没有可领取的排行奖励"

        self.claims.record_claims(
            uid, activity_key, recorded, timestamp=now.strftime(TIMESTAMP_FMT)
        )
        out = ["活动排行奖励领取成功：", *lines]
        if already:
            out.append("已领取：" + "、".join(already))
        return True, "\n".join(out)


__all__ = ["ActivityRankClaimApplication"]
