from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class MapOperation:
    operation_id: str
    user_id: str

    def validate(self) -> None:
        if not self.operation_id or not self.user_id:
            raise ValueError("operation_id and user_id are required")


@dataclass(frozen=True)
class InteractiveActionDecision:
    action: dict[str, Any]


@dataclass(frozen=True)
class InteractiveRewardDecision:
    plan: tuple[tuple[str, int, int, float], ...]
    message: str


def decide_interactive_reward(roll: float, pool_key: str) -> InteractiveRewardDecision:
    roll = float(roll)
    if not 0.0 <= roll <= 1.0 or not pool_key:
        raise ValueError("valid reward roll and pool are required")
    if roll < 0.10:
        return InteractiveRewardDecision((("stone_low", 1, 1, 1.0),), "你惊动了附近的异兽，只来得及捡走些散落资源。")
    if roll < 0.30:
        return InteractiveRewardDecision(((pool_key, 1, 2, 1.0), ("stone_low", 1, 2, 1.0), ("wash_stone_low", 1, 1, 0.15)), "运气极佳，收获颇丰！")
    return InteractiveRewardDecision(((pool_key, 1, 2, 1.0), ("stone_low", 1, 1, 0.55)), "")


def decide_interactive_action(
    *, operation_id: str, action_type: str, node: dict[str, Any], pool_key: str,
    started_at: datetime, wait_seconds: int, resolve_timeout: int,
    cooldown_seconds: int, cost: int, success: bool,
) -> InteractiveActionDecision:
    if not operation_id or not action_type or wait_seconds < 0 or resolve_timeout < 0 or cooldown_seconds < 0 or cost < 0:
        raise ValueError("valid interactive action parameters are required")
    ready_at = started_at + timedelta(seconds=int(wait_seconds))
    expire_at = ready_at + timedelta(seconds=int(resolve_timeout))
    return InteractiveActionDecision({
        "action_id": operation_id,
        "action": action_type,
        "node_name": str(node.get("name", "")),
        "node_type": str(node.get("type", "")),
        "pool_key": str(pool_key),
        "start_ts": started_at.strftime("%Y-%m-%d %H:%M:%S"),
        "ready_ts": ready_at.strftime("%Y-%m-%d %H:%M:%S"),
        "expire_ts": expire_at.strftime("%Y-%m-%d %H:%M:%S"),
        "wait_sec": int(wait_seconds),
        "cost": int(cost),
        "cooldown_sec": int(cooldown_seconds),
        "success": bool(success),
    })


__all__ = ["InteractiveActionDecision", "InteractiveRewardDecision", "MapOperation", "decide_interactive_action", "decide_interactive_reward"]
