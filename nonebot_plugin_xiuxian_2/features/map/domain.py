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


__all__ = ["InteractiveActionDecision", "MapOperation", "decide_interactive_action"]
