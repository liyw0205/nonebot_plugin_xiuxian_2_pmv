"""Feature-owned Work offer generation and settlement decision ports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class WorkSettlementDecision:
    """The frozen random result passed to the settlement transaction."""

    base_exp: int
    exp_gain: int
    success_kind: str
    success: bool
    big_success: bool
    item_id: int


class WorkRewardApplication:
    """Own Work's random generation and result selection.

    The legacy data files remain the source of the reward catalogue for now,
    but the matcher only sees this feature port.  Injecting the random source
    keeps retries deterministic once the caller has captured the decision.
    """

    def generate_offer(
        self,
        *,
        user_id: str,
        user_level: str,
        exp: int,
        clock: Any,
        random_source: Any,
        reward_multiplier: int | None = None,
    ) -> tuple[list[list[Any]], dict[str, Any]]:
        # Import lazily so importing the feature does not load the legacy
        # matcher or its NoneBot registration side effects.
        from ...xiuxian.xiuxian_work.workmake import workmake

        generated = workmake(
            user_level,
            exp,
            user_level,
            random_source=random_source,
        )
        tasks: dict[str, dict[str, Any]] = {}
        task_order: list[str] = []
        task_list: list[list[Any]] = []
        for name, values in generated.items():
            row = list(values)
            if reward_multiplier is not None:
                row[1] = int(row[1] * int(reward_multiplier))
            task_order.append(str(name))
            tasks[str(name)] = {
                "rate": row[0],
                "award": row[1],
                "time": row[2],
                "item_id": row[3],
                "success_msg": row[4],
                "fail_msg": row[5],
            }
            task_list.append([str(name), *row])
        offer = {
            "tasks": tasks,
            "task_order": task_order,
            "status": 1,
            "refresh_time": clock.now().strftime("%Y-%m-%d %H:%M:%S"),
            "user_level": user_level,
        }
        return task_list, offer

    def generate_capture_offer(
        self,
        *,
        user_id: str,
        user_level: str,
        exp: int,
        clock: Any,
        random_source: Any,
    ) -> tuple[list[list[Any]], dict[str, Any], int]:
        """Preserve capture-token draw order while keeping it feature-owned."""
        task_list, offer = self.generate_offer(
            user_id=user_id,
            user_level=user_level,
            exp=exp,
            clock=clock,
            random_source=random_source,
        )
        multiplier = int(random_source.randint(2, 5))
        for task in offer["tasks"].values():
            task["award"] = int(task["award"] * multiplier)
        for task in task_list:
            task[2] = int(task[2] * multiplier)
        return task_list, offer, multiplier

    def resolve_settlement(
        self,
        *,
        offer_snapshot: Mapping[str, Any],
        work_name: str,
        random_source: Any,
    ) -> WorkSettlementDecision:
        tasks = offer_snapshot.get("tasks")
        task = tasks.get(work_name) if isinstance(tasks, Mapping) else None
        if not isinstance(task, Mapping):
            raise ValueError("work task is missing from the frozen offer")
        base_exp = int(task.get("award", 0) or 0)
        rate = int(task.get("rate", 0) or 0)
        big_success = rate >= 100
        success = int(random_source.randint(1, 100)) <= rate
        item_id = int(task.get("item_id", 0) or 0) if success else 0
        if big_success:
            exp_gain = int(base_exp * random_source.uniform(1.5, 2.5))
            success_kind = "big"
        elif success:
            exp_gain = base_exp
            success_kind = "ok"
        else:
            exp_gain = base_exp // 2
            success_kind = "half"
        return WorkSettlementDecision(
            base_exp=base_exp,
            exp_gain=exp_gain,
            success_kind=success_kind,
            success=success,
            big_success=big_success,
            item_id=item_id,
        )


__all__ = ["WorkRewardApplication", "WorkSettlementDecision"]
