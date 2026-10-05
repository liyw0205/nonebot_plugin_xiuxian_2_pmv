from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from ...xiuxian.xiuxian_config import convert_rank
from ...xiuxian.xiuxian_utils.numeric_bind import percent_exp_reward
from ...xiuxian.xiuxian_utils.utils import number_to
from .event_resolver import TrainingEvents


def _state_copy(state: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(state)
    result["weekly_purchases"] = dict(state.get("weekly_purchases") or {})
    if isinstance(result.get("last_time"), datetime):
        result["last_time"] = result["last_time"].strftime("%Y-%m-%d %H:%M:%S")
    return result


def build_training_event_plan(
    *,
    user_id: str,
    training_state: Mapping[str, Any],
    context: Mapping[str, Any],
    now: datetime,
    items: Any,
    random_source: Any,
    max_goods_num: int,
) -> dict[str, Any]:
    user_info = dict(context["user"])
    expected_state = _state_copy(training_state)
    state = dict(training_state)
    state["weekly_purchases"] = dict(training_state.get("weekly_purchases") or {})
    state["last_time"] = now

    event_type = random_source.choices(
        ["progress_plus_1", "progress_plus_2", "nothing", "progress_minus_1", "progress_minus_2"],
        weights=[35, 30, 20, 10, 5],
    )[0]
    event_context = dict(context)
    sub_buff_id = int(event_context.get("buff_info", {}).get("sub_buff", 0) or 0)
    event_context["sub_buff_data"] = items.get_data_by_item_id(sub_buff_id) if sub_buff_id else None
    event_result = TrainingEvents(random_source=random_source).handle_event(
        user_id,
        user_info,
        event_type,
        context=event_context,
        items=items,
    )

    stone_delta = int(event_result.get("amount", 0)) if event_result.get("type") == "stone" else 0
    exp_delta = int(event_result.get("amount", 0)) if event_result.get("type") == "exp" else 0
    hp_delta = int(event_result.get("amount", 0)) if event_result.get("type") == "hp" else 0
    event_items: list[dict[str, Any]] = []
    if event_result.get("type") == "item":
        item_info = items.get_data_by_item_id(event_result["item_id"])
        event_items.append(
            {
                "id": event_result["item_id"],
                "name": event_result["item_name"],
                "type": item_info["type"],
                "amount": -1 if event_result.get("lost") else 1,
            }
        )

    progress_change = {
        "progress_plus_1": 2,
        "progress_plus_2": 2,
        "nothing": 1,
        "progress_minus_1": 0,
        "progress_minus_2": -1,
    }[event_type]
    state["progress"] = max(0, int(state["progress"]) + progress_change)
    if event_result.get("type") == "points":
        state["points"] = int(state["points"]) + int(event_result["amount"])
    state["last_event"] = str(event_result.get("message", ""))

    if state["progress"] >= 12:
        state["progress"] = 0
        state["completed"] = int(state["completed"]) + 1
        state["max_progress"] = max(int(state["max_progress"]), 12)
        user_rank = convert_rank(user_info["level"])[0]
        exp_reward = percent_exp_reward(
            user_info["exp"], 0.01, user_info["level"],
            divide_by_three=True, anchor="gap",
        )
        stone_reward = random_source.randint(5_000_000, 10_000_000)
        points_reward = 1000
        state["points"] = int(state["points"]) + points_reward
        min_rank = max(user_rank - 16, 16)
        item_rank = random_source.randint(min_rank, min_rank + 20)
        item_type = random_source.choice(["功法", "神通", "药材"])
        item_ids = items.get_random_id_list_by_rank_and_item_type(item_rank, item_type)
        if item_ids:
            item_id = random_source.choice(item_ids)
            item_info = items.get_data_by_item_id(item_id)
            reward_items = [
                {"id": item_id, "name": item_info["name"], "type": item_info["type"], "amount": 1}
            ]
            item_reward_message = f"\n随机物品：{item_info['level']}:{item_info['name']}"
        else:
            reward_items = []
            item_reward_message = ""
        state["last_event"] += (
            f"\n恭喜道友完成一个历练进程！获得：\n"
            f"修为+{number_to(exp_reward)}\n"
            f"灵石+{number_to(stone_reward)}\n"
            f"成就点+{points_reward}{item_reward_message}"
        )
        stone_delta += stone_reward
        exp_delta += exp_reward
        event_items.extend(reward_items)

    state["max_progress"] = max(int(state["max_progress"]), int(state["progress"]))
    state = _state_copy(state)
    return {
        "expected_state": expected_state,
        "state": state,
        "expected_user": {
            key: int(user_info.get(key, 0) or 0) for key in ("stone", "exp", "hp", "mp")
        },
        "stone_delta": stone_delta,
        "exp_delta": exp_delta,
        "hp_delta": hp_delta,
        "items": event_items,
        "max_goods_num": int(max_goods_num),
        "message": state["last_event"],
    }


__all__ = ["build_training_event_plan"]
