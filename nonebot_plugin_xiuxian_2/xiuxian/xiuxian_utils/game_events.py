from __future__ import annotations

from typing import Any

try:
    from nonebot.log import logger
except Exception:  # pragma: no cover
    logger = None

from .economy_log import safe_log_economy_change


EVENT_STAT_KEYS: dict[str, tuple[str, ...]] = {
    "sign_in": ("修仙签到",),
    "cultivation_minute": ("修炼时长",),
    "out_closing_minute": ("闭关时长",),
    "xu_out_closing_minute": ("虚神界闭关时长",),
    "work_complete": ("悬赏令结算次数",),
    "boss_attack": ("世界BOSS讨伐次数",),
    "world_event_attack": ("世界事件参与",),
    "sect_task_complete": ("宗门任务", "宗门任务完成"),
    "pet_travel_claim": ("宠物游历领取",),
    "map_mission_complete": ("地图委托完成",),
    "dongfu_harvest": ("洞府收获",),
    "dungeon_clear": ("副本通关",),
    "trade_buy": ("交易购买", "拍卖成交"),
    "trade_sell": ("交易出售", "拍卖成交"),
    "mix_elixir_complete": ("炼丹次数",),
}

EVENT_TASK_KEYS: dict[str, tuple[tuple[str, int | None], ...]] = {
    "sign_in": (("sign_in", 1),),
    "cultivation_minute": (("cultivation_time", None),),
    "out_closing_minute": (("out_closing", None),),
    "xu_out_closing_minute": (("xu_out_closing", None),),
    "work_complete": (("work", 1),),
    "boss_attack": (("boss", 1),),
    "sect_task_complete": (("sect_task_complete", None),),
    "pet_travel_claim": (("pet_travel_claim", None),),
    "map_mission_complete": (("map_mission_complete", None),),
    "dongfu_harvest": (("dongfu_harvest", None),),
    "dungeon_clear": (("dungeon_clear", None),),
    "mix_elixir_complete": (("mix_elixir_complete", None),),
}

ECONOMY_EVENT_KEYS = {
    "sect_task_complete",
    "pet_travel_claim",
    "map_mission_complete",
    "dongfu_harvest",
    "dungeon_clear",
    "trade_buy",
    "trade_sell",
    "world_event_attack",
}


def _log_warning(message: str) -> None:
    if logger:
        logger.warning(message)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _extract_economy_delta(meta: dict[str, Any]) -> dict[str, Any]:
    reward = meta.get("reward")
    if not isinstance(reward, dict):
        reward = {}

    return {
        "stone_delta": _to_int(meta.get("stone_delta", reward.get("stone", 0))),
        "exp_delta": _to_int(meta.get("exp_delta", reward.get("exp", 0))),
        "sect_contribution_delta": _to_int(
            meta.get("sect_contribution_delta", reward.get("sect_contribution", 0))
        ),
        "sect_scale_delta": _to_int(meta.get("sect_scale_delta", reward.get("sect_scale", 0))),
        "sect_materials_delta": _to_int(
            meta.get("sect_materials_delta", reward.get("sect_materials", 0))
        ),
        "item_delta": meta.get("item_delta", reward.get("items", [])),
    }


def _record_statistics(user_id: str, event_key: str, amount: int, meta: dict[str, Any]) -> list[str]:
    try:
        from .utils import update_statistics_value
    except Exception as exc:
        _log_warning(f"加载统计入口失败：{exc}")
        return []

    stat_keys = list(EVENT_STAT_KEYS.get(event_key, ()))
    extra_keys = meta.get("stat_keys")
    if isinstance(extra_keys, str):
        stat_keys.append(extra_keys)
    elif isinstance(extra_keys, (list, tuple, set)):
        stat_keys.extend(str(key) for key in extra_keys if key)

    updated: list[str] = []
    for stat_key in dict.fromkeys(stat_keys):
        try:
            update_statistics_value(user_id, stat_key, increment=amount)
            updated.append(stat_key)
        except Exception as exc:
            _log_warning(f"记录统计失败：user_id={user_id}, event={event_key}, stat={stat_key}, error={exc}")
    return updated


def _record_task_progress(user_id: str, event_key: str, amount: int, meta: dict[str, Any]) -> list[str]:
    try:
        from ..xiuxian_tasks.task_data import record_task_progress
    except Exception as exc:
        _log_warning(f"加载任务入口失败：{exc}")
        return []

    mappings = list(EVENT_TASK_KEYS.get(event_key, ()))
    task_key = meta.get("task_key")
    if task_key:
        mappings.append((str(task_key), None))
    extra_task_keys = meta.get("task_keys")
    if isinstance(extra_task_keys, (list, tuple, set)):
        mappings.extend((str(key), None) for key in extra_task_keys if key)

    completed: list[str] = []
    seen: set[str] = set()
    for mapped_key, mapped_amount in mappings:
        if mapped_key in seen:
            continue
        seen.add(mapped_key)
        try:
            completed.extend(record_task_progress(user_id, mapped_key, mapped_amount or amount))
        except Exception as exc:
            _log_warning(
                f"记录任务进度失败：user_id={user_id}, event={event_key}, task={mapped_key}, error={exc}"
            )
    return completed


def _check_titles(user_id: str, meta: dict[str, Any]) -> list[str]:
    if meta.get("check_titles") is False:
        return []
    try:
        from ..xiuxian_title.title_data import check_and_unlock_titles
    except Exception:
        return []

    try:
        unlocked = check_and_unlock_titles(user_id)
        return unlocked if isinstance(unlocked, list) else []
    except Exception as exc:
        _log_warning(f"检查称号成就失败：user_id={user_id}, error={exc}")
        return []


def record_game_event(
    user_id: str,
    event_key: str,
    amount: int = 1,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    user_id = str(user_id)
    event_key = str(event_key)
    amount = max(0, _to_int(amount, 1))
    meta = meta or {}
    result: dict[str, Any] = {
        "user_id": user_id,
        "event_key": event_key,
        "amount": amount,
        "statistics": [],
        "tasks": [],
        "titles": [],
        "economy_log_id": 0,
    }

    if amount <= 0:
        return result

    if meta.get("skip_statistics") is not True:
        result["statistics"] = _record_statistics(user_id, event_key, amount, meta)
    result["tasks"] = _record_task_progress(user_id, event_key, amount, meta)
    result["titles"] = _check_titles(user_id, meta)

    if meta.get("log_economy") or event_key in ECONOMY_EVENT_KEYS:
        economy_delta = _extract_economy_delta(meta)
        result["economy_log_id"] = safe_log_economy_change(
            user_id=user_id,
            sect_id=meta.get("sect_id"),
            source=str(meta.get("source") or event_key),
            action=str(meta.get("action") or "event"),
            detail={"event_key": event_key, "amount": amount, **dict(meta.get("detail") or {})},
            **economy_delta,
        )

    return result


def safe_record_game_event(
    user_id: str,
    event_key: str,
    amount: int = 1,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return record_game_event(user_id, event_key, amount, meta)
    except Exception as exc:
        _log_warning(f"记录玩法事件失败：user_id={user_id}, event={event_key}, error={exc}")
        return {
            "user_id": str(user_id),
            "event_key": str(event_key),
            "amount": amount,
            "statistics": [],
            "tasks": [],
            "titles": [],
            "economy_log_id": 0,
        }
