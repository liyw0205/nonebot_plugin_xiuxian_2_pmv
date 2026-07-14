from __future__ import annotations

import copy
import json


PAST_LIFE_FIELDS = (
    "state",
    "stage",
    "revision",
    "alloc",
    "accumulated",
    "talent",
    "birth_scenario",
    "total_score",
    "score_breakdown",
    "event_indices",
    "event_snapshots",
    "early_death_rolls",
    "history",
    "last_run_time",
    "total_runs",
    "best_ending",
    "best_score",
    "endings_log",
    "achievement_points",
)

JSON_FIELDS = {
    "alloc",
    "accumulated",
    "score_breakdown",
    "event_indices",
    "event_snapshots",
    "early_death_rolls",
    "history",
    "endings_log",
}

INTEGER_FIELDS = {
    "state",
    "stage",
    "revision",
    "total_score",
    "total_runs",
    "best_score",
    "achievement_points",
}

_DEFAULT_STATE = {
    "state": 0,
    "stage": 0,
    "revision": 0,
    "alloc": {},
    "accumulated": {"悟性": 0, "机缘": 0, "根骨": 0, "气运": 0, "心性": 0},
    "talent": "",
    "birth_scenario": "",
    "total_score": 0,
    "score_breakdown": {},
    "event_indices": [],
    "event_snapshots": [],
    "early_death_rolls": {},
    "history": [],
    "last_run_time": None,
    "total_runs": 0,
    "best_ending": "",
    "best_score": 0,
    "endings_log": [],
    "achievement_points": 0,
}


def new_default_state() -> dict:
    return copy.deepcopy(_DEFAULT_STATE)


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def decode_field(field: str, value):
    if value is None:
        return None
    if field in JSON_FIELDS and isinstance(value, str):
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return value
    if field in INTEGER_FIELDS:
        try:
            return int(value)
        except (TypeError, ValueError):
            return value
    return value


def normalize_state(value: dict | None) -> dict:
    defaults = new_default_state()
    source = dict(value or {})
    state = {}
    for field in PAST_LIFE_FIELDS:
        raw = source.get(field)
        if raw is None:
            raw = defaults[field]
        state[field] = decode_field(field, raw)
    return state


def encode_field(field: str, value):
    return canonical(value) if field in JSON_FIELDS else value
