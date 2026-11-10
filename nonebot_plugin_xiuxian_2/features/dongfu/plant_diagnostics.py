"""Identify the exact plot field rejected by a snapshot check."""

import json


def plant_diagnosis(actual, expected):
    if isinstance(expected, str):
        expected = json.loads(expected)
    if len(actual) != len(expected):
        return {"reason": "plant_count_changed"}
    for index, (current, before) in enumerate(zip(actual, expected), 1):
        for key in ("slot", "seed_id", "seed_name", "plant_start", "plant_finish", "fertilizer"):
            if current.get(key) != before.get(key):
                return {"reason": "plant_" + key + "_changed", "subject": "plot:" + str(index)}
    return {"reason": "invalid_snapshot"}
