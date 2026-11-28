"""Frozen non-monotonic viewers for exact range counts and real training."""
from __future__ import annotations

import random

from tests import mixed_rank_gameplay_fixture as gameplay

seed = gameplay.seed
classify_result = gameplay.classify_result
check_invariants = gameplay.check_invariants


def phase_specs():
    return [
        {"mode": "cold", "concurrency": 10, "requests": 128, "arrival_rate": 10, "order": "descending"},
        {"mode": "cold", "concurrency": 1, "requests": 128, "arrival_rate": 10, "order": "shuffled"},
        {"mode": "cold", "concurrency": 10, "requests": 128, "arrival_rate": 10, "order": "shuffled"},
        {"mode": "readmostly", "concurrency": 10, "requests": 256, "arrival_rate": 20, "order": "shuffled"},
        {"mode": "mixed", "concurrency": 10, "requests": 256, "arrival_rate": 20, "order": "shuffled"},
        {"mode": "contention", "concurrency": 10, "requests": 128, "arrival_rate": 20, "order": "shuffled"},
        {"mode": "mixed", "concurrency": 20, "requests": 256, "arrival_rate": 50, "order": "shuffled"},
    ]


def request_plan(manifest, mode, count, phase_number=0):
    spec = phase_specs()[phase_number]
    if (mode, count) != (spec["mode"], spec["requests"]):
        raise ValueError("range workload does not match its frozen phase")
    actors = list(manifest["actors"])
    if spec["order"] == "descending":
        actors.reverse()
    else:
        random.Random(20261127).shuffle(actors)
    return gameplay.request_plan({**manifest, "actors": actors}, mode, count, phase_number)
