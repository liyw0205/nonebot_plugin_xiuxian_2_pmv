#!/usr/bin/env python3
"""Run whole unittest modules in one deterministic shard."""

from __future__ import annotations

import argparse
import sys
import time
import unittest
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class _TimingResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timings = []
        self._test_started_at = None

    def startTest(self, test):
        self._test_started_at = time.perf_counter()
        super().startTest(test)

    def stopTest(self, test):
        if self._test_started_at is not None:
            self.timings.append((time.perf_counter() - self._test_started_at, test.id()))
        self._test_started_at = None
        super().stopTest(test)


def _cases(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from _cases(test)
        else:
            yield test


def _partition(cases, shard_count):
    by_module = defaultdict(list)
    for case in cases:
        by_module[case.__class__.__module__].append(case)

    assignments = {}
    loads = [0] * shard_count
    for module, module_cases in sorted(
        by_module.items(), key=lambda item: (-len(item[1]), item[0])
    ):
        shard = min(range(shard_count), key=lambda index: (loads[index], index))
        assignments[module] = shard
        loads[shard] += len(module_cases)

    groups = [[] for _ in range(shard_count)]
    for case in cases:
        groups[assignments[case.__class__.__module__]].append(case)
    return groups, assignments


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--shard",
        required=True,
        help="zero-based shard and total count, for example 0/2",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="show deterministic shard sizes without running tests",
    )
    args = parser.parse_args()

    try:
        shard_value, count_value = args.shard.split("/", 1)
        shard_index, shard_count = int(shard_value), int(count_value)
    except ValueError as exc:
        parser.error(f"invalid --shard value: {exc}")
    if shard_count < 2 or not 0 <= shard_index < shard_count:
        parser.error("--shard must be INDEX/COUNT with COUNT >= 2 and INDEX in range")

    sys.path.insert(0, str(ROOT))
    discovered = unittest.defaultTestLoader.discover(
        start_dir=str(ROOT),
        pattern="test_*.py",
        top_level_dir=str(ROOT),
    )
    cases = list(_cases(discovered))
    groups, assignments = _partition(cases, shard_count)
    module_counts = [
        sum(shard == index for shard in assignments.values())
        for index in range(shard_count)
    ]
    for index, group in enumerate(groups):
        print(
            f"shard {index + 1}/{shard_count}: {len(group)} tests, "
            f"{module_counts[index]} modules",
            flush=True,
        )

    if sum(map(len, groups)) != len(cases):
        raise RuntimeError("unittest shard partition lost or duplicated test cases")
    if args.list:
        print(f"discovered {len(cases)} tests in {len(assignments)} modules")
        return 0

    result = unittest.TextTestRunner(verbosity=0, resultclass=_TimingResult).run(
        unittest.TestSuite(groups[shard_index])
    )
    for seconds, test_id in sorted(result.timings, reverse=True)[:15]:
        print(f"slow test {seconds:.3f}s {test_id}")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
