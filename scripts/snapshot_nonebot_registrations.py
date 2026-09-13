#!/usr/bin/env python3
"""Capture a fresh-process snapshot of migrated command registrations."""

from __future__ import annotations

import json
import sys
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    # NoneBot/plugin imports emit diagnostics; keep the snapshot machine-readable.
    with redirect_stdout(StringIO()):
        import nonebot

        nonebot.init()
        import nonebot_plugin_xiuxian_2  # noqa: F401
        from nonebot.matcher import matchers

    migrated = {"今日运势", "占卜", "卜卦", "求签", "运势", "算命", "修仙签到", "签到", "送灵石"}
    snapshot: list[dict[str, object]] = []
    for matcher in matchers.get(1, []):
        rule = repr(getattr(matcher, "rule", ""))
        module = str(getattr(matcher, "module_name", ""))
        if any(command in rule for command in migrated):
            snapshot.append({"module": module, "priority": getattr(matcher, "priority", None), "rule": rule})
    commands = {command: sum(command in str(item["rule"]) for item in snapshot) for command in sorted(migrated)}
    print(json.dumps({"schema": 1, "migrated_command_matcher_counts": commands, "matchers": snapshot}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
