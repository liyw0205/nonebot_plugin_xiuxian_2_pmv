#!/usr/bin/env python3
"""Compare play daily caps vs weekly task targets (pace plan Task 0 / L4).

Week effective = daily_cap * 7 * k, k in {0.65, 0.71, 0.80}.

Usage:
  python3 scripts/audit_daily_weekly_caps.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian"

K_BAND = (0.65, 0.71, 0.80)


def _int_assign(text: str, name: str, default: int | None = None) -> int:
    m = re.search(rf"{re.escape(name)}\s*=\s*(\d+)", text)
    if not m:
        if default is not None:
            return default
        raise SystemExit(f"missing {name}")
    return int(m.group(1))


def _task_targets() -> dict[str, tuple[int, str]]:
    text = (PKG / "xiuxian_tasks" / "task_data.py").read_text(encoding="utf-8")
    out: dict[str, tuple[int, str]] = {}
    for m in re.finditer(
        r'key="(weekly_[^"]+)"[\s\S]*?desc="([^"]*)"[\s\S]*?target=(\d+)',
        text,
    ):
        out[m.group(1)] = (int(m.group(3)), m.group(2))
    return out


def _daily_caps() -> list[dict]:
    work = (PKG / "xiuxian_work" / "__init__.py").read_text(encoding="utf-8")
    sect = (PKG / "xiuxian_sect" / "sectconfig.py").read_text(encoding="utf-8")
    mp = (PKG / "xiuxian_map" / "__init__.py").read_text(encoding="utf-8")
    boss = (PKG / "xiuxian_boss" / "__init__.py").read_text(encoding="utf-8")
    cfg = (PKG / "xiuxian_config.py").read_text(encoding="utf-8")

    work_n = _int_assign(work, "count")
    m = re.search(r'["\']每日宗门任务次上限["\']\s*:\s*(\d+)', sect)
    sect_n = int(m.group(1)) if m else -1
    block = re.search(r"DAILY_LIMIT_CONFIG\s*=\s*\{([^}]+)\}", mp, re.S)
    map_caps = dict(re.findall(r'"(gather|combat|explore)"\s*:\s*(\d+)', block.group(1))) if block else {}
    boss_n = _int_assign(boss, "battle_count")
    mentor = _int_assign(cfg, "self.mentor_transmission_limit")

    # play rows: name, daily_cap, weekly_task_key (optional)
    return [
        {"play": "work_refresh", "daily": work_n, "weekly_key": "weekly_work", "note": "刷新次数≈结算机会上沿"},
        {"play": "sect_task", "daily": sect_n, "weekly_key": "weekly_sect_task_complete", "note": ""},
        {"play": "boss_battle", "daily": boss_n, "weekly_key": "weekly_boss", "note": ""},
        {"play": "map_gather", "daily": int(map_caps.get("gather", 0)), "weekly_key": None, "note": "委托另计"},
        {"play": "map_combat", "daily": int(map_caps.get("combat", 0)), "weekly_key": None, "note": ""},
        {"play": "map_explore", "daily": int(map_caps.get("explore", 0)), "weekly_key": None, "note": ""},
        {"play": "map_mission", "daily": 1, "weekly_key": "weekly_map_mission_complete", "note": "日任务点亮1；供给随玩法"},
        {"play": "mentor_transmit", "daily": mentor, "weekly_key": None, "note": ""},
        {"play": "dungeon_clear", "daily": 1, "weekly_key": "weekly_dungeon_clear", "note": "日重置池近似1通/日"},
    ]


def main() -> int:
    tasks = _task_targets()
    rows = _daily_caps()
    print("=== daily / weekly caps audit ===")
    print(f"{'play':<18} {'day':>4} {'x7':>6} {'k65':>6} {'k71':>6} {'k80':>6} {'weekly':>8} {'flag':<8} note")
    for r in rows:
        d = int(r["daily"])
        band = [d * 7 * k for k in K_BAND]
        full = d * 7
        wk = r.get("weekly_key")
        tgt = tasks.get(wk, (None, ""))[0] if wk else None
        flag = "-"
        if tgt is not None and d > 0:
            lo, mid, hi = band
            if tgt > hi * 1.05:
                flag = "HIGH"
            elif tgt < lo * 0.5:
                flag = "LOW"
            else:
                flag = "OK"
        tgt_s = str(tgt) if tgt is not None else "-"
        print(
            f"{r['play']:<18} {d:>4} {full:>6.0f} {band[0]:>6.1f} {band[1]:>6.1f} {band[2]:>6.1f} "
            f"{tgt_s:>8} {flag:<8} {r.get('note','')}"
        )

    print()
    print("weekly tasks raw:")
    for k, (t, desc) in sorted(tasks.items()):
        print(f"  {k}: target={t}  {desc}")

    # closing_exp spot
    cfg = (PKG / "xiuxian_config.py").read_text(encoding="utf-8")
    m = re.search(r"self\.closing_exp\s*=\s*(\d+)", cfg)
    if m:
        print()
        print(f"closing_exp={m.group(1)} (L1)")
    print("flag HIGH = weekly >> day*7*0.80; LOW = weekly << day*7*0.65")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
