#!/usr/bin/env python3
"""Offline closing-pace table for the pace plan (Task 0).

No NoneBot import. Reads 境界/灵根 JSON + source constants.

Usage:
  python3 scripts/audit_closing_pace.py
  python3 scripts/audit_closing_pace.py --closing-exp 270 --stage 祭道境初期
  python3 scripts/audit_closing_pace.py --double-minutes 120
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian"
DATA_CANDIDATES = [
    ROOT / "data" / "xiuxian",
    Path("/root/xiu2/data/xiuxian"),
]


def _find_data_dir() -> Path:
    for d in DATA_CANDIDATES:
        if (d / "境界.json").is_file() and (d / "灵根.json").is_file():
            return d
    raise SystemExit("境界.json / 灵根.json not found under data candidates")


def _read_closing_exp(default: int = 270) -> int:
    text = (PKG / "xiuxian_config.py").read_text(encoding="utf-8")
    m = re.search(r"self\.closing_exp\s*=\s*(\d+)", text)
    return int(m.group(1)) if m else default


def _skill(name: str) -> tuple[float, float]:
    """ratebuff, clo_exp from 主功法.json."""
    path = _find_data_dir() / "功法" / "主功法.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    for _k, v in data.items():
        if isinstance(v, dict) and v.get("name") == name:
            return float(v.get("ratebuff") or 0), float(v.get("clo_exp") or 0)
    raise SystemExit(f"skill not found: {name}")


def _root_rate(root_name: str, root_level: int = 0) -> float:
    data = json.loads((_find_data_dir() / "灵根.json").read_text(encoding="utf-8"))
    if root_name != "命运道果":
        return float(data[root_name]["type_speeds"])
    # mirror xiuxian2_handle.get_root_rate fate ladder
    base = float(data["永恒道果"]["type_speeds"])
    step = float(data["命运道果"]["type_speeds"])
    total = 0.0
    rem = int(root_level)
    cur = step
    while rem > 0:
        n = min(rem, 5)
        total += n * cur
        rem -= n
        cur = round(max(0.5, cur - 0.3), 2)
        if cur <= 0.5:
            total += rem * 0.5
            break
    return base + total


def _stage_gap(stage: str) -> tuple[float, float, str]:
    levels = json.loads((_find_data_dir() / "境界.json").read_text(encoding="utf-8"))
    keys = list(levels.keys())
    if stage not in keys:
        raise SystemExit(f"unknown stage: {stage}")
    i = keys.index(stage)
    if i + 1 >= len(keys):
        raise SystemExit(f"no next stage after {stage}")
    cur_p = float(levels[stage]["power"])
    next_name = keys[i + 1]
    next_p = float(levels[next_name]["power"])
    spend = float(levels[stage]["spend"])
    return max(0.0, next_p - cur_p), spend, next_name


def per_min_normal(
    closing_exp: float,
    root: float,
    spend: float,
    ratebuff: float,
    clo_exp: float,
    blessed: float = 0.0,
) -> float:
    return (
        closing_exp
        * root
        * spend
        * (1.0 + ratebuff)
        * (1.0 + clo_exp)
        * (1.0 + blessed * 0.5)
    )


def per_min_xu(
    closing_exp: float,
    root: float,
    spend: float,
    ratebuff: float,
    clo_exp: float,
    impart_exp_up: float,
    impart_lv: int,
    blessed: float = 0.0,
    double: bool = True,
) -> float:
    base = (
        closing_exp
        * root
        * spend
        * (1.0 + ratebuff)
        * (1.0 + clo_exp)
        * (1.0 + blessed * 0.5 / 1.5)
        * (1.0 + impart_exp_up)
    )
    if double:
        return base * (1.0 + impart_lv * 0.1)
    return base


def days_from_per_min(rem: float, per_min: float) -> float:
    if per_min <= 0:
        return float("inf")
    return rem / per_min / 1440.0


def calendar_days(rem: float, double_pm: float, single_pm: float, double_min_per_day: float) -> float:
    """Fill rem with double_min_per_day at double_pm, rest of day unused (hang only double budget)."""
    if double_min_per_day <= 0 or double_pm <= 0:
        return float("inf")
    per_day = double_min_per_day * double_pm
    return rem / per_day


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stage", default="祭道境初期")
    ap.add_argument("--closing-exp", type=float, default=None)
    ap.add_argument("--root", default="永恒道果")
    ap.add_argument("--root-level", type=int, default=0, help="命运道果 root_level")
    ap.add_argument("--double-minutes", type=float, default=120.0, help="assumed daily xu double minutes")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    closing_exp = float(args.closing_exp if args.closing_exp is not None else _read_closing_exp())
    rem, spend, nxt = _stage_gap(args.stage)
    root = _root_rate(args.root, args.root_level)
    bajiu = _skill("八九玄功")
    zizai = _skill("自在天功")

    rows = []

    def add(label: str, pm: float, extra: dict | None = None):
        d = days_from_per_min(rem, pm)
        row = {"label": label, "per_min": pm, "theory_days": d}
        if extra:
            row.update(extra)
        rows.append(row)

    add(
        "普通 裸根(无功法)",
        per_min_normal(closing_exp, root, spend, 0, 0, 0),
    )
    add(
        "普通 八九 洞天0",
        per_min_normal(closing_exp, root, spend, bajiu[0], bajiu[1], 0),
    )
    add(
        "普通 八九 洞天1",
        per_min_normal(closing_exp, root, spend, bajiu[0], bajiu[1], 1),
    )
    add(
        "普通 自在 洞天0",
        per_min_normal(closing_exp, root, spend, zizai[0], zizai[1], 0),
    )

    for up, lv, tag in [
        (0.0, 0, "无卡 lv0"),
        (1.5, 0, "半卡1.5 lv0"),
        (3.0, 0, "满卡3 lv0"),
        (3.0, 10, "满卡3 lv10"),
        (3.0, 17, "满卡3 lv17"),
        (3.0, 30, "满卡3 lv30"),
    ]:
        pm_d = per_min_xu(closing_exp, root, spend, bajiu[0], bajiu[1], up, lv, 0, True)
        pm_s = per_min_xu(closing_exp, root, spend, bajiu[0], bajiu[1], up, lv, 0, False)
        cal = calendar_days(rem, pm_d, pm_s, args.double_minutes)
        add(
            f"虚神界 double 八九 {tag}",
            pm_d,
            {
                "calendar_days_if_double_min/day": cal,
                "double_min_per_day": args.double_minutes,
            },
        )

    meta = {
        "data_dir": str(_find_data_dir()),
        "stage": args.stage,
        "next_stage": nxt,
        "rem": rem,
        "spend": spend,
        "closing_exp": closing_exp,
        "root": args.root,
        "root_rate": root,
        "bajiu_ratebuff_clo": bajiu,
        "zizai_ratebuff_clo": zizai,
        "note": "theory_days assumes continuous hang; xu calendar uses only double-min/day",
    }

    if args.json:
        json.dump({"meta": meta, "rows": rows}, sys.stdout, ensure_ascii=False, indent=2)
        print()
        return 0

    print("=== closing pace audit ===")
    for k, v in meta.items():
        print(f"{k}: {v}")
    print()
    print(f"{'label':<36} {'per_min':>14} {'theory_d':>10} {'cal_d@dbl':>10}")
    for r in rows:
        cal = r.get("calendar_days_if_double_min/day")
        cal_s = f"{cal:.2f}" if isinstance(cal, float) else "-"
        print(
            f"{r['label']:<36} {r['per_min']:>14.3e} {r['theory_days']:>10.2f} {cal_s:>10}"
        )
    print()
    print("design anchors (reference only): normal~40d, xu calendar~30-35d")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
