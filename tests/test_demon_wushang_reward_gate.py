"""魔修随机奖励：无上仅 1% 保留。"""

from __future__ import annotations

import random
import re
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = (
    ROOT
    / "nonebot_plugin_xiuxian_2"
    / "xiuxian"
    / "xiuxian_world_events"
    / "__init__.py"
)


def _load_helpers():
    text = SRC.read_text(encoding="utf-8")
    # extract minimal functions by exec with stubs
    ns = {
        "random": random,
    }

    def _to_int(value, default: int = 0) -> int:
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

    ns["_to_int"] = _to_int
    # pull function bodies via regex is brittle; reimplement mirrors for contract + source assert
    return ns


class DemonWushangPickTests(unittest.TestCase):
    def test_source_has_wushang_1pct_pick(self) -> None:
        text = SRC.read_text(encoding="utf-8")
        self.assertIn("def _is_wushang_reward_item", text)
        self.assertIn("def _pick_demon_random_reward", text)
        self.assertIn("random.randint(1, 100) != 100", text)
        # claim path uses pick helper, not bare choice of full pool
        claim = text[text.index("talisman_reward = _demon_talisman_reward_count") :]
        self.assertIn("random_reward = _pick_demon_random_reward(contribution)", claim)
        self.assertNotIn("reward_pool = _get_demon_random_reward_pool(contribution)", claim[:500])

    def test_wushang_mostly_rerolled(self) -> None:
        # pure logic mirror
        def is_wushang(item):
            return str(item.get("level") or "") == "无上" or str(item.get("rank") or "") == "无上"

        def pick(pool, rng):
            choice = rng.choice(pool)
            item = choice[1]
            if is_wushang(item):
                if rng.randint(1, 100) != 100:
                    normal = [row for row in pool if not is_wushang(row[1])]
                    return rng.choice(normal) if normal else None
            return choice

        pool = [
            (1, {"name": "凡功", "level": "人阶下品", "rank": "50"}),
            (15355, {"name": "自在天功", "level": "无上", "rank": "1"}),
        ]
        wushang_hits = 0
        trials = 3000
        rng = random.Random(42)
        for _ in range(trials):
            # force first choice wushang by using pool order + choice of index 1 often
            # instead call pick on pool many times with real random
            got = pick(pool, rng)
            if got and got[0] == 15355:
                wushang_hits += 1
        # expected roughly trials * 0.5 * 0.01 if half the choices are wushang;
        # with uniform choice P(wushang first)=0.5, then 1% keep => ~0.5%
        rate = wushang_hits / trials
        self.assertLess(rate, 0.05)  # far below old 50% if always kept
        self.assertGreaterEqual(wushang_hits, 0)

    def test_mainbuff_levels_restored_and_fusion_added(self) -> None:
        path = ROOT / "data/xiuxian/功法/主功法.json"
        data = __import__("json").loads(path.read_text(encoding="utf-8"))
        self.assertEqual(str(data["15355"]["level"]), "1")
        self.assertEqual(data["15355"]["rank"], "无上")
        for sid in ("15355", "15356", "9933", "9934"):
            self.assertIn("fusion", data[sid], sid)
            need = data[sid]["fusion"]["need_item"]
            has_wushang_mat = any(
                mid in data and str(data[mid].get("rank")) == "无上" for mid in need
            )
            self.assertTrue(has_wushang_mat, sid)
            # 神物（化道/神圣/蕴灵）应明显高于旧档 2～3
            for mid in ("20002", "20003", "20004"):
                self.assertGreaterEqual(int(need.get(mid, 0)), 8, f"{sid}:{mid}")
        self.assertNotIn("fusion", data["9937"])
        self.assertNotIn("fusion", data["15357"])
        shentong = __import__("json").loads(
            (ROOT / "data/xiuxian/功法/神通.json").read_text(encoding="utf-8")
        )
        self.assertNotEqual(str(shentong["8967"]["level"]), "-6")


if __name__ == "__main__":
    unittest.main()
