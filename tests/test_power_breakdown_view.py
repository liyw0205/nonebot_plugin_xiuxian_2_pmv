"""战力拆解：三因子公式、升境预估与降级行为。

战力公式来自 `xiuxian2_handle.py::update_power2`：
`power = round(exp × root_rate × level_spend, 0)`，SQLite 的 `round(x,0)` 对 .5
是远离零方向。本套件钉住：

1. 计算值与公式逐值一致（含 .5 舍入）；
2. 灵根倍率线性生效；
3. `命运道果` 走注入的阶梯倍率，不被默认值覆盖；
4. 未知境界 fail-closed，不伪造战力；
5. 缺 `power` 列仍能算，差异段省略；
6. 缺表降级且不建表；
7. 账面与计算不一致时如实给差异；
8. 升境预估 = 同修为 × 下一境系数，并给出距突破所需修为；
9. 主导项判定；
10. 命令/帮助/manifest 合同。
"""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.base.power_breakdown import (
    PowerBreakdownApplication,
    _round_half_up,
    format_power_breakdown,
)

ME = "10001"

LEVELS = {
    "江湖好手": {"power": 1000, "spend": 1.0},
    "感气境初期": {"power": 2000, "spend": 1.2},
    "感气境中期": {"power": 3000, "spend": 1.5},
}
RATES = {"天灵根": 1.5, "凡根": 1.0}


def rate_fn(root_type, root_level):
    return RATES[root_type]


def create_player_db(path: Path, columns=None) -> None:
    cols = columns or (
        "user_id",
        "level",
        "exp",
        "root",
        "root_type",
        "root_level",
        "power",
    )
    defs = {
        "user_id": "TEXT",
        "level": "TEXT",
        "exp": "INTEGER DEFAULT 0",
        "root": "TEXT",
        "root_type": "TEXT",
        "root_level": "INTEGER DEFAULT 0",
        "power": "INTEGER DEFAULT 0",
    }
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE user_xiuxian(" + ",".join(f'"{c}" {defs[c]}' for c in cols) + ")")
        conn.commit()


def seed(path: Path, row: dict) -> None:
    with sqlite3.connect(path) as conn:
        keys = ",".join(f'"{k}"' for k in row)
        marks = ",".join("?" for _ in row)
        conn.execute(f"INSERT INTO user_xiuxian({keys}) VALUES({marks})", list(row.values()))
        conn.commit()


class PowerBreakdownTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "player.db"
        self.app = PowerBreakdownApplication(
            str(self.db), root_rate_fn=rate_fn, level_data_fn=lambda: LEVELS
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def make(self, **overrides) -> dict:
        row = {
            "user_id": ME,
            "level": "感气境初期",
            "exp": 1000,
            "root": "天灵根",
            "root_type": "天灵根",
            "root_level": 0,
            "power": 1800,
        }
        row.update(overrides)
        return row

    # -------------------------------------------------- 1. 公式一致
    def test_matches_formula_and_rounding(self) -> None:
        create_player_db(self.db)
        # round(1000 × 1.5 × 1.2) = 1800
        seed(self.db, self.make(power=1800))
        p = self.app.breakdown(ME)
        self.assertTrue(p["ok"])
        self.assertEqual(p["power_computed"], 1800)
        self.assertEqual(p["power_delta"], 0)

        # .5 远离零：3 × 1.0 × 0.5 = 1.5 -> 2
        self.assertEqual(_round_half_up(1.5), 2)
        self.assertEqual(_round_half_up(2.5), 3)
        self.assertEqual(_round_half_up(-1.5), -2)

    # -------------------------------------------------- 2. 灵根线性
    def test_root_rate_is_linear(self) -> None:
        create_player_db(self.db)
        seed(self.db, self.make(root="凡根", root_type="凡根", power=1200))
        low = self.app.breakdown(ME)["power_computed"]
        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE user_xiuxian SET root='天灵根', root_type='天灵根'")
            conn.commit()
        high = self.app.breakdown(ME)["power_computed"]
        self.assertEqual(low, 1200)
        self.assertEqual(high, 1800)
        self.assertAlmostEqual(high / low, RATES["天灵根"] / RATES["凡根"], places=6)

    # -------------------------------------------------- 3. 命运道果阶梯
    def test_fate_root_uses_injected_ladder(self) -> None:
        create_player_db(self.db)
        seed(self.db, self.make(root="命运道果", root_type="命运道果", root_level=7))
        calls = []

        def fate_rate(root_type, root_level):
            calls.append((root_type, root_level))
            return 2.4

        app = PowerBreakdownApplication(
            str(self.db), root_rate_fn=fate_rate, level_data_fn=lambda: LEVELS
        )
        p = app.breakdown(ME)
        self.assertEqual(calls, [("命运道果", 7)])
        self.assertEqual(p["root_rate"], 2.4)
        self.assertEqual(p["power_computed"], 2880)  # 1000 × 2.4 × 1.2

    # -------------------------------------------------- 4. 未知境界
    def test_unknown_level_fails_closed(self) -> None:
        create_player_db(self.db)
        seed(self.db, self.make(level="不存在的境界"))
        p = self.app.breakdown(ME)
        self.assertFalse(p["ok"])
        self.assertIsNone(p["power_computed"])
        self.assertIn("境界", p["notice"])
        self.assertIn("无法拆解", format_power_breakdown(p))

    # -------------------------------------------------- 5. 缺 power 列
    def test_missing_power_column_still_computes(self) -> None:
        create_player_db(self.db, columns=("user_id", "level", "exp", "root", "root_type"))
        seed(self.db, {"user_id": ME, "level": "感气境初期", "exp": 1000,
                       "root": "天灵根", "root_type": "天灵根"})
        p = self.app.breakdown(ME)
        self.assertTrue(p["ok"])
        self.assertEqual(p["power_computed"], 1800)
        self.assertIsNone(p["power_stored"])
        self.assertIsNone(p["power_delta"])
        self.assertNotIn("账面战力", format_power_breakdown(p))

    # -------------------------------------------------- 6. 缺表
    def test_missing_table_degrades_without_creating(self) -> None:
        self.db.parent.mkdir(parents=True, exist_ok=True)
        sqlite3.connect(self.db).close()
        p = self.app.breakdown(ME)
        self.assertFalse(p["ok"])
        self.assertIn("玩家库", p["notice"])
        with sqlite3.connect(self.db) as conn:
            names = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        self.assertNotIn("user_xiuxian", names, "只读视图不得建表")

    # -------------------------------------------------- 7. 账面差异
    def test_stored_power_mismatch_is_reported(self) -> None:
        create_player_db(self.db)
        seed(self.db, self.make(power=1000))
        p = self.app.breakdown(ME)
        self.assertEqual(p["power_computed"], 1800)
        self.assertEqual(p["power_stored"], 1000)
        self.assertEqual(p["power_delta"], 800)
        text = format_power_breakdown(p)
        self.assertIn("+800", text)
        self.assertIn("下次战力刷新会校正", text)

    # -------------------------------------------------- 8. 升境预估
    def test_next_level_preview(self) -> None:
        create_player_db(self.db)
        seed(self.db, self.make(exp=1000, level="感气境初期", power=1800))
        p = self.app.breakdown(ME)
        self.assertEqual(p["next_level"], "感气境中期")
        self.assertAlmostEqual(p["next_level_spend"], 1.5)
        # 同修为 1000 × 1.5 × 1.5 = 2250
        self.assertEqual(p["next_power_if_same_exp"], 2250)
        # power=2000 为升到下一境所需修为；已有 1000 → 还需 1000
        self.assertEqual(p["exp_to_next"], 1000)

    def test_last_level_has_no_preview(self) -> None:
        create_player_db(self.db)
        seed(self.db, self.make(level="感气境中期", power=4500))
        p = self.app.breakdown(ME)
        self.assertEqual(p["next_level"], "")
        self.assertIsNone(p["next_power_if_same_exp"])
        self.assertNotIn("下一境", format_power_breakdown(p))

    # -------------------------------------------------- 9. 主导项
    def test_dominant_lever(self) -> None:
        create_player_db(self.db)
        seed(self.db, self.make(power=1800))
        p = self.app.breakdown(ME)
        # 突破系数比 1.2/1.0=1.25 > 灵根 1.5? 否 -> 灵根更大
        self.assertEqual(p["dominant"], "灵根")
        self.assertIn("当前最划算：灵根", format_power_breakdown(p))

        app = PowerBreakdownApplication(
            str(self.db),
            root_rate_fn=lambda root_type, root_level: 1.0,
            level_data_fn=lambda: LEVELS,
        )
        # 灵根 1.0，突破 1.25 -> 突破更大
        self.assertEqual(app.breakdown(ME)["dominant"], "突破")

    # -------------------------------------------------- 10. 命令与帮助
    def test_command_help_and_manifest_contract(self) -> None:
        root = Path(__file__).resolve().parents[1]
        base = (root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_base" / "__init__.py").read_text(encoding="utf-8")
        manifest = (root / "nonebot_plugin_xiuxian_2" / "features" / "base" / "manifest.py").read_text(encoding="utf-8")
        self.assertIn('on_command("战力拆解"', base)
        self.assertIn('aliases={"我的战力", "战力明细"}', base)
        self.assertIn("format_power_breakdown(payload", base)
        self.assertIn("- 战力拆解", base)
        self.assertIn("三因子如何合成战力", base)
        self.assertIn('CommandSpec("战力拆解", aliases=("我的战力", "战力明细")', manifest)


if __name__ == "__main__":
    unittest.main()
