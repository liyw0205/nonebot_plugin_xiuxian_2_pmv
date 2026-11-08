"""高境界虚神界修炼：不得用 int64 上限截断修为/战力。

线上/代码证据（用户在 v1.2.8 后指出，已核对本仓库当前实现）：

* 训练 handler 曾用 ``power=min(SQLITE_MAX_INT, int(round(...)))`` 把战力压到 int64；
* ``ImpartTrainingSettlementService.settle`` 曾把 ``power``/``expected_exp``/``exp_gain``
  一律 ``min(..., SQLITE_MAX_INT)``，并把新修为写成
  ``new_exp = min(SQLITE_MAX_INT, expected_exp + exp_gain)``。

``expected_exp`` 同时是**并发校验令牌**（与库内 ``user_xiuxian.exp`` 比对）。
一旦境界高到修为超过 int64，库内值读回来是 REAL（约 15–17 位有效数字），
``int(...)`` 得到的仍是超过 int64 的整数；它被 clamp 成 int64 后必然与库内值不等，
于是一次正常修炼被判成 ``state_changed``——「高境界修炼必然失败」。
存储层的溢出保护由共享绑定 ``bind_sqlite_param`` 在绑定参数时完成，
不需要也不允许在业务层按存储上限截断数值。

保留的仍是**玩法上限与校验**：exp_gain 必须为正、exp_cost 必须为正、
承载上限 100、每日次数等。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_impart_pk.transaction_service import (
    ImpartTrainingSettlementService,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend

HUGE_EXP = 10**19 + 7          # 远超 int64，且 REAL 往返后仍可精确还原
HUGE_POWER = 10**21 + 5
HUGE_GAIN = 10**18 + 3


class HighRealmTrainingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.game, self.impart, self.player = (
            root / "game.db",
            root / "impart.db",
            root / "player.db",
        )
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,power INTEGER)"
            )
            # 高境界：修为已超过 int64，写入经共享绑定落成十进制 TEXT/REAL
            conn.execute(
                "INSERT INTO user_xiuxian VALUES(%s,%s,%s)", ("u", HUGE_EXP, HUGE_POWER)
            )
        with db_backend.transaction(self.impart) as conn:
            conn.execute(
                "CREATE TABLE xiuxian_impart(user_id TEXT PRIMARY KEY,exp_day INTEGER,impart_lv INTEGER)"
            )
            conn.execute("INSERT INTO xiuxian_impart VALUES(%s,%s,%s)", ("u", 10**6, 3))
        self.service = ImpartTrainingSettlementService(self.game, self.impart, self.player)
        self.daily = {
            "pk_num": 7,
            "impart_num": 10,
            "exp_used": 20,
            "exp_count": 1,
            "exp_load": 10,
            "exp_gain": 30,
        }

    # -- helpers ---------------------------------------------------------
    def _stored(self, column: str):
        with db_backend.connection(self.game) as conn:
            row = conn.execute(
                f"SELECT {column} FROM user_xiuxian WHERE user_id='u'"
            ).fetchone()
        return row[0]

    def call(self, *, operation="train", expected_exp=None, gain=HUGE_GAIN, power=HUGE_POWER):
        """镜像 handler：`current_exp = int(user_info['exp'])` 作为并发令牌。"""
        if expected_exp is None:
            expected_exp = int(self._stored("exp"))
        return self.service.settle(
            operation,
            "u",
            expected_exp=expected_exp,
            expected_exp_day=10**6,
            expected_daily={
                key: self.daily[key]
                for key in ("exp_used", "exp_count", "exp_load", "exp_gain")
            },
            exp_cost=100,
            exp_gain=gain,
            exp_load_gain=5,
            power=power,
            legacy_state=self.daily,
        )

    # -- regressions -----------------------------------------------------
    def test_high_realm_training_settles_instead_of_state_changed(self) -> None:
        """int64 以上的修为不得被判成 state_changed。"""
        result = self.call()

        self.assertEqual("applied", result.status)
        self.assertGreater(result.exp, 2**63 - 1)

    def test_new_exp_keeps_full_precision_beyond_int64(self) -> None:
        expected = int(self._stored("exp"))
        result = self.call()

        self.assertEqual(expected + HUGE_GAIN, result.exp)
        self.assertGreater(result.exp, 2**63 - 1)
        # exp_gain 是当日累计（已有 30），同样不被 int64 截断
        self.assertEqual(self.daily["exp_gain"] + HUGE_GAIN, result.exp_gain)

    def test_power_is_written_without_int64_clamp(self) -> None:
        self.call(power=HUGE_POWER)

        stored = self._stored("power")
        # NUMERIC 亲和性把超大整数存成 REAL，关键是量级没有被压到 int64。
        self.assertGreater(float(stored), float(2**63 - 1))
        self.assertAlmostEqual(HUGE_POWER / float(stored), 1.0, places=12)

    def test_gain_just_above_int64_is_not_capped(self) -> None:
        result = self.call(gain=2**63)

        self.assertEqual("applied", result.status)
        self.assertEqual(self.daily["exp_gain"] + 2**63, result.exp_gain)
        self.assertGreater(result.exp_gain, 2**63)

    def test_state_changed_is_still_reported_for_a_stale_token(self) -> None:
        """校验没有被削弱：陈旧的 expected_exp 依旧 state_changed。"""
        stale = int(self._stored("exp")) - 1000

        self.assertEqual("state_changed", self.call(expected_exp=stale).status)

    def test_validation_and_gameplay_caps_still_apply(self) -> None:
        with self.assertRaises(ValueError):
            self.call(gain=0)

        self.call()
        with db_backend.connection(self.player) as conn:
            load = conn.execute(
                "SELECT exp_load FROM impart_pk_daily WHERE user_id='u'"
            ).fetchone()[0]
        self.assertLessEqual(int(load), 100)  # 承载上限仍是玩法上限


class NoStorageLimitClampTests(unittest.TestCase):
    """结算路径不得再按 SQLite 存储上限截断数值。"""

    def test_training_settlement_source_has_no_int64_clamp(self) -> None:
        source = Path(
            "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_impart_pk/transaction_service.py"
        ).read_text(encoding="utf-8")
        start = source.index("class ImpartTrainingSettlementService")
        section = source[start:]

        self.assertNotIn("SQLITE_MAX_INT", section)
        self.assertNotIn("min(SQLITE_MAX_INT", section)

    def test_training_handler_does_not_clamp_power(self) -> None:
        source = Path(
            "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_impart_pk/__init__.py"
        ).read_text(encoding="utf-8")
        handler = source[
            source.index("async def impart_pk_exp_") : source.index("async def impart_pk_go_")
        ]

        self.assertNotIn("min(SQLITE_MAX_INT", handler)
        self.assertIn("power=int(round((current_exp + exp) * level_rate * realm_rate))", handler)


if __name__ == "__main__":
    unittest.main()
