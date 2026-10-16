"""Stage 开放内容 gate: every claim path refuses inside a closed stage.

The legacy claim paths already refuse when the running stage's ``features`` do
not open a subsystem. The two feature-owned claim applications (stage and rank)
must refuse identically, so that one-click 活动领取 is consistent across all six
steps.

Written as ``unittest.TestCase`` because the CI quality job collects tests with
``python -m unittest discover``.
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_schema,
)
from nonebot_plugin_xiuxian_2.features.activity.rank_claim_application import (
    ActivityRankClaimApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.stage_claim_application import (
    ActivityStageClaimApplication,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config import (
    _runtime_feature_refusal,
)

NOW = datetime(2026, 10, 15, 12)
CLOSED = "当前阶段【正式期】不开放活动领奖"


def _config(*, features, enabled: bool = True) -> dict:
    return {
        "template_key": "spring",
        "enabled": enabled,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "extensions": {
            "stages": [
                {
                    "key": "live",
                    "name": "正式期",
                    "stage_type": "open",
                    "start_time": "0",
                    "end_time": "无限",
                    "features": list(features),
                    "multiplier": 1.0,
                }
            ]
        },
    }


class ActivityStageGateTests(unittest.TestCase):
    def _database(self) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)
        return database

    @staticmethod
    def _clock():
        return SimpleNamespace(now=lambda: NOW)

    # --- 阶段一：统一拒绝文案 -----------------------------------------

    def test_refusal_helper_matches_the_legacy_wording(self) -> None:
        open_stage = _config(features=["sign", "claim"])
        closed_stage = _config(features=["sign"])
        self.assertEqual(_runtime_feature_refusal(open_stage, "claim", "活动领奖"), "")
        self.assertEqual(
            _runtime_feature_refusal(closed_stage, "claim", "活动领奖"), CLOSED
        )
        # 旧路径标签沿用同一句式，避免同一阶段出现两套文案。
        self.assertEqual(
            _runtime_feature_refusal(closed_stage, "shop", "活动商店"),
            "当前阶段【正式期】不开放活动商店",
        )
        self.assertEqual(
            _runtime_feature_refusal(closed_stage, "exchange", "集字兑换"),
            "当前阶段【正式期】不开放集字兑换",
        )

    def test_refusal_helper_reports_the_activity_reason_when_not_open(self) -> None:
        refusal = _runtime_feature_refusal(_config(features=["sign"], enabled=False), "claim", "活动领奖")
        self.assertTrue(refusal)
        self.assertNotIn("不开放", refusal)

    # --- 阶段二：阶段领取门禁 -----------------------------------------

    def test_stage_claim_refused_when_the_stage_closes_claim(self) -> None:
        database = self._database()
        application = ActivityStageClaimApplication(
            database, config_loader=lambda: _config(features=["sign"]), clock=self._clock()
        )
        ok, text = application.claim("u1")
        self.assertFalse(ok)
        self.assertEqual(text, CLOSED)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            self.assertEqual(
                uow.query_one("SELECT count(*) AS n FROM activity_stage_claim")["n"], 0
            )

    def test_stage_claim_passes_the_gate_when_the_stage_opens_claim(self) -> None:
        database = self._database()
        application = ActivityStageClaimApplication(
            database,
            config_loader=lambda: _config(features=["sign", "claim"]),
            clock=self._clock(),
        )
        ok, text = application.claim("u1")
        self.assertFalse(ok)
        self.assertNotEqual(text, CLOSED)

    # --- 阶段二：排行领取门禁 -----------------------------------------

    def test_rank_claim_refused_when_the_stage_closes_claim(self) -> None:
        database = self._database()
        application = ActivityRankClaimApplication(
            database, config_loader=lambda: _config(features=["sign"]), clock=self._clock()
        )
        ok, text = application.claim("u1")
        self.assertFalse(ok)
        self.assertEqual(text, CLOSED)
        with DatabaseUnitOfWork(database, read_only=True) as uow:
            self.assertEqual(
                uow.query_one("SELECT count(*) AS n FROM activity_rank_claim")["n"], 0
            )

    def test_rank_claim_passes_the_gate_when_the_stage_opens_claim(self) -> None:
        database = self._database()
        application = ActivityRankClaimApplication(
            database,
            config_loader=lambda: _config(features=["sign", "claim"]),
            clock=self._clock(),
        )
        ok, text = application.claim("u1")
        self.assertFalse(ok)
        self.assertNotEqual(text, CLOSED)


if __name__ == "__main__":
    unittest.main()
