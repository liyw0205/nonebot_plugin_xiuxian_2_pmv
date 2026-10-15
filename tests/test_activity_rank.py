"""Activity rank feature: config, board queries, claim, views, help.

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
from nonebot_plugin_xiuxian_2.features.activity.rank import (
    board_by_query,
    board_labels,
    position_text,
    rewards_for_board,
    tier_for_position,
    value_text,
)
from nonebot_plugin_xiuxian_2.features.activity.rank_claim_application import (
    ActivityRankClaimApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.rank_claim_repository import (
    ActivityRankClaimRepository,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_application import (
    ActivityReadModelApplication,
)
from nonebot_plugin_xiuxian_2.features.activity.read_model_repository import (
    ActivityReadModelSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity.activity_config import (
    DEFAULT_RANK_BOARDS,
    activity_rank_config,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.message_markdown import (
    strip_md_command_links,
)

NOW = datetime(2026, 10, 15, 12)
ROOT = Path(__file__).resolve().parents[1]
ACTIVITY_MODULE = (
    ROOT / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_activity" / "__init__.py"
)

BOARDS = [
    {"key": "sign", "label": "签到榜", "source": "sign", "limit": 10},
    {"key": "points", "label": "积分榜", "source": "points", "limit": 2},
]
REWARDS = [
    {
        "board": "points",
        "rank_min": 1,
        "rank_max": 1,
        "key": "champion",
        "name": "榜首",
        "reward": "灵石x1000",
    },
    {
        "board": "points",
        "rank_min": 2,
        "rank_max": 3,
        "key": "top3",
        "name": "前三",
        "reward": "灵石x500",
    },
]


def _config(rank=None, *, enabled: bool = True) -> dict:
    extensions: dict = {}
    if rank is not None:
        extensions["rank"] = rank
    return {
        "template_key": "spring",
        "enabled": enabled,
        "name": "春日庆典",
        "start_time": "0",
        "end_time": "无限",
        "extensions": extensions,
    }


class ActivityRankTests(unittest.TestCase):
    def _database(self, *, with_names: bool = False) -> Path:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        database = Path(tmp.name) / "game.db"
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            apply_activity_state_schema(uow)
            if with_names:
                uow.execute(
                    "CREATE TABLE IF NOT EXISTS user_xiuxian "
                    "(user_id TEXT PRIMARY KEY,user_name TEXT)"
                )
        return database

    @staticmethod
    def _seed_points(database: Path, rows) -> None:
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            for user_id, points, total in rows:
                uow.execute(
                    "INSERT INTO activity_point_balance"
                    "(activity_key,user_id,points,total_points,update_time) "
                    "VALUES('spring',?,?,?,'')",
                    (user_id, points, total),
                )

    # --- 阶段一：配置与数据 ---------------------------------------------

    def test_rank_config_defaults_to_sign_board_without_rewards(self) -> None:
        rank = activity_rank_config(_config())
        self.assertEqual(rank["boards"], DEFAULT_RANK_BOARDS)
        self.assertEqual(rank["rewards"], [])

    def test_rank_config_normalizes_boards_and_rewards(self) -> None:
        rank = activity_rank_config(
            _config(
                {
                    "boards": [
                        {"source": "points", "limit": 0},
                        {"source": "points"},
                        {"source": "unknown"},
                        "not-a-row",
                        {
                            "key": "collect_key",
                            "source": "collect",
                            "label": "字牌榜",
                            "limit": 500,
                        },
                    ],
                    "rewards": [
                        {"board": "unknown", "rank_min": 1, "reward": "灵石x1"},
                        {"board": "points", "rank_min": 2, "rank_max": 1, "reward": "灵石x2"},
                        {"board": "points", "rank_min": 2, "rank_max": 1, "reward": "灵石x9"},
                        {"board": "points", "rank_min": 5, "reward": ""},
                        {
                            "board": "points",
                            "rank_min": 5,
                            "rank_max": 5,
                            "name": "第五",
                            "reward": "灵石x5",
                        },
                    ],
                }
            )
        )
        self.assertEqual(
            rank["boards"],
            [
                {"key": "points", "label": "积分榜", "source": "points", "limit": 1},
                {
                    "key": "collect_key",
                    "label": "字牌榜",
                    "source": "collect",
                    "limit": 100,
                },
            ],
        )
        self.assertEqual(
            rank["rewards"],
            [
                {
                    "board": "points",
                    "rank_min": 2,
                    "rank_max": 2,
                    "key": "points_2_2",
                    "name": "第2-2名",
                    "reward": "灵石x2",
                },
                {
                    "board": "points",
                    "rank_min": 5,
                    "rank_max": 5,
                    "key": "points_5_5",
                    "name": "第五",
                    "reward": "灵石x5",
                },
            ],
        )

    # --- 阶段二：榜单数据层（纯逻辑） -----------------------------------

    def test_board_resolution_and_tier_matching(self) -> None:
        boards = activity_rank_config(_config({"boards": BOARDS}))["boards"]
        self.assertEqual(board_by_query(boards, "积分榜")["source"], "points")
        self.assertEqual(board_by_query(boards, "points")["key"], "points")
        self.assertEqual(board_by_query(boards, "sign")["key"], "sign")
        self.assertIsNone(board_by_query(boards, "不存在"))
        self.assertIsNone(board_by_query(boards, ""))
        self.assertEqual(board_labels(boards), "签到榜、积分榜")

        tiers = rewards_for_board(REWARDS, {"key": "points", "source": "points"})
        self.assertEqual([tier["key"] for tier in tiers], ["champion", "top3"])
        self.assertEqual(
            rewards_for_board(REWARDS, {"key": "sign", "source": "sign"}), []
        )
        self.assertEqual(tier_for_position(tiers, 1)["key"], "champion")
        self.assertEqual(tier_for_position(tiers, 3)["key"], "top3")
        self.assertIsNone(tier_for_position(tiers, 4))
        self.assertIsNone(tier_for_position(tiers, 0))

        self.assertEqual(value_text("points", 12), "累计积分 12分")
        self.assertEqual(position_text(2, 10), "第 2 名 / 共 10 人")
        self.assertEqual(position_text(0, 10), "尚未上榜")

    def test_board_rank_position_and_total(self) -> None:
        database = self._database(with_names=True)
        self._seed_points(database, [("u1", 5, 100), ("u2", 40, 40), ("u3", 10, 60)])
        repository = ActivityReadModelSqlRepository(database)

        rows = repository.board_rank("points", "spring", 10)
        self.assertEqual([row["user_id"] for row in rows], ["u1", "u3", "u2"])
        self.assertEqual(rows[0]["value"], 100)
        self.assertEqual(repository.board_rank("points", "spring", 2), rows[:2])
        self.assertEqual(repository.board_position("points", "spring", "u3"), (2, 60))
        self.assertEqual(repository.board_position("points", "spring", "u9"), (0, 0))
        self.assertEqual(repository.board_total("points", "spring"), 3)

        # 集字榜按字牌总数排名。
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            for char, count in (("春", 2), ("节", 3)):
                uow.execute(
                    "INSERT INTO activity_collect_inventory"
                    "(activity_key,user_id,word_char,count,update_time) "
                    "VALUES('spring','u2',?,?,'')",
                    (char, count),
                )
            uow.execute(
                "INSERT INTO activity_collect_inventory"
                "(activity_key,user_id,word_char,count,update_time) "
                "VALUES('spring','u1','春',1,'')"
            )
        self.assertEqual(
            repository.board_rank("collect", "spring", 10)[0]["user_id"], "u2"
        )
        self.assertEqual(repository.board_position("collect", "spring", "u2"), (1, 5))
        self.assertEqual(repository.board_total("collect", "spring"), 2)

        # 签到榜同分时早签在前。
        with DatabaseUnitOfWork(database, immediate=True) as uow:
            uow.execute(
                "INSERT INTO activity_user"
                "(user_id,sign_days,total_sign_days,last_sign_date) "
                "VALUES('u1',3,9,'2026-10-01')"
            )
            uow.execute(
                "INSERT INTO activity_user"
                "(user_id,sign_days,total_sign_days,last_sign_date) "
                "VALUES('u2',3,9,'2026-09-01')"
            )
            uow.execute(
                "INSERT INTO user_xiuxian(user_id,user_name) VALUES('u1','甲')"
            )
            uow.execute(
                "INSERT INTO user_xiuxian(user_id,user_name) VALUES('u2','乙')"
            )
        self.assertEqual(
            [row["user_id"] for row in repository.board_rank("sign", "spring", 10)],
            ["u2", "u1"],
        )
        self.assertEqual(repository.board_position("sign", "spring", "u1"), (2, 3))
        self.assertEqual(repository.board_total("sign", "spring"), 2)
        self.assertEqual(repository.board_rank("sign", "spring", 10)[0]["user_name"], "乙")

        # 空表与未知榜单不报错。
        self.assertEqual(repository.board_rank("pass", "spring", 10), [])
        self.assertEqual(repository.board_position("pass", "spring", "u1"), (0, 0))
        self.assertEqual(repository.board_total("pass", "spring"), 0)
        self.assertEqual(repository.board_rank("nope", "spring", 10), [])
        self.assertEqual(repository.board_rank("points", "spring", 0), [])

    # --- 阶段四：结算与领取 ---------------------------------------------

    def test_claim_delivers_once_and_repeat_is_a_noop(self) -> None:
        database = self._database()
        self._seed_points(database, [("u1", 5, 100), ("u2", 40, 40)])
        config = _config({"boards": BOARDS, "rewards": REWARDS})
        sent: list[tuple[str, str]] = []
        app = ActivityRankClaimApplication(
            database,
            clock=SimpleNamespace(now=lambda: NOW),
            config_loader=lambda: config,
            reward_sender=lambda uid, text: sent.append((uid, text)) or ["发放成功"],
        )
        ok, text = app.claim("u1")
        self.assertTrue(ok, text)
        self.assertIn("活动排行奖励领取成功", text)
        self.assertIn("积分榜｜榜首", text)
        self.assertEqual(sent, [("u1", "灵石x1000")])
        self.assertEqual(
            ActivityRankClaimRepository(database).claimed_tiers("u1", "spring"),
            {("points", "champion")},
        )
        # 重复领取不重复发放。
        ok, text = app.claim("u1")
        self.assertFalse(ok)
        self.assertIn("排行奖励已领取", text)
        self.assertEqual(len(sent), 1)

    def test_claim_failure_boundaries(self) -> None:
        database = self._database()
        config = _config({"boards": BOARDS, "rewards": REWARDS})

        def app_for(cfg, *, sender=None):
            return ActivityRankClaimApplication(
                database,
                clock=SimpleNamespace(now=lambda: NOW),
                config_loader=lambda: cfg,
                reward_sender=sender or (lambda uid, text: ["ok"]),
            )

        # 活动未开启。
        ok, text = app_for(
            _config({"boards": BOARDS, "rewards": REWARDS}, enabled=False)
        ).claim("u1")
        self.assertFalse(ok)
        self.assertIn("活动未开启", text)
        # 未配置排行奖励。
        ok, text = app_for(_config({"boards": BOARDS})).claim("u1")
        self.assertFalse(ok)
        self.assertEqual(text, "当前活动未配置排行奖励")
        # 未知榜单。
        ok, text = app_for(config).claim("u1", query="不存在")
        self.assertFalse(ok)
        self.assertIn("未找到榜单【不存在】", text)
        self.assertIn("签到榜、积分榜", text)
        # 未上榜。
        ok, text = app_for(config).claim("u1")
        self.assertFalse(ok)
        self.assertIn("你尚未上榜", text)
        # 上榜但未达到奖励名次（只有榜首与前三档）。
        self._seed_points(
            database, [("u1", 5, 100), ("u2", 40, 80), ("u3", 30, 60), ("u4", 20, 40)]
        )
        ok, text = app_for(config).claim("u4")
        self.assertFalse(ok)
        self.assertIn("未达到排行奖励名次", text)
        # 榜单没有配置档位。
        ok, text = app_for(config).claim("u1", query="签到榜")
        self.assertFalse(ok)
        self.assertIn("该榜单未配置排行奖励", text)

    # --- 阶段三/五：玩家入口、呈现与帮助 --------------------------------

    def test_rank_views_report_boards_and_position(self) -> None:
        database = self._database()
        self._seed_points(database, [("u1", 5, 100), ("u2", 40, 40)])
        config = _config({"boards": BOARDS, "rewards": REWARDS})
        app = ActivityReadModelApplication(
            database,
            config_loader=lambda: config,
            clock=SimpleNamespace(now=lambda: NOW),
        )
        overview = app.rank_overview_text("u1")
        self.assertIn("排行榜", overview)
        self.assertIn("积分榜：第 1 名 / 共 2 人（累计积分 100分）", overview)
        self.assertIn("活动排行领取", overview)

        board = app.rank_board_text("u1", "积分榜")
        self.assertIn("【春日庆典 · 积分榜】", board)
        self.assertIn("我的名次：第 1 名 / 共 2 人（累计积分 100分）", board)
        self.assertIn("可领取：榜首（灵石x1000）", board)
        self.assertIn("活动排行领取", board)

        unknown = app.rank_board_text("u1", "不存在")
        self.assertIn("未找到榜单【不存在】", unknown)

        # 未上榜时给出确定文案。
        self.assertIn("尚未上榜", app.rank_board_text("u9", "积分榜"))

        # 默认配置只保留签到榜且无奖励。
        default_app = ActivityReadModelApplication(
            database,
            config_loader=lambda: _config(),
            clock=SimpleNamespace(now=lambda: NOW),
        )
        default_overview = default_app.rank_overview_text("u1")
        self.assertIn("签到榜", default_overview)
        self.assertNotIn("活动排行领取", default_overview)

        # 纯文本降级不泄漏 markdown 或蓝字 URL。
        plain = strip_md_command_links(board)
        self.assertNotIn("mqqapi", plain)
        self.assertNotIn("**", plain)

    def test_help_and_command_wiring_document_rank_commands(self) -> None:
        source = ACTIVITY_MODULE.read_text(encoding="utf-8")
        start = source.index('ACTIVITY_HELP = """') + len('ACTIVITY_HELP = """')
        help_text = source[start : source.index('"""', start)]
        self.assertIn("活动排行", help_text)
        self.assertIn("活动排行领取", help_text)
        self.assertIn("领取排行奖励", help_text)

        self.assertIn(
            'on_command("活动排行领取", aliases={"领取排行奖励"}', source
        )
        self.assertIn('"claim_rank_rewards"', source)
        self.assertIn("rank_board_text", source)
        self.assertIn("rank_overview_text", source)


if __name__ == "__main__":
    unittest.main()
