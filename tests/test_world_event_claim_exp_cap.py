"""魔修入侵领奖：修为不得越过本境上限，回执与回放都返回实际入账值。

`WorldEventClaimSqlRepository.claim` 曾直接 `exp = exp + 奖励值`，而悬赏令结算、
宗门任务、历练与闭关都在 `set_closing_type(level) × closing_exp_upper_limit`
处截断。本套件驱动真实仓储（真实建表形状 + 真实 app/player 双库附加），钉住：
低于上限原值入账；贴近上限按余量截断；已满则修为 0、灵石与物品照发；回执存实际值；
同一 operation 回放返回实际值。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.world_events import migrations as world_events_migrations
from nonebot_plugin_xiuxian_2.features.world_events.repository import (
    WorldEventClaimSqlRepository,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from tests.test_db_backend import db_backend


class Triple:
    """game 库 + player 库，表形状取真实迁移（world_events 启动迁移）。"""

    def __init__(self, root: Path) -> None:
        self.game = root / "game.db"
        self.player = root / "player.db"
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            world_events_migrations.apply_world_events_claim(uow)
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            world_events_migrations.apply_world_events_player(uow)
            world_events_migrations.apply_world_events_claim_statistics(uow)
        with db_backend.transaction(self.game) as conn:
            conn.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)"
            )
            conn.execute(
                "CREATE TABLE back (user_id TEXT, goods_id INTEGER, goods_name TEXT, "
                "goods_type TEXT, goods_num INTEGER, create_time TEXT, update_time TEXT, "
                "bind_num INTEGER, PRIMARY KEY(user_id, goods_id))"
            )
            conn.execute("INSERT INTO user_xiuxian VALUES ('u', 1000, 0)")
        with db_backend.transaction(self.player) as conn:
            conn.execute(
                "INSERT INTO world_event_state(user_id, event_id, claimed) VALUES ('demon','evt','{}')"
            )

    def set_exp(self, value: int) -> None:
        with db_backend.transaction(self.game) as conn:
            conn.execute("UPDATE user_xiuxian SET exp=? WHERE user_id='u'", (value,))

    def exp(self) -> int:
        with db_backend.connection(self.game) as conn:
            return int(conn.execute("SELECT exp FROM user_xiuxian WHERE user_id='u'").fetchone()[0])

    def receipt(self, operation_id: str) -> tuple[int, int]:
        with db_backend.connection(self.game) as conn:
            row = conn.execute(
                "SELECT stone, exp FROM demon_claim_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return (int(row[0]), int(row[1]))

    def item_count(self, goods_id: int) -> int:
        with db_backend.connection(self.game) as conn:
            row = conn.execute(
                "SELECT goods_num FROM back WHERE user_id='u' AND goods_id=?", (goods_id,)
            ).fetchone()
        return int(row[0]) if row else 0


CLAIM_ITEMS = ({"id": 20010, "name": "灵签宝箓", "type": "特殊物品", "amount": 2},)


class DemonClaimExpCapTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.triple = Triple(Path(self._tmp.name))
        self.repo = WorldEventClaimSqlRepository(self.triple.game, self.triple.player)

    def claim(self, op: str, exp: int, max_exp: int | None, *, claimed=None, event="evt"):
        return self.repo.claim(
            op,
            "demon",
            event,
            "u",
            {} if claimed is None else claimed,
            500,
            exp,
            CLAIM_ITEMS,
            9999,
            max_exp=max_exp,
        )

    def test_below_cap_grants_full_reward(self) -> None:
        self.triple.set_exp(1_000)
        result = self.claim("op1", 400, 10_000)
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.exp, 400)
        self.assertEqual(self.triple.exp(), 1_400)
        self.assertEqual(self.triple.receipt("op1"), (500, 400))
        self.assertEqual(self.triple.item_count(20010), 2)

    def test_near_cap_grants_headroom_only(self) -> None:
        self.triple.set_exp(9_700)
        result = self.claim("op2", 400, 10_000)
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.exp, 300)
        self.assertEqual(self.triple.exp(), 10_000)
        # 灵石与物品不受修为上限影响
        self.assertEqual(result.stone, 500)
        self.assertEqual(self.triple.item_count(20010), 2)

    def test_at_cap_grants_zero_exp_but_other_rewards(self) -> None:
        self.triple.set_exp(10_000)
        result = self.claim("op3", 400, 10_000)
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.exp, 0)
        self.assertEqual(self.triple.exp(), 10_000)
        self.assertEqual(result.stone, 500)
        self.assertEqual(self.triple.receipt("op3"), (500, 0))

    def test_replay_returns_applied_exp_not_requested(self) -> None:
        self.triple.set_exp(9_900)
        first = self.claim("op4", 400, 10_000)
        self.assertEqual(first.exp, 100)
        replay = self.claim("op4", 400, 10_000)
        self.assertEqual(replay.status, "duplicate")
        self.assertEqual(replay.exp, 100)

    def test_without_max_exp_keeps_historical_behavior(self) -> None:
        self.triple.set_exp(10_000)
        result = self.claim("op5", 400, None)
        self.assertEqual(result.exp, 400)
        self.assertEqual(self.triple.exp(), 10_400)

    def test_oversized_values_stay_bindable(self) -> None:
        huge = 10**20
        self.triple.set_exp(0)
        result = self.claim("op6", huge, huge * 2)
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.exp, huge)
        self.assertEqual(self.triple.receipt("op6")[1], huge)


if __name__ == "__main__":
    unittest.main()
