"""P1 排行榜本人名次（共享渲染 + 四个仓储的 rank_of）聚焦套件。

用真实迁移建库、真实仓储取值，钉住：并列名次、榜外玩家仍有名次、
零/无记录空态、缺库/缺表只读降级（不建表），以及六个视图都接同一渲染行。
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state
from nonebot_plugin_xiuxian_2.features.arena.state_repository import ArenaStateRepository
from nonebot_plugin_xiuxian_2.features.base.ranking_position import (
    PersonalRank,
    personal_rank_line,
)
from nonebot_plugin_xiuxian_2.features.boss.integral_repository import (
    BossIntegralSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.boss.migrations import apply_boss_player_schema
from nonebot_plugin_xiuxian_2.features.tower.migrations import apply_tower_state
from nonebot_plugin_xiuxian_2.features.tower.state_repository import TowerStateRepository
from nonebot_plugin_xiuxian_2.features.training.leaderboard_repository import (
    TrainingLeaderboardSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.training.migrations import (
    apply_training_leaderboard_indexes,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

REPO_ROOT = Path(__file__).resolve().parents[1]
PACKAGE = REPO_ROOT / "nonebot_plugin_xiuxian_2"


def _read(relative: str) -> str:
    return (PACKAGE / relative).read_text(encoding="utf-8")


def _handler_body(source: str, marker: str) -> str:
    """从 ``async def <marker>`` 起截到下一个装饰器/顶层 def，用于源合同断言。"""
    start = source.index(f"async def {marker}")
    rest = source[start + len(f"async def {marker}"):]
    match = re.search(r"\n@|\n(?=async def )|\n(?=def )", rest)
    return source[start:] if match is None else source[start: start + len(f"async def {marker}") + match.start()]


class _TempDatabaseTestCase(unittest.TestCase):
    """每个用例一个真实临时库；用后自动清理。"""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def _boss_database(self) -> Path:
        database = self.root / "boss_player.db"
        with DatabaseUnitOfWork(database) as uow:
            apply_boss_player_schema(uow)
        return database

    def _tower_database(self) -> Path:
        database = self.root / "tower_player.db"
        with DatabaseUnitOfWork(database) as uow:
            apply_tower_state(uow)
        return database

    def _arena_database(self) -> Path:
        database = self.root / "arena_player.db"
        with DatabaseUnitOfWork(database) as uow:
            apply_arena_state(uow)
        return database

    def _training_databases(self) -> tuple[Path, Path]:
        game = self.root / "training_game.db"
        player = self.root / "training_player.db"
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT)")
        with DatabaseUnitOfWork(player) as uow:
            apply_training_leaderboard_indexes(uow)
        return game, player


class PersonalRankLineTests(unittest.TestCase):
    def test_ranked_line_carries_rank_total_and_value(self):
        line = personal_rank_line(PersonalRank(3, 12, 456), label="世界积分", unit="点")

        self.assertEqual(line, "你的名次：第3名 / 共12人（世界积分 456点）")

    def test_number_formatter_is_applied_to_the_value_only(self):
        line = personal_rank_line(
            PersonalRank(1, 5, 12345),
            label="通天塔积分",
            unit="点",
            number=lambda value: f"{value:,}",
        )

        self.assertEqual(line, "你的名次：第1名 / 共5人（通天塔积分 12,345点）")

    def test_total_is_clamped_up_to_rank_so_it_never_reads_inconsistent(self):
        line = personal_rank_line(PersonalRank(4, 0, 10), label="历练积分", unit="点")

        self.assertEqual(line, "你的名次：第4名 / 共4人（历练积分 10点）")

    def test_no_record_and_missing_entry_share_the_same_sentence(self):
        for entry in (PersonalRank(0, 7, 0), None, PersonalRank()):
            with self.subTest(entry=entry):
                self.assertEqual(
                    personal_rank_line(entry, label="竞技场积分"),
                    "你还没有竞技场积分记录。",
                )

    def test_ranked_property_treats_non_positive_rank_as_unranked(self):
        self.assertFalse(PersonalRank(0, 0, 0).ranked)
        self.assertTrue(PersonalRank(1, 1, 1).ranked)


class BossIntegralRankTests(_TempDatabaseTestCase):
    def test_ties_share_a_rank_and_out_of_page_players_still_get_theirs(self):
        database = self._boss_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO boss_limit(user_id,integral) VALUES(?,?)",
                (
                    ("leader", 900),
                    ("tie-a", 500),
                    ("tie-b", 500),
                    ("viewer", 120),
                    ("tail", 10),
                ),
            )
        repository = BossIntegralSqlRepository(database)

        viewer = repository.integral_rank("viewer")
        tail = repository.integral_rank("tail")
        leader = repository.integral_rank("leader")

        self.assertEqual((viewer.rank, viewer.total, viewer.value), (4, 5, 120))
        self.assertEqual((tail.rank, tail.total, tail.value), (5, 5, 10))
        self.assertEqual((leader.rank, leader.value), (1, 900))
        self.assertEqual(repository.integral_rank("tie-a").rank, 2)

    def test_duplicate_rows_are_deduped_by_first_row_like_the_top_slice(self):
        # boss_limit 迁移带 UNIQUE(user_id)，用无约束表复现“同 user 多行取首行”口径。
        database = self.root / "dup_boss.db"
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("CREATE TABLE boss_limit(user_id TEXT,integral INTEGER)")
            uow.executemany(
                "INSERT INTO boss_limit(user_id,integral) VALUES(?,?)",
                (("viewer", 100), ("viewer", 999), ("rival", 300)),
            )

        entry = BossIntegralSqlRepository(database).integral_rank("viewer")

        self.assertEqual((entry.rank, entry.total, entry.value), (2, 2, 100))

    def test_zero_or_absent_value_is_not_a_rank(self):
        database = self._boss_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO boss_limit(user_id,integral) VALUES(?,?)",
                (("zero", 0), ("scored", 300)),
            )
        repository = BossIntegralSqlRepository(database)

        zero = repository.integral_rank("zero")
        absent = repository.integral_rank("nobody")

        self.assertEqual((zero.rank, zero.total, zero.value), (0, 1, 0))
        self.assertEqual((absent.rank, absent.total, absent.value), (0, 1, 0))

    def test_missing_database_and_schema_degrade_without_creating_anything(self):
        missing = self.root / "missing_boss.db"
        self.assertEqual(BossIntegralSqlRepository(missing).integral_rank("u"), PersonalRank())
        self.assertFalse(missing.exists())

        blank = self.root / "blank_boss.db"
        with DatabaseUnitOfWork(blank) as uow:
            uow.execute("CREATE TABLE unrelated(x INTEGER)")
        self.assertEqual(BossIntegralSqlRepository(blank).integral_rank("u"), PersonalRank())
        with DatabaseUnitOfWork(blank, read_only=True) as uow:
            tables = {
                str(row["name"])
                for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
            }
        self.assertEqual(tables, {"unrelated"})


class TowerRankTests(_TempDatabaseTestCase):
    def test_both_fields_rank_independently_with_ties(self):
        database = self._tower_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO tower(user_id,current_floor,score) VALUES(?,?,?)",
                (("a", 30, 900), ("b", 40, 500), ("viewer", 10, 500)),
            )
        repository = TowerStateRepository(database)

        floor = repository.rank_of("viewer", "current_floor")
        score = repository.rank_of("viewer", "score")

        self.assertEqual((floor.rank, floor.total, floor.value), (3, 3, 10))
        self.assertEqual((score.rank, score.total, score.value), (2, 3, 500))
        self.assertEqual(repository.rank_of("b", "score").rank, 2, "同分并列同一名次")

    def test_unsupported_field_is_rejected(self):
        database = self._tower_database()

        with self.assertRaises(ValueError):
            TowerStateRepository(database).rank_of("viewer", "weekly_purchases")

    def test_missing_database_and_table_degrade_without_ddl(self):
        missing = self.root / "missing_tower.db"
        self.assertEqual(
            TowerStateRepository(missing).rank_of("u", "current_floor"), PersonalRank()
        )
        self.assertFalse(missing.exists())

        blank = self.root / "blank_tower.db"
        with DatabaseUnitOfWork(blank) as uow:
            uow.execute("CREATE TABLE tower(user_id TEXT)")
        self.assertEqual(TowerStateRepository(blank).rank_of("u", "current_floor"), PersonalRank())


class ArenaRankTests(_TempDatabaseTestCase):
    def test_ties_share_a_rank_and_out_of_page_player_still_gets_theirs(self):
        database = self._arena_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO arena(user_id,score) VALUES(?,?)",
                (("leader", 3200), ("tie-a", 1900), ("tie-b", 1900), ("viewer", 1200)),
            )

        entry = ArenaStateRepository(database).rank_of("viewer")
        tied = ArenaStateRepository(database).rank_of("tie-b")

        self.assertEqual((entry.rank, entry.total, entry.value), (4, 4, 1200))
        self.assertEqual((tied.rank, tied.total, tied.value), (2, 4, 1900))

    def test_zero_score_has_no_rank_but_still_counts_the_rated_field(self):
        database = self._arena_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO arena(user_id,score) VALUES(?,?)",
                (("zero", 0), ("scored", 1500)),
            )

        entry = ArenaStateRepository(database).rank_of("zero")

        self.assertEqual((entry.rank, entry.total, entry.value), (0, 1, 0))

    def test_missing_database_and_table_degrade_without_ddl(self):
        missing = self.root / "missing_arena.db"
        self.assertEqual(ArenaStateRepository(missing).rank_of("u"), PersonalRank())
        self.assertFalse(missing.exists())

        blank = self.root / "blank_arena.db"
        with DatabaseUnitOfWork(blank) as uow:
            uow.execute("CREATE TABLE arena(user_id TEXT)")
        self.assertEqual(ArenaStateRepository(blank).rank_of("u"), PersonalRank())


class TrainingRankTests(_TempDatabaseTestCase):
    def test_both_fields_rank_independently_with_ties(self):
        game, player = self._training_databases()
        with DatabaseUnitOfWork(player) as uow:
            uow.executemany(
                "INSERT INTO training(user_id,completed,points) VALUES(?,?,?)",
                (("a", 12, 900), ("b", 6, 500), ("viewer", 4, 500)),
            )
        repository = TrainingLeaderboardSqlRepository(game, player)

        completed = repository.rank_of("completed", "viewer")
        points = repository.rank_of("points", "viewer")

        self.assertEqual((completed.rank, completed.total, completed.value), (3, 3, 4))
        self.assertEqual((points.rank, points.total, points.value), (2, 3, 500))

    def test_unsupported_field_is_rejected(self):
        game, player = self._training_databases()

        with self.assertRaises(ValueError):
            TrainingLeaderboardSqlRepository(game, player).rank_of("progress", "viewer")

    def test_missing_database_and_table_degrade_without_ddl(self):
        game, player = self._training_databases()
        missing = self.root / "missing_training.db"
        self.assertEqual(
            TrainingLeaderboardSqlRepository(game, missing).rank_of("points", "u"),
            PersonalRank(),
        )
        self.assertFalse(missing.exists())

        blank = self.root / "blank_training.db"
        with DatabaseUnitOfWork(blank) as uow:
            uow.execute("CREATE TABLE training(user_id TEXT)")
        self.assertEqual(
            TrainingLeaderboardSqlRepository(game, blank).rank_of("points", "u"),
            PersonalRank(),
        )
        self.assertTrue(player.is_file())


class RankingViewWiringTests(unittest.TestCase):
    CASES = (
        ("xiuxian/xiuxian_boss/__init__.py", "boss_integral_rank_", "世界BOSS积分排行榜"),
        ("xiuxian/xiuxian_tower/__init__.py", "tower_rank_", "通天塔排行榜"),
        ("xiuxian/xiuxian_tower/__init__.py", "tower_integral_rank_", "通天塔积分排行榜"),
        ("xiuxian/xiuxian_training/__init__.py", "_training_rank_message", "历练排行榜"),
        ("xiuxian/xiuxian_arena/__init__.py", "arena_ranking_", "竞技场排行榜"),
    )

    def test_every_ranking_view_renders_the_shared_personal_line(self):
        for relative, marker, title in self.CASES:
            with self.subTest(view=title):
                source = _read(relative)
                if marker == "_training_rank_message":
                    body = source[source.index("def _training_rank_message"):]
                else:
                    body = _handler_body(source, marker)
                self.assertIn("personal_rank_line(", body)
                self.assertIn("ranking_position import personal_rank_line", source)

    def test_training_rank_message_still_starts_with_the_top_slice(self):
        source = _read("xiuxian/xiuxian_training/__init__.py")
        body = source[source.index("def _training_rank_message"):]

        self.assertIn("第{index}位 | ", body)
        self.assertIn("enumerate(rows, start=1)", body)
        self.assertIn("if viewer_id:", body, "无 viewer 时必须保持旧输出（可被旧调用方复用）")

    def test_handlers_pass_the_session_user_into_the_personal_line(self):
        boss = _handler_body(_read("xiuxian/xiuxian_boss/__init__.py"), "boss_integral_rank_")
        tower = _read("xiuxian/xiuxian_tower/__init__.py")
        arena = _handler_body(_read("xiuxian/xiuxian_arena/__init__.py"), "arena_ranking_")

        self.assertIn('viewer_id = str(user_info["user_id"])', boss)
        self.assertIn('viewer_id = str(user_info["user_id"])', tower)
        self.assertIn("viewer_id = str(event.get_user_id())", arena)

    def test_views_keep_their_existing_top_slice_calls(self):
        self.assertIn(
            "boss_integral_application.top_integrals(50)",
            _read("xiuxian/xiuxian_boss/__init__.py"),
        )
        tower = _read("xiuxian/xiuxian_tower/__init__.py")
        self.assertIn('tower_limit.ranking("current_floor")', tower)
        self.assertIn('tower_limit.ranking("score")', tower)
        self.assertIn(
            "arena_limit.get_arena_ranking(limit=10)", _read("xiuxian/xiuxian_arena/__init__.py")
        )
        self.assertIn(
            "training_application.leaderboard(field, limit=50)",
            _read("xiuxian/xiuxian_training/__init__.py"),
        )

    def test_arena_facade_delegates_rank_of_to_the_feature_application(self):
        source = _read("xiuxian/xiuxian_arena/arena_limit.py")

        self.assertIn("def rank_of(self, user_id):", source)
        self.assertIn("return state_application.rank_of(user_id)", source)

    def test_tower_facade_delegates_rank_of_to_the_feature_application(self):
        source = _read("xiuxian/xiuxian_tower/tower_limit.py")

        self.assertIn('def rank_of(self, user_id, field: str = "current_floor"):', source)
        self.assertIn("return state_application.rank_of(user_id, field)", source)


if __name__ == "__main__":
    unittest.main()
