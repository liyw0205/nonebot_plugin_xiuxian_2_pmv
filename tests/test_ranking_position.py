"""P1 排行榜本人名次（共享渲染 + 四个仓储的 rank_of）聚焦套件。

用真实迁移建库、真实仓储取值，钉住：并列名次、榜外玩家仍有名次、
零/无记录空态、缺库/缺表只读降级（不建表），以及六个视图都接同一渲染行。
"""

from __future__ import annotations

import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.arena import state_repository as arena_state_repository
from nonebot_plugin_xiuxian_2.features.arena.migrations import apply_arena_state
from nonebot_plugin_xiuxian_2.features.arena.state_application import ArenaStateApplication
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
from nonebot_plugin_xiuxian_2.features.tower.state_application import TowerStateApplication
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
    def test_public_top_slice_and_personal_rank_are_both_live(self):
        database = self._boss_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO boss_limit(user_id,integral) VALUES(?,?)",
                (("u1", 100), ("u2", 80)),
            )
        repository = BossIntegralSqlRepository(database)

        first = repository.top_integrals(2)
        self.assertEqual(["u1", "u2"], [row[0] for row in first])
        self.assertEqual(first, repository.top_integrals(2))
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("UPDATE boss_limit SET integral=120 WHERE user_id='u2'")
        self.assertEqual("u2", repository.top_integrals(2)[0][0])
        rank = repository.integral_rank("u2")
        self.assertEqual((1, 2, 120), (rank.rank, rank.total, rank.value))

    def test_public_snapshot_is_stale_by_design_but_personal_rank_is_live(self):
        from nonebot_plugin_xiuxian_2.features.base.public_display_snapshot import (
            display_snapshot_freshness,
        )
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
            process_read_cache,
        )

        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        database = self._boss_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO boss_limit(user_id,integral) VALUES(?,?)",
                (("leader", 100), ("viewer", 50)),
            )
        repository = BossIntegralSqlRepository(database)
        first = repository.top_integrals_snapshot(2)
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("UPDATE boss_limit SET integral=120 WHERE user_id='viewer'")

        cached = repository.top_integrals_snapshot(2)
        self.assertEqual(first, cached)
        self.assertEqual(1, repository.integral_rank("viewer").rank)
        self.assertIn("最长缓存 60 分钟", display_snapshot_freshness(cached))
        self.assertIn("个人名次与资格按实时数据计算", display_snapshot_freshness(cached))

        process_read_cache.invalidate_snapshot_database(database)
        self.assertEqual("viewer", repository.top_integrals_snapshot(2)["rows"][0][0])


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
    def test_public_top_slice_is_live_with_viewer_rank(self):
        database = self._tower_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO tower(user_id,current_floor,max_floor,score,weekly_purchases) "
                "VALUES(?,?,0,?,'{}')",
                (("u1", 12, 20), ("u2", 10, 30)),
            )
        application = TowerStateApplication(database)

        first = application.ranking("current_floor", 2)
        self.assertEqual(["u1", "u2"], [row[0] for row in first])
        self.assertEqual(first, application.ranking("current_floor", 2))
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("UPDATE tower SET current_floor=15 WHERE user_id='u2'")
        self.assertEqual("u2", application.ranking("current_floor", 2)[0][0])
        self.assertEqual(1, application.rank_of("u2", "current_floor").rank)

    def test_public_snapshot_does_not_replace_the_live_viewer_rank(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
            process_read_cache,
        )

        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        database = self._tower_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO tower(user_id,current_floor,score) VALUES(?,?,?)",
                (("leader", 20, 100), ("viewer", 10, 50)),
            )
        application = TowerStateApplication(database)
        original_execute = DatabaseUnitOfWork.execute
        schema_probes = []

        def count_schema_probes(uow, sql, params=()):
            if 'table_info("tower")' in sql:
                schema_probes.append(sql)
            return original_execute(uow, sql, params)

        with patch.object(
            DatabaseUnitOfWork,
            "execute",
            autospec=True,
            side_effect=count_schema_probes,
        ):
            first, first_rank = application.ranking_snapshot_with_rank(
                "viewer", "current_floor", 2
            )
        self.assertEqual(1, len(schema_probes))
        self.assertEqual(PersonalRank(2, 2, 10), first_rank)
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("UPDATE tower SET current_floor=25 WHERE user_id='viewer'")

        schema_probes.clear()
        with patch.object(
            DatabaseUnitOfWork,
            "execute",
            autospec=True,
            side_effect=count_schema_probes,
        ):
            cached, live_rank = application.ranking_snapshot_with_rank(
                "viewer", "current_floor", 2
            )
        self.assertEqual(0, len(schema_probes))
        self.assertEqual(first, cached)
        self.assertEqual(PersonalRank(1, 2, 25), live_rank)
        process_read_cache.invalidate_snapshot_database(database)
        self.assertEqual(
            "viewer", application.ranking_snapshot("current_floor", 2)["rows"][0][0]
        )

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
    def test_public_top_slice_is_live_with_personal_rank(self):
        database = self._arena_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO arena(user_id,score) VALUES(?,?)",
                (("u1", 100), ("u2", 80)),
            )
        application = ArenaStateApplication(database)

        first = application.ranking(2)
        self.assertEqual(["u1", "u2"], [row[0] for row in first])
        self.assertEqual(first, application.ranking(2))
        with DatabaseUnitOfWork(database) as uow:
            uow.execute("UPDATE arena SET score=120 WHERE user_id='u2'")
        self.assertEqual("u2", application.ranking(2)[0][0])
        self.assertEqual(1, application.rank_of("u2").rank)

    def test_top_slice_and_exact_viewer_rank_share_one_schema_snapshot(self):
        database = self._arena_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO arena(user_id,score) VALUES(?,?)",
                (("leader", 3200), ("tie-a", 1900), ("tie-b", 1900), ("viewer", 1200)),
            )
        application = ArenaStateApplication(database)
        with patch(
            "nonebot_plugin_xiuxian_2.features.arena.state_repository.table_ready",
            wraps=arena_state_repository.table_ready,
        ) as schema_check:
            rows, rank = application.ranking_with_rank(2, "viewer")

        self.assertEqual(["leader", "tie-a"], [user_id for user_id, _score in rows])
        self.assertEqual(PersonalRank(4, 4, 1200), rank)
        schema_check.assert_called_once()

    def test_combined_snapshot_load_shares_schema_read_and_keeps_viewer_rank_live(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
            process_read_cache,
        )

        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        database = self._boss_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO boss_limit(user_id,integral) VALUES(?,?)",
                (("leader", 100), ("viewer", 50)),
            )
        repository = BossIntegralSqlRepository(database)

        with (
            patch.object(repository, "_ready", wraps=repository._ready) as ready,
            patch.object(
                repository, "_top_integrals_in_uow",
                wraps=repository._top_integrals_in_uow,
            ) as read_top,
        ):
            first, first_rank = repository.top_integrals_snapshot_with_rank(2, "viewer")
        self.assertEqual(1, ready.call_count)
        read_top.assert_called_once()
        self.assertEqual((2, 2, 50), (first_rank.rank, first_rank.total, first_rank.value))

        with DatabaseUnitOfWork(database) as uow:
            uow.execute("UPDATE boss_limit SET integral=120 WHERE user_id='viewer'")
        with (
            patch.object(repository, "_ready", wraps=repository._ready) as ready,
            patch.object(
                repository, "_top_integrals_in_uow",
                wraps=repository._top_integrals_in_uow,
            ) as read_top,
        ):
            cached, live_rank = repository.top_integrals_snapshot_with_rank(2, "viewer")

        self.assertEqual(1, ready.call_count)
        self.assertEqual(first, cached)
        self.assertEqual((1, 2, 120), (live_rank.rank, live_rank.total, live_rank.value))
        read_top.assert_not_called()

    def test_ties_share_a_rank_and_out_of_page_player_still_gets_theirs(self):
        database = self._arena_database()
        with DatabaseUnitOfWork(database) as uow:
            uow.executemany(
                "INSERT INTO arena(user_id,score) VALUES(?,?)",
                (("leader", 3200), ("tie-a", 1900), ("tie-b", 1900), ("viewer", 1200)),
            )

        entry = ArenaStateRepository(database).rank_of("viewer")
        tied = ArenaStateRepository(database).rank_of("tie-b")
        rows, combined_entry = ArenaStateRepository(database).ranking_with_rank(2, "viewer")

        self.assertEqual((entry.rank, entry.total, entry.value), (4, 4, 1200))
        self.assertEqual((tied.rank, tied.total, tied.value), (2, 4, 1900))
        self.assertEqual(["leader", "tie-a"], [user_id for user_id, _score in rows])
        self.assertEqual(entry, combined_entry)

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
    def test_public_top_slice_and_names_are_live_with_viewer_rank(self):
        game, player = self._training_databases()
        with DatabaseUnitOfWork(game) as uow:
            uow.executemany(
                "INSERT INTO user_xiuxian(user_id,user_name) VALUES(?,?)",
                (("u1", "道友一"), ("u2", "道友二")),
            )
        with DatabaseUnitOfWork(player) as uow:
            uow.executemany(
                "INSERT INTO training(user_id,points,completed) VALUES(?,?,?)",
                (("u1", 100, 5), ("u2", 80, 4)),
            )
        repository = TrainingLeaderboardSqlRepository(game, player)

        first = repository.top("points")
        self.assertEqual(["u1", "u2"], [row["user_id"] for row in first])
        self.assertEqual(first, repository.top("points"))
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("UPDATE user_xiuxian SET user_name='新道号' WHERE user_id='u2'")
        with DatabaseUnitOfWork(player) as uow:
            uow.execute("UPDATE training SET points=120 WHERE user_id='u2'")
        refreshed = repository.top("points")
        self.assertEqual(["u2", "u1"], [row["user_id"] for row in refreshed])
        self.assertEqual("新道号", refreshed[0]["user_name"])
        self.assertEqual(1, repository.rank_of("points", "u2").rank)

    def test_public_snapshot_tracks_both_databases_and_personal_rank_stays_live(self):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import (
            process_read_cache,
        )

        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        game, player = self._training_databases()
        with DatabaseUnitOfWork(game) as uow:
            uow.execute("INSERT INTO user_xiuxian(user_id,user_name) VALUES('u','旧名')")
        with DatabaseUnitOfWork(player) as uow:
            uow.execute("INSERT INTO training(user_id,points,completed) VALUES('u',10,1)")
            uow.execute("INSERT INTO training(user_id,points,completed) VALUES('v',20,2)")
        repository = TrainingLeaderboardSqlRepository(game, player)
        first = repository.top_snapshot("points")

        with DatabaseUnitOfWork(game) as uow:
            uow.execute("UPDATE user_xiuxian SET user_name='新名' WHERE user_id='u'")
        with DatabaseUnitOfWork(player) as uow:
            uow.execute("UPDATE training SET points=30 WHERE user_id='u'")

        self.assertEqual(first, repository.top_snapshot("points"))
        self.assertEqual(1, repository.rank_of("points", "u").rank)
        process_read_cache.invalidate_snapshot_database(player)
        refreshed = repository.top_snapshot("points")
        self.assertEqual("新名", refreshed["rows"][0]["user_name"])
        self.assertEqual("u", refreshed["rows"][0]["user_id"])

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


class RankQueryCompositionTests(_TempDatabaseTestCase):
    def _track_query_one(self, callback):
        statements = []
        original = DatabaseUnitOfWork.query_one

        def tracked(uow, sql, params=()):
            statements.append(str(sql))
            return original(uow, sql, params)

        with patch.object(DatabaseUnitOfWork, "query_one", tracked):
            callback()
        return statements

    def test_live_personal_standings_use_one_aggregate_query(self):
        boss_database = self.root / "boss.db"
        with DatabaseUnitOfWork(boss_database) as uow:
            apply_boss_player_schema(uow)
            uow.executemany(
                "INSERT INTO boss_limit(user_id,integral) VALUES(?,?)",
                (("leader", 100), ("viewer", 50)),
            )
        boss_queries = self._track_query_one(
            lambda: BossIntegralSqlRepository(boss_database).integral_rank("viewer")
        )
        self.assertEqual(1, sum("WITH ranked AS MATERIALIZED" in sql for sql in boss_queries))

        tower_database = self.root / "tower.db"
        with DatabaseUnitOfWork(tower_database) as uow:
            apply_tower_state(uow)
            uow.executemany(
                "INSERT INTO tower(user_id,current_floor,score) VALUES(?,?,?)",
                (("leader", 20, 100), ("viewer", 10, 50)),
            )
        tower_queries = self._track_query_one(
            lambda: TowerStateRepository(tower_database).rank_of("viewer")
        )
        self.assertEqual(1, len(tower_queries))
        self.assertIn(" AS better ", tower_queries[0])
        self.assertIn(" AS total", tower_queries[0])

        game, player = self._training_databases()
        with DatabaseUnitOfWork(player) as uow:
            uow.executemany(
                "INSERT INTO training(user_id,points,completed) VALUES(?,?,?)",
                (("leader", 100, 10), ("viewer", 50, 5)),
            )
        training_queries = self._track_query_one(
            lambda: TrainingLeaderboardSqlRepository(game, player).rank_of(
                "points", "viewer"
            )
        )
        self.assertEqual(1, len(training_queries))
        self.assertIn("WITH ranked AS MATERIALIZED", training_queries[0])


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
        self.assertIn(
            "training_application.leaderboard_snapshot_with_rank(", body
        )
        self.assertIn("display_snapshot_freshness(snapshot)", body)
        self.assertIn("if viewer_id:", body, "无 viewer 时必须保持旧输出（可被旧调用方复用）")

    def test_handlers_pass_the_session_user_into_the_personal_line(self):
        boss = _handler_body(_read("xiuxian/xiuxian_boss/__init__.py"), "boss_integral_rank_")
        tower = _read("xiuxian/xiuxian_tower/__init__.py")
        arena = _handler_body(_read("xiuxian/xiuxian_arena/__init__.py"), "arena_ranking_")

        self.assertIn('viewer_id = str(user_info["user_id"])', boss)
        self.assertIn('viewer_id = str(user_info["user_id"])', tower)
        self.assertIn("viewer_id = str(event.get_user_id())", arena)

    def test_views_keep_their_existing_top_slice_calls(self):
        boss = _read("xiuxian/xiuxian_boss/__init__.py")
        self.assertIn("boss_integral_application.top_integrals_snapshot_with_rank(", boss)
        tower = _read("xiuxian/xiuxian_tower/__init__.py")
        self.assertIn('tower_limit.ranking_snapshot_with_rank(\n        viewer_id, "current_floor"', tower)
        self.assertIn('tower_limit.ranking_snapshot_with_rank(viewer_id, "score")', tower)
        self.assertIn(
            "arena_limit.get_arena_ranking_with_rank(",
            _read("xiuxian/xiuxian_arena/__init__.py"),
        )
        training = _read("xiuxian/xiuxian_training/__init__.py")
        self.assertIn("training_application.leaderboard_snapshot(field, limit=50)", training)
        self.assertIn("training_application.leaderboard_snapshot_with_rank(", training)

    def test_fanout_rank_views_batch_display_profile_reads(self):
        cases = (
            ("xiuxian/xiuxian_arena/__init__.py", "arena_ranking_"),
            ("xiuxian/xiuxian_boss/__init__.py", "boss_integral_rank_"),
            ("xiuxian/xiuxian_impart_pk/__init__.py", "impart_top_"),
            ("xiuxian/xiuxian_impart_pk/__init__.py", "impart_pk_list_"),
            ("xiuxian/xiuxian_tower/__init__.py", "tower_rank_"),
            ("xiuxian/xiuxian_tower/__init__.py", "tower_integral_rank_"),
            ("xiuxian/xiuxian_buff/partner.py", "partner_rank_"),
        )
        for relative, marker in cases:
            with self.subTest(handler=marker):
                body = _handler_body(_read(relative), marker)
                self.assertTrue(
                    "get_user_names_by_ids" in body
                    or "get_user_display_profiles_by_ids" in body
                    or "get_impart_rank_display" in body
                )
                self.assertNotIn("_sql_message().get_user_info_with_id(", body)

        partner_source = _read("xiuxian/xiuxian_buff/partner.py")
        mentor = _handler_body(partner_source, "mentor_rank_")
        self.assertIn('get_all_fields(\n            "statistics"', mentor)
        self.assertIn('get_all_fields(\n            "mentor"', mentor)
        self.assertIn("get_user_display_profiles_by_ids", mentor)
        self.assertIn("process_read_cache.get_or_load", mentor)
        self.assertIn('"mentor-rank-display-v1"', mentor)
        self.assertIn("            20,", mentor)
        self.assertIn('get_paths().game_db', mentor)
        self.assertIn('"file-identity"', mentor)
        self.assertIn("ttl=60 * 60", mentor)
        self.assertNotIn("get_valid_apprentices(", mentor)
        self.assertNotIn("get_user_profile(", mentor)
        self.assertLess(mentor.index("rank_rows.sort("), mentor.index("get_user_display_profiles_by_ids"))
        self.assertIn("rank_rows[offset : offset + 400]", mentor)
        self.assertIn("len(display_rows) == 20", mentor)

        partner = _handler_body(partner_source, "partner_rank_")
        self.assertIn('get_all_field_data(', partner)
        self.assertIn('"affection", cache_ttl=0', partner)
        self.assertIn("get_fields_by_ids(", partner)
        self.assertIn('"partner", [user_id', partner)
        self.assertIn("get_user_display_profiles_by_ids", partner)
        self.assertIn('"partner-rank-display-v1", 50', partner)
        self.assertIn("ttl=60 * 60", partner)
        self.assertIn("if user_info is None or partner_info is None", partner)
        self.assertNotIn("get_field_data(", partner)

    def test_arena_facade_delegates_rank_of_to_the_feature_application(self):
        source = _read("xiuxian/xiuxian_arena/arena_limit.py")

        self.assertIn("def rank_of(self, user_id):", source)
        self.assertIn("return state_application.rank_of(user_id)", source)
        self.assertIn("return state_application.ranking_with_rank(limit, user_id)", source)

    def test_tower_facade_delegates_rank_of_to_the_feature_application(self):
        source = _read("xiuxian/xiuxian_tower/tower_limit.py")

        self.assertIn('def rank_of(self, user_id, field: str = "current_floor"):', source)
        self.assertIn("return state_application.rank_of(user_id, field)", source)


if __name__ == "__main__":
    unittest.main()
