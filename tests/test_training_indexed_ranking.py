"""Exact indexed training ranks retain the live legacy rank contract."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from nonebot_plugin_xiuxian_2.features.base.indexed_ranking import TRAINING_RANK_INDEXES
from nonebot_plugin_xiuxian_2.features.base.ranking_position import PersonalRank
from nonebot_plugin_xiuxian_2.features.training import leaderboard_repository as leaderboard
from nonebot_plugin_xiuxian_2.features.training.migrations import apply_training_exact_rank_indexes
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.shared_read_cache import process_read_cache


def _legacy_rank(uow, field, user_id, *, schema="main"):
    row = uow.query_one(
        "WITH ranked AS MATERIALIZED ("
        f'SELECT t.user_id,COALESCE(t."{field}",0) AS value FROM {schema}.training t '
        "WHERE t.user_id IS NOT NULL AND TRIM(CAST(t.user_id AS TEXT))<>'' "
        f"AND t.rowid=(SELECT MIN(c.rowid) FROM {schema}.training c WHERE c.user_id=t.user_id)), "
        "target AS (SELECT COALESCE((SELECT value FROM ranked WHERE user_id=?),0) AS value) "
        "SELECT target.value AS value,"
        "COALESCE(SUM(CASE WHEN xiuxian_int_cmp(ranked.value,0)>0 THEN 1 ELSE 0 END),0) AS total,"
        "COALESCE(SUM(CASE WHEN target.value>0 AND xiuxian_int_cmp(ranked.value,target.value)>0 "
        "THEN 1 ELSE 0 END),0) AS better FROM ranked CROSS JOIN target",
        (user_id,),
    ) or {}
    value = as_int_like(row.get("value"))
    return PersonalRank(
        int(row.get("better") or 0) + 1 if value > 0 else 0,
        int(row.get("total") or 0),
        value if value > 0 else 0,
    )


class TrainingIndexedRankingTests(unittest.TestCase):
    def setUp(self):
        process_read_cache.clear()
        self.addCleanup(process_read_cache.clear)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.game = Path(self.temp.name) / "game.db"
        self.player = Path(self.temp.name) / "player.db"
        self.huge = 10**100
        self.rows = [
            ("leader", str(self.huge + 10)),
            ("viewer", str(self.huge + 1).encode("ascii")),
            ("tie", str(self.huge + 1)),
            ("normal", "1e3"),
            ("decimal", "19.99"),
            ("real", 19.9),
            ("zero", 0),
            ("negative", "-2"),
            ("null", None),
            ("bad", "not-a-number"),
            ("nan", "NaN"),
            ("infinite", "Infinity"),
            ("bad_blob", b"\xff"),
            ("", str(self.huge + 500)),
            (" ", str(self.huge + 600)),
            (None, str(self.huge + 700)),
            (None, str(self.huge + 800)),
        ]
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT,user_name TEXT)")
            uow.executemany(
                "INSERT INTO user_xiuxian VALUES(?,?)",
                ((actor, actor) for actor, _ in self.rows if actor),
            )
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE training(user_id TEXT PRIMARY KEY,points,completed)")
            uow.executemany(
                "INSERT INTO training(user_id,points,completed) VALUES(?,?,?)",
                ((actor, value, value) for actor, value in self.rows),
            )
            apply_training_exact_rank_indexes(uow)
        self.repository = leaderboard.TrainingLeaderboardSqlRepository(self.game, self.player)

    def test_indexed_and_reference_ranks_match_for_both_fields_main_and_attached(self):
        actors = [actor for actor, _ in self.rows if actor and actor.strip()] + ["absent"]
        with patch.object(
            leaderboard, "indexed_personal_rank", wraps=leaderboard.indexed_personal_rank,
        ) as indexed:
            for schema, path in (("main", self.player), ("player_data", self.game)):
                with DatabaseUnitOfWork(path, read_only=True) as uow:
                    if schema == "player_data":
                        uow.attach_database(self.player, schema)
                    for field in ("points", "completed"):
                        for actor in actors:
                            with self.subTest(schema=schema, field=field, actor=actor):
                                expected = _legacy_rank(uow, field, actor, schema=schema)
                                actual = self.repository._rank_in_uow(
                                    uow, field, actor, schema=schema, database_path=self.player,
                                )
                                self.assertEqual(expected, actual)
                                if actor in {"viewer", "tie"}:
                                    self.assertEqual(PersonalRank(2, 6, self.huge + 1), actual)
                                if actor in {"decimal", "real"}:
                                    self.assertEqual(PersonalRank(5, 6, 19), actual)
        self.assertEqual(4 * len(actors), indexed.call_count)

    def test_empty_and_nonpositive_population_still_uses_indexed_path(self):
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("DELETE FROM training")
        with patch.object(
            leaderboard, "indexed_personal_rank", wraps=leaderboard.indexed_personal_rank,
        ) as indexed:
            self.assertEqual(PersonalRank(), self.repository.rank_of("points", "viewer"))
            with DatabaseUnitOfWork(self.player) as uow:
                uow.executemany(
                    "INSERT INTO training(user_id,points,completed) VALUES(?,?,?)",
                    (("viewer", "-1", "-1"), ("zero", 0, 0), ("bad", "bad", "bad")),
                )
            for field in ("points", "completed"):
                self.assertEqual(PersonalRank(), self.repository.rank_of(field, "viewer"))
        self.assertEqual(3, indexed.call_count)

    def test_legacy_duplicates_keep_first_row_and_use_fallback_in_both_schemas(self):
        legacy = self.player.parent / "legacy.db"
        with DatabaseUnitOfWork(legacy) as uow:
            uow.execute("CREATE TABLE training(user_id TEXT,points,completed)")
            uow.executemany(
                "INSERT INTO training(user_id,points,completed) VALUES(?,?,?)",
                (("viewer", 0, 0), ("viewer", str(self.huge), str(self.huge)),
                 ("leader", 10, 10), ("tie", 10, 10), (" ", 100, 100), (None, 100, 100)),
            )
            apply_training_exact_rank_indexes(uow)
        repository = leaderboard.TrainingLeaderboardSqlRepository(self.game, legacy)
        with patch.object(
            leaderboard, "indexed_personal_rank", side_effect=AssertionError("legacy duplicate fast path"),
        ):
            for first_value in (0, -1, 5):
                with DatabaseUnitOfWork(legacy) as uow:
                    uow.execute("UPDATE training SET points=?,completed=? WHERE rowid=1",
                                (first_value, first_value))
                for schema, path in (("main", legacy), ("player_data", self.game)):
                    with DatabaseUnitOfWork(path, read_only=True) as uow:
                        if schema == "player_data":
                            uow.attach_database(legacy, schema)
                        for field in ("points", "completed"):
                            with self.subTest(first_value=first_value, schema=schema, field=field):
                                actual = repository._rank_in_uow(
                                    uow, field, "viewer", schema=schema, database_path=legacy,
                                )
                                self.assertEqual(_legacy_rank(uow, field, "viewer", schema=schema), actual)
                                expected = PersonalRank(3, 3, 5) if first_value == 5 else PersonalRank(0, 2, 0)
                                self.assertEqual(expected, actual)

    def test_external_updates_deletes_and_inserts_change_both_live_ranks(self):
        snapshots = {}
        for field in ("points", "completed"):
            snapshot, rank = self.repository.top_snapshot_with_rank(field, "viewer", limit=2)
            self.assertEqual(PersonalRank(2, 6, self.huge + 1), rank)
            snapshots[field] = snapshot
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("UPDATE training SET points=?,completed=? WHERE user_id='viewer'",
                        (str(self.huge + 100), str(self.huge + 100)))
        for field in ("points", "completed"):
            snapshot, rank = self.repository.top_snapshot_with_rank(field, "viewer", limit=2)
            self.assertEqual(snapshots[field], snapshot)
            self.assertEqual(PersonalRank(1, 6, self.huge + 100), rank)
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("DELETE FROM training WHERE user_id='leader'")
            uow.execute("INSERT INTO training(user_id,points,completed) VALUES('new',?,?)",
                        (str(self.huge + 200), str(self.huge + 200)))
        for field in ("points", "completed"):
            self.assertEqual(PersonalRank(2, 6, self.huge + 100), self.repository.rank_of(field, "viewer"))

    def test_missing_index_uses_legacy_query_until_index_is_available_again(self):
        spec = TRAINING_RANK_INDEXES["points"]
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(f'DROP INDEX "{spec.name}"')
        with patch.object(
            leaderboard, "indexed_personal_rank", side_effect=AssertionError("missing index fast path"),
        ):
            self.assertEqual(PersonalRank(2, 6, self.huge + 1), self.repository.rank_of("points", "viewer"))
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute(spec.create_sql)
        with patch.object(
            leaderboard, "indexed_personal_rank", wraps=leaderboard.indexed_personal_rank,
        ) as indexed:
            self.assertEqual(PersonalRank(2, 6, self.huge + 1), self.repository.rank_of("points", "viewer"))
        indexed.assert_called_once()

    def test_existing_static_attached_entry_keeps_its_signature_and_reference_result(self):
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            uow.attach_database(self.player, "player_data")
            with patch.object(
                leaderboard, "indexed_personal_rank", side_effect=AssertionError("unspecified database fast path"),
            ):
                result = leaderboard.TrainingLeaderboardSqlRepository._rank_in_uow(
                    uow, "points", "viewer",
                )
            self.assertEqual(_legacy_rank(uow, "points", "viewer", schema="player_data"), result)


if __name__ == "__main__":
    unittest.main()
