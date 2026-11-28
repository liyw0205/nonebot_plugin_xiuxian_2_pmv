"""Shared ranking projections preserve legacy identities and bound name IO."""
from pathlib import Path
import tempfile
import unittest

from nonebot_plugin_xiuxian_2.core.numeric import as_int_like
from nonebot_plugin_xiuxian_2.features.base.first_user_rows import first_user_rows
from nonebot_plugin_xiuxian_2.features.base.ranking_position import PersonalRank
from nonebot_plugin_xiuxian_2.features.boss.integral_repository import BossIntegralSqlRepository
from nonebot_plugin_xiuxian_2.features.training.leaderboard_repository import TrainingLeaderboardSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


def _legacy(uow, table, field, actor):
    deduped = (
        f'SELECT t.user_id,COALESCE(t."{field}",0) AS value FROM {table} t '
        "WHERE t.user_id IS NOT NULL AND TRIM(CAST(t.user_id AS TEXT))<>'' "
        f"AND t.rowid=(SELECT MIN(c.rowid) FROM {table} c WHERE c.user_id=t.user_id)"
    )
    top = uow.query_all(
        f"SELECT t.user_id,COALESCE(t.{field},0) AS value FROM {table} t "
        "WHERE t.user_id IS NOT NULL AND TRIM(CAST(t.user_id AS TEXT))<>'' "
        f"AND t.rowid=(SELECT MIN(c.rowid) FROM {table} c WHERE c.user_id=t.user_id) "
        f"ORDER BY xiuxian_int_text(COALESCE(t.{field},0)) COLLATE xiuxian_integer DESC,t.rowid ASC LIMIT 50"
    )
    rank = uow.query_one(
        f"WITH ranked AS MATERIALIZED ({deduped}),target AS ("
        "SELECT COALESCE((SELECT value FROM ranked WHERE user_id=?),0) AS value) "
        "SELECT target.value AS value,"
        "COALESCE(SUM(CASE WHEN xiuxian_int_cmp(ranked.value,0)>0 THEN 1 ELSE 0 END),0) AS total,"
        "COALESCE(SUM(CASE WHEN target.value>0 AND xiuxian_int_cmp(ranked.value,target.value)>0 "
        "THEN 1 ELSE 0 END),0) AS better FROM ranked CROSS JOIN target", (actor,),
    ) or {}
    value = max(0, as_int_like(rank.get("value")))
    return top, PersonalRank(int(rank.get("better") or 0) + 1 if value else 0,
                             int(rank.get("total") or 0), value)


class GroupedRankingReadStageTests(unittest.TestCase):
    def test_grouped_identity_matches_correlated_legacy_for_both_modules(self):
        shapes = ("TEXT", "TEXT COLLATE NOCASE", "TEXT COLLATE RTRIM", "", "NUMERIC")
        data = [("first", 0), ("first", "999"), ("Ab", 7), ("ab", 40),
                ("trail ", 8), ("trail", 90), (" ", 500), ("", 600), (None, 800),
                ("tie", 7), ("huge", str(10**100)), ("blob", str(10**100).encode()),
                ("bad", "bad"), ("null", None), ("science", "1e3"),
                (7, 17), ("7", 33), (7.0, 22)]
        with tempfile.TemporaryDirectory() as temp:
            for index, shape in enumerate(shapes):
                path = Path(temp) / f"case{index}.db"
                with DatabaseUnitOfWork(path) as uow:
                    for table in ("training", "boss_limit"):
                        uow.execute(f"CREATE TABLE {table}(user_id {shape},points,completed,integral)")
                        uow.executemany(f"INSERT INTO {table} VALUES(?,?,?,?)",
                                        ((actor, value, value, value) for actor, value in data))
                    for actor in ("first", "Ab", "trail", "huge", "science", "missing", "7"):
                        with self.subTest(shape=shape, actor=actor):
                            _top, rank = _legacy(uow, "training", "points", actor)
                            self.assertEqual(rank, TrainingLeaderboardSqlRepository._rank_in_uow(
                                uow, "points", actor, schema="main"))
                            top, rank = _legacy(uow, "boss_limit", "integral", actor)
                            self.assertEqual(rank, BossIntegralSqlRepository._integral_rank_in_uow(uow, actor))
                            self.assertEqual([(str(row["user_id"]), as_int_like(row["value"])) for row in top],
                                             BossIntegralSqlRepository._top_integrals_in_uow(uow, 50))

    def test_grouping_precedes_identity_filter_and_reuses_original_collation(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "player.db"
            with DatabaseUnitOfWork(path) as uow:
                uow.execute("CREATE TABLE boss_limit(user_id TEXT COLLATE RTRIM,integral)")
                uow.executemany("INSERT INTO boss_limit VALUES(?,?)", [("", 1), (" ", 999), ("u", 5)])
                self.assertEqual([("u", 5)], BossIntegralSqlRepository._top_integrals_in_uow(uow, 50))
                plan = uow.query_all(
                    "EXPLAIN QUERY PLAN SELECT entry.user_id FROM boss_limit entry "
                    f"JOIN ({first_user_rows('boss_limit')}) first "
                    "ON first.user_id=entry.user_id AND first.first_rowid=entry.rowid"
                )
                self.assertFalse(any("CORRELATED" in row["detail"] for row in plan))

    def test_training_names_are_read_only_after_limit_with_exact_order_and_first_name(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = Path(temp) / "game.db", Path(temp) / "player.db"
            with DatabaseUnitOfWork(player) as uow:
                uow.execute("CREATE TABLE training(user_id,points,completed)")
                uow.executemany("INSERT INTO training VALUES(?,?,?)",
                                ((str(index), index // 2, index // 2) for index in range(200)))
                uow.execute("INSERT INTO training VALUES('199',NULL,NULL)")
            with DatabaseUnitOfWork(game) as uow:
                uow.execute("CREATE TABLE people(user_id,user_name)")
                uow.executemany("INSERT INTO people VALUES(?,?)", ((str(i), f"name{i}") for i in range(200)))
                uow.execute("INSERT INTO people VALUES('199','wrong-second-name')")
                uow.execute("CREATE VIEW user_xiuxian AS SELECT rowid,user_id,observe_name(user_name) AS user_name FROM people")
                uow.attach_database(player, "player_data")
                names = []
                uow.connection.create_function("observe_name", 1, lambda value: names.append(value) or value)
                for field in ("points", "completed"):
                    names.clear()
                    rows = TrainingLeaderboardSqlRepository._top_in_uow(uow, field, 50)
                    self.assertEqual(50, len(rows))
                    self.assertEqual(50, len(names))
                    self.assertEqual([str(i) for i in range(198, 148, -2) for i in (i, i + 1)],
                                     [row["user_id"] for row in rows])
                    self.assertEqual("name199", rows[1]["user_name"])

    def test_first_user_projection_accepts_only_controlled_tables_and_schemas(self):
        for table, schema in (("users", "main"), ("training; DROP TABLE training", "main"),
                              ("training", "temp")):
            with self.subTest(table=table, schema=schema), self.assertRaises(ValueError):
                first_user_rows(table, schema=schema)


if __name__ == "__main__":
    unittest.main()
