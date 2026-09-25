import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork
from ..migrations import apply_mixelixir_refine_claim, apply_mixelixir_refine_claim_player
from ..refine_reward_repository import MixelixirRefineRewardSqlRepository
from ..application import MixelixirApplication
from ....plugin import apply_platform_schema, build_migrations, migrations_for_database
from tests.test_db_backend import db_backend


class MixelixirRefineRewardRepositoryTests(unittest.TestCase):
    expected = {
        "丹药控火": "1",
        "炼丹记录": {"19": {"name": "旧丹", "num": "2"}},
        "炼丹经验": "10",
    }
    updated = {
        "丹药控火": "1",
        "炼丹记录": {
            "19": {"name": "旧丹", "num": 2},
            "20": {"name": "丹", "num": 1},
        },
        "炼丹经验": "15",
    }

    def _databases(self, temp: str) -> tuple[Path, Path]:
        game, player = Path(temp) / "game.db", Path(temp) / "player.db"
        with db_backend.transaction(game) as conn:
            conn.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,mixelixir_num INTEGER)")
            conn.execute("INSERT INTO user_xiuxian VALUES('u',2)")
            conn.execute("INSERT INTO user_xiuxian VALUES('other',0)")
            conn.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
                "goods_num INTEGER,bind_num INTEGER,PRIMARY KEY(user_id,goods_id))"
            )
        with DatabaseUnitOfWork(game) as uow:
            apply_mixelixir_refine_claim(uow)
        with db_backend.transaction(player) as conn:
            conn.execute(
                "CREATE TABLE mix_elixir_info(user_id TEXT PRIMARY KEY,丹药控火 TEXT,炼丹记录 TEXT,炼丹经验 TEXT)"
            )
            conn.execute(
                "INSERT INTO mix_elixir_info VALUES('u','1',?, '10')",
                (json.dumps({"19": {"num": 2, "name": "旧丹"}}, ensure_ascii=False),),
            )
        with DatabaseUnitOfWork(player) as uow:
            apply_mixelixir_refine_claim_player(uow)
        return game, player

    def _task(self, game: Path, task_id="t1", *, expected=None, updated=None, reward_id=20, quantity=2):
        with DatabaseUnitOfWork(game) as uow:
            uow.execute(
                "INSERT INTO mixelixir_refine_tasks "
                "(task_id,user_id,recipe_set_id,recipe_key,status,materials_json,reward_id,reward_name,"
                "reward_quantity,expected_mix_state,updated_mix_state) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task_id,
                    "u",
                    "custom",
                    "recipe-a",
                    "ready",
                    "[[1,2]]",
                    reward_id,
                    "丹",
                    quantity,
                    json.dumps(expected or self.expected, ensure_ascii=False),
                    json.dumps(updated or self.updated, ensure_ascii=False),
                ),
            )

    def test_claim_updates_inventory_mix_state_statistics_and_replays_once(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self._databases(temp)
            self._task(game)
            repo = MixelixirRefineRewardSqlRepository(game, player)
            self.assertEqual(repo.latest_ready_task("u", "recipe-a"), "t1")
            first = repo.claim("claim-1", "u", "t1", 99)
            replay = repo.claim("claim-1", "u", "t1", 99)
            self.assertEqual((first.status, replay.status), ("applied", "duplicate"))
            self.assertEqual(repo.latest_ready_task("u"), None)
            with db_backend.transaction(game) as conn:
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=20").fetchone()[0], 2)
                self.assertEqual(conn.execute("SELECT status FROM mixelixir_refine_tasks WHERE task_id='t1'").fetchone()[0], "claimed")
            with db_backend.transaction(player) as conn:
                row = conn.execute("SELECT 丹药控火,炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='u'").fetchone()
                self.assertEqual((row[0], json.loads(row[1]), row[2]), ("1", self.updated["炼丹记录"], "15"))
                self.assertEqual(conn.execute('SELECT "炼丹次数" FROM statistics WHERE user_id=\'u\'').fetchone()[0], 1)

    def test_stale_mix_snapshot_and_full_inventory_leave_task_unclaimed(self):
        for stale, full in ((True, False), (False, True)):
            with self.subTest(stale=stale, full=full), tempfile.TemporaryDirectory() as temp:
                game, player = self._databases(temp)
                if stale:
                    with db_backend.transaction(player) as conn:
                        conn.execute("UPDATE mix_elixir_info SET 炼丹经验='9' WHERE user_id='u'")
                self._task(game, quantity=2)
                if full:
                    with db_backend.transaction(game) as conn:
                        conn.execute("INSERT INTO back VALUES('u',20,'丹','丹药',98,0)")
                result = MixelixirRefineRewardSqlRepository(game, player).claim("claim-1", "u", "t1", 99)
                self.assertEqual(result.status, "state_changed" if stale else "inventory_full")
                with db_backend.transaction(game) as conn:
                    self.assertEqual(conn.execute("SELECT status FROM mixelixir_refine_tasks WHERE task_id='t1'").fetchone()[0], "ready")
                    count = conn.execute("SELECT COUNT(*) FROM back WHERE user_id='u' AND goods_id=20").fetchone()[0]
                    if full:
                        self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE user_id='u' AND goods_id=20").fetchone()[0], 98)
                    else:
                        self.assertEqual(count, 0)
                with db_backend.transaction(player) as conn:
                    self.assertEqual(conn.execute("SELECT COUNT(*) FROM statistics WHERE user_id='u'").fetchone()[0], 0)

    def test_task_owner_and_operation_identity_are_checked(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self._databases(temp)
            self._task(game)
            self._task(game, "t2")
            repo = MixelixirRefineRewardSqlRepository(game, player)
            self.assertEqual(repo.claim("claim-1", "other", "t1", 99).status, "task_missing")
            self.assertEqual(repo.claim("claim-1", "u", "t1", 99).status, "applied")
            self.assertEqual(repo.claim("claim-1", "u", "t2", 99).status, "operation_conflict")

    def test_attached_player_failure_rolls_back_game_and_player_updates(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self._databases(temp)
            self._task(game)
            with db_backend.transaction(player) as conn:
                conn.execute(
                    "CREATE TRIGGER fail_alchemy_stats BEFORE INSERT ON statistics "
                    "BEGIN SELECT RAISE(ABORT,'injected failure'); END"
                )
            repo = MixelixirRefineRewardSqlRepository(game, player)
            with self.assertRaises(sqlite3.IntegrityError):
                repo.claim("claim-1", "u", "t1", 99)
            with db_backend.transaction(game) as conn:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM back WHERE goods_id=20").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT status FROM mixelixir_refine_tasks WHERE task_id='t1'").fetchone()[0], "ready")
            with db_backend.transaction(player) as conn:
                row = conn.execute("SELECT 丹药控火,炼丹记录,炼丹经验 FROM mix_elixir_info WHERE user_id='u'").fetchone()
                self.assertEqual((row[0], json.loads(row[1]), row[2]), ("1", {"19": {"num": 2, "name": "旧丹"}}, "10"))

    def test_request_does_not_create_missing_operation_table(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self._databases(temp)
            self._task(game)
            with db_backend.transaction(game) as conn:
                conn.execute("DROP TABLE mixelixir_refine_reward_operations")
            result = MixelixirRefineRewardSqlRepository(game, player).claim("claim-1", "u", "t1", 99)
            self.assertEqual(result.status, "schema_missing")
            with db_backend.transaction(game) as conn:
                self.assertIsNone(
                    conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='mixelixir_refine_reward_operations'"
                    ).fetchone()
                )

    def test_refine_claim_migrations_are_routed_to_the_owning_databases(self):
        catalog = build_migrations()
        game = {item.version for item in migrations_for_database(catalog, "game_db")}
        player = {item.version for item in migrations_for_database(catalog, "player_db")}
        self.assertIn("mixelixir.002", game)
        self.assertNotIn("mixelixir.002", player)
        self.assertIn("mixelixir.003", player)
        self.assertNotIn("mixelixir.003", game)

    def test_game_migration_upgrades_an_existing_incomplete_task_table(self):
        with tempfile.TemporaryDirectory() as temp:
            game = Path(temp) / "game.db"
            with DatabaseUnitOfWork(game) as uow:
                uow.execute(
                    "CREATE TABLE mixelixir_refine_tasks(task_id TEXT PRIMARY KEY,user_id TEXT NOT NULL,"
                    "recipe_set_id TEXT NOT NULL,status TEXT NOT NULL,materials_json TEXT NOT NULL,"
                    "reward_quantity INTEGER NOT NULL,created_at TIMESTAMP,claimed_at TIMESTAMP)"
                )
                apply_mixelixir_refine_claim(uow)
                columns = {row["name"] for row in uow.query_all("PRAGMA table_info(mixelixir_refine_tasks)")}
                operations = {
                    row["name"]
                    for row in uow.query_all("SELECT name FROM sqlite_master WHERE type='table'")
                }
            self.assertTrue({"recipe_key", "reward_id", "reward_name", "expected_mix_state", "updated_mix_state"}.issubset(columns))
            self.assertTrue({"mixelixir_refine_cost_operations", "mixelixir_refine_reward_operations"}.issubset(operations))

    def test_application_runs_the_two_phase_refine_flow_once(self):
        with tempfile.TemporaryDirectory() as temp:
            game, player = self._databases(temp)
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
                uow.execute("INSERT INTO back VALUES('u',1,'草','药材',3,0)")
                uow.execute("INSERT INTO back VALUES('u',2,'炉','炼丹炉',1,0)")
            app = MixelixirApplication(game, player)
            started = app.refine_cost(
                operation_id="cost-1",
                user_id="u",
                recipe_set_id="custom",
                daily_count=2,
                expected_snapshot=self.expected,
                updated_mix_state=self.updated,
                max_goods_num=99,
                recipe_key="recipe-a",
                materials={1: 2},
                furnace_id=2,
                reward_id=20,
                reward_name="丹",
                reward_quantity=1,
            )
            task_id = started.data["task_id"]
            claimed = app.refine_reward(
                operation_id="reward-1", user_id="u", task_id=task_id, max_goods_num=99
            )
            replay = app.refine_reward(
                operation_id="reward-1", user_id="u", task_id=task_id, max_goods_num=99
            )
            self.assertTrue(started.ok and claimed.ok and replay.replayed)
            with db_backend.transaction(game) as conn:
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=1").fetchone()[0], 1)
                self.assertEqual(conn.execute("SELECT goods_num FROM back WHERE goods_id=20").fetchone()[0], 1)
            with db_backend.transaction(player) as conn:
                self.assertEqual(conn.execute('SELECT "炼丹次数" FROM statistics WHERE user_id=\'u\'').fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
