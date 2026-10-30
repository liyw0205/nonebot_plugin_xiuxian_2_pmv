"""虚神界统计列契约：`虚神界闭关` / `投影虚神界` 在真实 schema 上必须可用。

两个迁移后仓储此前都依赖
``CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY, "<列>" INTEGER DEFAULT 0)``
把业务列内联进建表语句。但 ``statistics`` 早已被 base/training/auction/boss 等迁移以
``CREATE TABLE IF NOT EXISTS statistics(user_id TEXT PRIMARY KEY)`` 建好，
``IF NOT EXISTS`` 分支是 no-op，内联列永远不会被创建。于是在真实库上：

* ``ImpartClosingEnterSqlRepository.enter()`` ->
  ``OperationalError: no such column: 虚神界闭关次数``
* ``ImpartProjectJoinSqlRepository.join()`` ->
  ``OperationalError: table statistics has no column named 虚神界投影次数``

两个 handler 在调用处都没有 try/except，异常直接冒泡 —— 玩家收不到任何回复。

本套件刻意用**生产形态**的 ``statistics``（只有 ``user_id``，绝不内联目标列），
而不是自建带目标列的表，这样回归一旦复现就会失败。
"""

from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.impart_pk.closing_enter_repository import (
    ImpartClosingEnterSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.project_join_repository import (
    ImpartProjectJoinSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.impart_pk.statistics_columns import (
    ensure_statistics_column,
    increment_statistics,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork

CLOSING_FIELD = "虚神界闭关次数"
PROJECT_FIELD = "虚神界投影次数"


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self._temp = tempfile.TemporaryDirectory()
        root = Path(self._temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        # 生产形态：statistics 只有 user_id（base/training/auction/boss 迁移建的形状）。
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            uow.execute("CREATE TABLE statistics(user_id TEXT PRIMARY KEY)")
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian(id INTEGER PRIMARY KEY,user_id TEXT UNIQUE,"
                "user_name TEXT,stone INTEGER,root_type TEXT,level INTEGER)"
            )
            uow.execute(
                "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,"
                "scheduled_time TEXT)"
            )
            self._player(uow, "u1", "天灵根")
            self._player(uow, "u2", "伪灵根")
            uow.execute("INSERT INTO user_xiuxian(id,user_id,user_name,stone,root_type,level)"
                        " VALUES(3,'u3','丙',100,'天灵根',10)")
            uow.execute("INSERT INTO user_cd(user_id,type,create_time) VALUES('u3',0,'2026-10-30 00:00:00')")

    def tearDown(self) -> None:
        self._temp.cleanup()

    @staticmethod
    def _player(uow, user_id: str, root: str) -> None:
        uow.execute(
            "INSERT INTO user_xiuxian(id,user_id,user_name,stone,root_type,level)"
            " VALUES(?,?,?,100,?,10)",
            (9 if user_id == "u2" else 1, user_id, user_id, root),
        )
        uow.execute(
            "INSERT INTO user_cd(user_id,type,create_time) VALUES(?,0,'2026-10-30 00:00:00')",
            (user_id,),
        )

    # -- helpers ---------------------------------------------------------
    def stat(self, field=CLOSING_FIELD, user_id="u1"):
        with sqlite3.connect(self.player) as conn:
            try:
                row = conn.execute(
                    f'SELECT "{field}" FROM statistics WHERE user_id=?', (user_id,)
                ).fetchone()
            except sqlite3.OperationalError:
                return "__no_column__"
        return None if row is None else row[0]

    def columns(self):
        with sqlite3.connect(self.player) as conn:
            return {r[1] for r in conn.execute("PRAGMA table_info(statistics)")}

    def closing(self) -> ImpartClosingEnterSqlRepository:
        return ImpartClosingEnterSqlRepository(str(self.game), str(self.player))

    def project(self, capacity: int = 40) -> ImpartProjectJoinSqlRepository:
        return ImpartProjectJoinSqlRepository(str(self.player), capacity)

    def reset_cd(self, user_id="u1") -> None:
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            uow.execute("UPDATE user_cd SET type=0 WHERE user_id=?", (user_id,))

    def set_root(self, user_id: str, root: str) -> None:
        with DatabaseUnitOfWork(str(self.game), immediate=True) as uow:
            uow.execute("UPDATE user_xiuxian SET root_type=? WHERE user_id=?", (root, user_id))


class ClosingEnterStatisticsTests(_Base):
    """`虚神界闭关` —— 此前 100% 抛 OperationalError。"""

    def test_enter_applies_on_production_statistics_shape(self) -> None:
        self.assertNotIn(CLOSING_FIELD, self.columns())
        result = self.closing().enter("op-1", "u1", "2026-10-30 10:00:00.000000")
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.entry_count, 1)
        self.assertEqual(self.stat(), 1)
        self.assertIn(CLOSING_FIELD, self.columns())

    def test_enter_accumulates_and_survives_missing_statistics_row(self) -> None:
        repo = self.closing()
        self.assertEqual(repo.enter("op-1", "u1", "t1").entry_count, 1)
        self.assertEqual(self.stat(), 1)
        # 玩家原本没有 statistics 行：UPDATE rowcount==0 时必须 INSERT 补行，
        # 否则第二次进入又会从 1 开始（旧实现的静默丢失）。
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            uow.execute("DELETE FROM statistics WHERE user_id='u1'")
        self.reset_cd()
        self.assertEqual(repo.enter("op-2", "u1", "t2").entry_count, 1)
        self.assertEqual(self.stat(), 1)

    def test_duplicate_operation_does_not_increment(self) -> None:
        repo = self.closing()
        repo.enter("op-1", "u1", "t1")
        again = repo.enter("op-1", "u1", "t1")
        self.assertEqual(again.status, "duplicate")
        self.assertEqual(self.stat(), 1)

    def test_busy_state_does_not_increment(self) -> None:
        repo = self.closing()
        repo.enter("op-1", "u1", "t1")
        # 进入后 user_cd.type=4，第二次进入应被挡住
        busy = repo.enter("op-2", "u1", "t2")
        self.assertIn(busy.status, {"busy", "state_changed"})
        self.assertEqual(self.stat(), 1)

    def test_ineligible_root_does_not_increment(self) -> None:
        self.set_root("u1", "伪灵根")
        result = self.closing().enter("op-1", "u1", "t1")
        self.assertEqual(result.status, "ineligible")
        self.assertIn(self.stat(), (None, 0, "__no_column__"))

    def test_old_database_with_existing_column_still_works(self) -> None:
        # 模拟旧路径已 ALTER 出该列的库
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{CLOSING_FIELD}" INTEGER DEFAULT 0')
            uow.execute('INSERT INTO statistics(user_id,"%s") VALUES(?,5)' % CLOSING_FIELD, ("u1",))
        self.assertEqual(self.stat(), 5)
        result = self.closing().enter("op-1", "u1", "t1")
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.entry_count, 6)
        self.assertEqual(self.stat(), 6)

    def test_statistics_table_absent_is_created(self) -> None:
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            uow.execute("DROP TABLE statistics")
        result = self.closing().enter("op-1", "u1", "t1")
        self.assertEqual(result.status, "applied")
        self.assertEqual(result.entry_count, 1)
        self.assertEqual(self.stat(), 1)


class ProjectJoinStatisticsTests(_Base):
    """`投影虚神界` —— 此前 100% 抛 'no column named 虚神界投影次数'。"""

    def test_join_applies_on_production_statistics_shape(self) -> None:
        self.assertNotIn(PROJECT_FIELD, self.columns())
        result = self.project().join("op-1", "u1")
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.stat(PROJECT_FIELD), 1)
        self.assertIn(PROJECT_FIELD, self.columns())

    def test_players_are_isolated(self) -> None:
        repo = self.project()
        repo.join("op-1", "u1")
        repo.join("op-2", "u3")
        self.assertEqual(self.stat(PROJECT_FIELD, "u1"), 1)
        self.assertEqual(self.stat(PROJECT_FIELD, "u3"), 1)

    def test_duplicate_operation_does_not_increment(self) -> None:
        repo = self.project()
        repo.join("op-1", "u1")
        again = repo.join("op-1", "u1")
        self.assertEqual(again.status, "duplicate")
        self.assertEqual(self.stat(PROJECT_FIELD, "u1"), 1)

    def test_already_joined_does_not_increment(self) -> None:
        repo = self.project()
        repo.join("op-1", "u1")
        result = repo.join("op-2", "u1")
        self.assertEqual(result.status, "already_joined")
        self.assertEqual(self.stat(PROJECT_FIELD, "u1"), 1)

    def test_pk_exhausted_does_not_increment(self) -> None:
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            uow.execute(
                "CREATE TABLE IF NOT EXISTS impart_pk_state(user_id TEXT PRIMARY KEY,"
                "pk_num INTEGER NOT NULL DEFAULT 7,win_num INTEGER NOT NULL DEFAULT 0)"
            )
            uow.execute("INSERT INTO impart_pk_state(user_id,pk_num,win_num) VALUES('u1',0,0)")
        result = self.project().join("op-1", "u1")
        self.assertEqual(result.status, "pk_exhausted")
        self.assertIn(self.stat(PROJECT_FIELD), (None, 0, "__no_column__"))

    def test_capacity_full_does_not_increment(self) -> None:
        repo = self.project(capacity=1)
        repo.join("op-1", "u1")
        result = repo.join("op-2", "u3")
        self.assertEqual(result.status, "capacity_full")
        self.assertEqual(self.stat(PROJECT_FIELD, "u1"), 1)
        self.assertIn(self.stat(PROJECT_FIELD, "u3"), (None, 0, "__no_column__"))

    def test_old_database_with_existing_column_still_works(self) -> None:
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            uow.execute(f'ALTER TABLE statistics ADD COLUMN "{PROJECT_FIELD}" INTEGER DEFAULT 0')
            uow.execute('INSERT INTO statistics(user_id,"%s") VALUES(?,4)' % PROJECT_FIELD, ("u1",))
        result = self.project().join("op-1", "u1")
        self.assertEqual(result.status, "applied")
        self.assertEqual(self.stat(PROJECT_FIELD, "u1"), 5)


class StatisticsColumnHelperTests(_Base):
    """共享兜底函数本身。"""

    def test_ensure_is_idempotent(self) -> None:
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            ensure_statistics_column(uow, PROJECT_FIELD, schema="")
            ensure_statistics_column(uow, PROJECT_FIELD, schema="")
        with sqlite3.connect(self.player) as conn:
            names = [r[1] for r in conn.execute("PRAGMA table_info(statistics)")]
        self.assertEqual(names.count(PROJECT_FIELD), 1)

    def test_increment_creates_row_and_returns_value(self) -> None:
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            first = increment_statistics(uow, "u1", PROJECT_FIELD, schema="")
            second = increment_statistics(uow, "u1", PROJECT_FIELD, schema="")
        self.assertEqual((first, second), (1, 2))
        self.assertEqual(self.stat(PROJECT_FIELD), 2)

    def test_no_drop_of_existing_columns(self) -> None:
        with DatabaseUnitOfWork(str(self.player), immediate=True) as uow:
            uow.execute("ALTER TABLE statistics ADD COLUMN 突破次数 INTEGER DEFAULT 0")
            ensure_statistics_column(uow, PROJECT_FIELD, schema="")
        self.assertIn("突破次数", self.columns())
        self.assertIn(PROJECT_FIELD, self.columns())


class HandlerSourceContractTests(unittest.TestCase):
    """两条命令仍然注册，且调用点仍指向修好的仓储路径。"""

    def test_matchers_and_call_sites(self) -> None:
        path = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "xiuxian"
            / "xiuxian_impart_pk"
            / "__init__.py"
        )
        source = path.read_text(encoding="utf-8")
        self.assertIn('on_command("虚神界闭关"', source)
        self.assertIn('on_command("投影虚神界"', source)
        self.assertIn("impart_pk_application.closing_enter(", source)
        self.assertIn("impart_pk_application.project_join(", source)

    def test_repositories_route_statistics_through_shared_helper(self) -> None:
        base = (
            Path(__file__).resolve().parents[1]
            / "nonebot_plugin_xiuxian_2"
            / "features"
            / "impart_pk"
        )
        closing = (base / "closing_enter_repository.py").read_text(encoding="utf-8")
        project = (base / "project_join_repository.py").read_text(encoding="utf-8")
        # 回归防线：不得再出现「把业务列内联进 CREATE TABLE ... statistics」的写法，
        # 也不得再直接 SELECT 未经 ensure 的统计列。
        for name, src in (("closing", closing), ("project", project)):
            self.assertNotRegex(
                src,
                r"CREATE TABLE IF NOT EXISTS (player_data\.)?statistics\([^)]*,",
                f"{name} must not inline statistics columns",
            )
        self.assertIn("increment_statistics", closing)
        self.assertIn("ensure_statistics_column", project)


if __name__ == "__main__":
    unittest.main()
