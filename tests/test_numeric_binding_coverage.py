"""大数值写入覆盖：两条 SQL 边界都必须守住修为/灵石/战力。

历史链：``number_count`` / ``sql_num`` 最初只长在 ``xiuxian2_handle`` 的写辅助上，
迁移到 ``transaction_service`` 与各 feature 仓储后，很多路径直接写大计数器并绕过了
那次归一化，于是线上出现

    OverflowError: Python int too large to convert to SQLite INTEGER

现在有两个共享边界，且都以 ``core.numeric.bind_sqlite_param`` **唯一实现**：

* ``xiuxian_utils.db_backend._adapt_param`` —— 旧 ``connect()`` 路径的每条语句；
* ``infrastructure.database.uow`` / ``readonly`` —— 新 feature 路径的每条语句。

两者都把超出 int64 的 Python int 绑成纯十进制 TEXT（不截断、不 clamp），
int64 以内仍是精确 INTEGER。本套件同时锁定这两条边界与旧辅助的等价性。
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.core.numeric import (
    SQLITE_MAX_INT,
    SQLITE_MIN_INT,
    adapt_sqlite_params,
    bind_sqlite_param,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import db_backend
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.numeric_bind import (
    as_int_like,
    number_count,
    sql_num,
    sql_num_nonneg,
)

HUGE_EXP = 10**19 + 7
HUGE_STONE = 10**24 + 11
HUGE_POWER = 10**29  # 境界.json: 至高.power


class LegacyHelperEquivalenceTests(unittest.TestCase):
    """``number_count`` / ``sql_num`` 与共享绑定必须是同一语义。"""

    def test_oversized_values_become_plain_digits_never_clamped(self) -> None:
        for value in (HUGE_EXP, HUGE_STONE, HUGE_POWER):
            with self.subTest(value=value):
                text = number_count(value)
                self.assertEqual(str(value), text)
                self.assertIsInstance(text, str)
                # 精确保留，不落到 int64 上限
                self.assertEqual(value, int(text))
                self.assertGreater(value, SQLITE_MAX_INT)

    def test_int64_range_stays_int(self) -> None:
        for value in (0, 1, 42, SQLITE_MAX_INT, SQLITE_MIN_INT, -7):
            with self.subTest(value=value):
                self.assertEqual(value, number_count(value))
                self.assertIsInstance(number_count(value), int)

    def test_sql_num_is_the_same_function(self) -> None:
        self.assertEqual(number_count(HUGE_POWER), sql_num(HUGE_POWER))
        self.assertEqual(number_count(7), sql_num(7))

    def test_nonneg_guard_rejects_negatives_but_keeps_large(self) -> None:
        with self.assertRaises(ValueError):
            sql_num_nonneg(-1)
        self.assertEqual(str(HUGE_STONE), sql_num_nonneg(HUGE_STONE))

    def test_helpers_agree_with_bind_sqlite_param(self) -> None:
        for value in (0, 7, SQLITE_MAX_INT, SQLITE_MAX_INT + 1, HUGE_EXP, HUGE_POWER, -HUGE_EXP):
            with self.subTest(value=value):
                self.assertEqual(bind_sqlite_param(value), number_count(value))
                self.assertEqual(bind_sqlite_param(value), sql_num(value))

    def test_read_path_parses_the_bound_text_back_exactly(self) -> None:
        self.assertEqual(HUGE_POWER, as_int_like(str(HUGE_POWER)))
        self.assertEqual(0, as_int_like(""))


class RawDriverBoundaryTests(unittest.TestCase):
    """旧 ``db_backend.connect()`` 路径：每条语句都要过绑定。"""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "game.db"
        self.connection = db_backend.connect(self.path)
        self.addCleanup(self.connection.close)
        self.connection.execute(
            "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, exp INTEGER,"
            " stone INTEGER, power NUMERIC)"
        )
        self.connection.execute(
            "INSERT INTO user_xiuxian VALUES (?,?,?,?)", ("u1", 0, 0, 0)
        )

    def test_large_cultivation_stone_and_power_write_survives(self) -> None:
        self.connection.execute(
            "UPDATE user_xiuxian SET exp=?, stone=?, power=? WHERE user_id=?",
            (HUGE_EXP, HUGE_STONE, HUGE_POWER, "u1"),
        )
        row = self.connection.execute(
            "SELECT exp, stone, power FROM user_xiuxian WHERE user_id='u1'"
        ).fetchone()

        # SQLite 的数值亲和性把超 int64 值存成 REAL（历史存储边界），
        # 关键是语句没有因 OverflowError 中断，且量级与相对精度还在。
        for stored, expected in zip(row, (HUGE_EXP, HUGE_STONE, HUGE_POWER)):
            self.assertAlmostEqual(expected / stored, 1.0, places=12)

    def test_executemany_adapts_every_row(self) -> None:
        self.connection.executemany(
            "INSERT INTO user_xiuxian VALUES (?,?,?,?)",
            [("u2", HUGE_EXP, 0, 0), ("u3", 5, HUGE_STONE, 0)],
        )
        count = self.connection.execute("SELECT COUNT(*) FROM user_xiuxian").fetchone()[0]
        self.assertEqual(3, count)

    def test_int64_range_value_is_exact_integer(self) -> None:
        self.connection.execute(
            "UPDATE user_xiuxian SET exp=? WHERE user_id='u1'", (SQLITE_MAX_INT,)
        )
        row = self.connection.execute(
            "SELECT exp, typeof(exp) FROM user_xiuxian WHERE user_id='u1'"
        ).fetchone()
        self.assertEqual(SQLITE_MAX_INT, row[0])
        self.assertEqual("integer", row[1])

    def test_adapt_param_is_the_shared_implementation(self) -> None:
        self.assertEqual(bind_sqlite_param(HUGE_POWER), db_backend._adapt_param(HUGE_POWER))
        self.assertEqual(
            (str(HUGE_POWER), 5), db_backend._adapt_params((HUGE_POWER, 5))
        )


class UnitOfWorkBoundaryTests(unittest.TestCase):
    """新 feature 路径：``DatabaseUnitOfWork`` 同样覆盖大数值写入。"""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "player.db"
        with DatabaseUnitOfWork(self.path) as uow:
            uow.execute(
                "CREATE TABLE user_xiuxian (user_id TEXT PRIMARY KEY, exp INTEGER,"
                " stone INTEGER, power NUMERIC)"
            )
            uow.execute("INSERT INTO user_xiuxian VALUES (?,?,?,?)", ("u1", 0, 0, 0))

    def test_large_values_write_and_read_back(self) -> None:
        with DatabaseUnitOfWork(self.path, immediate=True) as uow:
            uow.execute(
                "UPDATE user_xiuxian SET exp=?, stone=?, power=? WHERE user_id=?",
                (HUGE_EXP, HUGE_STONE, HUGE_POWER, "u1"),
            )
            changed = uow.execute(
                "UPDATE user_xiuxian SET exp=exp+? WHERE user_id=? AND exp=?",
                (10, "u1", HUGE_EXP),
            )
            self.assertEqual(1, changed.rowcount)

        with DatabaseUnitOfWork(self.path, read_only=True) as uow:
            row = uow.query_one("SELECT exp, stone, power FROM user_xiuxian WHERE user_id='u1'")
        for stored, expected in zip(
            (row["exp"], row["stone"], row["power"]),
            (HUGE_EXP + 10, HUGE_STONE, HUGE_POWER),
        ):
            self.assertAlmostEqual(expected / stored, 1.0, places=12)

    def test_executemany_and_readonly_queries_use_the_guard(self) -> None:
        with DatabaseUnitOfWork(self.path, immediate=True) as uow:
            uow.executemany(
                "INSERT INTO user_xiuxian VALUES (?,?,?,?)",
                [(f"u{i}", HUGE_EXP + i, HUGE_STONE, HUGE_POWER) for i in range(2, 6)],
            )
        with DatabaseUnitOfWork(self.path, read_only=True) as uow:
            rows = uow.query_all("SELECT exp FROM user_xiuxian WHERE user_id<>'u1'")
            self.assertEqual(4, len(rows))
            self.assertEqual(
                "u2", uow.query_one("SELECT user_id FROM user_xiuxian WHERE exp=?", (HUGE_EXP + 2,))["user_id"]
            )

    def test_adapt_sqlite_params_shapes(self) -> None:
        self.assertEqual((), adapt_sqlite_params(None))
        self.assertEqual((str(HUGE_POWER),), adapt_sqlite_params(HUGE_POWER))
        self.assertEqual(
            {"a": str(HUGE_POWER), "b": 1}, adapt_sqlite_params({"a": HUGE_POWER, "b": 1})
        )
        self.assertEqual((str(HUGE_POWER), 1), adapt_sqlite_params([HUGE_POWER, 1]))


if __name__ == "__main__":
    unittest.main()
