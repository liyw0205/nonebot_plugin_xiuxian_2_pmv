from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import unittest

from ..bot_overview_repository import BotOverviewSqlRepository
from ....infrastructure.database import DatabaseUnitOfWork


class BotOverviewSqlRepositoryTests(unittest.TestCase):
    def test_snapshot_preserves_legacy_counts_and_does_not_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game_database = Path(directory) / "game.db"
            trade_database = Path(directory) / "trade.db"
            now = datetime(2026, 10, 4, 12, 0)

            with DatabaseUnitOfWork(game_database) as uow:
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT)")
                uow.executemany(
                    "INSERT INTO user_xiuxian(user_id) VALUES (?)",
                    [(f"user-{index}",) for index in range(5)],
                )
                uow.execute("CREATE TABLE user_cd(user_id TEXT, create_time TEXT)")
                uow.executemany(
                    "INSERT INTO user_cd(user_id, create_time) VALUES (?, ?)",
                    [
                        ("today-1", now.strftime("%Y-%m-%d 01:00:00")),
                        ("today-1", now.strftime("%Y-%m-%d 02:00:00")),
                        ("today-2", now.strftime("%Y-%m-%d 03:00:00")),
                        ("yesterday-1", (now - timedelta(days=1)).strftime("%Y-%m-%d 01:00:00")),
                        ("yesterday-2", (now - timedelta(days=1)).strftime("%Y-%m-%d 02:00:00")),
                        ("six-days-ago", (now - timedelta(days=6)).strftime("%Y-%m-%d 01:00:00")),
                        ("seven-days-ago", (now - timedelta(days=7)).strftime("%Y-%m-%d 01:00:00")),
                    ],
                )
                uow.execute("CREATE TABLE back(goods_num INTEGER)")
                uow.executemany("INSERT INTO back(goods_num) VALUES (?)", [(5,), (7,), (None,)])

            with DatabaseUnitOfWork(trade_database) as uow:
                uow.execute("CREATE TABLE xianshi_item(user_id TEXT, quantity INTEGER)")
                uow.execute("CREATE TABLE guishi_item(user_id TEXT, item_type TEXT, quantity INTEGER)")
                uow.executemany(
                    "INSERT INTO xianshi_item(user_id, quantity) VALUES (?, ?)",
                    [("0", 999), ("seller", 2), ("empty", 0)],
                )
                uow.executemany(
                    "INSERT INTO guishi_item(user_id, item_type, quantity) VALUES (?, ?, ?)",
                    [
                        ("seller-2", "baitan", 3),
                        ("seller-3", "摆摊", 4),
                        ("buyer", "qiugou", 200),
                        ("0", "baitan", 10),
                        ("seller-4", "baitan", -1),
                    ],
                )

            repository = BotOverviewSqlRepository(game_database, trade_database)
            snapshot = repository.snapshot(now=now)
            self.assertEqual(
                (
                    snapshot.total_users,
                    snapshot.today_active_users,
                    snapshot.yesterday_active_users,
                    snapshot.last_7days_active_users,
                    snapshot.total_items_quantity,
                    snapshot.total_goods_quantity,
                ),
                (5, 2, 2, 5, 12, 9),
            )

            with DatabaseUnitOfWork(game_database) as uow:
                uow.execute("INSERT INTO back(goods_num) VALUES (3)")
            with DatabaseUnitOfWork(trade_database) as uow:
                uow.execute("UPDATE xianshi_item SET quantity=3 WHERE user_id='seller'")

            refreshed = repository.snapshot(now=now)
            self.assertEqual(refreshed.total_items_quantity, 15)
            self.assertEqual(refreshed.total_goods_quantity, 10)

    def test_missing_database_or_table_returns_unavailable_without_creating_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            game_database = Path(directory) / "missing-game.db"
            trade_database = Path(directory) / "missing-trade.db"
            repository = BotOverviewSqlRepository(game_database, trade_database)

            missing = repository.snapshot(now=datetime(2026, 10, 4))
            self.assertEqual(
                (
                    missing.total_users,
                    missing.today_active_users,
                    missing.yesterday_active_users,
                    missing.last_7days_active_users,
                    missing.total_items_quantity,
                    missing.total_goods_quantity,
                ),
                (None, None, None, None, None, None),
            )
            self.assertFalse(game_database.exists())
            self.assertFalse(trade_database.exists())

            with DatabaseUnitOfWork(game_database) as uow:
                uow.execute("CREATE TABLE user_cd(user_id TEXT, create_time TEXT)")
                uow.execute("CREATE TABLE back(goods_num INTEGER)")
            with DatabaseUnitOfWork(trade_database) as uow:
                uow.execute("CREATE TABLE xianshi_item(user_id TEXT, quantity INTEGER)")

            incomplete = repository.snapshot(now=datetime(2026, 10, 4))
            self.assertEqual(
                (
                    incomplete.total_users,
                    incomplete.today_active_users,
                    incomplete.yesterday_active_users,
                    incomplete.last_7days_active_users,
                    incomplete.total_items_quantity,
                    incomplete.total_goods_quantity,
                ),
                (None, None, None, None, None, None),
            )
            table_query = "SELECT name FROM sqlite_master WHERE type='table'"
            with DatabaseUnitOfWork(game_database, read_only=True) as uow:
                tables = {row["name"] for row in uow.query_all(table_query)}
            with DatabaseUnitOfWork(trade_database, read_only=True) as uow:
                tables |= {row["name"] for row in uow.query_all(table_query)}
            self.assertNotIn("user_xiuxian", tables)
            self.assertNotIn("guishi_item", tables)


if __name__ == "__main__":
    unittest.main()
