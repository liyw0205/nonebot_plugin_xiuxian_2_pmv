from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.compatibility.legacy_xianshi_schema import (
    LegacyXianshiSchemaAdapter,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class LegacyXianshiSchemaAdapterTests(unittest.TestCase):
    def test_imports_legacy_listing_once_into_game_database(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            game = root / "game.db"
            legacy = root / "trade.db"
            with DatabaseUnitOfWork(legacy) as uow:
                uow.execute(
                    "CREATE TABLE xianshi_item ("
                    "id TEXT PRIMARY KEY,user_id TEXT,goods_id INTEGER,name TEXT,type TEXT,"
                    "price INTEGER,quantity INTEGER)"
                )
                uow.execute(
                    "INSERT INTO xianshi_item VALUES(?,?,?,?,?,?,?)",
                    ("listing-1", "seller", 1, "法器", "装备", 100, 2),
                )

            adapter = LegacyXianshiSchemaAdapter(game, max_goods_num=99)
            adapter.initialize(legacy)
            adapter.initialize(legacy)

            with DatabaseUnitOfWork(game, read_only=True) as uow:
                rows = uow.query_all("SELECT id,user_id,name,quantity FROM xianshi_item")
                self.assertEqual(
                    rows,
                    [{"id": "listing-1", "user_id": "seller", "name": "法器", "quantity": 2}],
                )


if __name__ == "__main__":
    unittest.main()
