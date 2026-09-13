from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.accessory_package.repository import AccessoryPackagePlayerRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork


class AccessorySchemaPolicyTests(unittest.TestCase):
    def test_require_existing_rejects_missing_player_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "player.db"
            repository = AccessoryPackagePlayerRepository(schema_policy="require_existing")
            with self.assertRaisesRegex(RuntimeError, "namespace reconciliation"):
                with DatabaseUnitOfWork(str(database)) as uow:
                    repository.ensure_schema(uow)


if __name__ == "__main__":
    unittest.main()
