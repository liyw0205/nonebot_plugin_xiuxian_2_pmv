from __future__ import annotations

import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import (
    download_xiuxian_data as backup_module,
)


class BackupSelectionTests(unittest.TestCase):
    def test_database_backup_contains_only_core_databases(self) -> None:
        self.assertEqual(
            backup_module.UpdateManager.__new__(backup_module.UpdateManager)._sqlite_db_names(),
            ["xiuxian.db", "xiuxian_impart.db", "player.db", "trade.db"],
        )
        self.assertNotIn("message.db", backup_module.CORE_SQLITE_DATABASES)
        self.assertNotIn("activity.db", backup_module.CORE_SQLITE_DATABASES)

    def test_full_backup_excludes_transient_databases(self) -> None:
        data_dir = Path("/tmp/data/xiuxian")
        self.assertTrue(
            backup_module._is_transient_backup_file(
                data_dir / "activity" / "activity.db",
                data_dir,
            )
        )
        self.assertTrue(
            backup_module._is_transient_backup_file(data_dir / "message.db", data_dir)
        )
        self.assertTrue(
            backup_module._is_transient_backup_file(
                data_dir / "activity" / "activity.db-wal",
                data_dir,
            )
        )
        self.assertTrue(
            backup_module._is_transient_backup_file(data_dir / "message.db-shm", data_dir)
        )
        self.assertFalse(
            backup_module._is_transient_backup_file(data_dir / "player.db", data_dir)
        )


if __name__ == "__main__":
    unittest.main()
