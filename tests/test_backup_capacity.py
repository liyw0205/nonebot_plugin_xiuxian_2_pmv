from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.backups import create_blueprint
from nonebot_plugin_xiuxian_2.infrastructure.database import BackupService, DatabaseCatalog
from nonebot_plugin_xiuxian_2.infrastructure.database.backup_capacity import (
    BackupCapacityError,
    InsufficientBackupSpace,
    MINIMUM_BACKUP_RESERVE_BYTES,
    backup_reserve_bytes,
)


class BackupCapacityTests(unittest.TestCase):
    @staticmethod
    def _catalog(root: Path) -> DatabaseCatalog:
        return DatabaseCatalog.from_paths({key: root / f"{key}.db" for key in DatabaseCatalog.KEYS})

    @staticmethod
    def _database(path: Path, value: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS sample(value TEXT)")
            connection.execute("DELETE FROM sample")
            connection.execute("INSERT INTO sample(value) VALUES(?)", (value,))

    @staticmethod
    def _read_value(path: Path) -> str:
        with sqlite3.connect(path) as connection:
            return str(connection.execute("SELECT value FROM sample").fetchone()[0])

    def test_reserve_has_floor_and_scales_with_backup_size(self) -> None:
        self.assertEqual(backup_reserve_bytes(1), MINIMUM_BACKUP_RESERVE_BYTES)
        self.assertEqual(backup_reserve_bytes(1024**3), (1024**3 + 9) // 10)

    def test_insufficient_backup_space_does_not_create_target(self) -> None:
        with tempfile.TemporaryDirectory(prefix="backup-capacity-create-") as temp_dir:
            root = Path(temp_dir)
            self._database(root / "game_db.db", "before")
            target = root / "not-created" / "backups"

            with patch(
                "nonebot_plugin_xiuxian_2.infrastructure.database.backup_capacity.shutil.disk_usage",
                return_value=SimpleNamespace(free=1),
            ):
                with self.assertRaises(InsufficientBackupSpace):
                    BackupService(self._catalog(root)).create(target)

            self.assertFalse(target.exists())

    def test_capacity_probe_failure_does_not_create_target(self) -> None:
        with tempfile.TemporaryDirectory(prefix="backup-capacity-probe-") as temp_dir:
            root = Path(temp_dir)
            self._database(root / "game_db.db", "before")
            target = root / "not-created" / "backups"

            with patch(
                "nonebot_plugin_xiuxian_2.infrastructure.database.backup_capacity.shutil.disk_usage",
                side_effect=OSError("probe failed"),
            ):
                with self.assertRaises(BackupCapacityError):
                    BackupService(self._catalog(root)).create(target)

            self.assertFalse(target.exists())

    def test_create_cleans_partial_backup_after_write_failure(self) -> None:
        with tempfile.TemporaryDirectory(prefix="backup-capacity-cleanup-") as temp_dir:
            root = Path(temp_dir)
            self._database(root / "game_db.db", "before")
            target = root / "backups"
            service = BackupService(self._catalog(root))
            usage = SimpleNamespace(free=10 * MINIMUM_BACKUP_RESERVE_BYTES)

            with (
                patch(
                    "nonebot_plugin_xiuxian_2.infrastructure.database.backup_capacity.shutil.disk_usage",
                    return_value=usage,
                ),
                patch.object(service, "_digest", side_effect=OSError("digest failed")),
            ):
                with self.assertRaisesRegex(OSError, "digest failed"):
                    service.create(target)

            self.assertTrue(target.is_dir())
            self.assertEqual(list(target.iterdir()), [])

    def test_restore_preflights_all_files_before_replacing_any(self) -> None:
        with tempfile.TemporaryDirectory(prefix="backup-capacity-restore-") as temp_dir:
            root = Path(temp_dir)
            catalog = self._catalog(root)
            self._database(catalog.path("game_db"), "backup-game")
            self._database(catalog.path("player_db"), "backup-player")
            config = root / "config.json"
            config.write_text('{"value":"backup"}', encoding="utf-8")
            service = BackupService(catalog, extra_files={"config": config})
            backup = service.create(root / "backups")

            self._database(catalog.path("game_db"), "current-game")
            self._database(catalog.path("player_db"), "current-player")
            config.write_text('{"value":"current"}', encoding="utf-8")

            with patch(
                "nonebot_plugin_xiuxian_2.infrastructure.database.backup_capacity.shutil.disk_usage",
                return_value=SimpleNamespace(free=1),
            ):
                with self.assertRaises(InsufficientBackupSpace):
                    service.restore(backup)

            self.assertEqual(self._read_value(catalog.path("game_db")), "current-game")
            self.assertEqual(self._read_value(catalog.path("player_db")), "current-player")
            self.assertEqual(config.read_text(encoding="utf-8"), '{"value":"current"}')
            self.assertFalse(list(root.rglob("*.restore.tmp")))

    def test_web_backup_returns_explicit_insufficient_storage(self) -> None:
        with tempfile.TemporaryDirectory(prefix="backup-capacity-web-") as temp_dir:
            root = Path(temp_dir)
            config = root / "config.json"
            config.write_text("{}", encoding="utf-8")
            paths = SimpleNamespace(backups=root / "backups", config_file=config)
            context = SimpleNamespace(paths=paths, database=self._catalog(root))
            app = Flask(__name__)
            app.secret_key = "test"
            app.register_blueprint(create_blueprint(context=context))
            client = app.test_client()
            with client.session_transaction() as session:
                session["_csrf_token"] = "token"

            with patch(
                "nonebot_plugin_xiuxian_2.infrastructure.database.backup_capacity.shutil.disk_usage",
                return_value=SimpleNamespace(free=1),
            ):
                response = client.post(
                    "/api/v1/backups",
                    headers={"X-CSRF-Token": "token"},
                )

            self.assertEqual(response.status_code, 507)
            self.assertEqual(response.get_json()["error"]["code"], "insufficient_storage")
            self.assertFalse(paths.backups.exists())


if __name__ == "__main__":
    unittest.main()
