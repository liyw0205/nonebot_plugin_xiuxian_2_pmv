from __future__ import annotations

import os
import zipfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from ..creation_application import (
    PluginBackupCreationApplication,
)
from ..creation_repository import (
    PluginBackupCreationRepository,
)


class FakeRuntime:
    def __init__(self) -> None:
        self.cloud_enabled = False
        self.upload_result = (True, "uploaded")
        self.cleanup_result = (True, "cleaned")
        self.cleanup_calls = 0

    def plugin_backup_now(self) -> datetime:
        return datetime(2026, 10, 7, 1, 2, 3)

    def plugin_backup_version(self) -> str:
        return "v1.2.3"

    def plugin_backup_cloud_enabled(self) -> bool:
        return self.cloud_enabled

    def plugin_backup_keep_days(self) -> int:
        return 10

    def plugin_backup_upload_cloud(self, path: Path) -> tuple[bool, str]:
        return self.upload_result

    def plugin_backup_cleanup_cloud(self) -> tuple[bool, str]:
        self.cleanup_calls += 1
        return self.cleanup_result


def _repository(root: Path) -> tuple[PluginBackupCreationRepository, Path, Path]:
    data = root / "data" / "xiuxian"
    plugin = root / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
    repository = PluginBackupCreationRepository(
        data / "backups", data, plugin, root
    )
    return repository, data, plugin


def test_archive_keeps_legacy_paths_and_skips_transient_and_cache_files(tmp_path: Path) -> None:
    repository, data, plugin = _repository(tmp_path)
    (data / "activity").mkdir(parents=True)
    (data / "cache" / "nested").mkdir(parents=True)
    (data / "config.json").write_text("{}", encoding="utf-8")
    (data / "message.db").write_bytes(b"message")
    (data / "message.db-wal").write_bytes(b"wal")
    (data / "activity" / "activity.db").write_bytes(b"activity")
    (data / "cache" / "nested" / "large.bin").write_bytes(b"cache")
    (plugin / "xiuxian").mkdir(parents=True)
    (plugin / "xiuxian" / "module.py").write_text("value = 1", encoding="utf-8")
    (plugin / "font").mkdir()
    (plugin / "font" / "large.ttf").write_bytes(b"font")

    visited: list[Path] = []
    real_walk = os.walk

    def observed_walk(*args, **kwargs):
        for root, dirs, files in real_walk(*args, **kwargs):
            visited.append(Path(root))
            yield root, dirs, files

    with patch("nonebot_plugin_xiuxian_2.features.plugin_backups.creation_repository.os.walk", observed_walk):
        archive_path = repository.create_local_backup(
            datetime(2026, 10, 7, 1, 2, 3), "v1.2.3"
        )

    assert archive_path.name == "backup_20261007_010203_v1.2.3.zip"
    with zipfile.ZipFile(archive_path) as archive:
        names = set(archive.namelist())
    assert "data/xiuxian/config.json" in names
    assert "src/plugins/nonebot_plugin_xiuxian_2/xiuxian/module.py" in names
    assert not any("message.db" in name or "activity.db" in name for name in names)
    assert not any("/cache/" in name or "/font/" in name for name in names)
    assert all("cache" not in path.parts for path in visited)


def test_archive_failure_preserves_existing_archive_and_removes_temporary_file(tmp_path: Path) -> None:
    repository, data, _plugin = _repository(tmp_path)
    data.mkdir(parents=True)
    existing = data / "backups" / "backup_20261007_010203_v1.2.3.zip"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"old archive")

    with patch(
        "nonebot_plugin_xiuxian_2.features.plugin_backups.creation_repository.zipfile.ZipFile",
        side_effect=RuntimeError("archive failed"),
    ):
        try:
            repository.create_local_backup(
                datetime(2026, 10, 7, 1, 2, 3), "v1.2.3"
            )
        except RuntimeError:
            pass
        else:
            raise AssertionError("archive failure should propagate to the application")

    assert existing.read_bytes() == b"old archive"
    assert sorted(path.name for path in existing.parent.iterdir()) == [existing.name]


def test_archive_preserves_data_paths_when_data_and_plugin_have_different_roots(tmp_path: Path) -> None:
    data = tmp_path / "volume" / "data" / "xiuxian"
    archive_root = tmp_path / "deployment"
    plugin = archive_root / "src" / "plugins" / "nonebot_plugin_xiuxian_2"
    data.mkdir(parents=True)
    plugin.mkdir(parents=True)
    (data / "config.json").write_text("{}", encoding="utf-8")
    (plugin / "module.py").write_text("value = 1", encoding="utf-8")
    repository = PluginBackupCreationRepository(data / "backups", data, plugin, archive_root)

    path = repository.create_local_backup(datetime(2026, 10, 7), "v1")

    with zipfile.ZipFile(path) as archive:
        assert set(archive.namelist()) == {
            "data/xiuxian/config.json",
            "src/plugins/nonebot_plugin_xiuxian_2/module.py",
        }


def test_cleanup_uses_backup_timestamp_and_never_follows_symlinks(tmp_path: Path) -> None:
    repository, _data, _plugin = _repository(tmp_path)
    backup_directory = tmp_path / "data" / "xiuxian" / "backups"
    backup_directory.mkdir(parents=True)
    old_archive = backup_directory / "backup_20260901_000000_v1.zip"
    current_archive = backup_directory / "backup_20261006_000000_v1.zip"
    target = backup_directory / "target.zip"
    old_archive.write_bytes(b"old")
    current_archive.write_bytes(b"current")
    target.write_bytes(b"target")
    link = backup_directory / "backup_20260801_000000_link.zip"
    try:
        link.symlink_to(target)
    except OSError:
        return

    repository.cleanup_local_backups(datetime(2026, 10, 7), 10)

    assert not old_archive.exists()
    assert current_archive.exists()
    assert target.exists()
    assert link.is_symlink()


def test_creation_upload_and_cleanup_failures_do_not_fail_local_backup(tmp_path: Path) -> None:
    repository, data, plugin = _repository(tmp_path)
    data.mkdir(parents=True)
    plugin.mkdir(parents=True)
    runtime = FakeRuntime()
    runtime.cloud_enabled = True
    runtime.upload_result = (False, "offline")
    runtime.cleanup_result = (False, "cleanup failed")
    application = PluginBackupCreationApplication(repository, runtime)

    result = application.create_backup_with_details()

    assert result.success is True
    assert result.cloud_uploaded is False
    assert Path(result.result).is_file()
    assert runtime.cleanup_calls == 0

    runtime.upload_result = (True, "uploaded")
    result = application.create_backup_with_details(defer_cloud_cleanup=True)
    assert result.success is True
    assert result.cloud_uploaded is True
    assert runtime.cleanup_calls == 0
    application.cleanup_cloud_backups()
    assert runtime.cleanup_calls == 1


def test_create_backup_lock_is_released_after_archive_failure(tmp_path: Path) -> None:
    repository, data, plugin = _repository(tmp_path)
    data.mkdir(parents=True)
    plugin.mkdir(parents=True)
    runtime = FakeRuntime()
    application = PluginBackupCreationApplication(repository, runtime)
    with patch.object(repository, "create_local_backup", side_effect=RuntimeError("disk error")):
        result = application.create_backup_with_details()
    assert result.success is False

    result = application.create_backup_with_details()
    assert result.success is True
