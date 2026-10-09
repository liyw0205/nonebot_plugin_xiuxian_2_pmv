from __future__ import annotations

import logging
import os
import re
import stat
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath

from .schemas import (
    ARCHIVE_PREFIX,
    ARCHIVE_SUFFIX,
    ARCHIVE_TIMESTAMP_PATTERN as _TIMESTAMP,
    SKIP_DIRECTORY_NAMES as _SKIP_DIRECTORY_NAMES,
    TRANSIENT_DATA_PATHS as _TRANSIENT_DATA_PATHS,
    VERSION_SAFE_PATTERN as _SAFE_VERSION,
)


_logger = logging.getLogger(__name__)


def _is_transient_data_file(path: Path, data_root: Path) -> bool:
    try:
        relative = PurePosixPath(path.relative_to(data_root).as_posix())
    except ValueError:
        return path.name in {"message.db", "message.db-wal", "message.db-shm"}

    if (
        relative.parent == PurePosixPath(".")
        and relative.name.startswith(".message.db.")
        and relative.name.endswith(".migrating")
    ):
        return True

    relative_text = relative.as_posix()
    for suffix in ("-wal", "-shm"):
        if relative_text.endswith(suffix):
            relative = PurePosixPath(relative_text[: -len(suffix)])
            break
    if relative in _TRANSIENT_DATA_PATHS:
        return True
    return relative.name == "message.db"


def _version_component(value: object) -> str:
    component = _SAFE_VERSION.sub("_", str(value or "unknown")).strip("._")
    return component[:128] or "unknown"


class PluginBackupCreationRepository:
    def __init__(
        self,
        backup_directory: str | Path,
        data_directory: str | Path,
        plugin_directory: str | Path,
        archive_root: str | Path,
    ) -> None:
        self._backup_directory = Path(backup_directory)
        self._data_directory = Path(data_directory)
        self._plugin_directory = Path(plugin_directory)
        self._archive_root = Path(archive_root)

    def create_local_backup(self, now: datetime, version: object) -> Path:
        filename = (
            f"{ARCHIVE_PREFIX}{now.strftime('%Y%m%d_%H%M%S')}"
            f"_{_version_component(version)}{ARCHIVE_SUFFIX}"
        )
        self._backup_directory.mkdir(parents=True, exist_ok=True)
        target = self._backup_directory / filename
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".plugin-backup-", suffix=".tmp", dir=self._backup_directory
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w+b") as stream:
                with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
                    self._add_tree(archive, self._data_directory, transient_data=True)
                    self._add_tree(archive, self._plugin_directory, transient_data=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, target)
            return target
        finally:
            temporary_path.unlink(missing_ok=True)

    def cleanup_local_backups(self, now: datetime, keep_days: int) -> tuple[bool, str]:
        if keep_days <= 0:
            return True, "本地保留天数<=0，跳过清理"
        try:
            entries = os.scandir(self._backup_directory)
        except FileNotFoundError:
            return True, "备份目录不存在，跳过清理"

        deleted = 0
        with entries:
            for entry in entries:
                if not entry.name.startswith(ARCHIVE_PREFIX) or not entry.name.endswith(ARCHIVE_SUFFIX):
                    continue
                try:
                    metadata = entry.stat(follow_symlinks=False)
                    if not stat.S_ISREG(metadata.st_mode):
                        continue
                    timestamp_match = _TIMESTAMP.search(Path(entry.name).stem)
                    if timestamp_match:
                        created = datetime.strptime(
                            timestamp_match.group(0), "%Y%m%d_%H%M%S"
                        )
                    else:
                        created = datetime.fromtimestamp(metadata.st_mtime)
                    if (now - created).days > keep_days:
                        Path(entry.path).unlink()
                        deleted += 1
                except (OSError, ValueError):
                    continue
        return True, f"本地清理完成，删除 {deleted} 个旧插件备份"

    def _add_tree(
        self,
        archive: zipfile.ZipFile,
        source_root: Path,
        *,
        transient_data: bool,
    ) -> None:
        if not source_root.is_dir():
            return
        for root, directories, filenames in os.walk(source_root, topdown=True, followlinks=False):
            root_path = Path(root)
            kept_directories: list[str] = []
            for directory in directories:
                directory_path = root_path / directory
                if directory in _SKIP_DIRECTORY_NAMES or directory_path.is_symlink():
                    continue
                kept_directories.append(directory)
            directories[:] = kept_directories

            for filename in filenames:
                file_path = root_path / filename
                try:
                    metadata = file_path.lstat()
                    if not stat.S_ISREG(metadata.st_mode):
                        continue
                    if transient_data and _is_transient_data_file(
                        file_path, self._data_directory
                    ):
                        continue
                    archive_root = (
                        self._data_directory.parent.parent
                        if transient_data
                        else self._archive_root
                    )
                    archive_name = file_path.relative_to(archive_root).as_posix()
                    archive.write(file_path, archive_name)
                except Exception as exc:
                    _logger.warning("备份文件跳过: %s, 错误: %s", file_path, exc)


__all__ = ["PluginBackupCreationRepository"]
