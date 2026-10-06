from __future__ import annotations

from typing import BinaryIO

from .file_repository import (
    InvalidPluginBackupFile,
    PluginBackupFileNotFound,
    PluginBackupFileRepository,
)


class PluginBackupFileApplication:
    def __init__(self, repository: PluginBackupFileRepository) -> None:
        self._repository = repository

    def open_plugin_backup(self, filename: str) -> BinaryIO:
        return self._repository.open_plugin_backup(filename)

    def delete_plugin_backup(self, filename: str) -> None:
        self._repository.delete_plugin_backup(filename)

    def delete_plugin_backups(
        self, filenames: list[object]
    ) -> tuple[list[str], list[dict[str, str]]]:
        deleted: list[str] = []
        failed: list[dict[str, str]] = []
        for value in filenames:
            filename = str(value)
            try:
                self._repository.delete_plugin_backup(filename)
            except InvalidPluginBackupFile:
                failed.append({"filename": filename, "reason": "无效文件名"})
            except PluginBackupFileNotFound:
                failed.append({"filename": filename, "reason": "文件不存在"})
            except OSError:
                failed.append({"filename": filename, "reason": "删除失败"})
            else:
                deleted.append(filename)
        return deleted, failed


__all__ = ["PluginBackupFileApplication"]
