from __future__ import annotations

from typing import Any

from .cloud_repository import (
    MAX_CLOUD_LIST_ENTRIES,
    InvalidCloudPluginBackup,
    PluginBackupCloudRepository,
    is_cloud_plugin_backup_filename,
)


MAX_CLOUD_BACKUP_BATCH = 100


class PluginBackupCloudApplication:
    def __init__(self, repository: PluginBackupCloudRepository) -> None:
        self._repository = repository

    def list_cloud_backups(self) -> tuple[bool, list[dict[str, Any]] | str]:
        success, result = self._repository.list_cloud_backups()
        if not success:
            return False, str(result)
        if not isinstance(result, list) or len(result) > MAX_CLOUD_LIST_ENTRIES:
            return False, "云端备份列表格式无效"
        return True, result

    def sync_cloud_backup(
        self, filename: str, *, overwrite: bool = False
    ) -> tuple[bool, object]:
        if not is_cloud_plugin_backup_filename(filename):
            return False, "无效文件名"
        try:
            if not overwrite and self._repository.local_backup_exists(filename):
                return False, "FILE_EXISTS"
            return self._repository.download_cloud_backup(filename, overwrite=overwrite)
        except (InvalidCloudPluginBackup, OSError, ValueError) as exc:
            return False, str(exc)

    def local_backup_exists(self, filename: str) -> bool:
        if not is_cloud_plugin_backup_filename(filename):
            raise InvalidCloudPluginBackup("无效文件名")
        return self._repository.local_backup_exists(filename)

    def sync_cloud_backups(
        self, filenames: list[object], *, overwrite: bool = False
    ) -> tuple[list[str], list[str], list[dict[str, str]]]:
        synced: list[str] = []
        exists: list[str] = []
        failed: list[dict[str, str]] = []
        for index, value in enumerate(filenames):
            if index >= MAX_CLOUD_BACKUP_BATCH:
                failed.append(
                    {"filename": "", "reason": f"单次最多同步 {MAX_CLOUD_BACKUP_BATCH} 个文件"}
                )
                break
            filename = str(value)
            if len(filename) > 255 or not is_cloud_plugin_backup_filename(filename):
                failed.append({"filename": filename[:255], "reason": "无效文件名"})
                continue
            if not overwrite:
                try:
                    if self._repository.local_backup_exists(filename):
                        exists.append(filename)
                        continue
                except (InvalidCloudPluginBackup, OSError, ValueError) as exc:
                    failed.append({"filename": filename, "reason": str(exc)})
                    continue
            success, result = self._repository.download_cloud_backup(
                filename, overwrite=overwrite
            )
            if success:
                synced.append(filename)
            elif result == "FILE_EXISTS":
                exists.append(filename)
            else:
                failed.append({"filename": filename, "reason": str(result)})
        return synced, exists, failed

    def delete_cloud_backup(self, filename: str) -> tuple[bool, str]:
        if not is_cloud_plugin_backup_filename(filename):
            return False, "无效文件名"
        return self._repository.delete_cloud_backup(filename)

    def delete_cloud_backups(
        self, filenames: list[object]
    ) -> tuple[list[str], list[dict[str, str]]]:
        deleted: list[str] = []
        failed: list[dict[str, str]] = []
        for index, value in enumerate(filenames):
            if index >= MAX_CLOUD_BACKUP_BATCH:
                failed.append(
                    {"filename": "", "reason": f"单次最多删除 {MAX_CLOUD_BACKUP_BATCH} 个文件"}
                )
                break
            filename = str(value)
            if len(filename) > 255 or not is_cloud_plugin_backup_filename(filename):
                failed.append({"filename": filename[:255], "reason": "无效文件名"})
                continue
            success, message = self._repository.delete_cloud_backup(filename)
            if success:
                deleted.append(filename)
            else:
                failed.append({"filename": filename, "reason": message})
        return deleted, failed


__all__ = ["MAX_CLOUD_BACKUP_BATCH", "PluginBackupCloudApplication"]
