from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any, Protocol

from .repository import (
    DatabaseBackupNotFound,
    DatabaseBackupRepository,
    InvalidDatabaseBackup,
    PartialDatabaseRestoreError,
    is_database_backup_archive_name,
)
from .schemas import DATABASE_ALIASES, MAX_DATABASE_BACKUP_BATCH


_logger = logging.getLogger(__name__)


class DatabaseBackupRuntime(Protocol):
    def database_backup_cloud_enabled(self) -> bool: ...

    def database_backup_keep_days(self) -> int: ...

    def database_backup_cleanup_cloud(self) -> tuple[bool, str]: ...


class DatabaseBackupApplication:
    _create_lock = threading.Lock()

    def __init__(
        self,
        repository: DatabaseBackupRepository,
        runtime: DatabaseBackupRuntime,
    ) -> None:
        self._repository = repository
        self._runtime = runtime

    def create_backup(self) -> tuple[bool, str]:
        if not self._create_lock.acquire(blocking=False):
            return False, "已有数据库备份任务正在执行"
        try:
            success, result, database_names = self._repository.create_local_backup()
            if not success:
                return False, str(result)

            archive_path = Path(result)
            if self._runtime.database_backup_cloud_enabled():
                uploaded, upload_message = self._repository.upload_cloud_backup(
                    archive_path
                )
                if uploaded:
                    cleaned, cleanup_message = self._runtime.database_backup_cleanup_cloud()
                    if not cleaned:
                        _logger.warning("数据库云备份清理失败: %s", cleanup_message)
                else:
                    _logger.warning("数据库云备份失败: %s", upload_message)

            cleaned, cleanup_message = self._repository.cleanup_local_backups(
                self._runtime.database_backup_keep_days()
            )
            if not cleaned:
                _logger.warning("数据库本地旧备份清理失败: %s", cleanup_message)
            return (
                True,
                f"数据库备份完成: {archive_path.name}，已备份: {', '.join(database_names)}",
            )
        except Exception as exc:
            return False, f"数据库备份失败: {exc}"
        finally:
            self._create_lock.release()

    def list_local_backups(self) -> list[dict[str, Any]]:
        return self._repository.list_local_backups()

    def list_cloud_backups(self) -> tuple[bool, list[dict[str, Any]] | str]:
        return self._repository.list_cloud_backups()

    def sync_cloud_backup(
        self, filename: object, *, overwrite: bool = False
    ) -> tuple[bool, object]:
        if not is_database_backup_archive_name(filename):
            return False, "无效数据库备份文件名"
        try:
            return self._repository.download_cloud_backup(
                filename, overwrite=overwrite
            )
        except (InvalidDatabaseBackup, OSError, ValueError) as exc:
            return False, str(exc)

    def sync_cloud_backups(
        self, filenames: list[object], *, overwrite: bool = False
    ) -> tuple[list[str], list[str], list[dict[str, str]]]:
        synced: list[str] = []
        exists: list[str] = []
        failed: list[dict[str, str]] = []
        for index, value in enumerate(filenames):
            if index >= MAX_DATABASE_BACKUP_BATCH:
                failed.append(
                    {
                        "filename": "",
                        "reason": f"单次最多同步 {MAX_DATABASE_BACKUP_BATCH} 个文件",
                    }
                )
                break
            filename = str(value)
            if len(filename) > 255 or not is_database_backup_archive_name(filename):
                failed.append({"filename": filename[:255], "reason": "无效文件名"})
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

    def delete_local_backups(
        self, filenames: list[object]
    ) -> tuple[list[str], list[dict[str, str]]]:
        deleted: list[str] = []
        failed: list[dict[str, str]] = []
        for index, value in enumerate(filenames):
            if index >= MAX_DATABASE_BACKUP_BATCH:
                failed.append(
                    {
                        "filename": "",
                        "reason": f"单次最多删除 {MAX_DATABASE_BACKUP_BATCH} 个文件",
                    }
                )
                break
            filename = str(value)
            if len(filename.encode("utf-8")) > 255 or not is_database_backup_archive_name(filename):
                failed.append({"filename": filename[:255], "reason": "无效文件名"})
                continue
            success, message = self._repository.delete_local_backup(filename)
            if success:
                deleted.append(filename)
            else:
                failed.append({"filename": filename, "reason": message})
        return deleted, failed

    def delete_cloud_backups(
        self, filenames: list[object]
    ) -> tuple[list[str], list[dict[str, str]]]:
        deleted: list[str] = []
        failed: list[dict[str, str]] = []
        for index, value in enumerate(filenames):
            if index >= MAX_DATABASE_BACKUP_BATCH:
                failed.append(
                    {
                        "filename": "",
                        "reason": f"单次最多删除 {MAX_DATABASE_BACKUP_BATCH} 个文件",
                    }
                )
                break
            filename = str(value)
            if len(filename) > 255 or not is_database_backup_archive_name(filename):
                failed.append({"filename": filename[:255], "reason": "无效文件名"})
                continue
            success, message = self._repository.delete_cloud_backup(filename)
            if success:
                deleted.append(filename)
            else:
                failed.append({"filename": filename, "reason": message})
        return deleted, failed

    def delete_cloud_backup(self, filename: object) -> tuple[bool, str]:
        if not is_database_backup_archive_name(filename):
            return False, "无效数据库备份文件名"
        return self._repository.delete_cloud_backup(filename)

    def restore_local_backup(
        self, filename: object, selected_databases: list[object]
    ) -> tuple[bool, str]:
        selected = self._normalize_selected_databases(selected_databases)
        if not selected:
            return False, "至少选择一个数据库进行恢复"
        try:
            with self._repository.stage_restore(filename, selected) as staged:
                restored, skipped = self._repository.restore_staged(staged)
            message = f"恢复完成，已恢复: {restored}"
            if skipped:
                message += f"，备份中不存在: {skipped}"
            return True, message
        except DatabaseBackupNotFound:
            return False, f"备份文件不存在: {filename}"
        except PartialDatabaseRestoreError as exc:
            partial = f"，此前已恢复: {list(exc.restored)}" if exc.restored else ""
            return False, f"数据库恢复失败: {exc}{partial}"
        except (InvalidDatabaseBackup, OSError, RuntimeError, ValueError) as exc:
            return False, f"数据库恢复失败: {exc}"
        except Exception as exc:
            return False, f"数据库恢复失败: {exc}"

    def restore_cloud_backup(
        self, filename: object, selected_databases: list[object]
    ) -> tuple[bool, str]:
        if not is_database_backup_archive_name(filename):
            return False, "无效云端数据库备份文件名"
        selected = self._normalize_selected_databases(selected_databases)
        if not selected:
            return False, "至少选择一个数据库进行恢复"
        success, result = self._repository.download_cloud_backup(
            filename, overwrite=True
        )
        if not success:
            try:
                local_exists = self._repository.local_backup_exists(filename)
            except (InvalidDatabaseBackup, OSError, ValueError) as exc:
                return False, f"云端数据库恢复失败: {exc}"
            if not local_exists:
                return False, f"云端下载失败: {result}"
            _logger.warning(
                "[DB云恢复] 重新下载失败，尝试使用本地已有备份 %s: %s",
                filename,
                result,
            )
        return self.restore_local_backup(filename, selected)

    @staticmethod
    def _normalize_selected_databases(values: list[object]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values or []:
            alias = Path(str(value)).name
            name = DATABASE_ALIASES.get(alias)
            if name and name not in seen:
                seen.add(name)
                normalized.append(name)
        return normalized


__all__ = ["DatabaseBackupApplication"]
