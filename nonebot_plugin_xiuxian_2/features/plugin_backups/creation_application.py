from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from .creation_repository import PluginBackupCreationRepository


_logger = logging.getLogger(__name__)


class PluginBackupCreationRuntime(Protocol):
    def plugin_backup_now(self) -> datetime: ...

    def plugin_backup_version(self) -> str: ...

    def plugin_backup_cloud_enabled(self) -> bool: ...

    def plugin_backup_keep_days(self) -> int: ...

    def plugin_backup_upload_cloud(self, path: Path) -> tuple[bool, str]: ...

    def plugin_backup_cleanup_cloud(self) -> tuple[bool, str]: ...


@dataclass(frozen=True, slots=True)
class PluginBackupCreationResult:
    success: bool
    result: Path | str
    cloud_uploaded: bool = False


class PluginBackupCreationApplication:
    _create_lock = threading.Lock()

    def __init__(
        self,
        repository: PluginBackupCreationRepository,
        runtime: PluginBackupCreationRuntime,
    ) -> None:
        self._repository = repository
        self._runtime = runtime

    def create_backup(self) -> tuple[bool, Path | str]:
        result = self.create_backup_with_details()
        return result.success, result.result

    def create_backup_with_details(
        self, *, defer_cloud_cleanup: bool = False
    ) -> PluginBackupCreationResult:
        if not self._create_lock.acquire(blocking=False):
            return PluginBackupCreationResult(False, "已有插件备份任务正在执行")
        try:
            return self._create_backup_with_details(
                defer_cloud_cleanup=defer_cloud_cleanup
            )
        finally:
            self._create_lock.release()

    def _create_backup_with_details(
        self, *, defer_cloud_cleanup: bool
    ) -> PluginBackupCreationResult:
        try:
            now = self._runtime.plugin_backup_now()
            path = self._repository.create_local_backup(
                now, self._runtime.plugin_backup_version()
            )
        except Exception as exc:
            _logger.exception("插件备份失败")
            return PluginBackupCreationResult(False, str(exc))

        cloud_uploaded = False
        try:
            if self._runtime.plugin_backup_cloud_enabled():
                uploaded, message = self._runtime.plugin_backup_upload_cloud(path)
                cloud_uploaded = uploaded
                if uploaded:
                    _logger.info("云备份结果: %s", message)
                    if not defer_cloud_cleanup:
                        self.cleanup_cloud_backups()
                else:
                    _logger.warning("云备份失败: %s", message)
        except Exception as exc:
            _logger.warning("云备份执行异常: %s", exc)

        try:
            cleaned, message = self._repository.cleanup_local_backups(
                now, self._runtime.plugin_backup_keep_days()
            )
            if not cleaned:
                _logger.warning("插件本地旧备份清理失败: %s", message)
        except Exception as exc:
            _logger.warning("插件本地旧备份清理异常: %s", exc)
        return PluginBackupCreationResult(True, path, cloud_uploaded)

    def cleanup_cloud_backups(self) -> None:
        try:
            cleaned, message = self._runtime.plugin_backup_cleanup_cloud()
            if cleaned:
                _logger.info("%s", message)
            else:
                _logger.warning("%s", message)
        except Exception as exc:
            _logger.warning("云端旧备份清理异常: %s", exc)


__all__ = [
    "PluginBackupCreationApplication",
    "PluginBackupCreationResult",
    "PluginBackupCreationRuntime",
]
