from __future__ import annotations

import threading

from .repository import ConfigBackupProvider, PluginBackupCreationPort
from .schemas import ManualBackupResult


class ManualBackupApplication:
    _backup_lock = threading.Lock()

    def __init__(
        self,
        plugin_backup: PluginBackupCreationPort,
        config_backup: ConfigBackupProvider,
    ) -> None:
        self._plugin_backup = plugin_backup
        self._config_backup = config_backup

    def create_backup(self) -> ManualBackupResult:
        if not self._backup_lock.acquire(blocking=False):
            return ManualBackupResult(
                False, "未执行", "未执行", "已有手动备份任务正在执行"
            )
        try:
            plugin = self._plugin_backup.create_backup_with_details(
                defer_cloud_cleanup=True
            )
            config_success, config_result, config_cloud_uploaded = (
                self._config_backup.backup_all_configs_with_details(
                    defer_cloud_cleanup=True
                )
            )

            if plugin.cloud_uploaded or config_cloud_uploaded:
                self._plugin_backup.cleanup_cloud_backups()

            errors: list[str] = []
            if not plugin.success:
                errors.append(f"插件备份失败: {plugin.result}")
            if not config_success:
                errors.append(f"配置备份失败: {config_result}")
            return ManualBackupResult(
                not errors,
                plugin.result,
                config_result,
                "; ".join(errors),
            )
        finally:
            self._backup_lock.release()


__all__ = ["ManualBackupApplication", "ManualBackupResult"]
