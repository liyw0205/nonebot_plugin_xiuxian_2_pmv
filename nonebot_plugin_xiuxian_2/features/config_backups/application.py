from __future__ import annotations

import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from .repository import ConfigBackupRepository, InvalidConfigBackup


_logger = logging.getLogger(__name__)


class ConfigBackupRuntime(Protocol):
    def configuration_backup_values(self) -> dict[str, Any]: ...

    def configuration_backup_version(self) -> str: ...

    def configuration_backup_now(self) -> datetime: ...

    def configuration_backup_cloud_enabled(self) -> bool: ...

    def configuration_backup_keep_days(self) -> int: ...

    def configuration_backup_cleanup_cloud(self) -> tuple[bool, str]: ...

    def configuration_backup_save_values(self, values: dict[str, Any]) -> tuple[bool, str]: ...


class ConfigBackupApplication:
    _create_lock = threading.Lock()

    def __init__(
        self,
        repository: ConfigBackupRepository,
        runtime: ConfigBackupRuntime,
    ) -> None:
        self._repository = repository
        self._runtime = runtime

    def export_config(
        self, selected_fields: object = None, *, export_all: bool = False
    ) -> tuple[dict[str, Any], str]:
        now = self._runtime.configuration_backup_now()
        fields = selected_fields if isinstance(selected_fields, list) else []
        config_values = self._runtime.configuration_backup_values()
        export_data = self._select_values(config_values, fields, export_all)
        export_data["_metadata"] = {
            "backup_time": now.isoformat(),
            "backup_fields": list(export_data.keys()) if export_all else fields,
            "version": self._runtime.configuration_backup_version(),
        }
        return export_data, f"xiuxian_config_export_{now.strftime('%Y%m%d_%H%M%S')}.json"

    def import_config(self, filename: object, stream: Any) -> Any:
        imported = self._repository.parse_uploaded_config(filename, stream)
        if isinstance(imported, dict):
            imported.pop("_metadata", None)
        return imported

    def create_local_backup(
        self, selected_fields: object = None, *, backup_all: bool = False
    ) -> Path:
        if not self._create_lock.acquire(blocking=False):
            raise InvalidConfigBackup("已有配置备份任务正在执行")
        try:
            now = self._runtime.configuration_backup_now()
            fields = selected_fields if isinstance(selected_fields, list) else []
            values = self._runtime.configuration_backup_values()
            backup_data = self._select_values(values, fields, backup_all)
            backup_data["_metadata"] = {
                "backup_time": now.isoformat(),
                "backup_fields": list(backup_data.keys()) if backup_all else fields,
                "version": self._runtime.configuration_backup_version(),
            }
            path = self._repository.create_local_backup(
                f"config_backup_{now.strftime('%Y%m%d_%H%M%S')}.json", backup_data
            )
            return path
        finally:
            self._create_lock.release()

    def backup_all_configs(self) -> tuple[bool, Path | str]:
        if not self._create_lock.acquire(blocking=False):
            return False, "已有配置备份任务正在执行"
        try:
            now = self._runtime.configuration_backup_now()
            values = self._runtime.configuration_backup_values()
            backup_data = dict(values)
            backup_data["_metadata"] = {
                "backup_time": now.isoformat(),
                "backup_fields": list(values.keys()),
                "version": self._runtime.configuration_backup_version(),
                "type": "config_backup",
                "backup_type": "full",
            }
            path = self._repository.create_local_backup(
                f"config_backup_{now.strftime('%Y%m%d_%H%M%S')}.json", backup_data
            )
            if self._runtime.configuration_backup_cloud_enabled():
                uploaded, message = self._repository.upload_cloud_backup(path.name)
                if uploaded:
                    self._cleanup_cloud()
                else:
                    _logger.warning("配置云备份失败: %s", message)
            self._cleanup_local(now)
            return True, path
        except Exception as exc:
            _logger.exception("配置备份失败")
            return False, f"配置备份失败: {exc}"
        finally:
            self._create_lock.release()

    def backup_cloud_config(self) -> tuple[bool, object]:
        if not self._create_lock.acquire(blocking=False):
            return False, "已有配置备份任务正在执行"
        try:
            now = self._runtime.configuration_backup_now()
            values = self._runtime.configuration_backup_values()
            backup_data = dict(values)
            backup_data["_metadata"] = {
                "backup_time": now.isoformat(),
                "backup_fields": list(values.keys()),
                "version": self._runtime.configuration_backup_version(),
                "type": "config_backup",
                "backup_type": "full",
            }
            path = self._repository.create_local_backup(
                f"config_backup_{now.strftime('%Y%m%d_%H%M%S')}.json", backup_data
            )
            uploaded, message = self._repository.upload_cloud_backup(path.name)
            self._cleanup_local(now)
            if not uploaded:
                return False, message
            if self._runtime.configuration_backup_cloud_enabled():
                self._cleanup_cloud()
            return True, path
        except Exception as exc:
            return False, f"云备份失败: {exc}"
        finally:
            self._create_lock.release()

    def list_local_backups(self) -> list[dict[str, Any]]:
        return self._repository.list_local_backups()

    def restore_local_backup(
        self, filename: object
    ) -> tuple[bool, dict[str, Any] | str]:
        try:
            data, metadata = self._repository.read_local_backup(filename)
            return True, {"data": data, "metadata": metadata}
        except (InvalidConfigBackup, OSError, ValueError) as exc:
            return False, str(exc)

    def delete_local_backup(self, filename: object) -> tuple[bool, str]:
        try:
            self._repository.delete_local_backup(filename)
            return True, f"配置备份文件删除成功: {filename}"
        except FileNotFoundError:
            return False, f"备份文件不存在: {filename}"
        except (InvalidConfigBackup, OSError, ValueError) as exc:
            return False, str(exc)

    def list_cloud_backups(self) -> tuple[bool, list[dict[str, Any]] | str]:
        return self._repository.list_cloud_backups()

    def create_cloud_backup(self, local_path: str | Path) -> tuple[bool, str]:
        return self._repository.upload_cloud_backup(Path(local_path).name)

    def sync_cloud_backup(
        self, filename: object, *, overwrite: bool = False
    ) -> tuple[bool, object]:
        return self._repository.download_cloud_backup(filename, overwrite=overwrite)

    def restore_cloud_backup(
        self, filename: object
    ) -> tuple[bool, dict[str, Any] | str]:
        try:
            if not self._repository.local_backup_exists(filename):
                downloaded, result = self._repository.download_cloud_backup(
                    filename, overwrite=False
                )
                if not downloaded:
                    return False, f"云端下载失败: {result}"
            data, metadata = self._repository.read_local_backup(filename)
            return True, {
                "data": data,
                "metadata": metadata,
                "local_path": str(self._repository.local_backup_path(filename)),
            }
        except (InvalidConfigBackup, OSError, ValueError) as exc:
            return False, f"云恢复失败: {exc}"

    def restore_config_from_backup(self, backup_path: str | Path) -> tuple[bool, str]:
        try:
            values = self._repository.read_backup_path(backup_path)
            success, message = self._runtime.configuration_backup_save_values(values)
            if not success:
                return False, f"保存配置失败: {message}"
            return True, "配置恢复成功"
        except (InvalidConfigBackup, OSError, ValueError) as exc:
            return False, f"恢复配置失败: {exc}"

    @staticmethod
    def _select_values(
        values: dict[str, Any], fields: list[Any], select_all: bool
    ) -> dict[str, Any]:
        if select_all or not fields:
            return dict(values)
        return {field: values[field] for field in fields if isinstance(field, str) and field in values}

    def _cleanup_local(self, now: datetime) -> None:
        try:
            self._repository.cleanup_local_backups(
                self._runtime.configuration_backup_keep_days(), now
            )
        except Exception as exc:
            _logger.warning("配置本地旧备份清理异常: %s", exc)

    def _cleanup_cloud(self) -> None:
        try:
            cleaned, message = self._runtime.configuration_backup_cleanup_cloud()
            if not cleaned:
                _logger.warning("配置云备份清理失败: %s", message)
        except Exception as exc:
            _logger.warning("配置云备份清理异常: %s", exc)


__all__ = ["ConfigBackupApplication", "ConfigBackupRuntime"]
