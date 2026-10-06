from __future__ import annotations

import os
import re
import shutil
import stat
import tempfile
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree as ET

import requests


MAX_CLOUD_LIST_BYTES = 2 * 1024 * 1024
MAX_CLOUD_LIST_ENTRIES = 1_000
MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES = 4 * 1024 * 1024 * 1024
RESTORE_DISK_RESERVE_BYTES = 64 * 1024 * 1024
_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_DAV_NS = {"d": "DAV:"}


class InvalidCloudPluginBackup(ValueError):
    pass


class PluginBackupCloudRuntime(Protocol):
    def plugin_backup_webdav_paths(self) -> tuple[bool, str, dict[str, Any] | None]: ...

    def plugin_backup_webdav_join_url(self, base_url: str, relative_path: str) -> str: ...

    def plugin_backup_webdav_format_time(self, value: str) -> str: ...


def is_cloud_plugin_backup_filename(value: object) -> bool:
    if not isinstance(value, str) or len(value.encode("utf-8")) > 255:
        return False
    if (
        not value
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or "\x00" in value
        or _WINDOWS_DRIVE.match(value)
        or Path(value).name != value
    ):
        return False
    return value.lower().endswith(".zip")


class PluginBackupCloudRepository:
    def __init__(
        self,
        backup_directory: str | Path,
        runtime: PluginBackupCloudRuntime,
        *,
        request: Any = requests.request,
        get: Any = requests.get,
        delete: Any = requests.delete,
    ) -> None:
        self._backup_directory = Path(backup_directory)
        self._runtime = runtime
        self._request = request
        self._get = get
        self._delete = delete

    def list_cloud_backups(self) -> tuple[bool, list[dict[str, Any]] | str]:
        response = None
        try:
            ok, _message, paths = self._runtime.plugin_backup_webdav_paths()
            if not ok or paths is None:
                return False, "未配置 WebDAV 信息。请前往配置管理设置。"
            response = self._request(
                "PROPFIND",
                paths["plugin_url"],
                auth=paths["auth"],
                timeout=15,
                headers={"Depth": "1"},
                stream=True,
            )
            status = int(getattr(response, "status_code", 0))
            if status not in {200, 207}:
                return False, f"无法连接到 WebDAV (HTTP {status})。"

            payload = self._read_response(response, MAX_CLOUD_LIST_BYTES)
            lowered = payload.lower()
            if b"<!doctype" in lowered or b"<!entity" in lowered:
                raise InvalidCloudPluginBackup("WebDAV XML 不允许声明实体")
            root = ET.fromstring(payload)
            responses = root.findall("d:response", _DAV_NS)
            if len(responses) > MAX_CLOUD_LIST_ENTRIES:
                raise InvalidCloudPluginBackup("云端备份条目超过限制")

            backup_folder = PurePosixPath(paths.get("plugin_rel", "backups")).name
            entries: list[dict[str, Any]] = []
            for item in responses:
                href = item.findtext("d:href", default="", namespaces=_DAV_NS)
                if not href:
                    continue
                name = unquote(PurePosixPath(urlsplit(href).path.rstrip("/")).name)
                if not name or name == backup_folder:
                    continue
                resource_type = item.find(".//d:resourcetype", _DAV_NS)
                if (
                    resource_type is not None
                    and resource_type.find("d:collection", _DAV_NS) is not None
                ):
                    continue
                if not is_cloud_plugin_backup_filename(name):
                    continue

                size_text = item.findtext(
                    ".//d:getcontentlength", default="", namespaces=_DAV_NS
                )
                modified = item.findtext(
                    ".//d:getlastmodified", default="", namespaces=_DAV_NS
                )
                size = int(size_text) if size_text.isdigit() else 0
                entries.append(
                    {
                        "filename": name,
                        "size": size,
                        "modified": self._runtime.plugin_backup_webdav_format_time(
                            modified
                        ),
                    }
                )

            return True, sorted(
                entries, key=lambda entry: entry["modified"], reverse=True
            )
        except (ET.ParseError, InvalidCloudPluginBackup, OSError, ValueError) as exc:
            return False, f"WebDAV 访问异常: {exc}"
        except Exception as exc:
            return False, f"WebDAV 访问异常: {exc}"
        finally:
            self._close_response(response)

    def local_backup_exists(self, filename: str) -> bool:
        name = self._validate_filename(filename)
        path = self._backup_directory / name
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidCloudPluginBackup("本地备份不是普通文件")
        return True

    def download_cloud_backup(
        self, filename: str, *, overwrite: bool
    ) -> tuple[bool, Path | str]:
        try:
            name = self._validate_filename(filename)
            if not overwrite and self.local_backup_exists(name):
                return False, "FILE_EXISTS"
            ok, _message, paths = self._runtime.plugin_backup_webdav_paths()
            if not ok or paths is None:
                return False, "未配置 WebDAV 信息"

            self._backup_directory.mkdir(parents=True, exist_ok=True)
            target_path = self._backup_directory / name
            remote_relative_path = "/".join(
                part for part in (paths.get("plugin_rel", ""), name) if part
            )
            remote_url = self._runtime.plugin_backup_webdav_join_url(
                paths["base_url"], remote_relative_path
            )
            response = self._get(
                remote_url,
                auth=paths["auth"],
                timeout=300,
                stream=True,
            )
            temporary_path: Path | None = None
            try:
                status = int(getattr(response, "status_code", 0))
                if status != 200:
                    return False, f"下载失败: HTTP {status}"

                content_length = self._content_length(response)
                if content_length > MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES:
                    return False, "云端备份文件超过大小限制"
                self._check_disk_space(
                    self._backup_directory, content_length or 64 * 1024
                )
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".plugin-cloud-backup.", dir=self._backup_directory
                )
                temporary_path = Path(temporary_name)
                written = 0
                next_space_check = 16 * 1024 * 1024
                try:
                    output = os.fdopen(descriptor, "wb")
                except Exception:
                    os.close(descriptor)
                    raise
                with output:
                    for chunk in self._iter_response(response):
                        if not chunk:
                            continue
                        written += len(chunk)
                        if written > MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES:
                            raise InvalidCloudPluginBackup(
                                "云端备份文件超过大小限制"
                            )
                        if written >= next_space_check:
                            self._check_disk_space(
                                self._backup_directory, len(chunk)
                            )
                            next_space_check = written + 16 * 1024 * 1024
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())

                if content_length and written != content_length:
                    raise InvalidCloudPluginBackup("下载文件长度与声明值不符")
                if not zipfile.is_zipfile(temporary_path):
                    return False, "下载完成但文件不是有效 zip"

                if overwrite:
                    os.replace(temporary_path, target_path)
                else:
                    try:
                        os.link(temporary_path, target_path)
                    except FileExistsError:
                        return False, "FILE_EXISTS"
                    temporary_path.unlink()
                return True, target_path
            finally:
                self._close_response(response)
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
        except (InvalidCloudPluginBackup, OSError, ValueError) as exc:
            return False, str(exc)
        except Exception as exc:
            return False, f"下载过程中出错: {exc}"

    def delete_cloud_backup(self, filename: str) -> tuple[bool, str]:
        try:
            name = self._validate_filename(filename)
            ok, _message, paths = self._runtime.plugin_backup_webdav_paths()
            if not ok or paths is None:
                return False, "未配置 WebDAV 信息"
            remote_relative_path = "/".join(
                part for part in (paths.get("plugin_rel", ""), name) if part
            )
            remote_url = self._runtime.plugin_backup_webdav_join_url(
                paths["base_url"], remote_relative_path
            )
            response = self._delete(remote_url, auth=paths["auth"], timeout=20)
            try:
                status = int(getattr(response, "status_code", 0))
            finally:
                self._close_response(response)
            if status in {200, 202, 204}:
                return True, f"已删除云端文件: {name}"
            return False, f"删除失败 HTTP {status}"
        except (InvalidCloudPluginBackup, OSError, ValueError) as exc:
            return False, str(exc)
        except Exception as exc:
            return False, f"删除云端文件失败: {exc}"

    @staticmethod
    def _validate_filename(filename: str) -> str:
        if not is_cloud_plugin_backup_filename(filename):
            raise InvalidCloudPluginBackup("无效文件名")
        return filename

    @staticmethod
    def _content_length(response: Any) -> int:
        try:
            length = int((getattr(response, "headers", {}) or {}).get("content-length", 0))
        except (TypeError, ValueError):
            return 0
        return max(0, length)

    @classmethod
    def _read_response(cls, response: Any, limit: int) -> bytes:
        declared_length = cls._content_length(response)
        if declared_length > limit:
            raise InvalidCloudPluginBackup("WebDAV 响应超过大小限制")
        payload = bytearray()
        for chunk in cls._iter_response(response):
            if not chunk:
                continue
            payload.extend(chunk)
            if len(payload) > limit:
                raise InvalidCloudPluginBackup("WebDAV 响应超过大小限制")
        if declared_length and declared_length != len(payload):
            raise InvalidCloudPluginBackup("WebDAV 响应长度与声明值不符")
        return bytes(payload)

    @staticmethod
    def _iter_response(response: Any):
        iterator = getattr(response, "iter_content", None)
        if callable(iterator):
            yield from iterator(chunk_size=64 * 1024)
            return
        payload = getattr(response, "content", None)
        if payload is None:
            payload = str(getattr(response, "text", "")).encode("utf-8")
        if payload:
            yield bytes(payload)

    @staticmethod
    def _check_disk_space(directory: Path, required: int) -> None:
        free = shutil.disk_usage(directory).free
        reserve = max(RESTORE_DISK_RESERVE_BYTES, free // 10)
        if required > max(0, free - reserve):
            raise InvalidCloudPluginBackup("磁盘空间不足，无法安全下载云端备份")

    @staticmethod
    def _close_response(response: Any) -> None:
        close = getattr(response, "close", None)
        if callable(close):
            close()


__all__ = [
    "InvalidCloudPluginBackup",
    "MAX_CLOUD_LIST_BYTES",
    "MAX_CLOUD_LIST_ENTRIES",
    "MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES",
    "PluginBackupCloudRepository",
    "is_cloud_plugin_backup_filename",
]
