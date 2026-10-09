from __future__ import annotations

import json
import errno
import os
import re
import stat
import tempfile
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Protocol
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree as ET

import requests

from .schemas import (
    CONFIG_BACKUP_PREFIX,
    CONFIG_BACKUP_SUFFIX,
    MAX_CONFIG_BACKUP_BYTES,
    MAX_CONFIG_CLOUD_LIST_BYTES,
    MAX_CONFIG_CLOUD_LIST_ENTRIES,
    is_config_backup_filename,
)


_DAV_NS = {"d": "DAV:"}


class InvalidConfigBackup(ValueError):
    pass


class ConfigBackupRuntime(Protocol):
    def configuration_backup_webdav_paths(self): ...

    def configuration_backup_webdav_join_url(self, base_url: str, relative_path: str) -> str: ...

    def configuration_backup_webdav_make_directories(self, base_url: str, relative_path: str, auth): ...

    def configuration_backup_format_time(self, value: str) -> str: ...


class ConfigBackupRepository:
    def __init__(
        self,
        backup_directory: str | Path,
        runtime: ConfigBackupRuntime,
        *,
        request: Any = requests.request,
        get: Any = requests.get,
        put: Any = requests.put,
    ) -> None:
        self._backup_directory = Path(backup_directory)
        self._runtime = runtime
        self._request = request
        self._get = get
        self._put = put

    def create_local_backup(self, filename: str, data: dict[str, Any]) -> Path:
        self._validate_local_filename(filename)
        payload = self._encode_json(data)
        if len(payload) > MAX_CONFIG_BACKUP_BYTES:
            raise InvalidConfigBackup("配置备份超过大小限制")
        self._ensure_directory()
        target = self._backup_directory / filename
        fd, temporary_name = tempfile.mkstemp(
            prefix=".config-backup-", suffix=".tmp", dir=self._backup_directory
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, target)
            return target
        finally:
            temporary_path.unlink(missing_ok=True)

    def parse_uploaded_config(self, filename: object, stream: Any) -> Any:
        if not isinstance(filename, str) or not filename.lower().endswith(CONFIG_BACKUP_SUFFIX):
            raise InvalidConfigBackup("只支持JSON格式文件")
        payload = self._read_limited_stream(stream, MAX_CONFIG_BACKUP_BYTES)
        try:
            return json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidConfigBackup("文件格式错误，不是有效的JSON") from exc

    def list_local_backups(self) -> list[dict[str, Any]]:
        backups: list[dict[str, Any]] = []
        self._ensure_directory_parent()
        try:
            entries = os.scandir(self._backup_directory)
        except FileNotFoundError:
            return backups
        with entries:
            for entry in entries:
                if not entry.name.startswith(CONFIG_BACKUP_PREFIX) or not entry.name.lower().endswith(CONFIG_BACKUP_SUFFIX):
                    continue
                try:
                    metadata = entry.stat(follow_symlinks=False)
                    if not stat.S_ISREG(metadata.st_mode):
                        continue
                    payload = self._read_path(Path(entry.path))
                    document = self._decode_json(payload)
                    backup_metadata = document.get("_metadata", {})
                    if not isinstance(backup_metadata, dict):
                        continue
                except (OSError, InvalidConfigBackup):
                    continue
                backups.append(
                    {
                        "filename": entry.name,
                        "backup_time": backup_metadata.get("backup_time", ""),
                        "version": backup_metadata.get("version", "unknown"),
                        "size": metadata.st_size,
                        "created_at": datetime.fromtimestamp(metadata.st_ctime).isoformat(),
                    }
                )
        backups.sort(key=lambda item: item["created_at"], reverse=True)
        return backups

    def read_local_backup(self, filename: object) -> tuple[dict[str, Any], dict[str, Any]]:
        path = self._local_path(filename)
        document = self._decode_json(self._read_path(path))
        data = dict(document)
        metadata = data.pop("_metadata", {})
        if not isinstance(metadata, dict):
            raise InvalidConfigBackup("配置备份元数据格式无效")
        return data, metadata

    def read_backup_path(self, backup_path: str | Path) -> dict[str, Any]:
        self._ensure_directory_parent()
        path = Path(backup_path)
        try:
            path.resolve().relative_to(self._backup_directory.resolve())
        except ValueError as exc:
            raise InvalidConfigBackup("配置备份路径不在允许目录内") from exc
        document = self._decode_json(self._read_path(path))
        document.pop("_metadata", None)
        return document

    def delete_local_backup(self, filename: object) -> None:
        path = self._local_path(filename)
        try:
            metadata = path.lstat()
        except FileNotFoundError as exc:
            raise FileNotFoundError("配置备份文件不存在") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidConfigBackup("无效配置备份文件")
        path.unlink()

    def cleanup_local_backups(self, keep_days: int, now: datetime) -> None:
        if keep_days <= 0:
            return
        self._ensure_directory_parent()
        try:
            entries = os.scandir(self._backup_directory)
        except FileNotFoundError:
            return
        deadline = now.timestamp() - timedelta(days=keep_days).total_seconds()
        with entries:
            for entry in entries:
                if not entry.name.startswith(CONFIG_BACKUP_PREFIX) or not entry.name.lower().endswith(CONFIG_BACKUP_SUFFIX):
                    continue
                try:
                    metadata = entry.stat(follow_symlinks=False)
                    if stat.S_ISREG(metadata.st_mode) and metadata.st_mtime < deadline:
                        Path(entry.path).unlink()
                except OSError:
                    continue

    def upload_cloud_backup(self, filename: object) -> tuple[bool, str]:
        try:
            path = self._local_path(filename)
            payload = self._read_path(path)
            ok, message, paths = self._runtime.configuration_backup_webdav_paths()
            if not ok or paths is None:
                return False, message or "未配置 WebDAV 信息"
            directory_ok, directory_message = self._runtime.configuration_backup_webdav_make_directories(
                paths["base_url"], paths["config_rel"], paths["auth"]
            )
            if not directory_ok:
                return False, directory_message
            remote_url = self._runtime.configuration_backup_webdav_join_url(
                paths["base_url"], f"{paths['config_rel']}/{path.name}"
            )
            response = self._put(remote_url, data=payload, auth=paths["auth"], timeout=60)
            if int(getattr(response, "status_code", 0)) in {200, 201, 204}:
                return True, f"配置备份已上传: {path.name}"
            return False, f"上传失败，HTTP {getattr(response, 'status_code', 0)}"
        except (InvalidConfigBackup, OSError, ValueError) as exc:
            return False, str(exc)
        except Exception as exc:
            return False, f"上传配置备份失败: {exc}"

    def list_cloud_backups(self) -> tuple[bool, list[dict[str, Any]] | str]:
        response = None
        try:
            ok, message, paths = self._runtime.configuration_backup_webdav_paths()
            if not ok or paths is None:
                return False, message or "未配置 WebDAV 信息"
            response = self._request(
                "PROPFIND",
                paths["config_url"],
                data=_CONFIG_PROPFIND_BODY,
                headers={"Depth": "1"},
                auth=paths["auth"],
                timeout=30,
                stream=True,
            )
            if int(getattr(response, "status_code", 0)) not in {200, 207}:
                return False, f"读取云端配置备份失败，HTTP {getattr(response, 'status_code', 0)}"
            payload = self._read_response(response, MAX_CONFIG_CLOUD_LIST_BYTES)
            if b"<!doctype" in payload.lower() or b"<!entity" in payload.lower():
                raise InvalidConfigBackup("WebDAV XML 不允许声明实体")
            root = ET.fromstring(payload)
            responses = root.findall("d:response", _DAV_NS)
            if len(responses) > MAX_CONFIG_CLOUD_LIST_ENTRIES:
                raise InvalidConfigBackup("云端配置备份条目超过限制")
            backups: list[dict[str, Any]] = []
            for item in responses:
                href = item.findtext("d:href", default="", namespaces=_DAV_NS)
                prop = item.find("d:propstat/d:prop", _DAV_NS)
                if not href or prop is None:
                    continue
                resource_type = prop.find("d:resourcetype", _DAV_NS)
                if resource_type is not None and resource_type.find("d:collection", _DAV_NS) is not None:
                    continue
                name = unquote(PurePosixPath(urlsplit(href).path.rstrip("/")).name)
                if not is_config_backup_filename(name):
                    continue
                size_text = prop.findtext("d:getcontentlength", default="", namespaces=_DAV_NS)
                modified = prop.findtext("d:getlastmodified", default="", namespaces=_DAV_NS)
                backups.append(
                    {
                        "filename": name,
                        "size": int(size_text) if size_text.isdigit() else 0,
                        "modified": self._runtime.configuration_backup_format_time(modified),
                    }
                )
            backups.sort(key=lambda item: item["modified"], reverse=True)
            return True, backups
        except (InvalidConfigBackup, ET.ParseError, OSError, ValueError) as exc:
            return False, str(exc)
        except Exception as exc:
            return False, f"获取云端配置备份失败: {exc}"
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()

    def download_cloud_backup(self, filename: object, *, overwrite: bool = False) -> tuple[bool, object]:
        response = None
        temporary_path: Path | None = None
        try:
            target = self._local_path(filename)
            self._ensure_directory()
            if target.exists() and not overwrite:
                return False, "FILE_EXISTS"
            ok, message, paths = self._runtime.configuration_backup_webdav_paths()
            if not ok or paths is None:
                return False, message or "未配置 WebDAV 信息"
            remote_url = self._runtime.configuration_backup_webdav_join_url(
                paths["base_url"], f"{paths['config_rel']}/{target.name}"
            )
            response = self._get(remote_url, auth=paths["auth"], timeout=60, stream=True)
            if int(getattr(response, "status_code", 0)) != 200:
                return False, f"下载失败，HTTP {getattr(response, 'status_code', 0)}"
            payload = self._read_response(response, MAX_CONFIG_BACKUP_BYTES)
            self._decode_json(payload)
            fd, temporary_name = tempfile.mkstemp(
                prefix=".config-download-", suffix=".tmp", dir=self._backup_directory
            )
            temporary_path = Path(temporary_name)
            with os.fdopen(fd, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            if overwrite:
                os.replace(temporary_path, target)
            else:
                try:
                    os.link(temporary_path, target)
                except FileExistsError:
                    return False, "FILE_EXISTS"
                temporary_path.unlink()
            temporary_path = None
            return True, target
        except (InvalidConfigBackup, OSError, ValueError) as exc:
            return False, str(exc)
        except Exception as exc:
            return False, f"下载配置备份失败: {exc}"
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            close = getattr(response, "close", None)
            if callable(close):
                close()

    def local_backup_exists(self, filename: object) -> bool:
        path = self._local_path(filename)
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidConfigBackup("无效配置备份文件")
        return True

    def local_backup_path(self, filename: object) -> Path:
        return self._local_path(filename)

    def _local_path(self, filename: object) -> Path:
        if not is_config_backup_filename(filename):
            raise InvalidConfigBackup("无效配置备份文件名")
        self._ensure_directory_parent()
        return self._backup_directory / str(filename)

    def _validate_local_filename(self, filename: str) -> None:
        if not is_config_backup_filename(filename) or not filename.startswith(CONFIG_BACKUP_PREFIX):
            raise InvalidConfigBackup("无效配置备份文件名")

    def _ensure_directory_parent(self) -> None:
        parent = self._backup_directory.parent
        try:
            metadata = parent.lstat()
        except FileNotFoundError:
            metadata = None
        if metadata is not None and (
            stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode)
        ):
            raise InvalidConfigBackup("配置备份目录无效")
        try:
            metadata = self._backup_directory.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise InvalidConfigBackup("配置备份目录无效")

    def _ensure_directory(self) -> None:
        self._ensure_directory_parent()
        self._backup_directory.mkdir(parents=True, exist_ok=True)
        metadata = self._backup_directory.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise InvalidConfigBackup("配置备份目录无效")

    @staticmethod
    def _encode_json(value: dict[str, Any]) -> bytes:
        if not isinstance(value, dict):
            raise InvalidConfigBackup("配置备份必须是 JSON 对象")
        try:
            return json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise InvalidConfigBackup(f"配置备份无法编码: {exc}") from exc

    @staticmethod
    def _decode_json(payload: bytes) -> dict[str, Any]:
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidConfigBackup("配置备份不是有效的 UTF-8 JSON") from exc
        if not isinstance(value, dict):
            raise InvalidConfigBackup("配置备份必须是 JSON 对象")
        return value

    @staticmethod
    def _read_path(path: Path) -> bytes:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise InvalidConfigBackup("无效配置备份文件") from exc
            raise
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                raise InvalidConfigBackup("无效配置备份文件")
            if metadata.st_size > MAX_CONFIG_BACKUP_BYTES:
                raise InvalidConfigBackup("配置备份超过大小限制")
            return ConfigBackupRepository._read_limited_stream(stream, MAX_CONFIG_BACKUP_BYTES)

    @staticmethod
    def _read_limited_stream(stream: Any, limit: int) -> bytes:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = stream.read(min(64 * 1024, limit + 1 - total))
            if not chunk:
                return b"".join(chunks)
            total += len(chunk)
            if total > limit:
                raise InvalidConfigBackup("配置备份超过大小限制")
            chunks.append(chunk)

    @classmethod
    def _read_response(cls, response: Any, limit: int) -> bytes:
        chunks: list[bytes] = []
        total = 0
        iterator = getattr(response, "iter_content", None)
        source = iterator(chunk_size=64 * 1024) if callable(iterator) else (getattr(response, "content", b""),)
        for chunk in source:
            if not chunk:
                continue
            total += len(chunk)
            if total > limit:
                raise InvalidConfigBackup("WebDAV 响应超过大小限制")
            chunks.append(chunk)
        return b"".join(chunks)


_CONFIG_PROPFIND_BODY = b"""<?xml version=\"1.0\" encoding=\"utf-8\" ?>
<d:propfind xmlns:d=\"DAV:\"><d:prop><d:getlastmodified /><d:getcontentlength /><d:resourcetype /></d:prop></d:propfind>"""


__all__ = [
    "MAX_CONFIG_BACKUP_BYTES",
    "MAX_CONFIG_CLOUD_LIST_BYTES",
    "MAX_CONFIG_CLOUD_LIST_ENTRIES",
    "ConfigBackupRepository",
    "InvalidConfigBackup",
    "is_config_backup_filename",
]
