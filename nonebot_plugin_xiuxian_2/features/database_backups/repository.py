from __future__ import annotations

import errno
import os
import re
import shutil
import stat
import tempfile
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterator, Protocol
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree as ET

import requests

from ...infrastructure.database.backup_capacity import preflight_capacity
from .schemas import (
    DATABASE_BACKUP_ARCHIVE_PATTERN,
    DATABASE_BACKUP_PREFIX,
    DATABASE_BACKUP_SUFFIX,
    MAX_DATABASE_BACKUP_CLOUD_LIST_BYTES,
    MAX_DATABASE_BACKUP_CLOUD_LIST_ENTRIES,
    MAX_DATABASE_BACKUP_DOWNLOAD_BYTES,
    MAX_DATABASE_RESTORE_BYTES,
    MAX_DATABASE_RESTORE_MEMBERS,
)


_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")
_DAV_NS = {"d": "DAV:"}


class InvalidDatabaseBackup(ValueError):
    pass


class DatabaseBackupNotFound(FileNotFoundError):
    pass


class PartialDatabaseRestoreError(RuntimeError):
    def __init__(self, restored: list[str], message: str) -> None:
        super().__init__(message)
        self.restored = tuple(restored)


class DatabaseBackupRuntime(Protocol):
    def database_backup_database_names(self) -> list[str]: ...

    def database_backup_database_path(self, name: str) -> Path: ...

    def database_backup_snapshot_sqlite(
        self, source: Path, destination: Path
    ) -> tuple[bool, str]: ...

    def database_backup_validate_sqlite(self, path: Path) -> tuple[bool, str]: ...

    def database_backup_restore_sqlite(
        self, source: Path, target: Path, name: str
    ) -> None: ...

    def database_backup_after_restore(self, names: list[str]) -> None: ...

    def database_backup_keep_days(self) -> int: ...

    def database_backup_cloud_enabled(self) -> bool: ...

    def database_backup_webdav_paths(
        self,
    ) -> tuple[bool, str, dict[str, Any] | None]: ...

    def database_backup_webdav_join_url(
        self, base_url: str, relative_path: str
    ) -> str: ...

    def database_backup_webdav_make_directories(
        self, base_url: str, relative_path: str, auth: tuple[str, str]
    ) -> tuple[bool, str]: ...

    def database_backup_cleanup_cloud(self) -> tuple[bool, str]: ...

    def database_backup_format_time(self, value: str) -> str: ...


@dataclass(frozen=True)
class StagedDatabaseBackup:
    databases: dict[str, Path]
    skipped: tuple[str, ...]


class DatabaseBackupRepository:
    def __init__(
        self,
        backup_directory: str | Path,
        data_directory: str | Path,
        runtime: DatabaseBackupRuntime,
        *,
        request: Any = requests.request,
        get: Any = requests.get,
        put: Any = requests.put,
        head: Any = requests.head,
        delete: Any = requests.delete,
        now: Callable[[], datetime] = datetime.now,
    ) -> None:
        self._backup_directory = Path(backup_directory)
        self._data_directory = Path(data_directory)
        self._runtime = runtime
        self._request = request
        self._get = get
        self._put = put
        self._head = head
        self._delete = delete
        self._now = now

    def create_local_backup(self) -> tuple[bool, Path | str, list[str]]:
        try:
            database_names = self._database_names()
            sources: list[tuple[str, Path, int]] = []
            for name in database_names:
                source = Path(self._runtime.database_backup_database_path(name))
                try:
                    metadata = source.lstat()
                except FileNotFoundError:
                    continue
                if not stat.S_ISREG(metadata.st_mode):
                    raise InvalidDatabaseBackup(f"数据库不是普通文件: {name}")
                sources.append((name, source, metadata.st_size))

            if not sources:
                return False, "未找到可备份的 SQLite 数据库文件", []

            total_bytes = sum(size for _name, _path, size in sources)
            preflight_capacity(
                {self._backup_directory: total_bytes * 2},
                operation="legacy database backup archive",
            )
            self._backup_directory.mkdir(parents=True, exist_ok=True)

            stamp = self._now().strftime("%Y%m%d_%H%M%S")
            archive_path = self._unique_archive_path(stamp)
            descriptor = -1
            temporary_path: Path | None = None
            added: list[str] = []
            try:
                with tempfile.TemporaryDirectory(
                    prefix="db-backup-snapshot-", dir=self._backup_directory
                ) as temporary_directory:
                    snapshot_root = Path(temporary_directory)
                    descriptor, temporary_name = tempfile.mkstemp(
                        prefix=".db-backup.", suffix=".tmp", dir=self._backup_directory
                    )
                    temporary_path = Path(temporary_name)
                    with os.fdopen(descriptor, "w+b") as archive_stream:
                        descriptor = -1
                        with zipfile.ZipFile(
                            archive_stream,
                            "w",
                            compression=zipfile.ZIP_DEFLATED,
                        ) as archive:
                            for name, source, _size in sources:
                                snapshot_path = snapshot_root / name
                                ok, message = self._runtime.database_backup_snapshot_sqlite(
                                    source, snapshot_path
                                )
                                if not ok:
                                    raise RuntimeError(message or f"{name} 快照失败")
                                archive.write(snapshot_path, arcname=name)
                                added.append(name)
                        archive_stream.flush()
                        os.fsync(archive_stream.fileno())
                os.replace(temporary_path, archive_path)
                return True, archive_path, added
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
        except Exception as exc:
            return False, f"数据库备份失败: {exc}", []

    def upload_cloud_backup(self, local_path: str | Path) -> tuple[bool, str]:
        response = None
        try:
            path = Path(local_path)
            if not path.is_file() or path.parent.resolve() != self._backup_directory.resolve():
                return False, f"本地数据库备份文件不存在: {path.name}"
            ok, message, paths = self._webdav_paths()
            if not ok or paths is None:
                return False, message

            remote_relative = "/".join(
                part for part in (str(paths["db_rel"]).strip("/"), path.name) if part
            )
            remote_url = self._runtime.database_backup_webdav_join_url(
                paths["base_url"], remote_relative
            )
            response = self._head(remote_url, auth=paths["auth"], timeout=15, allow_redirects=False)
            if int(getattr(response, "status_code", 0)) in {200, 204, 206, 301, 302}:
                return True, f"远端已存在，跳过上传: {remote_relative}"
            self._close_response(response)
            response = None

            created, create_message = self._runtime.database_backup_webdav_make_directories(
                paths["base_url"], paths["db_rel"], paths["auth"]
            )
            if not created:
                return False, create_message
            with path.open("rb") as stream:
                response = self._put(
                    remote_url, data=stream, auth=paths["auth"], timeout=120
                )
            status = int(getattr(response, "status_code", 0))
            if status in {200, 201, 204}:
                return True, f"上传成功: {remote_relative}"
            return False, f"上传失败 HTTP {status}: {remote_relative}"
        except Exception as exc:
            return False, f"WebDAV上传异常: {exc}"
        finally:
            self._close_response(response)

    def cleanup_local_backups(self, keep_days: int) -> tuple[bool, str]:
        try:
            keep_days = int(keep_days)
            if keep_days <= 0:
                return True, "本地保留天数<=0，跳过清理"
            if not self._backup_directory.exists():
                return True, "备份目录不存在，跳过清理"

            cutoff = self._now().timestamp() - keep_days * 24 * 60 * 60
            deleted = 0
            for path in self._backup_directory.glob(f"{DATABASE_BACKUP_PREFIX}*{DATABASE_BACKUP_SUFFIX}"):
                try:
                    metadata = path.lstat()
                    if not stat.S_ISREG(metadata.st_mode):
                        continue
                    created = self._backup_timestamp(path, metadata.st_mtime)
                    if created.timestamp() < cutoff:
                        path.unlink()
                        deleted += 1
                except FileNotFoundError:
                    continue
            return True, f"数据库本地旧备份清理完成：删除{deleted}个（>{keep_days}天）"
        except Exception as exc:
            return False, f"清理旧备份失败: {exc}"

    def list_local_backups(self) -> list[dict[str, Any]]:
        if not self._backup_directory.exists():
            return []
        entries: list[dict[str, Any]] = []
        for path in self._backup_directory.glob(f"{DATABASE_BACKUP_PREFIX}*{DATABASE_BACKUP_SUFFIX}"):
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(metadata.st_mode):
                continue
            created_at = datetime.fromtimestamp(metadata.st_ctime).isoformat()
            entries.append(
                {
                    "filename": path.name,
                    "size": metadata.st_size,
                    "created_at": created_at,
                    "type": "sqlite",
                }
            )
        entries.sort(key=lambda item: item["created_at"], reverse=True)
        return entries[:MAX_DATABASE_BACKUP_CLOUD_LIST_ENTRIES]

    def delete_local_backup(self, filename: object) -> tuple[bool, str]:
        try:
            name = self._validate_archive_name(filename)
            path = self._backup_directory / name
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                raise InvalidDatabaseBackup("数据库备份不是普通文件")
            path.unlink()
            return True, f"已删除本地数据库备份: {name}"
        except FileNotFoundError:
            return False, "文件不存在"
        except (InvalidDatabaseBackup, OSError, ValueError) as exc:
            return False, str(exc)

    def list_cloud_backups(self) -> tuple[bool, list[dict[str, Any]] | str]:
        response = None
        try:
            ok, message, paths = self._webdav_paths()
            if not ok or paths is None:
                return False, message or "未配置 WebDAV 信息"
            response = self._request(
                "PROPFIND",
                paths["db_url"],
                auth=paths["auth"],
                timeout=20,
                headers={"Depth": "1"},
                stream=True,
            )
            status = int(getattr(response, "status_code", 0))
            if status not in {200, 207}:
                return False, f"读取云端目录失败 HTTP {status}"
            payload = self._read_response(response, MAX_DATABASE_BACKUP_CLOUD_LIST_BYTES)
            lowered = payload.lower()
            if b"<!doctype" in lowered or b"<!entity" in lowered:
                raise InvalidDatabaseBackup("WebDAV XML 不允许声明实体")
            root = ET.fromstring(payload)
            responses = root.findall("d:response", _DAV_NS)
            if len(responses) > MAX_DATABASE_BACKUP_CLOUD_LIST_ENTRIES:
                raise InvalidDatabaseBackup("云端数据库备份条目超过限制")

            backups: list[dict[str, Any]] = []
            for item in responses:
                href = item.findtext("d:href", default="", namespaces=_DAV_NS)
                if not href:
                    continue
                name = unquote(PurePosixPath(urlsplit(href).path.rstrip("/")).name)
                if not is_database_backup_archive_name(name):
                    continue
                resource_type = item.find(".//d:resourcetype", _DAV_NS)
                if (
                    resource_type is not None
                    and resource_type.find("d:collection", _DAV_NS) is not None
                ):
                    continue
                size_text = item.findtext(
                    ".//d:getcontentlength", default="", namespaces=_DAV_NS
                )
                modified = item.findtext(
                    ".//d:getlastmodified", default="", namespaces=_DAV_NS
                )
                backups.append(
                    {
                        "filename": name,
                        "size": int(size_text) if size_text.isdigit() else 0,
                        "modified": self._runtime.database_backup_format_time(modified),
                    }
                )
            backups.sort(key=lambda item: item["modified"], reverse=True)
            return True, backups
        except Exception as exc:
            return False, f"读取云端数据库备份失败: {exc}"
        finally:
            self._close_response(response)

    def local_backup_exists(self, filename: object) -> bool:
        name = self._validate_archive_name(filename)
        path = self._backup_directory / name
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidDatabaseBackup("数据库备份不是普通文件")
        return True

    def download_cloud_backup(
        self, filename: object, *, overwrite: bool
    ) -> tuple[bool, Path | str]:
        response = None
        temporary_path: Path | None = None
        try:
            name = self._validate_archive_name(filename)
            if not overwrite and self.local_backup_exists(name):
                return False, "FILE_EXISTS"
            ok, message, paths = self._webdav_paths()
            if not ok or paths is None:
                return False, message or "未配置 WebDAV 信息"

            remote_relative = "/".join(
                part for part in (str(paths["db_rel"]).strip("/"), name) if part
            )
            remote_url = self._runtime.database_backup_webdav_join_url(
                paths["base_url"], remote_relative
            )
            response = self._get(
                remote_url, auth=paths["auth"], timeout=120, stream=True
            )
            status = int(getattr(response, "status_code", 0))
            if status != 200:
                return False, f"下载失败 HTTP {status}"

            content_length = self._content_length(response)
            if content_length > MAX_DATABASE_BACKUP_DOWNLOAD_BYTES:
                raise InvalidDatabaseBackup("云端数据库备份超过大小限制")
            self._preflight_download_space(content_length or 64 * 1024)
            self._backup_directory.mkdir(parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".db-cloud-backup.", suffix=".tmp", dir=self._backup_directory
            )
            temporary_path = Path(temporary_name)
            written = 0
            next_space_check = 16 * 1024 * 1024
            with os.fdopen(descriptor, "wb") as stream:
                for chunk in self._iter_response(response):
                    if not chunk:
                        continue
                    written += len(chunk)
                    if written > MAX_DATABASE_BACKUP_DOWNLOAD_BYTES:
                        raise InvalidDatabaseBackup("云端数据库备份超过大小限制")
                    if written >= next_space_check:
                        self._preflight_download_space(len(chunk))
                        next_space_check = written + 16 * 1024 * 1024
                    stream.write(chunk)
                stream.flush()
                os.fsync(stream.fileno())

            if content_length and written != content_length:
                raise InvalidDatabaseBackup("下载文件长度与声明值不符")
            if not zipfile.is_zipfile(temporary_path):
                raise InvalidDatabaseBackup("下载完成但文件不是有效 zip")
            target_path = self._backup_directory / name
            if overwrite:
                os.replace(temporary_path, target_path)
            else:
                try:
                    os.link(temporary_path, target_path)
                except FileExistsError:
                    return False, "FILE_EXISTS"
                temporary_path.unlink()
            return True, target_path
        except Exception as exc:
            return False, f"下载数据库备份失败: {exc}"
        finally:
            self._close_response(response)
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def delete_cloud_backup(self, filename: object) -> tuple[bool, str]:
        response = None
        try:
            name = self._validate_archive_name(filename)
            ok, message, paths = self._webdav_paths()
            if not ok or paths is None:
                return False, message or "未配置 WebDAV 信息"
            remote_relative = "/".join(
                part for part in (str(paths["db_rel"]).strip("/"), name) if part
            )
            remote_url = self._runtime.database_backup_webdav_join_url(
                paths["base_url"], remote_relative
            )
            response = self._delete(
                remote_url, auth=paths["auth"], timeout=20
            )
            status = int(getattr(response, "status_code", 0))
            if status in {200, 202, 204}:
                return True, f"已删除云端数据库备份: {name}"
            return False, f"删除失败 HTTP {status}"
        except Exception as exc:
            return False, f"删除云端数据库备份失败: {exc}"
        finally:
            self._close_response(response)

    @contextmanager
    def stage_restore(
        self, filename: object, selected_databases: list[str]
    ) -> Iterator[StagedDatabaseBackup]:
        name = self._validate_archive_name(filename)
        archive_stream = self._open_local_archive(name)
        temporary_directory: tempfile.TemporaryDirectory[str] | None = None
        try:
            with archive_stream:
                with zipfile.ZipFile(archive_stream, "r") as archive:
                    members = self._validated_members(archive)
                    staged_members: dict[str, dict[str, zipfile.ZipInfo]] = {}
                    skipped: list[str] = []
                    for database in selected_databases:
                        candidates = (database, f"data/xiuxian/{database}")
                        member = next((members[candidate] for candidate in candidates if candidate in members), None)
                        if member is None:
                            skipped.append(database)
                            continue
                        sidecars: dict[str, zipfile.ZipInfo] = {}
                        for suffix in ("-wal", "-shm"):
                            sidecar = next(
                                (
                                    members[candidate + suffix]
                                    for candidate in candidates
                                    if candidate + suffix in members
                                ),
                                None,
                            )
                            if sidecar is not None:
                                sidecars[suffix] = sidecar
                        staged_members[database] = {"": member, **sidecars}

                    payload_size = sum(
                        info.file_size
                        for entries in staged_members.values()
                        for info in entries.values()
                    )
                    if payload_size > MAX_DATABASE_RESTORE_BYTES:
                        raise InvalidDatabaseBackup("数据库恢复内容超过大小限制")
                    self._preflight_restore_space(payload_size, selected_databases)
                    self._backup_directory.mkdir(parents=True, exist_ok=True)
                    temporary_directory = tempfile.TemporaryDirectory(
                        prefix="db-backup-restore-", dir=self._backup_directory
                    )
                    stage_root = Path(temporary_directory.name)
                    staged: dict[str, Path] = {}
                    for database, entries in staged_members.items():
                        source_path = stage_root / database
                        with archive.open(entries[""], "r") as source, source_path.open("wb") as target:
                            self._copy_member(source, target, entries[""].file_size)
                        for suffix, info in entries.items():
                            if not suffix:
                                continue
                            sidecar_path = stage_root / f"{database}{suffix}"
                            with archive.open(info, "r") as source, sidecar_path.open("wb") as target:
                                self._copy_member(source, target, info.file_size)
                        ok, message = self._runtime.database_backup_validate_sqlite(
                            source_path
                        )
                        if not ok:
                            database_payload_size = sum(
                                info.file_size for info in entries.values()
                            )
                            preflight_capacity(
                                {self._backup_directory: database_payload_size},
                                operation="legacy database restore recovery staging",
                            )
                            clean_path = stage_root / f"{database}.clean"
                            ok, message = self._runtime.database_backup_snapshot_sqlite(
                                source_path, clean_path
                            )
                            if not ok:
                                raise InvalidDatabaseBackup(
                                    f"{database} 备份库校验未通过: {message}"
                                )
                            staged[database] = clean_path
                        else:
                            staged[database] = source_path
                    yield StagedDatabaseBackup(staged, tuple(skipped))
        except DatabaseBackupNotFound:
            raise
        except (zipfile.BadZipFile, EOFError) as exc:
            raise InvalidDatabaseBackup(f"数据库备份 ZIP 已损坏: {exc}") from exc
        finally:
            if temporary_directory is not None:
                temporary_directory.cleanup()

    def restore_staged(
        self, staged: StagedDatabaseBackup
    ) -> tuple[list[str], list[str]]:
        restored: list[str] = []
        attempted: list[str] = []
        error: Exception | None = None
        for name, source in staged.databases.items():
            attempted.append(name)
            try:
                self._runtime.database_backup_restore_sqlite(
                    source, self._runtime.database_backup_database_path(name), name
                )
                restored.append(name)
            except Exception as exc:
                error = exc
                break
        if attempted:
            try:
                self._runtime.database_backup_after_restore(attempted)
            except Exception as exc:
                if error is None:
                    error = exc
        if error is not None:
            raise PartialDatabaseRestoreError(restored, str(error)) from error
        return restored, list(staged.skipped)

    def _preflight_restore_space(
        self, payload_size: int, selected_databases: list[str]
    ) -> None:
        existing_size = 0
        for name in selected_databases:
            path = Path(self._runtime.database_backup_database_path(name))
            try:
                metadata = path.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise InvalidDatabaseBackup(f"目标数据库不是普通文件: {name}")
            existing_size += metadata.st_size
        requirements = {
            self._backup_directory: payload_size,
            self._data_directory: payload_size,
            self._backup_directory.parent / "db_restore_before": existing_size,
        }
        preflight_capacity(requirements, operation="legacy database backup restore")

    @staticmethod
    def _copy_member(source: Any, target: Any, expected_size: int) -> None:
        copied = 0
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            copied += len(chunk)
            if copied > expected_size or copied > MAX_DATABASE_RESTORE_BYTES:
                raise InvalidDatabaseBackup("数据库归档成员长度超过声明值")
            target.write(chunk)
        if copied != expected_size:
            raise InvalidDatabaseBackup("数据库归档成员长度与声明值不符")

    def _validated_members(
        self, archive: zipfile.ZipFile
    ) -> dict[str, zipfile.ZipInfo]:
        infos = archive.infolist()
        if len(infos) > MAX_DATABASE_RESTORE_MEMBERS:
            raise InvalidDatabaseBackup("数据库备份成员数量超过限制")
        members: dict[str, zipfile.ZipInfo] = {}
        for info in infos:
            normalized = self._archive_member_name(info.filename)
            if not normalized:
                continue
            file_type = stat.S_IFMT((info.external_attr >> 16) & 0xFFFF)
            allowed_types = {0, stat.S_IFDIR} if info.is_dir() else {0, stat.S_IFREG}
            if file_type not in allowed_types:
                raise InvalidDatabaseBackup("数据库备份包含链接或特殊文件")
            if info.is_dir():
                continue
            if normalized in members:
                raise InvalidDatabaseBackup("数据库备份包含重复成员")
            members[normalized] = info
        return members

    @staticmethod
    def _archive_member_name(value: str) -> str:
        name = str(value or "")
        if "\\" in name or "\x00" in name:
            raise InvalidDatabaseBackup("数据库备份成员路径非法")
        while name.startswith("./"):
            name = name[2:]
        if not name or name == ".":
            return ""
        if name.startswith("/") or _WINDOWS_DRIVE.match(name):
            raise InvalidDatabaseBackup("数据库备份成员路径非法")
        parts = PurePosixPath(name).parts
        if any(part in {"", ".", ".."} for part in parts):
            raise InvalidDatabaseBackup("数据库备份成员路径非法")
        return PurePosixPath(*parts).as_posix()

    def _open_local_archive(self, filename: str):
        path = self._backup_directory / filename
        try:
            metadata = path.lstat()
        except FileNotFoundError as exc:
            raise DatabaseBackupNotFound(filename) from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidDatabaseBackup("数据库备份不是普通文件")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise DatabaseBackupNotFound(filename) from exc
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.EISDIR}:
                raise InvalidDatabaseBackup("数据库备份不是普通文件") from exc
            raise
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise InvalidDatabaseBackup("数据库备份不是普通文件")
            return os.fdopen(descriptor, "rb")
        except Exception:
            os.close(descriptor)
            raise

    def _webdav_paths(self) -> tuple[bool, str, dict[str, Any] | None]:
        return self._runtime.database_backup_webdav_paths()

    @classmethod
    def _read_response(cls, response: Any, limit: int) -> bytes:
        declared = cls._content_length(response)
        if declared > limit:
            raise InvalidDatabaseBackup("WebDAV 响应超过大小限制")
        payload = bytearray()
        for chunk in cls._iter_response(response):
            if not chunk:
                continue
            payload.extend(chunk)
            if len(payload) > limit:
                raise InvalidDatabaseBackup("WebDAV 响应超过大小限制")
        if declared and len(payload) != declared:
            raise InvalidDatabaseBackup("WebDAV 响应长度与声明值不符")
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
    def _content_length(response: Any) -> int:
        try:
            return max(0, int((getattr(response, "headers", {}) or {}).get("content-length", 0)))
        except (TypeError, ValueError):
            return 0

    def _preflight_download_space(self, required_bytes: int) -> None:
        preflight_capacity(
            {self._backup_directory: max(0, int(required_bytes))},
            operation="legacy database backup download",
        )

    @staticmethod
    def _validate_archive_name(value: object) -> str:
        name = str(value or "")
        if (
            not name
            or len(name.encode("utf-8")) > 255
            or name in {".", ".."}
            or Path(name).name != name
            or "/" in name
            or "\\" in name
            or "\x00" in name
            or _WINDOWS_DRIVE.match(name)
            or not name.lower().endswith(DATABASE_BACKUP_SUFFIX)
        ):
            raise InvalidDatabaseBackup("无效数据库备份文件名")
        return name

    def _database_names(self) -> list[str]:
        names = self._runtime.database_backup_database_names()
        result: list[str] = []
        for raw in names:
            name = str(raw)
            if (
                not name
                or name in {".", ".."}
                or Path(name).name != name
                or "/" in name
                or "\\" in name
                or not name.lower().endswith(".db")
            ):
                raise InvalidDatabaseBackup("数据库清单包含非法文件名")
            if name not in result:
                result.append(name)
        return result

    def _unique_archive_path(self, stamp: str) -> Path:
        candidate = (
                self._backup_directory
                / f"{DATABASE_BACKUP_PREFIX}{stamp}{DATABASE_BACKUP_SUFFIX}"
            )
        suffix = 1
        while candidate.exists():
            candidate = (
                    self._backup_directory
                    / f"{DATABASE_BACKUP_PREFIX}{stamp}_{suffix}{DATABASE_BACKUP_SUFFIX}"
                )
            suffix += 1
        return candidate

    @staticmethod
    def _backup_timestamp(path: Path, fallback: float) -> datetime:
        match = re.search(r"(?P<stamp>\d{8}_\d{6})", path.stem)
        if match:
            try:
                return datetime.strptime(match.group("stamp"), "%Y%m%d_%H%M%S")
            except ValueError:
                pass
        return datetime.fromtimestamp(fallback)

    @staticmethod
    def _close_response(response: Any) -> None:
        close = getattr(response, "close", None)
        if callable(close):
            close()


def is_database_backup_archive_name(value: object) -> bool:
    if not isinstance(value, str) or len(value.encode("utf-8")) > 255:
        return False
    if (
        not value
        or Path(value).name != value
        or "/" in value
        or "\\" in value
        or "\x00" in value
        or _WINDOWS_DRIVE.match(value)
    ):
        return False
    return DATABASE_BACKUP_ARCHIVE_PATTERN.fullmatch(value) is not None


__all__ = [
    "DatabaseBackupNotFound",
    "DatabaseBackupRepository",
    "InvalidDatabaseBackup",
    "MAX_DATABASE_BACKUP_CLOUD_LIST_BYTES",
    "MAX_DATABASE_BACKUP_CLOUD_LIST_ENTRIES",
    "MAX_DATABASE_BACKUP_DOWNLOAD_BYTES",
    "MAX_DATABASE_RESTORE_BYTES",
    "MAX_DATABASE_RESTORE_MEMBERS",
    "PartialDatabaseRestoreError",
    "StagedDatabaseBackup",
    "is_database_backup_archive_name",
]
