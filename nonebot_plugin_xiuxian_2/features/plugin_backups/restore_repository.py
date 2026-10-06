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
from pathlib import Path, PurePosixPath
from typing import Callable, Iterator


PLUGIN_ARCHIVE_ROOT = PurePosixPath("src/plugins/nonebot_plugin_xiuxian_2")
MAX_ARCHIVE_MEMBERS = 100_000
RESTORE_DISK_RESERVE_BYTES = 64 * 1024 * 1024
_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:")


class InvalidPluginBackupArchive(ValueError):
    pass


class PluginBackupArchiveNotFound(FileNotFoundError):
    pass


@dataclass(frozen=True)
class StagedPluginBackup:
    root: Path
    data_root: Path | None
    plugin_root: Path | None


class PluginBackupRestoreRepository:
    def __init__(
        self,
        backup_directory: str | Path,
        data_root: str | Path,
        plugin_root: str | Path,
        *,
        temporary_directory: str | Path | None = None,
    ) -> None:
        self._backup_directory = Path(backup_directory)
        self._data_root = Path(data_root)
        self._plugin_root = Path(plugin_root)
        self._temporary_directory = (
            Path(temporary_directory) if temporary_directory is not None else None
        )

    def backup_exists(self, filename: str) -> bool:
        path = self._archive_path(filename)
        try:
            path.lstat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise InvalidPluginBackupArchive(f"无法读取备份文件: {exc}") from exc
        return True

    @contextmanager
    def stage_backup(
        self, filename: str, database_names: set[str]
    ) -> Iterator[StagedPluginBackup]:
        archive_path = self._archive_path(filename)
        archive_stream = self._open_archive(archive_path)
        temporary_directory = tempfile.TemporaryDirectory(
            prefix="plugin-backup-restore-",
            dir=self._temporary_directory,
        )
        stage_root = Path(temporary_directory.name)
        try:
            with archive_stream:
                with zipfile.ZipFile(archive_stream, "r") as archive:
                    members = self._validate_archive(archive, stage_root, database_names)
                    data_root, plugin_root = self._extract_archive(
                        archive, members, stage_root
                    )
            if data_root is None and plugin_root is None:
                raise InvalidPluginBackupArchive("备份不包含可恢复的插件或数据文件")
            yield StagedPluginBackup(stage_root, data_root, plugin_root)
        except (zipfile.BadZipFile, EOFError) as exc:
            raise InvalidPluginBackupArchive(f"备份 ZIP 已损坏: {exc}") from exc
        finally:
            temporary_directory.cleanup()

    def merge_data(
        self,
        source_root: Path,
        database_names: set[str],
        restore_database: Callable[[Path, Path, str], None],
    ) -> list[str]:
        return self._merge_tree(
            source_root, self._data_root, database_names, restore_database
        )

    def merge_plugin(self, source_root: Path) -> None:
        self._merge_tree(source_root, self._plugin_root, set(), None)

    def write_version(self, version_file: Path, version: str) -> None:
        version_file.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".version.txt.", suffix=".tmp", dir=version_file.parent
        )
        temporary_file = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(version)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_file, version_file)
        finally:
            temporary_file.unlink(missing_ok=True)

    def _archive_path(self, filename: str) -> Path:
        name = str(filename or "")
        if (
            not name
            or name in {".", ".."}
            or Path(name).name != name
            or "/" in name
            or "\\" in name
            or "\x00" in name
            or _WINDOWS_DRIVE.match(name)
            or not name.lower().endswith(".zip")
        ):
            raise InvalidPluginBackupArchive("无效备份文件名")
        return self._backup_directory / name

    @staticmethod
    def _open_archive(path: Path):
        try:
            metadata = path.lstat()
        except FileNotFoundError as exc:
            raise PluginBackupArchiveNotFound(path.name) from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidPluginBackupArchive("备份不是普通文件")

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise PluginBackupArchiveNotFound(path.name) from exc
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.EISDIR}:
                raise InvalidPluginBackupArchive("备份不是普通文件") from exc
            raise
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise InvalidPluginBackupArchive("备份不是普通文件")
            return os.fdopen(descriptor, "rb")
        except Exception:
            os.close(descriptor)
            raise

    def _validate_archive(
        self,
        archive: zipfile.ZipFile,
        stage_root: Path,
        database_names: set[str],
    ) -> list[tuple[zipfile.ZipInfo, str]]:
        infos = archive.infolist()
        if len(infos) > MAX_ARCHIVE_MEMBERS:
            raise InvalidPluginBackupArchive("备份成员数量超过限制")

        members: list[tuple[zipfile.ZipInfo, str]] = []
        seen: dict[str, bool] = {}
        total_size = 0
        has_data_file = False
        has_plugin_file = False
        for info in infos:
            name = self._safe_member_name(info.filename)
            if not name:
                continue
            is_directory = info.is_dir()
            self._validate_member_type(info, is_directory)
            folded_name = name.casefold()
            if folded_name in seen:
                raise InvalidPluginBackupArchive(f"备份包含重复路径: {name}")
            seen[folded_name] = is_directory

            path = PurePosixPath(name)
            if path.parts[0] == "data" and len(path.parts) > 1:
                has_data_file = has_data_file or not is_directory
            elif (
                len(path.parts) > len(PLUGIN_ARCHIVE_ROOT.parts)
                and path.parts[: len(PLUGIN_ARCHIVE_ROOT.parts)]
                == PLUGIN_ARCHIVE_ROOT.parts
            ):
                has_plugin_file = has_plugin_file or not is_directory
            elif name not in {"data", PLUGIN_ARCHIVE_ROOT.as_posix()}:
                raise InvalidPluginBackupArchive(f"备份包含不支持的路径: {name}")

            total_size += info.file_size
            members.append((info, name))

        if not has_data_file and not has_plugin_file:
            raise InvalidPluginBackupArchive("备份不包含可恢复的插件或数据文件")

        ordered_names = sorted(seen)
        for index, folded_name in enumerate(ordered_names):
            is_directory = seen[folded_name]
            if is_directory:
                continue
            prefix = folded_name + "/"
            if index + 1 < len(ordered_names) and ordered_names[index + 1].startswith(prefix):
                raise InvalidPluginBackupArchive("备份文件与目录路径冲突")

        self._validate_disk_capacity(members, total_size, stage_root, database_names)

        return members

    def _validate_disk_capacity(
        self,
        members: list[tuple[zipfile.ZipInfo, str]],
        total_size: int,
        stage_root: Path,
        database_names: set[str],
    ) -> None:
        requirements: dict[int, tuple[Path, int]] = {}

        def add_requirement(path: Path, size: int) -> int:
            location = self._nearest_existing_directory(path)
            device = location.stat().st_dev
            current = requirements.get(device, (location, 0))
            requirements[device] = (current[0], current[1] + size)
            return device

        add_requirement(stage_root, total_size)
        for info, name in members:
            if info.is_dir():
                continue
            parts = PurePosixPath(name).parts
            if parts[0] == "data":
                relative_parts = parts[1:]
                if not relative_parts or self._is_sqlite_sidecar(relative_parts[-1], database_names):
                    continue
                target_path = self._data_root.joinpath(*relative_parts)
                if relative_parts[-1] in database_names:
                    self._validate_target_file(target_path)
                    add_requirement(target_path.parent, info.file_size * 2)
                    try:
                        current_size = target_path.lstat().st_size
                    except FileNotFoundError:
                        current_size = 0
                    add_requirement(
                        self._backup_directory / "db_restore_before", current_size
                    )
                else:
                    add_requirement(target_path.parent, info.file_size)
            elif parts[: len(PLUGIN_ARCHIVE_ROOT.parts)] == PLUGIN_ARCHIVE_ROOT.parts:
                relative_parts = parts[len(PLUGIN_ARCHIVE_ROOT.parts) :]
                if relative_parts:
                    add_requirement(
                        self._plugin_root.joinpath(*relative_parts).parent,
                        info.file_size,
                    )

        for directory, required in requirements.values():
            free_bytes = shutil.disk_usage(directory).free
            reserve = max(RESTORE_DISK_RESERVE_BYTES, free_bytes // 10)
            if required > max(0, free_bytes - reserve):
                raise InvalidPluginBackupArchive(
                    "磁盘空间不足，无法安全暂存并恢复备份"
                )

    @staticmethod
    def _nearest_existing_directory(path: Path) -> Path:
        current = path.resolve(strict=False)
        while not current.exists():
            current = current.parent
        return current if current.is_dir() else current.parent

    @staticmethod
    def _validate_member_type(info: zipfile.ZipInfo, is_directory: bool) -> None:
        if info.flag_bits & 0x1:
            raise InvalidPluginBackupArchive("不支持加密 ZIP 备份")
        if info.file_size < 0 or info.compress_size < 0:
            raise InvalidPluginBackupArchive("备份包含无效文件大小")
        if info.create_system == 3:
            mode = info.external_attr >> 16
            kind = stat.S_IFMT(mode)
            expected = stat.S_IFDIR if is_directory else stat.S_IFREG
            if kind not in {0, expected}:
                raise InvalidPluginBackupArchive("备份包含链接或特殊文件")

    @staticmethod
    def _safe_member_name(member_name: str) -> str:
        name = str(member_name or "").replace("\\", "/")
        while name.startswith("./"):
            name = name[2:]
        if not name or name == ".":
            return ""
        if name.startswith("/") or _WINDOWS_DRIVE.match(name) or "\x00" in name:
            raise InvalidPluginBackupArchive(f"压缩包成员路径非法: {member_name}")
        path = PurePosixPath(name)
        if any(part == ".." for part in path.parts):
            raise InvalidPluginBackupArchive(f"压缩包成员路径非法: {member_name}")
        return path.as_posix()

    @staticmethod
    def _extract_archive(
        archive: zipfile.ZipFile,
        members: list[tuple[zipfile.ZipInfo, str]],
        stage_root: Path,
    ) -> tuple[Path | None, Path | None]:
        for info, name in members:
            if not name:
                continue
            output_path = stage_root.joinpath(*PurePosixPath(name).parts)
            if info.is_dir():
                output_path.mkdir(parents=True, exist_ok=True)
                continue
            output_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with archive.open(info, "r") as source, output_path.open("xb") as target:
                    file_bytes = 0
                    while True:
                        chunk = source.read(64 * 1024)
                        if not chunk:
                            break
                        file_bytes += len(chunk)
                        if file_bytes > info.file_size:
                            raise InvalidPluginBackupArchive("备份实际解压量超过声明值")
                        target.write(chunk)
                    if file_bytes != info.file_size:
                        raise InvalidPluginBackupArchive("备份实际解压量与声明值不符")
            except (zipfile.BadZipFile, EOFError) as exc:
                raise InvalidPluginBackupArchive(f"备份 ZIP 已损坏: {exc}") from exc

        data_path = stage_root / "data"
        plugin_path = stage_root.joinpath(*PLUGIN_ARCHIVE_ROOT.parts)
        return (
            data_path if data_path.is_dir() else None,
            plugin_path if plugin_path.is_dir() else None,
        )

    def _merge_tree(
        self,
        source_root: Path,
        target_root: Path,
        database_names: set[str],
        restore_database: Callable[[Path, Path, str], None] | None,
    ) -> list[str]:
        target_root = target_root.resolve(strict=False)
        target_root.mkdir(parents=True, exist_ok=True)
        if not stat.S_ISDIR(target_root.lstat().st_mode):
            raise InvalidPluginBackupArchive("恢复目标不是目录")

        restored_databases: list[str] = []
        for source_path in sorted(source_root.rglob("*")):
            relative = source_path.relative_to(source_root)
            if source_path.is_dir():
                self._ensure_target_directory(target_root, relative.parts)
                continue
            if not source_path.is_file():
                raise InvalidPluginBackupArchive("备份包含不支持的文件类型")
            name = source_path.name
            if self._is_sqlite_sidecar(name, database_names):
                continue

            target_path = target_root.joinpath(*relative.parts)
            target_parent = self._ensure_target_directory(target_root, relative.parts[:-1])
            self._validate_target_file(target_path)
            if name in database_names:
                if restore_database is None:
                    raise RuntimeError("SQLite 恢复运行时未配置")
                restore_database(source_path, target_path, name)
                restored_databases.append(name)
            else:
                self._copy_file_atomically(source_path, target_parent, target_path.name)
        return restored_databases

    @staticmethod
    def _is_sqlite_sidecar(filename: str, database_names: set[str]) -> bool:
        return any(
            filename in {f"{database_name}-wal", f"{database_name}-shm"}
            for database_name in database_names
        )

    @staticmethod
    def _ensure_target_directory(target_root: Path, parts: tuple[str, ...]) -> Path:
        current = target_root
        for part in parts:
            current = current / part
            try:
                metadata = current.lstat()
            except FileNotFoundError:
                current.mkdir()
                metadata = current.lstat()
            if not stat.S_ISDIR(metadata.st_mode):
                raise InvalidPluginBackupArchive("恢复目标路径包含符号链接或非目录")
        return current

    @staticmethod
    def _validate_target_file(path: Path) -> None:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidPluginBackupArchive("恢复目标包含符号链接或非普通文件")

    @staticmethod
    def _copy_file_atomically(source: Path, target_parent: Path, filename: str) -> None:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".plugin-backup-restore.", dir=target_parent
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as output_stream:
                with source.open("rb") as input_stream:
                    shutil.copyfileobj(input_stream, output_stream)
                output_stream.flush()
                os.fsync(output_stream.fileno())
            shutil.copystat(source, temporary_path, follow_symlinks=False)
            os.replace(temporary_path, target_parent / filename)
        finally:
            temporary_path.unlink(missing_ok=True)


__all__ = [
    "InvalidPluginBackupArchive",
    "PluginBackupArchiveNotFound",
    "PluginBackupRestoreRepository",
    "StagedPluginBackup",
]
