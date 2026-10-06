from __future__ import annotations

import errno
import os
import stat
from pathlib import Path
from typing import BinaryIO

from .repository import is_plugin_backup_filename


class InvalidPluginBackupFile(ValueError):
    pass


class PluginBackupFileNotFound(FileNotFoundError):
    pass


class PluginBackupFileRepository:
    def __init__(self, backup_directory: str | Path) -> None:
        self._backup_directory = Path(backup_directory)

    def open_plugin_backup(self, filename: str) -> BinaryIO:
        path = self._path_for(filename)
        try:
            metadata = path.lstat()
        except FileNotFoundError as exc:
            raise PluginBackupFileNotFound(filename) from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidPluginBackupFile(filename)

        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise PluginBackupFileNotFound(filename) from exc
        except OSError as exc:
            if exc.errno in {errno.ELOOP, errno.EISDIR}:
                raise InvalidPluginBackupFile(filename) from exc
            raise

        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise InvalidPluginBackupFile(filename)
            return os.fdopen(descriptor, "rb")
        except Exception:
            os.close(descriptor)
            raise

    def delete_plugin_backup(self, filename: str) -> None:
        path = self._path_for(filename)
        try:
            metadata = path.lstat()
        except FileNotFoundError as exc:
            raise PluginBackupFileNotFound(filename) from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise InvalidPluginBackupFile(filename)
        try:
            path.unlink()
        except FileNotFoundError as exc:
            raise PluginBackupFileNotFound(filename) from exc

    def _path_for(self, filename: str) -> Path:
        if not is_plugin_backup_filename(filename):
            raise InvalidPluginBackupFile(str(filename))
        return self._backup_directory / filename


__all__ = [
    "InvalidPluginBackupFile",
    "PluginBackupFileNotFound",
    "PluginBackupFileRepository",
]
