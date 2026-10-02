"""Fail-closed disk capacity checks for backup and restore writes."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Mapping


MINIMUM_BACKUP_RESERVE_BYTES = 64 * 1024 * 1024
BACKUP_RESERVE_PERCENT = 10


class BackupCapacityError(OSError):
    code = "storage_capacity_unavailable"
    status = 503

    def __init__(self, message: str, *, details: Mapping[str, object] | None = None) -> None:
        super().__init__(message)
        self.details = dict(details or {})


class InsufficientBackupSpace(BackupCapacityError):
    code = "insufficient_storage"
    status = 507


def backup_reserve_bytes(payload_bytes: int, *, additional_bytes: int = 0) -> int:
    if payload_bytes < 0 or additional_bytes < 0:
        raise ValueError("capacity estimates must not be negative")
    percentage_reserve = (payload_bytes * BACKUP_RESERVE_PERCENT + 99) // 100
    return max(MINIMUM_BACKUP_RESERVE_BYTES, percentage_reserve) + additional_bytes


def _existing_capacity_directory(path: Path) -> Path:
    candidate = path.expanduser().resolve(strict=False)
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise OSError("no existing parent directory")
        candidate = parent
    return candidate if candidate.is_dir() else candidate.parent


def preflight_capacity(
    requirements: Mapping[str | Path, int],
    *,
    operation: str,
    additional_reserve_bytes: int = 0,
) -> None:
    """Check all writes before creating directories or replacing any files."""
    if additional_reserve_bytes < 0:
        raise ValueError("additional_reserve_bytes must not be negative")

    groups: dict[int, dict[str, object]] = {}
    for raw_path, raw_bytes in requirements.items():
        path = Path(raw_path)
        size = int(raw_bytes)
        if size < 0:
            raise ValueError("capacity estimates must not be negative")
        try:
            directory = _existing_capacity_directory(path)
            device = directory.stat().st_dev
        except OSError as exc:
            raise BackupCapacityError(
                f"cannot determine storage capacity for {operation}",
                details={"target": str(path), "reason": str(exc)},
            ) from exc
        group = groups.setdefault(device, {"directory": directory, "bytes": 0})
        group["bytes"] = int(group["bytes"]) + size

    for group in groups.values():
        directory = Path(group["directory"])
        payload_bytes = int(group["bytes"])
        reserve_bytes = backup_reserve_bytes(
            payload_bytes, additional_bytes=additional_reserve_bytes
        )
        required_bytes = payload_bytes + reserve_bytes
        try:
            available_bytes = shutil.disk_usage(directory).free
        except OSError as exc:
            raise BackupCapacityError(
                f"cannot determine storage capacity for {operation}",
                details={"target": str(directory), "reason": str(exc)},
            ) from exc
        if available_bytes < required_bytes:
            raise InsufficientBackupSpace(
                f"insufficient storage for {operation}: need {required_bytes} bytes "
                f"including a {reserve_bytes}-byte reserve, have {available_bytes} bytes",
                details={
                    "operation": operation,
                    "target": str(directory),
                    "payload_bytes": payload_bytes,
                    "reserve_bytes": reserve_bytes,
                    "required_bytes": required_bytes,
                    "available_bytes": available_bytes,
                },
            )


__all__ = [
    "BACKUP_RESERVE_PERCENT",
    "MINIMUM_BACKUP_RESERVE_BYTES",
    "BackupCapacityError",
    "InsufficientBackupSpace",
    "backup_reserve_bytes",
    "preflight_capacity",
]
