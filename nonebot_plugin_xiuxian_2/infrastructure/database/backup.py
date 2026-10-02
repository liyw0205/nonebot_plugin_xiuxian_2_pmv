"""Atomic, metadata-bearing SQLite backup and restore helpers."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path
from typing import Any, Mapping

from ..clock import SystemClock
from .catalog import DatabaseCatalog
from .backup_capacity import BackupCapacityError, preflight_capacity


class BackupService:
    def __init__(self, catalog: DatabaseCatalog, *, extra_files: Mapping[str, str | Path] | None = None, clock: Any | None = None) -> None:
        self.catalog = catalog
        self.extra_files = {str(key): Path(value) for key, value in (extra_files or {}).items()}
        self.extra_files.setdefault(
            "legacy_activity", catalog.path("game_db").parent / "activity" / "activity.db"
        )
        self.clock = clock or SystemClock()

    @staticmethod
    def _digest(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _sqlite_snapshot_size(path: Path) -> int:
        if not path.is_file():
            raise BackupCapacityError(
                "cannot estimate backup size",
                details={"target": str(path), "reason": "database is not a regular file"},
            )
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=2.0
            )
            page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
            page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
            return page_count * page_size
        except (OSError, sqlite3.DatabaseError) as exc:
            raise BackupCapacityError(
                "cannot estimate backup size",
                details={"target": str(path), "reason": str(exc)},
            ) from exc
        finally:
            if connection is not None:
                connection.close()

    def _create_size_estimates(self) -> dict[Path, int]:
        estimates: dict[Path, int] = {}
        for spec in self.catalog.specs():
            source = spec.path
            if source.exists():
                estimates[source] = self._sqlite_snapshot_size(source)
        for key, source in self.extra_files.items():
            if not source.exists() or not source.is_file():
                continue
            estimates[source] = (
                self._sqlite_snapshot_size(source)
                if key == "legacy_activity"
                else source.stat().st_size
            )
        return estimates

    def create(self, destination: str | Path) -> Path:
        target = Path(destination).expanduser().resolve()
        preflight_capacity(
            {target: sum(self._create_size_estimates().values())},
            operation="backup",
        )
        target.mkdir(parents=True, exist_ok=True)
        stamp = self.clock.now().strftime("%Y%m%dT%H%M%SZ")
        directory = target / stamp
        directory.mkdir()
        files: list[dict[str, Any]] = []
        try:
            for spec in self.catalog.specs():
                source = spec.path
                if not source.exists():
                    continue
                destination_file = directory / source.name
                temporary = destination_file.with_suffix(destination_file.suffix + ".tmp")
                source_connection = sqlite3.connect(source)
                target_connection = sqlite3.connect(temporary)
                try:
                    source_connection.backup(target_connection)
                finally:
                    target_connection.close()
                    source_connection.close()
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                temporary.replace(destination_file)
                files.append({"database": spec.key, "name": source.name, "sha256": self._digest(destination_file), "size": destination_file.stat().st_size})
            for key, source in self.extra_files.items():
                if not source.exists() or not source.is_file():
                    continue
                destination_file = directory / source.name
                temporary = destination_file.with_suffix(destination_file.suffix + ".tmp")
                if key == "legacy_activity":
                    source_connection = sqlite3.connect(f"{source.resolve().as_uri()}?mode=ro", uri=True)
                    target_connection = sqlite3.connect(temporary)
                    try:
                        source_connection.backup(target_connection)
                    finally:
                        target_connection.close()
                        source_connection.close()
                else:
                    shutil.copy2(source, temporary)
                with temporary.open("rb") as stream:
                    os.fsync(stream.fileno())
                temporary.replace(destination_file)
                files.append({"file": key, "name": source.name, "sha256": self._digest(destination_file), "size": destination_file.stat().st_size})
            manifest = directory / "manifest.json"
            manifest.write_text(json.dumps({"created_at": self.clock.now().isoformat(), "files": files}, ensure_ascii=False, indent=2), encoding="utf-8")
            return directory
        except Exception:
            shutil.rmtree(directory, ignore_errors=True)
            raise

    def restore(self, backup: str | Path, *, dry_run: bool = False) -> dict[str, Any]:
        source = Path(backup).expanduser().resolve()
        manifest_path = source / "manifest.json"
        if not manifest_path.is_file():
            raise ValueError("backup manifest is missing")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        restore_plan: list[tuple[Path, Path, str]] = []
        for item in manifest.get("files", []):
            name = Path(str(item.get("name", ""))).name
            candidate = source / name
            if not candidate.is_file() or self._digest(candidate) != item.get("sha256"):
                raise ValueError(f"backup checksum mismatch: {name}")
            key = str(item.get("database", ""))
            if key:
                destination = self.catalog.path(key)
                restored_key = key
            else:
                file_key = str(item.get("file", ""))
                if file_key not in self.extra_files:
                    raise ValueError(f"unknown backup file: {file_key}")
                destination = self.extra_files[file_key]
                restored_key = file_key
            restore_plan.append((candidate, destination, restored_key))

        if not dry_run and restore_plan:
            capacity_requirements: dict[Path, int] = {}
            for candidate, destination, _ in restore_plan:
                capacity_requirements[destination] = (
                    capacity_requirements.get(destination, 0) + candidate.stat().st_size
                )
            preflight_capacity(
                capacity_requirements,
                operation="restore",
            )

        restored: list[str] = []
        for candidate, destination, restored_key in restore_plan:
            if not dry_run:
                destination.parent.mkdir(parents=True, exist_ok=True)
                temporary = destination.with_suffix(destination.suffix + ".restore.tmp")
                try:
                    shutil.copy2(candidate, temporary)
                    temporary.replace(destination)
                except Exception:
                    temporary.unlink(missing_ok=True)
                    raise
            restored.append(restored_key)
        return {"dry_run": dry_run, "restored": restored, "manifest": manifest}


__all__ = ["BackupService"]
