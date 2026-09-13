from __future__ import annotations

import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory


# These directories are created by the runtime and must never be copied into
# a test data directory.  Static JSON/image assets live beside them in the
# legacy layout, so the copy is intentionally file-based rather than a whole
# tree copy.
_RUNTIME_DIRS = {"backups", "cache", "logs", "players"}
_RUNTIME_FILES = {
    "blackhouse.json",
    "compatibility_hits.json",
    "message_db_config.json",
    "scheduler_overrides.json",
    "web_secret_key",
}
_DATABASE_SUFFIXES = (".db", ".db-shm", ".db-wal", ".sqlite", ".sqlite3")


def copy_static_data(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> Path:
    """Overlay immutable repository assets onto an isolated data directory."""
    source_path = Path(source).resolve()
    target_path = Path(target).resolve()
    target_path.mkdir(parents=True, exist_ok=True)
    for path in source_path.rglob("*"):
        relative = path.relative_to(source_path)
        if any(part in _RUNTIME_DIRS for part in relative.parts):
            continue
        if not path.is_file():
            continue
        if path.name in _RUNTIME_FILES:
            continue
        if any(path.name.endswith(suffix) for suffix in _DATABASE_SUFFIXES):
            continue
        destination = target_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, destination)
    return target_path


@contextmanager
def isolated_data_dir():
    """Unittest equivalent of the pytest fixture."""
    with TemporaryDirectory(prefix="xiuxian-test-") as directory:
        previous = os.environ.get("XIUXIAN_DATA_DIR")
        data_dir = copy_static_data(
            Path(__file__).resolve().parents[1] / "data" / "xiuxian",
            Path(directory) / "data",
        )
        os.environ["XIUXIAN_DATA_DIR"] = str(data_dir)
        from nonebot_plugin_xiuxian_2.paths import configure_paths, reset_paths_for_test

        configure_paths(os.environ["XIUXIAN_DATA_DIR"])
        try:
            yield data_dir
        finally:
            if previous is None:
                os.environ.pop("XIUXIAN_DATA_DIR", None)
            else:
                os.environ["XIUXIAN_DATA_DIR"] = previous
            reset_paths_for_test()


__all__ = ["copy_static_data", "isolated_data_dir"]
