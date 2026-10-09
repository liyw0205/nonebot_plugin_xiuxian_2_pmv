"""Ports the release-check and upgrade owner needs.

``UpdateProvider`` is the only dependency of ``UpdateApplication``: the release
directory, the download, the three backup steps, the extract and the rollback all
sit behind it.  In production the implementation is the legacy ``UpdateManager``;
this slice never touches a network stack, a tar reader or a database itself.  The
two mapping aliases name the untrusted release documents the provider hands back,
so callers can annotate against the port instead of the legacy class.
"""

from __future__ import annotations

from pathlib import Path
from typing import Mapping, Protocol, Sequence


Release = Mapping[str, object]
ReleaseAsset = Mapping[str, object]


class UpdateProvider(Protocol):
    def get_current_version(self) -> str: ...

    def get_latest_releases(self, count: int) -> Sequence[Release]: ...

    def prepare_release_asset(self, tag: str) -> tuple[bool, ReleaseAsset | str]: ...

    def enhanced_backup_current_version(self) -> tuple[bool, object]: ...

    def backup_db_files(self) -> tuple[bool, object]: ...

    def backup_all_configs(self) -> tuple[bool, Path | str]: ...

    def download_release(
        self, tag: str, *, target_asset: ReleaseAsset
    ) -> tuple[bool, Path | str]: ...

    def extract_update(
        self, archive: Path, backup: bool = False, *, release_tag: str
    ) -> tuple[bool, str]: ...

    def restore_config_from_backup(self, path: Path) -> tuple[bool, str]: ...

    def cleanup_download(self, path: Path) -> None: ...


__all__ = ["Release", "ReleaseAsset", "UpdateProvider"]
