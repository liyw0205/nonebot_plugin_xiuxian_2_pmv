from __future__ import annotations

import logging
import re
import threading
from pathlib import Path
from typing import Mapping, Protocol, Sequence


Release = Mapping[str, object]
ReleaseAsset = Mapping[str, object]
UPDATE_ASSET_NAME = "project.tar.gz"
_RELEASE_TAG_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}\Z")
_logger = logging.getLogger(__name__)


def is_valid_release_tag(value: object) -> bool:
    return isinstance(value, str) and _RELEASE_TAG_RE.fullmatch(value) is not None


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


class UpdateApplication:
    _update_lock = threading.Lock()

    def __init__(self, provider: UpdateProvider) -> None:
        self._provider = provider

    def current_version(self) -> str:
        return self._provider.get_current_version()

    def latest_releases(self, count: int = 10) -> Sequence[Release]:
        return self._provider.get_latest_releases(count)

    def check_update(self) -> tuple[Release | None, str]:
        releases = self.latest_releases(1)
        if not releases:
            return None, "无法获取更新信息"

        latest_release = releases[0]
        latest_version = str(latest_release.get("tag_name", ""))
        current_version = self.current_version()
        if latest_version != current_version:
            return latest_release, f"发现新版本 {latest_version}，当前版本 {current_version}"
        return None, "当前已是最新版本"

    def perform_update_with_backup(self, release_tag: object) -> tuple[bool, str]:
        if not is_valid_release_tag(release_tag):
            return False, "无效 release 标签"

        if not UpdateApplication._update_lock.acquire(blocking=False):
            return False, "已有更新任务正在执行"

        archive_path: Path | None = None
        try:
            prepared, asset_or_message = self._provider.prepare_release_asset(release_tag)
            if not prepared:
                return False, str(asset_or_message)
            if (
                not isinstance(asset_or_message, Mapping)
                or asset_or_message.get("name") != UPDATE_ASSET_NAME
            ):
                return False, f"更新资源预检失败: 未找到 {UPDATE_ASSET_NAME}"
            target_asset = asset_or_message

            backups = (
                ("插件", self._provider.enhanced_backup_current_version),
                ("数据库", self._provider.backup_db_files),
                ("配置", self._provider.backup_all_configs),
            )
            config_backup_path: Path | None = None
            for label, create_backup in backups:
                backed_up, backup_result = create_backup()
                if not backed_up:
                    return False, f"{label}备份失败: {backup_result}"
                if label == "配置":
                    config_backup_path = Path(backup_result)

            downloaded, archive_or_message = self._provider.download_release(
                release_tag, target_asset=target_asset
            )
            if not downloaded:
                return False, str(archive_or_message)
            archive_path = Path(archive_or_message)

            updated, message = self._provider.extract_update(
                archive_path,
                backup=False,
                release_tag=release_tag,
            )
            if not updated:
                return False, message

            if config_backup_path is not None:
                try:
                    restored, restore_message = self._provider.restore_config_from_backup(
                        config_backup_path
                    )
                    if not restored:
                        _logger.warning("配置恢复失败: %s", restore_message)
                except Exception as exc:
                    _logger.warning("配置恢复失败: %s", exc)

            return True, message
        except Exception as exc:
            return False, f"更新过程中出现错误: {exc}"
        finally:
            if archive_path is not None:
                try:
                    self._provider.cleanup_download(archive_path)
                except Exception as exc:
                    _logger.warning("清理更新下载文件失败: %s", exc)
            UpdateApplication._update_lock.release()


__all__ = ["UpdateApplication", "UpdateProvider", "is_valid_release_tag"]
