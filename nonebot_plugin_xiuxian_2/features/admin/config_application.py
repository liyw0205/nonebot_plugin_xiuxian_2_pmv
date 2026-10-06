from __future__ import annotations

from ...paths import get_paths
from .config_repository import AdminConfigRepository


class AdminConfigApplication:
    def __init__(self, repository: AdminConfigRepository | None = None):
        self.repository = repository or AdminConfigRepository(get_paths().data / "config.json")

    def set_switch(self, name: str, enabled: bool) -> bool:
        return self.repository.set_switch(name, enabled)

    def set_group_enabled(self, group_id, enabled: bool) -> bool:
        return self.repository.set_list_member("group", group_id, not enabled)

    def set_group_welcome(self, group_id, *, enabled: bool, globally_enabled: bool = True):
        if not str(group_id or "").strip():
            return False, "缺少群ID"
        if enabled and not globally_enabled:
            return False, "全局进群欢迎已关闭，请先开启全局配置"
        changed = self.repository.set_list_member("welcome_disabled_groups", group_id, not enabled)
        state = "开启" if enabled else "关闭"
        return changed, f"本群进群欢迎已{state}" + ("" if changed else "，无需重复操作")
