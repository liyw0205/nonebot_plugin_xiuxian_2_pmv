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

    def is_full_message_group(self, group_id) -> bool:
        return self.repository.is_full_message_group(group_id)

    def set_full_message_group(self, group_id, *, enabled: bool) -> bool:
        return self.repository.set_full_message_group(group_id, enabled)

    def get_group_remark(self, group_id) -> str:
        return self.repository.get_group_remark(group_id)

    def get_group_remarks(self) -> dict[str, str]:
        return self.repository.get_group_remarks()

    def set_group_remark(self, group_id, remark: str) -> tuple[bool, str]:
        return self.repository.set_group_remark(group_id, remark)

    @staticmethod
    def session_pin_key(scene: str, target_id: str) -> str:
        return AdminConfigRepository.session_pin_key(scene, target_id)

    def get_pinned_sessions(self) -> list[str]:
        return self.repository.get_pinned_sessions()

    def get_message_session_preferences(self) -> dict[str, set[str] | dict[str, str]]:
        data = self.repository.read_data()
        return {
            "full_message_groups": set(data.get("full_message_groups", [])),
            "group_remarks": dict(data.get("group_remarks", {})),
            "pinned_sessions": set(data.get("pinned_sessions", [])),
        }

    def is_session_pinned(self, scene: str, target_id: str) -> bool:
        return self.repository.is_session_pinned(scene, target_id)

    def set_session_pinned(self, scene: str, target_id: str, pinned: bool) -> tuple[bool, str]:
        return self.repository.set_session_pinned(scene, target_id, pinned)

    @staticmethod
    def _message_db_owner():
        from ...xiuxian.xiuxian_utils import message_db

        return message_db

    def get_message_db_config(self) -> dict[str, int]:
        return self._message_db_owner().get_message_db_config()

    def update_message_db_config(self, values: dict) -> dict[str, int]:
        return self._message_db_owner().update_message_db_config(values)

    def is_message_record_enabled(self) -> bool:
        return self._message_db_owner().is_message_record_enabled()
