from __future__ import annotations

from typing import Any

from ...paths import get_paths
from .command_control_repository import AdminCommandControlRepository


class AdminCommandControlApplication:
    def __init__(self, repository: AdminCommandControlRepository | None = None):
        self.repository = repository or AdminCommandControlRepository(
            get_paths().data / "command_disable.json"
        )

    def read_entries(self) -> dict[str, dict[str, Any]]:
        return self.repository.read_entries()

    def sync_command_registry(self, registry: dict[str, str]) -> dict[str, dict[str, Any]]:
        return self.repository.sync_command_registry(registry)

    def rebuild_alias_index(self, alias_map: dict[str, str]) -> None:
        self.repository.rebuild_alias_index(alias_map)

    def resolve_primary_name(self, name: str) -> str:
        return self.repository.resolve_primary_name(name)

    def is_command_disabled(self, name: str) -> bool:
        return self.repository.is_command_disabled(name)

    def known_commands(self) -> frozenset[str]:
        return self.repository.known_commands()

    def known_modules(self) -> frozenset[str]:
        return self.repository.known_modules()

    def commands_in_module(self, module: str) -> list[str]:
        return self.repository.commands_in_module(module)

    def set_command_disabled(self, name: str, *, disabled: bool) -> tuple[bool, str]:
        return self.repository.set_command_disabled(name, disabled=disabled)

    def apply_disable_targets(self, raw: str, *, disabled: bool) -> tuple[list[str], list[str]]:
        return self.repository.apply_disable_targets(raw, disabled=disabled)

    def collect_command_list_rows(
        self, raw_filter: str = "", *, only_disabled: bool = False
    ) -> list[tuple[str, str, str]]:
        return self.repository.collect_command_list_rows(raw_filter, only_disabled=only_disabled)
