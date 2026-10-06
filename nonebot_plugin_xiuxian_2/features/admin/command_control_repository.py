from __future__ import annotations

import copy
import json
from pathlib import Path
from threading import RLock
from typing import Any

from ...infrastructure.filesystem import atomic_write


COMMAND_DISABLE_EXEMPT_MODULE = "xiuxian_admin"
_LOCK = RLock()


class AdminCommandControlRepository:
    """Share persisted flags and runtime command indexes across bot and web callers."""

    _cache: dict[Path, tuple[tuple[int, int, int], dict[str, Any]]] = {}
    _active_cache: dict[Path, dict[str, dict[str, Any]]] = {}
    _registries: dict[Path, dict[str, str]] = {}
    _aliases: dict[Path, dict[str, str]] = {}

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).resolve()

    @staticmethod
    def _commands(document: dict[str, Any]) -> dict[str, Any]:
        commands = document.get("commands", document)
        if not isinstance(commands, dict):
            raise ValueError("command control commands must contain an object")
        return commands

    def _document_view(self) -> dict[str, Any]:
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            if self._cache.pop(self.path, None) is not None:
                self._active_cache.pop(self.path, None)
            return {"commands": {}}
        key = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
        cached = self._cache.get(self.path)
        if cached is None or cached[0] != key:
            with self.path.open(encoding="utf-8") as stream:
                document = json.load(stream)
            if not isinstance(document, dict):
                raise ValueError("command control file must contain an object")
            self._commands(document)
            self._cache[self.path] = (key, document)
            self._active_cache.pop(self.path, None)
        return self._cache[self.path][1]

    def _read_document(self) -> dict[str, Any]:
        return copy.deepcopy(self._document_view())

    @staticmethod
    def _entry(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return {
                "disabled": bool(value.get("disabled", False)),
                "module": str(value.get("module") or ""),
            }
        return {"disabled": bool(value), "module": ""}

    @classmethod
    def _locations(cls, document: dict[str, Any]) -> dict[str, str]:
        return {
            name.strip(): name
            for name in cls._commands(document)
            if isinstance(name, str) and name.strip()
        }

    def _entries(
        self, document: dict[str, Any], *, locations: dict[str, str] | None = None,
    ) -> dict[str, dict[str, Any]]:
        commands = self._commands(document)
        if locations is None:
            locations = self._locations(document)
        entries = {
            name: self._entry(commands[key])
            for name, key in locations.items()
        }
        registry = self._registries.get(self.path)
        if registry is None:
            return entries
        return {
            name: {
                "disabled": bool(entries.get(name, {}).get("disabled", False)),
                "module": module or str(entries.get(name, {}).get("module") or ""),
            }
            for name, module in registry.items()
        }

    def _active_entries(self) -> dict[str, dict[str, Any]]:
        document = self._document_view()
        if self.path not in self._active_cache:
            self._active_cache[self.path] = self._entries(document)
        return self._active_cache[self.path]

    def _persist(self, document: dict[str, Any]) -> None:
        atomic_write(self.path, json.dumps(document, ensure_ascii=False, indent=2).encode("utf-8"))
        # A failed replace must leave both the persisted and cached views unchanged.
        self._cache.pop(self.path, None)
        self._active_cache.pop(self.path, None)

    def read_entries(self) -> dict[str, dict[str, Any]]:
        with _LOCK:
            return copy.deepcopy(self._active_entries())

    def sync_command_registry(self, registry: dict[str, str]) -> dict[str, dict[str, Any]]:
        if not isinstance(registry, dict):
            raise TypeError("command registry must be a dict")
        cleaned: dict[str, str] = {}
        for name, module in registry.items():
            if not isinstance(name, str) or not isinstance(module, str):
                raise TypeError("command names and modules must be strings")
            name = name.strip()
            if name:
                cleaned[name] = module.strip()
        cleaned = dict(sorted(cleaned.items()))
        with _LOCK:
            document = self._read_document()
            before = copy.deepcopy(document)
            commands = self._commands(document)
            locations = self._locations(document)
            for name, module in cleaned.items():
                key = locations.get(name, name)
                old = commands.get(key)
                normalized = self._entry(old)
                final_module = module or normalized["module"]
                if key not in commands:
                    commands[key] = {"disabled": False, "module": final_module}
                elif isinstance(old, dict):
                    if final_module != normalized["module"]:
                        old["module"] = final_module
                elif final_module:
                    commands[key] = {"disabled": normalized["disabled"], "module": final_module}
            if document != before:
                self._persist(document)
            # Dormant entries remain on disk, but never join the active route registry.
            if self._registries.get(self.path) != cleaned:
                self._registries[self.path] = cleaned
                self._active_cache.pop(self.path, None)
            return self.read_entries()

    def rebuild_alias_index(self, alias_map: dict[str, str]) -> None:
        if not isinstance(alias_map, dict):
            raise TypeError("command aliases must be a dict")
        cleaned: dict[str, str] = {}
        for trigger, primary in alias_map.items():
            if not isinstance(trigger, str) or not isinstance(primary, str):
                raise TypeError("command aliases must be strings")
            trigger, primary = trigger.strip(), primary.strip()
            if trigger and primary:
                cleaned[trigger] = primary
        with _LOCK:
            self._aliases[self.path] = cleaned

    def resolve_primary_name(self, name: str) -> str:
        key = (name or "").strip()
        with _LOCK:
            return self._aliases.get(self.path, {}).get(key, key) if key else ""

    def known_commands(self) -> frozenset[str]:
        with _LOCK:
            return frozenset(self._active_entries())

    def known_modules(self) -> frozenset[str]:
        with _LOCK:
            return frozenset(
                module for info in self._active_entries().values()
                if (module := str(info["module"]).strip())
            )

    def is_command_disabled(self, name: str) -> bool:
        with _LOCK:
            primary = self.resolve_primary_name(name)
            entry = self._active_entries().get(primary)
            return bool(entry and entry["module"] != COMMAND_DISABLE_EXEMPT_MODULE and entry["disabled"])

    @staticmethod
    def _module_commands(entries: dict[str, dict[str, Any]], module: str) -> list[str]:
        if not module or module == COMMAND_DISABLE_EXEMPT_MODULE:
            return []
        return sorted(name for name, entry in entries.items() if entry["module"] == module)

    def commands_in_module(self, module: str) -> list[str]:
        with _LOCK:
            return self._module_commands(self._active_entries(), (module or "").strip())

    @classmethod
    def _set_disabled(
        cls, document: dict[str, Any], name: str, entry: dict[str, Any], disabled: bool,
        *, locations: dict[str, str],
    ) -> None:
        commands = cls._commands(document)
        key = locations.get(name, name)
        if key not in commands:
            commands[key] = {"disabled": disabled, "module": entry["module"]}
        elif cls._entry(commands[key])["disabled"] != disabled:
            if isinstance(commands[key], dict):
                commands[key]["disabled"] = disabled
            else:
                commands[key] = disabled

    def set_command_disabled(self, name: str, *, disabled: bool) -> tuple[bool, str]:
        if not isinstance(disabled, bool):
            raise TypeError("disabled must be a bool")
        with _LOCK:
            primary = self.resolve_primary_name(name)
            if not primary:
                return False, "请指定指令名"
            document = self._read_document()
            locations = self._locations(document)
            entry = self._entries(document, locations=locations).get(primary)
            if entry is None:
                return False, f"未登记指令：{primary}"
            if entry["module"] == COMMAND_DISABLE_EXEMPT_MODULE:
                return False, f"管理员指令不可禁用：{primary}"
            before = copy.deepcopy(document)
            self._set_disabled(document, primary, entry, disabled, locations=locations)
            if document != before:
                self._persist(document)
            return True, ""

    def apply_disable_targets(self, raw: str, *, disabled: bool) -> tuple[list[str], list[str]]:
        if not isinstance(disabled, bool):
            raise TypeError("disabled must be a bool")
        text = (raw or "").strip()
        if not text:
            return [], ["请指定指令名或子模块，多个用英文逗号分隔"]
        tokens = [token.strip() for token in text.replace("，", ",").split(",") if token.strip()]
        if not tokens:
            return [], ["请指定指令名或子模块"]
        with _LOCK:
            document = self._read_document()
            before = copy.deepcopy(document)
            locations = self._locations(document)
            entries = self._entries(document, locations=locations)
            changed: list[str] = []
            errors: list[str] = []
            seen: set[str] = set()
            for token in tokens:
                if token == COMMAND_DISABLE_EXEMPT_MODULE:
                    errors.append("xiuxian_admin 不参与指令禁用")
                    continue
                if token in entries:
                    targets = [token]
                else:
                    targets = self._module_commands(entries, token)
                    if not targets:
                        primary = self.resolve_primary_name(token)
                        targets = [primary] if primary in entries else []
                if not targets:
                    errors.append(
                        f"子模块 {token} 下无已登记指令" if token.startswith("xiuxian_") else f"未登记：{token}"
                    )
                    continue
                for name in targets:
                    if entries[name]["module"] == COMMAND_DISABLE_EXEMPT_MODULE:
                        errors.append(f"管理员指令不可禁用：{name}")
                    elif name not in seen:
                        self._set_disabled(document, name, entries[name], disabled, locations=locations)
                        changed.append(name)
                        seen.add(name)
            if document != before:
                self._persist(document)
            return changed, errors

    def collect_command_list_rows(
        self, raw_filter: str = "", *, only_disabled: bool = False,
    ) -> list[tuple[str, str, str]]:
        tokens = [
            token.strip()
            for token in (raw_filter or "").strip().replace("，", ",").replace("/", ",").split(",")
            if token.strip()
        ]
        rows: list[tuple[str, str, str]] = []
        with _LOCK:
            for name, entry in self._active_entries().items():
                module, disabled = entry["module"], entry["disabled"]
                if module == COMMAND_DISABLE_EXEMPT_MODULE or (only_disabled and not disabled):
                    continue
                if tokens and not any(token in name or token in module for token in tokens):
                    continue
                rows.append((name, module, "禁用" if disabled else "启用"))
        return sorted(rows, key=lambda row: (row[1] or "\uffff", row[0]))


__all__ = ["AdminCommandControlRepository", "COMMAND_DISABLE_EXEMPT_MODULE"]
