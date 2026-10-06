from __future__ import annotations

import copy
import json
from pathlib import Path
from threading import RLock
from typing import Callable, TypeVar

from ...infrastructure.filesystem import atomic_write


T = TypeVar("T")
_LOCK = RLock()


class AdminConfigRepository:
    """Shared JSON owner for bot commands, legacy readers and the web thread."""

    _cache_key = None
    _cache_data = None
    _LIST_KEYS = ("group", "welcome_disabled_groups", "full_message_groups", "pinned_sessions")
    _SWITCH_KEYS = ("private", "root_selection", "sect_name")

    def __init__(self, path: str | Path):
        self.config_jsonpath = Path(path)

    @staticmethod
    def _as_str_list(value) -> list[str]:
        if not isinstance(value, list):
            return []
        return list(dict.fromkeys(text for item in value if (text := str(item or "").strip())))

    @staticmethod
    def _as_str_dict(value) -> dict:
        if not isinstance(value, dict):
            return {}
        return {
            key: text
            for k, v in value.items()
            if (key := str(k or "").strip()) and (text := str(v or "").strip())
        }

    @classmethod
    def _normalize_data(cls, data):
        if not isinstance(data, dict):
            raise ValueError("admin config must contain an object")
        for key in cls._LIST_KEYS:
            data[key] = cls._as_str_list(data.get(key, []))
        data["group_remarks"] = cls._as_str_dict(data.get("group_remarks", {}))
        for key in cls._SWITCH_KEYS:
            data.setdefault(key, True)
        return data

    @staticmethod
    def _clone_data(data):
        return copy.deepcopy(data)

    def create_default_config(self):
        with _LOCK:
            if not self.config_jsonpath.exists():
                self._persist(self._normalize_data({}))

    def read_data(self):
        with _LOCK:
            try:
                stat = self.config_jsonpath.stat()
            except FileNotFoundError:
                return self._normalize_data({})
            key = (self.config_jsonpath.absolute(), stat.st_ino, stat.st_mtime_ns, stat.st_size)
            if key != AdminConfigRepository._cache_key:
                with self.config_jsonpath.open(encoding="utf-8") as stream:
                    data = self._normalize_data(json.load(stream))
                AdminConfigRepository._cache_data = self._clone_data(data)
                AdminConfigRepository._cache_key = key
            return self._clone_data(AdminConfigRepository._cache_data)

    def _persist(self, data: dict) -> bool:
        with _LOCK:
            atomic_write(
                self.config_jsonpath,
                json.dumps(data, ensure_ascii=False, indent=4).encode("utf-8"),
            )
            # Invalidate only after replacement; failed writes keep the old snapshot.
            AdminConfigRepository._cache_key = None
            AdminConfigRepository._cache_data = None
            return True

    def update(self, change: Callable[[dict], T]) -> T:
        with _LOCK:
            data = self.read_data()
            before = self._clone_data(data)
            result = change(data)
            if data != before:
                self._persist(data)
            return result

    def set_switch(self, name: str, enabled: bool) -> bool:
        if name not in self._SWITCH_KEYS:
            raise KeyError(name)
        if not isinstance(enabled, bool):
            raise TypeError("enabled must be a bool")

        def change(data):
            changed = bool(data[name]) != enabled
            if changed:
                data[name] = enabled
            return changed

        return self.update(change)

    def set_list_member(self, name: str, identity, present: bool) -> bool:
        if name not in self._LIST_KEYS:
            raise KeyError(name)
        identity = str(identity or "").strip()
        if not identity:
            raise ValueError("missing group/session identity")

        def change(data):
            values = data[name]
            if (identity in values) == present:
                return False
            if present:
                values.append(identity)
            else:
                values.remove(identity)
            return True

        return self.update(change)

    def write_data(self, key, id=None):
        switches = {3: ("private", True), 4: ("private", False),
                    5: ("root_selection", True), 6: ("root_selection", False),
                    7: ("sect_name", True), 8: ("sect_name", False)}
        lists = {1: ("group", True), 2: ("group", False),
                 9: ("welcome_disabled_groups", True), 10: ("welcome_disabled_groups", False),
                 11: ("full_message_groups", True), 12: ("full_message_groups", False)}
        if key in switches:
            self.set_switch(*switches[key])
        elif key in lists and str(id or "").strip():
            name, present = lists[key]
            self.set_list_member(name, id, present)
        return True
