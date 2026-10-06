from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from pathlib import Path
from threading import RLock
from typing import Any


ITEM_PATHS = (
    ("功法", "功法", "主功法.json"), ("辅修功法", "功法", "辅修功法.json"),
    ("神通", "功法", "神通.json"), ("身法", "功法", "身法.json"), ("瞳术", "功法", "瞳术.json"),
    ("法器", "装备", "法器.json"), ("防具", "装备", "防具.json"), ("饰品", "装备", "饰品.json"),
    ("丹药", "丹药", "丹药.json"), ("礼包", "礼包", ""), ("药材", "丹药", "药材.json"),
    ("合成丹药", "丹药", "炼丹丹药.json"), ("炼丹炉", "丹药", "炼丹炉.json"),
    ("聚灵旗", "修炼物品", "聚灵旗.json"), ("称号", "修炼物品", "称号.json"),
    ("神物", "丹药", "神物.json"), ("特殊物品", "修炼物品", "特殊物品.json"),
)
SKILL_TYPES = frozenset({"功法", "辅修功法", "神通", "身法", "瞳术"})


class _CatalogMapping(Mapping):
    """A stable legacy handle whose iterators retain one published generation."""

    def __init__(self, repository: "AdminItemCatalogRepository", index: int) -> None:
        self.repository = repository
        self.index = index

    def _view(self):
        with self.repository.lock:
            return self.repository._state[self.index]

    def __getitem__(self, key):
        return copy.deepcopy(self._view()[key])

    def __iter__(self):
        return iter(self._view())

    def __len__(self):
        return len(self._view())

    def items(self):
        return copy.deepcopy(self._view()).items()

    def keys(self):
        return self._view().keys()

    def values(self):
        return copy.deepcopy(self._view()).values()

    def copy(self):
        return copy.deepcopy(self._view())


class AdminItemCatalogRepository:
    """Build catalogs off-state and publish items and package provenance together."""

    def __init__(self, type_to_path: Mapping[str, str | Path]) -> None:
        self.type_to_path = {name: Path(path) for name, path in type_to_path.items()}
        self.lock = RLock()
        self._reload_lock = RLock()
        self._state: tuple[dict[str, dict[str, Any]], dict[str, Path]] = ({}, {})
        self._loaded = False
        self.items = _CatalogMapping(self, 0)
        self.package_source_map = _CatalogMapping(self, 1)

    @staticmethod
    def read_file(path: str | Path) -> dict:
        with Path(path).open(encoding="utf-8") as stream:
            data = json.load(stream)
        if not isinstance(data, dict):
            raise ValueError("item category must contain an object")
        return data

    def read_category(
        self, item_type: str, *, strict: bool = True,
        errors: list[dict[str, str]] | None = None,
    ) -> tuple[dict, dict[str, Path]]:
        path = self.type_to_path[item_type]
        if item_type == "礼包":
            return self.read_bundle(path, item_type, strict=strict, errors=errors)
        return self.read_file(path), {}

    def read_bundle(
        self, path: str | Path, item_type: str = "礼包", *, strict: bool = True,
        errors: list[dict[str, str]] | None = None,
    ) -> tuple[dict, dict[str, Path]]:
        path = Path(path)
        if not path.is_dir():
            data = self.read_file(path)
            return data, {str(key): path for key in data} if item_type == "礼包" else {}
        files = sorted(path.glob("*.json"))
        split_files = [file for file in files if file.name != "礼包.json"]
        if item_type == "礼包" and split_files:
            files = split_files
        data, sources = {}, {}
        for file in files:
            try:
                rows = self.read_file(file)
            except (OSError, ValueError) as exc:
                if strict:
                    raise
                if errors is not None:
                    errors.append({"item_type": item_type, "error_type": type(exc).__name__})
                continue
            for item_id, row in rows.items():
                item_id = str(item_id)
                if item_id not in data:
                    data[item_id] = row
                    if item_type == "礼包":
                        sources[item_id] = file
        return data, sources

    @staticmethod
    def _normalize_item(item_id: str, value: Any, item_type: str) -> dict[str, Any]:
        if not item_id.strip() or not isinstance(value, dict):
            raise ValueError("item id and object are required")
        item = copy.deepcopy(value)
        if item_type in SKILL_TYPES:
            if "rank" not in item or "level" not in item:
                raise ValueError("skill item requires rank and level")
            item["type"] = "技能"
            item["rank"], item["level"] = item["level"], item["rank"]
        item["item_type"] = item_type
        return item

    def _load(self, *, strict: bool) -> dict[str, Any]:
        items: dict[str, dict[str, Any]] = {}
        sources: dict[str, Path] = {}
        errors: list[dict[str, str]] = []
        categories = 0
        ordered = [name for name, _, _ in ITEM_PATHS if name in self.type_to_path]
        ordered.extend(name for name in self.type_to_path if name not in ordered)
        for item_type in ordered:
            try:
                rows, category_sources = self.read_category(item_type, strict=strict, errors=errors)
            except (OSError, ValueError) as exc:
                if strict:
                    raise
                errors.append({"item_type": item_type, "error_type": type(exc).__name__})
                continue
            categories += 1
            sources.update(category_sources)
            for item_id, value in rows.items():
                item_id = str(item_id)
                if item_id in items:
                    continue
                try:
                    items[item_id] = self._normalize_item(item_id, value, item_type)
                except ValueError as exc:
                    if strict:
                        raise
                    errors.append({"item_type": item_type, "error_type": type(exc).__name__})
                    sources.pop(item_id, None)
        with self.lock:
            self._state = (items, sources)
            self._loaded = True
        return {
            "status": "reloaded" if strict else "initialized_degraded" if errors else "initialized",
            "item_count": len(items), "category_count": categories, "errors": errors,
        }

    def ensure_loaded(self) -> dict[str, Any] | None:
        with self._reload_lock:
            if self._loaded:
                return None
            return self._load(strict=False)

    def reload(self) -> dict[str, Any]:
        with self._reload_lock:
            return self._load(strict=True)

    def merge_items(self, rows: dict, item_type: str) -> None:
        if not isinstance(rows, dict):
            raise ValueError("item category must contain an object")
        with self._reload_lock, self.lock:
            items, sources = self._state
            merged = dict(items)
            for key, value in rows.items():
                item_id = str(key)
                if item_id not in merged:
                    merged[item_id] = self._normalize_item(item_id, value, item_type)
            self._state = (merged, sources)

    def get_data_by_item_id(self, item_id):
        if item_id is None:
            return None
        with self.lock:
            value = self._state[0].get(str(item_id))
        return copy.deepcopy(value)

    def get_data_by_item_name(self, item_name):
        if item_name is None:
            return None, None
        item_name = str(item_name).strip()
        if item_name.isdigit():
            item = self.get_data_by_item_id(item_name)
            return (int(item_name), item) if item else (None, None)
        with self.lock:
            items = self._state[0]
        for item_id, item in items.items():
            if str(item.get("name")) == item_name:
                return int(item_id), copy.deepcopy(item)
        return None, None

    def get_data_by_item_type(self, item_type):
        with self.lock:
            items = self._state[0]
        return {key: copy.deepcopy(value) for key, value in items.items() if value["item_type"] in item_type}

    def get_fusion_items(self):
        with self.lock:
            items = self._state[0]
        return [f"{value['name']} ({value['item_type']})" for value in items.values() if "fusion" in value]

    def get_random_id_list_by_rank_and_item_type(self, fanil_rank: int, item_type=None):
        with self.lock:
            items = self._state[0]
        result = []
        for key, value in items.items():
            try:
                rank = int(value["rank"])
            except (ValueError, TypeError, KeyError, OverflowError):
                continue
            if (item_type is None or value["item_type"] in item_type) and fanil_rank <= rank <= fanil_rank + 40:
                result.append(key)
        return result

    def snapshot(self) -> tuple[dict, dict]:
        with self.lock:
            state = self._state
        return copy.deepcopy(state)


_OWNERS: dict[Path, AdminItemCatalogRepository] = {}
_OWNER_LOCK = RLock()


def get_item_catalog_repository(data_root: str | Path) -> AdminItemCatalogRepository:
    data_root = Path(data_root).resolve()
    with _OWNER_LOCK:
        if data_root not in _OWNERS:
            paths = {name: data_root / directory / filename for name, directory, filename in ITEM_PATHS}
            _OWNERS[data_root] = AdminItemCatalogRepository(paths)
        return _OWNERS[data_root]


__all__ = ["AdminItemCatalogRepository", "get_item_catalog_repository"]
