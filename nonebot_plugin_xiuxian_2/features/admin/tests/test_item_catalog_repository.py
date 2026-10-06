from __future__ import annotations

import importlib.util
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

import pytest


def load_catalog_modules(monkeypatch, data_root, *, legacy=False):
    prefix = "_isolated_admin_item_catalog"
    package = Path(__file__).parents[3]
    for suffix in ("", ".features", ".features.admin", ".xiuxian", ".xiuxian.xiuxian_utils"):
        module = ModuleType(prefix + suffix)
        module.__path__ = []
        monkeypatch.setitem(sys.modules, module.__name__, module)
    paths = ModuleType(prefix + ".paths")
    paths.get_paths = lambda: SimpleNamespace(data=data_root)
    monkeypatch.setitem(sys.modules, paths.__name__, paths)
    loaded = {}
    files = {
        "repository": ("features.admin.item_catalog_repository", package / "features/admin/item_catalog_repository.py"),
        "application": ("features.admin.item_catalog_application", package / "features/admin/item_catalog_application.py"),
    }
    if legacy:
        log = ModuleType("nonebot.log")
        log.logger = SimpleNamespace(info=lambda *args: None, warning=lambda *args: None, error=lambda *args: None)
        monkeypatch.setitem(sys.modules, "nonebot.log", log)
        files["legacy"] = ("xiuxian.xiuxian_utils.item_json", package / "xiuxian/xiuxian_utils/item_json.py")
    for key, (suffix, path) in files.items():
        spec = importlib.util.spec_from_file_location(prefix + "." + suffix, path)
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
        loaded[key] = module
    return SimpleNamespace(**loaded)


def empty_catalog_files(repository):
    for item_type, path in repository.type_to_path.items():
        if item_type == "礼包":
            path.mkdir(parents=True, exist_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}", encoding="utf-8")


def write_category(repository, item_type, rows, *, filename="a.json"):
    path = repository.type_to_path[item_type]
    if item_type == "礼包":
        path.mkdir(parents=True, exist_ok=True)
        path = path / filename
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return path


@pytest.fixture
def catalog(monkeypatch, tmp_path):
    modules = load_catalog_modules(monkeypatch, tmp_path)
    repository = modules.repository.get_item_catalog_repository(tmp_path)
    empty_catalog_files(repository)
    return SimpleNamespace(modules=modules, repository=repository, root=tmp_path)


def test_empty_categories_and_empty_package_directory_are_valid(catalog):
    result = catalog.modules.application.AdminItemCatalogApplication(catalog.repository).reload()

    assert result == {"status": "reloaded", "item_count": 0, "category_count": 17, "errors": []}
    assert catalog.repository.snapshot() == ({}, {})


def test_skill_conversion_and_split_package_precedence_preserve_sources_and_files(catalog):
    for index, item_type in enumerate(("功法", "辅修功法", "神通", "身法", "瞳术")):
        write_category(catalog.repository, item_type, {
            str(100 + index): {"name": item_type, "rank": "quality", "level": index + 1, "unknown": {"keep": [1]}},
        })
    package_a = write_category(catalog.repository, "礼包", {"900": {"name": "first"}}, filename="a.json")
    write_category(catalog.repository, "礼包", {"900": {"name": "second"}}, filename="b.json")
    legacy = catalog.repository.type_to_path["礼包"] / "礼包.json"
    legacy.write_text("{ignored broken legacy", encoding="utf-8")
    write_category(catalog.repository, "法器", {"100": {"name": "later category duplicate"}})
    before = {path: path.read_bytes() for path in catalog.root.rglob("*.json")}

    catalog.repository.reload()

    for index in range(5):
        item = catalog.repository.get_data_by_item_id(100 + index)
        assert (item["rank"], item["level"], item["type"]) == (index + 1, "quality", "技能")
        assert item["unknown"] == {"keep": [1]}
    assert catalog.repository.get_data_by_item_id("900")["name"] == "first"
    assert catalog.repository.package_source_map["900"] == package_a
    assert {path: path.read_bytes() for path in before} == before


def test_strict_late_failure_preserves_both_old_cache_and_package_source_map(catalog):
    write_category(catalog.repository, "功法", {"100": {"name": "old", "rank": "quality", "level": 1}})
    old_source = write_category(catalog.repository, "礼包", {"900": {"name": "old package"}})
    catalog.repository.reload()
    old = catalog.repository.snapshot()
    write_category(catalog.repository, "功法", {"100": {"name": "new", "rank": "quality", "level": 2}})
    old_source.unlink()
    new_source = write_category(catalog.repository, "礼包", {"900": {"name": "new package"}}, filename="b.json")
    last_category = catalog.repository.type_to_path["特殊物品"]
    last_category.write_text("{broken", encoding="utf-8")

    with pytest.raises(ValueError):
        catalog.repository.reload()

    assert catalog.repository.snapshot() == old
    assert catalog.repository.package_source_map["900"] == old_source
    last_category.write_text("{}", encoding="utf-8")
    catalog.repository.reload()
    assert catalog.repository.get_data_by_item_id("100")["name"] == "new"
    assert catalog.repository.package_source_map["900"] == new_source


@pytest.mark.parametrize("bad", ["[]", "null", "", '{"100":[]}', '{"100":{"rank":1}}'])
def test_invalid_category_or_skill_never_partially_publishes(catalog, bad):
    write_category(catalog.repository, "法器", {"500": {"name": "kept"}})
    catalog.repository.reload()
    original = catalog.repository.snapshot()
    catalog.repository.type_to_path["功法"].write_text(bad, encoding="utf-8")

    with pytest.raises(ValueError):
        catalog.repository.reload()

    assert catalog.repository.snapshot() == original


def test_startup_degrades_without_missing_files_blocking_plugin_then_reload_is_strict(monkeypatch, tmp_path):
    modules = load_catalog_modules(monkeypatch, tmp_path)
    repository = modules.repository.get_item_catalog_repository(tmp_path)
    write_category(repository, "法器", {"500": {"name": "available"}})

    initial = repository.ensure_loaded()

    assert initial["status"] == "initialized_degraded"
    assert initial["errors"]
    assert repository.get_data_by_item_id("500")["name"] == "available"
    before = repository.snapshot()
    with pytest.raises(FileNotFoundError):
        repository.reload()
    assert repository.snapshot() == before
    empty_catalog_files(repository)
    assert repository.reload()["status"] == "reloaded"


def test_unreadable_later_package_file_does_not_publish_the_earlier_file(catalog):
    first = write_category(catalog.repository, "礼包", {"900": {"name": "first"}})
    second = write_category(catalog.repository, "礼包", {"901": {"name": "second"}}, filename="b.json")
    catalog.repository.reload()
    before = catalog.repository.snapshot()
    write_category(catalog.repository, "礼包", {"900": {"name": "modified"}})
    read_file = catalog.repository.read_file

    def fail(path):
        if path == second:
            raise PermissionError("unreadable")
        return read_file(path)

    with patch.object(catalog.repository, "read_file", side_effect=fail):
        with pytest.raises(PermissionError):
            catalog.repository.reload()
    assert catalog.repository.snapshot() == before
    assert catalog.repository.package_source_map["900"] == first


def test_existing_mapping_handles_and_iterators_keep_complete_generations(catalog):
    write_category(catalog.repository, "法器", {"500": {"name": "old"}})
    catalog.repository.reload()
    cache = catalog.repository.items
    sources = catalog.repository.package_source_map
    old_items = cache.items()
    write_category(catalog.repository, "法器", {"501": {"name": "new"}})

    catalog.repository.reload()

    assert cache is catalog.repository.items
    assert sources is catalog.repository.package_source_map
    assert [key for key, _ in old_items] == ["500"]
    assert list(cache) == ["501"]
    assert cache["501"]["name"] == "new"


def test_readers_can_observe_old_snapshot_while_reload_builds_then_see_one_commit(catalog):
    write_category(catalog.repository, "法器", {"500": {"name": "old"}})
    catalog.repository.reload()
    old = catalog.repository.snapshot()
    write_category(catalog.repository, "法器", {"501": {"name": "new"}})
    entered, release = Event(), Event()
    read_file = catalog.repository.read_file

    def pause(path):
        if path == catalog.repository.type_to_path["特殊物品"]:
            entered.set()
            assert release.wait(5)
        return read_file(path)

    with patch.object(catalog.repository, "read_file", side_effect=pause):
        with ThreadPoolExecutor(max_workers=1) as pool:
            running = pool.submit(catalog.repository.reload)
            try:
                assert entered.wait(5)
                assert catalog.repository.snapshot() == old
                assert list(catalog.repository.items) == ["500"]
            finally:
                release.set()
            assert running.result(timeout=5)["item_count"] == 1
    assert list(catalog.repository.items) == ["501"]


def test_single_item_and_filtered_queries_copy_only_returned_mutable_items(catalog):
    write_category(catalog.repository, "法器", {"500": {"name": "weapon", "rank": 20, "nested": [1], "fusion": {}}})
    write_category(catalog.repository, "防具", {"600": {"name": "armor", "rank": 61}})
    catalog.repository.reload()
    module = catalog.modules.repository
    deepcopy = module.copy.deepcopy

    with patch.object(module.copy, "deepcopy", wraps=deepcopy) as clone:
        selected = catalog.repository.get_data_by_item_type(["法器"])
        assert sum(call.args[0] is catalog.repository._state[0]["500"] for call in clone.call_args_list) == 1
        assert not any(call.args[0] is catalog.repository._state[0]["600"] for call in clone.call_args_list)
        assert not any(call.args[0] is catalog.repository._state[0] for call in clone.call_args_list)
    selected["500"]["nested"].append(2)
    item = catalog.repository.get_data_by_item_id("500")
    item["nested"].append(3)
    assert catalog.repository.get_data_by_item_name("weapon")[1]["nested"] == [1]
    with patch.object(module.copy, "deepcopy", side_effect=AssertionError("scalar query copied catalog")):
        assert catalog.repository.get_fusion_items() == ["weapon (法器)"]
        assert catalog.repository.get_random_id_list_by_rank_and_item_type(20) == ["500"]
        assert catalog.repository.get_random_id_list_by_rank_and_item_type(21, ["防具"]) == ["600"]


def test_default_repository_is_shared_by_path_and_falsey_injected_owner_is_used(catalog):
    assert catalog.modules.repository.get_item_catalog_repository(catalog.root / ".") is catalog.repository
    assert catalog.modules.repository.get_item_catalog_repository(catalog.root / "other") is not catalog.repository

    class FalseyOwner:
        def __bool__(self):
            return False

        def reload(self):
            return {"status": "injected"}

    owner = FalseyOwner()
    application = catalog.modules.application.AdminItemCatalogApplication(owner)
    assert application.repository is owner
    assert application.reload() == {"status": "injected"}
