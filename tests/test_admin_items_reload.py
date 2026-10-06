from __future__ import annotations

import ast
import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest


ROOT = Path(__file__).resolve().parents[1]
ADMIN = ROOT / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin/__init__.py"


@pytest.fixture
def legacy_catalog(monkeypatch, tmp_path):
    helper_path = ROOT / "nonebot_plugin_xiuxian_2/features/admin/tests/test_item_catalog_repository.py"
    spec = importlib.util.spec_from_file_location("_item_catalog_test_helpers", helper_path)
    helpers = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helpers)
    modules = helpers.load_catalog_modules(monkeypatch, tmp_path, legacy=True)
    repository = modules.legacy._ITEM_CATALOG_REPOSITORY
    helpers.empty_catalog_files(repository)
    return SimpleNamespace(modules=modules, repository=repository, helpers=helpers, root=tmp_path)


class Finished(Exception):
    pass


def run_reload_handler(items):
    source = ADMIN.read_text(encoding="utf-8")
    function = next(node for node in ast.parse(source).body if isinstance(node, ast.AsyncFunctionDef) and node.name == "items_refresh_")
    function.decorator_list = []
    messages = []

    async def handle_send(bot, event, text):
        messages.append(text)

    async def finish():
        raise Finished

    namespace = {
        "asyncio": asyncio, "items": items, "Bot": object, "GroupMessageEvent": object,
        "PrivateMessageEvent": object, "handle_send": handle_send,
        "items_refresh": SimpleNamespace(finish=finish), "logger": SimpleNamespace(warning=lambda *args: None),
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(ADMIN), "exec"), namespace)
    with pytest.raises(Finished):
        asyncio.run(namespace["items_refresh_"](object(), object()))
    return messages


def test_real_items_reload_handler_updates_shared_compatibility_views_without_file_writes(legacy_catalog):
    legacy = legacy_catalog.modules.legacy
    repository = legacy_catalog.repository
    write_category = legacy_catalog.helpers.write_category
    write_category(repository, "法器", {"500": {"name": "old"}})
    items = legacy.Items()
    cache = legacy.ITEMS_CACHE
    assert items.items is cache
    assert legacy.Items() is items
    assert items.repository is repository
    write_category(repository, "法器", {"501": {"name": "new", "rank": 10}})
    files = {path: path.read_bytes() for path in legacy_catalog.root.rglob("*.json")}

    with patch.object(items, "readf", side_effect=AssertionError("legacy loader used")), patch.object(
        items, "revert_to_original_files", side_effect=AssertionError("source write used"),
    ):
        messages = run_reload_handler(items)

    assert messages == ["重载items完成"]
    assert items.items is cache
    assert list(cache) == ["501"]
    assert items.get_data_by_item_id(501)["name"] == "new"
    assert items.get_data_by_item_name("new")[0] == 501
    assert items.get_data_by_item_type(["法器"])["501"]["name"] == "new"
    assert items.get_random_id_list_by_rank_and_item_type(10, ["法器"]) == ["501"]
    assert {path: path.read_bytes() for path in files} == files


def test_real_items_reload_handler_reports_late_failure_and_retains_both_maps(legacy_catalog):
    repository = legacy_catalog.repository
    write_category = legacy_catalog.helpers.write_category
    write_category(repository, "法器", {"500": {"name": "old"}})
    package = write_category(repository, "礼包", {"900": {"name": "old package"}})
    items = legacy_catalog.modules.legacy.Items()
    before = repository.snapshot()
    source_view = items.package_source_map
    write_category(repository, "法器", {"501": {"name": "new"}})
    package.unlink()
    write_category(repository, "礼包", {"901": {"name": "new package"}}, filename="b.json")
    repository.type_to_path["特殊物品"].write_text("{broken", encoding="utf-8")

    messages = run_reload_handler(items)

    assert len(messages) == 1
    assert "重载items失败" in messages[0]
    assert "重载items完成" not in messages[0]
    assert repository.snapshot() == before
    assert items.package_source_map is source_view
    assert source_view["900"] == package


def test_items_initialization_failure_can_retry_without_poisoned_singleton(legacy_catalog):
    legacy = legacy_catalog.modules.legacy
    with patch.object(legacy_catalog.repository, "ensure_loaded", side_effect=RuntimeError("initialization failed")):
        with pytest.raises(RuntimeError, match="initialization failed"):
            legacy.Items()
    assert not legacy.Items._has_init.get(legacy.items_num)

    items = legacy.Items()

    assert legacy.Items._has_init[legacy.items_num] is True
    assert items.get_data_by_item_id("absent") is None
    assert items.refresh()["status"] == "reloaded"


def test_startup_with_missing_categories_remains_available_but_admin_reload_fails(legacy_catalog):
    repository = legacy_catalog.repository
    repository.type_to_path["功法"].unlink()
    legacy_catalog.helpers.write_category(repository, "法器", {"500": {"name": "available"}})

    items = legacy_catalog.modules.legacy.Items()

    assert items.get_data_by_item_id("500")["name"] == "available"
    assert "重载items失败" in run_reload_handler(items)[0]
    assert items.get_data_by_item_id("500")["name"] == "available"


def test_legacy_item_queries_return_local_editable_values_not_shared_mutable_catalog(legacy_catalog):
    legacy_catalog.helpers.write_category(legacy_catalog.repository, "法器", {
        "500": {"name": "weapon", "desc": "original", "nested": [1], "fusion": {}},
    })
    items = legacy_catalog.modules.legacy.Items()
    info = items.get_data_by_item_id("500")
    info["desc"] = "formatted locally"
    info["nested"].append(2)
    by_name = items.get_data_by_item_name("weapon")[1]
    by_name["nested"].append(3)
    exposed = items.items["500"]
    exposed["nested"].append(4)

    assert items.get_data_by_item_id("500")["desc"] == "original"
    assert items.get_data_by_item_id("500")["nested"] == [1]
    assert items.get_fusion_items() == ["weapon (法器)"]


def test_reload_registration_still_requires_superuser():
    tree = ast.parse(ADMIN.read_text(encoding="utf-8"))
    registration = next(
        node.value for node in tree.body if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "items_refresh" for target in node.targets)
    )
    assert isinstance(registration, ast.Call)
    assert any(keyword.arg == "permission" and ast.unparse(keyword.value) == "SUPERUSER" for keyword in registration.keywords)
