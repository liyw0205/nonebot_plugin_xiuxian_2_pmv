from __future__ import annotations

import ast
import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from nonebot_plugin_xiuxian_2.features.admin.config_application import AdminConfigApplication
from nonebot_plugin_xiuxian_2.features.admin.config_repository import AdminConfigRepository
from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_config


ADMIN = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_admin"


@pytest.fixture
def config(tmp_path, monkeypatch):
    monkeypatch.setattr(xiuxian_config, "DATABASE", tmp_path)
    repository = AdminConfigRepository(tmp_path / "config.json")
    return AdminConfigApplication(repository), repository


def test_defaults_are_read_only_and_legacy_facade_shares_owner(config):
    application, repository = config
    assert repository.read_data()["private"] is True
    assert not repository.config_jsonpath.exists()
    legacy = xiuxian_config.JsonConfig()
    assert application.set_switch("private", False)
    assert not legacy.is_private_enabled()
    assert not application.set_switch("private", False)
    legacy.write_data(3)
    assert repository.read_data()["private"] is True
    for key, field in ((6, "root_selection"), (8, "sect_name")):
        legacy.write_data(key)
        assert repository.read_data()[field] is False
        assert application.set_switch(field, True)
    assert legacy.is_auto_root_selection_enabled()
    assert legacy.is_auto_sect_name_enabled()


def test_group_and_welcome_defaults_duplicates_and_global_gate(config):
    application, repository = config
    assert not application.set_group_enabled("g1", True)
    assert application.set_group_enabled("g1", False)
    assert not application.set_group_enabled("g1", False)
    assert application.set_group_enabled("g1", True)
    assert application.set_group_welcome("g1", enabled=False)[0]
    assert not application.set_group_welcome("g1", enabled=False)[0]
    assert not application.set_group_welcome("g1", enabled=True, globally_enabled=False)[0]
    assert repository.read_data()["welcome_disabled_groups"] == ["g1"]
    assert application.set_group_welcome("g1", enabled=True)[0]
    assert not application.set_group_welcome("", enabled=True)[0]
    with pytest.raises(ValueError):
        application.set_group_enabled("", False)
    with pytest.raises(KeyError):
        application.set_switch("private_enabled", True)


def test_message_group_config_methods_preserve_legacy_behavior(config):
    application, repository = config

    assert not application.is_full_message_group("")
    assert not application.set_full_message_group("", enabled=True)
    assert application.set_full_message_group(" g1 ", enabled=True)
    assert application.is_full_message_group("g1")
    assert repository.read_data()["full_message_groups"] == ["g1"]
    assert not application.set_full_message_group("g1", enabled=True)
    assert application.set_full_message_group("g1", enabled=False)
    assert not application.is_full_message_group("g1")

    assert application.set_group_remark(" g1 ", "  " + "仙" * 40 + "  ") == (True, f"已备注：{'仙' * 32}")
    assert application.get_group_remark("g1") == "仙" * 32
    assert application.get_group_remarks() == {"g1": "仙" * 32}
    assert application.set_group_remark("g1", " ") == (True, "已清除备注")
    assert application.get_group_remarks() == {}
    assert application.set_group_remark("", "备注") == (False, "缺少群ID")

    assert application.set_session_pinned("group", "g1", True) == (True, "已置顶")
    assert application.set_session_pinned("private", "u1", True) == (True, "已置顶")
    assert application.get_pinned_sessions() == ["private:u1", "group:g1"]
    assert application.is_session_pinned("group", "g1")
    assert application.set_session_pinned("group", "g1", True) == (False, "已置顶")
    assert application.set_session_pinned("group", "g1", False) == (True, "已取消置顶")
    assert not application.is_session_pinned("group", "g1")
    assert application.set_session_pinned("", "g1", True) == (False, "缺少会话标识")
    assert application.set_session_pinned("group", "", True) == (False, "缺少会话标识")


def test_message_database_config_application_uses_existing_owner(tmp_path, monkeypatch):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import message_db

    path = tmp_path / "message_db_config.json"
    monkeypatch.setattr(message_db, "_message_db_config_path", lambda: path)
    monkeypatch.setattr(message_db, "message_db_max_size_mb", 1000)
    monkeypatch.setattr(message_db, "message_group_keep_days", 0)
    monkeypatch.setattr(message_db, "message_private_keep_days", 0)
    application = AdminConfigApplication(AdminConfigRepository(tmp_path / "config.json"))

    assert application.get_message_db_config() == {
        "message_db_max_size_mb": 1000,
        "message_group_keep_days": 0,
        "message_private_keep_days": 0,
    }
    saved = application.update_message_db_config({
        "message_db_max_size_mb": 0,
        "message_group_keep_days": 14,
        "message_private_keep_days": 30,
    })
    assert saved == {
        "message_db_max_size_mb": 0,
        "message_group_keep_days": 14,
        "message_private_keep_days": 30,
    }
    assert not application.is_message_record_enabled()
    assert json.loads(path.read_text(encoding="utf-8")) == saved


def test_legacy_metadata_and_unknown_keys_survive_switch_updates(config):
    application, repository = config
    legacy = xiuxian_config.JsonConfig()
    repository.update(lambda data: data.update(custom={"values": [1]}))
    assert legacy.set_group_remark("g", " remark ")[0]
    assert legacy.set_session_pinned("group", "g", True)[0]
    assert legacy.mark_full_message_group("g")
    assert not legacy.mark_full_message_group("g")
    assert application.set_switch("private", False)
    snapshot = repository.read_data()
    snapshot["custom"]["values"].append(2)
    assert repository.read_data()["custom"] == {"values": [1]}
    assert legacy.get_group_remark("g") == "remark"
    assert legacy.get_pinned_sessions() == ["group:g"]
    assert legacy.is_full_message_group("g")
    assert legacy.unmark_full_message_group("g")
    assert not legacy.unmark_full_message_group("g")
    assert legacy.set_session_pinned("group", "g", False)[0]
    assert legacy.set_group_remark("g", "")[0]
    assert legacy.get_group_remarks() == {}


def test_cache_is_path_aware_and_observes_replacement(config, tmp_path):
    application, repository = config
    application.set_switch("private", False)
    original = repository.config_jsonpath.stat()
    other = AdminConfigRepository(tmp_path / "other.json")
    other.create_default_config()
    os.utime(other.config_jsonpath, ns=(original.st_atime_ns, original.st_mtime_ns))
    assert other.read_data()["private"] is True
    assert repository.read_data()["private"] is False
    os.replace(other.config_jsonpath, repository.config_jsonpath)
    assert repository.read_data()["private"] is True


def test_failed_replace_and_corrupt_input_do_not_publish_or_destroy_config(config):
    application, repository = config
    repository.create_default_config()
    original = repository.config_jsonpath.read_bytes()
    with patch("nonebot_plugin_xiuxian_2.infrastructure.filesystem.atomic.os.replace", side_effect=OSError("disk")):
        with pytest.raises(OSError):
            application.set_switch("private", False)
    assert repository.config_jsonpath.read_bytes() == original
    assert repository.read_data()["private"] is True
    assert list(repository.config_jsonpath.parent.glob(".config.json.*.tmp")) == []
    for bad in (b"{broken", b"[]"):
        repository.config_jsonpath.write_bytes(bad)
        with pytest.raises(ValueError):
            application.set_switch("private", False)
        assert repository.config_jsonpath.read_bytes() == bad


def test_repeated_switch_does_not_write_and_parallel_instances_do_not_lose_groups(config):
    application, repository = config
    application.set_switch("private", False)
    with patch.object(repository, "_persist", side_effect=AssertionError("duplicate write")):
        assert not application.set_switch("private", False)

    def disable(index):
        other = AdminConfigApplication(AdminConfigRepository(repository.config_jsonpath))
        return other.set_group_enabled(str(index), False)

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert all(pool.map(disable, range(1, 21)))
    data = json.loads(repository.config_jsonpath.read_text(encoding="utf-8"))
    assert set(data["group"]) == {str(index) for index in range(1, 21)}
    assert data["private"] is False


class Finished(Exception):
    pass


class PrivateEvent:
    def __init__(self, message):
        self.message = message


def run_handler(application, handler_name, message, *, welcome=False, private=False, allowed=True):
    source = ADMIN / ("group_welcome.py" if welcome else "__init__.py")
    tree = ast.parse(source.read_text(encoding="utf-8"))
    handler = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == handler_name)
    handler.decorator_list = []
    output = []

    async def assign_bot(**kwargs):
        return kwargs["bot"], "g"

    async def handle_send(bot, event, text, **kwargs):
        output.append(text)

    async def finish():
        raise Finished

    namespace = {
        "Bot": object, "GroupMessageEvent": SimpleNamespace, "PrivateMessageEvent": PrivateEvent,
        "assign_bot": assign_bot, "handle_send": handle_send,
        "admin_config_application": application, "XiuConfig": xiuxian_config.XiuConfig,
        "_can_toggle_welcome": lambda bot, event: allowed,
    }
    for name in ("set_xiuxian", "set_private_chat", "set_auto_root", "set_auto_sect_name",
                 "welcome_enable_cmd", "welcome_disable_cmd"):
        namespace[name] = SimpleNamespace(finish=finish)
    exec(compile(ast.Module(body=[handler], type_ignores=[]), str(source), "exec"), namespace)
    event = PrivateEvent(message) if private else SimpleNamespace(message=message, group_id="g")
    with pytest.raises(Finished):
        asyncio.run(namespace[handler_name](object(), event))
    return output


@pytest.mark.parametrize("handler,disable,enable,key", [
    ("set_private_chat_", "禁用私聊功能", "启用私聊功能", "private"),
    ("set_auto_root_", "关闭自动灵根", "开启自动灵根", "root_selection"),
    ("set_auto_sect_name_", "禁用自动宗名", "启用自动宗名", "sect_name"),
])
def test_real_switch_handlers_disable_reenable_and_report_duplicates(config, handler, disable, enable, key):
    application, repository = config
    assert "重复操作" not in run_handler(application, handler, disable)[0]
    assert repository.read_data()[key] is False
    assert "重复操作" in run_handler(application, handler, disable)[0]
    assert "重复操作" not in run_handler(application, handler, enable)[0]
    assert repository.read_data()[key] is True
    assert "重复操作" in run_handler(application, handler, enable)[0]
    assert "指令错误" in run_handler(application, handler, "invalid")[0]


def test_real_group_and_welcome_handlers_preserve_permission_guards(config):
    application, repository = config
    assert "请在群内" in run_handler(application, "open_xiuxian_", "禁用修仙功能", private=True)[0]
    run_handler(application, "open_xiuxian_", "禁用修仙功能")
    assert repository.read_data()["group"] == ["g"]
    assert "重复操作" in run_handler(application, "open_xiuxian_", "禁用修仙功能")[0]
    run_handler(application, "open_xiuxian_", "启用修仙功能")
    assert repository.read_data()["group"] == []
    assert "仅群主" in run_handler(application, "welcome_disable_", "", welcome=True, allowed=False)[0]
    assert repository.read_data()["welcome_disabled_groups"] == []
    run_handler(application, "welcome_disable_", "", welcome=True)
    assert repository.read_data()["welcome_disabled_groups"] == ["g"]
    assert "请在群内" in run_handler(application, "welcome_enable_", "", welcome=True, private=True)[0]
    run_handler(application, "welcome_enable_", "", welcome=True)
    assert repository.read_data()["welcome_disabled_groups"] == []


def test_real_handler_does_not_report_success_after_write_failure(config):
    application, repository = config
    with patch.object(repository, "_persist", side_effect=OSError("disk")):
        with pytest.raises(OSError):
            run_handler(application, "set_private_chat_", "禁用私聊功能")
