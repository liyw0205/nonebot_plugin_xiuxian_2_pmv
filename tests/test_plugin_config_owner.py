from __future__ import annotations

from types import SimpleNamespace

import tests  # Keep web imports and runtime paths isolated from deployment data.
import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.features.plugin_config.application import (
    PluginConfigApplication,
)
from nonebot_plugin_xiuxian_2.features.plugin_config import schema as config_schema
from nonebot_plugin_xiuxian_2.features.plugin_config import runtime as config_runtime
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import config as config_routes
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.download_xiuxian_data import (
    UpdateManager,
)


CSRF_TOKEN = "plugin-config-routes-csrf"


def test_application_owns_schema_read_projection_and_write_delegation(tmp_path):
    values = SimpleNamespace(
        bot_uin=123456,
        put_bot=["bot-a", "bot-b"],
        web_secret_key="session-secret",
        webdav_pass="webdav-secret",
    )
    writes = []

    def writer(path, new_values, field_types):
        writes.append((path, new_values, field_types))
        return True, "配置保存成功"

    source_file = tmp_path / "xiuxian_config.py"
    application = PluginConfigApplication(
        config_factory=lambda: values,
        config_file=source_file,
        writer=writer,
    )

    assert application.editable_fields is config_schema.CONFIG_EDITABLE_FIELDS
    assert set(application.get_values()) == {
        "bot_uin",
        "put_bot",
        "web_secret_key",
        "webdav_pass",
    }
    assert application.get_values()["webdav_pass"] == "webdav-secret"

    categories = application.config_by_category()
    base_fields = {item["field_name"]: item for item in categories["基础设置"]}
    backup_fields = {item["field_name"]: item for item in categories["云备份设置"]}
    assert base_fields["bot_uin"]["value"] == 123456
    assert base_fields["put_bot"]["value"] == "bot-a, bot-b"
    assert backup_fields["webdav_pass"]["value"] == "webdav-secret"

    assert application.save_values({"bot_uin": "456"}) == (True, "配置保存成功")
    assert application.save_values(
        {"bot_uin": "789"}, include_restart_notice=True
    ) == (True, "配置保存成功，重启机器人后生效。")
    assert writes == [
        (
            source_file,
            {"bot_uin": "456"},
            {name: meta["type"] for name, meta in config_schema.CONFIG_EDITABLE_FIELDS.items()},
        ),
        (
            source_file,
            {"bot_uin": "789"},
            {name: meta["type"] for name, meta in config_schema.CONFIG_EDITABLE_FIELDS.items()},
        ),
    ]


def test_legacy_config_routes_keep_template_auth_csrf_and_json_contract(
    tmp_path, monkeypatch
):
    values = SimpleNamespace(
        bot_uin=123456,
        put_bot=["bot-a", "bot-b"],
        web_secret_key="session-secret",
        webdav_pass="webdav-secret",
    )
    writes = []

    def writer(path, new_values, field_types):
        writes.append((path, new_values, field_types))
        return True, "配置保存成功"

    application = PluginConfigApplication(
        config_factory=lambda: values,
        config_file=tmp_path / "xiuxian_config.py",
        writer=writer,
    )
    monkeypatch.setattr(core, "ADMIN_IDS", {"admin-1"})
    monkeypatch.setitem(core.app.config, "TESTING", True)
    monkeypatch.setitem(core.app.config, "SECRET_KEY", "plugin-config-route-tests")
    monkeypatch.setattr(config_routes, "plugin_config_application", application)
    client = core.app.test_client()

    anonymous_page = client.get("/config")
    assert anonymous_page.status_code == 302
    assert anonymous_page.headers["Location"].endswith("/login")

    with client.session_transaction() as session:
        session["admin_id"] = "not-an-admin"
        session["_csrf_token"] = CSRF_TOKEN
    denied_page = client.get("/config")
    assert denied_page.status_code == 302
    assert denied_page.headers["Location"].endswith("/login")

    with client.session_transaction() as session:
        session["admin_id"] = "admin-1"
        session["_csrf_token"] = CSRF_TOKEN
    page = client.get("/config")
    assert page.status_code == 200
    assert "修仙配置管理" in page.get_data(as_text=True)
    assert "123456" in page.get_data(as_text=True)
    assert "bot-a, bot-b" in page.get_data(as_text=True)
    assert "webdav-secret" in page.get_data(as_text=True)

    missing_csrf = client.post("/save_config", json={"bot_uin": 456})
    assert missing_csrf.status_code == 403
    assert missing_csrf.get_json() == {
        "success": False,
        "error": "CSRF 校验失败，请刷新页面后重试",
    }
    assert writes == []

    empty = client.post(
        "/save_config",
        json={},
        headers={"X-CSRF-Token": CSRF_TOKEN},
    )
    assert empty.status_code == 200
    assert empty.get_json() == {"success": False, "error": "无效的配置数据"}

    saved = client.post(
        "/save_config",
        json={"bot_uin": 456},
        headers={"X-CSRF-Token": CSRF_TOKEN},
    )
    assert saved.status_code == 200
    assert saved.get_json() == {
        "success": True,
        "message": "配置保存成功，重启机器人后生效。",
    }
    assert writes[0][0] == tmp_path / "xiuxian_config.py"
    assert writes[0][1] == {"bot_uin": 456}


def test_update_manager_config_backup_callbacks_share_plugin_config_owner(monkeypatch):
    calls = []

    class ApplicationFake:
        def get_values(self):
            calls.append(("get_values",))
            return {"bot_uin": 123}

        def save_values(self, values):
            calls.append(("save_values", values))
            return True, "saved"

    application = ApplicationFake()
    monkeypatch.setattr(config_runtime, "plugin_config_application", application)
    manager = UpdateManager.__new__(UpdateManager)

    assert manager.configuration_backup_values() == {"bot_uin": 123}
    assert manager.save_config_values({"bot_uin": 456}) == (True, "saved")
    assert manager.configuration_backup_save_values({"bot_uin": 789}) == (
        True,
        "saved",
    )
    assert calls == [
        ("get_values",),
        ("save_values", {"bot_uin": 456}),
        ("save_values", {"bot_uin": 789}),
    ]
