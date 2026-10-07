from __future__ import annotations

import tests  # Establish isolated data paths before importing plugin modules.
import nonebot
import pytest
from unittest.mock import Mock

nonebot.init()

from nonebot_plugin_xiuxian_2.features.admin.command_control_application import (
    AdminCommandControlApplication,
)
from nonebot_plugin_xiuxian_2.features.admin.command_control_repository import (
    AdminCommandControlRepository,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import app, core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import command_registry_web as routes


REGISTRY = {
    "灵石": "xiuxian_base",
    "修仙签到": "xiuxian_base",
    "指令禁用": "xiuxian_admin",
}
ALIASES = {"钱包": "灵石", "签到": "修仙签到"}
CSRF_TOKEN = "command-registry-test-token"


@pytest.fixture
def command_registry_client(tmp_path, monkeypatch):
    repository = AdminCommandControlRepository(tmp_path / "command_disable.json")
    repository.sync_command_registry(REGISTRY)
    repository.rebuild_alias_index(ALIASES)

    monkeypatch.setattr(core, "ADMIN_IDS", {"test-admin"})
    monkeypatch.setitem(app.config, "TESTING", True)
    monkeypatch.setitem(app.config, "SECRET_KEY", "command-registry-test-secret")
    application = AdminCommandControlApplication(repository)
    application_factory = Mock(return_value=application)
    application_factory.collect_command_list_rows = Mock(
        wraps=application.collect_command_list_rows
    )
    monkeypatch.setattr(
        application,
        "collect_command_list_rows",
        application_factory.collect_command_list_rows,
    )
    monkeypatch.setattr(routes, "AdminCommandControlApplication", application_factory)

    return app.test_client(), repository, application_factory


def _login(client):
    with client.session_transaction() as session:
        session["admin_id"] = "test-admin"
        session["_csrf_token"] = CSRF_TOKEN


def _post(client, path, payload):
    return client.post(
        path,
        json=payload,
        headers={"X-CSRF-Token": CSRF_TOKEN},
    )


def _assert_api_error(response, message):
    assert response.status_code == 200
    body = response.get_json()
    assert body["success"] is False
    assert message in body["error"]


def test_registry_page_requires_login_and_preserves_query_filter_stats(command_registry_client):
    client, repository, application_factory = command_registry_client
    repository.set_command_disabled("灵石", disabled=True)

    anonymous = client.get("/command_registry?q=xiuxian_base&only_disabled=1")
    assert anonymous.status_code == 302
    assert anonymous.headers["Location"].endswith("/login")
    application_factory.assert_not_called()

    _login(client)
    all_commands = client.get("/command_registry")
    assert all_commands.status_code == 200
    all_commands_body = all_commands.get_data(as_text=True)
    assert "1 个子模块" in all_commands_body
    assert "2 条指令" in all_commands_body
    application_factory.assert_called_once_with()
    application_factory.collect_command_list_rows.assert_called_once_with(
        "", only_disabled=False
    )
    application_factory.reset_mock()

    unfiltered = client.get("/command_registry?q=xiuxian_base")
    assert unfiltered.status_code == 200
    body = unfiltered.get_data(as_text=True)
    assert 'name="q" type="search" value="xiuxian_base"' in body
    assert "1 个子模块" in body
    assert "2 条指令" in body
    assert "已禁用 1" in body
    assert "灵石" in body and "修仙签到" in body

    filtered = client.get("/command_registry?q=xiuxian_base&only_disabled=yes")
    assert filtered.status_code == 200
    body = filtered.get_data(as_text=True)
    assert 'name="q" type="search" value="xiuxian_base"' in body
    assert 'name="only_disabled" value="1" checked' in body
    assert "1 个子模块" in body
    assert "1 条指令" in body
    assert "已禁用 1" in body
    assert "灵石" in body
    assert "修仙签到" not in body
    assert application_factory.call_count == 2
    query_calls = application_factory.collect_command_list_rows.call_args_list
    assert len(query_calls) == 2
    assert query_calls[0].args == ("xiuxian_base",)
    assert query_calls[0].kwargs == {"only_disabled": False}
    assert query_calls[1].args == ("xiuxian_base",)
    assert query_calls[1].kwargs == {"only_disabled": True}


@pytest.mark.parametrize(
    ("path", "payload", "message"),
    (
        ("/api/command_registry/toggle", ["not", "an", "object"], "缺少指令名"),
        ("/api/command_registry/bulk_toggle", "not-an-object", "缺少子模块名"),
    ),
)
def test_post_routes_reject_non_object_json_without_server_error(
    command_registry_client, path, payload, message
):
    client, _, _ = command_registry_client
    _login(client)

    response = _post(client, path, payload)

    _assert_api_error(response, message)


def test_both_post_routes_require_an_admin_session_and_csrf(command_registry_client):
    client, _, application_factory = command_registry_client
    paths = (
        "/api/command_registry/toggle",
        "/api/command_registry/bulk_toggle",
    )

    for path in paths:
        response = client.post(path, json={})
        assert response.status_code == 401
        assert response.get_json() == {"success": False, "error": "未登录"}

    application_factory.assert_not_called()

    _login(client)
    for path in paths:
        response = client.post(path, json={})
        assert response.status_code == 403
        assert response.get_json()["success"] is False
        assert "CSRF" in response.get_json()["error"]

    application_factory.assert_not_called()


def test_toggle_maps_enabled_and_disabled_booleans_to_repository_state(command_registry_client):
    client, repository, _ = command_registry_client
    _login(client)

    response = _post(client, "/api/command_registry/toggle", {"name": "钱包", "disabled": True})
    assert response.status_code == 200
    assert response.get_json() == {"success": True, "name": "钱包", "disabled": True}
    assert repository.is_command_disabled("灵石")

    response = _post(
        client,
        "/api/command_registry/toggle",
        {"name": "灵石", "disabled": False, "enabled": False},
    )
    assert response.get_json()["disabled"] is False
    assert not repository.is_command_disabled("灵石")

    response = _post(client, "/api/command_registry/toggle", {"name": "灵石", "enabled": False})
    assert response.get_json()["disabled"] is True
    assert repository.is_command_disabled("灵石")

    response = _post(client, "/api/command_registry/toggle", {"name": "灵石", "enabled": True})
    assert response.get_json()["disabled"] is False
    assert not repository.is_command_disabled("灵石")

    response = _post(
        client,
        "/api/command_registry/toggle",
        {"name": "灵石", "disabled": "false"},
    )
    assert response.get_json()["disabled"] is True
    assert repository.is_command_disabled("灵石")


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        ({"disabled": True}, "缺少指令名"),
        ({"name": "灵石"}, "请指定 enabled 或 disabled"),
        ({"name": "not-registered", "disabled": True}, "未登记指令"),
        ({"name": "指令禁用", "disabled": True}, "管理员指令不可禁用"),
    ),
)
def test_toggle_returns_existing_validation_and_repository_errors(
    command_registry_client, payload, message
):
    client, repository, _ = command_registry_client
    _login(client)

    response = _post(client, "/api/command_registry/toggle", payload)

    _assert_api_error(response, message)
    assert not repository.is_command_disabled("灵石")
    assert not repository.is_command_disabled("指令禁用")


def test_bulk_toggle_applies_module_and_reports_target_count(command_registry_client):
    client, repository, _ = command_registry_client
    _login(client)

    response = _post(
        client,
        "/api/command_registry/bulk_toggle",
        {"module": "xiuxian_base", "disabled": True},
    )
    assert response.status_code == 200
    assert response.get_json() == {
        "success": True,
        "module": "xiuxian_base",
        "disabled": True,
        "count": 2,
    }
    assert repository.is_command_disabled("灵石")
    assert repository.is_command_disabled("修仙签到")
    assert not repository.is_command_disabled("指令禁用")

    repeated = _post(
        client,
        "/api/command_registry/bulk_toggle",
        {"module": "xiuxian_base", "disabled": True},
    )
    assert repeated.get_json()["success"] is True
    assert repeated.get_json()["count"] == 2
    assert repository.is_command_disabled("灵石")
    assert repository.is_command_disabled("修仙签到")

    response = _post(
        client,
        "/api/command_registry/bulk_toggle",
        {"module": "xiuxian_base", "disabled": False},
    )
    assert response.get_json()["count"] == 2
    assert not repository.is_command_disabled("灵石")
    assert not repository.is_command_disabled("修仙签到")


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        ({"disabled": True}, "缺少子模块名"),
        ({"module": "xiuxian_base"}, "请指定 disabled"),
        ({"module": "xiuxian_base", "enabled": False}, "请指定 disabled"),
        ({"module": "xiuxian_admin", "disabled": True}, "管理员模块不参与指令禁用"),
        ({"module": "xiuxian_missing", "disabled": True}, "下无已登记指令"),
    ),
)
def test_bulk_toggle_rejects_missing_unknown_and_exempt_modules(
    command_registry_client, payload, message
):
    client, repository, _ = command_registry_client
    _login(client)

    response = _post(client, "/api/command_registry/bulk_toggle", payload)

    _assert_api_error(response, message)
    assert not repository.is_command_disabled("灵石")
    assert not repository.is_command_disabled("指令禁用")
