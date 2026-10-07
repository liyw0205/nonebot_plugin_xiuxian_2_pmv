from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import patch

import nonebot
from flask import session

nonebot.init()

core = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web.core")
routes = importlib.import_module("nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web.commands")


def _admin_client():
    client = core.app.test_client()
    with client.session_transaction() as session:
        session["admin_id"] = "admin-1"
        session["_csrf_token"] = "csrf-test-token"
    return client


def test_commands_page_and_execute_route_keep_admin_and_csrf_boundaries():
    with patch.object(core, "ADMIN_IDS", {"admin-1"}):
        client = core.app.test_client()
        redirect = client.get("/commands")
        assert redirect.status_code == 302
        assert redirect.headers["Location"].endswith("/login")
        assert client.post("/execute_command", json={}).status_code == 401

        client = _admin_client()
        page = client.get("/commands")
        assert page.status_code == 200
        assert b"commands.html" not in page.data
        assert b"gm_command" in page.data

        missing_csrf = client.post(
            "/execute_command", json={"command_name": "unknown"}
        )
        assert missing_csrf.status_code == 403
        allowed = client.post(
            "/execute_command",
            headers={"X-CSRF-Token": "csrf-test-token"},
            json={"command_name": "unknown"},
        )
        assert allowed.status_code == 200
        assert allowed.get_json() == {"success": False, "error": "未知命令: unknown"}


def test_execute_command_routes_world_exp_through_admin_asset_owner():
    calls = []

    class Application:
        def adjust_exp_all(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(status="adjusted", data={"affected_users": 3}, ok=True)

    with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
        routes, "admin_asset_application", Application()
    ):
        response = _admin_client().post(
            "/execute_command",
            headers={"X-CSRF-Token": "csrf-test-token"},
            json={"command_name": "adjust_exp_command", "target": "全服", "amount": "25"},
        )

    assert response.status_code == 200
    assert response.get_json() == {
        "success": True,
        "message": "全服增加 25 修为成功",
    }
    assert len(calls) == 1
    assert calls[0]["operator_id"] == "admin-1"
    assert calls[0]["requested_delta"] == 25


def test_execute_command_keeps_zero_and_empty_world_exp_as_legacy_success():
    class Application:
        def adjust_exp_all(self, **kwargs):
            return SimpleNamespace(status="no_targets", data={"affected_users": 0}, ok=False)

    with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
        routes, "admin_asset_application", Application()
    ):
        client = _admin_client()
        zero = client.post(
            "/execute_command",
            headers={"X-CSRF-Token": "csrf-test-token"},
            json={"command_name": "adjust_exp_command", "target": "全服", "amount": "0"},
        )
        empty = client.post(
            "/execute_command",
            headers={"X-CSRF-Token": "csrf-test-token"},
            json={"command_name": "adjust_exp_command", "target": "全服", "amount": "5"},
        )

    assert zero.get_json() == {
        "success": True,
        "message": "全服增加 0 修为成功",
    }
    assert empty.get_json() == {
        "success": True,
        "message": "全服增加 5 修为成功",
    }


def test_execute_command_reuses_request_id_for_retries():
    operation_ids = []

    class Application:
        def adjust_stone(self, **kwargs):
            operation_ids.append(kwargs["operation_id"])
            return SimpleNamespace(
                status="adjusted",
                data={"status": "adjusted", "applied_delta": 3, "final_stone": 3},
            )

    payload = {
        "command_name": "gm_command",
        "target": "指定用户",
        "username": "道号",
        "amount": "3",
        "request_id": "retry-123",
    }
    user = {"user_id": "u1", "stone": 0}
    with patch.object(routes, "admin_asset_application", Application()), patch.object(
        routes, "get_user_by_name", lambda _name: user
    ):
        responses = []
        for _ in range(2):
            with core.app.test_request_context(
                "/execute_command", method="POST", json=payload
            ):
                session["admin_id"] = "admin-1"
                responses.append(routes.execute_command().get_json())

    assert all(response["success"] for response in responses)
    assert operation_ids[0] == operation_ids[1]


def test_execute_command_reports_actual_clamped_single_user_delta():
    class Application:
        def adjust_stone(self, **_kwargs):
            return SimpleNamespace(
                status="adjusted",
                data={"status": "adjusted", "applied_delta": -2, "final_stone": 0},
            )

    with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
        routes, "admin_asset_application", Application()
    ), patch.object(
        routes, "get_user_by_name", lambda _name: {"user_id": "u1", "stone": 2}
    ):
        response = _admin_client().post(
            "/execute_command",
            headers={"X-CSRF-Token": "csrf-test-token"},
            json={
                "command_name": "gm_command",
                "target": "指定用户",
                "username": "道号",
                "amount": "-8",
            },
        )

    assert response.get_json() == {
        "success": True,
        "message": "成功向 道号 减少 2 灵石",
    }


def test_zero_world_impart_adjustment_does_not_scan_users_or_create_rows():
    with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
        routes, "_user_ids", side_effect=AssertionError("zero adjustment must not scan users")
    ):
        response = _admin_client().post(
            "/execute_command",
            headers={"X-CSRF-Token": "csrf-test-token"},
            json={
                "command_name": "ccll_command",
                "target": "全服",
                "amount": "0",
            },
        )

    assert response.get_json() == {
        "success": True,
        "message": "全服发放 0 思恋结晶成功，影响 0 名用户",
    }


def test_execute_command_resumes_accessory_batch_with_empty_current_roster():
    calls = []
    batch = SimpleNamespace(
        status="applied", total=2, completed=2, affected_users=1,
        affected_quantity=2, removed=2,
    )

    class Application:
        def find_running_accessory_batch(self, **kwargs):
            calls.append(("find_running_accessory_batch", kwargs))
            return "active-batch"

        def grant_accessory_batch(self, *args, **kwargs):
            calls.append(("grant_accessory_batch", args, kwargs))
            return batch

    item_catalog = SimpleNamespace(
        get_data_by_item_name=lambda _name: (
            2, {"name": "饰品", "type": "饰品", "item_type": "饰品"}
        )
    )
    with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
        routes, "admin_asset_application", Application()
    ), patch.object(routes, "items", item_catalog), patch.object(
        routes, "_user_ids", lambda: []
    ), patch.object(routes, "_accessory_tools", lambda: (1000, lambda *_args: {})):
        with core.app.test_request_context(
            "/execute_command", method="POST", json={
                "command_name": "cz", "target": "全服", "item": "饰品", "amount": "2"
            }
        ):
            session["admin_id"] = "admin-1"
            response = routes.execute_command().get_json()

    assert response == {
        "success": True,
        "message": "全服发放【饰品】饰品 x2（1阶）成功，影响 1 名用户",
    }
    assert [call[0] for call in calls] == [
        "find_running_accessory_batch", "grant_accessory_batch"
    ]
    assert calls[1][1][2] == []


def test_execute_command_dispatches_each_asset_family_to_admin_application():
    calls = []
    batch = SimpleNamespace(
        status="applied", total=1, completed=1, affected_users=1,
        affected_quantity=2, removed=2,
    )
    outcome = SimpleNamespace(
        status="applied", data={"status": "applied", "affected_quantity": 2}, ok=True
    )

    class Application:
        def __getattr__(self, name):
            def call(*args, **kwargs):
                calls.append((name, args, kwargs))
                if name.startswith("find_running_"):
                    return None
                if name == "snapshot_impart_stone":
                    return SimpleNamespace(status="ok", stone=5)
                if name in {"adjust_stone", "adjust_exp", "adjust_exp_all"}:
                    return SimpleNamespace(
                        status="adjusted", data={"status": "adjusted"}, ok=True
                    )
                if name == "destroy_item":
                    return SimpleNamespace(
                        status="destroyed",
                        data={"status": "destroyed", "removed_quantity": 2},
                        ok=True,
                    )
                if name in {"adjust_stone_batch", "adjust_item_batch", "grant_accessory_batch", "destroy_accessory_batch", "adjust_impart_stone_batch"}:
                    return batch
                return outcome

            return call

    user = {
        "user_id": "u1", "user_name": "道号", "stone": 20, "exp": 50,
        "level": "旧境界", "hp": 10, "mp": 20, "atk": 5, "power": 30,
        "root": "旧灵根", "root_type": "混沌灵根", "root_level": 0,
    }
    item_catalog = SimpleNamespace(
        get_data_by_item_name=lambda name: (
            (1, {"name": "普通物品", "type": "丹药"})
            if name == "普通物品"
            else (2, {"name": "饰品", "type": "饰品", "item_type": "饰品"})
        )
    )
    data = SimpleNamespace(
        level_data=lambda: {
            "旧境界": {"power": 50, "spend": 1.0},
            "新境界": {"power": 100, "spend": 2.0},
        }
    )

    def execute(payload):
        with core.app.test_request_context(
            "/execute_command", method="POST", json=payload
        ):
            session["admin_id"] = "admin-1"
            return routes.execute_command().get_json()

    with patch.object(routes, "admin_asset_application", Application()), patch.object(
        routes, "get_user_by_name", lambda _name: user
    ), patch.object(routes, "items", item_catalog), patch.object(
        routes, "_item_quantity", lambda *_args: 4
    ), patch.object(routes, "_user_ids", lambda: ["u1"]), patch.object(
        routes, "_accessory_tools", lambda: (1000, lambda *_args: {"uid": "acc"})
    ), patch.object(routes, "jsondata", data), patch.object(
        routes, "convert_rank", lambda _rank: (None, {"新境界"})
    ), patch.object(routes, "get_root_rate", lambda *_args: 1.5), patch.object(
        routes, "WEB_CONFIG", SimpleNamespace(max_goods_num=99)
    ):
        requests = [
            {"command_name": "gm_command", "target": "指定用户", "username": "道号", "amount": "3"},
            {"command_name": "gm_command", "target": "全服", "amount": "3"},
            {"command_name": "adjust_exp_command", "target": "指定用户", "username": "道号", "amount": "3"},
            {"command_name": "adjust_exp_command", "target": "全服", "amount": "3"},
            {"command_name": "gmm_command", "username": "道号", "root_type": "1"},
            {"command_name": "zaohua_xiuxian", "username": "道号", "level": "新境界"},
            {"command_name": "cz", "target": "指定用户", "username": "道号", "item": "普通物品", "amount": "2"},
            {"command_name": "cz", "target": "全服", "item": "普通物品", "amount": "2"},
            {"command_name": "cz", "target": "指定用户", "username": "道号", "item": "饰品", "amount": "2", "quality": "3"},
            {"command_name": "cz", "target": "全服", "item": "饰品", "amount": "2", "quality": "3"},
            {"command_name": "hmll", "target": "指定用户", "username": "道号", "item": "普通物品", "amount": "2"},
            {"command_name": "hmll", "target": "全服", "item": "普通物品", "amount": "2"},
            {"command_name": "hmll", "target": "指定用户", "username": "道号", "item": "饰品", "amount": "2"},
            {"command_name": "hmll", "target": "全服", "item": "饰品", "amount": "2"},
            {"command_name": "ccll_command", "target": "指定用户", "username": "道号", "amount": "3"},
            {"command_name": "ccll_command", "target": "全服", "amount": "3"},
        ]
        results = [execute(payload) for payload in requests]

    assert all(result["success"] for result in results)
    assert results[10]["message"] == "成功从 道号 扣除 普通物品 x2"
    assert results[12]["message"] == "成功从 道号 扣除【饰品】饰品 x2"
    called = {name for name, _args, _kwargs in calls}
    expected_calls = {
        "adjust_stone", "adjust_stone_batch", "adjust_exp", "adjust_exp_all",
        "change_root", "change_level", "grant_item", "adjust_item_batch",
        "adjust_accessory", "grant_accessory_batch", "destroy_item",
        "destroy_accessory_batch", "snapshot_impart_stone", "adjust_impart_stone",
        "adjust_impart_stone_batch",
    }
    assert expected_calls.issubset(called), expected_calls - called
    accessory_actions = [kwargs["action"] for name, _args, kwargs in calls if name == "adjust_accessory"]
    assert accessory_actions == ["grant", "destroy"]
    assert sum(name == "destroy_item" for name, _args, _kwargs in calls) == 1
    assert all(kwargs.get("operator_id") == "admin-1" for _, _args, kwargs in calls if "operator_id" in kwargs)
