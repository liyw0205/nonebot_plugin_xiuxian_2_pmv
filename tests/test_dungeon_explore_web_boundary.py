from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest
from flask import Flask

from nonebot_plugin_xiuxian_2.adapters.web.blueprints.dungeon import create_blueprint
from nonebot_plugin_xiuxian_2.features.dungeon.application import DungeonApplication
from nonebot_plugin_xiuxian_2.features.dungeon.manifest import FEATURE
from nonebot_plugin_xiuxian_2.features.dungeon.repository import DungeonSessionSqlRepository
from tests.test_db_backend import db_backend


ROUTES = (
    ("/api/v1/dungeon/purchase", "purchase"),
    ("/api/v1/dungeon/explore/replay", "replay"),
    ("/api/v1/dungeon/explore/intent", "prepare_intent"),
    ("/api/v1/dungeon/explore/prepare", "prepare_resolution"),
    ("/api/v1/dungeon/explore/settle", "settle"),
)


def web_app(application, permission):
    app = Flask(__name__)
    app.config.update(TESTING=True, SECRET_KEY="dungeon-web-boundary-test")
    app.register_blueprint(create_blueprint(application=application, permission=permission))
    return app


def set_csrf(client, token="session-a"):
    with client.session_transaction() as session:
        session["_csrf_token"] = token


@pytest.fixture
def boundary():
    application = Mock(spec=DungeonApplication)
    permission = Mock(return_value=True)
    application.purchase.return_value = SimpleNamespace(
        ok=True, to_dict=lambda: {"status": "applied"}
    )
    for method in ("replay", "prepare_intent", "prepare_resolution", "settle"):
        getattr(application, method).return_value = {"status": "applied"}
    app = web_app(application, permission)
    client = app.test_client()
    set_csrf(client)
    return app, client, application, permission


def test_manifest_declares_all_five_guarded_post_routes(boundary):
    app, _, _, _ = boundary
    expected = {(path, "POST") for path, _ in ROUTES}
    declared = {
        (route.path, method): route.permission
        for route in FEATURE.routes
        for method in route.methods
    }
    actual = {
        (rule.rule, method)
        for rule in app.url_map.iter_rules()
        if rule.rule.startswith("/api/v1/dungeon/")
        for method in rule.methods - {"HEAD", "OPTIONS"}
    }
    assert actual == expected
    assert declared == {key: "user" for key in expected}


@pytest.mark.parametrize("path,method", ROUTES)
def test_denied_permission_precedes_csrf_and_never_calls_application(boundary, path, method):
    _, client, application, permission = boundary
    permission.return_value = False
    response = client.post(path, json={"user_id": "u", "operation_id": "op"})
    assert response.status_code == 403
    assert response.get_json()["error"]["code"] == "forbidden"
    assert application.mock_calls == []
    assert permission.mock_calls == [call("user")]


@pytest.mark.parametrize("path,method", ROUTES)
@pytest.mark.parametrize("csrf_case", ("missing", "wrong", "other_session"))
def test_invalid_csrf_never_calls_application(boundary, path, method, csrf_case):
    app, client, application, permission = boundary
    headers = {}
    if csrf_case == "wrong":
        headers["X-CSRF-Token"] = "invalid"
    elif csrf_case == "other_session":
        other_client = app.test_client()
        set_csrf(other_client, "session-b")
        headers["X-CSRF-Token"] = "session-a"
        client = other_client
    response = client.post(path, headers=headers, json={"user_id": "u", "operation_id": "op"})
    assert response.status_code == 403
    assert response.get_json()["error"]["code"] == "csrf_failed"
    assert application.mock_calls == []
    assert permission.mock_calls == [call("user")]


@pytest.mark.parametrize("path,method", ROUTES)
@pytest.mark.parametrize("header_key", (None, "header-operation"))
def test_idempotency_header_takes_priority_and_body_remains_fallback(boundary, path, method, header_key):
    _, client, application, _ = boundary
    payload = {"operation_id": "body-operation", "user_id": "u"}
    if method == "purchase":
        payload.update(
            item_id=9, item_name="reward", item_type="item", quantity=1,
            unit_cost=10, expected_stone=100, max_goods=99,
        )
    elif method == "prepare_intent":
        payload["intent"] = {"seed_version": "test-version"}
    elif method == "prepare_resolution":
        payload["plan"] = {"response": {"message": "ready"}}
    elif method == "settle":
        payload["max_goods_num"] = 99
    headers = {"X-CSRF-Token": "session-a", "X-Request-ID": "dungeon-web-test"}
    if header_key is not None:
        headers["Idempotency-Key"] = header_key
    response = client.post(path, headers=headers, json=payload)
    assert response.status_code == 200
    assert response.get_json()["request_id"] == "dungeon-web-test"
    expected = {**payload, "operation_id": header_key or payload["operation_id"]}
    assert application.mock_calls == [getattr(call, method)(**expected)]


@pytest.fixture
def persisted_explore(tmp_path):
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with db_backend.transaction(game) as connection:
        connection.execute(
            "CREATE TABLE dungeon_explore_operations ("
            "operation_id TEXT PRIMARY KEY,request_identity TEXT,phase TEXT,"
            "prepared_json TEXT,intent_json TEXT,result_status TEXT,result_json TEXT,"
            "current_layer INTEGER,dungeon_status TEXT,updated_at TEXT)"
        )
        connection.execute(
            "INSERT INTO dungeon_explore_operations VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                "saved-operation",
                json.dumps({"action": "explore", "user_id": "owner"}, sort_keys=True),
                "completed",
                json.dumps({"private_plan": "owner-plan"}),
                json.dumps({"private_intent": "owner-intent"}),
                "won",
                json.dumps({"message": "owner-result"}),
                2,
                "completed",
                "",
            ),
        )
    repository = DungeonSessionSqlRepository(game, player)
    application = DungeonApplication(game, player, repository=repository)
    client = web_app(application, lambda _: True).test_client()
    set_csrf(client)
    return client, game, player


def test_same_user_replay_returns_persisted_response_without_player_database(persisted_explore):
    client, game, player = persisted_explore
    before = game.read_bytes()
    response = client.post(
        "/api/v1/dungeon/explore/replay",
        headers={"X-CSRF-Token": "session-a", "Idempotency-Key": "saved-operation"},
        json={"user_id": "owner", "operation_id": "ignored-body-key"},
    )
    assert response.status_code == 200
    data = response.get_json()["data"]
    assert (data["status"], data["phase"], data["response"]) == (
        "duplicate", "completed", {"message": "owner-result"}
    )
    assert game.read_bytes() == before
    assert not player.exists()


@pytest.mark.parametrize("path,method", ROUTES[1:])
def test_other_user_conflict_does_not_disclose_saved_explore_data(persisted_explore, path, method):
    client, game, player = persisted_explore
    before = game.read_bytes()
    response = client.post(
        path,
        headers={"X-CSRF-Token": "session-a", "Idempotency-Key": "saved-operation"},
        json={
            "user_id": "other", "intent": {"seed_version": "other"},
            "plan": {"response": {"message": "other"}}, "max_goods_num": 99,
        },
    )
    assert response.status_code == 409
    data = response.get_json()["data"]
    assert data["status"] == "operation_conflict"
    assert data["response"] == {}
    assert data["plan"] == {}
    assert not data.get("intent")
    for private_value in ("owner", "owner-plan", "owner-intent", "owner-result", "won"):
        assert private_value not in response.get_data(as_text=True)
    assert game.read_bytes() == before
    assert not player.exists()
