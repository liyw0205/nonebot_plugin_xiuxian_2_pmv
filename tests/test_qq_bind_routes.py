from __future__ import annotations

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.features.qq_bind import QqBindResponse
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import qq_bind_routes as routes


def _admin_client():
    client = core.app.test_client()
    with client.session_transaction() as current_session:
        current_session["admin_id"] = "admin-1"
        current_session["_csrf_token"] = "csrf-token"
    return client


def test_qq_bind_routes_keep_admin_and_csrf_boundaries(monkeypatch):
    calls = []

    class Application:
        async def start(self):
            calls.append("start")
            return QqBindResponse({"success": True, "task_id": "task-1"})

        def qr_png(self, task_id):
            calls.append(("qr", task_id))
            return b"png" if task_id == "task-1" else None

        async def poll(self, task_id):
            calls.append(("poll", task_id))
            return QqBindResponse({"success": True, "status": "waiting"})

        def restart_capability(self):
            calls.append("capability")
            return {"success": True, "automatic": False}

        def restart(self, confirm):
            calls.append(("restart", confirm))
            return QqBindResponse({"success": False}, 400)

    monkeypatch.setattr(routes, "_qq_bind_application", Application())
    monkeypatch.setattr(core, "ADMIN_IDS", {"admin-1"})
    client = _admin_client()

    assert client.get("/api/config/qq-bind/restart-capability").status_code == 200
    assert client.get("/api/config/qq-bind/qr/task-1").mimetype == "image/png"
    assert client.get("/api/config/qq-bind/qr/expired").status_code == 404
    assert client.post("/api/config/qq-bind/start", json={}).status_code == 403
    assert client.post("/api/config/qq-bind/poll", json={}).status_code == 403
    assert client.post("/api/config/qq-bind/restart", json={}).status_code == 403

    headers = {"X-CSRF-Token": "csrf-token"}
    start = client.post("/api/config/qq-bind/start", json={}, headers=headers)
    poll = client.post(
        "/api/config/qq-bind/poll", json={"task_id": "task-1"}, headers=headers
    )
    restart = client.post(
        "/api/config/qq-bind/restart", json={"confirm": True}, headers=headers
    )

    assert start.get_json() == {"success": True, "task_id": "task-1"}
    assert poll.get_json() == {"success": True, "status": "waiting"}
    assert restart.status_code == 400
    assert calls == [
        "capability",
        ("qr", "task-1"),
        ("qr", "expired"),
        "start",
        ("poll", "task-1"),
        ("restart", True),
    ]


def test_qq_bind_route_denies_anonymous_api_access(monkeypatch):
    monkeypatch.setattr(core, "ADMIN_IDS", {"admin-1"})
    client = core.app.test_client()

    assert client.get("/api/config/qq-bind/restart-capability").status_code == 401
    assert client.post(
        "/api/config/qq-bind/start",
        json={},
        headers={"X-CSRF-Token": "csrf-token"},
    ).status_code == 401
