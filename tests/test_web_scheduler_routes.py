from __future__ import annotations

import tests  # Establish isolated data paths before importing plugin modules.
import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import scheduler as scheduler_routes


CSRF_TOKEN = "scheduler-routes-csrf"


class SchedulerApplicationFake:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.jobs = [{"id": "cleanup", "enabled": True, "name": "Cleanup"}]

    def list_jobs(self):
        self.calls.append(("list_jobs", (), {}))
        return self.jobs

    def set_enabled(self, job_id, enabled):
        self.calls.append(("set_enabled", (job_id, enabled), {}))
        if job_id == "missing-job":
            raise ValueError("任务不存在")
        return {"id": job_id, "enabled": enabled}

    def reschedule(self, job_id, trigger):
        self.calls.append(("reschedule", (job_id, trigger), {}))
        if job_id == "bad-schedule":
            raise ValueError("计划格式无效")
        return {"id": job_id, "trigger": trigger}

    def queue_manual_run(self, job_id):
        self.calls.append(("queue_manual_run", (job_id,), {}))
        return {
            "id": job_id,
            "queued": True,
            "run_id": "run-1",
            "status": "queued",
        }

    def get_run(self, run_id):
        self.calls.append(("get_run", (run_id,), {}))
        if run_id != "run-1":
            raise ValueError("手动执行记录不存在")
        return {
            "run_id": run_id,
            "job_id": "cleanup",
            "status": "succeeded",
            "queued_at": "2026-10-08T10:00:00+08:00",
        }


@pytest.fixture
def scheduler_client(monkeypatch):
    application = SchedulerApplicationFake()
    monkeypatch.setattr(core, "ADMIN_IDS", {"admin-1"})
    monkeypatch.setitem(core.app.config, "TESTING", True)
    monkeypatch.setitem(core.app.config, "SECRET_KEY", "scheduler-route-tests")
    monkeypatch.setattr(scheduler_routes, "scheduler_admin_application", application)
    return core.app.test_client(), application


def _login(client, admin_id: str = "admin-1", *, csrf: bool = True) -> None:
    with client.session_transaction() as session:
        session["admin_id"] = admin_id
        if csrf:
            session["_csrf_token"] = CSRF_TOKEN


def _post(client, path: str, payload: dict, *, csrf: bool = True):
    headers = {"X-CSRF-Token": CSRF_TOKEN} if csrf else {}
    return client.post(path, json=payload, headers=headers)


def test_scheduler_page_and_job_list_keep_admin_gate_and_response_contract(scheduler_client):
    client, application = scheduler_client

    anonymous_page = client.get("/scheduler")
    anonymous_list = client.get("/api/scheduler/jobs")
    assert anonymous_page.status_code == 302
    assert anonymous_page.headers["Location"].endswith("/login")
    assert anonymous_list.status_code == 401
    assert anonymous_list.get_json() == {"success": False, "error": "未登录"}

    _login(client, "not-an-admin")
    denied_page = client.get("/scheduler")
    denied_list = client.get("/api/scheduler/jobs")
    assert denied_page.status_code == 302
    assert denied_list.status_code == 401
    assert application.calls == []

    _login(client)
    page = client.get("/scheduler")
    jobs = client.get("/api/scheduler/jobs")
    assert page.status_code == 200
    assert "定时任务" in page.get_data(as_text=True)
    assert jobs.status_code == 200
    assert jobs.get_json() == {"success": True, "jobs": application.jobs}
    assert application.calls == [("list_jobs", (), {})]


def test_enabled_route_validates_bool_and_preserves_success_and_error_envelopes(scheduler_client):
    client, application = scheduler_client

    anonymous = _post(
        client,
        "/api/scheduler/jobs/cleanup/enabled",
        {"enabled": False},
        csrf=False,
    )
    assert anonymous.status_code == 401
    assert anonymous.get_json() == {"success": False, "error": "未登录"}
    assert application.calls == []

    _login(client)

    missing_csrf = _post(client, "/api/scheduler/jobs/cleanup/enabled", {"enabled": False}, csrf=False)
    invalid_bool = _post(client, "/api/scheduler/jobs/cleanup/enabled", {"enabled": "false"})
    assert missing_csrf.status_code == 403
    assert missing_csrf.get_json() == {
        "success": False,
        "error": "CSRF 校验失败，请刷新页面后重试",
    }
    assert invalid_bool.status_code == 400
    assert invalid_bool.get_json() == {"success": False, "error": "enabled 必须是布尔值"}
    assert application.calls == []

    changed = _post(client, "/api/scheduler/jobs/cleanup/enabled", {"enabled": False})
    missing = _post(client, "/api/scheduler/jobs/missing-job/enabled", {"enabled": True})
    assert changed.status_code == 200
    assert changed.get_json() == {"success": True, "job": {"id": "cleanup", "enabled": False}}
    assert missing.status_code == 400
    assert missing.get_json() == {"success": False, "error": "任务不存在"}
    assert application.calls == [
        ("set_enabled", ("cleanup", False), {}),
        ("set_enabled", ("missing-job", True), {}),
    ]


def test_reschedule_route_preserves_success_and_value_error_contract(scheduler_client):
    client, application = scheduler_client
    _login(client)

    trigger = {"type": "cron", "fields": {"minute": "5", "hour": "2"}}
    changed = _post(client, "/api/scheduler/jobs/cleanup/schedule", {"trigger": trigger})
    rejected = _post(client, "/api/scheduler/jobs/bad-schedule/schedule", {"trigger": "invalid"})

    assert changed.status_code == 200
    assert changed.get_json() == {
        "success": True,
        "job": {"id": "cleanup", "trigger": trigger},
    }
    assert rejected.status_code == 400
    assert rejected.get_json() == {"success": False, "error": "计划格式无效"}
    assert application.calls == [
        ("reschedule", ("cleanup", trigger), {}),
        ("reschedule", ("bad-schedule", "invalid"), {}),
    ]


def test_manual_run_returns_queued_dto_and_run_status_has_404_contract(scheduler_client):
    client, application = scheduler_client
    _login(client)

    queued = _post(client, "/api/scheduler/jobs/cleanup/run", {})
    status = client.get("/api/scheduler/runs/run-1")
    missing = client.get("/api/scheduler/runs/unknown-run")

    assert queued.status_code == 200
    assert queued.get_json() == {
        "success": True,
        "id": "cleanup",
        "queued": True,
        "run_id": "run-1",
        "status": "queued",
    }
    assert status.status_code == 200
    assert status.get_json() == {
        "success": True,
        "run": {
            "run_id": "run-1",
            "job_id": "cleanup",
            "status": "succeeded",
            "queued_at": "2026-10-08T10:00:00+08:00",
        },
    }
    assert missing.status_code == 404
    assert missing.get_json() == {"success": False, "error": "手动执行记录不存在"}
    assert application.calls == [
        ("queue_manual_run", ("cleanup",), {}),
        ("get_run", ("run-1",), {}),
        ("get_run", ("unknown-run",), {}),
    ]
