from __future__ import annotations

import asyncio
import base64
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.qq_bind import QqBindApplication, QqBindResponse


class TaskStore:
    def __init__(self):
        self.tasks = {}
        self.results = {}

    def add(self, task_id, key):
        self.tasks[task_id] = (1.0, key)

    def get(self, task_id):
        return self.tasks.get(task_id)

    def pop(self, task_id):
        return self.tasks.pop(task_id, None)

    def complete(self, task_id, result):
        self.tasks.pop(task_id, None)
        self.results[task_id] = {
            key: value
            for key, value in result.items()
            if key in {"success", "status", "appid", "replaced", "message"}
        }

    def completed(self, task_id):
        result = self.results.get(task_id)
        return dict(result) if result else None


def _application(**overrides):
    dependencies = {
        "tasks": TaskStore(),
        "create_task": _async_result({"retcode": 0, "data": {"task_id": "task-1"}}),
        "poll_task": _async_result({"retcode": 0, "data": {"status": 1}}),
        "bind_page_url": lambda task_id: f"https://q.qq.com/connect?task_id={task_id}",
        "qr_png_bytes": lambda content: content.encode(),
        "decrypt_secret": lambda encrypted, key: "decrypted-secret",
        "merge_env": lambda path, appid, secret: True,
        "env_file": lambda: Path("/tmp/.env.dev"),
        "detect_restart": lambda root: {"automatic": True, "mode": "manager", "message": "ready"},
        "schedule_restart": lambda capability: True,
        "project_root": lambda: Path("/app"),
    }
    dependencies.update(overrides)
    return QqBindApplication(**dependencies)


def _async_result(value):
    async def call(*_args):
        return value

    return call


def test_start_creates_private_key_and_keeps_task_for_qr():
    tasks = TaskStore()
    create_calls = []

    async def create(key):
        create_calls.append(key)
        return {"retcode": 0, "data": {"task_id": "task-1"}}

    app = _application(tasks=tasks, create_task=create)
    response = asyncio.run(app.start())

    assert response == QqBindResponse(
        {
            "success": True,
            "task_id": "task-1",
            "status": "waiting",
            "qr_url": "/api/config/qq-bind/qr/task-1",
            "connect_url": "https://q.qq.com/connect?task_id=task-1",
        }
    )
    key = create_calls[0]
    assert len(base64.b64decode(key)) == 32
    assert tasks.get("task-1")[1] == key
    assert app.qr_png("task-1").startswith(b"https://q.qq.com/connect")
    assert app.qr_png("expired") is None


def test_create_failure_does_not_store_task():
    tasks = TaskStore()
    app = _application(
        tasks=tasks,
        create_task=_async_result({"retcode": 1, "msg": "remote unavailable"}),
    )

    response = asyncio.run(app.start())

    assert response == QqBindResponse(
        {"success": False, "error": "remote unavailable"}, 502
    )
    assert tasks.tasks == {}


def test_poll_completion_is_cached_without_secret_and_replayed():
    tasks = TaskStore()
    poll_calls = []
    merged = []

    async def poll(task_id):
        return await _recording_poll(
            poll_calls,
            task_id,
            {
                "retcode": 0,
                "data": {
                    "status": 2,
                    "bot_appid": "10001",
                    "bot_encrypt_secret": "cipher",
                },
            },
        )

    app = _application(
        tasks=tasks,
        poll_task=poll,
        merge_env=lambda path, appid, secret: merged.append((path, appid, secret)) or True,
    )
    tasks.add("task-1", "private-key")

    response = asyncio.run(app.poll("task-1"))
    replay = asyncio.run(app.poll("task-1"))

    assert response.body["status"] == "completed"
    assert replay == response
    assert poll_calls == ["task-1"]
    assert merged == [(Path("/tmp/.env.dev"), "10001", "decrypted-secret")]
    assert "secret" not in repr(tasks.completed("task-1"))


def test_poll_waiting_remote_error_and_remote_expiry_keep_legacy_responses():
    tasks = TaskStore()
    results = {
        "waiting": {"retcode": 0, "data": {"status": 1}},
        "failed": {"retcode": 9, "msg": "upstream unavailable"},
        "expired": {"retcode": 0, "data": {"status": 3}},
    }

    async def poll(task_id):
        return results[task_id]

    app = _application(tasks=tasks, poll_task=poll)
    for task_id in results:
        tasks.add(task_id, f"key-{task_id}")

    assert asyncio.run(app.poll("waiting")).body == {
        "success": True,
        "status": "waiting",
    }
    assert tasks.get("waiting") is not None
    assert asyncio.run(app.poll("failed")).body == {
        "success": False,
        "status": "error",
        "error": "upstream unavailable",
    }
    assert tasks.get("failed") is not None
    assert asyncio.run(app.poll("expired")).body == {
        "success": True,
        "status": "expired",
    }
    assert tasks.get("expired") is None
    assert asyncio.run(app.poll("missing")).body == {
        "success": True,
        "status": "expired",
    }


async def _recording_poll(calls, task_id, result=None):
    calls.append(task_id)
    return result or {"retcode": 0, "data": {"status": 1}}


def test_restart_requires_explicit_confirmation_and_schedules_only_when_supported():
    scheduled = []
    app = _application(schedule_restart=lambda capability: scheduled.append(capability) or True)

    assert app.restart("true").status_code == 400
    assert app.restart_capability() == {
        "success": True,
        "automatic": True,
        "mode": "manager",
        "message": "ready",
    }
    result = app.restart(True)

    assert result.body["scheduled"] is True
    assert len(scheduled) == 1

    unavailable = _application(
        detect_restart=lambda root: {"automatic": False, "mode": "manual", "message": "restart manually"}
    )
    assert unavailable.restart(True).status_code == 409
