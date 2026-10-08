from __future__ import annotations

import io
from unittest.mock import Mock

import tests  # Keep web-module imports on the isolated test data directory.
import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.features.qq_image_upload.application import (
    QqImageUploadApplication,
)
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core, system


CSRF_TOKEN = "web-upload-image-csrf"


class FakeAdapter:
    def __init__(self, name: str) -> None:
        self.name = name

    def get_name(self) -> str:
        return self.name


class FakeBot:
    def __init__(self, adapter_name: str) -> None:
        self.adapter = FakeAdapter(adapter_name)


@pytest.fixture
def upload_client(monkeypatch):
    monkeypatch.setattr(core, "ADMIN_IDS", {"admin-1"})
    monkeypatch.setitem(core.app.config, "TESTING", True)
    monkeypatch.setitem(core.app.config, "SECRET_KEY", "web-upload-image-test")
    return core.app.test_client()


def _login(client, admin_id: str = "admin-1") -> None:
    with client.session_transaction() as session:
        session["admin_id"] = admin_id
        session["_csrf_token"] = CSRF_TOKEN


def _form_data(image: bytes = b"image-bytes") -> dict:
    return {
        "channel_id": "channel-1",
        "image": (io.BytesIO(image), "image.png"),
    }


def _application(upload):
    return QqImageUploadApplication(upload)


def test_remote_anonymous_upload_is_denied_even_with_forwarded_loopback(upload_client, monkeypatch):
    get_bots = Mock(return_value={"qq": FakeBot("QQ")})
    monkeypatch.setattr(system, "get_bots", get_bots)

    response = upload_client.post(
        "/upload_image",
        data=_form_data(),
        headers={"X-Forwarded-For": "127.0.0.1"},
        environ_base={"REMOTE_ADDR": "203.0.113.5"},
    )

    assert response.status_code == 401
    assert response.get_json() == {"success": False, "error": "未登录"}
    get_bots.assert_not_called()


def test_loopback_upload_keeps_session_and_csrf_bypass(upload_client, monkeypatch):
    calls = []

    async def upload(**kwargs):
        calls.append(kwargs)
        return "https://image.example.test/hash"

    bot = FakeBot("QQ")
    monkeypatch.setattr(system, "get_bots", lambda: {"qq": bot})
    monkeypatch.setattr(system, "qq_image_upload_application", _application(upload))

    response = upload_client.post(
        "/upload_image",
        data=_form_data(),
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    )

    assert response.status_code == 200
    assert response.get_json() == {
        "success": True,
        "url": "https://image.example.test/hash",
    }
    assert calls == [{
        "bot": bot,
        "channel_id": "channel-1",
        "image": b"image-bytes",
        "mode": "md5",
    }]


def test_remote_admin_upload_requires_and_accepts_session_csrf(upload_client, monkeypatch):
    calls = []

    async def upload(**kwargs):
        calls.append(kwargs)
        return "https://image.example.test/admin-upload"

    bot = FakeBot("QQ")
    monkeypatch.setattr(system, "get_bots", lambda: {"qq": bot})
    monkeypatch.setattr(system, "qq_image_upload_application", _application(upload))
    _login(upload_client)

    missing_csrf = upload_client.post(
        "/upload_image",
        data=_form_data(),
        environ_base={"REMOTE_ADDR": "203.0.113.5"},
    )
    assert missing_csrf.status_code == 403
    assert missing_csrf.get_data(as_text=True) == "CSRF 校验失败，请刷新页面后重试"
    assert calls == []

    accepted = upload_client.post(
        "/upload_image",
        data=_form_data(),
        headers={"X-CSRF-Token": CSRF_TOKEN},
        environ_base={"REMOTE_ADDR": "203.0.113.5"},
    )
    assert accepted.status_code == 200
    assert accepted.get_json() == {
        "success": True,
        "url": "https://image.example.test/admin-upload",
    }
    assert len(calls) == 1


def test_missing_multipart_fields_keep_legacy_error(upload_client):
    response = upload_client.post(
        "/upload_image",
        data={"channel_id": "channel-1"},
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    )

    assert response.status_code == 400
    assert response.get_json() == {
        "success": False,
        "error": "缺少参数 image 或 channel_id",
    }


def test_missing_qq_bot_keeps_legacy_error(upload_client, monkeypatch):
    monkeypatch.setattr(system, "get_bots", lambda: {"other": FakeBot("Other")})
    upload = Mock()
    monkeypatch.setattr(system, "qq_image_upload_application", _application(upload))

    response = upload_client.post(
        "/upload_image",
        data=_form_data(),
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    )

    assert response.status_code == 500
    assert response.get_json() == {
        "success": False,
        "error": "未找到在线的 QQBot 实例",
    }
    upload.assert_not_called()


def test_adapter_exception_keeps_logged_error_response(upload_client, monkeypatch):
    async def upload(**kwargs):
        raise RuntimeError("adapter unavailable")

    bot = FakeBot("QQ")
    logger_error = Mock()
    monkeypatch.setattr(system, "get_bots", lambda: {"qq": bot})
    monkeypatch.setattr(system, "qq_image_upload_application", _application(upload))
    monkeypatch.setattr(system.logger, "error", logger_error)

    response = upload_client.post(
        "/upload_image",
        data=_form_data(),
        environ_base={"REMOTE_ADDR": "127.0.0.1"},
    )

    assert response.status_code == 500
    assert response.get_json() == {
        "success": False,
        "error": "adapter unavailable",
    }
    logger_error.assert_called_once_with("接口上传图片异常: adapter unavailable")
