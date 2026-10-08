from __future__ import annotations

import re
from pathlib import Path

import tests  # Keep web-module imports on the isolated test data directory.
import nonebot
import pytest

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import messages as message_routes
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import stickers as sticker_routes


CSRF_TOKEN = "sticker-routes-csrf"
_PACK_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
_STICKER_FILE_RE = re.compile(r"^[A-Za-z0-9._-]+\.webp$")


class StickerApplicationFake:
    def __init__(self, packs_root: Path) -> None:
        self.packs_root = packs_root
        self.calls: list[tuple[str, dict]] = []
        self.catalog_result = {
            "success": True,
            "installed": False,
            "version": 0,
            "remote_version": 9,
            "updated_at": "2026-10-08",
            "packs": [{"id": "pack-a", "name": "Pack A", "count": 2, "installed": False}],
        }
        self.install_result = {
            "success": True,
            "job_id": "job-1",
            "status": "running",
            "stage": "queued",
            "percent": 0,
            "message": "准备下载表情包",
            "pack_id": "pack-a",
        }
        self.status_result = {
            "success": True,
            "job_id": "job-1",
            "status": "complete",
            "stage": "complete",
            "percent": 100,
            "message": "Pack A 下载完成",
            "pack_id": "pack-a",
        }

    def catalog(self, force_refresh: bool = False) -> dict:
        self.calls.append(("catalog", {"force_refresh": force_refresh}))
        return self.catalog_result

    def start_install(self, *, pack_id: str, force: bool = False) -> dict:
        self.calls.append(("start_install", {"pack_id": pack_id, "force": force}))
        return self.install_result

    def install_status(self, job_id: str) -> dict | None:
        self.calls.append(("install_status", {"job_id": job_id}))
        return self.status_result if job_id == "job-1" else None

    def resolve_file(self, pack_id: str, filename: str) -> Path | None:
        self.calls.append(("resolve_file", {"pack_id": pack_id, "filename": filename}))
        if not _PACK_ID_RE.fullmatch(str(pack_id)) or not _STICKER_FILE_RE.fullmatch(str(filename)):
            return None

        root = self.packs_root.resolve()
        path = self.packs_root / pack_id / filename
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(root)
        except (OSError, RuntimeError, ValueError):
            return None
        return resolved if resolved.is_file() else None

    def resolve_sticker_path(self, token: str) -> Path | None:
        self.calls.append(("resolve_sticker_path", {"token": token}))
        pack_id, separator, stem = str(token or "").partition("/")
        if not separator or not stem:
            return None
        filename = stem if stem.lower().endswith(".webp") else f"{stem}.webp"
        return self.resolve_file(pack_id, filename)


@pytest.fixture
def sticker_client(tmp_path, monkeypatch):
    packs_root = tmp_path / "stickers" / "packs"
    (packs_root / "pack-a").mkdir(parents=True)
    application = StickerApplicationFake(packs_root)

    monkeypatch.setattr(core, "ADMIN_IDS", {"admin-1"})
    monkeypatch.setitem(core.app.config, "TESTING", True)
    monkeypatch.setitem(core.app.config, "SECRET_KEY", "sticker-route-tests")
    monkeypatch.setattr(sticker_routes, "sticker_application", application, raising=False)
    monkeypatch.setattr(message_routes, "sticker_application", application)

    # These legacy entry points must not perform network or runtime-data work.
    for name in ("fetch_remote_catalog", "start_install_job", "get_install_job"):
        if hasattr(sticker_routes, name):
            monkeypatch.setattr(
                sticker_routes,
                name,
                lambda *args, _name=name, **kwargs: pytest.fail(
                    f"sticker route bypassed sticker_application via {_name}"
                ),
            )
    if hasattr(sticker_routes, "stickers_packs_dir"):
        monkeypatch.setattr(
            sticker_routes,
            "stickers_packs_dir",
            lambda: pytest.fail("sticker route accessed the legacy runtime packs directory"),
        )

    return core.app.test_client(), application, packs_root


def _login(client, admin_id: str = "admin-1", *, csrf: bool = True) -> None:
    with client.session_transaction() as session:
        session["admin_id"] = admin_id
        if csrf:
            session["_csrf_token"] = CSRF_TOKEN


def test_sticker_routes_keep_original_urls_and_admin_permissions(sticker_client):
    client, application, _ = sticker_client
    requests = (
        ("GET", "/api/messages/stickers"),
        ("POST", "/api/messages/stickers/install"),
        ("GET", "/api/messages/stickers/install/job-1"),
        ("GET", "/api/messages/stickers/file/pack-a/1.webp"),
    )

    for method, path in requests:
        response = client.open(path, method=method, json={} if method == "POST" else None)
        assert response.status_code == 401
        assert response.get_json() == {"success": False, "error": "未登录"}

    _login(client, "not-an-admin")
    denied = client.get("/api/messages/stickers")
    assert denied.status_code == 401
    assert denied.get_json() == {"success": False, "error": "未登录"}
    assert application.calls == []


def test_catalog_preserves_envelope_and_refresh_query(sticker_client):
    client, application, _ = sticker_client
    _login(client)

    response = client.get("/api/messages/stickers?refresh=1")

    assert response.status_code == 200
    assert response.get_json() == application.catalog_result
    assert application.calls == [("catalog", {"force_refresh": True})]


def test_install_requires_csrf_and_returns_running_job_dto(sticker_client):
    client, application, _ = sticker_client
    _login(client)

    missing_csrf = client.post(
        "/api/messages/stickers/install",
        json={"pack_id": "pack-a"},
    )
    assert missing_csrf.status_code == 403
    assert missing_csrf.get_json() == {
        "success": False,
        "error": "CSRF 校验失败，请刷新页面后重试",
    }
    assert application.calls == []

    response = client.post(
        "/api/messages/stickers/install?force=1",
        json={"pack_id": "pack-a"},
        headers={"X-CSRF-Token": CSRF_TOKEN},
    )

    assert response.status_code == 202
    assert response.get_json() == application.install_result
    assert application.calls == [
        ("start_install", {"pack_id": "pack-a", "force": True})
    ]


def test_install_status_returns_job_dto_and_unknown_job_404(sticker_client):
    client, application, _ = sticker_client
    _login(client)

    response = client.get("/api/messages/stickers/install/job-1")
    assert response.status_code == 200
    assert response.get_json() == application.status_result

    missing = client.get("/api/messages/stickers/install/missing-job")
    assert missing.status_code == 404
    assert missing.get_json() == {"success": False, "error": "安装任务不存在"}
    assert application.calls == [
        ("install_status", {"job_id": "job-1"}),
        ("install_status", {"job_id": "missing-job"}),
    ]


def test_file_route_serves_image_headers_and_rejects_traversal_and_symlinks(sticker_client):
    client, application, packs_root = sticker_client
    _login(client)
    image = packs_root / "pack-a" / "1.webp"
    image.write_bytes(b"RIFF-test-WEBP")

    response = client.get("/api/messages/stickers/file/pack-a/1.webp")
    assert response.status_code == 200
    assert response.data == b"RIFF-test-WEBP"
    assert response.mimetype == "image/webp"
    assert response.headers["Cache-Control"] == "private, max-age=86400"
    assert response.headers["Content-Disposition"] == 'inline; filename="1.webp"'

    calls_before_rejections = len(application.calls)
    traversal = client.get(
        "/api/messages/stickers/file/pack-a/%2e%2e%2foutside.webp"
    )
    assert traversal.status_code == 404

    outside = packs_root.parent / "outside.webp"
    outside.write_bytes(b"outside")
    symlink = packs_root / "pack-a" / "linked.webp"
    symlink.symlink_to(outside)
    linked = client.get("/api/messages/stickers/file/pack-a/linked.webp")
    assert linked.status_code == 404
    assert len(application.calls) >= calls_before_rejections + 1
    assert all(name == "resolve_file" for name, _ in application.calls)


def test_message_send_uses_sticker_owner_resolver(sticker_client):
    _, application, packs_root = sticker_client
    image = packs_root / "pack-a" / "2.webp"
    image.write_bytes(b"RIFF-second-WEBP")

    assert message_routes.sticker_application is application
    resolved = message_routes.sticker_application.resolve_sticker_path("pack-a/2")

    assert resolved == image.resolve()
    assert application.calls == [
        ("resolve_sticker_path", {"token": "pack-a/2"}),
        ("resolve_file", {"pack_id": "pack-a", "filename": "2.webp"})
    ]
