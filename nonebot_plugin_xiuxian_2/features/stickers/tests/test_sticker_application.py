from __future__ import annotations

import hashlib
import io
import json
import stat
import zipfile
from pathlib import Path
from urllib.request import Request

import pytest

from nonebot_plugin_xiuxian_2.features.stickers.factory import build_sticker_application
from nonebot_plugin_xiuxian_2.features.stickers.repository import (
    MAX_STICKER_ARCHIVE_BYTES,
    MAX_STICKER_ARCHIVE_MEMBERS,
    StickerRepository,
)


class FakeResponse:
    def __init__(self, payload: bytes, url: str, headers: dict[str, str] | None = None):
        self._stream = io.BytesIO(payload)
        self._url = url
        self.headers = headers or {"Content-Length": str(len(payload))}

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def geturl(self) -> str:
        return self._url

    def close(self) -> None:
        self._stream.close()


class FakeRemote:
    def __init__(self, responses: dict[str, bytes]):
        self.responses = responses
        self.requests: list[str] = []

    def __call__(self, request: Request, timeout: int):
        self.requests.append(request.full_url)
        # Tests use the GitHub primary URL; ghproxy remains available as a fallback.
        payload = self.responses.get(request.full_url)
        if payload is None:
            raise OSError("not found")
        return FakeResponse(payload, request.full_url)


def make_zip(*entries: tuple[str, bytes], symlink: str | None = None) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in entries:
            archive.writestr(name, data)
        if symlink:
            info = zipfile.ZipInfo(symlink)
            info.create_system = 3
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(info, "outside")
    return buffer.getvalue()


def make_remote(pack_bytes: bytes, pack_id: str = "memes") -> tuple[dict[str, bytes], str]:
    zip_name = f"{pack_id}.zip"
    manifest = {
        "version": 7,
        "updated_at": "2026-10-08T00:00:00Z",
        "packs": [
            {
                "id": pack_id,
                "name": "Memes",
                "zip": zip_name,
                "sha256": hashlib.sha256(pack_bytes).hexdigest(),
                "count": 1,
            }
        ],
    }
    manifest_url = StickerRepository.remote_manifest_url()
    asset_url = StickerRepository.remote_asset_url(zip_name)
    return {manifest_url: json.dumps(manifest).encode(), asset_url: pack_bytes}, asset_url


def test_factory_uses_explicit_root_and_catalog_keeps_remote_shape(tmp_path: Path):
    archive = make_zip(("memes/1.webp", b"image"))
    responses, _ = make_remote(archive)
    remote = FakeRemote(responses)
    app = build_sticker_application(tmp_path / "stickers", open_url=remote)

    catalog = app.catalog()
    app.catalog()
    assert remote.requests.count(StickerRepository.remote_manifest_url()) == 1
    app.catalog(force_refresh=True)
    assert remote.requests.count(StickerRepository.remote_manifest_url()) == 2

    assert catalog["success"] is True
    assert catalog["remote_version"] == 7
    assert catalog["packs"] == [
        {
            "id": "memes",
            "name": "Memes",
            "count": 1,
            "remote_count": 1,
            "installed": False,
            "cover_url": "",
            "items": [],
        }
    ]


def test_install_exposes_catalog_and_resolved_files(tmp_path: Path):
    archive = make_zip(
        ("memes/pack.json", b'{"name":"Custom","cover":"2.webp"}'),
        ("memes/10.webp", b"ten"),
        ("memes/2.webp", b"two"),
    )
    responses, _ = make_remote(archive)
    app = build_sticker_application(
        tmp_path / "stickers",
        open_url=FakeRemote(responses),
        thread_starter=lambda target: target(),
    )

    started = app.start_install("memes")
    status = app.install_status(started["job_id"])

    assert status is not None and status["status"] == "complete"
    assert [item["file"] for item in status["catalog"]["packs"][0]["items"]] == [
        "2.webp",
        "10.webp",
    ]
    assert status["catalog"]["packs"][0]["name"] == "Memes"
    assert app.resolve_sticker_path("memes/2") == (tmp_path / "stickers/packs/memes/2.webp")
    assert app.resolve_file("memes", "2.webp") is not None
    assert app.resolve_file("memes", "../2.webp") is None
    assert app.resolve_sticker_path("memes/../2") is None
    assert app.install_status("missing") is None


@pytest.mark.parametrize(
    "archive, message",
    [
        (make_zip(("memes/../escape.webp", b"bad"), ("memes/1.webp", b"ok")), "非法 zip 路径"),
        (make_zip(("memes/1.webp", b"ok"), symlink="memes/2.webp"), "符号链接"),
    ],
)
def test_invalid_archive_keeps_previous_install(tmp_path: Path, archive: bytes, message: str):
    root = tmp_path / "stickers"
    old_pack = root / "packs/memes"
    old_pack.mkdir(parents=True)
    (old_pack / "1.webp").write_bytes(b"old")
    (old_pack / "pack.json").write_text(
        json.dumps({"id": "memes", "name": "Old", "items": ["1.webp"]}), encoding="utf-8"
    )
    (root / "manifest.json").write_text(
        json.dumps({"version": 1, "packs": [{"id": "memes", "name": "Old"}]}),
        encoding="utf-8",
    )
    responses, _ = make_remote(archive)
    app = build_sticker_application(root, open_url=FakeRemote(responses), thread_starter=lambda target: target())

    started = app.start_install("memes", force=True)
    status = app.install_status(started["job_id"])

    assert status is not None and status["status"] == "error"
    assert message in status["error"]
    assert (old_pack / "1.webp").read_bytes() == b"old"
    assert json.loads((root / "manifest.json").read_text(encoding="utf-8"))["version"] == 1


def test_manifest_write_failure_rolls_back_replaced_pack(tmp_path: Path, monkeypatch):
    archive = make_zip(("memes/1.webp", b"new"))
    responses, _ = make_remote(archive)
    root = tmp_path / "stickers"
    old_pack = root / "packs/memes"
    old_pack.mkdir(parents=True)
    (old_pack / "1.webp").write_bytes(b"old")
    (old_pack / "pack.json").write_text(
        json.dumps({"id": "memes", "name": "Old", "items": ["1.webp"]}), encoding="utf-8"
    )
    manifest = root / "manifest.json"
    manifest.write_text(json.dumps({"version": 1, "packs": [{"id": "memes"}]}), encoding="utf-8")
    app = build_sticker_application(root, open_url=FakeRemote(responses), thread_starter=lambda target: target())
    repository_module = __import__(
        "nonebot_plugin_xiuxian_2.features.stickers.repository", fromlist=["atomic_write"]
    )
    original_atomic_write = repository_module.atomic_write
    calls = 0

    def fail_local_manifest(path, data):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("disk full")
        return original_atomic_write(path, data)

    monkeypatch.setattr(repository_module, "atomic_write", fail_local_manifest)

    started = app.start_install("memes", force=True)

    assert app.install_status(started["job_id"])["status"] == "error"
    assert (old_pack / "1.webp").read_bytes() == b"old"
    assert json.loads(manifest.read_text(encoding="utf-8"))["version"] == 1


def test_archive_member_limit_is_checked_before_install(tmp_path: Path, monkeypatch):
    archive = make_zip(("memes/1.webp", b"ok"))
    responses, _ = make_remote(archive)
    app = build_sticker_application(tmp_path, open_url=FakeRemote(responses), thread_starter=lambda target: target())
    monkeypatch.setattr(
        "nonebot_plugin_xiuxian_2.features.stickers.repository.MAX_STICKER_ARCHIVE_MEMBERS", 0
    )

    started = app.start_install("memes")

    assert app.install_status(started["job_id"])["status"] == "error"
    assert "成员数量超过限制" in app.install_status(started["job_id"])["error"]


def test_bad_redirect_target_is_rejected_before_install(tmp_path: Path):
    archive = make_zip(("memes/1.webp", b"image"))
    responses, asset_url = make_remote(archive)
    class RedirectingRemote(FakeRemote):
        def __call__(self, request: Request, timeout: int):
            if request.full_url == asset_url:
                self.requests.append(request.full_url)
                return FakeResponse(responses[asset_url], "https://127.0.0.1/stickers.zip")
            return super().__call__(request, timeout)

    app = build_sticker_application(
        tmp_path,
        open_url=RedirectingRemote(responses),
        thread_starter=lambda target: target(),
    )

    started = app.start_install("memes")
    status = app.install_status(started["job_id"])

    assert status is not None and status["status"] == "error"
    assert "允许范围" in status["error"]
    assert not (tmp_path / "packs/memes/1.webp").exists()


def test_archive_download_byte_limit_is_enforced(tmp_path: Path):
    archive = make_zip(("memes/1.webp", b"image"))
    responses, asset_url = make_remote(archive)
    app = build_sticker_application(tmp_path, open_url=FakeRemote(responses), thread_starter=lambda target: target())
    repository = app._repository
    with pytest.raises(RuntimeError, match="大小限制"):
        repository._download_file(asset_url, tmp_path / "too-large.zip", max_bytes=len(archive) - 1)


def test_application_deduplicates_running_jobs():
    from nonebot_plugin_xiuxian_2.features.stickers.application import StickerApplication

    app = StickerApplication(StickerRepository("/unused"), thread_starter=lambda target: None)
    first = app.start_install("memes")
    second = app.start_install("memes")

    assert first["job_id"] == second["job_id"]
    assert app.install_status(first["job_id"])["status"] == "running"


def test_download_archive_rejects_oversize_content_length(tmp_path: Path):
    payload = b"x" * 16
    remote = lambda request, timeout: FakeResponse(
        payload, request.full_url, {"Content-Length": str(MAX_STICKER_ARCHIVE_BYTES + 1)}
    )
    repository = StickerRepository(tmp_path, open_url=remote)

    with pytest.raises(RuntimeError, match="大小限制"):
        repository._download_file(StickerRepository.remote_asset_url("memes.zip"), tmp_path / "x.zip")
