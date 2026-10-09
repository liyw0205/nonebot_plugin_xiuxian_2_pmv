from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import stat
import tempfile
import threading
import uuid
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any, Callable
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from ...infrastructure.filesystem import atomic_write


from .schemas import (
    ALLOWED_DOWNLOAD_HOSTS as _ALLOWED_HOSTS,
    ARCHIVE_TIMEOUT_SECONDS,
    DOWNLOAD_CHUNK_BYTES,
    DOWNLOAD_PROXY_PREFIX,
    DOWNLOAD_USER_AGENT,
    FILE_REPO_NAME,
    FILE_REPO_OWNER,
    INITIAL_REQUEST_HOSTS,
    MANIFEST_TIMEOUT_SECONDS,
    MAX_MANIFEST_BYTES,
    MAX_STICKER_ARCHIVE_BYTES,
    MAX_STICKER_ARCHIVE_MEMBERS,
    MAX_STICKER_FILE_BYTES,
    MAX_STICKER_FILES,
    MAX_STICKER_UNCOMPRESSED_BYTES,
    PACK_ID_PATTERN as _PACK_ID_RE,
    SHA256_PATTERN as _SHA256_RE,
    STICKER_FILE_PATTERN as _STICKER_FILE_RE,
    STICKER_TOKEN_PATTERN as _STICKER_TOKEN_RE,
    STICKERS_MANIFEST_NAME,
    STICKERS_RELEASE_TAG,
    ZIP_NAME_PATTERN as _ZIP_NAME_RE,
)
_LOGGER = logging.getLogger(__name__)
_INSTALL_LOCK = threading.RLock()


def _safe_pack_id(value: object) -> str | None:
    pack_id = str(value or "").strip().lower()
    return pack_id if _PACK_ID_RE.fullmatch(pack_id) else None


def _safe_sticker_filename(value: object) -> str | None:
    name = str(value or "").strip()
    return name if _STICKER_FILE_RE.fullmatch(name) else None


def _validate_download_url(url: str, *, initial: bool = False) -> None:
    parts = urlsplit(url)
    try:
        port = parts.port
    except ValueError as exc:
        raise RuntimeError("远端下载地址无效") from exc
    host = (parts.hostname or "").lower()
    if (
        parts.scheme != "https"
        or host not in _ALLOWED_HOSTS
        or parts.username is not None
        or parts.password is not None
        or port not in (None, 443)
    ):
        raise RuntimeError("远端下载来源不在允许范围内")
    if initial and host not in INITIAL_REQUEST_HOSTS:
        raise RuntimeError("远端下载来源不在允许范围内")


class _ScopedRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        _validate_download_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _open_scoped_url(request: Request, timeout: int):
    _validate_download_url(request.full_url, initial=True)
    return build_opener(_ScopedRedirectHandler()).open(request, timeout=timeout)


def _sort_names(names: list[str]) -> list[str]:
    return sorted(
        names,
        key=lambda name: (0, int(Path(name).stem))
        if Path(name).stem.isdigit()
        else (1, name),
    )


class StickerRepository:
    """Filesystem and fixed-release owner for sticker catalog and package data."""

    def __init__(
        self,
        root: str | Path,
        *,
        open_url: Callable[[Request, int], Any] | None = None,
    ) -> None:
        self.root = Path(root)
        self.packs_dir = self.root / "packs"
        self.cache_dir = self.root / "cache"
        self.local_manifest_path = self.root / "manifest.json"
        self.remote_manifest_path = self.root / "remote-manifest.json"
        self._open_url = open_url or _open_scoped_url

    def _ensure_directories(self) -> None:
        self.packs_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def remote_manifest_url() -> str:
        return (
            f"https://github.com/{FILE_REPO_OWNER}/{FILE_REPO_NAME}"
            f"/releases/download/{STICKERS_RELEASE_TAG}/{STICKERS_MANIFEST_NAME}"
        )

    @staticmethod
    def remote_asset_url(name: str) -> str:
        if not _ZIP_NAME_RE.fullmatch(name):
            raise RuntimeError("远端表情包文件名无效")
        return (
            f"https://github.com/{FILE_REPO_OWNER}/{FILE_REPO_NAME}"
            f"/releases/download/{STICKERS_RELEASE_TAG}/{name}"
        )

    def _read_limited(self, url: str, limit: int, timeout: int) -> bytes:
        candidates = (url, f"{DOWNLOAD_PROXY_PREFIX}{url}")
        errors: list[Exception] = []
        for candidate in candidates:
            response = None
            try:
                _validate_download_url(candidate, initial=True)
                request = Request(candidate, headers={"User-Agent": DOWNLOAD_USER_AGENT})
                response = self._open_url(request, timeout)
                _validate_download_url(response.geturl())
                total = int(response.headers.get("Content-Length") or 0)
                if total > limit:
                    raise RuntimeError("远端 manifest 超过大小限制")
                chunks: list[bytes] = []
                downloaded = 0
                while True:
                    chunk = response.read(DOWNLOAD_CHUNK_BYTES)
                    if not chunk:
                        break
                    downloaded += len(chunk)
                    if downloaded > limit:
                        raise RuntimeError("远端 manifest 超过大小限制")
                    chunks.append(chunk)
                payload = b"".join(chunks)
                if not payload:
                    raise RuntimeError("远端返回内容为空")
                return payload
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                _LOGGER.warning("stickers download failed: %s: %s", candidate, exc)
            finally:
                if response is not None:
                    response.close()
        reason = "; ".join(str(error) for error in errors)
        raise RuntimeError(f"下载失败: {url}: {reason}")

    def _download_file(
        self,
        url: str,
        destination: Path,
        *,
        progress: Callable[[int, int], None] | None = None,
        max_bytes: int = MAX_STICKER_ARCHIVE_BYTES,
    ) -> str:
        candidates = (url, f"{DOWNLOAD_PROXY_PREFIX}{url}")
        errors: list[Exception] = []
        for candidate in candidates:
            response = None
            downloaded = 0
            digest = hashlib.sha256()
            try:
                _validate_download_url(candidate, initial=True)
                request = Request(candidate, headers={"User-Agent": DOWNLOAD_USER_AGENT})
                response = self._open_url(request, ARCHIVE_TIMEOUT_SECONDS)
                _validate_download_url(response.geturl())
                total = int(response.headers.get("Content-Length") or 0)
                if total > max_bytes:
                    raise RuntimeError("表情包压缩文件超过大小限制")
                with destination.open("wb") as output:
                    while True:
                        chunk = response.read(DOWNLOAD_CHUNK_BYTES)
                        if not chunk:
                            break
                        downloaded += len(chunk)
                        if downloaded > max_bytes:
                            raise RuntimeError("表情包压缩文件超过大小限制")
                        output.write(chunk)
                        digest.update(chunk)
                        if progress:
                            progress(downloaded, total)
                if not downloaded:
                    raise RuntimeError("下载内容为空")
                return digest.hexdigest()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
                destination.unlink(missing_ok=True)
                _LOGGER.warning("stickers download failed: %s: %s", candidate, exc)
            finally:
                if response is not None:
                    response.close()
        reason = "; ".join(str(error) for error in errors)
        raise RuntimeError(f"下载失败: {url}: {reason}")

    @staticmethod
    def _validate_remote_manifest(data: object) -> dict[str, Any]:
        if not isinstance(data, dict) or not isinstance(data.get("packs"), list):
            raise RuntimeError("远端 manifest 无效")
        packs: list[dict[str, Any]] = []
        seen: set[str] = set()
        for raw in data["packs"]:
            if not isinstance(raw, dict):
                continue
            pack_id = _safe_pack_id(raw.get("id"))
            zip_name = str(raw.get("zip") or "").strip()
            if not pack_id or pack_id in seen or not _ZIP_NAME_RE.fullmatch(zip_name):
                continue
            sha = str(raw.get("sha256") or "").strip().lower()
            if sha and not _SHA256_RE.fullmatch(sha):
                continue
            item = dict(raw)
            item["id"] = pack_id
            item["zip"] = zip_name
            if sha:
                item["sha256"] = sha
            packs.append(item)
            seen.add(pack_id)
        return {**data, "packs": packs}

    @staticmethod
    def _load_json(path: Path) -> object:
        return json.loads(path.read_text(encoding="utf-8"))

    def load_local_manifest(self) -> dict[str, Any] | None:
        try:
            data = self._load_json(self.local_manifest_path)
        except (OSError, ValueError, TypeError):
            return None
        if not isinstance(data, dict) or not isinstance(data.get("packs", []), list):
            return None
        return data

    def load_remote_catalog_cache(self) -> dict[str, Any] | None:
        try:
            data = self._validate_remote_manifest(self._load_json(self.remote_manifest_path))
        except (OSError, ValueError, TypeError, RuntimeError):
            return None
        return data

    def fetch_remote_catalog(self, *, force: bool = False) -> dict[str, Any]:
        cached = self.load_remote_catalog_cache()
        if cached is not None and not force:
            return cached
        raw = self._read_limited(
            self.remote_manifest_url(),
            MAX_MANIFEST_BYTES,
            MANIFEST_TIMEOUT_SECONDS,
        )
        try:
            remote = self._validate_remote_manifest(json.loads(raw.decode("utf-8")))
        except (UnicodeDecodeError, ValueError, TypeError) as exc:
            raise RuntimeError("远端 manifest 无效") from exc
        self._ensure_directories()
        atomic_write(
            self.remote_manifest_path,
            json.dumps(remote, ensure_ascii=False, indent=2).encode("utf-8"),
        )
        return remote

    def _pack_meta(self, pack_id: str) -> dict[str, Any] | None:
        pack_dir = self.packs_dir / pack_id
        try:
            meta = self._load_json(pack_dir / "pack.json")
        except (OSError, ValueError, TypeError):
            return None
        if not isinstance(meta, dict):
            return None
        items = meta.get("items")
        if not isinstance(items, list) or not items:
            items = _sort_names([path.name for path in pack_dir.glob("*.webp")])
            meta["items"] = items
            meta["count"] = len(items)
        return meta

    def build_local_catalog(self) -> dict[str, Any]:
        manifest = self.load_local_manifest() or {}
        packs_out: list[dict[str, Any]] = []
        for entry in manifest.get("packs") or []:
            if not isinstance(entry, dict):
                continue
            pack_id = _safe_pack_id(entry.get("id"))
            if not pack_id:
                continue
            meta = self._pack_meta(pack_id)
            if not meta:
                continue
            items = []
            raw_items = meta.get("items") if isinstance(meta.get("items"), list) else []
            for raw_name in raw_items:
                filename = _safe_sticker_filename(raw_name)
                if not filename or self.resolve_file(pack_id, filename) is None:
                    continue
                stem = Path(filename).stem
                items.append(
                    {
                        "id": stem,
                        "file": filename,
                        "token": f"{pack_id}/{stem}",
                        "url": f"/api/messages/stickers/file/{pack_id}/{filename}",
                    }
                )
            cover = _safe_sticker_filename(meta.get("cover") or "")
            if cover and self.resolve_file(pack_id, cover) is None:
                cover = None
            if not cover and items:
                cover = items[0]["file"]
            packs_out.append(
                {
                    "id": pack_id,
                    "name": str(entry.get("name") or meta.get("name") or pack_id),
                    "count": len(items),
                    "cover_url": f"/api/messages/stickers/file/{pack_id}/{cover}" if cover else "",
                    "items": items,
                }
            )
        return {
            "success": True,
            "installed": bool(packs_out),
            "version": int(manifest.get("version") or 0),
            "updated_at": manifest.get("updated_at"),
            "packs": packs_out,
        }

    def build_merged_catalog(self, remote: dict[str, Any]) -> dict[str, Any]:
        local_catalog = self.build_local_catalog()
        installed = {pack["id"]: pack for pack in local_catalog.get("packs") or []}
        packs_out = []
        for remote_pack in remote.get("packs") or []:
            pack_id = _safe_pack_id(remote_pack.get("id"))
            if not pack_id:
                continue
            local_pack = installed.get(pack_id)
            remote_count = remote_pack.get("count") or (local_pack or {}).get("count") or 0
            try:
                remote_count = max(0, int(remote_count))
            except (TypeError, ValueError):
                remote_count = 0
            if local_pack:
                pack = dict(local_pack)
                pack["installed"] = True
                pack["remote_count"] = remote_count
            else:
                pack = {
                    "id": pack_id,
                    "name": str(remote_pack.get("name") or pack_id),
                    "count": remote_count,
                    "remote_count": remote_count,
                    "installed": False,
                    "cover_url": "",
                    "items": [],
                }
            packs_out.append(pack)
        try:
            remote_version = int(remote.get("version") or 0)
        except (TypeError, ValueError):
            remote_version = 0
        return {
            "success": True,
            "installed": bool(installed),
            "version": int(local_catalog.get("version") or 0),
            "remote_version": remote_version,
            "updated_at": remote.get("updated_at"),
            "packs": packs_out,
        }

    def _extract_to_staging(self, zip_path: Path, pack_id: str) -> tuple[Path, dict[str, Any]]:
        self._ensure_directories()
        stage_dir = Path(tempfile.mkdtemp(prefix=f".{pack_id}.staging-", dir=self.packs_dir))
        try:
            with zipfile.ZipFile(zip_path) as archive:
                members = archive.infolist()
                if len(members) > MAX_STICKER_ARCHIVE_MEMBERS:
                    raise RuntimeError("表情包压缩包成员数量超过限制")
                extracted_bytes = 0
                selected: list[tuple[zipfile.ZipInfo, str]] = []
                names: set[str] = set()
                for info in members:
                    raw_name = info.filename
                    normalized = raw_name.replace("\\", "/")
                    path = PurePosixPath(normalized)
                    parts = path.parts
                    if (
                        not normalized
                        or "\x00" in normalized
                        or path.is_absolute()
                        or re.match(r"^[A-Za-z]:", normalized)
                        or ".." in parts
                    ):
                        raise RuntimeError(f"非法 zip 路径: {raw_name}")
                    mode = (info.external_attr >> 16) & 0xFFFF
                    kind = stat.S_IFMT(mode)
                    if kind == stat.S_IFLNK:
                        raise RuntimeError(f"zip 不允许符号链接: {raw_name}")
                    if kind not in (0, stat.S_IFREG, stat.S_IFDIR):
                        raise RuntimeError(f"zip 包含不支持的文件类型: {raw_name}")
                    if info.is_dir():
                        continue
                    if info.flag_bits & 0x1:
                        raise RuntimeError("表情包压缩包不允许加密成员")
                    if info.file_size > MAX_STICKER_FILE_BYTES:
                        raise RuntimeError("表情包单文件超过大小限制")
                    extracted_bytes += info.file_size
                    if extracted_bytes > MAX_STICKER_UNCOMPRESSED_BYTES:
                        raise RuntimeError("表情包解压后超过大小限制")
                    if not parts:
                        continue
                    rel_parts = parts[1:] if parts[0] == pack_id else parts
                    if len(rel_parts) != 1:
                        continue
                    leaf = rel_parts[0]
                    if leaf == "pack.json":
                        target_name = leaf
                    else:
                        target_name = _safe_sticker_filename(leaf)
                        if target_name is None:
                            continue
                    if target_name in names:
                        raise RuntimeError(f"zip 包含重复文件: {target_name}")
                    names.add(target_name)
                    selected.append((info, target_name))
                    if len(selected) > MAX_STICKER_FILES:
                        raise RuntimeError("表情包文件数量超过限制")

                for info, target_name in selected:
                    target = stage_dir / target_name
                    copied = 0
                    with archive.open(info) as source, target.open("xb") as output:
                        while True:
                            chunk = source.read(DOWNLOAD_CHUNK_BYTES)
                            if not chunk:
                                break
                            copied += len(chunk)
                            if copied > MAX_STICKER_FILE_BYTES:
                                raise RuntimeError("表情包单文件超过大小限制")
                            output.write(chunk)
                    if copied != info.file_size:
                        raise RuntimeError(f"表情包文件大小不匹配: {info.filename}")

            webps = _sort_names([path.name for path in stage_dir.glob("*.webp")])
            if not webps:
                raise RuntimeError(f"表情包为空: {pack_id}")
            try:
                meta = self._load_json(stage_dir / "pack.json")
            except (OSError, ValueError, TypeError):
                meta = {}
            if not isinstance(meta, dict):
                meta = {}
            meta["id"] = pack_id
            meta["name"] = str(meta.get("name") or pack_id)
            meta["format"] = "webp"
            meta["items"] = webps
            meta["count"] = len(webps)
            if not isinstance(meta.get("cover"), str) or meta["cover"] not in webps:
                meta["cover"] = webps[0]
            atomic_write(
                stage_dir / "pack.json",
                json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8"),
            )
            return stage_dir, meta
        except BaseException:
            shutil.rmtree(stage_dir, ignore_errors=True)
            raise

    def _installed_by_id(self) -> dict[str, dict[str, Any]]:
        manifest = self.load_local_manifest() or {"version": 0, "packs": []}
        return {
            pack_id: dict(entry)
            for entry in manifest.get("packs") or []
            if isinstance(entry, dict) and (pack_id := _safe_pack_id(entry.get("id")))
        }

    def install_pack(
        self,
        pack_id: str,
        *,
        force: bool = False,
        progress: Callable[..., None] | None = None,
    ) -> dict[str, Any]:
        selected_id = _safe_pack_id(pack_id)
        if not selected_id:
            raise RuntimeError("无效表情包 ID")

        def report(**state: Any) -> None:
            if progress:
                progress(**state)

        self._ensure_directories()
        report(stage="manifest", percent=2, message="正在读取表情包清单")
        remote = self.fetch_remote_catalog()
        selected = next((p for p in remote["packs"] if p["id"] == selected_id), None)
        if selected is None:
            raise RuntimeError("表情包不在远端清单中")

        pack_name = str(selected.get("name") or selected_id)
        zip_name = selected["zip"]
        expect_sha = str(selected.get("sha256") or "").lower()
        remote_asset_url = self.remote_asset_url(zip_name)
        with _INSTALL_LOCK:
            installed_by_id = self._installed_by_id()
            pack_dir = self.packs_dir / selected_id
            if selected_id in installed_by_id and (pack_dir / "pack.json").is_file() and not force:
                report(stage="complete", percent=100, message="表情包已下载")
                return self.build_local_catalog()

            fd, temp_name = tempfile.mkstemp(prefix=f".{selected_id}-", suffix=".zip", dir=self.cache_dir)
            os.close(fd)
            archive_path = Path(temp_name)
            staging_path: Path | None = None
            backup_path: Path | None = None
            installed_new = False
            try:
                def on_download(downloaded: int, total: int) -> None:
                    percent = min(86, int(5 + ((downloaded / total) if total else 0) * 81))
                    report(
                        stage="download",
                        percent=percent,
                        message=f"正在下载 {pack_name}",
                        pack_id=selected_id,
                        pack_name=pack_name,
                        downloaded=downloaded,
                        total=total,
                    )

                report(
                    stage="download",
                    percent=5,
                    message=f"正在下载 {pack_name}",
                    pack_id=selected_id,
                    pack_name=pack_name,
                    downloaded=0,
                    total=0,
                )
                got_sha = self._download_file(remote_asset_url, archive_path, progress=on_download)
                if expect_sha and got_sha != expect_sha:
                    raise RuntimeError(f"{zip_name} sha256 不匹配")
                report(stage="verify", percent=90, message=f"正在校验 {pack_name}")
                report(stage="extract", percent=95, message=f"正在安装 {pack_name}")
                staging_path, meta = self._extract_to_staging(archive_path, selected_id)

                installed_by_id[selected_id] = {
                    "id": selected_id,
                    "name": pack_name,
                    "zip": zip_name,
                    "sha256": got_sha,
                    "count": int(meta.get("count") or 0),
                    "format": "webp",
                    "cover": meta.get("cover"),
                }
                new_manifest = {
                    "version": int(remote.get("version") or 1),
                    "updated_at": remote.get("updated_at"),
                    "packs": list(installed_by_id.values()),
                }
                if pack_dir.exists():
                    backup_path = self.packs_dir / f".{selected_id}.backup-{uuid.uuid4().hex}"
                    os.replace(pack_dir, backup_path)
                try:
                    os.replace(staging_path, pack_dir)
                    staging_path = None
                    installed_new = True
                    atomic_write(
                        self.local_manifest_path,
                        json.dumps(new_manifest, ensure_ascii=False, indent=2).encode("utf-8"),
                    )
                except BaseException:
                    if installed_new:
                        shutil.rmtree(pack_dir, ignore_errors=True)
                        installed_new = False
                    if backup_path is not None and backup_path.exists():
                        os.replace(backup_path, pack_dir)
                        backup_path = None
                    raise
                if backup_path is not None:
                    shutil.rmtree(backup_path, ignore_errors=True)
                    backup_path = None
                catalog = self.build_local_catalog()
                report(stage="complete", percent=100, message=f"{pack_name} 下载完成")
                return catalog
            finally:
                archive_path.unlink(missing_ok=True)
                if staging_path is not None:
                    shutil.rmtree(staging_path, ignore_errors=True)
                if backup_path is not None and backup_path.exists():
                    if not pack_dir.exists():
                        os.replace(backup_path, pack_dir)
                    else:
                        shutil.rmtree(backup_path, ignore_errors=True)

    def resolve_file(self, pack_id: str, filename: str) -> Path | None:
        safe_id = _safe_pack_id(pack_id)
        safe_name = _safe_sticker_filename(filename)
        if not safe_id or not safe_name:
            return None
        try:
            packs_root = self.packs_dir.resolve()
            pack_root = (packs_root / safe_id).resolve()
            path = (pack_root / safe_name).resolve()
            if pack_root.parent != packs_root or pack_root not in path.parents or not path.is_file():
                return None
            return path
        except OSError:
            return None

    def resolve_sticker_path(self, token: str) -> Path | None:
        match = _STICKER_TOKEN_RE.fullmatch(str(token or "").strip())
        if not match:
            return None
        pack_id, stem = match.groups()
        filename = stem if stem.lower().endswith(".webp") else f"{stem}.webp"
        return self.resolve_file(pack_id, filename)


__all__ = [
    "FILE_REPO_NAME",
    "FILE_REPO_OWNER",
    "MAX_MANIFEST_BYTES",
    "MAX_STICKER_ARCHIVE_BYTES",
    "MAX_STICKER_ARCHIVE_MEMBERS",
    "MAX_STICKER_FILES",
    "MAX_STICKER_FILE_BYTES",
    "MAX_STICKER_UNCOMPRESSED_BYTES",
    "STICKERS_MANIFEST_NAME",
    "STICKERS_RELEASE_TAG",
    "StickerRepository",
]
