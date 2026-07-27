"""消息面板表情包：文件仓库固定 Release 下载、本地缓存、catalog 与发送解析。"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import threading
import uuid
import zipfile
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

from flask import Response, abort, jsonify, request, session

from ...paths import get_paths
from .core import app, logger

FILE_REPO_OWNER = "liyw0205"
FILE_REPO_NAME = "nonebot_plugin_xiuxian_2_pmv_file"
STICKERS_RELEASE_TAG = "stickers-latest"
STICKERS_MANIFEST_NAME = "stickers-manifest.json"

_PACK_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
_STICKER_FILE_RE = re.compile(r"^[A-Za-z0-9._-]+\.webp$")
_STICKER_TOKEN_RE = re.compile(r"^([a-z0-9][a-z0-9_-]{0,31})/([A-Za-z0-9._-]+)$")


def stickers_root() -> Path:
    root = get_paths().data / "stickers"
    root.mkdir(parents=True, exist_ok=True)
    (root / "packs").mkdir(parents=True, exist_ok=True)
    (root / "cache").mkdir(parents=True, exist_ok=True)
    return root


def stickers_packs_dir() -> Path:
    return stickers_root() / "packs"


def stickers_cache_dir() -> Path:
    return stickers_root() / "cache"


def local_manifest_path() -> Path:
    return stickers_root() / "manifest.json"


def remote_catalog_cache_path() -> Path:
    return stickers_root() / "remote-manifest.json"


def remote_manifest_url() -> str:
    return (
        f"https://github.com/{FILE_REPO_OWNER}/{FILE_REPO_NAME}"
        f"/releases/download/{STICKERS_RELEASE_TAG}/{STICKERS_MANIFEST_NAME}"
    )


def remote_asset_url(name: str) -> str:
    return (
        f"https://github.com/{FILE_REPO_OWNER}/{FILE_REPO_NAME}"
        f"/releases/download/{STICKERS_RELEASE_TAG}/{name}"
    )


def _require_admin():
    if "admin_id" not in session:
        return jsonify({"success": False, "error": "未登录"}), 401
    return None


def _download_bytes(
    url: str,
    timeout: int = 60,
    progress: Callable[[int, int], None] | None = None,
) -> bytes:
    urls = [url, f"https://ghproxy.net/{url}"]
    last_err: Exception | None = None
    for u in urls:
        try:
            req = Request(u, headers={"User-Agent": "xiuxian-web-stickers/1.0"})
            with urlopen(req, timeout=timeout) as resp:
                total = int(resp.headers.get("Content-Length") or 0)
                chunks: list[bytes] = []
                downloaded = 0
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    downloaded += len(chunk)
                    if progress:
                        progress(downloaded, total)
                data = b"".join(chunks)
            if data:
                return data
        except Exception as e:  # noqa: BLE001
            last_err = e
            logger.warning(f"stickers download failed: {u}: {e}")
    raise RuntimeError(f"下载失败: {url}: {last_err}")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _safe_pack_id(pack_id: str) -> str | None:
    pack_id = str(pack_id or "").strip().lower()
    if not _PACK_ID_RE.fullmatch(pack_id):
        return None
    return pack_id


def _safe_sticker_filename(name: str) -> str | None:
    name = str(name or "").strip()
    if not _STICKER_FILE_RE.fullmatch(name):
        return None
    return name


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_local_manifest() -> dict[str, Any] | None:
    path = local_manifest_path()
    if not path.exists():
        return None
    try:
        data = _load_json(path)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def load_remote_catalog_cache() -> dict[str, Any] | None:
    path = remote_catalog_cache_path()
    if not path.exists():
        return None
    try:
        data = _load_json(path)
        return data if isinstance(data, dict) and isinstance(data.get("packs"), list) else None
    except Exception:
        return None


def fetch_remote_catalog(force: bool = False) -> dict[str, Any]:
    cached = load_remote_catalog_cache()
    if cached is not None and not force:
        return cached
    remote = json.loads(_download_bytes(remote_manifest_url(), timeout=20).decode("utf-8"))
    if not isinstance(remote, dict) or not isinstance(remote.get("packs"), list):
        raise RuntimeError("远端 manifest 无效")
    remote_catalog_cache_path().write_text(
        json.dumps(remote, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return remote


def is_installed() -> bool:
    man = load_local_manifest()
    if not man:
        return False
    packs = man.get("packs") or []
    if not packs:
        return False
    for p in packs:
        pid = _safe_pack_id(str(p.get("id") or ""))
        if not pid:
            return False
        pack_dir = stickers_packs_dir() / pid
        pack_json = pack_dir / "pack.json"
        if not pack_json.exists():
            return False
    return True


def _extract_pack_zip(zip_path: Path, pack_id: str) -> dict[str, Any]:
    pack_dir = stickers_packs_dir() / pack_id
    if pack_dir.exists():
        shutil.rmtree(pack_dir)
    pack_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename.replace("\\", "/").lstrip("/")
            # only allow pack_id/xxx.webp or pack_id/pack.json
            parts = [p for p in name.split("/") if p and p != "."]
            if ".." in parts:
                raise RuntimeError(f"非法 zip 路径: {info.filename}")
            if not parts:
                continue
            # accept both pack_id/file and bare file
            if parts[0] == pack_id:
                rel_parts = parts[1:]
            else:
                rel_parts = parts
            if not rel_parts or len(rel_parts) != 1:
                continue
            leaf = rel_parts[0]
            if leaf == "pack.json":
                target = pack_dir / "pack.json"
            else:
                safe = _safe_sticker_filename(leaf)
                if not safe:
                    continue
                target = pack_dir / safe
            target.write_bytes(zf.read(info))

    pack_json_path = pack_dir / "pack.json"
    webps = sorted(
        [p.name for p in pack_dir.glob("*.webp")],
        key=lambda s: (0, int(Path(s).stem)) if Path(s).stem.isdigit() else (1, s),
    )
    if not webps:
        raise RuntimeError(f"表情包为空: {pack_id}")

    if pack_json_path.exists():
        try:
            meta = _load_json(pack_json_path)
        except Exception:
            meta = {}
    else:
        meta = {}

    if not isinstance(meta, dict):
        meta = {}
    meta["id"] = pack_id
    meta["name"] = str(meta.get("name") or pack_id)
    meta["format"] = "webp"
    meta["items"] = webps
    meta["count"] = len(webps)
    if not meta.get("cover") or meta["cover"] not in webps:
        meta["cover"] = webps[0]
    pack_json_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta


_INSTALL_JOBS: dict[str, dict[str, Any]] = {}
_INSTALL_JOBS_LOCK = threading.Lock()


def _set_install_job(job_id: str, **changes: Any) -> None:
    with _INSTALL_JOBS_LOCK:
        job = _INSTALL_JOBS.get(job_id)
        if job is not None:
            job.update(changes)


def get_install_job(job_id: str) -> dict[str, Any] | None:
    with _INSTALL_JOBS_LOCK:
        job = _INSTALL_JOBS.get(str(job_id or ""))
        return dict(job) if job else None


def install_stickers(
    pack_id: str,
    force: bool = False,
    progress: Callable[..., None] | None = None,
) -> dict[str, Any]:
    def report(**state: Any) -> None:
        if progress:
            progress(**state)

    selected_id = _safe_pack_id(pack_id)
    if not selected_id:
        raise RuntimeError("无效表情包 ID")

    report(stage="manifest", percent=2, message="正在读取表情包清单")
    remote = fetch_remote_catalog(force=False)
    selected = next(
        (
            p
            for p in remote.get("packs") or []
            if isinstance(p, dict) and _safe_pack_id(str(p.get("id") or "")) == selected_id
        ),
        None,
    )
    if selected is None:
        raise RuntimeError("表情包不在远端清单中")

    local = load_local_manifest() or {"version": 0, "packs": []}
    installed_by_id = {
        str(p.get("id") or ""): p
        for p in local.get("packs") or []
        if isinstance(p, dict)
    }
    pack_dir = stickers_packs_dir() / selected_id
    if selected_id in installed_by_id and (pack_dir / "pack.json").exists() and not force:
        report(stage="complete", percent=100, message="表情包已下载")
        return build_catalog()

    zip_name = str(selected.get("zip") or "").strip()
    expect_sha = str(selected.get("sha256") or "").strip().lower()
    if not zip_name or "/" in zip_name or "\\" in zip_name:
        raise RuntimeError("远端表情包文件名无效")
    pack_name = str(selected.get("name") or selected_id)

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
    data = _download_bytes(remote_asset_url(zip_name), progress=on_download)
    report(stage="verify", percent=90, message=f"正在校验 {pack_name}")
    got = _sha256_bytes(data)
    if expect_sha and got != expect_sha:
        raise RuntimeError(f"{zip_name} sha256 不匹配")
    cache_path = stickers_cache_dir() / zip_name
    cache_path.write_bytes(data)
    report(stage="extract", percent=95, message=f"正在安装 {pack_name}")
    meta = _extract_pack_zip(cache_path, selected_id)
    installed_by_id[selected_id] = {
        "id": selected_id,
        "name": pack_name,
        "zip": zip_name,
        "sha256": got,
        "count": int(meta.get("count") or 0),
        "format": "webp",
        "cover": meta.get("cover"),
    }
    local = {
        "version": int(remote.get("version") or 1),
        "updated_at": remote.get("updated_at"),
        "packs": list(installed_by_id.values()),
    }
    local_manifest_path().write_text(json.dumps(local, ensure_ascii=False, indent=2), encoding="utf-8")
    catalog = build_catalog()
    report(stage="complete", percent=100, message=f"{pack_name} 下载完成")
    return catalog


def _run_install_job(job_id: str, pack_id: str, force: bool) -> None:
    try:
        catalog = install_stickers(
            pack_id=pack_id,
            force=force,
            progress=lambda **state: _set_install_job(job_id, **state),
        )
        pack = next((p for p in catalog.get("packs") or [] if p.get("id") == pack_id), {})
        _set_install_job(
            job_id,
            status="complete",
            stage="complete",
            percent=100,
            message=f"{pack.get('name') or pack_id} 下载完成",
            pack_id=pack_id,
            catalog=catalog,
        )
    except Exception as e:  # noqa: BLE001
        logger.exception("stickers install job failed")
        _set_install_job(
            job_id,
            status="error",
            stage="error",
            message=f"安装表情包失败: {e}",
            error=str(e),
        )


def start_install_job(pack_id: str, force: bool = False) -> dict[str, Any]:
    selected_id = _safe_pack_id(pack_id)
    if not selected_id:
        raise RuntimeError("无效表情包 ID")
    with _INSTALL_JOBS_LOCK:
        for job in _INSTALL_JOBS.values():
            if job.get("status") == "running" and job.get("pack_id") == selected_id:
                return dict(job)
        job_id = uuid.uuid4().hex
        job = {
            "success": True,
            "job_id": job_id,
            "status": "running",
            "stage": "queued",
            "percent": 0,
            "message": "准备下载表情包",
            "pack_id": selected_id,
        }
        _INSTALL_JOBS[job_id] = job
    threading.Thread(
        target=_run_install_job,
        args=(job_id, selected_id, force),
        daemon=True,
    ).start()
    return dict(job)


def _pack_meta(pack_id: str) -> dict[str, Any] | None:
    pack_dir = stickers_packs_dir() / pack_id
    pack_json = pack_dir / "pack.json"
    if not pack_json.exists():
        return None
    try:
        meta = _load_json(pack_json)
    except Exception:
        return None
    if not isinstance(meta, dict):
        return None
    items = meta.get("items")
    if not isinstance(items, list) or not items:
        items = sorted(
            [p.name for p in pack_dir.glob("*.webp")],
            key=lambda s: (0, int(Path(s).stem)) if Path(s).stem.isdigit() else (1, s),
        )
        meta["items"] = items
        meta["count"] = len(items)
    return meta


def build_catalog() -> dict[str, Any]:
    man = load_local_manifest() or {}
    packs_out = []
    for p in man.get("packs") or []:
        if not isinstance(p, dict):
            continue
        pack_id = _safe_pack_id(str(p.get("id") or ""))
        if not pack_id:
            continue
        meta = _pack_meta(pack_id)
        if not meta:
            continue
        items = []
        for name in meta.get("items") or []:
            safe = _safe_sticker_filename(str(name))
            if not safe:
                continue
            stem = Path(safe).stem
            items.append(
                {
                    "id": stem,
                    "file": safe,
                    "token": f"{pack_id}/{stem}",
                    "url": f"/api/messages/stickers/file/{pack_id}/{safe}",
                }
            )
        cover = _safe_sticker_filename(str(meta.get("cover") or (items[0]["file"] if items else "")))
        packs_out.append(
            {
                "id": pack_id,
                "name": str(p.get("name") or meta.get("name") or pack_id),
                "count": len(items),
                "cover_url": f"/api/messages/stickers/file/{pack_id}/{cover}" if cover else "",
                "items": items,
            }
        )
    return {
        "success": True,
        "installed": bool(packs_out),
        "version": int(man.get("version") or 0),
        "updated_at": man.get("updated_at"),
        "packs": packs_out,
    }


def resolve_sticker_path(token: str) -> Path | None:
    token = str(token or "").strip()
    m = _STICKER_TOKEN_RE.fullmatch(token)
    if not m:
        return None
    pack_id = _safe_pack_id(m.group(1))
    stem = m.group(2)
    if not pack_id:
        return None
    # allow with or without .webp
    if stem.lower().endswith(".webp"):
        filename = _safe_sticker_filename(stem)
    else:
        filename = _safe_sticker_filename(f"{stem}.webp")
    if not filename:
        return None
    path = stickers_packs_dir() / pack_id / filename
    try:
        path = path.resolve()
        root = stickers_packs_dir().resolve()
        if root not in path.parents and path != root:
            return None
    except Exception:
        return None
    if not path.is_file():
        return None
    return path


def build_merged_catalog(remote: dict[str, Any]) -> dict[str, Any]:
    local_catalog = build_catalog()
    installed = {p["id"]: p for p in local_catalog.get("packs") or []}
    packs_out = []
    for remote_pack in remote.get("packs") or []:
        if not isinstance(remote_pack, dict):
            continue
        pack_id = _safe_pack_id(str(remote_pack.get("id") or ""))
        if not pack_id:
            continue
        local_pack = installed.get(pack_id)
        if local_pack:
            pack = dict(local_pack)
            pack["installed"] = True
            pack["remote_count"] = int(remote_pack.get("count") or pack.get("count") or 0)
        else:
            pack = {
                "id": pack_id,
                "name": str(remote_pack.get("name") or pack_id),
                "count": int(remote_pack.get("count") or 0),
                "remote_count": int(remote_pack.get("count") or 0),
                "installed": False,
                "cover_url": "",
                "items": [],
            }
        packs_out.append(pack)
    return {
        "success": True,
        "installed": bool(installed),
        "version": int(local_catalog.get("version") or 0),
        "remote_version": int(remote.get("version") or 0),
        "updated_at": remote.get("updated_at"),
        "packs": packs_out,
    }


@app.route("/api/messages/stickers", methods=["GET"])
def api_messages_stickers():
    denied = _require_admin()
    if denied:
        return denied
    try:
        refresh = str(request.args.get("refresh") or "").lower() in ("1", "true", "yes", "on")
        remote = fetch_remote_catalog(force=refresh)
        return jsonify(build_merged_catalog(remote))
    except Exception as e:  # noqa: BLE001
        return jsonify({"success": False, "error": f"获取表情包目录失败: {e}"})


@app.route("/api/messages/stickers/install", methods=["POST"])
def api_messages_stickers_install():
    denied = _require_admin()
    if denied:
        return denied
    try:
        data = request.get_json(silent=True) or {}
        pack_id = _safe_pack_id(str(data.get("pack_id") or request.args.get("pack_id") or ""))
        if not pack_id:
            return jsonify({"success": False, "error": "未指定表情包"}), 400
        force = str(data.get("force") or request.args.get("force") or "").lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        job = start_install_job(pack_id=pack_id, force=force)
        return jsonify(job), 202 if job.get("status") == "running" else 200
    except Exception as e:  # noqa: BLE001
        logger.exception("stickers install failed")
        return jsonify({"success": False, "error": f"安装表情包失败: {e}"})


@app.route("/api/messages/stickers/install/<job_id>", methods=["GET"])
def api_messages_stickers_install_status(job_id: str):
    denied = _require_admin()
    if denied:
        return denied
    job = get_install_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "安装任务不存在"}), 404
    return jsonify(job)


@app.route("/api/messages/stickers/file/<pack_id>/<path:filename>", methods=["GET"])
def api_messages_stickers_file(pack_id: str, filename: str):
    denied = _require_admin()
    if denied:
        return denied
    pack_id = _safe_pack_id(pack_id)
    filename = _safe_sticker_filename(filename)
    if not pack_id or not filename:
        abort(404)
    path = stickers_packs_dir() / pack_id / filename
    if not path.is_file():
        abort(404)
    return Response(
        path.read_bytes(),
        mimetype="image/webp",
        headers={
            "Cache-Control": "private, max-age=86400",
            "Content-Disposition": f'inline; filename="{filename}"',
        },
    )
