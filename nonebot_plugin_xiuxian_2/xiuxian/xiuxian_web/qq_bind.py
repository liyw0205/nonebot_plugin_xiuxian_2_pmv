from __future__ import annotations

import base64
import io
import json
import os
import re
import shutil
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import aiohttp
import qrcode
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_BIND_CREATE_URL = "https://q.qq.com/lite/create_bind_task"
_BIND_POLL_URL = "https://q.qq.com/lite/poll_bind_result"
_BIND_PAGE_URL = (
    "https://q.qq.com/qqbot/openclaw/connect.html"
    "?task_id={task_id}&source=xiuxian&_wv=2"
)
_QQ_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 "
        "Chrome/109.0.5414.118 Mobile Safari/537.36"
    ),
    "Origin": "https://q.qq.com",
    "Referer": "https://q.qq.com/",
}
_QQ_BOTS_RE = re.compile(
    r"(?ms)^QQ_BOTS\s*=\s*(['\"])(.*?)\1\s*(?=\n[A-Za-z_][A-Za-z0-9_]*\s*=|\Z)"
)


class BindTaskStore:
    def __init__(self, ttl: int = 600):
        self.ttl = max(1, int(ttl))
        self._tasks: dict[str, tuple[float, str]] = {}
        self._completed: dict[str, tuple[float, dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def _prune(self) -> None:
        cutoff = time.time() - self.ttl
        for task_id, (created, _) in list(self._tasks.items()):
            if created < cutoff:
                self._tasks.pop(task_id, None)
        for task_id, (created, _) in list(self._completed.items()):
            if created < cutoff:
                self._completed.pop(task_id, None)

    def add(self, task_id: str, key: str) -> None:
        with self._lock:
            self._prune()
            self._tasks[str(task_id)] = (time.time(), str(key))

    def get(self, task_id: str) -> tuple[float, str] | None:
        with self._lock:
            self._prune()
            return self._tasks.get(str(task_id))

    def pop(self, task_id: str) -> tuple[float, str] | None:
        with self._lock:
            self._prune()
            return self._tasks.pop(str(task_id), None)

    def complete(self, task_id: str, result: dict[str, Any]) -> None:
        public = {
            key: value
            for key, value in result.items()
            if key in {"success", "status", "appid", "replaced", "message"}
        }
        with self._lock:
            self._prune()
            self._tasks.pop(str(task_id), None)
            self._completed[str(task_id)] = (time.time(), public)

    def completed(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._prune()
            entry = self._completed.get(str(task_id))
            return dict(entry[1]) if entry else None

    def public_status(self, task_id: str, status: str) -> dict[str, str]:
        if self.get(task_id) is None:
            return {"status": "expired"}
        return {"status": str(status)}


def decrypt_bind_secret(encrypted_b64: str, key_b64: str) -> str:
    encrypted = base64.b64decode(encrypted_b64, validate=True)
    key = base64.b64decode(key_b64, validate=True)
    if len(encrypted) < 29 or len(key) != 32:
        raise ValueError("无效的绑定密文")
    nonce, body = encrypted[:12], encrypted[12:]
    return AESGCM(key).decrypt(nonce, body, None).decode("utf-8")


def qr_png_bytes(content: str) -> bytes:
    image = qrcode.make(str(content))
    output = io.BytesIO()
    image.save(output, format="PNG")
    return output.getvalue()


def _parse_qq_bots(text: str) -> tuple[list[dict[str, Any]], re.Match[str] | None]:
    match = _QQ_BOTS_RE.search(text)
    if match is None:
        return [], None
    value = match.group(2).strip()
    bots = json.loads(value or "[]")
    if isinstance(bots, dict):
        bots = [bots]
    if not isinstance(bots, list) or not all(isinstance(item, dict) for item in bots):
        raise ValueError("QQ_BOTS 必须是机器人对象列表")
    return bots, match


def merge_qq_bots_env(env_file: Path, appid: str, secret: str) -> bool:
    appid = str(appid).strip()
    secret = str(secret).strip()
    if not appid or not secret:
        raise ValueError("绑定结果缺少 AppID 或 Secret")

    env_file = Path(env_file)
    text = env_file.read_text(encoding="utf-8") if env_file.exists() else ""
    bots, match = _parse_qq_bots(text)
    selected = next(
        (
            dict(bot)
            for bot in bots
            if str(bot.get("id") or "").strip() == appid
        ),
        None,
    )
    replaced = bool(bots)
    bot = selected or {"id": appid}
    bot["id"] = appid
    bot["token"] = secret
    bot["secret"] = secret
    intent = bot.get("intent")
    if not isinstance(intent, dict):
        intent = {}
    intent.update(
        {
            "c2c_group_at_messages": True,
            "direct_message": True,
        }
    )
    bot["intent"] = intent
    bot["use_websocket"] = True
    bots = [bot]

    assignment = "QQ_BOTS='\n" + json.dumps(bots, ensure_ascii=False, indent=2) + "\n'"
    if match is None:
        updated = text.rstrip() + ("\n" if text.strip() else "") + assignment + "\n"
    else:
        updated = text[: match.start()] + assignment + text[match.end() :]
        if not updated.endswith("\n"):
            updated += "\n"

    env_file.parent.mkdir(parents=True, exist_ok=True)
    if env_file.exists():
        shutil.copy2(env_file, env_file.with_name(env_file.name + ".bak"))
    fd, temporary = tempfile.mkstemp(prefix=env_file.name + ".", dir=env_file.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(updated)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, env_file)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return replaced


async def create_bind_task(key_b64: str) -> dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout, headers=_QQ_HEADERS) as client:
        async with client.post(_BIND_CREATE_URL, json={"key": key_b64}) as response:
            return await response.json(content_type=None)


async def poll_bind_result(task_id: str) -> dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout, headers=_QQ_HEADERS) as client:
        async with client.post(_BIND_POLL_URL, json={"task_id": task_id}) as response:
            return await response.json(content_type=None)


def bind_page_url(task_id: str) -> str:
    from urllib.parse import quote

    return _BIND_PAGE_URL.format(task_id=quote(str(task_id), safe=""))
