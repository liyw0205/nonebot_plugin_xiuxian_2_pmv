from __future__ import annotations

import base64
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol


class QqBindTaskStore(Protocol):
    def add(self, task_id: str, key: str) -> None: ...

    def get(self, task_id: str) -> tuple[float, str] | None: ...

    def pop(self, task_id: str) -> tuple[float, str] | None: ...

    def complete(self, task_id: str, result: dict[str, Any]) -> None: ...

    def completed(self, task_id: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True)
class QqBindResponse:
    body: dict[str, Any]
    status_code: int = 200


class QqBindApplication:
    def __init__(
        self,
        *,
        tasks: QqBindTaskStore,
        create_task: Callable[[str], Awaitable[dict[str, Any]]],
        poll_task: Callable[[str], Awaitable[dict[str, Any]]],
        bind_page_url: Callable[[str], str],
        qr_png_bytes: Callable[[str], bytes],
        decrypt_secret: Callable[[str, str], str],
        merge_env: Callable[[Path, str, str], bool],
        env_file: Callable[[], Path],
        detect_restart: Callable[[Path], dict[str, Any]],
        schedule_restart: Callable[[dict[str, Any]], bool],
        project_root: Callable[[], Path],
    ) -> None:
        self._tasks = tasks
        self._create_task = create_task
        self._poll_task = poll_task
        self._bind_page_url = bind_page_url
        self._qr_png_bytes = qr_png_bytes
        self._decrypt_secret = decrypt_secret
        self._merge_env = merge_env
        self._env_file = env_file
        self._detect_restart = detect_restart
        self._schedule_restart = schedule_restart
        self._project_root = project_root

    async def start(self) -> QqBindResponse:
        key = base64.b64encode(secrets.token_bytes(32)).decode("ascii")
        result = await self._create_task(key)
        task_id = str((result.get("data") or {}).get("task_id") or "")
        if result.get("retcode") != 0 or not task_id:
            return QqBindResponse(
                {"success": False, "error": result.get("msg") or "创建扫码绑定任务失败"},
                502,
            )
        self._tasks.add(task_id, key)
        return QqBindResponse(
            {
                "success": True,
                "task_id": task_id,
                "status": "waiting",
                "qr_url": f"/api/config/qq-bind/qr/{task_id}",
                "connect_url": self._bind_page_url(task_id),
            }
        )

    def qr_png(self, task_id: str) -> bytes | None:
        if self._tasks.get(task_id) is None:
            return None
        return self._qr_png_bytes(self._bind_page_url(task_id))

    async def poll(self, task_id: str) -> QqBindResponse:
        completed = self._tasks.completed(task_id)
        if completed is not None:
            return QqBindResponse(completed)
        entry = self._tasks.get(task_id)
        if not task_id or entry is None:
            return QqBindResponse({"success": True, "status": "expired"})

        result = await self._poll_task(task_id)
        if result.get("retcode") != 0:
            return QqBindResponse(
                {
                    "success": False,
                    "status": "error",
                    "error": result.get("msg") or "查询绑定结果失败",
                }
            )
        data = result.get("data") or {}
        status = data.get("status")
        if status == 3:
            self._tasks.pop(task_id)
            return QqBindResponse({"success": True, "status": "expired"})
        if status != 2:
            return QqBindResponse({"success": True, "status": "waiting"})

        entry = self._tasks.pop(task_id)
        appid = str(data.get("bot_appid") or "")
        encrypted = str(data.get("bot_encrypt_secret") or "")
        if entry is None or not appid or not encrypted:
            return QqBindResponse(
                {
                    "success": False,
                    "status": "error",
                    "error": "绑定结果缺少 AppID 或 Secret",
                }
            )
        try:
            secret = self._decrypt_secret(encrypted, entry[1])
            replaced = self._merge_env(self._env_file(), appid, secret)
        except Exception as exc:
            return QqBindResponse(
                {
                    "success": False,
                    "status": "error",
                    "error": f"写入 QQ_BOTS 失败：{exc}",
                },
                500,
            )
        response = {
            "success": True,
            "status": "completed",
            "appid": appid,
            "replaced": replaced,
            "message": "QQ 已确认绑定，配置已安全落盘。重启后将仅通过该机器人建立 WebSocket 连接。",
        }
        self._tasks.complete(task_id, response)
        return QqBindResponse(response)

    def restart_capability(self) -> dict[str, Any]:
        capability = self._detect_restart(self._project_root())
        return {
            "success": True,
            "automatic": bool(capability.get("automatic")),
            "mode": capability.get("mode"),
            "message": capability.get("message"),
        }

    def restart(self, confirm: object) -> QqBindResponse:
        if confirm is not True:
            return QqBindResponse({"success": False, "error": "需要明确确认重启"}, 400)
        capability = self._detect_restart(self._project_root())
        if not capability.get("automatic"):
            return QqBindResponse(
                {
                    "success": False,
                    "automatic": False,
                    "mode": capability.get("mode"),
                    "error": capability.get("message"),
                },
                409,
            )
        if not self._schedule_restart(capability):
            return QqBindResponse({"success": False, "error": "无法提交重启任务"}, 500)
        return QqBindResponse(
            {
                "success": True,
                "scheduled": True,
                "message": "重启任务已提交，页面连接将暂时中断。",
            }
        )
