from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from .broadcast_repository import AdminBroadcastRepository, BroadcastHandle, adapter_family


class AdminBroadcastApplication:
    """Coordinate history and sender ports without owning platform API details."""

    def __init__(self, repository: AdminBroadcastRepository, *, history, sender) -> None:
        self.repository = repository
        self.history = history
        self.sender = sender

    @staticmethod
    def _identity(adapter: Any, bot_id: Any) -> tuple[str, str]:
        return str(adapter or "").strip(), str(bot_id or "").strip()

    async def start(
        self, bot: Any, *, adapter: str, bot_id: str, kind: str, content: str,
        duration_minutes: int = 1440, markdown: bool = False,
    ) -> dict[str, Any]:
        result = {"id": "", "task": None, "success_count": 0, "pending_count": 0, "failed_count": 0}
        adapter, bot_id = self._identity(adapter, bot_id)
        if not adapter or not bot_id:
            return dict(result, status="invalid_identity")
        if not adapter_family(adapter):
            return dict(result, status="invalid_adapter")
        if kind not in {"group", "private", "global"}:
            return dict(result, status="invalid_kind")
        content = str(content or "").strip()
        if not content:
            return dict(result, status="invalid_content")
        try:
            duration_minutes = int(duration_minutes)
        except (TypeError, ValueError, OverflowError):
            duration_minutes = 1440
        if duration_minutes <= 0:
            duration_minutes = 1440
        try:
            self.repository.now() + timedelta(minutes=duration_minutes)
        except OverflowError:
            return dict(result, status="invalid_duration")
        try:
            targets = list(await self.history(adapter, bot_id, kind, self.repository.now()))
            targets = [
                {
                    "scene": str(target.get("scene") or ""),
                    "target_id": str(target.get("target_id") or "").strip(),
                    "message_id": str(target.get("message_id") or ""),
                }
                for target in targets
            ]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            return dict(result, status="history_failed", error_type=type(exc).__name__)
        try:
            handle, _ = self.repository.create(
                adapter=adapter, bot_id=bot_id, kind=kind, content=content,
                duration_minutes=duration_minutes, markdown=markdown,
            )
        except Exception as exc:
            return dict(result, status="create_failed", error_type=type(exc).__name__)
        result["id"] = handle.task_id
        for target in targets:
            delivery = await self._deliver(
                bot, handle, adapter=adapter, bot_id=bot_id,
                scene=str(target.get("scene") or ""), target_id=str(target.get("target_id") or ""),
                source_message_id=str(target.get("message_id") or ""),
            )
            if delivery["status"] == "sent":
                result["success_count"] += 1
            elif delivery["status"] == "pending_audit" and delivery.get("attempted"):
                result["pending_count"] += 1
            elif delivery["status"] == "failed":
                result["failed_count"] += 1
            elif delivery["status"] in {"stopped", "cancelled"}:
                break
        task = self.repository.snapshot(handle)
        result["task"] = task
        result["status"] = "stopped" if task is None else "cancelled" if task["canceled"] else "created"
        return result

    async def _deliver(
        self, bot: Any, handle: BroadcastHandle, *, adapter: str, bot_id: str,
        scene: str, target_id: str, source_message_id: str,
    ) -> dict[str, Any]:
        claim, task, status = self.repository.claim(
            handle, adapter=adapter, bot_id=bot_id, scene=scene, target_id=target_id,
        )
        result = {"id": handle.task_id, "scene": scene, "target_id": target_id, "status": status, "attempted": False}
        if claim is None:
            return result
        try:
            if adapter_family(adapter) == "qq" and not source_message_id:
                raise ValueError("missing source message id")
            sent = await self.sender(bot, task, scene, target_id, source_message_id)
            status = sent.get("status") if isinstance(sent, dict) else getattr(sent, "status", None)
            if not isinstance(status, str) or status not in {"sent", "pending_audit"}:
                self.repository.finish(claim, status="failed", error_type="UnexpectedSendStatus")
                return dict(result, status="failed", attempted=True, error_type="UnexpectedSendStatus")
        except asyncio.CancelledError:
            self.repository.finish(claim, status="failed", error_type="CancelledError")
            raise
        except Exception as exc:
            error_type = type(exc).__name__
            self.repository.finish(claim, status="failed", error_type=error_type)
            return dict(result, status="failed", attempted=True, error_type=error_type)
        self.repository.finish(claim, status=status)
        return dict(result, status=status, attempted=True)

    async def patch_event(
        self, bot: Any, *, adapter: str, bot_id: str, scene: str,
        target_id: str, source_message_id: str = "",
    ) -> list[dict[str, Any]]:
        adapter, bot_id = self._identity(adapter, bot_id)
        target_id = str(target_id or "").strip()
        if not adapter_family(adapter) or not bot_id or not target_id:
            return []
        results = []
        for handle in self.repository.active_handles(adapter=adapter, bot_id=bot_id, scene=scene):
            results.append(await self._deliver(
                bot, handle, adapter=adapter, bot_id=bot_id, scene=scene,
                target_id=target_id, source_message_id=str(source_message_id or ""),
            ))
        return results

    def status(self) -> list[dict[str, Any]]:
        return self.repository.status()

    def cancel(self, task_id: str) -> dict[str, Any]:
        return self.repository.cancel(task_id)

    def clear(self, kind: str | None = None) -> dict[str, Any]:
        return self.repository.clear(kind)


__all__ = ["AdminBroadcastApplication"]
