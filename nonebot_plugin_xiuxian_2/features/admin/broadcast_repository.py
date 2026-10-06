from __future__ import annotations

import copy
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import RLock
from typing import Any, Callable


GROUP_SCENES = frozenset({"group", "channel_group"})
PRIVATE_SCENES = frozenset({"private", "channel_private"})


def accepts_scene(kind: str, scene: str) -> bool:
    return (
        (kind in {"group", "global"} and scene in GROUP_SCENES)
        or (kind in {"private", "global"} and scene in PRIVATE_SCENES)
    )


def adapter_family(adapter: str) -> str:
    if adapter == "QQ":
        return "qq"
    normalized = adapter.lower().replace(" ", "").replace("_", "").replace("-", "")
    if normalized in {"onebot", "onebot11", "onebotv11", "ob11", "v11"}:
        return "ob11"
    return ""


@dataclass(frozen=True)
class BroadcastHandle:
    task_id: str
    generation: int


@dataclass(frozen=True)
class BroadcastClaim:
    handle: BroadcastHandle
    token: int
    scene: str
    target_id: str


class AdminBroadcastRepository:
    """Process-local task state; no lock is held while a sender awaits the network."""

    def __init__(
        self, *, now: Callable[[], datetime] | None = None,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        self._clock = now or datetime.now
        self._id_factory = id_factory or (lambda: "BC" + uuid.uuid4().hex[:8].upper())
        self._lock = RLock()
        self._tasks: dict[str, dict[str, Any]] = {}
        self._generation = 0
        self._claim_token = 0

    def now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime):
            raise TypeError("broadcast clock must return datetime")
        return value

    def _cleanup(self, now: datetime) -> None:
        for task_id in [key for key, task in self._tasks.items() if now >= task["_expires_at"]]:
            del self._tasks[task_id]

    @staticmethod
    def _snapshot(task: dict[str, Any], now: datetime) -> dict[str, Any]:
        result = copy.deepcopy({key: value for key, value in task.items() if not key.startswith("_")})
        result["inflight_groups"] = {
            key for key, claim in task["_inflight"].items() if claim.scene in GROUP_SCENES
        }
        result["inflight_users"] = {
            key for key, claim in task["_inflight"].items() if claim.scene in PRIVATE_SCENES
        }
        result["pending_count"] = len(result["pending_groups"]) + len(result["pending_users"])
        result["inflight_count"] = len(task["_inflight"])
        result["remaining_seconds"] = max(0, int((task["_expires_at"] - now).total_seconds()))
        return result

    def create(
        self, *, adapter: str, bot_id: str, kind: str, content: str,
        duration_minutes: int, markdown: bool,
    ) -> tuple[BroadcastHandle, dict[str, Any]]:
        if not adapter_family(adapter) or not bot_id or kind not in {"group", "private", "global"}:
            raise ValueError("invalid broadcast identity or kind")
        if not content or duration_minutes <= 0:
            raise ValueError("invalid broadcast content or duration")
        with self._lock:
            now = self.now()
            self._cleanup(now)
            expires_at = now + timedelta(minutes=duration_minutes)
            task_id = str(self._id_factory()).strip().upper()
            if not task_id or task_id in self._tasks:
                raise RuntimeError("broadcast id conflict")
            self._generation += 1
            handle = BroadcastHandle(task_id, self._generation)
            task = {
                "id": task_id, "kind": kind, "adapter": adapter, "bot_id": bot_id,
                "content": content, "markdown": bool(markdown),
                "created_at": now.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_minutes": duration_minutes,
                "expire_at": expires_at.strftime("%Y-%m-%d %H:%M:%S"),
                "canceled": False,
                "sent_groups": set(), "sent_users": set(),
                "known_groups": set(), "known_users": set(),
                "pending_groups": set(), "pending_users": set(),
                "errors": [], "error_count": 0,
                "_generation": handle.generation, "_expires_at": expires_at, "_inflight": {},
            }
            self._tasks[task_id] = task
            return handle, self._snapshot(task, now)

    def status(self) -> list[dict[str, Any]]:
        with self._lock:
            now = self.now()
            self._cleanup(now)
            return [self._snapshot(task, now) for task in self._tasks.values()]

    def snapshot(self, handle: BroadcastHandle) -> dict[str, Any] | None:
        with self._lock:
            now = self.now()
            self._cleanup(now)
            task = self._tasks.get(handle.task_id)
            if task is None or task["_generation"] != handle.generation:
                return None
            return self._snapshot(task, now)

    def active_handles(self, *, adapter: str, bot_id: str, scene: str) -> list[BroadcastHandle]:
        if not adapter or not bot_id:
            return []
        with self._lock:
            self._cleanup(self.now())
            return [
                BroadcastHandle(task["id"], task["_generation"])
                for task in self._tasks.values()
                if not task["canceled"] and task["adapter"] == adapter and task["bot_id"] == bot_id
                and accepts_scene(task["kind"], scene)
            ]

    def claim(
        self, handle: BroadcastHandle, *, adapter: str, bot_id: str, scene: str, target_id: str,
    ) -> tuple[BroadcastClaim | None, dict[str, Any] | None, str]:
        with self._lock:
            now = self.now()
            self._cleanup(now)
            task = self._tasks.get(handle.task_id)
            if task is None or task["_generation"] != handle.generation:
                return None, None, "stopped"
            if task["canceled"]:
                return None, None, "cancelled"
            if not adapter or not bot_id or adapter != task["adapter"] or bot_id != task["bot_id"]:
                return None, None, "identity_mismatch"
            if not target_id:
                return None, None, "invalid_target"
            if not accepts_scene(task["kind"], scene):
                return None, None, "scene_mismatch"
            bucket = "groups" if scene in GROUP_SCENES else "users"
            key = f"{scene}:{target_id}"
            if key in task[f"sent_{bucket}"]:
                return None, None, "already_sent"
            if key in task[f"pending_{bucket}"]:
                return None, None, "pending_audit"
            if key in task["_inflight"]:
                return None, None, "inflight"
            self._claim_token += 1
            claim = BroadcastClaim(handle, self._claim_token, scene, target_id)
            task["_inflight"][key] = claim
            task[f"known_{bucket}"].add(key)
            payload = {
                field: task[field]
                for field in ("id", "kind", "adapter", "bot_id", "content", "markdown")
            }
            return claim, payload, "claimed"

    def finish(self, claim: BroadcastClaim, *, status: str, error_type: str = "") -> bool:
        if status not in {"sent", "pending_audit", "failed"}:
            raise ValueError("invalid broadcast delivery status")
        with self._lock:
            now = self.now()
            self._cleanup(now)
            task = self._tasks.get(claim.handle.task_id)
            key = f"{claim.scene}:{claim.target_id}"
            if (
                task is None or task["_generation"] != claim.handle.generation
                or task["_inflight"].get(key) != claim
            ):
                return False
            del task["_inflight"][key]
            bucket = "groups" if claim.scene in GROUP_SCENES else "users"
            if status == "sent":
                task[f"sent_{bucket}"].add(key)
            elif status == "pending_audit":
                task[f"pending_{bucket}"].add(key)
            else:
                task["error_count"] += 1
                task["errors"].append({
                    "time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "scene": claim.scene, "target_id": claim.target_id,
                    "error": error_type or "DeliveryFailed",
                })
                del task["errors"][:-50]
            return True

    def cancel(self, task_id: str) -> dict[str, Any]:
        task_id = str(task_id or "").strip().upper()
        if not task_id:
            return {"status": "missing_id", "id": "", "inflight_count": 0}
        with self._lock:
            self._cleanup(self.now())
            task = self._tasks.get(task_id)
            if task is None:
                return {"status": "not_found", "id": task_id, "inflight_count": 0}
            task["canceled"] = True
            return {"status": "cancelled", "id": task_id, "inflight_count": len(task["_inflight"])}

    def clear(self, kind: str | None = None) -> dict[str, Any]:
        if kind is not None:
            kind = str(kind).strip().lower()
        if kind in {"", "all", "全部", "所有"}:
            kind = None
        if kind is not None and kind not in {"group", "private", "global"}:
            return {"status": "invalid_kind", "kind": kind, "count": 0, "inflight_count": 0}
        with self._lock:
            self._cleanup(self.now())
            if not self._tasks:
                return {"status": "empty", "kind": kind, "count": 0, "inflight_count": 0}
            selected = [key for key, task in self._tasks.items() if kind is None or task["kind"] == kind]
            inflight_count = sum(len(self._tasks[key]["_inflight"]) for key in selected)
            for key in selected:
                del self._tasks[key]
            return {"status": "cleared", "kind": kind, "count": len(selected), "inflight_count": inflight_count}


__all__ = [
    "AdminBroadcastRepository", "BroadcastHandle", "BroadcastClaim", "accepts_scene", "adapter_family",
]
