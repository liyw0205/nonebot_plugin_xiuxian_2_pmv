from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from nonebot.log import logger

from ...paths import get_paths
from ..xiuxian_utils.json_store import load_json_file, save_json_file


SCHEDULE_STORE = get_paths().data / "scheduler_overrides.json"
_DEFAULT_STORE = {"version": 1, "jobs": {}}
_CRON_FIELDS = ("year", "month", "day", "week", "day_of_week", "hour", "minute", "second")
_MANUAL_PREFIX = "web-manual:"


class SchedulerJobManager:
    """Persisted runtime controls for registered APScheduler jobs."""

    def __init__(self, scheduler, store_path: str | Path = SCHEDULE_STORE) -> None:
        self._scheduler = scheduler
        self._store_path = Path(store_path)
        self._lock = RLock()

    def _load_store(self) -> dict[str, Any]:
        data = load_json_file(self._store_path, _DEFAULT_STORE, dict)
        jobs = data.get("jobs") if isinstance(data, dict) else None
        return {"version": 1, "jobs": jobs if isinstance(jobs, dict) else {}}

    def _save_store(self, data: dict[str, Any]) -> None:
        save_json_file(self._store_path, data)

    @staticmethod
    def _serialize_trigger(trigger) -> dict[str, Any]:
        if isinstance(trigger, CronTrigger):
            return {
                "type": "cron",
                "fields": {field.name: str(field) for field in trigger.fields},
            }
        if isinstance(trigger, IntervalTrigger):
            return {
                "type": "interval",
                "seconds": max(int(trigger.interval.total_seconds()), 1),
            }
        return {"type": "readonly", "description": str(trigger)}

    @classmethod
    def _build_trigger(cls, spec: object, *, timezone=None):
        if not isinstance(spec, dict):
            raise ValueError("定时配置必须是对象")
        trigger_type = str(spec.get("type") or "").strip().lower()
        if trigger_type == "cron":
            raw_fields = spec.get("fields")
            if not isinstance(raw_fields, dict):
                raise ValueError("Cron 定时配置缺少 fields")
            fields = {}
            for name, value in raw_fields.items():
                if name not in _CRON_FIELDS:
                    raise ValueError(f"不支持的 Cron 字段：{name}")
                text = str(value).strip()
                if not text or len(text) > 64:
                    raise ValueError(f"Cron 字段 {name} 无效")
                fields[name] = text
            if not fields:
                raise ValueError("至少需要设置一个 Cron 字段")
            return CronTrigger(timezone=timezone, **fields)
        if trigger_type == "interval":
            try:
                seconds = int(spec.get("seconds"))
            except (TypeError, ValueError) as exc:
                raise ValueError("间隔秒数必须是整数") from exc
            if not 1 <= seconds <= 31 * 24 * 60 * 60:
                raise ValueError("间隔秒数必须在 1 秒到 31 天之间")
            return IntervalTrigger(seconds=seconds, timezone=timezone)
        raise ValueError("仅支持 cron 和 interval 定时")

    def _get_job(self, job_id: str):
        job = self._scheduler.get_job(str(job_id))
        if job is None or str(job.id).startswith(_MANUAL_PREFIX):
            raise ValueError("定时任务不存在")
        return job

    def _job_data(self, job) -> dict[str, Any]:
        return {
            "id": str(job.id),
            "name": str(job.name or job.id),
            "enabled": job.next_run_time is not None,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": self._serialize_trigger(job.trigger),
            "max_instances": int(job.max_instances),
            "coalesce": bool(job.coalesce),
        }

    def list_jobs(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                self._job_data(job)
                for job in sorted(self._scheduler.get_jobs(), key=lambda item: str(item.id))
                if not str(job.id).startswith(_MANUAL_PREFIX)
            ]

    def set_enabled(self, job_id: str, enabled: bool) -> dict[str, Any]:
        with self._lock:
            job = self._get_job(job_id)
            if enabled:
                self._scheduler.resume_job(job.id)
            else:
                self._scheduler.pause_job(job.id)
            store = self._load_store()
            entry = store["jobs"].setdefault(str(job.id), {})
            entry["enabled"] = bool(enabled)
            self._save_store(store)
            return self._job_data(self._get_job(job.id))

    def reschedule(self, job_id: str, trigger_spec: object) -> dict[str, Any]:
        with self._lock:
            job = self._get_job(job_id)
            was_paused = job.next_run_time is None
            trigger = self._build_trigger(
                trigger_spec,
                timezone=getattr(job.trigger, "timezone", None),
            )
            self._scheduler.reschedule_job(job.id, trigger=trigger)
            if was_paused:
                self._scheduler.pause_job(job.id)
            store = self._load_store()
            entry = store["jobs"].setdefault(str(job.id), {})
            entry["trigger"] = self._serialize_trigger(trigger)
            self._save_store(store)
            return self._job_data(self._get_job(job.id))

    def queue_manual_run(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            job = self._get_job(job_id)
            manual_id = f"{_MANUAL_PREFIX}{job.id}"
            if self._scheduler.get_job(manual_id) is not None:
                raise ValueError("该任务已有一次手动执行等待调度")
            self._scheduler.add_job(
                job.func,
                trigger="date",
                run_date=datetime.now(getattr(self._scheduler, "timezone", None)),
                args=job.args,
                kwargs=job.kwargs,
                id=manual_id,
                name=f"手动执行：{job.name or job.id}",
                max_instances=1,
                misfire_grace_time=300,
            )
            return {"id": str(job.id), "queued": True}

    def apply_persisted_overrides(self) -> None:
        with self._lock:
            for job_id, entry in self._load_store()["jobs"].items():
                if not isinstance(entry, dict):
                    continue
                job = self._scheduler.get_job(job_id)
                if job is None:
                    continue
                try:
                    if "trigger" in entry:
                        self._scheduler.reschedule_job(
                            job.id,
                            trigger=self._build_trigger(
                                entry["trigger"],
                                timezone=getattr(job.trigger, "timezone", None),
                            ),
                        )
                    if entry.get("enabled") is False:
                        self._scheduler.pause_job(job.id)
                    elif entry.get("enabled") is True:
                        self._scheduler.resume_job(job.id)
                except (TypeError, ValueError) as exc:
                    logger.warning(f"忽略无效的定时任务覆盖配置 {job_id}: {exc}")
