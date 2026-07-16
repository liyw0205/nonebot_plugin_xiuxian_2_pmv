from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED,
)
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from nonebot.log import logger

from ...paths import get_paths
from ..xiuxian_utils.json_store import load_json_file, save_json_file


SCHEDULE_STORE = get_paths().data / "scheduler_overrides.json"
_DEFAULT_STORE = {"version": 1, "jobs": {}}
_CRON_FIELDS = ("year", "month", "day", "week", "day_of_week", "hour", "minute", "second")
_MANUAL_PREFIX = "web-manual:"
_RUN_HISTORY_LIMIT = 100

# 前端展示用中文名（按 job.id / 函数名匹配）
_JOB_TITLES: dict[str, str] = {
    "auto_guishi_transactions": "鬼市自动交易",
    "auto_guishi_transactions_job": "鬼市自动交易",
    "clear_expired_baitan_orders": "清理超时摆摊",
    "clear_expired_baitan_orders_job": "清理超时摆摊",
    "materialsupdate_": "发放宗门资材",
    "materialsupdate": "发放宗门资材",
    "daily_reset_work_refresh_num": "悬赏令次数重置",
    "scheduled_rift_generation_job": "秘境重置",
    "auto_handle_inactive_sect_owners_job": "处理宗门状态",
    "reset_data_by_time_job": "处理早晚数据",
    "backup_database_files": "数据库备份",
}


def _humanize_job_id(job_id: str) -> str:
    text = str(job_id or "").strip()
    if not text:
        return "未命名任务"
    if text in _JOB_TITLES:
        return _JOB_TITLES[text]
    # 去掉常见后缀
    base = text
    for suffix in ("_job", "_"):
        if base.endswith(suffix) and len(base) > len(suffix):
            base = base[: -len(suffix)]
    if base in _JOB_TITLES:
        return _JOB_TITLES[base]
    # snake_case → 空格，尽量可读
    pretty = base.replace("-", " ").replace("_", " ").strip()
    return pretty or text


class SchedulerJobManager:
    """Persisted runtime controls for registered APScheduler jobs."""

    def __init__(self, scheduler, store_path: str | Path = SCHEDULE_STORE) -> None:
        self._scheduler = scheduler
        self._store_path = Path(store_path)
        self._lock = RLock()
        self._runs: dict[str, dict[str, Any]] = {}
        self._manual_jobs: dict[str, str] = {}
        self._last_runs: dict[str, dict[str, Any]] = {}
        self._scheduler.add_listener(
            self._handle_job_event,
            EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
        )

    def _now(self) -> str:
        return datetime.now(getattr(self._scheduler, "timezone", None)).isoformat()

    def _handle_job_event(self, event) -> None:
        manual_job_id = str(event.job_id)
        with self._lock:
            run_id = self._manual_jobs.get(manual_job_id)
            if run_id is None:
                return
            run = self._runs.get(run_id)
            if run is None:
                self._manual_jobs.pop(manual_job_id, None)
                return
            if event.code == EVENT_JOB_SUBMITTED:
                if run["status"] == "queued":
                    run["status"] = "running"
                    run["started_at"] = self._now()
                return

            self._manual_jobs.pop(manual_job_id, None)
            run["finished_at"] = self._now()
            if event.code == EVENT_JOB_EXECUTED:
                run["status"] = "succeeded"
                run["error"] = None
            elif event.code == EVENT_JOB_MISSED:
                run["status"] = "failed"
                run["error"] = "任务错过调度时间"
            else:
                run["status"] = "failed"
                exception = getattr(event, "exception", None)
                run["error"] = (
                    f"{type(exception).__name__}: {exception}"
                    if exception is not None
                    else "任务执行失败"
                )
            self._last_runs[run["job_id"]] = dict(run)

    def _load_store(self) -> dict[str, Any]:
        data = load_json_file(self._store_path, _DEFAULT_STORE, dict)
        jobs = data.get("jobs") if isinstance(data, dict) else None
        return {"version": 1, "jobs": jobs if isinstance(jobs, dict) else {}}

    def _save_store(self, data: dict[str, Any]) -> None:
        save_json_file(self._store_path, data)

    @staticmethod
    def _serialize_trigger(trigger) -> dict[str, Any]:
        if isinstance(trigger, CronTrigger):
            # 只返回实际参与调度的字段，避免 year/week 等噪音
            fields = {
                field.name: str(field)
                for field in trigger.fields
                if str(field) not in {"*", "*/1"} or field.name in {"hour", "minute", "second", "day_of_week", "day", "month"}
            }
            # 保证常用字段顺序
            ordered = {
                key: fields[key]
                for key in ("minute", "hour", "day", "month", "day_of_week", "second", "year", "week")
                if key in fields
            }
            return {
                "type": "cron",
                "fields": ordered or {field.name: str(field) for field in trigger.fields},
                "summary": SchedulerJobManager._cron_summary(ordered or fields),
            }
        if isinstance(trigger, IntervalTrigger):
            seconds = max(int(trigger.interval.total_seconds()), 1)
            return {
                "type": "interval",
                "seconds": seconds,
                "summary": SchedulerJobManager._interval_summary(seconds),
            }
        return {"type": "readonly", "description": str(trigger), "summary": str(trigger)}

    @staticmethod
    def _interval_summary(seconds: int) -> str:
        n = int(seconds)
        if n % 86400 == 0:
            return f"每 {n // 86400} 天"
        if n % 3600 == 0:
            return f"每 {n // 3600} 小时"
        if n % 60 == 0:
            return f"每 {n // 60} 分钟"
        return f"每 {n} 秒"

    @staticmethod
    def _cron_summary(fields: dict[str, str]) -> str:
        f = {k: str(v) for k, v in (fields or {}).items()}
        minute = f.get("minute", "*")
        hour = f.get("hour", "*")
        day = f.get("day", "*")
        month = f.get("month", "*")
        dow = f.get("day_of_week", "*")
        second = f.get("second", "0")

        # 常见模式
        if hour == "*" and minute.isdigit() and day in {"*", "*/1"} and month in {"*", "*/1"} and dow in {"*", "*/1"}:
            if minute == "0":
                return "每小时"
            return f"每小时的第 {minute} 分"
        if hour.startswith("*/") and minute in {"0", "00"}:
            try:
                n = int(hour[2:])
                if n > 0:
                    return f"每 {n} 小时"
            except ValueError:
                pass
        if hour.isdigit() and minute.isdigit() and day in {"*", "*/1"} and month in {"*", "*/1"} and dow in {"*", "*/1"}:
            return f"每天 {int(hour):02d}:{int(minute):02d}"
        if dow not in {"*", "*/1"} and hour.isdigit() and minute.isdigit():
            dow_map = {
                "mon": "周一", "tue": "周二", "wed": "周三", "thu": "周四",
                "fri": "周五", "sat": "周六", "sun": "周日",
                "0": "周一", "1": "周二", "2": "周三", "3": "周四",
                "4": "周五", "5": "周六", "6": "周日", "7": "周日",
            }
            label = dow_map.get(str(dow).lower(), f"周{dow}")
            return f"每{label} {int(hour):02d}:{int(minute):02d}"

        parts = []
        if month not in {"*", "*/1"}:
            parts.append(f"月={month}")
        if day not in {"*", "*/1"}:
            parts.append(f"日={day}")
        if dow not in {"*", "*/1"}:
            parts.append(f"周={dow}")
        if hour not in {"*", "*/1"}:
            parts.append(f"时={hour}")
        if minute not in {"*", "*/1"}:
            parts.append(f"分={minute}")
        if second not in {"0", "00", "*", "*/1"}:
            parts.append(f"秒={second}")
        return " ".join(parts) if parts else "自定义 cron"

    @classmethod
    def _build_trigger(cls, spec: object, *, timezone=None):
        if not isinstance(spec, dict):
            raise ValueError("定时配置必须是对象")
        trigger_type = str(spec.get("type") or "").strip().lower()
        # 兼容 preset 快捷：{"preset":"hourly"} → cron
        preset = str(spec.get("preset") or "").strip().lower()
        if preset:
            presets = {
                "hourly": {"type": "cron", "fields": {"minute": "0", "second": "0"}},
                "every_hour": {"type": "cron", "fields": {"minute": "0", "second": "0"}},
                "every_2h": {"type": "cron", "fields": {"hour": "*/2", "minute": "0", "second": "0"}},
                "every_4h": {"type": "cron", "fields": {"hour": "*/4", "minute": "0", "second": "0"}},
                "every_6h": {"type": "cron", "fields": {"hour": "*/6", "minute": "0", "second": "0"}},
                "every_12h": {"type": "cron", "fields": {"hour": "*/12", "minute": "0", "second": "0"}},
                "daily": {"type": "cron", "fields": {"hour": "0", "minute": "0", "second": "0"}},
                "every_day": {"type": "cron", "fields": {"hour": "0", "minute": "0", "second": "0"}},
                "daily_8": {"type": "cron", "fields": {"hour": "8", "minute": "0", "second": "0"}},
                "daily_12": {"type": "cron", "fields": {"hour": "12", "minute": "0", "second": "0"}},
                "weekly": {"type": "cron", "fields": {"day_of_week": "mon", "hour": "0", "minute": "0", "second": "0"}},
                "every_week": {"type": "cron", "fields": {"day_of_week": "mon", "hour": "0", "minute": "0", "second": "0"}},
                "every_10m": {"type": "interval", "seconds": 600},
                "every_30m": {"type": "interval", "seconds": 1800},
                "every_1h_interval": {"type": "interval", "seconds": 3600},
            }
            if preset not in presets:
                raise ValueError(f"不支持的快捷计划：{preset}")
            return cls._build_trigger(presets[preset], timezone=timezone)

        if trigger_type == "cron":
            raw_fields = spec.get("fields")
            # 也支持 expression: "分 时 日 月 周" 五段
            expression = str(spec.get("expression") or "").strip()
            if expression and not isinstance(raw_fields, dict):
                parts = expression.split()
                if len(parts) != 5:
                    raise ValueError("cron 表达式需 5 段：分 时 日 月 周")
                raw_fields = {
                    "minute": parts[0],
                    "hour": parts[1],
                    "day": parts[2],
                    "month": parts[3],
                    "day_of_week": parts[4],
                    "second": str(spec.get("second") or "0"),
                }
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
            # 未指定 second 时默认 0，避免每秒触发
            fields.setdefault("second", "0")
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
        job_id = str(job.id)
        raw_name = str(job.name or job.id)
        title = _humanize_job_id(job_id)
        # 若 APScheduler name 已是中文且与 id 不同，优先用 name
        if raw_name and raw_name != job_id and any("\u4e00" <= ch <= "\u9fff" for ch in raw_name):
            title = raw_name
        trigger = self._serialize_trigger(job.trigger)
        data = {
            "id": job_id,
            "name": title,
            "raw_name": raw_name,
            "title": title,
            "enabled": job.next_run_time is not None,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": trigger,
            "schedule_text": trigger.get("summary") or trigger.get("description") or "",
            "max_instances": int(job.max_instances),
            "coalesce": bool(job.coalesce),
        }
        last_run = self._last_runs.get(job_id)
        data["last_run"] = dict(last_run) if last_run else None
        return data

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
            if (
                manual_id in self._manual_jobs
                or self._scheduler.get_job(manual_id) is not None
            ):
                raise ValueError("该任务已有一次手动执行正在排队或运行")
            run_id = uuid4().hex
            while len(self._runs) >= _RUN_HISTORY_LIMIT:
                self._runs.pop(next(iter(self._runs)))
            run = {
                "run_id": run_id,
                "job_id": str(job.id),
                "status": "queued",
                "queued_at": self._now(),
                "started_at": None,
                "finished_at": None,
                "error": None,
            }
            self._runs[run_id] = run
            self._manual_jobs[manual_id] = run_id
            try:
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
            except Exception:
                self._manual_jobs.pop(manual_id, None)
                self._runs.pop(run_id, None)
                raise
            return {
                "id": str(job.id),
                "queued": True,
                "run_id": run_id,
                "status": "queued",
            }

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            run = self._runs.get(str(run_id))
            if run is None:
                raise ValueError("手动执行记录不存在")
            return dict(run)

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
