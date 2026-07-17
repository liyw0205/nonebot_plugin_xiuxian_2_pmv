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
    # 鬼市 / 拍卖
    "auto_guishi_transactions": "鬼市自动交易",
    "auto_guishi_transactions_job": "鬼市自动交易",
    "clear_expired_baitan_orders": "清理超时摆摊",
    "clear_expired_baitan_orders_job": "清理超时摆摊",
    "auto_start_auction": "自动开启拍卖",
    "auto_start_auction_job": "自动开启拍卖",
    "check_auction_end": "拍卖收尾检查",
    "check_auction_end_job": "拍卖收尾检查",
    # 宗门 / 傀儡
    "materialsupdate_": "发放宗门资材",
    "materialsupdate": "发放宗门资材",
    "sect_materials_grant": "发放宗门资材",
    "auto_harvest": "灵田傀儡自动收取",
    "auto_harvest_scheduled": "灵田傀儡自动收取",
    "generate_all_bosses": "自动生成世界BOSS",
    "generate_all_bosses_task": "自动生成世界BOSS",
    "daily_dungeon_reset": "每日副本重置",
    # 世界事件
    "demon_invasion_schedule": "魔修入侵开关",
    "demon_invasion_schedule_job": "魔修入侵开关",
    "demon_invasion_refresh_schedule": "魔修入侵刷新",
    "demon_invasion_refresh_schedule_job": "魔修入侵刷新",
    "spirit_vein_schedule": "灵脉争夺",
    "spirit_vein_schedule_job": "灵脉争夺",
    # 系统限流 / 体力
    "reset_message_rate_limits": "重置消息频率限制",
    "limit_all_message_": "重置消息频率限制",
    "recover_user_stamina": "体力恢复",
    "limit_all_stamina_": "体力恢复",
    # 日常重置
    "daily_reset_sign": "每日签到重置",
    "daily_reset_beg": "仙途奇缘重置",
    "daily_reset_day_num": "丹药次数重置",
    "daily_reset_mixelixir_num": "炼丹次数重置",
    "daily_reset_impart_num": "传承抽卡重置",
    "daily_reset_arena": "竞技场每日重置",
    "weekly_reduce_arena_rank": "竞技场周排名衰减",
    "daily_reset_lottery": "抽奖次数重置",
    "daily_reset_stone_limits": "灵石获取上限重置",
    "daily_reset_xiangyuan": "香缘重置",
    "daily_reset_boss_limits": "Boss次数重置",
    "daily_reset_two_exp": "双修次数重置",
    "daily_reset_impart_pk": "传承对决重置",
    "daily_clean_expired_items": "清理过期物品",
    "cleanup_media_parser_cache_job": "清理媒体解析缓存",
    "weekly_reduce_impart_lv": "传承等级周衰减",
    "weekly_reset_tower_floors": "通天塔周重置",
    "daily_add_impart_lv": "传承等级日增长",
    "daily_reset_illusion": "幻境重置",
    "daily_reset_sect_task": "宗门任务重置",
    "daily_reset_work_refresh_num": "悬赏令次数重置",
    "scheduled_rift_generation_job": "秘境重置",
    "auto_handle_inactive_sect_owners_job": "处理不活跃宗主",
    "reset_data_by_time_job": "早晚数据重置",
    "backup_database_files": "数据库备份",
    "newapi_auto_checkin_daily": "NewAPI 自动签到",
}


def _looks_like_uuid(value: str) -> bool:
    text = str(value or "").strip().lower().replace("-", "")
    return len(text) == 32 and all(ch in "0123456789abcdef" for ch in text)


def _humanize_job_id(job_id: str) -> str:
    text = str(job_id or "").strip()
    if not text:
        return "未命名任务"
    # 先直接查表（含函数名 materialsupdate_ / limit_all_stamina_）
    if text in _JOB_TITLES:
        return _JOB_TITLES[text]
    # 去掉常见后缀再查
    base = text
    for suffix in ("_job", "_scheduled", "_task", "_"):
        if base.endswith(suffix) and len(base) > len(suffix):
            candidate = base[: -len(suffix)]
            if candidate in _JOB_TITLES:
                return _JOB_TITLES[candidate]
            base = candidate
    if base in _JOB_TITLES:
        return _JOB_TITLES[base]
    # UUID 本身不当标题
    if _looks_like_uuid(text):
        return ""
    # snake_case 可读化
    pretty = base.replace("-", " ").replace("_", " ").strip()
    replacements = (
        ("daily reset ", "每日重置·"),
        ("weekly reset ", "每周重置·"),
        ("daily ", "每日·"),
        ("weekly ", "每周·"),
        ("auto ", "自动·"),
        ("reset ", "重置·"),
        ("check ", "检查·"),
        ("generate all ", "生成·"),
    )
    lower = pretty.lower()
    for old, new in replacements:
        if lower.startswith(old):
            pretty = new + pretty[len(old):]
            break
    return pretty or text


def _resolve_job_title(job_id: str, raw_name: str = "", func_name: str = "") -> str:
    """按 id → 函数名 → APScheduler name 解析中文标题，避免 UUID 裸奔。"""
    candidates = [job_id, func_name, raw_name]
    for candidate in candidates:
        if not candidate:
            continue
        title = _humanize_job_id(str(candidate))
        if not title:
            continue
        # 跳过仍是 UUID / 空
        if _looks_like_uuid(title):
            continue
        if title in {"未命名任务", "未命名定时任务"}:
            continue
        # 若标题几乎等于原始英文 id，继续尝试更好候选
        if title.replace(" ", "_").replace("·", "_") in {
            str(candidate),
            str(candidate).replace("-", "_"),
        } and candidate not in _JOB_TITLES:
            # 可能是可读化英文，先记下，后面没更好的再用
            continue
        if any("\u4e00" <= ch <= "\u9fff" for ch in title) or candidate in _JOB_TITLES:
            return title
    # 第二轮：接受可读英文/中文混合
    for candidate in candidates:
        if not candidate or _looks_like_uuid(str(candidate)):
            continue
        title = _humanize_job_id(str(candidate))
        if title and not _looks_like_uuid(title):
            return title
    # 最后：用函数名拼一个不丢人的标题
    for candidate in (func_name, raw_name, job_id):
        if candidate and not _looks_like_uuid(str(candidate)):
            return f"定时任务·{candidate}"
    return "后台定时任务"


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

        def _norm(value: str) -> str:
            text = str(value or "*").strip()
            return "*" if text in {"*", "*/1"} else text

        minute = _norm(f.get("minute", "*"))
        hour = _norm(f.get("hour", "*"))
        day = _norm(f.get("day", "*"))
        month = _norm(f.get("month", "*"))
        dow = _norm(f.get("day_of_week", "*"))
        second = _norm(f.get("second", "0"))
        if second in {"0", "00"}:
            second = "0"

        # 每小时 / 每小时的第 N 分
        if hour == "*" and minute.isdigit() and day == "*" and month == "*" and dow == "*":
            if minute in {"0", "00"}:
                return "每小时"
            return f"每小时的第 {int(minute)} 分"

        # 每 N 小时 / 每 N 小时的第 M 分  （如 时=*/4 分=10）
        if hour.startswith("*/") and day == "*" and month == "*" and dow == "*":
            try:
                n = int(hour[2:])
            except ValueError:
                n = 0
            if n > 0:
                if minute in {"0", "00", "*"}:
                    return f"每 {n} 小时"
                if minute.isdigit():
                    return f"每 {n} 小时的第 {int(minute)} 分"

        # 每天 HH:MM
        if hour.isdigit() and minute.isdigit() and day == "*" and month == "*" and dow == "*":
            return f"每天 {int(hour):02d}:{int(minute):02d}"

        # 每天多个整点：如 0,12
        if (
            all(part.isdigit() for part in hour.split(","))
            and minute.isdigit()
            and day == "*"
            and month == "*"
            and dow == "*"
        ):
            hours = ",".join(f"{int(part):02d}" for part in hour.split(","))
            return f"每天 {hours}:{int(minute):02d}"

        # 每小时段：如 8-22 的第 30 分
        if "-" in hour and minute.isdigit() and day == "*" and month == "*" and dow == "*":
            return f"每小时 {hour} 点段的第 {int(minute)} 分"

        # 每周 X HH:MM
        if dow != "*" and hour.isdigit() and minute.isdigit():
            dow_map = {
                "mon": "周一", "tue": "周二", "wed": "周三", "thu": "周四",
                "fri": "周五", "sat": "周六", "sun": "周日",
                "0": "周一", "1": "周二", "2": "周三", "3": "周四",
                "4": "周五", "5": "周六", "6": "周日", "7": "周日",
            }
            label = dow_map.get(str(dow).lower(), f"周{dow}")
            return f"每{label} {int(hour):02d}:{int(minute):02d}"

        # 兜底：尽量自然
        parts = []
        if month != "*":
            parts.append(f"每年 {month} 月")
        if day != "*":
            parts.append(f"{day} 日")
        if dow != "*":
            parts.append(f"周 {dow}")
        if hour != "*":
            parts.append(f"{hour} 时")
        if minute != "*":
            parts.append(f"{minute} 分")
        if second not in {"0", "*"}:
            parts.append(f"{second} 秒")
        return "、".join(parts) if parts else "自定义计划"

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
        func_name = ""
        try:
            func = getattr(job, "func", None)
            func_name = str(getattr(func, "__name__", "") or "")
        except Exception:
            func_name = ""

        title = _resolve_job_title(job_id, raw_name=raw_name, func_name=func_name)
        # 若 APScheduler name 已是中文且与 id 不同，优先用 name
        if raw_name and raw_name != job_id and any("\u4e00" <= ch <= "\u9fff" for ch in raw_name):
            title = raw_name

        trigger = self._serialize_trigger(job.trigger)
        data = {
            "id": job_id,
            "name": title,
            "raw_name": raw_name,
            "func_name": func_name,
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
