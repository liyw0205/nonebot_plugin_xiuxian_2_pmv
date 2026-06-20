from copy import deepcopy
from datetime import datetime

from .core import *  # noqa: F401,F403
from ..xiuxian_activity.service import (
    CONFIG_PATH as ACTIVITY_CONFIG_PATH,
    activity_state,
    load_config as load_activity_config,
    parse_reward,
    save_config as save_activity_config,
)


TASK_TEMPLATES = {
    "daily_sign": {
        "key": "daily_sign",
        "name": "今日问道",
        "description": "完成修仙签到 1 次",
        "target": 1,
        "events": ["sign_in"],
    },
    "daily_out_closing": {
        "key": "daily_out_closing",
        "name": "闭关归元",
        "description": "完成出关或虚神界出关 1 次",
        "target": 1,
        "events": ["out_closing", "xu_out_closing"],
    },
    "daily_work": {
        "key": "daily_work",
        "name": "悬赏历练",
        "description": "结算悬赏令 1 次",
        "target": 1,
        "events": ["work"],
    },
    "daily_boss": {
        "key": "daily_boss",
        "name": "斩妖除魔",
        "description": "讨伐世界BOSS 1 次",
        "target": 1,
        "events": ["boss"],
    },
    "daily_sect_task_complete": {
        "key": "daily_sect_task_complete",
        "name": "同门小务",
        "description": "完成宗门任务 1 次",
        "target": 1,
        "events": ["sect_task_complete"],
    },
    "daily_pet_travel_claim": {
        "key": "daily_pet_travel_claim",
        "name": "灵宠归来",
        "description": "领取宠物游历 1 次",
        "target": 1,
        "events": ["pet_travel_claim"],
    },
    "daily_dongfu_harvest": {
        "key": "daily_dongfu_harvest",
        "name": "洞府经营",
        "description": "完成洞府收获 1 次",
        "target": 1,
        "events": ["dongfu_harvest"],
    },
    "daily_map_mission_complete": {
        "key": "daily_map_mission_complete",
        "name": "寻踪问路",
        "description": "完成地图委托 1 次",
        "target": 1,
        "events": ["map_mission_complete"],
    },
    "weekly_sign": {
        "key": "weekly_sign",
        "name": "七日勤修",
        "description": "本周完成修仙签到 6 次",
        "target": 6,
        "events": ["sign_in"],
    },
    "weekly_out_closing": {
        "key": "weekly_out_closing",
        "name": "道心不辍",
        "description": "本周累计修炼、出关或虚神界出关 7200 分钟",
        "target": 7200,
        "events": ["cultivation_time", "out_closing", "xu_out_closing"],
    },
    "weekly_work": {
        "key": "weekly_work",
        "name": "悬赏达人",
        "description": "本周结算悬赏令 25 次",
        "target": 25,
        "events": ["work"],
    },
    "weekly_boss": {
        "key": "weekly_boss",
        "name": "伏魔周行",
        "description": "本周讨伐世界BOSS 150 次",
        "target": 150,
        "events": ["boss"],
    },
    "weekly_sect_task_complete": {
        "key": "weekly_sect_task_complete",
        "name": "宗门勤务",
        "description": "本周完成宗门任务 15 次",
        "target": 15,
        "events": ["sect_task_complete"],
    },
    "weekly_map_mission_complete": {
        "key": "weekly_map_mission_complete",
        "name": "山河踏遍",
        "description": "本周完成地图委托 20 次",
        "target": 20,
        "events": ["map_mission_complete"],
    },
    "weekly_elixir_or_dongfu": {
        "key": "weekly_elixir_or_dongfu",
        "name": "炼丹不辍",
        "description": "本周炼丹或洞府收获累计 20 次",
        "target": 20,
        "events": ["dongfu_harvest", "mix_elixir_complete"],
    },
    "weekly_dungeon_clear": {
        "key": "weekly_dungeon_clear",
        "name": "组队试炼",
        "description": "本周完成副本 10 次",
        "target": 10,
        "events": ["dungeon_clear"],
    },
}


def _task_row(key: str, reward: str, *, target: int | None = None, name: str | None = None) -> dict:
    task = deepcopy(TASK_TEMPLATES[key])
    if target is not None:
        task["target"] = target
    if name:
        task["name"] = name
    task["reward"] = reward
    return task


ACTIVITY_TEMPLATE_DEFINITIONS = {
    "festival_sign": {
        "name": "通用节日签到",
        "description": "适合多数短期节日活动，含基础日常与周常目标。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "festival_sign",
            "enabled": True,
            "festival_name": "XX节日",
            "name": "XX节日签到活动",
            "description": "节日期间每日签到领取奖励，完成活动日常与周常目标可获得额外补给。",
            "start_time": "0",
            "end_time": "无限",
            "sign_command": "活动签到",
            "daily_rewards": [
                {"day": 1, "name": "首日签到", "reward": "灵石x50000"},
                {"day": 2, "name": "次日签到", "reward": "灵石x80000"},
                {"day": 3, "name": "三日签到", "reward": "灵石x100000"},
                {"day": 4, "name": "四日签到", "reward": "灵石x120000"},
                {"day": 5, "name": "五日签到", "reward": "灵石x150000"},
                {"day": 6, "name": "六日签到", "reward": "灵石x180000"},
                {"day": 7, "name": "七日签到", "reward": "灵石x200000,渡厄丹x1"},
            ],
            "milestone_rewards": [
                {"days": 3, "name": "累计三日", "reward": "灵石x150000"},
                {"days": 7, "name": "累计七日", "reward": "灵石x500000,渡厄丹x1"},
            ],
            "daily_tasks": [
                _task_row("daily_sign", "灵石x50000"),
                _task_row("daily_work", "灵石x60000"),
                _task_row("daily_dongfu_harvest", "灵石x60000"),
            ],
            "weekly_tasks": [
                _task_row("weekly_sign", "灵石x200000"),
                _task_row("weekly_work", "灵石x300000"),
            ],
            "extra_rules": [],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "spring_festival": {
        "name": "春节签到",
        "description": "适合春节、元宵等新春周期，偏签到、宗门和洞府经营。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "spring_festival",
            "enabled": True,
            "festival_name": "春节",
            "name": "春节签到活动",
            "description": "新春期间每日签到领取年礼，完成迎春节日任务可获得额外红包奖励。",
            "start_time": "0",
            "end_time": "无限",
            "sign_command": "活动签到",
            "daily_rewards": [
                {"day": 1, "name": "迎春礼", "reward": "灵石x100000"},
                {"day": 2, "name": "纳福礼", "reward": "灵石x120000"},
                {"day": 3, "name": "开运礼", "reward": "灵石x150000"},
                {"day": 4, "name": "贺岁礼", "reward": "灵石x180000"},
                {"day": 5, "name": "团圆礼", "reward": "灵石x200000"},
                {"day": 6, "name": "鸿运礼", "reward": "灵石x250000"},
                {"day": 7, "name": "新春大礼", "reward": "灵石x300000,渡厄丹x1"},
            ],
            "milestone_rewards": [
                {"days": 3, "name": "三日红包", "reward": "灵石x200000"},
                {"days": 7, "name": "七日年礼", "reward": "灵石x800000,渡厄丹x2"},
            ],
            "daily_tasks": [
                _task_row("daily_sign", "灵石x80000", name="新春问道"),
                _task_row("daily_sect_task_complete", "灵石x100000", name="同门拜年"),
                _task_row("daily_dongfu_harvest", "灵石x100000", name="洞府纳福"),
                _task_row("daily_pet_travel_claim", "灵石x80000", name="灵宠迎春"),
            ],
            "weekly_tasks": [
                _task_row("weekly_sign", "灵石x500000", name="七日迎春"),
                _task_row("weekly_sect_task_complete", "灵石x600000", name="宗门贺岁"),
                _task_row("weekly_elixir_or_dongfu", "灵石x600000,渡厄丹x1", name="炉火添福"),
            ],
            "extra_rules": ["春节主题奖励可按服务器经济情况调整"],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "anniversary": {
        "name": "周年庆签到",
        "description": "适合服庆、版本周年和长期纪念活动，任务覆盖更完整。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "anniversary",
            "enabled": True,
            "festival_name": "周年庆",
            "name": "周年庆签到活动",
            "description": "周年庆期间每日签到领取庆典补给，完成纪念任务可获得额外奖励。",
            "start_time": "0",
            "end_time": "无限",
            "sign_command": "活动签到",
            "daily_rewards": [
                {"day": 1, "name": "庆典补给一", "reward": "灵石x150000"},
                {"day": 2, "name": "庆典补给二", "reward": "灵石x180000"},
                {"day": 3, "name": "庆典补给三", "reward": "灵石x200000"},
                {"day": 4, "name": "庆典补给四", "reward": "灵石x220000"},
                {"day": 5, "name": "庆典补给五", "reward": "灵石x250000"},
                {"day": 6, "name": "庆典补给六", "reward": "灵石x280000"},
                {"day": 7, "name": "周年纪念礼", "reward": "灵石x500000,渡厄丹x1"},
            ],
            "milestone_rewards": [
                {"days": 5, "name": "五日纪念", "reward": "灵石x300000"},
                {"days": 7, "name": "周年礼盒", "reward": "灵石x1000000,渡厄丹x2"},
            ],
            "daily_tasks": [
                _task_row("daily_sign", "灵石x100000", name="庆典问道"),
                _task_row("daily_work", "灵石x120000", name="庆典悬赏"),
                _task_row("daily_boss", "灵石x150000", name="庆典伏魔"),
                _task_row("daily_map_mission_complete", "灵石x120000", name="山河巡礼"),
                _task_row("daily_dongfu_harvest", "灵石x100000", name="洞府补给"),
            ],
            "weekly_tasks": [
                _task_row("weekly_sign", "灵石x800000", name="周年勤修"),
                _task_row("weekly_work", "灵石x1000000", name="悬赏庆典"),
                _task_row("weekly_boss", "灵石x1200000,渡厄丹x1", name="伏魔庆典", target=100),
                _task_row("weekly_map_mission_complete", "灵石x1000000", name="山河庆典"),
            ],
            "extra_rules": ["后续可追加纪念称号、抽奖或兑换入口"],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "rank_warmup": {
        "name": "冲榜预热签到",
        "description": "适合大型活动开始前预热，任务偏悬赏、BOSS 和地图委托。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "rank_warmup",
            "enabled": True,
            "festival_name": "冲榜预热",
            "name": "冲榜预热签到活动",
            "description": "活动预热期每日签到领取基础补给，通过悬赏、伏魔和地图委托提前积累活跃。",
            "start_time": "0",
            "end_time": "无限",
            "sign_command": "活动签到",
            "daily_rewards": [
                {"day": 1, "name": "预热补给一", "reward": "灵石x50000"},
                {"day": 2, "name": "预热补给二", "reward": "灵石x60000"},
                {"day": 3, "name": "预热补给三", "reward": "灵石x70000"},
                {"day": 4, "name": "预热补给四", "reward": "灵石x80000"},
                {"day": 5, "name": "预热补给五", "reward": "灵石x100000"},
            ],
            "milestone_rewards": [
                {"days": 3, "name": "预热三日", "reward": "灵石x100000"},
                {"days": 5, "name": "预热五日", "reward": "灵石x200000,渡厄丹x1"},
            ],
            "daily_tasks": [
                _task_row("daily_work", "灵石x50000", name="预热悬赏"),
                _task_row("daily_boss", "灵石x60000", name="预热伏魔"),
                _task_row("daily_map_mission_complete", "灵石x50000", name="预热委托"),
            ],
            "weekly_tasks": [
                _task_row("weekly_work", "灵石x250000", name="悬赏蓄势", target=15),
                _task_row("weekly_boss", "灵石x300000", name="伏魔蓄势", target=80),
                _task_row("weekly_map_mission_complete", "灵石x250000", name="山河蓄势", target=12),
            ],
            "extra_rules": ["正式冲榜规则、积分来源和兑换内容后续补充"],
            "extensions": {"repeat_last_daily_reward": False},
        },
    },
}


def _serialize_templates():
    return {
        key: {
            "key": key,
            "name": value["name"],
            "description": value["description"],
            "config": deepcopy(value["config"]),
        }
        for key, value in ACTIVITY_TEMPLATE_DEFINITIONS.items()
    }


def _clean_text(value, default: str = "") -> str:
    text = str(value if value is not None else "").strip()
    return text or default


def _as_bool(value, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in ("true", "1", "yes", "on", "开启"):
        return True
    if text in ("false", "0", "no", "off", "关闭"):
        return False
    return default


def _validate_time_text(value: str, field_name: str):
    text = _clean_text(value)
    if text in ("", "0", "无限"):
        return
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            datetime.strptime(text, fmt)
            return
        except ValueError:
            pass
    raise ValueError(f"{field_name}格式应为 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS")


def _to_positive_int(value, field_name: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{field_name}必须是正整数")
    if number <= 0:
        raise ValueError(f"{field_name}必须是正整数")
    return number


def _normalize_reward_rows(rows, number_key: str, number_label: str) -> list[dict]:
    normalized = []
    seen_numbers = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        reward = _clean_text(row.get("reward"))
        name = _clean_text(row.get("name"))
        raw_number = row.get(number_key)
        if raw_number in (None, "") and not reward and not name:
            continue
        number = _to_positive_int(raw_number, number_label)
        if number in seen_numbers:
            raise ValueError(f"{number_label}{number}重复")
        seen_numbers.add(number)
        parse_reward(reward)
        normalized.append({
            number_key: number,
            "name": name or f"{number_label}{number}",
            "reward": reward,
        })
    return sorted(normalized, key=lambda item: item[number_key])


def _normalize_events(value) -> list[str]:
    if isinstance(value, str):
        source = value.replace("\n", ",").split(",")
    elif isinstance(value, list):
        source = value
    else:
        source = []

    events = []
    seen_events = set()
    for item in source:
        event = _clean_text(item)
        if not event or event in seen_events:
            continue
        seen_events.add(event)
        events.append(event)
    return events


def _normalize_task_rows(rows, cycle_label: str) -> list[dict]:
    normalized = []
    seen_keys = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        key = _clean_text(row.get("key"))
        name = _clean_text(row.get("name"))
        description = _clean_text(row.get("description") or row.get("desc"))
        reward = _clean_text(row.get("reward"))
        events = _normalize_events(row.get("events"))
        raw_target = row.get("target")

        if not any((key, name, description, reward, events)):
            continue
        if not key:
            raise ValueError(f"{cycle_label}任务ID不能为空")
        if key in seen_keys:
            raise ValueError(f"{cycle_label}任务ID {key} 重复")
        seen_keys.add(key)
        target = _to_positive_int(raw_target, f"{cycle_label}任务目标")
        if reward:
            parse_reward(reward)
        normalized.append({
            "key": key,
            "name": name or key,
            "description": description,
            "target": target,
            "events": events,
            "reward": reward,
        })
    return normalized


def _normalize_extra_rules(value) -> list[str]:
    if isinstance(value, str):
        source = value.splitlines()
    elif isinstance(value, list):
        source = value
    else:
        source = []
    return [_clean_text(item) for item in source if _clean_text(item)]


def _as_reward_rows(value, number_key: str) -> list[dict]:
    rows = []
    if not isinstance(value, list):
        return rows
    for item in value:
        if not isinstance(item, dict):
            continue
        rows.append({
            number_key: item.get(number_key, ""),
            "name": item.get("name", ""),
            "reward": item.get("reward", ""),
        })
    return rows


def _as_task_rows(value) -> list[dict]:
    rows = []
    if not isinstance(value, list):
        return rows
    for item in value:
        if not isinstance(item, dict):
            continue
        rows.append({
            "key": item.get("key", ""),
            "name": item.get("name", ""),
            "description": item.get("description", item.get("desc", "")),
            "target": item.get("target", ""),
            "events": _normalize_events(item.get("events")),
            "reward": item.get("reward", ""),
        })
    return rows


def _prepare_activity_config(config: dict) -> dict:
    if not isinstance(config, dict):
        config = {}

    extensions = config.get("extensions") if isinstance(config.get("extensions"), dict) else {}
    prepared = {
        "template_type": _clean_text(config.get("template_type"), "festival_sign"),
        "template_key": _clean_text(config.get("template_key"), config.get("template_type") or "festival_sign"),
        "enabled": _as_bool(config.get("enabled")),
        "festival_name": _clean_text(config.get("festival_name"), "XX节日"),
        "name": _clean_text(config.get("name"), "XX节日签到活动"),
        "description": _clean_text(config.get("description")),
        "start_time": _clean_text(config.get("start_time"), "0"),
        "end_time": _clean_text(config.get("end_time"), "无限"),
        "sign_command": _clean_text(config.get("sign_command"), "活动签到"),
        "daily_rewards": _as_reward_rows(config.get("daily_rewards"), "day"),
        "milestone_rewards": _as_reward_rows(config.get("milestone_rewards"), "days"),
        "daily_tasks": _as_task_rows(config.get("daily_tasks")),
        "weekly_tasks": _as_task_rows(config.get("weekly_tasks")),
        "extra_rules": _normalize_extra_rules(config.get("extra_rules")),
        "extensions": {
            "repeat_last_daily_reward": _as_bool(extensions.get("repeat_last_daily_reward"), True),
        },
    }
    return prepared


def _normalize_activity_config(data: dict) -> dict:
    if not isinstance(data, dict):
        raise ValueError("活动配置数据无效")

    extensions = data.get("extensions") if isinstance(data.get("extensions"), dict) else {}
    extensions = dict(extensions)
    extensions["repeat_last_daily_reward"] = _as_bool(extensions.get("repeat_last_daily_reward"), True)

    start_time = _clean_text(data.get("start_time"), "0")
    end_time = _clean_text(data.get("end_time"), "无限")
    _validate_time_text(start_time, "开始时间")
    _validate_time_text(end_time, "结束时间")

    daily_rewards = _normalize_reward_rows(data.get("daily_rewards"), "day", "第几天")
    milestone_rewards = _normalize_reward_rows(data.get("milestone_rewards"), "days", "累计天数")
    daily_tasks = _normalize_task_rows(data.get("daily_tasks"), "每日活动")
    weekly_tasks = _normalize_task_rows(data.get("weekly_tasks"), "周常活动")
    if not daily_rewards:
        raise ValueError("至少需要配置一条每日签到奖励")

    template_key = _clean_text(data.get("template_key"), "custom")
    template_type = _clean_text(data.get("template_type"), "festival_sign")

    return {
        "template_type": template_type,
        "template_key": template_key,
        "enabled": _as_bool(data.get("enabled")),
        "festival_name": _clean_text(data.get("festival_name"), "XX节日"),
        "name": _clean_text(data.get("name"), "XX节日签到活动"),
        "description": _clean_text(data.get("description")),
        "start_time": start_time,
        "end_time": end_time,
        "sign_command": _clean_text(data.get("sign_command"), "活动签到"),
        "daily_rewards": daily_rewards,
        "milestone_rewards": milestone_rewards,
        "daily_tasks": daily_tasks,
        "weekly_tasks": weekly_tasks,
        "extra_rules": _normalize_extra_rules(data.get("extra_rules")),
        "extensions": extensions,
    }


@app.route("/activity")
def activity_management():
    if "admin_id" not in session:
        return redirect(url_for("login"))

    config = _prepare_activity_config(load_activity_config())
    ok, reason = activity_state(config)
    return render_template(
        "activity.html",
        activity_config=config,
        activity_templates=_serialize_templates(),
        activity_state={"ok": ok, "text": "进行中" if ok else reason},
        activity_config_path=str(ACTIVITY_CONFIG_PATH),
    )


@app.route("/api/activity/config", methods=["GET", "POST"])
def api_activity_config():
    if "admin_id" not in session:
        return api_error("未登录")

    if request.method == "GET":
        config = _prepare_activity_config(load_activity_config())
        ok, reason = activity_state(config)
        return api_success(
            config=config,
            templates=_serialize_templates(),
            state={"ok": ok, "text": "进行中" if ok else reason},
            config_path=str(ACTIVITY_CONFIG_PATH),
        )

    try:
        payload = request.get_json() or {}
        config = _normalize_activity_config(payload.get("config", payload))
        save_activity_config(config)
        ok, reason = activity_state(config)
        return api_success(
            message="活动配置已保存",
            config=config,
            state={"ok": ok, "text": "进行中" if ok else reason},
        )
    except Exception as e:
        return api_error(str(e))


@app.route("/api/activity/template/<template_key>")
def api_activity_template(template_key):
    if "admin_id" not in session:
        return api_error("未登录")

    template = ACTIVITY_TEMPLATE_DEFINITIONS.get(str(template_key))
    if not template:
        return api_error("活动模板不存在")
    return api_success(template=_serialize_templates()[template_key])
