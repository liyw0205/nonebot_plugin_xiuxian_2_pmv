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


ACTIVITY_TEMPLATE_DEFINITIONS = {
    "festival_sign": {
        "name": "通用节日签到",
        "description": "适合多数短期节日活动。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "festival_sign",
            "enabled": True,
            "festival_name": "XX节日",
            "name": "XX节日签到活动",
            "description": "节日期间每日签到领取奖励，累计签到达到指定天数可领取额外奖励。",
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
            "extra_rules": [],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "spring_festival": {
        "name": "春节签到",
        "description": "适合春节、元宵等新春周期。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "spring_festival",
            "enabled": True,
            "festival_name": "春节",
            "name": "春节签到活动",
            "description": "新春期间每日签到领取年礼，累计签到可获得额外红包奖励。",
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
            "extra_rules": ["春节主题奖励可按服务器经济情况调整"],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "anniversary": {
        "name": "周年庆签到",
        "description": "适合服庆、版本周年和长期纪念活动。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "anniversary",
            "enabled": True,
            "festival_name": "周年庆",
            "name": "周年庆签到活动",
            "description": "周年庆期间每日签到领取庆典补给，累计签到可获得纪念奖励。",
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
            "extra_rules": ["后续可追加纪念称号、抽奖或兑换入口"],
            "extensions": {"repeat_last_daily_reward": True},
        },
    },
    "rank_warmup": {
        "name": "冲榜预热签到",
        "description": "适合大型活动开始前的预热签到。",
        "config": {
            "template_type": "festival_sign",
            "template_key": "rank_warmup",
            "enabled": True,
            "festival_name": "冲榜预热",
            "name": "冲榜预热签到活动",
            "description": "活动预热期每日签到领取基础补给，正式玩法细节后续开放。",
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
