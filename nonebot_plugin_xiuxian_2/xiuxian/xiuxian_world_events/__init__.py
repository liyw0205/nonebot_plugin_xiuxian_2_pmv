import random
from datetime import datetime, timedelta
from threading import RLock

from nonebot import require
from nonebot.log import logger
from nonebot.permission import SUPERUSER

from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.utils import (
    check_user,
    check_user_type,
    handle_send,
    log_message,
    number_to,
    send_help_message,
    send_msg_handler,
    update_statistics_value,
)
from ..xiuxian_utils.xiuxian2_handle import (
    PlayerDataManager,
    XiuxianDateManage,
    leave_harm_time,
)


scheduler = require("nonebot_plugin_apscheduler").scheduler
sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()

EVENT_TABLE = "world_event_state"
EVENT_KEY = "global"
SPIRIT_VEIN_EVENT_KEY = "spirit_vein"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
EVENT_START_HOUR = 18
EVENT_END_HOUR = 22
BOSS_REAL_HP_MULTIPLIER = 10000
MAX_SINGLE_DAMAGE_RATIO = 0.1
MAX_PURSUIT_DAMAGE_RATIO = 0.05
DEMON_ATTACK_LIMIT = 3
DEMON_STONE_REWARD_CAP = 50000000
SPIRIT_VEIN_TRIGGER_CHANCE = 0.10
SPIRIT_VEIN_MIN_DURATION = 30
SPIRIT_VEIN_MAX_DURATION = 180
SPIRIT_VEIN_EXP_BONUS_RATE = 0.20
SPIRIT_VEIN_TIANTI_BONUS_RATE = 0.20

REALM_LIST = [
    "感气境",
    "练气境",
    "筑基境",
    "结丹境",
    "金丹境",
    "元神境",
    "化神境",
    "炼神境",
    "返虚境",
    "大乘境",
    "虚道境",
    "斩我境",
    "遁一境",
    "至尊境",
    "微光境",
    "星芒境",
    "月华境",
    "耀日境",
    "祭道境",
    "自在境",
    "破虚境",
    "无界境",
    "混元境",
    "造化境",
    "永恒境",
]

DEMON_NAMES = [
    "血衣魔修",
    "噬魂魔君",
    "玄阴散人",
    "赤煞道人",
    "无相魔影",
    "黑莲魔尊",
]

_state_lock = RLock()


world_event_help = on_command("世界事件帮助", priority=5, block=True)
world_event_info = on_command("魔修入侵", aliases={"魔修入侵状态"}, priority=6, block=True)
spirit_vein_info = on_command("天降灵脉", aliases={"天降灵脉状态"}, priority=6, block=True)
start_demon_invasion = on_command("开启魔修入侵", aliases={"魔修入侵开启"}, permission=SUPERUSER, priority=5, block=True)
close_world_event = on_command("关闭魔修入侵", aliases={"魔修入侵关闭"}, permission=SUPERUSER, priority=5, block=True)
start_spirit_vein = on_command("开启天降灵脉", aliases={"天降灵脉开启"}, permission=SUPERUSER, priority=5, block=True)
close_spirit_vein = on_command("关闭天降灵脉", aliases={"天降灵脉关闭"}, permission=SUPERUSER, priority=5, block=True)
attack_demon_invasion = on_command("讨伐魔修", aliases={"攻击魔修", "魔修讨伐"}, priority=6, block=True)
claim_demon_reward = on_command("领取魔修奖励", priority=6, block=True)


__world_event_help__ = f"""
世界事件帮助

魔修入侵：
  ▶ 每日 {EVENT_START_HOUR}:00 至 {EVENT_END_HOUR}:00
  ▶ 讨伐魔修 - 按自身境界挑战对应境界魔修
  ▶ 领取魔修奖励 - 入侵结束或对应境界魔修被击退后按贡献领取奖励
  ▶ 每期最多讨伐{DEMON_ATTACK_LIMIT}次、领取一次
  ▶ 魔修被击退后可立即领奖，每小时30分会刷新已击退的魔修
  ▶ 灵石固定奖池{number_to(DEMON_STONE_REWARD_CAP)}，按贡献占比瓜分

天降灵脉：
  ▶ 每小时30分有{int(SPIRIT_VEIN_TRIGGER_CHANCE * 100)}%概率开启
  ▶ 每次持续{SPIRIT_VEIN_MIN_DURATION}-{SPIRIT_VEIN_MAX_DURATION}分钟
  ▶ 持续期间修炼、出关、虚神界出关获得修为+{int(SPIRIT_VEIN_EXP_BONUS_RATE * 100)}%
  ▶ 持续期间炼体结算获得炼体气血+{int(SPIRIT_VEIN_TIANTI_BONUS_RATE * 100)}%
  ▶ 天降灵脉 - 查看当前灵脉状态

规则：
  ▶ 每个境界会生成独立魔修。
  ▶ 战斗血条只用于对战，真实 BOSS 血条为战斗血条的 {BOSS_REAL_HP_MULTIPLIER} 倍。
  ▶ 伤害按真实削减血量记录贡献。
  ▶ 天降灵脉持续期间再次触发时会自动跳过，不刷新持续时间。
""".strip()


def _now() -> datetime:
    return datetime.now()


def _current_period() -> str:
    return _now().strftime("%Y-%m-%d")


def _format_time(value: datetime) -> str:
    return value.strftime(TIME_FORMAT)


def _parse_time(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in (TIME_FORMAT, "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def _to_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _load_state(event_key: str = EVENT_KEY) -> dict:
    state = player_data_manager.get_fields(event_key, EVENT_TABLE) or {}
    state.pop("user_id", None)
    state.setdefault("active", 0)
    state.setdefault("status", "idle")
    state.setdefault("event_id", "")
    state.setdefault("event_type", "demon_invasion")
    state.setdefault("name", "魔修入侵")
    state.setdefault("period", "")
    state.setdefault("manual", 0)
    state.setdefault("bosses", {})
    state.setdefault("participants", {})
    state.setdefault("claimed", {})
    state.setdefault("started_at", "")
    state.setdefault("ends_at", "")
    state.setdefault("last_result", "")
    if not isinstance(state.get("bosses"), dict):
        state["bosses"] = {}
    if not isinstance(state.get("participants"), dict):
        state["participants"] = {}
    if not isinstance(state.get("claimed"), dict):
        state["claimed"] = {}
    return state


def _save_state(state: dict, event_key: str = EVENT_KEY) -> None:
    fields = {
        "active": "INTEGER",
        "status": "TEXT",
        "event_id": "TEXT",
        "event_type": "TEXT",
        "name": "TEXT",
        "period": "TEXT",
        "manual": "INTEGER",
        "bosses": "TEXT",
        "participants": "TEXT",
        "claimed": "TEXT",
        "started_at": "TEXT",
        "ends_at": "TEXT",
        "last_result": "TEXT",
    }
    for field, data_type in fields.items():
        player_data_manager.update_or_write_data(
            event_key,
            EVENT_TABLE,
            field,
            state.get(field, "" if data_type == "TEXT" else 0),
            data_type=data_type,
        )


def _event_id(period: str) -> str:
    return f"demon_invasion:{period}"


def _spirit_vein_event_id(started_at: datetime) -> str:
    return f"spirit_vein:{started_at.strftime('%Y%m%d%H%M')}"


def _get_level_power(realm: str) -> int:
    level_data = jsondata.level_data()
    for suffix in ("中期", "初期", "圆满"):
        key = f"{realm}{suffix}"
        if key in level_data:
            return max(_to_int(level_data[key].get("power"), 1), 1)
    return 100000


def _normalize_realm(level: str) -> str:
    level = str(level or "")
    for suffix in ("初期", "中期", "后期", "圆满"):
        if level.endswith(suffix):
            return level[: -len(suffix)]
    if level in REALM_LIST:
        return level
    for realm in REALM_LIST:
        if level.startswith(realm):
            return realm
    return "感气境"


def _create_demon_boss(realm: str, wave: int = 1) -> dict:
    power = _get_level_power(realm)
    battle_hp = int(power * random.randint(32, 42))
    battle_mp = int(power * random.randint(6, 9))
    battle_atk = int(power * random.uniform(3.8, 5.2))
    real_hp = battle_hp * BOSS_REAL_HP_MULTIPLIER
    name = f"{random.choice(DEMON_NAMES)}·{realm}"
    return {
        "name": name,
        "jj": realm,
        "wave": max(1, int(wave)),
        "气血": battle_hp,
        "总血量": battle_hp,
        "真元": battle_mp,
        "攻击": battle_atk,
        "battle_hp": battle_hp,
        "battle_max_hp": battle_hp,
        "boss_hp": real_hp,
        "boss_max_hp": real_hp,
        "max_stone": DEMON_STONE_REWARD_CAP,
        "stone": DEMON_STONE_REWARD_CAP,
        "monster_type": "boss",
    }


def _create_all_bosses() -> dict:
    return {realm: _create_demon_boss(realm) for realm in REALM_LIST}


def _build_active_state(period: str | None = None, manual: bool = False) -> dict:
    period = period or _current_period()
    now = _now()
    return {
        "active": 1,
        "status": "active",
        "event_id": _event_id(period),
        "event_type": "demon_invasion",
        "name": "魔修入侵",
        "period": period,
        "manual": 1 if manual else 0,
        "bosses": _create_all_bosses(),
        "participants": {},
        "claimed": {},
        "started_at": _format_time(now.replace(hour=EVENT_START_HOUR, minute=0, second=0, microsecond=0)),
        "ends_at": _format_time(now.replace(hour=EVENT_END_HOUR, minute=0, second=0, microsecond=0)),
        "last_result": "",
    }


def _build_spirit_vein_state(duration_minutes: int | None = None, manual: bool = False) -> dict:
    now = _now()
    duration_minutes = duration_minutes or random.randint(SPIRIT_VEIN_MIN_DURATION, SPIRIT_VEIN_MAX_DURATION)
    duration_minutes = max(SPIRIT_VEIN_MIN_DURATION, min(int(duration_minutes), SPIRIT_VEIN_MAX_DURATION))
    ends_at = now + timedelta(minutes=duration_minutes)
    return {
        "active": 1,
        "status": "active",
        "event_id": _spirit_vein_event_id(now),
        "event_type": "spirit_vein",
        "name": "天降灵脉",
        "period": now.strftime("%Y-%m-%d"),
        "manual": 1 if manual else 0,
        "bosses": {},
        "participants": {},
        "claimed": {},
        "started_at": _format_time(now),
        "ends_at": _format_time(ends_at),
        "last_result": f"天降灵脉已开启，持续{duration_minutes}分钟。",
    }


def _is_auto_window(now: datetime | None = None) -> bool:
    now = now or _now()
    return EVENT_START_HOUR <= now.hour < EVENT_END_HOUR


def _ensure_spirit_vein_state(now: datetime | None = None) -> dict:
    now = now or _now()
    state = _load_state(SPIRIT_VEIN_EVENT_KEY)
    if state.get("event_type") != "spirit_vein" and not state.get("event_id"):
        state["event_type"] = "spirit_vein"
        state["name"] = "天降灵脉"

    ends_at = _parse_time(state.get("ends_at"))
    if state.get("status") == "active" and ends_at and now >= ends_at:
        state["active"] = 0
        state["status"] = "finished"
        state["last_result"] = f"天降灵脉已于{ends_at.strftime('%H:%M')}消散。"
        _save_state(state, SPIRIT_VEIN_EVENT_KEY)
    return state


def _is_spirit_vein_active(now: datetime | None = None) -> bool:
    now = now or _now()
    with _state_lock:
        state = _ensure_spirit_vein_state(now)
    ends_at = _parse_time(state.get("ends_at"))
    return state.get("status") == "active" and bool(ends_at) and now < ends_at


def get_spirit_vein_exp_multiplier() -> float:
    return 1 + SPIRIT_VEIN_EXP_BONUS_RATE if _is_spirit_vein_active() else 1.0


def get_spirit_vein_tianti_multiplier() -> float:
    return 1 + SPIRIT_VEIN_TIANTI_BONUS_RATE if _is_spirit_vein_active() else 1.0


def get_spirit_vein_exp_bonus_msg() -> str:
    if not _is_spirit_vein_active():
        return ""
    return f"\n天降灵脉加成：修为+{int(SPIRIT_VEIN_EXP_BONUS_RATE * 100)}%"


def get_spirit_vein_tianti_bonus_msg() -> str:
    if not _is_spirit_vein_active():
        return ""
    return f"\n天降灵脉加成：炼体气血+{int(SPIRIT_VEIN_TIANTI_BONUS_RATE * 100)}%"


def _build_spirit_vein_message(state: dict) -> str:
    state = _ensure_spirit_vein_state()
    if state.get("status") == "active":
        ends_at = _parse_time(state.get("ends_at"))
        now = _now()
        left_minutes = max(0, int(((ends_at or now) - now).total_seconds() // 60))
        return (
            "【天降灵脉】\n"
            "状态：进行中\n"
            f"开始时间：{state.get('started_at') or '未知'}\n"
            f"结束时间：{state.get('ends_at') or '未知'}\n"
            f"剩余时间：约{left_minutes}分钟\n"
            f"修为加成：+{int(SPIRIT_VEIN_EXP_BONUS_RATE * 100)}%\n"
            f"炼体加成：+{int(SPIRIT_VEIN_TIANTI_BONUS_RATE * 100)}%"
        )
    last_result = state.get("last_result") or "当前没有开启中的天降灵脉。"
    return (
        f"{last_result}\n"
        f"触发规则：每小时30分有{int(SPIRIT_VEIN_TRIGGER_CHANCE * 100)}%概率开启。\n"
        f"开启后持续{SPIRIT_VEIN_MIN_DURATION}-{SPIRIT_VEIN_MAX_DURATION}分钟。"
    )


def _try_start_auto_spirit_vein() -> tuple[dict, str]:
    now = _now()
    state = _ensure_spirit_vein_state(now)
    if state.get("status") == "active":
        return state, "天降灵脉仍在持续，本次触发检查自动跳过。"
    if random.random() >= SPIRIT_VEIN_TRIGGER_CHANCE:
        return state, "天降灵脉触发检查完成，本次未开启。"

    state = _build_spirit_vein_state()
    _save_state(state, SPIRIT_VEIN_EVENT_KEY)
    return state, f"天降灵脉已自动开启，持续至{state.get('ends_at')}。"


def _start_spirit_vein_manual(duration_minutes: int | None = None) -> dict:
    state = _build_spirit_vein_state(duration_minutes=duration_minutes, manual=True)
    _save_state(state, SPIRIT_VEIN_EVENT_KEY)
    return state


def _close_spirit_vein_manual() -> dict:
    state = _load_state(SPIRIT_VEIN_EVENT_KEY)
    state["active"] = 0
    state["status"] = "finished"
    state["event_type"] = "spirit_vein"
    state["name"] = "天降灵脉"
    state["manual"] = 1
    state["last_result"] = "天降灵脉已手动关闭。"
    _save_state(state, SPIRIT_VEIN_EVENT_KEY)
    return state


def _ensure_daily_state() -> dict:
    state = _load_state()
    period = _current_period()
    is_manual = bool(_to_int(state.get("manual"), 0))
    if _is_auto_window():
        if state.get("period") == period and is_manual and state.get("status") == "finished":
            return state
        if state.get("status") != "active" or state.get("period") != period:
            state = _build_active_state(period)
            _save_state(state)
    elif state.get("status") == "active" and not is_manual:
        state["active"] = 0
        state["status"] = "finished"
        state["last_result"] = "今日魔修入侵已于22:00结束。"
        _save_state(state)
    return state


def _format_participant_rank(participants: dict, realm: str | None = None, limit: int = 5) -> str:
    rows = []
    for item in participants.values():
        if realm and item.get("realm") != realm:
            continue
        rows.append(item)
    rows.sort(key=lambda item: _to_int(item.get("damage"), 0), reverse=True)
    if not rows:
        return "暂无贡献记录"

    lines = []
    for index, item in enumerate(rows[:limit], 1):
        name = item.get("name") or item.get("user_id") or "未知道友"
        damage = _to_int(item.get("damage"), 0)
        attacks = _to_int(item.get("attacks"), 0)
        lines.append(f"{index}. {name}：{number_to(damage)}真实伤害，出手{attacks}次")
    return "\n".join(lines)


def _get_user_boss(state: dict, user_info: dict, create_missing: bool = True) -> tuple[str, dict | None]:
    realm = _normalize_realm(user_info.get("level", ""))
    bosses = state.setdefault("bosses", {})
    boss_info = bosses.get(realm)
    if boss_info is None and create_missing and realm in REALM_LIST:
        boss_info = _create_demon_boss(realm)
        bosses[realm] = boss_info
    elif boss_info is not None:
        boss_info.setdefault("wave", 1)
    return realm, boss_info


def _build_state_message(state: dict, user_info: dict | None = None) -> str:
    status = state.get("status")
    if status == "idle":
        return f"当前没有开启中的魔修入侵。\n开启时间：每日{EVENT_START_HOUR}:00-{EVENT_END_HOUR}:00。"
    if status == "closed":
        return state.get("last_result") or "今日魔修入侵已结束。"

    lines = [
        f"【{state.get('name') or '魔修入侵'}】",
        f"状态：{'进行中' if status == 'active' else '已结束'}",
        f"时间：每日{EVENT_START_HOUR}:00-{EVENT_END_HOUR}:00",
        f"本期：{state.get('period') or _current_period()}",
        f"结束时间：{state.get('ends_at') or '22:00'}",
    ]

    if user_info:
        realm, boss_info = _get_user_boss(state, user_info, create_missing=status == "active")
        if boss_info:
            boss_hp = max(_to_int(boss_info.get("boss_hp"), 0), 0)
            boss_max_hp = max(_to_int(boss_info.get("boss_max_hp"), 0), 1)
            battle_hp = max(_to_int(boss_info.get("battle_hp", boss_info.get("气血")), 0), 0)
            battle_max_hp = max(_to_int(boss_info.get("battle_max_hp", boss_info.get("总血量")), 0), 1)
            lines.extend(
                [
                    "",
                    f"你的境界魔修：{boss_info.get('name', realm)}（第{max(_to_int(boss_info.get('wave'), 1), 1)}波）",
                    f"真实血条：{number_to(boss_hp)} / {number_to(boss_max_hp)}",
                    f"对战血条：{number_to(battle_hp)} / {number_to(battle_max_hp)}",
                    f"灵石固定奖池：{number_to(DEMON_STONE_REWARD_CAP)}",
                    "",
                    "本境界贡献排行：",
                    _format_participant_rank(state.get("participants", {}), realm),
                ]
            )

    if status == "active":
        lines.append("")
        lines.append("发送【讨伐魔修】参与战斗。")
    else:
        lines.append("")
        lines.append("发送【领取魔修奖励】领取本期贡献奖励。")
    return "\n".join(lines)


def _extract_total_damage(status_list: list, user_id: str) -> int:
    total_damage = 0
    for item in status_list or []:
        for _, stats in item.items():
            if stats.get("team_id") == 0 and str(stats.get("user_id")) == str(user_id):
                total_damage += max(0, _to_int(stats.get("total_dmg"), 0))
    return total_damage


def _participant_key(user_id: str, realm: str) -> str:
    return f"{realm}:{user_id}"


def _record_wave(record: dict | None) -> int:
    return max(_to_int((record or {}).get("wave"), 1), 1)


def _find_participant_record(state: dict, user_id: str, preferred_realm: str | None = None) -> tuple[str, dict | None]:
    participants = state.get("participants", {})
    if preferred_realm:
        record_key = _participant_key(user_id, preferred_realm)
        record = participants.get(record_key)
        if record:
            return record_key, record

    for record_key, record in participants.items():
        if str(record.get("user_id")) == str(user_id):
            return record_key, record
    return "", None


def _has_user_claimed(claimed: dict, user_id: str, record_key: str | None = None) -> bool:
    if record_key and claimed.get(record_key):
        return True
    suffix = f":{user_id}"
    for key, value in claimed.items():
        if value and str(key).endswith(suffix):
            return True
    return False


def _record_reward_ready(state: dict, record: dict | None) -> bool:
    if not record:
        return False
    if _to_int(record.get("reward_ready"), 0) == 1:
        return True

    realm = record.get("realm")
    boss_info = state.get("bosses", {}).get(realm)
    if not boss_info:
        return False
    return _record_wave(record) == max(_to_int(boss_info.get("wave"), 1), 1) and _to_int(boss_info.get("boss_hp"), 0) <= 0


def _record_participant(state: dict, user_info: dict, realm: str, wave: int, damage: int, killed: bool) -> None:
    user_id = str(user_info["user_id"])
    participants = state.setdefault("participants", {})
    record_key = _participant_key(user_id, realm)
    record = participants.get(record_key, {})
    record["user_id"] = user_id
    record["realm"] = realm
    record["wave"] = max(1, int(wave))
    record["name"] = user_info.get("user_name") or user_info.get("user_id") or user_id
    record["damage"] = _to_int(record.get("damage"), 0) + max(0, int(damage))
    record["attacks"] = _to_int(record.get("attacks"), 0) + 1
    if killed:
        record["last_hit"] = 1
    participants[record_key] = record


def _total_recorded_damage(participants: dict, realm: str, wave: int | None = None) -> int:
    return sum(
        max(0, _to_int(item.get("damage"), 0))
        for item in participants.values()
        if item.get("realm") == realm and (wave is None or _record_wave(item) == wave)
    )


def _mark_wave_reward_ready(state: dict, realm: str, wave: int) -> int:
    participants = state.setdefault("participants", {})
    total_damage = max(_total_recorded_damage(participants, realm, wave), 1)
    for item in participants.values():
        if item.get("realm") != realm or _record_wave(item) != wave:
            continue
        if _to_int(item.get("damage"), 0) <= 0:
            continue
        item["reward_ready"] = 1
        item["reward_wave"] = wave
        item["reward_total_damage"] = total_damage
    return total_damage


def _start_auto_demon_invasion() -> dict:
    state = _build_active_state(_current_period())
    _save_state(state)
    return state


def _finish_auto_demon_invasion() -> tuple[dict, bool]:
    state = _load_state()
    if state.get("status") != "active":
        return state, False

    state["active"] = 0
    state["status"] = "finished"
    state["last_result"] = "今日魔修入侵已于22:00结束。"
    _save_state(state)
    return state, True


def _refresh_defeated_demon_bosses() -> tuple[dict, list[str]]:
    state = _ensure_daily_state()
    if state.get("status") != "active":
        return state, []

    refreshed_realms: list[str] = []
    bosses = state.setdefault("bosses", {})
    for realm, boss_info in list(bosses.items()):
        if _to_int(boss_info.get("boss_hp"), 0) > 0:
            continue
        wave = max(_to_int(boss_info.get("wave"), 1), 1)
        _mark_wave_reward_ready(state, realm, wave)
        bosses[realm] = _create_demon_boss(realm, wave=wave + 1)
        refreshed_realms.append(realm)

    if refreshed_realms:
        state["last_result"] = f"每小时30分刷新检查：{', '.join(refreshed_realms)}魔修已重新入侵。"
        _save_state(state)
    return state, refreshed_realms


@scheduler.scheduled_job("cron", hour=f"{EVENT_START_HOUR},{EVENT_END_HOUR}", minute=0, id="demon_invasion_schedule")
async def demon_invasion_schedule_job():
    now = _now()
    with _state_lock:
        if now.hour == EVENT_START_HOUR:
            state = _start_auto_demon_invasion()
            log_text = f"每日魔修入侵已自动开启，本期生成{len(state.get('bosses', {}))}个境界魔修。"
        elif now.hour == EVENT_END_HOUR:
            _, finished = _finish_auto_demon_invasion()
            log_text = "每日魔修入侵已自动结束。" if finished else "每日魔修入侵结束检查完成，当前无进行中的入侵。"
        else:
            log_text = ""

    if log_text:
        logger.info(log_text)


@scheduler.scheduled_job(
    "cron",
    hour=f"{EVENT_START_HOUR}-{EVENT_END_HOUR - 1}",
    minute=30,
    second=0,
    id="demon_invasion_refresh_schedule",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1,
)
async def demon_invasion_refresh_schedule_job():
    with _state_lock:
        _, refreshed_realms = _refresh_defeated_demon_bosses()
    if refreshed_realms:
        logger.info(f"魔修入侵刷新检查完成，刷新境界：{', '.join(refreshed_realms)}")
    else:
        logger.info("魔修入侵刷新检查完成，暂无已击退魔修需要刷新。")


@scheduler.scheduled_job(
    "cron",
    minute=30,
    second=0,
    id="spirit_vein_schedule",
    misfire_grace_time=300,
    coalesce=True,
    max_instances=1,
)
async def spirit_vein_schedule_job():
    with _state_lock:
        _, log_text = _try_start_auto_spirit_vein()
    logger.info(log_text)


@world_event_help.handle(parameterless=[Cooldown(cd_time=0)])
async def world_event_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot,
        event,
        __world_event_help__,
        k1="状态",
        v1="魔修入侵状态",
        k2="灵脉",
        v2="天降灵脉",
        k3="讨伐",
        v3="讨伐魔修",
    )
    await world_event_help.finish()


@world_event_info.handle(parameterless=[Cooldown(cd_time=0)])
async def world_event_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, _ = check_user(event)
    with _state_lock:
        state = _ensure_daily_state()
        msg = _build_state_message(state, user_info if is_user else None)
        _save_state(state)
    await handle_send(
        bot,
        event,
        msg,
        md_type="世界事件",
        k1="讨伐",
        v1="讨伐魔修",
        k2="领奖",
        v2="领取魔修奖励",
        k3="帮助",
        v3="世界事件帮助",
    )
    await world_event_info.finish()


@spirit_vein_info.handle(parameterless=[Cooldown(cd_time=0)])
async def spirit_vein_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    with _state_lock:
        state = _ensure_spirit_vein_state()
        msg = _build_spirit_vein_message(state)
    await handle_send(
        bot,
        event,
        msg,
        md_type="世界事件",
        k1="修炼",
        v1="修炼",
        k2="出关",
        v2="出关",
        k3="炼体",
        v3="炼体结算",
    )
    await spirit_vein_info.finish()


@start_demon_invasion.handle(parameterless=[Cooldown(cd_time=0)])
async def start_demon_invasion_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    with _state_lock:
        state = _build_active_state(_current_period(), manual=True)
        _save_state(state)
    msg = (
        "魔修入侵已手动开启。\n"
        f"本期生成{len(state.get('bosses', {}))}个境界魔修。\n"
        "玩家发送【讨伐魔修】会自动挑战自身境界魔修。"
    )
    await handle_send(bot, event, msg, md_type="世界事件", k1="状态", v1="魔修入侵状态")
    await start_demon_invasion.finish()


@start_spirit_vein.handle(parameterless=[Cooldown(cd_time=0)])
async def start_spirit_vein_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    with _state_lock:
        state = _start_spirit_vein_manual()
    msg = (
        "天降灵脉已手动开启。\n"
        f"持续至：{state.get('ends_at')}\n"
        f"修为加成：+{int(SPIRIT_VEIN_EXP_BONUS_RATE * 100)}%\n"
        f"炼体加成：+{int(SPIRIT_VEIN_TIANTI_BONUS_RATE * 100)}%"
    )
    await handle_send(bot, event, msg, md_type="世界事件", k1="状态", v1="天降灵脉状态")
    await start_spirit_vein.finish()


@close_world_event.handle(parameterless=[Cooldown(cd_time=0)])
async def close_world_event_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    with _state_lock:
        state = _load_state()
        state["active"] = 0
        state["status"] = "finished"
        state["manual"] = 1
        state["last_result"] = "魔修入侵已手动结束。"
        _save_state(state)
    await handle_send(bot, event, "魔修入侵已手动结束。", md_type="世界事件", k1="状态", v1="魔修入侵状态")
    await close_world_event.finish()


@close_spirit_vein.handle(parameterless=[Cooldown(cd_time=0)])
async def close_spirit_vein_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    with _state_lock:
        _close_spirit_vein_manual()
    await handle_send(bot, event, "天降灵脉已手动关闭。", md_type="世界事件", k1="状态", v1="天降灵脉状态")
    await close_spirit_vein.finish()


@attack_demon_invasion.handle(parameterless=[Cooldown(cd_time=30)])
async def attack_demon_invasion_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await attack_demon_invasion.finish()

    user_id = str(user_info["user_id"])
    is_type, msg = check_user_type(user_id, 0)
    if not is_type:
        await handle_send(bot, event, msg, md_type="世界事件", k1="帮助", v1="世界事件帮助")
        await attack_demon_invasion.finish()

    sql_message.update_last_check_info_time(user_id)
    if user_info["hp"] is None or user_info["hp"] == 0:
        sql_message.update_user_hp(user_id)
        user_info = sql_message.get_user_info_with_id(user_id)

    if user_info["hp"] <= user_info["exp"] / 10:
        harm_time = leave_harm_time(user_id)
        msg = (
            f"重伤未愈，动弹不得！距离脱离危险还需要{harm_time}分钟！\n"
            f"请道友进行闭关，或者使用药品恢复气血。"
        )
        await handle_send(bot, event, msg, md_type="世界事件", k1="状态", v1="我的状态")
        await attack_demon_invasion.finish()

    with _state_lock:
        state = _ensure_daily_state()
        no_active_event = state.get("status") != "active"
        attack_limit_msg = ""
        realm = ""
        boss_snapshot = {}
        battle_hp = 1
        pursuit_mode = False
        if not no_active_event:
            realm, boss_info = _get_user_boss(state, user_info)
            if boss_info is None:
                no_active_event = True
            else:
                record_key, existing_record = _find_participant_record(state, user_id, realm)
                boss_wave = max(_to_int(boss_info.get("wave"), 1), 1)
                existing_attacks = _to_int((existing_record or {}).get("attacks"), 0)
                existing_wave = _record_wave(existing_record)
                boss_defeated = _to_int(boss_info.get("boss_hp"), 0) <= 0
                already_claimed = _has_user_claimed(state.get("claimed", {}), user_id, record_key)
                if already_claimed:
                    attack_limit_msg = "你已领取本期魔修奖励，不能继续追击。"
                elif existing_record and existing_wave == boss_wave and existing_attacks >= DEMON_ATTACK_LIMIT:
                    attack_limit_msg = f"你本期已经讨伐魔修{DEMON_ATTACK_LIMIT}次，不能继续出手。"
                else:
                    pursuit_mode = boss_defeated
                    boss_snapshot = dict(boss_info)
                    battle_hp = max(_to_int(boss_snapshot.get("battle_max_hp", boss_snapshot.get("battle_hp")), 0), 1)
                    boss_snapshot["气血"] = battle_hp
                    boss_snapshot["总血量"] = max(_to_int(boss_snapshot.get("battle_max_hp", boss_snapshot.get("总血量")), 0), 1)
                    _save_state(state)

    if no_active_event:
        msg = f"当前没有正在进行的魔修入侵。\n开启时间：每日{EVENT_START_HOUR}:00-{EVENT_END_HOUR}:00。"
        await handle_send(bot, event, msg, md_type="世界事件", k1="状态", v1="魔修入侵状态")
        await attack_demon_invasion.finish()
    if attack_limit_msg:
        await handle_send(bot, event, attack_limit_msg, md_type="世界事件", k1="领奖", v1="领取魔修奖励")
        await attack_demon_invasion.finish()

    result, victor, bossinfo_new, status_list = await Boss_fight(
        user_id,
        boss_snapshot,
        bot_id=bot.self_id,
        return_status=True,
    )

    total_damage = _extract_total_damage(status_list, user_id)

    real_damage = 0
    boss_now_hp = 0
    boss_all_hp = 1
    killed = False
    attack_duplicate_after_fight = False
    with _state_lock:
        state = _ensure_daily_state()
        battle_closed = state.get("status") != "active"
        if not battle_closed:
            boss_info = state.get("bosses", {}).get(realm)
            if not boss_info:
                battle_closed = True
            else:
                boss_wave = max(_to_int(boss_info.get("wave"), 1), 1)
                snapshot_wave = max(_to_int(boss_snapshot.get("wave"), 1), 1)
                if boss_wave != snapshot_wave:
                    battle_closed = True
                else:
                    record_key, existing_record = _find_participant_record(state, user_id, realm)
                    existing_attacks = _to_int((existing_record or {}).get("attacks"), 0)
                    existing_wave = _record_wave(existing_record)
                    already_claimed = _has_user_claimed(state.get("claimed", {}), user_id, record_key)
                    if (
                        already_claimed
                        or (
                            existing_record
                            and existing_wave == boss_wave
                            and existing_attacks >= DEMON_ATTACK_LIMIT
                        )
                    ):
                        battle_closed = True
                        attack_duplicate_after_fight = True
                    else:
                        boss_all_hp = max(_to_int(boss_info.get("boss_max_hp"), 0), 1)
                        current_hp = max(_to_int(boss_info.get("boss_hp"), 0), 0)
                        pursuit_mode = current_hp <= 0
                        damage_ratio = MAX_PURSUIT_DAMAGE_RATIO if pursuit_mode else MAX_SINGLE_DAMAGE_RATIO
                        max_single_damage = max(int(boss_all_hp * damage_ratio), 1)
                        raw_real_damage = total_damage * BOSS_REAL_HP_MULTIPLIER
                        if pursuit_mode:
                            real_damage = min(raw_real_damage, max_single_damage)
                            boss_now_hp = current_hp
                        else:
                            real_damage = min(raw_real_damage, max_single_damage, current_hp)
                            boss_now_hp = max(current_hp - real_damage, 0)
                        boss_info["boss_hp"] = boss_now_hp
                        boss_info["battle_hp"] = boss_info.get("battle_max_hp", boss_info.get("battle_hp", battle_hp))
                        boss_info["气血"] = boss_info["battle_hp"]
                        boss_info["总血量"] = boss_info.get("battle_max_hp", boss_info.get("总血量", battle_hp))
                        killed = (not pursuit_mode) and boss_now_hp <= 0

                        _record_participant(state, user_info, realm, boss_wave, real_damage, killed)
                        if pursuit_mode:
                            _mark_wave_reward_ready(state, realm, boss_wave)
                            boss_info["last_result"] = f"{user_info.get('user_name', user_id)}追击了{realm}魔修。"
                        elif killed:
                            _mark_wave_reward_ready(state, realm, boss_wave)
                            boss_info["battle_hp"] = 0
                            boss_info["气血"] = 0
                            boss_info["last_result"] = f"{user_info.get('user_name', user_id)}击退了{realm}魔修。"
                        state["bosses"][realm] = boss_info
                        _save_state(state)

    if battle_closed:
        msg = "你本期已经讨伐过魔修，本次战斗未重复计入贡献。" if attack_duplicate_after_fight else "本场魔修入侵已经结束或已刷新，本次战斗未计入贡献。"
        await handle_send(bot, event, msg, md_type="世界事件", k1="领奖", v1="领取魔修奖励")
        await attack_demon_invasion.finish()

    update_statistics_value(user_id, "魔修入侵参与")
    if real_damage > 0:
        update_statistics_value(user_id, "魔修入侵伤害", increment=real_damage)
    if killed:
        update_statistics_value(user_id, "魔修入侵击退")

    try:
        await send_msg_handler(bot, event, result)
    except Exception:
        msg = "对战消息发送错误，可能被风控。\n"
    else:
        msg = ""

    if pursuit_mode:
        msg += (
            f"道友追击已败退的{boss_snapshot.get('name', '魔修')}。\n"
            f"本次战斗造成伤害：{number_to(total_damage)}\n"
            f"追击贡献上限：{int(MAX_PURSUIT_DAMAGE_RATIO * 100)}%\n"
            f"本次追击贡献：{number_to(real_damage)}\n"
            f"{realm}魔修真实血量：{number_to(boss_now_hp)} / {number_to(boss_all_hp)}"
        )
    elif killed:
        msg += (
            f"恭喜道友击退{boss_snapshot.get('name', '魔修')}！\n"
            f"本次战斗造成伤害：{number_to(total_damage)}\n"
            f"贡献上限：{int(MAX_SINGLE_DAMAGE_RATIO * 100)}%\n"
            f"实际削减真实血条：{number_to(real_damage)}\n"
            f"参与者可发送【领取魔修奖励】按贡献领取奖励。"
        )
    else:
        result_text = "战胜了魔修化身" if victor == "群友赢了" else "不敌魔修，负伤退走"
        msg += (
            f"道友{result_text}。\n"
            f"本次战斗造成伤害：{number_to(total_damage)}\n"
            f"贡献上限：{int(MAX_SINGLE_DAMAGE_RATIO * 100)}%\n"
            f"实际削减真实血条：{number_to(real_damage)}\n"
            f"{realm}魔修真实血量：{number_to(boss_now_hp)} / {number_to(boss_all_hp)}"
        )

    await handle_send(
        bot,
        event,
        msg,
        md_type="世界事件",
        k1="领奖",
        v1="领取魔修奖励",
        k2="状态",
        v2="魔修入侵状态",
        k3="帮助",
        v3="世界事件帮助",
    )
    log_message(user_id, msg)
    await attack_demon_invasion.finish()


@claim_demon_reward.handle(parameterless=[Cooldown(cd_time=0)])
async def claim_demon_reward_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await claim_demon_reward.finish()

    user_id = str(user_info["user_id"])
    with _state_lock:
        state = _ensure_daily_state()
        preferred_realm = _normalize_realm(user_info.get("level", ""))
        participants = state.get("participants", {})
        record_key, record = _find_participant_record(state, user_id, preferred_realm)

        realm = (record or {}).get("realm") or preferred_realm
        wave = _record_wave(record)
        reward_ready = _record_reward_ready(state, record)
        no_reward_event = state.get("status") not in ("active", "finished")
        no_contribution = not record or _to_int(record.get("damage"), 0) <= 0
        reward_pending = state.get("status") == "active" and not no_contribution and not reward_ready

        claimed = state.setdefault("claimed", {})
        already_claimed = _has_user_claimed(claimed, user_id, record_key) if not no_reward_event else False

        if not (reward_pending or no_reward_event or no_contribution or already_claimed):
            total_damage = max(_to_int(record.get("reward_total_damage"), 0), _total_recorded_damage(participants, realm, wave), 1)
            damage = _to_int(record.get("damage"), 0)
            contribution = damage / total_damage
            stone_reward = min(int(DEMON_STONE_REWARD_CAP * contribution), DEMON_STONE_REWARD_CAP)
            exp_reward = int(max(_to_int(user_info.get("exp"), 0), 1) * min(0.05, 0.005 + 0.045 * contribution))

            claimed[record_key] = True
            _save_state(state)

    if reward_pending:
        msg = f"你参与的第{wave}波{realm}魔修尚未被击退，暂不能领取奖励。"
        await handle_send(bot, event, msg, md_type="世界事件", k1="讨伐", v1="讨伐魔修")
        await claim_demon_reward.finish()
    if no_reward_event:
        msg = "当前没有可领取奖励的魔修入侵。"
        await handle_send(bot, event, msg, md_type="世界事件", k1="状态", v1="魔修入侵状态")
        await claim_demon_reward.finish()
    if no_contribution:
        msg = f"你没有本期{realm}魔修入侵的有效贡献，无法领取奖励。"
        await handle_send(bot, event, msg, md_type="世界事件", k1="状态", v1="魔修入侵状态")
        await claim_demon_reward.finish()
    if already_claimed:
        msg = f"你已经领取过本期{realm}魔修入侵奖励了。"
        await handle_send(bot, event, msg, md_type="世界事件", k1="状态", v1="魔修入侵状态")
        await claim_demon_reward.finish()

    sql_message.update_ls(user_id, stone_reward, 1)
    if exp_reward > 0:
        sql_message.update_exp(user_id, exp_reward)
    update_statistics_value(user_id, "魔修入侵领奖")

    msg = (
        f"领取魔修入侵奖励成功！\n"
        f"对应境界：{realm}\n"
        f"对应波次：第{wave}波\n"
        f"贡献伤害：{number_to(damage)}\n"
        f"贡献占比：{contribution * 100:.2f}%\n"
        f"获得灵石：{number_to(stone_reward)}\n"
        f"获得修为：{number_to(exp_reward)}"
    )
    await handle_send(
        bot,
        event,
        msg,
        md_type="世界事件",
        k1="状态",
        v1="魔修入侵状态",
        k2="任务",
        v2="我的任务",
        k3="成就",
        v3="我的成就",
    )
    await claim_demon_reward.finish()
