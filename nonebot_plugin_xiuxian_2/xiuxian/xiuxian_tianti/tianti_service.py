from datetime import datetime

from ..xiuxian_config import XiuConfig
from ..xiuxian_world_events import get_spirit_vein_tianti_multiplier
from .tianti_data import get_next_tianti_level_name, get_tianti_level_data


def get_tianti_cap(data: dict) -> int:
    """
    上限规则：next_need_hp * closing_exp_upper_limit
    """
    next_name = get_next_tianti_level_name(data["tianti_level"])
    if not next_name:
        return 10**30
    need_hp = int(get_tianti_level_data(next_name)["need_hp"])
    return int(need_hp * XiuConfig().closing_exp_upper_limit)


def calc_qiaoxue_bonus(data: dict):
    """
    统计已开窍穴总加成
    """
    base_ratio = 0.0
    gain_pct = 0.0
    detail_list = data.get("opened_qiaoxue_detail", [])
    for q in detail_list:
        et = q["effect_type"]
        ev = float(q["effect_value"])
        if et == "base_per_min_ratio":
            base_ratio += ev
        elif et == "hp_gain_pct":
            gain_pct += ev
    return base_ratio, gain_pct


def parse_tianti_time(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def clear_medicine_bath(data: dict):
    data["medicine_last_time"] = None
    data["medicine_end_time"] = None
    data["medicine_effect"] = 0.0
    data["medicine_name"] = ""


def get_active_medicine_bath(data: dict, now_t: datetime):
    end_t = parse_tianti_time(data.get("medicine_end_time"))
    if not end_t or now_t > end_t:
        return None
    try:
        effect = float(data.get("medicine_effect", 0) or 0)
    except Exception:
        effect = 0.0
    if effect <= 1:
        return None
    return {
        "name": data.get("medicine_name") or "未知药材",
        "effect": effect,
        "end_time": end_t,
    }


def get_sect_fairyland_bonus(level: int) -> float:
    try:
        level = int(level or 0)
    except Exception:
        level = 0
    return max(0, min(level, 10)) * 0.05


def _apply_tianti_minutes(data: dict, mins: int, now_t: datetime, sect_fairyland_level: int = 0):
    lvl_data = get_tianti_level_data(data["tianti_level"])
    base_per_min = int(lvl_data["hp_gain_per_min"])
    base_ratio, gain_pct = calc_qiaoxue_bonus(data)
    real_per_min = int(base_per_min * (1 + base_ratio))

    bath = get_active_medicine_bath(data, now_t)
    bath_effect = bath["effect"] if bath else 1.0
    sect_bonus = get_sect_fairyland_bonus(sect_fairyland_level)
    spirit_vein_multiplier = get_spirit_vein_tianti_multiplier()
    bath_expired = False
    if not bath and data.get("medicine_end_time"):
        clear_medicine_bath(data)
        bath_expired = True

    gain = int(mins * real_per_min * (1 + gain_pct) * bath_effect * (1 + sect_bonus) * spirit_vein_multiplier)
    cap = get_tianti_cap(data)
    old_hp = int(data["tianti_hp"])
    new_hp = min(cap, old_hp + gain)
    real_gain = max(0, new_hp - old_hp)
    data["tianti_hp"] = new_hp

    return {
        "status": "ok",
        "mins": mins,
        "real_gain": real_gain,
        "new_hp": new_hp,
        "cap": cap,
        "bath": bath,
        "bath_expired": bath_expired,
        "sect_bonus": sect_bonus,
        "spirit_vein_bonus": spirit_vein_multiplier - 1,
    }


def calc_tianti_gain_rate(data: dict, now_t: datetime | None = None, sect_fairyland_level: int = 0):
    """
    计算当前炼体每分钟收益，保持与实际结算公式一致。
    """
    now_t = now_t or datetime.now()
    lvl_data = get_tianti_level_data(data["tianti_level"])
    base_per_min = int(lvl_data["hp_gain_per_min"])
    base_ratio, gain_pct = calc_qiaoxue_bonus(data)
    real_per_min = int(base_per_min * (1 + base_ratio))

    bath = get_active_medicine_bath(data, now_t)
    bath_effect = bath["effect"] if bath else 1.0
    sect_bonus = get_sect_fairyland_bonus(sect_fairyland_level)
    spirit_vein_multiplier = get_spirit_vein_tianti_multiplier()
    per_min = int(real_per_min * (1 + gain_pct) * bath_effect * (1 + sect_bonus) * spirit_vein_multiplier)

    return {
        "base_per_min": base_per_min,
        "base_ratio": base_ratio,
        "gain_pct": gain_pct,
        "bath": bath,
        "bath_effect": bath_effect,
        "sect_bonus": sect_bonus,
        "spirit_vein_bonus": spirit_vein_multiplier - 1,
        "per_min": per_min,
        "efficiency": (per_min / base_per_min) if base_per_min > 0 else 0,
    }


def settle_tianti_gain(data: dict, now_t: datetime, sect_fairyland_level: int = 0):
    last_t = parse_tianti_time(data.get("last_settle_time"))
    if not last_t:
        data["last_settle_time"] = now_t.strftime("%Y-%m-%d %H:%M:%S")
        return {"status": "init"}

    mins = max(0, int((now_t - last_t).total_seconds() // 60))
    if mins <= 0:
        return {"status": "empty", "mins": mins}

    result = _apply_tianti_minutes(data, mins, now_t, sect_fairyland_level)
    data["last_settle_time"] = now_t.strftime("%Y-%m-%d %H:%M:%S")
    return result


def grant_tianti_settle_minutes(
    data: dict,
    minutes: int,
    now_t: datetime | None = None,
    sect_fairyland_level: int = 0,
):
    """
    按当前炼体状态发放指定分钟数的炼体气血，不改变正常炼体结算时间。
    """
    now_t = now_t or datetime.now()
    mins = max(0, int(minutes))
    if mins <= 0:
        return {"status": "empty", "mins": mins}
    return _apply_tianti_minutes(data, mins, now_t, sect_fairyland_level)
