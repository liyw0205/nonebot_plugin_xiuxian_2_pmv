from ...paths import get_paths

from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager
from .relation_utils import (
    _is_none_like,
    _normalize_dict,
    _normalize_history,
    _normalize_id_list,
    safe_int,
)


PLAYERSDATA = get_paths().players
MENTOR_HISTORY_LIMIT = 50
MENTOR_BREAKTHROUGH_REWARD_LIMIT = 27

player_data_manager = PlayerDataManager()


def default_partner_data():
    return {
        "partner_id": None,
        "bind_time": None,
        "affection": 0,
    }


def default_mentor_data():
    return {
        "mentor_id": None,
        "apprentice_ids": [],
        "bind_time": None,
        "mentor_cd_until": None,
        "apprentice_cd_until": None,
        "mentor_rebind_cd": {},
        "mentor_history": [],
        "mentor_protect": "off",
        "mentor_apply_time": None,
        "mentor_apply_target": None,
        "breakthrough_reward_count": 0,
    }


def bind_partner_storage(manager=None, mentor_history_limit=None, mentor_breakthrough_reward_limit=None):
    """绑定 partner.py 中的存储实例和配置上限，保持运行时行为一致。"""
    global player_data_manager, MENTOR_HISTORY_LIMIT, MENTOR_BREAKTHROUGH_REWARD_LIMIT

    if manager is not None:
        player_data_manager = manager
    if mentor_history_limit is not None:
        MENTOR_HISTORY_LIMIT = mentor_history_limit
    if mentor_breakthrough_reward_limit is not None:
        MENTOR_BREAKTHROUGH_REWARD_LIMIT = mentor_breakthrough_reward_limit


def load_partner(user_id):
    """
    加载用户自己的道侣数据。

    修复点：
    1. 不再读取对方的 partner 表，避免亲密度、绑定时间读错。
    2. 兼容历史 "None" / "null" / "" 脏数据。
    """
    info = player_data_manager.get_fields(str(user_id), "partner")

    if not info:
        return default_partner_data()

    partner_id = info.get("partner_id")
    bind_time = info.get("bind_time")
    affection = info.get("affection")

    if _is_none_like(partner_id):
        partner_id = None
    else:
        partner_id = str(partner_id)

    if _is_none_like(bind_time):
        bind_time = None
    else:
        bind_time = str(bind_time)

    affection = safe_int(affection, 0)

    return {
        "partner_id": partner_id,
        "bind_time": bind_time,
        "affection": affection,
    }


def save_partner(user_id, data):
    """
    保存用户道侣数据。

    注意：
    如果你已经修复 PlayerDataManager.update_or_write_data，使 None 写入 SQL NULL，
    这里可以直接传 None。
    """
    partner_id = data.get("partner_id")
    bind_time = data.get("bind_time")
    affection = data.get("affection")

    if _is_none_like(partner_id):
        partner_id = None
    else:
        partner_id = str(partner_id)

    if _is_none_like(bind_time):
        bind_time = None
    else:
        bind_time = str(bind_time)

    affection = safe_int(affection, 0)

    player_data_manager.update_or_write_data(
        str(user_id), "partner", "partner_id", partner_id, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "partner", "bind_time", bind_time, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "partner", "affection", affection, data_type="INTEGER"
    )


def load_mentor(user_id):
    """加载用户师徒数据。"""
    info = player_data_manager.get_fields(str(user_id), "mentor")

    if not info:
        return default_mentor_data()

    mentor_id = info.get("mentor_id")
    if _is_none_like(mentor_id):
        mentor_id = None
    else:
        mentor_id = str(mentor_id)

    bind_time = info.get("bind_time")
    if _is_none_like(bind_time):
        bind_time = None
    else:
        bind_time = str(bind_time)

    mentor_cd_until = info.get("mentor_cd_until")
    if _is_none_like(mentor_cd_until):
        mentor_cd_until = None
    else:
        mentor_cd_until = str(mentor_cd_until)

    apprentice_cd_until = info.get("apprentice_cd_until")
    if _is_none_like(apprentice_cd_until):
        apprentice_cd_until = None
    else:
        apprentice_cd_until = str(apprentice_cd_until)

    mentor_protect = str(info.get("mentor_protect") or "off").strip().lower()
    if mentor_protect not in ["on", "off"]:
        mentor_protect = "off"

    mentor_apply_time = info.get("mentor_apply_time")
    if _is_none_like(mentor_apply_time):
        mentor_apply_time = None
    else:
        mentor_apply_time = str(mentor_apply_time)

    mentor_apply_target = info.get("mentor_apply_target")
    if _is_none_like(mentor_apply_target):
        mentor_apply_target = None
    else:
        mentor_apply_target = str(mentor_apply_target)

    return {
        "mentor_id": mentor_id,
        "apprentice_ids": _normalize_id_list(info.get("apprentice_ids")),
        "bind_time": bind_time,
        "mentor_cd_until": mentor_cd_until,
        "apprentice_cd_until": apprentice_cd_until,
        "mentor_rebind_cd": _normalize_dict(info.get("mentor_rebind_cd")),
        "mentor_history": _normalize_history(info.get("mentor_history")),
        "mentor_protect": mentor_protect,
        "mentor_apply_time": mentor_apply_time,
        "mentor_apply_target": mentor_apply_target,
        "breakthrough_reward_count": safe_int(info.get("breakthrough_reward_count"), 0),
    }


def save_mentor(user_id, data):
    """保存用户师徒数据。"""
    mentor_id = data.get("mentor_id")
    if _is_none_like(mentor_id):
        mentor_id = None
    else:
        mentor_id = str(mentor_id)

    apprentice_ids = _normalize_id_list(data.get("apprentice_ids"))

    bind_time = data.get("bind_time")
    if _is_none_like(bind_time):
        bind_time = None
    else:
        bind_time = str(bind_time)

    mentor_cd_until = data.get("mentor_cd_until")
    if _is_none_like(mentor_cd_until):
        mentor_cd_until = None
    else:
        mentor_cd_until = str(mentor_cd_until)

    apprentice_cd_until = data.get("apprentice_cd_until")
    if _is_none_like(apprentice_cd_until):
        apprentice_cd_until = None
    else:
        apprentice_cd_until = str(apprentice_cd_until)

    mentor_protect = str(data.get("mentor_protect") or "off").strip().lower()
    if mentor_protect not in ["on", "off"]:
        mentor_protect = "off"

    mentor_apply_time = data.get("mentor_apply_time")
    if _is_none_like(mentor_apply_time):
        mentor_apply_time = None
    else:
        mentor_apply_time = str(mentor_apply_time)

    mentor_apply_target = data.get("mentor_apply_target")
    if _is_none_like(mentor_apply_target):
        mentor_apply_target = None
    else:
        mentor_apply_target = str(mentor_apply_target)

    mentor_rebind_cd = _normalize_dict(data.get("mentor_rebind_cd"))
    mentor_history = _normalize_history(data.get("mentor_history"))[-MENTOR_HISTORY_LIMIT:]
    breakthrough_reward_count = min(
        max(safe_int(data.get("breakthrough_reward_count"), 0), 0),
        MENTOR_BREAKTHROUGH_REWARD_LIMIT,
    )

    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "mentor_id", mentor_id, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "apprentice_ids", apprentice_ids, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "bind_time", bind_time, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "mentor_cd_until", mentor_cd_until, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "apprentice_cd_until", apprentice_cd_until, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "mentor_rebind_cd", mentor_rebind_cd, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "mentor_history", mentor_history, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "mentor_protect", mentor_protect, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "mentor_apply_time", mentor_apply_time, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "mentor_apply_target", mentor_apply_target, data_type="TEXT"
    )
    player_data_manager.update_or_write_data(
        str(user_id), "mentor", "breakthrough_reward_count", breakthrough_reward_count, data_type="INTEGER"
    )


__all__ = [
    "PLAYERSDATA",
    "default_partner_data",
    "default_mentor_data",
    "bind_partner_storage",
    "load_partner",
    "save_partner",
    "load_mentor",
    "save_mentor",
]
