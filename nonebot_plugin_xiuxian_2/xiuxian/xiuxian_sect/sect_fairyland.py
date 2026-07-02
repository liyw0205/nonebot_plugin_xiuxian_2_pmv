from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager


SECT_FAIRYLAND_MAX_LEVEL = 10
SECT_FAIRYLAND_CLAIM_TABLE = "sect_fairyland_claim"
SECT_FAIRYLAND_CONFIG = {
    0: {"name": "暂无", "minutes": 0, "bonus": 0.0, "stone": 0, "materials": 0},
    1: {"name": "初建炼体堂", "minutes": 30, "bonus": 0.05, "stone": 20000000, "materials": 200000000},
    2: {"name": "筋骨灵泉", "minutes": 45, "bonus": 0.10, "stone": 50000000, "materials": 500000000},
    3: {"name": "龙象淬体阵", "minutes": 60, "bonus": 0.15, "stone": 100000000, "materials": 1000000000},
    4: {"name": "玄元炼体场", "minutes": 90, "bonus": 0.20, "stone": 200000000, "materials": 2000000000},
    5: {"name": "太古锻身台", "minutes": 120, "bonus": 0.25, "stone": 400000000, "materials": 4000000000},
    6: {"name": "不灭淬体堂", "minutes": 150, "bonus": 0.30, "stone": 800000000, "materials": 8000000000},
    7: {"name": "地脉锻骨阵", "minutes": 180, "bonus": 0.35, "stone": 1600000000, "materials": 16000000000},
    8: {"name": "万象炼体场", "minutes": 240, "bonus": 0.40, "stone": 3200000000, "materials": 32000000000},
    9: {"name": "鸿蒙淬体台", "minutes": 300, "bonus": 0.45, "stone": 6400000000, "materials": 64000000000},
    10: {"name": "永恒炼体堂", "minutes": 360, "bonus": 0.50, "stone": 12800000000, "materials": 128000000000},
}

_player_data_manager = PlayerDataManager()


def _to_int(value, default: int = 0) -> int:
    try:
        return int(value or default)
    except Exception:
        return default


def _get_sect_fairyland_level(sect_info: dict | None) -> int:
    if not sect_info:
        return 0
    return max(0, min(SECT_FAIRYLAND_MAX_LEVEL, _to_int(sect_info.get("sect_fairyland", 0))))


def _get_sect_fairyland_config(level: int) -> dict:
    level = max(0, min(SECT_FAIRYLAND_MAX_LEVEL, _to_int(level)))
    return SECT_FAIRYLAND_CONFIG[level]


def _fairyland_claim_key(sect_id) -> str:
    return f"last_claim_{sect_id}"


def _get_fairyland_last_claim(user_id, sect_id) -> str:
    data = _player_data_manager.get_fields(str(user_id), SECT_FAIRYLAND_CLAIM_TABLE) or {}
    value = data.get(_fairyland_claim_key(sect_id), "")
    return str(value or "")


def _set_fairyland_last_claim(user_id, sect_id, day: str):
    _player_data_manager.update_or_write_data(
        str(user_id),
        SECT_FAIRYLAND_CLAIM_TABLE,
        _fairyland_claim_key(sect_id),
        day,
        data_type="TEXT",
    )
