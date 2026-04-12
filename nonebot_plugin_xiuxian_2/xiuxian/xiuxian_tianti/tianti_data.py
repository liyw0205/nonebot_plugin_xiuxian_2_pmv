try:
    import ujson as json
except ImportError:
    import json

from pathlib import Path
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager
from ..xiuxian_config import convert_rank

DATABASE = Path() / "data" / "xiuxian"
TIANTI_LEVEL_FILE = DATABASE / "炼体" / "炼体境界.json"
TIANTI_QIAOXUE_FILE = DATABASE / "炼体" / "炼体窍穴.json"

player_data_manager = PlayerDataManager()

_TIANTI_LEVEL_CACHE = None
_TIANTI_QIAOXUE_CACHE = None


def _read_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_tianti_level_all():
    global _TIANTI_LEVEL_CACHE
    if _TIANTI_LEVEL_CACHE is None:
        _TIANTI_LEVEL_CACHE = _read_json(TIANTI_LEVEL_FILE)
    return _TIANTI_LEVEL_CACHE


def get_tianti_level_order():
    all_data = get_tianti_level_all()
    arr = sorted(all_data.items(), key=lambda kv: int(kv[1]["rank"]))
    return [x[0] for x in arr]


def get_tianti_level_data(level_name: str):
    return get_tianti_level_all()[level_name]


def get_next_tianti_level_name(level_name: str):
    order = get_tianti_level_order()
    if level_name not in order:
        return None
    idx = order.index(level_name)
    return order[idx + 1] if idx < len(order) - 1 else None


def get_tianti_level_index(level_name: str, is_xiuxian=False):
    if is_xiuxian:
        return convert_rank(level_name)[0]
    lvl = get_tianti_level_data(level_name)
    return 100000 - int(lvl["rank"])


def get_qiaoxue_all():
    global _TIANTI_QIAOXUE_CACHE
    if _TIANTI_QIAOXUE_CACHE is None:
        _TIANTI_QIAOXUE_CACHE = _read_json(TIANTI_QIAOXUE_FILE)
    return _TIANTI_QIAOXUE_CACHE


def get_qiaoxue_pool():
    return get_qiaoxue_all().get("窍穴", [])


def get_qiaoxue_map():
    return {x["name"]: x for x in get_qiaoxue_pool()}


def get_opened_qiaoxue_count(user_data: dict):
    return len(user_data.get("opened_qiaoxue", []))


class TiantiDataManager:
    TABLE = "tianti_info"

    def _default(self):
        order = get_tianti_level_order()
        return {
            "tianti_level": order[0],
            "tianti_hp": 0,
            "last_settle_time": None,
            "medicine_last_time": None,
            "opened_qiaoxue": [],
            "opened_qiaoxue_detail": [],
            "qiaoxue_stage_opened": {}
        }

    def get_user_tianti_info(self, user_id: str):
        user_id = str(user_id)
        row = player_data_manager.get_fields(user_id, self.TABLE)
        if not row:
            data = self._default()
            self.save_user_tianti_info(user_id, data)
            return data

        data = self._default()
        for k in data.keys():
            if k in row and row[k] is not None:
                data[k] = row[k]

        if isinstance(data["opened_qiaoxue"], str):
            data["opened_qiaoxue"] = json.loads(data["opened_qiaoxue"])
        if isinstance(data["opened_qiaoxue_detail"], str):
            data["opened_qiaoxue_detail"] = json.loads(data["opened_qiaoxue_detail"])
        if isinstance(data["qiaoxue_stage_opened"], str):
            data["qiaoxue_stage_opened"] = json.loads(data["qiaoxue_stage_opened"])

        return data

    def save_user_tianti_info(self, user_id: str, data: dict):
        user_id = str(user_id)
        for k, v in data.items():
            player_data_manager.update_or_write_data(user_id, self.TABLE, k, v, data_type="TEXT")