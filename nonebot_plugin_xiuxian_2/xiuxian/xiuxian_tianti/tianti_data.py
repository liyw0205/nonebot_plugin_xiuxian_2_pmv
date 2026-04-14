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
    all_data = get_tianti_level_all()
    if level_name not in all_data:
        # 回退到第一个境界，避免异常数据直接炸
        order = get_tianti_level_order()
        return all_data[order[0]]
    return all_data[level_name]


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


def _safe_json_loads(value, default):
    """
    安全解析 JSON 字符串，失败则返回 default
    """
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    if isinstance(value, str):
        txt = value.strip()
        if txt.lower() in ("", "none", "null"):
            return default
        try:
            return json.loads(txt)
        except Exception:
            return default
    return default


def _dedup_keep_order(seq):
    """
    去重并保持顺序
    """
    seen = set()
    res = []
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        res.append(x)
    return res


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

    def _normalize_level(self, level_name):
        """
        炼体境界容错：
        非法时回退到第一境界
        """
        order = get_tianti_level_order()
        if level_name in order:
            return level_name
        return order[0]

    def _normalize_hp(self, hp):
        """
        气血容错：
        非法时回退 0
        """
        try:
            hp = int(hp)
            return max(0, hp)
        except Exception:
            return 0

    def _normalize_opened_qiaoxue(self, opened_list):
        """
        已开窍穴列表清洗：
        - 必须是列表
        - 只保留存在于配置中的窍穴
        - 去重保序
        """
        qmap = get_qiaoxue_map()
        arr = _safe_json_loads(opened_list, [])
        if not isinstance(arr, list):
            arr = []

        cleaned = []
        for name in arr:
            if not isinstance(name, str):
                continue
            if name in qmap:
                cleaned.append(name)

        return _dedup_keep_order(cleaned)

    def _normalize_opened_qiaoxue_detail(self, detail_list):
        """
        已开窍穴详情清洗：
        - 必须是列表
        - 只保留合法窍穴
        - 缺失字段时尽量从配置补全
        - 同名窍穴只保留第一次出现
        """
        qmap = get_qiaoxue_map()
        arr = _safe_json_loads(detail_list, [])
        if not isinstance(arr, list):
            arr = []

        cleaned = []
        seen = set()

        for item in arr:
            if not isinstance(item, dict):
                continue

            name = item.get("name")
            if not isinstance(name, str):
                continue
            if name not in qmap:
                continue
            if name in seen:
                continue

            cfg = qmap[name]

            group = item.get("group", cfg.get("group"))
            effect_type = item.get("effect_type", cfg.get("effect_type"))
            effect_value = item.get("effect_value", cfg.get("effect_value"))

            try:
                effect_value = float(effect_value)
            except Exception:
                effect_value = float(cfg.get("effect_value", 0))

            cleaned.append({
                "name": name,
                "group": group,
                "effect_type": effect_type,
                "effect_value": effect_value
            })
            seen.add(name)

        return cleaned

    def _rebuild_detail_from_opened(self, opened_list, detail_list):
        """
        以 opened_qiaoxue 为准，重建/补全 detail
        规则：
        - opened 中有，detail 中没有 -> 从配置补
        - detail 中有，opened 中没有 -> 丢弃
        - 顺序以 opened 为准
        """
        qmap = get_qiaoxue_map()
        detail_map = {}

        for item in detail_list:
            name = item.get("name")
            if name:
                detail_map[name] = item

        rebuilt = []
        for name in opened_list:
            if name in detail_map:
                rebuilt.append(detail_map[name])
            else:
                cfg = qmap.get(name)
                if not cfg:
                    continue
                rebuilt.append({
                    "name": cfg["name"],
                    "group": cfg["group"],
                    "effect_type": cfg["effect_type"],
                    "effect_value": float(cfg["effect_value"])
                })
        return rebuilt

    def _normalize_stage_opened(self, stage_data):
        """
        旧字段兼容：
        仅保证它是 dict[str, int]，不参与核心限制逻辑
        """
        data = _safe_json_loads(stage_data, {})
        if not isinstance(data, dict):
            return {}

        cleaned = {}
        for k, v in data.items():
            if not isinstance(k, str):
                continue
            try:
                cleaned[k] = max(0, int(v))
            except Exception:
                cleaned[k] = 0
        return cleaned

    def _clean_user_data(self, row_data: dict):
        """
        对数据库取出的用户炼体数据做完整清洗
        """
        data = self._default()

        # 先覆盖已有字段
        for k in data.keys():
            if k in row_data and row_data[k] is not None:
                data[k] = row_data[k]

        # 基础字段清洗
        data["tianti_level"] = self._normalize_level(data.get("tianti_level"))
        data["tianti_hp"] = self._normalize_hp(data.get("tianti_hp"))

        # 时间字段保留原始值，交给上层命令逻辑兼容处理
        if data.get("last_settle_time") in ("", "null", "None", "none", 0):
            data["last_settle_time"] = None
        if data.get("medicine_last_time") in ("", "null", "None", "none", 0):
            data["medicine_last_time"] = None

        # 窍穴字段清洗
        opened = self._normalize_opened_qiaoxue(data.get("opened_qiaoxue"))
        detail = self._normalize_opened_qiaoxue_detail(data.get("opened_qiaoxue_detail"))
        detail = self._rebuild_detail_from_opened(opened, detail)

        data["opened_qiaoxue"] = opened
        data["opened_qiaoxue_detail"] = detail
        data["qiaoxue_stage_opened"] = self._normalize_stage_opened(data.get("qiaoxue_stage_opened"))

        return data

    def get_user_tianti_info(self, user_id: str):
        user_id = str(user_id)
        row = player_data_manager.get_fields(user_id, self.TABLE)

        if not row:
            data = self._default()
            self.save_user_tianti_info(user_id, data)
            return data

        data = self._clean_user_data(row)

        # 如果清洗后和原始数据明显不一致，可选择回写一次，防止以后继续脏
        # 这里直接回写，保证后续读取都是干净数据
        self.save_user_tianti_info(user_id, data)
        return data

    def save_user_tianti_info(self, user_id: str, data: dict):
        user_id = str(user_id)

        # 保存前再次清洗，避免上层写入脏数据
        clean_data = self._clean_user_data(data)

        for k, v in clean_data.items():
            if isinstance(v, (list, dict)):
                v = json.dumps(v, ensure_ascii=False)
            player_data_manager.update_or_write_data(user_id, self.TABLE, k, v, data_type="TEXT")