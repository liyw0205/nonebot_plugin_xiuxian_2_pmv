# xiuxian_trade/auction_config.py
import json
from pathlib import Path
from typing import Dict, Any, Optional

CONFIG_PATH = Path(__file__).parent / "auction_config.json"

DEFAULT_CONFIG: Dict[str, Any] = {
    "schedule": {
        "start_hour": 17,
        "start_minute": 0,
        "duration_hours": 5,
        "enabled": True,
        "last_auto_start_date": ""  # YYYY-MM-DD
    },
    "rules": {
        "max_user_items": 3,
        "min_price": 1_000_000,
        "min_increment_percent": 0.1,
        "min_bid_increment": 1_000_000,
        "fee_rate": 0.2
    },
    "auction_status": {
        "active": False,
        "start_time": "",                 # YYYYMMDDHHMMSS
        "end_time": "",                   # YYYYMMDDHHMMSS
        "last_display_refresh_time": "",  # YYYYMMDDHHMMSS
        "items_count": 0
    }
}

SYSTEM_ITEMS: Dict[str, Dict[str, int]] = {
        "安神灵液": {"id": 1412, "start_price": 550000},
        "魇龙之血": {"id": 1413, "start_price": 550000},
        "化劫丹": {"id": 1414, "start_price": 700000},
        "太上玄门丹": {"id": 1415, "start_price": 15000000},
        "金仙破厄丹": {"id": 1416, "start_price": 20000000},
        "太乙炼髓丹": {"id": 1417, "start_price": 50000000},
        "地仙玄丸": {"id": 2014, "start_price": 500000},
        "消冰宝丸": {"id": 2015, "start_price": 1000000},
        "遁一丹": {"id": 1418, "start_price": 7000000},
        "至尊丹": {"id": 1419, "start_price": 1000000},
        "极品至尊丹": {"id": 1421, "start_price": 30000000},
        "极品遁一丹": {"id": 1420, "start_price": 50000000},
        "太清玉液丹": {"id": 15151, "start_price": 70000000},
        "一气鸿蒙丹": {"id": 15152, "start_price": 9000000},
        "三纹清灵丹": {"id": 15153, "start_price": 110000000},
        "生骨丹": {"id": 1101, "start_price": 1000},
        "化瘀丹": {"id": 1102, "start_price": 3000},
        "培元丹": {"id": 1103, "start_price": 5000},
        "固元丹": {"id": 1104, "start_price": 10000},
        "黄龙丹": {"id": 1105, "start_price": 15000},
        "回元丹": {"id": 1106, "start_price": 25000},
        "回春丹": {"id": 1107, "start_price": 40000},
        "养元丹": {"id": 1108, "start_price": 60000},
        "太元真丹": {"id": 1109, "start_price": 80000},
        "九阳真丹": {"id": 1110, "start_price": 100000},
        "无始经": {"id": 9914, "start_price": 55000000},
        "不灭天功": {"id": 9913, "start_price": 55000000},
        "射日弓": {"id": 8000, "start_price": 3500000000},
        "青龙偃月刀": {"id": 7097, "start_price": 350000000},
        "万魔渡": {"id": 9924, "start_price": 350000000},
        "血海魔铠": {"id": 6094, "start_price": 500000000},
        "万剑归宗": {"id": 8920, "start_price": 700000000},
        "华光猎影": {"id": 8921, "start_price": 600000000},
        "灭剑血胧": {"id": 8922, "start_price": 600000000},
        "混沌星陨劫": {"id": 8913, "start_price": 700000000},
        "九幽炼狱火": {"id": 8914, "start_price": 650000000},
        "地煞七十二术": {"id": 8915, "start_price": 250000000},
        "天罡三十六法": {"id": 8916, "start_price": 800000000},
        "万法归一剑": {"id": 8960, "start_price": 700000000},
        "陨铁炉": {"id": 4003, "start_price": 1000000000},
        "雕花紫铜炉": {"id": 4002, "start_price": 8000000000},
        "风神诀": {"id": 9926, "start_price": 350000000},
        "三丰丹经": {"id": 9920, "start_price": 8500000000},
        "暗渊灭世功": {"id": 9935, "start_price": 1200000000},
        "太清丹经": {"id": 9933, "start_price": 10000000000000},
        "大道归一丹": {"id": 15102, "start_price": 500000000},
        "天地玄功": {"id": 9934, "start_price": 11500000000000},
        "渡劫天功": {"id": 9931, "start_price": 12000000000},
        "道师符经": {"id": 9921, "start_price": 55000000000},
        "易名符": {"id": 20011, "start_price": 10000000},
        "蕴灵石": {"id": 20004, "start_price": 100000000},
        "神圣石": {"id": 20003, "start_price": 1000000000},
        "化道石": {"id": 20002, "start_price": 10000000000}
    }


def _deep_merge(user_cfg: Dict[str, Any], default_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """递归补齐缺失键，不覆盖已有值"""
    out = dict(user_cfg)
    for k, v in default_cfg.items():
        if k not in out:
            out[k] = v
        elif isinstance(v, dict) and isinstance(out[k], dict):
            out[k] = _deep_merge(out[k], v)
    return out


def save_config(config: Dict[str, Any]) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


def get_auction_config() -> Dict[str, Any]:
    """读取配置（自动补全+纠偏）"""
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return dict(DEFAULT_CONFIG)

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        if not isinstance(cfg, dict):
            raise ValueError("config root must be object")
    except Exception:
        cfg = dict(DEFAULT_CONFIG)
        save_config(cfg)
        return cfg

    merged = _deep_merge(cfg, DEFAULT_CONFIG)

    # ===== 字段纠偏 =====
    sch = merged["schedule"]
    sch["start_hour"] = min(max(int(sch.get("start_hour", 17)), 0), 23)
    sch["start_minute"] = min(max(int(sch.get("start_minute", 0)), 0), 59)
    sch["duration_hours"] = max(int(sch.get("duration_hours", 5)), 1)
    sch["enabled"] = bool(sch.get("enabled", True))
    sch["last_auto_start_date"] = str(sch.get("last_auto_start_date", ""))

    rules = merged["rules"]
    rules["max_user_items"] = max(int(rules.get("max_user_items", 3)), 1)
    rules["min_price"] = max(int(rules.get("min_price", 1_000_000)), 1)
    rules["min_increment_percent"] = max(float(rules.get("min_increment_percent", 0.1)), 0.0)
    rules["min_bid_increment"] = max(int(rules.get("min_bid_increment", 1_000_000)), 1)
    rules["fee_rate"] = min(max(float(rules.get("fee_rate", 0.2)), 0.0), 1.0)

    st = merged["auction_status"]
    st["active"] = bool(st.get("active", False))
    st["start_time"] = str(st.get("start_time", ""))
    st["end_time"] = str(st.get("end_time", ""))
    st["last_display_refresh_time"] = str(st.get("last_display_refresh_time", ""))
    st["items_count"] = max(int(st.get("items_count", 0)), 0)

    # 若有变更则写回
    if merged != cfg:
        save_config(merged)

    return merged


def get_system_items() -> Dict[str, Any]:
    return SYSTEM_ITEMS


def get_auction_schedule() -> Dict[str, Any]:
    return get_auction_config()["schedule"]


def get_auction_rules() -> Dict[str, Any]:
    return get_auction_config()["rules"]


def get_auction_status_from_config_file() -> Dict[str, Any]:
    return get_auction_config()["auction_status"]


def set_auction_config_value(key: str, value: Any, sub_key: Optional[str] = None) -> None:
    """
    通用写入：
    - sub_key=None: cfg[key] = value
    - sub_key!=None: cfg[key][sub_key] = value
    """
    cfg = get_auction_config()
    if sub_key is None:
        cfg[key] = value
    else:
        if key not in cfg or not isinstance(cfg[key], dict):
            cfg[key] = {}
        cfg[key][sub_key] = value
    save_config(cfg)


def get_auction_config_value(key: str, sub_key: Optional[str] = None) -> Any:
    cfg = get_auction_config()
    if key not in cfg:
        return None
    if sub_key is None:
        return cfg[key]
    if isinstance(cfg[key], dict):
        return cfg[key].get(sub_key)
    return None


def update_schedule(new_schedule: Dict[str, Any]) -> None:
    cfg = get_auction_config()
    if "schedule" not in cfg or not isinstance(cfg["schedule"], dict):
        cfg["schedule"] = {}
    cfg["schedule"].update(new_schedule)
    save_config(cfg)


# 初始化时确保配置文件存在
get_auction_config()