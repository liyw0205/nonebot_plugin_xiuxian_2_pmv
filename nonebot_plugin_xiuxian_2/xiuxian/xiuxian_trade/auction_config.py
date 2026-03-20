# xiuxian_trade/auction_config.py
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

CONFIG_PATH = Path(__file__).parent / "auction_config.json"

DEFAULT_CONFIG = {
    "schedule": { # 拍卖调度配置
        "start_hour": 17, # 每日17点开始
        "start_minute": 0,
        "duration_hours": 5, # 持续5小时
        "enabled": True, # 是否启用自动拍卖
        "last_auto_start_date": "" # 上次自动开启的日期，防止当日重复开启, 格式: YYYY-MM-DD
    },
    "rules": { # 拍卖规则
        "max_user_items": 3, # 每人最多上架3件
        "min_price": 1000000, # 拍卖最低起拍价
        "min_increment_percent": 0.1, # 最低加价比例
        "min_bid_increment": 1000000, # 绝对最低加价金额 (新增此字段，用于竞拍逻辑)
        "fee_rate": 0.2 # 拍卖手续费
    },
    "auction_status": { # 拍卖运行时状态，存储在数据库中
        "active": False,
        "start_time": "", # YYYYMMDDhhmmss 格式
        "end_time": "",   # YYYYMMDDhhmmss 格式
        "last_display_refresh_time": "", # YYYYMMDDhhmmss 格式
        "items_count": 0 # 当前拍卖中的物品数量
    }
}

SYSTEM_ITEMS = {
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

def get_system_items() -> Dict[str, Any]:
    """获取系统拍卖物品列表"""
    return SYSTEM_ITEMS

def get_auction_config() -> Dict[str, Any]:
    """加载拍卖配置，如果文件不存在则创建默认配置"""
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
        # 合并新增字段，以防更新后配置文件缺少新字段
        for key, default_value in DEFAULT_CONFIG.items():
            if key not in config:
                config[key] = default_value
            elif isinstance(default_value, dict) and isinstance(config[key], dict):
                # 递归合并嵌套字典，只添加缺少的字段
                for sub_key, sub_value in default_value.items():
                    if sub_key not in config[key]:
                        config[key][sub_key] = sub_value
        return config

def save_config(config: Dict[str, Any]):
    """保存拍卖配置"""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)

def get_auction_schedule() -> Dict[str, Any]:
    """获取拍卖时间表配置"""
    return get_auction_config()["schedule"]

def get_auction_rules() -> Dict[str, Any]:
    """获取拍卖规则配置"""
    return get_auction_config()["rules"]

def get_auction_status_from_config_file() -> Dict[str, Any]:
    """从配置文件中获取拍卖状态配置"""
    return get_auction_config()["auction_status"]

# --- 新增的通用配置设置函数 ---
def set_auction_config_value(key: str, value: Any, sub_key: Optional[str] = None):
    """
    设置拍卖配置中的某个值。
    如果提供了sub_key，则设置config[key][sub_key] = value。
    否则，设置config[key] = value。
    """
    config = get_auction_config()
    if sub_key:
        if key not in config or not isinstance(config[key], dict):
            # 如果主键不存在或不是字典，则创建一个字典来存储子键值
            config[key] = {}
        config[key][sub_key] = value
    else:
        config[key] = value
    save_config(config)

# --- 新增的通用配置获取函数 ---
def get_auction_config_value(key: str, sub_key: Optional[str] = None) -> Any:
    """
    获取拍卖配置中的某个值。
    如果提供了sub_key，则获取config[key][sub_key]。
    否则，获取config[key]。
    """
    config = get_auction_config()
    if key not in config:
        return None # 或根据需要返回默认值
    
    if sub_key:
        if isinstance(config[key], dict) and sub_key in config[key]:
            return config[key][sub_key]
        return None # 或根据需要返回默认值
    return config[key]

# --- update_schedule 函数 ---
def update_schedule(new_schedule: Dict[str, Any]):
    """
    更新拍卖时间表配置。
    new_schedule是一个字典，包含要更新的schedule字段。
    """
    config = get_auction_config()
    config["schedule"].update(new_schedule)
    save_config(config)

# 初始化时确保配置存在
get_auction_config()