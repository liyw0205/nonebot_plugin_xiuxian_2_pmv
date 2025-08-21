import json
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "auction_config.json"
AUCTION_DATA_PATH = Path(__file__).parent / "auctions_data"
AUCTION_DATA_PATH.mkdir(parents=True, exist_ok=True)

DEFAULT_CONFIG = {
    "system_items": {
        "安神灵液": {"id": 1412, "start_price": 550000},
        "魇龙之血": {"id": 1413, "start_price": 550000},
        "化劫丹": {"id": 1414, "start_price": 700000},
        "太上玄门丹": {"id": 1415, "start_price": 15000000},
        "金仙破厄丹": {"id": 1416, "start_price": 20000000},
        "太乙炼髓丹": {"id": 1417, "start_price": 50000000},
        "地仙玄丸": {"id": 2014, "start_price": 500000},
        "消冰宝丸": {"id": 2015, "start_price": 1000000},
        "遁一丹": {"id": 1418, "start_price": 70000000},
        "至尊丹": {"id": 1419, "start_price": 100000000},
        "极品至尊丹": {"id": 1421, "start_price": 300000000},
        "极品遁一丹": {"id": 1420, "start_price": 150000000},
        "生骨丹": {"id": 1101, "start_price": 1000},
        "化瘀丹": {"id": 1102, "start_price": 3000},
        "培元丹": {"id": 1103, "start_price": 5000},
        "培元丹plus": {"id": 1104, "start_price": 10000},
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
        "风神诀": {"id": 9926, "start_price": 350000000},
        "三丰丹经": {"id": 9920, "start_price": 8500000000},
        "暗渊灭世功": {"id": 9935, "start_price": 1200000000},
        "太清丹经": {"id": 9933, "start_price": 10000000000000},
        "大道归一丹": {"id": 15102, "start_price": 500000000},
        "天地玄功": {"id": 9934, "start_price": 11500000000000},
        "渡劫天功": {"id": 9931, "start_price": 1200000000},
        "道师符经": {"id": 9921, "start_price": 55000000000}
    },
    "schedule": {
        "start_hour": 17,
        "start_minute": 0,
        "duration_hours": 2,
        "enabled": True
    },
    "rules": {
        "max_user_items": 3,
        "min_price": 1000000,  # 调整为100万
        "fee_rate": 0.2
    }
}

def get_auction_config():
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG
    
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
        # 合并新增字段
        for key, default_value in DEFAULT_CONFIG.items():
            if key not in config:
                config[key] = default_value
            elif isinstance(default_value, dict):
                for sub_key, sub_value in default_value.items():
                    if sub_key not in config[key]:
                        config[key][sub_key] = sub_value
        return config

def save_config(config):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)

def get_system_items():
    return get_auction_config()["system_items"]

def get_auction_schedule():
    return get_auction_config()["schedule"]

def get_auction_rules():
    return get_auction_config()["rules"]

def update_schedule(new_schedule):
    config = get_auction_config()
    config["schedule"].update(new_schedule)
    save_config(config)