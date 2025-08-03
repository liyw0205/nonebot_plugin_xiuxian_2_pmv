try:
    import ujson as json
except ImportError:
    import json
import os
from pathlib import Path

configkey = ["open", "rift"]
CONFIG = {
    "open": [
        "000000",
    ],
    "rift": {
        "东玄域": {
            "type_rate": 30,
            "rank": 1,
            "time": 60,
        },
        "西玄域": {
            "type_rate": 30,
            "rank": 1,
            "time": 60,
        },
        "妖域": {
            "type_rate": 20,
            "rank": 2,
            "time": 90,
        },
        "乱魔海": {
            "type_rate": 20,
            "rank": 2,
            "time": 90,
        },
        "幻雾林": {
            "type_rate": 10,
            "rank": 3,
            "time": 120,
        },
        "狐鸣山": {
            "type_rate": 10,
            "rank": 3,
            "time": 120,
        },
        "云梦泽": {
            "type_rate": 5,
            "rank": 4,
            "time": 150,
        },
        "乱星原": {
            "type_rate": 5,
            "rank": 4,
            "time": 150,
        },
        "黑水湖": {
            "type_rate": 2,
            "rank": 5,
            "time": 180,
        },
        "幽冥谷": {
            "type_rate": 2,
            "rank": 5,
            "time": 180,
        },
        "天剑峰": {
            "type_rate": 1,
            "rank": 5,
            "time": 180,
        },
        "龙渊": {
            "type_rate": 1,
            "rank": 5,
            "time": 180,
        }
    }
}

CONFIGJSONPATH = Path(__file__).parent
FILEPATH = CONFIGJSONPATH / 'config.json'


def get_rift_config():
    try:
        config = readf()
        for key in configkey:
            if key not in list(config.keys()):
                config[key] = CONFIG[key]
        savef_rift(config)
    except:
        config = CONFIG
        savef_rift(config)
    return config


def readf():
    with open(FILEPATH, "r", encoding="UTF-8") as f:
        data = f.read()
    return json.loads(data)


def savef_rift(data):
    data = json.dumps(data, ensure_ascii=False, indent=3)
    savemode = "w" if os.path.exists(FILEPATH) else "x"
    with open(FILEPATH, mode=savemode, encoding="UTF-8") as f:
        f.write(data)
        f.close()
    return True
