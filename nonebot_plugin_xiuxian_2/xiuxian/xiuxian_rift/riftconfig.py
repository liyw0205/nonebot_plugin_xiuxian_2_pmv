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
            "type_rate": 200,  # 概率
            "rank": 1,  # 增幅等级
            "time": 10,  # 时间，单位分
        },
        "西玄域": {
            "type_rate": 200,
            "rank": 1,
            "time": 10,
        },
        "妖域": {
            "type_rate": 100,
            "rank": 2,
            "time": 20,
        },
        "乱魔海": {
            "type_rate": 100,
            "rank": 2,
            "time": 20,
        },
        "幻雾林": {
            "type_rate": 50,
            "rank": 3,
            "time": 30,
        },
        "狐鸣山": {
            "type_rate": 50,
            "rank": 3,
            "time": 30,
        },
        "云梦泽": {
            "type_rate": 25,
            "rank": 4,
            "time": 40,
        },
        "乱星原": {
            "type_rate": 12,
            "rank": 4,
            "time": 40,
        },
        "黑水湖": {
            "type_rate": 6,
            "rank": 5,
            "time": 50,
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
