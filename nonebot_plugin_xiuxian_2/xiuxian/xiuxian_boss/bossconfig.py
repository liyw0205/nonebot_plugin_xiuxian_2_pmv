try:
    import ujson as json
except ImportError:
    import json
import os
from pathlib import Path

configkey = ["Boss灵石", "Boss名字", "Boss倍率", "Boss生成时间参数", 'open', "世界积分商品"]
CONFIG = {
    "open": {
        "000000": {}
            },
    "Boss灵石": {
        '感气境': [10000000, 15000000, 20000000],
        '练气境': [10000000, 15000000, 20000000],
        '筑基境': [10000000, 15000000, 20000000],
        '结丹境': [10000000, 15000000, 20000000],
        '金丹境': [10000000, 15000000, 20000000],
        '元神境': [10000000, 15000000, 20000000],
        '化神境': [20000000, 25000000, 30000000],
        '炼神境': [20000000, 25000000, 30000000],
        '返虚境': [20000000, 25000000, 30000000],
        '大乘境': [20000000, 25000000, 30000000],
        '虚道境': [20000000, 25000000, 30000000],
        '斩我境': [20000000, 25000000, 30000000],
        '遁一境': [30000000, 35000000, 40000000],
        '至尊境': [30000000, 35000000, 40000000],
        '微光境': [40000000, 45000000, 50000000],
        '星芒境': [40000000, 45000000, 50000000],
        '月华境': [45000000, 50000000, 55000000],
        '耀日境': [45000000, 50000000, 55000000],
        '祭道境': [50000000, 55000000, 60000000],
        '自在境': [50000000, 55000000, 60000000],
        '破虚境': [60000000, 65000000, 70000000],
        '无界境': [60000000, 65000000, 70000000],
        '混元境': [60000000, 65000000, 70000000],
        '造化境': [60000000, 65000000, 70000000],
        '永恒境': [60000000, 65000000, 70000000]
    },
    "Boss名字": [
        "九寒",
        "精卫",
        "少姜",
        "陵光",
        "莫女",
        "术方",
        "卫起",
        "血枫",
        "以向",
        "砂鲛",
        "鲲鹏",
        "天龙",
        "莉莉丝",
        "霍德尔",
        "历飞雨",
        "神风王",
        "衣以候",
        "金凰儿",
        "元磁道人",
        "外道贩卖鬼",
        "散发着威压的尸体"
        ],  # 生成的Boss名字，自行修改
    "讨伐世界Boss体力消耗": 10,
    "Boss倍率": {
        # Boss属性：大境界平均修为是基础数值，气血：300倍，真元：10倍，攻击力：0.5倍
        # 作为参考：人物的属性，修为是基础数值，气血：0.5倍，真元：1倍，攻击力：0.1倍
        "气血": 300,
        "真元": 10,
        "攻击": 0.5
    },
    "Boss生成时间参数": {  # Boss生成的时间，2个不可全为0
        "hours": 0,
        "minutes": 45
    },
    "世界积分商品": {
        "1": {
            "id": 1999,
            "cost": 1000,
            "weekly_limit": 10
        },
        "2": {
            "id": 4003,
            "cost": 5000,
            "weekly_limit": 1
        },
        "3": {
            "id": 4002,
            "cost": 25000,
            "weekly_limit": 1
        },
        "4": {
            "id": 4001,
            "cost": 100000,
            "weekly_limit": 1
        },
        "5": {
            "id": 2500,
            "cost": 5000,
            "weekly_limit": 1
        },
        "6": {
            "id": 2501,
            "cost": 10000,
            "weekly_limit": 1
        },
        "7": {
            "id": 2502,
            "cost": 20000,
            "weekly_limit": 1
        },
        "8": {
            "id": 2503,
            "cost": 40000,
            "weekly_limit": 1
        },
        "9": {
            "id": 2504,
            "cost": 80000,
            "weekly_limit": 1
        },
        "10": {
            "id": 2505,
            "cost": 160000,
            "weekly_limit": 1
        },
        "11": {
            "id": 7085,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "12": {
            "id": 8931,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "13": {
            "id": 9937,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "14": {
            "id": 10402,
            "cost": 700000,
            "weekly_limit": 1
        },
        "15": {
            "id": 10403,
            "cost": 1000000,
            "weekly_limit": 1
        },
        "16": {
            "id": 10411,
            "cost": 1200000,
            "weekly_limit": 1
        },
        "17": {
            "id": 20004,
            "cost": 10000,
            "weekly_limit": 10
        },
        "18": {
            "id": 20003,
            "cost": 50000,
            "weekly_limit": 3
        },
        "19": {
            "id": 20002,
            "cost": 200000,
            "weekly_limit": 1
        },
        "20": {
            "id": 20005,
            "cost": 1000,
            "weekly_limit": 10
        },
        "21": {
            "id": 15357,
            "cost": 100000,
            "weekly_limit": 1
        },
        "22": {
            "id": 9935,
            "cost": 100000,
            "weekly_limit": 1
        },
        "23": {
            "id": 9940,
            "cost": 100000,
            "weekly_limit": 1
        },
        "24": {
            "id": 10405,
            "cost": 50000,
            "weekly_limit": 1
        },
        "25": {
            "id": 10410,
            "cost": 1000000,
            "weekly_limit": 1
        },
        "26": {
            "id": 10412,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "27": {
            "id": 8933,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "28": {
            "id": 8934,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "29": {
            "id": 8935,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "30": {
            "id": 8936,
            "cost": 2000000,
            "weekly_limit": 1
        },
        "31": {
            "id": 20011,
            "cost": 10000,
            "weekly_limit": 1
        },
        "32": {
            "id": 20006,
            "cost": 5000,
            "weekly_limit": 1
        }
    }
}


CONFIGJSONPATH = Path(__file__).parent
FILEPATH = CONFIGJSONPATH / 'config.json'

def get_boss_config():
    """加载配置，失败时返回默认配置但不覆盖文件"""
    if not os.path.exists(FILEPATH):
        # 如果文件不存在，保存默认配置
        savef_boss(CONFIG)
        return CONFIG
    config = readf()
    # 确保所有键存在
    for key in configkey:
        if key not in config:
            config[key] = CONFIG[key]
    return config

def readf():
    """读取配置文件"""
    try:
        with open(FILEPATH, "r", encoding="UTF-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"警告: 读取 {FILEPATH} 失败: {e}")
        return CONFIG

def savef_boss(data):
    """保存配置"""
    try:
        # 确保目录存在
        os.makedirs(CONFIGJSONPATH, exist_ok=True)
        with open(FILEPATH, "w", encoding="UTF-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=3)
        return True
    except Exception as e:
        print(f"错误: 保存 {FILEPATH} 失败: {e}")
        return False