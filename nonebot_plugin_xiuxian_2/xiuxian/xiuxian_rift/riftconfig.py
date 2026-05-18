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


def get_rift_config():
    """直接返回内置配置，不再读取/写入 config.json"""
    return CONFIG
