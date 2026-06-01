DEFAULT_CONFIG = {
    "体力消耗": {
        "单层爬塔": 5,
        "连续爬塔": 20
    },
    "积分奖励": {
        "每层基础": 100,
        "每10层额外": 500
    },
    "灵石奖励": {
        "每层基础": 1000000,
        "每10层额外": 5000000
    },
    "修为奖励": {
        "每10层": 0.001
    },
    "商店商品": {
        "1999": {
            "name": "渡厄丹",
            "cost": 1000,
            "weekly_limit": 10
        },
        "20012": {
            "name": "秘境加速券",
            "cost": 10000,
            "weekly_limit": 5
        },
        "20004": {
            "name": "蕴灵石",
            "cost": 10000,
            "weekly_limit": 10
        },
        "20003": {
            "name": "神圣石",
            "cost": 50000,
            "weekly_limit": 3
        },
        "20002": {
            "name": "化道石",
            "cost": 200000,
            "weekly_limit": 1
        },
        "20005": {
            "name": "祈愿石",
            "cost": 2000,
            "weekly_limit": 10
        },
        "15357": {
            "name": "八九玄功",
            "cost": 100000,
            "weekly_limit": 1
        },
        "9935": {
            "name": "暗渊灭世功",
            "cost": 100000,
            "weekly_limit": 1
        },
        "9940": {
            "name": "化功大法",
            "cost": 100000,
            "weekly_limit": 1
        },
        "10405": {
            "name": "醉仙",
            "cost": 50000,
            "weekly_limit": 1
        },
        "20011": {
            "name": "易名符",
            "cost": 10000,
            "weekly_limit": 1
        },
        "20006": {
            "name": "福缘石",
            "cost": 5000,
            "weekly_limit": 1
        },
        "20023": {
            "name": "洗练石",
            "cost": 8000,
            "weekly_limit": 10
        },
        "20027": {
            "name": "一阶天地灵髓",
            "cost": 1200,
            "weekly_limit": 5
        },
        "20028": {
            "name": "二阶天地灵髓",
            "cost": 2400,
            "weekly_limit": 4
        },
        "20029": {
            "name": "三阶天地灵髓",
            "cost": 4500,
            "weekly_limit": 3
        },
        "20032": {
            "name": "启明石",
            "cost": 60000,
            "weekly_limit": 1
        },
        "20009": {
            "name": "神秘经书",
            "cost": 6000,
            "weekly_limit": 1
        },
        "18133": {
            "name": "随机一至三阶饰品礼包",
            "cost": 8000,
            "weekly_limit": 1
        },
        "18134": {
            "name": "随机四至五阶饰品礼包",
            "cost": 26000,
            "weekly_limit": 1
        },
        "20033": {
            "name": "山河灵宠蛋",
            "cost": 3000,
            "weekly_limit": 1
        },
        "20034": {
            "name": "云纹灵宠蛋",
            "cost": 6000,
            "weekly_limit": 1
        },
        "20035": {
            "name": "星辉灵宠蛋",
            "cost": 12000,
            "weekly_limit": 1
        },
        "20036": {
            "name": "天命灵宠蛋",
            "cost": 30000,
            "weekly_limit": 1
        },
        "20037": {
            "name": "太初灵宠蛋",
            "cost": 70000,
            "weekly_limit": 1
        },
        "15601": {
            "name": "血刹碎乾坤",
            "cost": 300000,
            "weekly_limit": 1
        },
        "12006": {
            "name": "银月忘川之铠甲",
            "cost": 360000,
            "weekly_limit": 1
        }
    },
    "重置时间": {
        "day_of_week": "mon",  # 每周一
        "hour": 0,
        "minute": 0
    }
}

class TowerData:
    def __init__(self):
        self.config = self.get_tower_config()
    
    def get_tower_config(self):
        """加载通天塔配置"""
        return DEFAULT_CONFIG

tower_data = TowerData()
