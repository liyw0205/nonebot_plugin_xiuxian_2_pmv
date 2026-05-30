DEFAULT_CONFIG = {
    "商店商品": {
        "1999": {
            "name": "渡厄丹",
            "cost": 1000,
            "weekly_limit": 10
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
            "cost": 3000,
            "weekly_limit": 8
        },
        "20027": {
            "name": "一阶天地灵髓",
            "cost": 500,
            "weekly_limit": 5
        },
        "20028": {
            "name": "二阶天地灵髓",
            "cost": 1000,
            "weekly_limit": 4
        },
        "20029": {
            "name": "三阶天地灵髓",
            "cost": 2200,
            "weekly_limit": 3
        },
        "20033": {
            "name": "山河灵宠蛋",
            "cost": 1200,
            "weekly_limit": 1
        },
        "20034": {
            "name": "云纹灵宠蛋",
            "cost": 3000,
            "weekly_limit": 1
        },
        "20035": {
            "name": "星辉灵宠蛋",
            "cost": 9000,
            "weekly_limit": 1
        },
        "20036": {
            "name": "天命灵宠蛋",
            "cost": 22000,
            "weekly_limit": 1
        },
        "20037": {
            "name": "太初灵宠蛋",
            "cost": 50000,
            "weekly_limit": 1
        },
        "18132": {
            "name": "随机一阶戒指饰品礼包",
            "cost": 1800,
            "weekly_limit": 1
        },
        "18133": {
            "name": "随机一至三阶饰品礼包",
            "cost": 6000,
            "weekly_limit": 1
        },
        "18159": {
            "name": "随机二阶套装礼包",
            "cost": 180000,
            "weekly_limit": 1
        },
        "15602": {
            "name": "无暇七绝剑",
            "cost": 260000,
            "weekly_limit": 1
        },
        "13002": {
            "name": "黑龙啸天印",
            "cost": 360000,
            "weekly_limit": 1
        }
    }
}

class TrainingData:
    def __init__(self):
        self.config = self.get_training_config()
    
    def get_training_config(self):
        """加载历练配置"""
        return DEFAULT_CONFIG
    
training_data = TrainingData()
