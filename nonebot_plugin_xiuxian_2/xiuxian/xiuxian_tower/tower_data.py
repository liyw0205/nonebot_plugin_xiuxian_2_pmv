import json
from pathlib import Path

TOWER_CONFIG_PATH = Path(__file__).parent / "tower_config.json"

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
        try:
            if not TOWER_CONFIG_PATH.exists():
                with open(TOWER_CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
                return DEFAULT_CONFIG
            
            with open(TOWER_CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            # 确保所有配置项都存在
            for key in DEFAULT_CONFIG:
                if key not in config:
                    config[key] = DEFAULT_CONFIG[key]
            
            return config
        except Exception as e:
            print(f"加载通天塔配置失败: {e}")
            return DEFAULT_CONFIG

tower_data = TowerData()
