import json
import os
from pathlib import Path

ARENA_SHOP_CONFIG_PATH = Path(__file__).parent / "arena_shop_config.json"

DEFAULT_ARENA_SHOP_CONFIG = {
    "商店商品": {
        "1999": {
            "name": "渡厄丹",
            "cost": 500,
            "weekly_limit": 5,
            "required_rank": "青铜"
        },
        "20004": {
            "name": "蕴灵石", 
            "cost": 1000,
            "weekly_limit": 3,
            "required_rank": "白银"
        },
        "20003": {
            "name": "神圣石",
            "cost": 2000,
            "weekly_limit": 2,
            "required_rank": "黄金"
        },
        "20002": {
            "name": "化道石",
            "cost": 5000,
            "weekly_limit": 1,
            "required_rank": "铂金"
        },
        "15357": {
            "name": "八九玄功",
            "cost": 8000,
            "weekly_limit": 1,
            "required_rank": "钻石"
        },
        "20011": {
            "name": "易名符",
            "cost": 1500,
            "weekly_limit": 2,
            "required_rank": "黄金"
        }
    }
}

class ArenaShopData:
    def __init__(self):
        self.config = self.get_arena_shop_config()
    
    def get_arena_shop_config(self):
        """加载竞技场商店配置"""
        try:
            if not ARENA_SHOP_CONFIG_PATH.exists():
                with open(ARENA_SHOP_CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(DEFAULT_ARENA_SHOP_CONFIG, f, indent=4, ensure_ascii=False)
                return DEFAULT_ARENA_SHOP_CONFIG
            
            with open(ARENA_SHOP_CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
            
            # 确保所有配置项都存在
            for key in DEFAULT_ARENA_SHOP_CONFIG:
                if key not in config:
                    config[key] = DEFAULT_ARENA_SHOP_CONFIG[key]
            
            return config
        except Exception as e:
            print(f"加载竞技场商店配置失败: {e}")
            return DEFAULT_ARENA_SHOP_CONFIG

arena_shop_data = ArenaShopData()
