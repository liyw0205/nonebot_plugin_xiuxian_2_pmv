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
            "cost": 5000,
            "weekly_limit": 3,
            "required_rank": "白银"
        },
        "20003": {
            "name": "神圣石",
            "cost": 32000,
            "weekly_limit": 2,
            "required_rank": "黄金"
        },
        "20002": {
            "name": "化道石",
            "cost": 88000,
            "weekly_limit": 1,
            "required_rank": "铂金"
        },
        "15357": {
            "name": "八九玄功",
            "cost": 10000,
            "weekly_limit": 1,
            "required_rank": "钻石"
        },
        "20011": {
            "name": "易名符",
            "cost": 1500,
            "weekly_limit": 2,
            "required_rank": "黄金"
        },
        "20019": {
            "name": "解绑符",
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
        return DEFAULT_ARENA_SHOP_CONFIG

arena_shop_data = ArenaShopData()
