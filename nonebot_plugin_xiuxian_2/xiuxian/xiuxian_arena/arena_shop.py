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
        },
        "20024": {
            "name": "竞技场挑战券",
            "cost": 800,
            "weekly_limit": 5,
            "required_rank": "青铜"
        },
        "20023": {
            "name": "洗练石",
            "cost": 1200,
            "weekly_limit": 10,
            "required_rank": "白银"
        },
        "18132": {
            "name": "随机一阶戒指饰品礼包",
            "cost": 6000,
            "weekly_limit": 1,
            "required_rank": "黄金"
        },
        "20032": {
            "name": "启明石",
            "cost": 8000,
            "weekly_limit": 1,
            "required_rank": "铂金"
        },
        "18133": {
            "name": "随机一至三阶饰品礼包",
            "cost": 18000,
            "weekly_limit": 1,
            "required_rank": "铂金"
        },
        "20033": {
            "name": "山河灵宠蛋",
            "cost": 300,
            "weekly_limit": 1,
            "required_rank": "青铜"
        },
        "20034": {
            "name": "云纹灵宠蛋",
            "cost": 800,
            "weekly_limit": 1,
            "required_rank": "白银"
        },
        "20035": {
            "name": "星辉灵宠蛋",
            "cost": 2200,
            "weekly_limit": 1,
            "required_rank": "铂金"
        },
        "20036": {
            "name": "天命灵宠蛋",
            "cost": 6000,
            "weekly_limit": 1,
            "required_rank": "钻石"
        },
        "20037": {
            "name": "太初灵宠蛋",
            "cost": 16000,
            "weekly_limit": 1,
            "required_rank": "王者"
        },
        "15600": {
            "name": "紫玄掌",
            "cost": 120000,
            "weekly_limit": 1,
            "required_rank": "王者"
        },
        "13001": {
            "name": "轩辕剑",
            "cost": 160000,
            "weekly_limit": 1,
            "required_rank": "王者"
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
