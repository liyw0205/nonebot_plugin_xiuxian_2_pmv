try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
from datetime import datetime
import os

class BossLimit:
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "boss_limit.json")
        self._init_data()

    def _init_data(self):
        """初始化数据文件"""
        if not os.path.exists(self.data_path):
            default_data = {
                "boss_integral": {},   # 每日BOSS积分记录
                "boss_stone": {},      # 每日BOSS灵石记录
                "weekly_purchases": {}, # 每周商品购买记录
                "boss_battle_count": {} # 每日讨伐次数记录
            }
            self._save_data(default_data)

    def _load_data(self):
        """加载数据"""
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._init_data()
            return self._load_data()

    def _save_data(self, data):
        """保存数据"""
        with open(self.data_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def get_integral(self, user_id):
        """获取用户今日已获得BOSS积分"""
        data = self._load_data()
        return data["boss_integral"].get(str(user_id), 0)

    def get_stone(self, user_id):
        """获取用户今日已获得BOSS灵石"""
        data = self._load_data()
        return data["boss_stone"].get(str(user_id), 0)

    def get_battle_count(self, user_id):
        """获取用户今日讨伐次数"""
        data = self._load_data()
        return data["boss_battle_count"].get(str(user_id), 0)

    def update_integral(self, user_id, amount):
        """更新用户今日BOSS积分"""
        data = self._load_data()
        user_id = str(user_id)
        data["boss_integral"][user_id] = data["boss_integral"].get(user_id, 0) + amount
        self._save_data(data)

    def update_stone(self, user_id, amount):
        """更新用户今日BOSS灵石"""
        data = self._load_data()
        user_id = str(user_id)
        data["boss_stone"][user_id] = data["boss_stone"].get(user_id, 0) + amount
        self._save_data(data)

    def update_battle_count(self, user_id):
        """更新用户今日讨伐次数"""
        data = self._load_data()
        user_id = str(user_id)
        current_count = data["boss_battle_count"].get(user_id, 0)
        data["boss_battle_count"][user_id] = current_count + 1
        self._save_data(data)

    def get_weekly_purchases(self, user_id, item_id):
        """获取用户本周已购买某商品的数量"""
        data = self._load_data()
        user_id = str(user_id)
        item_id = str(item_id)
        
        if user_id in data["weekly_purchases"]:
            user_data = data["weekly_purchases"][user_id]
            if "_last_reset" in user_data:
                last_reset = datetime.strptime(user_data["_last_reset"], "%Y-%m-%d")
                current_week = datetime.now().isocalendar()[1]
                last_week = last_reset.isocalendar()[1]
                current_year = datetime.now().year
                last_year = last_reset.year
                
                if current_week != last_week or current_year != last_year:
                    data["weekly_purchases"][user_id] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
                    self._save_data(data)
                    return 0
            else:
                data["weekly_purchases"][user_id] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
                self._save_data(data)
                return 0
        
        return data["weekly_purchases"].get(user_id, {}).get(item_id, 0)

    def update_weekly_purchase(self, user_id, item_id, quantity):
        """更新用户本周购买某商品的数量"""
        data = self._load_data()
        user_id = str(user_id)
        item_id = str(item_id)
        
        if user_id not in data["weekly_purchases"]:
            data["weekly_purchases"][user_id] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
        
        current = data["weekly_purchases"][user_id].get(item_id, 0)
        data["weekly_purchases"][user_id][item_id] = current + quantity
        
        self._save_data(data)

    def reset_limits(self):
        """重置所有每日BOSS奖励限制"""
        data = self._load_data()
        data["boss_integral"] = {}
        data["boss_stone"] = {}
        data["boss_battle_count"] = {}  # 重置讨伐次数
        self._save_data(data)

boss_limit = BossLimit()
