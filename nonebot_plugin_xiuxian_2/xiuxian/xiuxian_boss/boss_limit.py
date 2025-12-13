try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
from datetime import datetime
import os
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager
player_data_manager = PlayerDataManager()

class BossLimit:
    def __init__(self):
        self.default_data = {
            "boss_integral": 0,   # 每日BOSS积分记录
            "boss_stone": 0,      # 每日BOSS灵石记录
            "weekly_purchases": {}, # 每周商品购买记录
            "boss_battle_count": 0 # 每日讨伐次数记录
        }

    def _load_data(self, user_id):
        """加载数据"""
        data = player_data_manager.get_fields(user_id, "boss_limit")
        if data is None:
            self._save_data(user_id, self.default_data)
            return self.default_data
        data["weekly_purchases"] = json.loads(data["weekly_purchases"])
        return data

    def _save_data(self, user_id, data):
        """保存数据"""
        player_data_manager.update_or_write_data(user_id, "boss_limit", "boss_integral", data["boss_integral"])
        player_data_manager.update_or_write_data(user_id, "boss_limit", "boss_stone", data["boss_stone"])
        player_data_manager.update_or_write_data(user_id, "boss_limit", "boss_battle_count", data["boss_battle_count"])
        player_data_manager.update_or_write_data(user_id, "boss_limit", "weekly_purchases", weekly_purchases_json)

    def get_integral(self, user_id):
        """获取用户今日已获得BOSS积分"""
        data = self._load_data(user_id)
        return data.get("boss_integral", 0)

    def get_stone(self, user_id):
        """获取用户今日已获得BOSS灵石"""
        data = self._load_data(user_id)
        return data.get("boss_stone", 0)

    def get_battle_count(self, user_id):
        """获取用户今日讨伐次数"""
        data = self._load_data(user_id)
        return data.get("boss_battle_count", 0)

    def update_integral(self, user_id, amount):
        """更新用户今日BOSS积分"""
        data = self._load_data(user_id)
        data["boss_integral"] = data.get("boss_integral", 0) + amount
        self._save_data(user_id, data)

    def update_stone(self, user_id, amount):
        """更新用户今日BOSS灵石"""
        data = self._load_data(user_id)
        data["boss_stone"] = data.get("boss_stone", 0) + amount
        self._save_data(user_id, data)

    def update_battle_count(self, user_id):
        """更新用户今日讨伐次数"""
        data = self._load_data(user_id)
        current_count = data.get("boss_battle_count", 0)
        data["boss_battle_count"] = current_count + 1
        self._save_data(user_id, data)

    def get_weekly_purchases(self, user_id, item_id):
        """获取用户本周已购买某商品的数量"""
        data = self._load_data(user_id)
        user_id = str(user_id)
        item_id = str(item_id)
        
        user_data = data["weekly_purchases"]
        if "_last_reset" in user_data:
            last_reset = datetime.strptime(user_data.get("_last_reset"), "%Y-%m-%d")
            current_week = datetime.now().isocalendar()[1]
            last_week = last_reset.isocalendar()[1]
            current_year = datetime.now().year
            last_year = last_reset.year
                
            if current_week != last_week or current_year != last_year:
                data["weekly_purchases"] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
                self._save_data(user_id, data)
                return 0
            else:
                return user_data.get(item_id, 0)
        else:
            data["weekly_purchases"] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
            self._save_data(user_id, data)
            return 0

    def update_weekly_purchase(self, user_id, item_id, quantity):
        """更新用户本周购买某商品的数量"""
        data = self._load_data(user_id)
        user_id = str(user_id)
        item_id = str(item_id)
        
        if "_last_reset" not in data["weekly_purchases"]:
            data["weekly_purchases"] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
        else:
            user_data = data["weekly_purchases"]
            current = user_data.get(item_id, 0)
            user_data[item_id] = current + quantity
            data["weekly_purchases"] = user_data
        self._save_data(user_id, data)

    def reset_limits(self, user_id):
        """重置所有每日BOSS奖励限制"""
        data = self._load_data(user_id)
        data["boss_integral"] = 0
        data["boss_stone"] = 0
        data["boss_battle_count"] = 0  # 重置讨伐次数
        self._save_data(user_id, data)

boss_limit = BossLimit()
