import json
import os
from pathlib import Path
from datetime import datetime
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager

player_data_manager = PlayerDataManager()

class TowerLimit:
    def __init__(self):
        self.table_name = "tower"

    def get_user_tower_info(self, user_id):
        """获取用户通天塔信息"""
        user_id = str(user_id)
        user_info = player_data_manager.get_fields(user_id, self.table_name)
        if not user_info:
            default_data = {
                "current_floor": 0,  # 当前层数
                "max_floor": 0,      # 历史最高层数
                "score": 0,          # 总积分
                "weekly_purchases": {}
            }
            player_data_manager.update_or_write_data(user_id, self.table_name, "current_floor", default_data["current_floor"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "max_floor", default_data["max_floor"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "score", default_data["score"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "weekly_purchases", default_data["weekly_purchases"])
            return default_data
        
        user_info["weekly_purchases"] = json.loads(user_info["weekly_purchases"])
        return user_info
    
    def save_user_tower_info(self, user_id, data):
        """保存用户历练信息"""
        user_id = str(user_id)
        save_data = data.copy()
        weekly_purchases_json = json.dumps(save_data["weekly_purchases"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "current_floor", save_data["current_floor"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "max_floor", save_data["max_floor"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "score", save_data["score"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "weekly_purchases", weekly_purchases_json)

    def get_weekly_purchases(self, user_id, item_id):
        """获取用户本周已购买某商品的数量"""
        data = self.get_user_tower_info(user_id)
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
                self.save_user_tower_info(user_id, data)
                return 0
            else:
                return user_data.get(item_id, 0)
        else:
            data["weekly_purchases"] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
            self.save_user_tower_info(user_id, data)
            return 0

    def update_weekly_purchase(self, user_id, item_id, quantity):
        """更新用户本周购买某商品的数量"""
        data = self.get_user_tower_info(user_id)
        user_id = str(user_id)
        item_id = str(item_id)
        
        if "_last_reset" not in data["weekly_purchases"]:
            data["weekly_purchases"] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
        else:
            user_data = data["weekly_purchases"]
            current = user_data.get(item_id, 0)
            user_data[item_id] = current + quantity
            data["weekly_purchases"] = user_data
        self.save_user_tower_info(user_id, data)

    def reset_all_floors(self):
        """重置所有通天塔层数"""
        player_data_manager.update_all_records("tower", "current_floor", 0)

tower_limit = TowerLimit()
