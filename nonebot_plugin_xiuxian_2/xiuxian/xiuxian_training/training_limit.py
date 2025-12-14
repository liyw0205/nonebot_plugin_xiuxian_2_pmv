import json
from pathlib import Path
from datetime import datetime, timedelta
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager

player_data_manager = PlayerDataManager()
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"

class TrainingLimit:
    def __init__(self):
        self.table_name = "training"

    def get_user_training_info(self, user_id):
        """获取用户历练信息"""
        user_id = str(user_id)
        user_info = player_data_manager.get_fields(user_id, self.table_name)
        if not user_info:
            default_data = {
                "progress": 0,        # 当前进度(0-12)
                "last_time": None,    # 上次历练时间
                "points": 0,          # 成就点
                "completed": 0,       # 累计完成次数
                "max_progress": 0,    # 历史最高进度
                "last_event": "",      # 最后经历的事件
                "weekly_purchases": {}
            }
            player_data_manager.update_or_write_data(user_id, self.table_name, "progress", default_data["progress"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "last_time", default_data["last_time"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "points", default_data["points"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "completed", default_data["completed"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "max_progress", default_data["max_progress"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "last_event", default_data["last_event"])
            player_data_manager.update_or_write_data(user_id, self.table_name, "weekly_purchases", default_data["weekly_purchases"])
            return default_data
        
        # 转换时间字符串为datetime对象
        if user_info["last_time"] == "None":
            user_info["last_time"] = None
        if user_info["last_time"] and isinstance(user_info["last_time"], str):
            user_info["last_time"] = datetime.strptime(user_info["last_time"], "%Y-%m-%d %H:%M:%S")
        user_info["weekly_purchases"] = json.loads(user_info["weekly_purchases"])
        return user_info
    
    def save_user_training_info(self, user_id, data):
        """保存用户历练信息"""
        user_id = str(user_id)
        save_data = data.copy()
        if isinstance(save_data["last_time"], datetime):
            save_data["last_time"] = save_data["last_time"].strftime("%Y-%m-%d %H:%M:%S")
        weekly_purchases_json = json.dumps(save_data["weekly_purchases"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "progress", save_data["progress"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "last_time", save_data["last_time"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "points", save_data["points"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "completed", save_data["completed"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "max_progress", save_data["max_progress"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "last_event", save_data["last_event"])
        player_data_manager.update_or_write_data(user_id, self.table_name, "weekly_purchases", weekly_purchases_json)

    def get_weekly_purchases(self, user_id, item_id):
        """获取用户本周已购买某商品的数量"""
        data = self.get_user_training_info(user_id)
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
                self.save_user_training_info(user_id, data)
                return 0
            else:
                return user_data.get(item_id, 0)
        else:
            data["weekly_purchases"] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
            self.save_user_training_info(user_id, data)
            return 0

    def update_weekly_purchase(self, user_id, item_id, quantity):
        """更新用户本周购买某商品的数量"""
        data = self.get_user_training_info(user_id)
        user_id = str(user_id)
        item_id = str(item_id)
        
        if "_last_reset" not in data["weekly_purchases"]:
            data["weekly_purchases"] = {"_last_reset": datetime.now().strftime("%Y-%m-%d")}
        else:
            user_data = data["weekly_purchases"]
            current = user_data.get(item_id, 0)
            user_data[item_id] = current + quantity
            data["weekly_purchases"] = user_data
        self.save_user_training_info(user_id, data)

    def reset_limits(self):
        """重置所有历练状态"""
        player_data_manager.update_all_records("training", "last_time", None)

training_limit = TrainingLimit()
