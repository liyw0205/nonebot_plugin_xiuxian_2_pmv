try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
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
                "boss_integral": {},  # 每日BOSS积分记录
                "boss_stone": {}      # 每日BOSS灵石记录
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

    def reset_limits(self):
        """重置所有BOSS奖励限制"""
        default_data = {
            "boss_integral": {},
            "boss_stone": {}
        }
        self._save_data(default_data)


boss_limit = BossLimit()