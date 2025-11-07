try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
import os
from datetime import datetime

class LOTTERY_POOL(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "lottery_pool.json")
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        except:
            self.info = {
                "pool": 0,
                "participants": [],
                "last_winner": None
            }
            data = json.dumps(self.info, ensure_ascii=False, indent=4)
            with open(self.data_path, mode="x", encoding="UTF-8") as f:
                f.write(data)
            with open(self.data_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)

    def __save(self):
        """保存数据"""
        with open(self.data_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=4)

    def get_pool(self):
        """获取当前奖池金额"""
        return self.data["pool"]

    def get_participants(self):
        """获取今日参与人数"""
        return len(self.data["participants"])

    def get_last_winner(self):
        """获取上次中奖信息"""
        return self.data["last_winner"]

    def add_participant(self, user_id):
        """添加参与者"""
        user_id = str(user_id)
        if user_id not in self.data["participants"]:
            self.data["participants"].append(user_id)
        self.__save()

    def deposit_to_pool(self, amount):
        """存入奖池"""
        self.data["pool"] += amount
        self.__save()

    def set_winner(self, user_id, user_name, amount, lottery_number):
        """设置中奖者并从奖池中减去中奖金额"""
        if amount > self.data["pool"]:
            amount = self.data["pool"]
        
        self.data["pool"] = max(0, self.data["pool"] - amount)
        
        self.data["last_winner"] = {
            'name': user_name,
            'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'amount': amount,
            'lottery_number': lottery_number
        }
        self.__save()

    def reset_daily(self):
        """每日重置参与记录（奖池不清空）"""
        self.data["participants"] = []
        self.__save()

lottery_pool = LOTTERY_POOL()
