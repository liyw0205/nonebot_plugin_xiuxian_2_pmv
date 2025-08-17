try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
import os


class STONE_LIMIT(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "stone_limit.json")
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        except:
            self.info = {"send_limit": {}, "receive_limit": {}}
            data = json.dumps(self.info, ensure_ascii=False, indent=4)
            with open(self.data_path, mode="x", encoding="UTF-8") as f:
                f.write(data)
                f.close()
            with open(self.data_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)

    def __save(self):
        """保存数据"""
        with open(self.data_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=4)

    def get_send_limit(self, user_id):
        """获取用户今日已送灵石额度"""
        user_id = str(user_id)
        try:
            return self.data["send_limit"].get(user_id, 0)
        except KeyError:
            self.data["send_limit"][user_id] = 0
            self.__save()
            return 0

    def get_receive_limit(self, user_id):
        """获取用户今日已收灵石额度"""
        user_id = str(user_id)
        try:
            return self.data["receive_limit"].get(user_id, 0)
        except KeyError:
            self.data["receive_limit"][user_id] = 0
            self.__save()
            return 0

    def update_send_limit(self, user_id, amount):
        """更新用户今日已送灵石额度"""
        user_id = str(user_id)
        self.data["send_limit"][user_id] = self.get_send_limit(user_id) + amount
        self.__save()

    def update_receive_limit(self, user_id, amount):
        """更新用户今日已收灵石额度"""
        user_id = str(user_id)
        self.data["receive_limit"][user_id] = self.get_receive_limit(user_id) + amount
        self.__save()

    def reset_limits(self):
        """重置所有额度"""
        self.data = {"send_limit": {}, "receive_limit": {}}
        self.__save()


stone_limit = STONE_LIMIT()
