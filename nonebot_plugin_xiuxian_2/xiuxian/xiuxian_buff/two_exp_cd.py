try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
import os


class TWO_EXP_CD(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "two_exp_cd.json")
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        except:
            self.info = {"two_exp_cd": {}}
            data = json.dumps(self.info, ensure_ascii=False, indent=4)
            with open(self.data_path, mode="x", encoding="UTF-8") as f:
                f.write(data)
                f.close()
            with open(self.data_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)

    def __save(self):
        """
        :return:保存
        """
        with open(self.data_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=4)

    def find_user(self, user_id):
        """
        匹配词条
        :param user_id:
        """
        user_id = str(user_id)
        try:
            if self.data["two_exp_cd"][user_id] >= 0:
                return self.data["two_exp_cd"][user_id]
        except:
            self.data["two_exp_cd"][user_id] = 0
            self.__save()
            return self.data["two_exp_cd"][user_id]

    def add_user(self, user_id) -> bool:
        """
        加入数据
        :param user_id: qq号
        :return: True or False
        """
        user_id = str(user_id)
        if self.find_user(user_id) >= 0:
            self.data["two_exp_cd"][user_id] = self.data["two_exp_cd"][user_id] + 1
            self.__save()
            return True

    def re_data(self):
        """
        重置数据
        """
        self.data = {"two_exp_cd": {}}
        self.__save()

    def remove_user(self, user_id, count=1):
        """减少用户的CD计数"""
        user_id = str(user_id)
        if user_id in self.data["two_exp_cd"]:
            current_count = self.data["two_exp_cd"][user_id]
            new_count = max(0, current_count - count)
            self.data["two_exp_cd"][user_id] = new_count
            self.__save()
            return True
        return False

two_exp_cd = TWO_EXP_CD()
