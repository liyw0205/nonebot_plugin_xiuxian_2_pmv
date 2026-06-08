try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
import os

class IMPART_PK(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "impart_pk.json")
        try:
            with open(self.data_path, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        except:
            self.info = {}
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

    def _default_user_data(self, user_number):
        return {
            "number": user_number,
            "pk_num": 7,
            "win_num": 0,
            "impart_num": 10,
            "exp_used": 0,
            "exp_count": 0,
            "exp_load": 0,
            "exp_gain": 0
        }

    def _ensure_user_fields(self, user_id):
        user_id = str(user_id)
        if user_id not in self.data:
            user_number = len(self.data) + 1
            self.data[user_id] = self._default_user_data(user_number)
            self.__save()
            return False

        defaults = self._default_user_data(self.data[user_id].get("number", len(self.data)))
        changed = False
        for key, value in defaults.items():
            if key not in self.data[user_id]:
                self.data[user_id][key] = value
                changed = True
        if changed:
            self.__save()
        return True

    def check_user_impart(self, user_id):
        """
        核对用户是否存在
        :param user_id:
        """
        return self._ensure_user_fields(user_id)

    def find_user_data(self, user_id):
        """
        匹配用户数据
        :param user_id:
        """
        user_id = str(user_id)
        self.check_user_impart(user_id)
        try:
            data_ = self.data[user_id]
            return data_
        except KeyError:
            return None

    def update_user_data(self, user_id, type_):
        """
        更新用户数据
        :param type_: TRUE or FALSE
        :param user_id:
        """
        user_id = str(user_id)
        self.check_user_impart(user_id)
        if type_:
            self.data[user_id]["win_num"] += 1
            self.__save()
            return True
        else:
            self.data[user_id]["pk_num"] -= 1
            self.__save()
            return True
            
    def update_user_impart_lv(self, user_id):
        """
        更新用户数据
        :param user_id:
        """
        user_id = str(user_id)
        self.check_user_impart(user_id)
        self.data[user_id]["impart_num"] -= 1
        self.__save()

    def add_exp_cultivation(self, user_id, exp_time, exp_load, exp_gain):
        """记录当日虚神界修炼已消耗时间、神魂承载百分比与已获得修为。"""
        user_id = str(user_id)
        self.check_user_impart(user_id)
        self.data[user_id]["exp_used"] += int(exp_time)
        self.data[user_id]["exp_count"] += 1
        current_load = max(0, int(self.data[user_id].get("exp_load", 0) or 0))
        self.data[user_id]["exp_load"] = min(100, current_load + max(0, int(exp_load)))
        self.data[user_id]["exp_gain"] += int(exp_gain)
        self.__save()

    def all_user_data(self):
        """
        查找所有用户数据
        """
        try:
            dict_ = self.data
            return dict_
        except KeyError:
            return None

    def re_data(self):
        """
        重置数据
        """
        self.data = {}
        self.__save()


impart_pk = IMPART_PK()
