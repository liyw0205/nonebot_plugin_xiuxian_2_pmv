try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
import os


class MENTOR_EXP_CD(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "mentor_exp_cd.json")
        try:
            with open(self.data_path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except Exception:
            self.info = {"mentor_exp_cd": {}}
            data = json.dumps(self.info, ensure_ascii=False, indent=4)
            with open(self.data_path, mode="w", encoding="UTF-8") as f:
                f.write(data)
            with open(self.data_path, "r", encoding="utf-8") as f:
                self.data = json.load(f)

    def __save(self):
        with open(self.data_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=4)

    def find_user(self, user_id):
        user_id = str(user_id)
        try:
            if self.data["mentor_exp_cd"][user_id] >= 0:
                return self.data["mentor_exp_cd"][user_id]
        except Exception:
            self.data["mentor_exp_cd"][user_id] = 0
            self.__save()
            return self.data["mentor_exp_cd"][user_id]

    def add_user(self, user_id) -> bool:
        user_id = str(user_id)
        if self.find_user(user_id) >= 0:
            self.data["mentor_exp_cd"][user_id] = self.data["mentor_exp_cd"][user_id] + 1
            self.__save()
            return True
        return False

    def re_data(self):
        self.data = {"mentor_exp_cd": {}}
        self.__save()

    def remove_user(self, user_id, count=1):
        user_id = str(user_id)
        if user_id in self.data["mentor_exp_cd"]:
            current_count = self.data["mentor_exp_cd"][user_id]
            self.data["mentor_exp_cd"][user_id] = max(0, current_count - count)
            self.__save()
            return True
        return False


mentor_exp_cd = MENTOR_EXP_CD()
