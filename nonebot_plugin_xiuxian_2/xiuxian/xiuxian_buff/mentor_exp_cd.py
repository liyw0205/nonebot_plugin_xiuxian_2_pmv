from pathlib import Path
import os

from ..xiuxian_utils.json_store import load_json_file, save_json_file


class MENTOR_EXP_CD(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "mentor_exp_cd.json")
        self.data = load_json_file(self.data_path, {"mentor_exp_cd": {}})

    def __save(self):
        save_json_file(self.data_path, self.data)

    def find_user(self, user_id):
        user_id = str(user_id)
        if user_id not in self.data["mentor_exp_cd"]:
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
