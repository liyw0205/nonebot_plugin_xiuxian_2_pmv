try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
import os
from threading import Lock

# 创建一个线程锁，用于文件读写操作
file_lock = Lock()


class TWO_EXP_CD(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = self.dir_path / "two_exp_cd.json"
        self.data = self.__load_data()

    def __load_data(self):
        """
        加载数据文件，如果文件不存在则创建
        """
        with file_lock:
            if not self.data_path.exists():
                initial_data = {"two_exp_cd": {}}
                with open(self.data_path, 'w', encoding='utf-8') as f:
                    json.dump(initial_data, f, ensure_ascii=False, indent=4)
                return initial_data
            else:
                try:
                    with open(self.data_path, 'r', encoding='utf-8') as f:
                        return json.load(f)
                except (json.JSONDecodeError, FileNotFoundError):
                    # 如果文件损坏或为空，则重新创建
                    initial_data = {"two_exp_cd": {}}
                    with open(self.data_path, 'w', encoding='utf-8') as f:
                        json.dump(initial_data, f, ensure_ascii=False, indent=4)
                    return initial_data

    def __save(self):
        """
        保存数据到文件
        """
        with file_lock:
            with open(self.data_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=4)

    def find_user(self, user_id):
        """
        查找用户双修次数，如果不存在则初始化为0
        :param user_id:
        """
        user_id = str(user_id)
        if "two_exp_cd" not in self.data:
            self.data["two_exp_cd"] = {}

        return self.data["two_exp_cd"].get(user_id, 0)

    def add_user(self, user_id) -> bool:
        """
        用户双修次数+1
        :param user_id: qq号
        :return: True
        """
        user_id = str(user_id)
        current_count = self.find_user(user_id)
        self.data["two_exp_cd"][user_id] = current_count + 1
        self.__save()
        return True

    def re_data(self):
        """
        重置所有用户数据
        """
        self.data = {"two_exp_cd": {}}
        self.__save()


two_exp_cd = TWO_EXP_CD()