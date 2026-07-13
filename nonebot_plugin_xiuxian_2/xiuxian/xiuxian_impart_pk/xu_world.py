from pathlib import Path
import os

from ..xiuxian_utils.json_store import load_json_file, save_json_file

XU_WORLD_MAX_USERS = 40


class XU_WORLD(object):
    def __init__(self):
        self.dir_path = Path(__file__).parent
        self.data_path = os.path.join(self.dir_path, "x_world.json")
        self.data = load_json_file(self.data_path, {})
        self.service = None

    def bind_service(self, service):
        self.service = service

    def __save(self):
        """
        :return:保存
        """
        save_json_file(self.data_path, self.data)

    def check_xu_world_num(self):
        """
        查看人数
        """
        num = len(self.all_xu_world_user())
        return num < XU_WORLD_MAX_USERS

    def check_xu_world_user_id(self, user_id):
        """
        检查是否加入
        """
        user_id = str(user_id)
        if self.service is not None:
            return self.service.contains(user_id, self.data.keys())
        return bool(self.data.get(user_id))

    def add_xu_world(self, user_id):
        """
        加入虚神界
        """
        user_id = str(user_id)
        if self.check_xu_world_user_id(user_id):
            return "你已经在虚神界中了！"

        if self.check_xu_world_num():
            self.data[user_id] = True
            self.__save()
            return "加入虚神界成功！"
        else:
            return "虚神界人数已满，道友现在无法加入！"

    def del_xu_world(self, user_id):
        """
        加入虚神界
        """
        user_id = str(user_id)
        if self.service is not None:
            return self.service.remove(user_id)
        del self.data[user_id]
        self.__save()

    def all_xu_world_user(self):
        """
        全部虚神界用户
        """
        if self.service is not None:
            return self.service.members(self.data.keys())
        all_user = self.data.keys()
        if all_user is None:
            return None
        else:
            return list(all_user)

    def re_data(self):
        """
        重置数据
        """
        if self.service is not None:
            self.service.reset_daily(self.data.keys())
            return
        self.data = {}
        self.__save()


xu_world = XU_WORLD()
