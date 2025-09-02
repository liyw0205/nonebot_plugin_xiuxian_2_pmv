try:
    import ujson as json
except ImportError:
    import json
from pathlib import Path
import os
from nonebot import logger
from .impart_all import impart_all

PATH_PERSON = Path() / "data" / "xiuxian" / "impart"


class IMPART_DATA(object):
    def __init__(self):
        self.dir_path_person = PATH_PERSON
        if not os.path.exists(self.dir_path_person):
            logger.opt(colors=True).info(f"<green>目录不存在，创建目录{self.dir_path_person}</green>")
            os.makedirs(self.dir_path_person)
        self.data_path_person = os.path.join(self.dir_path_person, "impart_person.json")
        self.data_all = impart_all

        try:
            with open(self.data_path_person, 'r', encoding='utf-8') as f:
                self.data_person = json.load(f)
        except:
            self.info = {}
            data = json.dumps(self.info, ensure_ascii=False, indent=4)
            with open(self.data_path_person, mode="x", encoding="UTF-8") as f:
                f.write(data)
                f.close()
            with open(self.data_path_person, 'r', encoding='utf-8') as f:
                self.data_person = json.load(f)

        # 自动转换旧数据格式
        self._convert_old_data_format()
        self.__save()

    def _convert_old_data_format(self):
        """将旧版列表格式数据转换为新版字典格式"""
        converted = False
        for user_id in self.data_person:
            if isinstance(self.data_person[user_id], list):
                # 旧格式是列表，转换为字典
                card_dict = {}
                for card_name in self.data_person[user_id]:
                    card_dict[card_name] = card_dict.get(card_name, 0) + 1
                self.data_person[user_id] = card_dict
                converted = True
        if converted:
            logger.opt(colors=True).info("<yellow>检测到旧版数据格式，已自动转换为新版字典格式</yellow>")

    def __save(self):
        """保存数据"""
        with open(self.data_path_person, 'w', encoding='utf-8') as f:
            json.dump(self.data_person, f, ensure_ascii=False, indent=4)

    def find_user_impart(self, user_id):
        """检查用户是否存在"""
        user_id = str(user_id)
        if user_id in self.data_person:
            return True
        else:
            self.data_person[user_id] = {}  # 新用户初始化为空字典
            self.__save()
            return False

    def data_person_add(self, user_id, name):
        """
        添加单张卡片
        :param user_id: 用户ID
        :param name: 卡片名称
        :return: (是否新卡, 当前该卡片数量)
        """
        user_id = str(user_id)
        if user_id not in self.data_person:
            self.data_person[user_id] = {}
        
        current_count = self.data_person[user_id].get(name, 0)
        is_new = current_count == 0
        
        self.data_person[user_id][name] = current_count + 1
        self.__save()
        
        return is_new, current_count + 1

    def data_person_add_batch(self, user_id, card_names):
        """
        批量添加卡片
        :param user_id: 用户ID
        :param card_names: 卡片名称列表
        :return: (新卡列表, 各卡片的当前数量字典)
        """
        user_id = str(user_id)
        if user_id not in self.data_person:
            self.data_person[user_id] = {}
        
        new_cards = []
        card_counts = {}
        
        # 先统计原始数量
        for name in set(card_names):
            card_counts[name] = self.data_person[user_id].get(name, 0)
        
        # 添加所有卡片
        for name in card_names:
            self.data_person[user_id][name] = self.data_person[user_id].get(name, 0) + 1
        
        self.__save()
        
        # 找出新卡
        for name, count in card_counts.items():
            if count == 0 and name in self.data_person[user_id]:
                new_cards.append(name)
        
        # 返回更新后的数量
        updated_counts = {name: self.data_person[user_id].get(name, 0) for name in card_counts.keys()}
        return new_cards, updated_counts

    def data_person_list(self, user_id):
        """
        获取用户所有卡片
        :param user_id: 用户QQ号
        :return: 字典 {卡名: 数量} 或 None
        """
        user_id = str(user_id)
        try:
            return self.data_person[user_id]
        except KeyError:
            return None

    def data_all_keys(self):
        """获取所有卡片名称列表"""
        return list(self.data_all.keys())

    def data_all_(self):
        """获取所有卡片数据"""
        return self.data_all


impart_data_json = IMPART_DATA()
