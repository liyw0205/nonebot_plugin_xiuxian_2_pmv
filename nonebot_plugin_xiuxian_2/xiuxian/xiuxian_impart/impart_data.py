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

        try:
            for key in self.data_person:
                if type(self.data_person[key]) is dict:
                    self.data_person[key] = list(self.data_person[key].keys())
                elif type(self.data_person[key]) is list:
                    pass
                else:
                    logger.opt(colors=True).info("<red>传承数据有未知类型错误！请检查！</red>")
        except:
            pass
        self.__save()

    def __save(self):
        """
        :return:保存
        """
        with open(self.data_path_person, 'w', encoding='utf-8') as f:
            json.dump(self.data_person, f, ensure_ascii=False, indent=4)

    def find_user_impart(self, user_id):
        """
        匹配词条
        :param user_id:
        """
        user_id = str(user_id)
        if user_id in self.data_person:
            return True
        else:
            self.data_person[user_id] = list()
            self.__save()
            return False

    def data_person_add(self, user_id, name):
        """
        添加词条
        :param name: 卡片名称
        :param user_id: 用户ID
        :return: (是否新卡, 当前该卡片数量)
        """
        user_id = str(user_id)
        if user_id not in self.data_person:
            self.data_person[user_id] = []
    
        # 统计当前用户拥有的该卡片数量
        current_count = self.data_person[user_id].count(name)
    
        # 添加卡片
        self.data_person[user_id].append(name)
        self.__save()
    
        # 返回是否新卡和当前总数
        return current_count == 0, current_count + 1

    def data_person_add_batch(self, user_id, card_names):
        """
        批量添加词条
        :param user_id: 用户ID
        :param card_names: 卡片名称列表
        :return: (新卡列表, 各卡片的当前数量字典)
        """
        user_id = str(user_id)
        if user_id not in self.data_person:
            self.data_person[user_id] = []
        
        new_cards = []
        card_counts = {}
        
        # 先统计原始数量
        for name in card_names:
            card_counts[name] = self.data_person[user_id].count(name)
        
        # 添加所有卡片
        self.data_person[user_id].extend(card_names)
        self.__save()
        
        # 找出新卡
        for name, count in card_counts.items():
            if count == 0:
                new_cards.append(name)
        
        return new_cards, {name: count + card_names.count(name) for name, count in card_counts.items()}

    def data_person_list(self, user_id):
        """
        查找所有传承卡片
        :param user_id: qq号
        """
        user_id = str(user_id)
        list_data_person_list = None
        try:
            list_data_person_list = self.data_person[user_id]
            return list_data_person_list
        except:
            return list_data_person_list

    def data_all_keys(self):
        return list(self.data_all.keys())

    def data_all_(self):
        return self.data_all


impart_data_json = IMPART_DATA()
