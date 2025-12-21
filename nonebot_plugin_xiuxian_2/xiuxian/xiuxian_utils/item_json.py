try:
    import ujson as json
except ImportError:
    import json
import os
import asyncio
from pathlib import Path
from typing import List
from nonebot.log import logger

READPATH = Path() / "data" / "xiuxian"
SKILLPATH = READPATH / "功法"
WEAPONPATH = READPATH / "装备"
ELIXIRPATH = READPATH / "丹药"
PACKAGESPATH = READPATH / "礼包"
XIULIANITEMPATH = READPATH / "修炼物品"
ITEMSFILEPATH = Path() / "data" / "xiuxian" / "items.json"
items_num = "123451234"

ITEMS_CACHE = {}

class Items:
    global items_num
    _instance = {}
    _has_init = {}

    def __new__(cls):
        if cls._instance.get(items_num) is None:
            cls._instance[items_num] = super(Items, cls).__new__(cls)
        return cls._instance[items_num]

    def __init__(self) -> None:
        global ITEMS_CACHE
        self.mainbuff_jsonpath = SKILLPATH / "主功法.json"
        self.subbuff_jsonpath = SKILLPATH / "辅修功法.json" 
        self.secbuff_jsonpath = SKILLPATH / "神通.json"
        self.effect1buff_jsonpath = SKILLPATH / "身法.json"
        self.effect2buff_jsonpath = SKILLPATH / "瞳术.json"
        self.weapon_jsonpath = WEAPONPATH / "法器.json"
        self.armor_jsonpath = WEAPONPATH / "防具.json"
        self.elixir_jsonpath = ELIXIRPATH / "丹药.json"
        self.lb_jsonpath = PACKAGESPATH / "礼包.json"
        self.yaocai_jsonpath = ELIXIRPATH / "药材.json"
        self.mix_elixir_type_jsonpath = ELIXIRPATH / "炼丹丹药.json"
        self.ldl_jsonpath = ELIXIRPATH / "炼丹炉.json"
        self.jlq_jsonpath = XIULIANITEMPATH / "聚灵旗.json"
        self.sw_jsonpath = ELIXIRPATH / "神物.json"
        self.special_jsonpath = XIULIANITEMPATH / "特殊物品.json"
        self.type_to_path = {
            "防具": self.armor_jsonpath,
            "法器": self.weapon_jsonpath,
            "功法": self.mainbuff_jsonpath,
            "辅修功法": self.subbuff_jsonpath,
            "神通": self.secbuff_jsonpath,
            "身法": self.effect1buff_jsonpath,
            "瞳术": self.effect2buff_jsonpath,
            "丹药": self.elixir_jsonpath,
            "礼包": self.lb_jsonpath,
            "药材": self.yaocai_jsonpath,
            "合成丹药": self.mix_elixir_type_jsonpath,
            "炼丹炉": self.ldl_jsonpath,
            "聚灵旗": self.jlq_jsonpath,
            "神物": self.sw_jsonpath,
            "特殊物品": self.special_jsonpath
        }
        self.items = ITEMS_CACHE
        if not self._has_init.get(items_num):
            self._has_init[items_num] = True
            ITEMS_CACHE.clear()
            Items().export_items_data()
            logger.info(f"载入items完成")

    def readf(self, FILEPATH):
        try:
            with open(FILEPATH, "r", encoding="UTF-8") as f:
                data = f.read()
                if data:
                    return json.loads(data)
                else:
                    return None
        except FileNotFoundError:
            logger.error(f"文件未找到: {FILEPATH}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析错误 {FILEPATH}: {e}")
            return None
        except PermissionError:
            logger.error(f"没有权限读取文件: {FILEPATH}")
            return None
        except UnicodeDecodeError:
            logger.error(f"文件编码错误，无法以UTF-8读取: {FILEPATH}")
            return None
        except Exception as e:
            logger.error(f"读取文件时发生未知错误 {FILEPATH}: {e}")
            return None

    def savef(self, data):
        FILEPATH = ITEMSFILEPATH
        data = json.dumps(data, ensure_ascii=False, indent=4)
        save_mode = "w" if os.path.exists(FILEPATH) else "x"
        with open(FILEPATH, mode=save_mode, encoding="UTF-8") as f:
            f.write(data)
            f.close()

    def refresh(self):
        """重新加载所有物品数据，更新 self.items"""
        global ITEMS_CACHE
        ITEMS_CACHE.clear()
        self.export_items_data()
        asyncio.sleep(3)
        self.revert_to_original_files()  
        logger.info(f"重载items结束")

    def export_items_data(self):
        """导出所有物品数据"""
        global ITEMS_CACHE
        item_types = [
            "功法", "辅修功法", "神通", "身法", "瞳术",
            "法器", "防具", "丹药", "礼包", "药材",
            "合成丹药", "炼丹炉", "聚灵旗", "神物", "特殊物品"
        ]
        for item_type in item_types:
            self.set_item_data(self.get_items_data(item_type), item_type)
        self.savef(ITEMS_CACHE)

    def revert_to_original_files(self):
        """
        将 items.json 中的数据回退到对应的各个物品 JSON 文件中。
        每个物品根据其 'item_type' 字段被分类并写回到相应的原始文件中。
        """
        global ITEMS_CACHE
        if not ITEMS_CACHE:
            return
        categorized_items = {item_type: {} for item_type in self.type_to_path.keys()}
        skill_types = ['功法', '神通', '辅修功法', '身法', '瞳术']
        for item_id, item_data in ITEMS_CACHE.items():
            item_type = item_data.get('item_type')
            if item_type in categorized_items:
                categorized_items[item_type][item_id] = item_data
                if item_type in skill_types:
                    item_data['rank'], item_data['level'] = item_data['level'], item_data['rank']
                    del item_data['type']
                    del item_data['item_type']
            else:
                logger.warning(f"未知的物品类型 '{item_type}'，物品 ID: {item_id}，名称: {item_data.get('name', '未知')}")

        for item_type, items_data in categorized_items.items():
            file_path = self.type_to_path.get(item_type)
            if file_path:
                try:
                    # 将数据转换为格式化的 JSON 字符串
                    data_str = json.dumps(items_data, ensure_ascii=False, indent=4)
                    # 写入文件，使用 'w' 模式覆盖原有内容
                    with open(file_path, 'w', encoding='UTF-8') as f:
                        f.write(data_str)
                except Exception as e:
                    logger.error(f"回退 '{item_type}' 数据到文件 {file_path} 时发生错误: {e}")
            else:
                logger.warning(f"未找到 '{item_type}' 对应的文件路径。")

    def get_items_data(self, item_type):
        """根据物品类型获取对应的数据"""
        file_path = self.type_to_path.get(item_type)
        if file_path:
            return self.readf(file_path)
        else:
            logger.warning(f"未知的物品类型: {item_type}")
            return None

    def set_item_data(self, dict_data, item_type):
        global ITEMS_CACHE
        if not dict_data:
            logger.warning(f"{item_type}加载失败！")
            return
        for k, v in dict_data.items():
            if k in ITEMS_CACHE:
                logger.warning(f"items：{k}已存在！")
                return
            if item_type in ['功法', '神通', '辅修功法', '身法', '瞳术']:
                v['type'] = '技能'
                v['rank'], v['level'] = v['level'], v['rank']
            ITEMS_CACHE[k] = v
            ITEMS_CACHE[k].update({'item_type': item_type})
            if '境界' in v:
                ITEMS_CACHE[k]['境界'] = v['境界']

    def get_data_by_item_id(self, item_id):
        """通过物品ID获取物品数据"""
        if item_id is None:
            return None
        return self.items[str(item_id)]
    
    def get_data_by_item_name(self, item_name):
        """通过物品名称获取物品ID和物品数据，如果item_name为数字ID，也支持通过ID查找，返回格式统一为 (item_id, item_data)"""
        if item_name.isdigit():
            item_id = item_name
            item_data = self.get_data_by_item_id(int(item_id))
            if item_data:
                return int(item_id), item_data
        else:
            for item_id, item in self.items.items():
                if str(item['name']) == str(item_name):
                    return int(item_id), item
        return None, None

    def get_fusion_items(self):
        """获取所有可合成的物品名称和类型"""
        fusion_items = []
        for item_id, item_data in self.items.items():
            if 'fusion' in item_data:
                fusion_items.append(f"{item_data['name']} ({item_data['item_type']})")
        return fusion_items

    def get_data_by_item_type(self, item_type):
        """获取指定类型"""
        temp_dict = {}
        for k, v in self.items.items():
            if v['item_type'] in item_type:
                temp_dict[k] = v
        return temp_dict

    def get_random_id_list_by_rank_and_item_type(
            self,
            fanil_rank: int,
            item_type: List = None
    ):
        """
        获取随机一个物品ID,可以指定物品类型,物品等级和用户等级相差40级以上会被抛弃
        :param fanil_rank:用户的最终rank,最终rank由用户rank和rank增幅事件构成
        :param item_type:type:list,物品类型，可以为空，枚举值：法器、防具、神通、身法、功法、丹药
        :return 获得的ID列表,type:list
        """
        l_id = []
        for k, v in self.items.items():
            if item_type is not None:
                if v['item_type'] in item_type and int(v['rank']) >= fanil_rank and int(v['rank']) - fanil_rank <= 40:
                    l_id.append(k)
                else:
                    continue
            else:
                if int(v['rank']) >= fanil_rank and int(v['rank']) - fanil_rank <= 40:
                    l_id.append(k)
                else:
                    continue
        return l_id
    