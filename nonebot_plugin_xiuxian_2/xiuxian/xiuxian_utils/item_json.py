try:
    import ujson as json
except ImportError:
    import json
import os
import threading
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
        if not self._has_init.get(items_num):
            self._has_init[items_num] = True
            self.lock = threading.RLock()

            self.mainbuff_jsonpath = SKILLPATH / "主功法.json"
            self.subbuff_jsonpath = SKILLPATH / "辅修功法.json"
            self.secbuff_jsonpath = SKILLPATH / "神通.json"
            self.effect1buff_jsonpath = SKILLPATH / "身法.json"
            self.effect2buff_jsonpath = SKILLPATH / "瞳术.json"
            self.weapon_jsonpath = WEAPONPATH / "法器.json"
            self.armor_jsonpath = WEAPONPATH / "防具.json"
            self.accessory_jsonpath = WEAPONPATH / "饰品.json"
            self.elixir_jsonpath = ELIXIRPATH / "丹药.json"
            self.lb_jsonpath = PACKAGESPATH / "礼包.json"
            self.yaocai_jsonpath = ELIXIRPATH / "药材.json"
            self.mix_elixir_type_jsonpath = ELIXIRPATH / "炼丹丹药.json"
            self.ldl_jsonpath = ELIXIRPATH / "炼丹炉.json"
            self.jlq_jsonpath = XIULIANITEMPATH / "聚灵旗.json"
            self.title_jsonpath = XIULIANITEMPATH / "称号.json"
            self.sw_jsonpath = ELIXIRPATH / "神物.json"
            self.special_jsonpath = XIULIANITEMPATH / "特殊物品.json"

            self.type_to_path = {
                "防具": self.armor_jsonpath,
                "法器": self.weapon_jsonpath,
                "饰品": self.accessory_jsonpath,
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
                "称号": self.title_jsonpath,
                "神物": self.sw_jsonpath,
                "特殊物品": self.special_jsonpath
            }

            self.items = ITEMS_CACHE
            self._load_items()
            logger.info("载入items完成")

    def _load_items(self):
        """首次加载/内部加载"""
        with self.lock:
            ITEMS_CACHE.clear()
            self.export_items_data()

    def refresh(self):
        """重载物品缓存，供 管理指令/热更新 调用"""
        with self.lock:
            ITEMS_CACHE.clear()
            self.export_items_data()
            self.items = ITEMS_CACHE
            logger.info("items 已从原始文件重新加载完成")

    def readf(self, filepath: Path):
        try:
            with open(filepath, "r", encoding="UTF-8") as f:
                data = f.read()
                if data:
                    return json.loads(data)
                return None
        except FileNotFoundError:
            logger.error(f"文件未找到: {filepath}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析错误 {filepath}: {e}")
            return None
        except PermissionError:
            logger.error(f"没有权限读取文件: {filepath}")
            return None
        except UnicodeDecodeError:
            logger.error(f"文件编码错误，无法以UTF-8读取: {filepath}")
            return None
        except Exception as e:
            logger.error(f"读取文件时发生未知错误 {filepath}: {e}")
            return None

    def savef(self, data):
        filepath = ITEMSFILEPATH
        filepath.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(data, ensure_ascii=False, indent=4)
        save_mode = "w" if os.path.exists(filepath) else "x"
        with open(filepath, mode=save_mode, encoding="UTF-8") as f:
            f.write(data)

    def export_items_data(self):
        """导出所有物品数据到缓存和 items.json"""
        global ITEMS_CACHE
        item_types = [
            "功法", "辅修功法", "神通", "身法", "瞳术",
            "法器", "防具", "饰品", "丹药", "礼包", "药材",
            "合成丹药", "炼丹炉", "聚灵旗", "称号", "神物", "特殊物品"
        ]
        for item_type in item_types:
            self.set_item_data(self.get_items_data(item_type), item_type)
        self.savef(ITEMS_CACHE)

    def revert_to_original_files(self):
        """
        将 items.json 中的数据回退到各原始 json 文件
        """
        global ITEMS_CACHE
        with self.lock:
            if not ITEMS_CACHE:
                return

            categorized_items = {item_type: {} for item_type in self.type_to_path.keys()}
            skill_types = ['功法', '神通', '辅修功法', '身法', '瞳术']

            for item_id, item_data in ITEMS_CACHE.items():
                item_type = item_data.get('item_type')
                if item_type in categorized_items:
                    item_copy = dict(item_data)
                    categorized_items[item_type][item_id] = item_copy
                    if item_type in skill_types:
                        item_copy['rank'], item_copy['level'] = item_copy['level'], item_copy['rank']
                        item_copy.pop('type', None)
                        item_copy.pop('item_type', None)
                else:
                    logger.warning(
                        f"未知的物品类型 '{item_type}'，物品 ID: {item_id}，名称: {item_data.get('name', '未知')}"
                    )

            for item_type, items_data in categorized_items.items():
                file_path = self.type_to_path.get(item_type)
                if file_path:
                    try:
                        data_str = json.dumps(items_data, ensure_ascii=False, indent=4)
                        with open(file_path, 'w', encoding='UTF-8') as f:
                            f.write(data_str)
                    except Exception as e:
                        logger.error(f"回退 '{item_type}' 数据到文件 {file_path} 时发生错误: {e}")
                else:
                    logger.warning(f"未找到 '{item_type}' 对应的文件路径。")

    def get_items_data(self, item_type):
        """根据物品类型获取对应数据"""
        file_path = self.type_to_path.get(item_type)
        if file_path:
            return self.readf(file_path)
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
                continue

            item_data = dict(v)

            if item_type in ['功法', '神通', '辅修功法', '身法', '瞳术']:
                item_data['type'] = '技能'
                item_data['rank'], item_data['level'] = item_data['level'], item_data['rank']

            item_data['item_type'] = item_type
            ITEMS_CACHE[k] = item_data

    def get_data_by_item_id(self, item_id):
        """通过物品ID获取物品数据"""
        if item_id is None:
            return None
        return self.items.get(str(item_id))

    def get_data_by_item_name(self, item_name):
        """
        通过物品名称获取物品ID和物品数据
        如果 item_name 为数字ID，也支持通过ID查找
        返回: (item_id, item_data)
        """
        if item_name is None:
            return None, None

        item_name = str(item_name).strip()
        if item_name.isdigit():
            item_id = item_name
            item_data = self.get_data_by_item_id(item_id)
            if item_data:
                return int(item_id), item_data
        else:
            for item_id, item in self.items.items():
                if str(item.get('name')) == item_name:
                    return int(item_id), item

        return None, None

    def get_fusion_items(self):
        """获取所有可合成的物品名称和类型"""
        fusion_items = []
        for _, item_data in self.items.items():
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
        获取随机物品ID列表
        """
        l_id = []
        for k, v in self.items.items():
            try:
                rank = int(v['rank'])
            except Exception:
                continue

            if item_type is not None:
                if v['item_type'] in item_type and rank >= fanil_rank and rank - fanil_rank <= 40:
                    l_id.append(k)
            else:
                if rank >= fanil_rank and rank - fanil_rank <= 40:
                    l_id.append(k)
        return l_id