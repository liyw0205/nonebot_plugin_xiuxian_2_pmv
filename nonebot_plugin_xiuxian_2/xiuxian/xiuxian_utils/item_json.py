try:
    import ujson as json
except ImportError:
    import json
import threading
from pathlib import Path
from typing import List

from nonebot.log import logger
from ...paths import get_paths

READPATH = get_paths().data
SKILLPATH = READPATH / "功法"
WEAPONPATH = READPATH / "装备"
ELIXIRPATH = READPATH / "丹药"
PACKAGESPATH = READPATH / "礼包"
XIULIANITEMPATH = READPATH / "修炼物品"
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
            self.lb_jsonpath = PACKAGESPATH
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
            self.package_source_map = {}
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

    def read_json_bundle(self, dirpath: Path, item_type: str):
        """读取目录下多个 json 并合并，主要用于按类拆分后的礼包数据。"""
        if not dirpath.exists():
            logger.error(f"目录未找到: {dirpath}")
            return None
        if not dirpath.is_dir():
            return self.readf(dirpath)

        bundle_data = {}
        if item_type == "礼包":
            self.package_source_map = {}

        json_files = sorted(dirpath.glob("*.json"))
        if item_type == "礼包":
            legacy_bundle = dirpath / "礼包.json"
            split_files = [filepath for filepath in json_files if filepath.name != legacy_bundle.name]
            if legacy_bundle in json_files and split_files:
                json_files = split_files

        for filepath in json_files:
            file_data = self.readf(filepath)
            if not file_data:
                continue
            if not isinstance(file_data, dict):
                logger.warning(f"{item_type}分类文件格式错误，已跳过: {filepath}")
                continue

            for item_id, item_data in file_data.items():
                item_id = str(item_id)
                if item_id in bundle_data:
                    logger.warning(f"{item_type}数据ID重复，已跳过 {filepath}: {item_id}")
                    continue
                bundle_data[item_id] = item_data
                if item_type == "礼包":
                    self.package_source_map[item_id] = filepath

        return bundle_data

    def export_items_data(self):
        """加载所有物品数据到内存缓存"""
        global ITEMS_CACHE
        item_types = [
            "功法", "辅修功法", "神通", "身法", "瞳术",
            "法器", "防具", "饰品", "丹药", "礼包", "药材",
            "合成丹药", "炼丹炉", "聚灵旗", "称号", "神物", "特殊物品"
        ]
        for item_type in item_types:
            self.set_item_data(self.get_items_data(item_type), item_type)

    def revert_to_original_files(self):
        """
        将内存缓存中的数据回退到各原始 json 文件
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
                        if item_type == "礼包" and Path(file_path).is_dir():
                            self.save_package_items_data(items_data)
                        else:
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
            if item_type == "礼包":
                return self.read_json_bundle(file_path, item_type)
            return self.readf(file_path)
        logger.warning(f"未知的物品类型: {item_type}")
        return None

    def save_package_items_data(self, items_data):
        """按现有来源文件保存礼包数据，新礼包按名称归类。"""
        PACKAGESPATH.mkdir(parents=True, exist_ok=True)
        grouped_items = {}
        for item_id, item_data in items_data.items():
            item_id = str(item_id)
            file_path = self.package_source_map.get(item_id)
            if not file_path:
                file_path = PACKAGESPATH / self.get_package_category_filename(item_data)
            grouped_items.setdefault(file_path, {})[item_id] = item_data

        for file_path, file_items in grouped_items.items():
            data_str = json.dumps(file_items, ensure_ascii=False, indent=4)
            with open(file_path, 'w', encoding='UTF-8') as f:
                f.write(data_str)

    @staticmethod
    def get_package_category_filename(item_data):
        name = str(item_data.get("name", ""))
        if "套装礼包" in name:
            return "饰品套装礼包.json"
        if "饰品礼包" in name or "戒指饰品礼包" in name:
            return "饰品随机礼包.json"
        if "秘境" in name:
            return "秘境礼包.json"
        if "聚灵旗" in name or "洞府" in name:
            return "聚灵旗礼包.json"
        if any(word in name for word in ["炼丹", "丹道", "丹", "渡劫", "冲境", "修为"]):
            return "丹药修炼礼包.json"
        if any(word in name for word in ["功法", "神通", "身法", "瞳术", "流派", "斗战", "控制", "暴击", "反击", "高机动"]):
            return "功法神通礼包.json"
        if any(word in name for word in ["法器", "防具", "神兵", "防御"]):
            return "装备礼包.json"
        if any(word in name for word in ["神物", "材料", "药材"]):
            return "神物材料礼包.json"
        if any(word in name for word in ["祈愿", "特殊道具", "灵石", "冲榜", "社交", "悬赏", "资源"]):
            return "资源功能礼包.json"
        if any(word in name for word in ["新手", "补偿", "感谢"]):
            return "基础礼包.json"
        return "综合礼包.json"

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
