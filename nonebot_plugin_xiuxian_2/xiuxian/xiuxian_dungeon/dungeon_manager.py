import json
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank

item_s = Items()

jinjie_list = [
    "感气境",
    "练气境",
    "筑基境",
    "结丹境",
    "金丹境",
    "元神境",
    "化神境",
    "炼神境",
    "返虚境",
    "大乘境",
    "虚道境",
    "斩我境",
    "遁一境",
    "至尊境",
    "微光境",
    "星芒境",
    "月华境",
    "耀日境",
    "祭道境",
    "自在境",
    "破虚境",
    "无界境",
    "混元境",
    "造化境",
    "永恒境"
]


class DungeonEvent:
    """副本事件类"""

    def __init__(self, event_data: Dict):
        self.event_id = event_data.get("event_id")
        self.event_type = self.event_id  # 兼容性
        self.weight = event_data.get("weight", 0)
        self.description = event_data.get("description", "")

        # 根据不同事件类型初始化数据
        if self.event_id == "trap":
            self.damage = event_data.get("damage", [0.1, 0.2])
        elif self.event_id == "monster":
            self.battle = event_data.get("battle", {})
        elif self.event_id == "treasure":
            self.drop_items = event_data.get("reward", {}).get("drop_items", {})
        elif self.event_id == "spirit_stone":
            self.stones = event_data.get("reward", {}).get("spirit_stone", [1, 2])
        elif self.event_id == "nothing":
            self.effect = event_data.get("effect", {})


class DungeonTemplate:
    """副本模板类"""

    def __init__(self, template_data: Dict):
        self.id = template_data.get("id")
        self.name = template_data.get("name")
        self.type = template_data.get("type")
        self.description = template_data.get("description")
        self.total_layers = template_data.get("total_layers", 5)

        # 初始化事件
        self.events: List[DungeonEvent] = []
        for event_data in template_data.get("events", []):
            self.events.append(DungeonEvent(event_data))

        # 怪物模板
        self.monster_templates = template_data.get("monster_templates", {})

        # BOSS配置
        self.boss_config = template_data.get("boss", {})

    def get_random_event(self) -> DungeonEvent:
        """根据权重随机获取一个事件"""
        weights = [event.weight for event in self.events]
        return random.choices(self.events, weights=weights, k=1)[0]

    def get_minion_info(self, template_type: str = "common") -> Dict[str, Any]:
        """根据模板生成怪物数据"""
        if template_type not in self.monster_templates:
            template_type = "common"

        template = self.monster_templates[template_type]

        # 随机生成名字
        prefix = random.choice(template.get("name_prefix", [""]))
        base_name = random.choice(template.get("base_names", ["怪物"]))
        name = f"{prefix}·{base_name}" if prefix else base_name

        # 生成属性
        hp_range = template.get("hp_range", [50, 100])
        mp_range = template.get("mp_range", [1, 2])
        attack_range = template.get("attack_range", [0.1, 0.5])

        # 技能
        skill_pool = template.get("skill_pool", [])
        skills = []
        if skill_pool:
            num_skills = min(1, len(skill_pool))
            skills = random.sample(skill_pool, num_skills)

        return {
            "name": name,
            "hp": random.randint(hp_range[0], hp_range[1]),
            "mp": random.randint(mp_range[0], mp_range[1]),
            "attack": round(random.uniform(attack_range[0], attack_range[1]), 1),
            "skills": skills,
            "reward": template.get("reward", {}),
        }

    def get_boss_info(self) -> Dict[str, Any]:
        """获取BOSS信息"""
        boss_config = self.boss_config
        hp_range = boss_config.get("hp_range", [100, 200])
        mp_range = boss_config.get("mp_range", [1, 2])
        attack_range = boss_config.get("attack_range", [0.3, 0.5])

        return {
            "name": boss_config.get("name", "副本BOSS"),
            "hp": random.randint(hp_range[0], hp_range[1]),
            "mp": random.randint(mp_range[0], mp_range[1]),
            "attack": round(random.uniform(attack_range[0], attack_range[1]), 2),
            "skills": boss_config.get("skills", []),
            "reward": boss_config.get("reward", {})
        }


class DungeonManager:
    """副本管理器"""

    def __init__(self):
        self.data_path = Path(__file__).parent.absolute()
        self.dungeon_data_path = self.data_path / "data"
        self.config_file = self.data_path / "data" / "副本.json"
        self.dungeon_info_file = self.data_path / "data" / "current_dungeon.json"
        self.player_status_path = self.data_path / "data" / "player_status.json"

        # 创建目录
        self.dungeon_data_path.mkdir(parents=True, exist_ok=True)

        # 加载副本模板
        self.dungeon_templates = self._load_dungeon_templates()

        # 当前活跃副本
        self.current_dungeon = None

        # 玩家状态
        self.player_status = {}

        # 加载玩家状态
        self.load_player_status()

        # 初始化副本
        self.reset_dungeon()

    def _get_current_date(self) -> str:
        """获取当前日期字符串（YYYY-MM-DD）"""
        return datetime.now().strftime("%Y-%m-%d")

    def _load_dungeon_templates(self) -> List[DungeonTemplate]:
        """加载副本模板"""
        templates = []

        # 尝试从文件加载
        config_file = self.config_file
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)

        # 创建模板对象
        for template_data in config_data:
            templates.append(DungeonTemplate(template_data))

        return templates

    def reset_dungeon(self) -> None:
        """重置副本（每日调用）- 优化版"""
        # 定义副本信息文件的保存路径
        dungeon_info_file = self.dungeon_info_file

        current_date = self._get_current_date()

        if dungeon_info_file.exists():
            try:
                with open(dungeon_info_file, 'r', encoding='utf-8') as f:
                    dungeon_info = json.load(f)

                if dungeon_info.get("date") == current_date:
                    saved_dungeon_id = dungeon_info.get("dungeon_id", "")

                    for template in self.dungeon_templates:
                        if template.id == saved_dungeon_id:
                            self.current_dungeon = template
                            return  # 找到后直接返回，不再重新选择

                    self.current_dungeon = random.choice(self.dungeon_templates)
                else:
                    self.current_dungeon = random.choice(self.dungeon_templates)

            except (json.JSONDecodeError, FileNotFoundError):
                self.current_dungeon = random.choice(self.dungeon_templates)
        else:
            self.current_dungeon = random.choice(self.dungeon_templates)

        dungeon_info = {
            "dungeon_id": self.current_dungeon.id if self.current_dungeon else "",
            "date": current_date
        }

        with open(dungeon_info_file, 'w', encoding='utf-8') as f:
            json.dump(dungeon_info, f, ensure_ascii=False, indent=2)

    def get_dungeon_progress(self) -> Dict[str, Any]:
        """获取当前副本进度信息"""
        if not self.current_dungeon:
            return {}

        return {
            "name": self.current_dungeon.name,
            "description": self.current_dungeon.description,
            "total_layers": self.current_dungeon.total_layers,
            "date": self._get_current_date(),
        }

    def load_player_status(self) -> None:
        """从文件加载玩家状态"""
        if self.player_status_path.exists():
            try:
                with open(self.player_status_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # 简洁转换：尝试将所有键转为整数，失败则保持原样
                    self.player_status = {}
                    for k, v in data.items():
                        try:
                            self.player_status[int(k)] = v
                        except ValueError:
                            self.player_status[k] = v
            except Exception as e:
                print(f"加载玩家状态失败: {e}")
                self.player_status = {}
        else:
            self.player_status = {}

    def save_player_status(self) -> None:
        """保存玩家状态到文件"""
        try:
            with open(self.player_status_path, 'w', encoding='utf-8') as f:
                # 简洁转换：所有键转为字符串
                json.dump(
                    {str(k): v for k, v in self.player_status.items()},
                    f, ensure_ascii=False, indent=2
                )
        except Exception as e:
            print(f"保存玩家状态失败: {e}")

    def get_player_status(self, user_id):
        """获取玩家副本状态"""
        if user_id not in self.player_status:
            # 初始化玩家状态
            self.player_status[user_id] = {
                "dungeon_id": self.current_dungeon.id if self.current_dungeon else "",
                "dungeon_name": self.current_dungeon.name if self.current_dungeon else "",
                "dungeon_status": "not_started",  # not_started, exploring, completed
                "current_layer": 0,
                "total_layers": self.current_dungeon.total_layers if self.current_dungeon else 0,
                "last_reset_date": self._get_current_date()
            }
            self.save_player_status()

        return self.player_status[user_id]

    def update_player_progress(self, user_id, layer_increment=1, status=None):
        """更新玩家进度"""
        if user_id in self.player_status:
            player_data = self.player_status[user_id]

            if status:
                player_data["dungeon_status"] = status

            if layer_increment > 0:
                player_data["dungeon_status"] = "exploring"
                player_data["current_layer"] += layer_increment

                # 检查是否完成副本
                if player_data["current_layer"] >= player_data["total_layers"]:
                    player_data["dungeon_status"] = "completed"
                    player_data["current_layer"] = player_data["total_layers"]

            self.save_player_status()

    def clear_all_player_status(self) -> None:
        """清空所有玩家状态"""
        self.player_status = {}
        self.save_player_status()

    def generate_drop_item(self, user_level, drop_items):
        """根据用户等级和掉落权重生成随机物品ID"""
        items = list(drop_items.keys())  # ['功法', '神通', '药材', '辅修功法', '法器', '防具']
        weights = list(drop_items.values())  # [55, 15, 15, 5, 5, 5]
        selected_item = random.choices(items, weights=weights, k=1)[0]
        if selected_item in ["法器", "防具", "辅修功法"]:
            base_rank = max(convert_rank(user_level)[0] - 22, 16)
        else:
            base_rank = max(convert_rank(user_level)[0] - 22, 5)
        zx_rank = random.randint(base_rank, min(base_rank + 35, 54))
        if zx_rank == 5 and random.randint(1, 100) != 100:
            zx_rank = 10
        items_id = item_s.get_random_id_list_by_rank_and_item_type((zx_rank), selected_item)
        if not items_id:
            item_id = 0
        else:
            item_id = random.choice(items_id)

        return item_id

    def creating_monsters(self, user_level, user_exp, monsters_info, monster_type="boss"):
        """获取monsters信息"""
        if not self.current_dungeon:
            return {}

        if len(user_level) == 5:
            level = user_level[:3]
        elif len(user_level) == 2:  # 对至高判断
            level = "永恒境"
        else:  # 对江湖好手判断
            level = "感气境"

        # monsters_jj = random.choice(jinjie_list[:jinjie_list.index(level) + 1])
        # stages = random.choice(["初期", "中期", "圆满"])
        # monsters_level = f"{monsters_jj}{stages}"
        # exp_rate = random.randint(8, 10)

        monsters_jj = level
        monsters_level = user_level
        exp = int(jsondata.level_data()[monsters_level]["power"])
        d1, d2 = 0.7, 1.0  # 线性比例
        min_val, max_val = 0.6, 1.2  # 最小值和最大值
        raw_rate = d1 + (user_exp - exp) * (d2 - d1) * 2 / exp
        exp_rate = round(max(min_val, min(max_val, raw_rate)), 2)
        monsters_exp = int(exp * exp_rate)
        spirit_stone = monsters_info["reward"]["spirit_stone"]
        stone = round(random.uniform(spirit_stone[0], spirit_stone[1]), 2)
        stone = (convert_rank('江湖好手')[0] - convert_rank(user_level)[0] + 1) * 100000 * stone
        item_id = 0
        if random.random() < monsters_info["reward"]["drop_chance"]:
            drop_items = monsters_info["reward"]["drop_items"]
            item_id = self.generate_drop_item(user_level, drop_items)

        return {
            "name": monsters_info["name"],
            "jj": monsters_jj,  # 境界，根据你的系统可能需要
            "气血": int(monsters_exp * monsters_info["hp"]),
            "总血量": int(monsters_exp * monsters_info["hp"]),
            "真元": int(monsters_exp * monsters_info["mp"]),
            "攻击": int(monsters_exp * monsters_info["attack"]),
            "skills": monsters_info["skills"],
            "experience": monsters_info["reward"]["experience"],
            "stone": stone,
            "item_id": item_id,
            "monster_type": monster_type
        }

    def trigger_event(self, user_level, user_exp):
        """触发一个随机事件"""

        # 获取随机事件
        event = self.current_dungeon.get_random_event()

        result = {
            "type": event.event_type,
            "description": event.description
        }

        # 根据事件类型添加额外数据
        if event.event_type == "trap":
            result["damage"] = round(random.uniform(event.damage[0], event.damage[1]), 1)

        elif event.event_type == "monster":
            battle_config = event.battle
            template_type = battle_config.get("template_type", "random")
            num_monsters = battle_config.get("num_monsters", [1, 3])
            minion_count = random.randint(num_monsters[0], num_monsters[1])
            enemy_data = []  # 存储所有敌人数据

            if template_type == "random":
                templates = battle_config.get("monster_templates", ["common"])
                elite_chance = battle_config.get("elite_chance", 0.2)
                for i in range(minion_count):
                    if random.random() < elite_chance and "elite" in templates:
                        template_type = "elite"
                    else:
                        template_type = "common"
                    minion_info = self.current_dungeon.get_minion_info(template_type)
                    minion = self.creating_monsters(user_level, user_exp, minion_info, monster_type="minion")
                    enemy_data.append(minion)
            result["monster_data"] = enemy_data

        elif event.event_type == "treasure":
            item_id = self.generate_drop_item(user_level, event.drop_items)
            result["drop_items"] = item_id

        elif event.event_type == "spirit_stone":
            stone = round(random.uniform(event.stones[0], event.stones[1]), 2)
            stone = (convert_rank('江湖好手')[0] - convert_rank(user_level)[0]) * 100000 * stone
            result["stones"] = stone

        return result

    def get_boss_data(self, user_level, user_exp):
        """获取BOSS和小怪信息（1个BOSS + 2个小怪）"""
        enemy_data = []  # 存储所有敌人数据

        boss_info = self.current_dungeon.get_boss_info()
        boss = self.creating_monsters(user_level, user_exp, boss_info)
        enemy_data.append(boss)

        minion_count = 2  # 生成2个小怪
        for i in range(minion_count):
            minion_info = self.current_dungeon.get_minion_info()
            minion = self.creating_monsters(user_level, user_exp, minion_info, monster_type="minion")
            enemy_data.append(minion)

        return enemy_data
