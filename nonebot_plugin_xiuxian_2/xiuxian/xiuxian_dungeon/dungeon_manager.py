import json
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager  # 导入 PlayerDataManager
from ..xiuxian_utils.utils import number_to  # 导入 number_to

item_s = Items()
player_data = PlayerDataManager()  # PlayerDataManager实例

# 将jinjie_list更改为只包含大境界名称，以及特殊境界
# 这样在创建怪物时，boss['jj']可以直接是大境界名称，
# 且 player_fight.py 中的 generate_boss_buff 可以在此基础上拼接“中期”
jinjie_list = [
    "江湖好手",
    "感气境", "练气境", "筑基境", "结丹境", "金丹境",
    "元神境", "化神境", "炼神境", "返虚境", "大乘境",
    "虚道境", "斩我境", "遁一境", "至尊境", "微光境",
    "星芒境", "月华境", "耀日境", "祭道境", "自在境",
    "破虚境", "无界境", "混元境", "造化境", "永恒境",
    "至高"
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
            "hp_base_multiplier": random.uniform(hp_range[0], hp_range[1]),  # 改为基数倍率
            "mp_base_multiplier": random.uniform(mp_range[0], mp_range[1]),  # 改为基数倍率
            "attack_base_multiplier": random.uniform(attack_range[0], attack_range[1]),  # 改为基数倍率
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
            "hp_base_multiplier": random.uniform(hp_range[0], hp_range[1]),  # 改为基数倍率
            "mp_base_multiplier": random.uniform(mp_range[0], mp_range[1]),  # 改为基数倍率
            "attack_base_multiplier": random.uniform(attack_range[0], attack_range[1]),  # 改为基数倍率
            "skills": boss_config.get("skills", []),
            "reward": boss_config.get("reward", {})
        }


class DungeonManager:
    """副本管理器"""

    DUNGEON_GLOBAL_STATE_TABLE = "dungeon_global_state"  # 全局副本状态表
    PLAYER_DUNGEON_STATUS_TABLE = "player_dungeon_status"  # 玩家副本状态表
    GLOBAL_USER_ID = "0"  # 用于存储全局信息的伪user_id

    def __init__(self):
        self.data_path = Path(__file__).parent.absolute()
        self.dungeon_data_path = self.data_path / "data"
        self.config_file = self.dungeon_data_path / "副本.json"  # 副本模板配置

        # 创建目录
        self.dungeon_data_path.mkdir(parents=True, exist_ok=True)

        # 加载副本模板 (从本地JSON文件加载)
        self.dungeon_templates = self._load_dungeon_templates()

        # 初始化表结构（先建表再补字段）
        self._init_dungeon_tables()

        # 当前活跃副本 (从数据库加载或生成)
        self.current_dungeon: Optional[DungeonTemplate] = None

    def _get_current_date(self) -> str:
        """获取当前日期字符串（YYYY-MM-DD）"""
        return datetime.now().strftime("%Y-%m-%d")

    def _init_dungeon_tables(self):
        """初始化副本相关表结构，确保字段存在"""
        player_data._ensure_table_exists(self.DUNGEON_GLOBAL_STATE_TABLE)
        player_data._ensure_table_exists(self.PLAYER_DUNGEON_STATUS_TABLE)

        # 全局状态表字段
        global_fields = {
            "dungeon_id": "TEXT",
            "dungeon_name": "TEXT",
            "date": "TEXT",
        }
        for f, t in global_fields.items():
            player_data._ensure_field_exists(self.DUNGEON_GLOBAL_STATE_TABLE, f, t)

        # 玩家状态表字段
        player_fields = {
            "dungeon_id": "TEXT",
            "dungeon_name": "TEXT",
            "dungeon_status": "TEXT",
            "current_layer": "INTEGER",
            "total_layers": "INTEGER",
            "last_reset_date": "TEXT",
        }
        for f, t in player_fields.items():
            player_data._ensure_field_exists(self.PLAYER_DUNGEON_STATUS_TABLE, f, t)

    def _load_dungeon_templates(self) -> List[DungeonTemplate]:
        """从本地JSON文件加载副本模板"""
        templates = []
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                for template_data in config_data:
                    templates.append(DungeonTemplate(template_data))
            except Exception as e:
                print(f"加载副本模板失败: {e}")
        return templates

    def reset_dungeon(self) -> None:
        """
        重置副本（每日调用）
        从 PlayerDataManager 加载全局副本信息，如果日期不匹配或无记录，则选择新副本。
        """
        current_date = self._get_current_date()
        global_dungeon_state = player_data.get_fields(self.GLOBAL_USER_ID, self.DUNGEON_GLOBAL_STATE_TABLE)

        new_dungeon_selected = False
        if global_dungeon_state and global_dungeon_state.get("date") == current_date:
            # 日期匹配，尝试加载已保存的副本
            saved_dungeon_id = global_dungeon_state.get("dungeon_id")
            found_template = False
            for template in self.dungeon_templates:
                if template.id == saved_dungeon_id:
                    self.current_dungeon = template
                    found_template = True
                    break
            if not found_template:  # 如果saved_dungeon_id对应的模板没找到，则重新随机
                self.current_dungeon = random.choice(self.dungeon_templates)
                new_dungeon_selected = True
        else:
            # 日期不匹配或无记录，选择新副本
            self.current_dungeon = random.choice(self.dungeon_templates)
            new_dungeon_selected = True

        # 如果选择了新副本或日期不匹配，更新全局状态
        if new_dungeon_selected or not global_dungeon_state or global_dungeon_state.get("date") != current_date:
            dungeon_info_to_save = {
                "dungeon_id": self.current_dungeon.id,
                "dungeon_name": self.current_dungeon.name,
                "date": current_date
            }
            for key, value in dungeon_info_to_save.items():
                player_data.update_or_write_data(self.GLOBAL_USER_ID, self.DUNGEON_GLOBAL_STATE_TABLE, key, value)

        # 清空所有玩家的副本进度，因为副本已经重置（或新的一天）
        self.clear_all_player_status()

    def get_dungeon_progress(self) -> Dict[str, Any]:
        """获取当前副本进度信息"""
        if not self.current_dungeon:
            # 如果 current_dungeon 尚未初始化，尝试再次加载或重置
            self.reset_dungeon()
            if not self.current_dungeon:  # 如果仍然没有，说明有问题
                return {"name": "未知副本", "description": "副本数据加载失败", "total_layers": 0, "date": self._get_current_date()}

        return {
            "name": self.current_dungeon.name,
            "description": self.current_dungeon.description,
            "total_layers": self.current_dungeon.total_layers,
            "date": self._get_current_date(),
        }

    def get_player_status(self, user_id) -> Dict[str, Any]:
        """
        获取玩家副本状态。
        如果玩家状态不存在或已过期，则初始化。
        """
        user_id_str = str(user_id)
        player_status_record = player_data.get_fields(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE)

        current_dungeon_id = self.current_dungeon.id if self.current_dungeon else "unknown"
        current_dungeon_name = self.current_dungeon.name if self.current_dungeon else "未知副本"
        current_dungeon_layers = self.current_dungeon.total_layers if self.current_dungeon else 0
        current_date = self._get_current_date()

        # 检查是否需要初始化或重置玩家状态
        if not player_status_record or \
           player_status_record.get("dungeon_id") != current_dungeon_id or \
           player_status_record.get("last_reset_date") != current_date:

            new_status = {
                "dungeon_id": current_dungeon_id,
                "dungeon_name": current_dungeon_name,
                "dungeon_status": "not_started",  # not_started, exploring, completed
                "current_layer": 0,
                "total_layers": current_dungeon_layers,
                "last_reset_date": current_date
            }
            for key, value in new_status.items():
                player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, key, value)
            return player_data.get_fields(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE)

        return player_status_record

    def update_player_progress(self, user_id, layer_increment=1, status: Optional[str] = None):
        """
        更新玩家副本进度。
        :param user_id: 玩家QQ号。
        :param layer_increment: 层数增量，默认为1。
        :param status: 强制设置的副本状态（如 "completed"）。
        """
        user_id_str = str(user_id)
        player_data_record = self.get_player_status(user_id)

        if status:
            player_data_record["dungeon_status"] = status

        # 只有在未完成或探索中状态下才更新层数
        if player_data_record["dungeon_status"] != "completed":
            if layer_increment > 0:
                player_data_record["dungeon_status"] = "exploring"
                player_data_record["current_layer"] += layer_increment

                # 检查是否完成副本
                if player_data_record["current_layer"] >= player_data_record["total_layers"]:
                    player_data_record["dungeon_status"] = "completed"
                    player_data_record["current_layer"] = player_data_record["total_layers"]

        # 保存回数据库
        for key, value in player_data_record.items():
            player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, key, value)

    def clear_all_player_status(self) -> None:
        """
        重置所有玩家的副本状态到“未开始”和第0层。
        注意：不使用 SQL UPDATE 语句，改为逐条写回。
        """
        # 确保表与字段存在
        self._init_dungeon_tables()

        current_date = self._get_current_date()
        current_dungeon_id = self.current_dungeon.id if self.current_dungeon else "unknown"
        current_dungeon_name = self.current_dungeon.name if self.current_dungeon else "未知副本"
        current_dungeon_layers = self.current_dungeon.total_layers if self.current_dungeon else 0

        # 读取所有玩家副本状态记录（逐条重置）
        all_records = player_data.get_all_records(self.PLAYER_DUNGEON_STATUS_TABLE)

        for record in all_records:
            uid = str(record.get("user_id", "")).strip()
            if not uid:
                continue

            reset_status = {
                "dungeon_id": current_dungeon_id,
                "dungeon_name": current_dungeon_name,
                "dungeon_status": "not_started",
                "current_layer": 0,
                "total_layers": current_dungeon_layers,
                "last_reset_date": current_date,
            }

            for key, value in reset_status.items():
                if key in ("current_layer", "total_layers"):
                    player_data.update_or_write_data(uid, self.PLAYER_DUNGEON_STATUS_TABLE, key, value, data_type="INTEGER")
                else:
                    player_data.update_or_write_data(uid, self.PLAYER_DUNGEON_STATUS_TABLE, key, value, data_type="TEXT")

    def generate_drop_item(self, user_level, drop_items):
        """根据用户等级和掉落权重生成随机物品ID"""
        items_list = list(drop_items.keys())
        weights = list(drop_items.values())
        selected_item = random.choices(items_list, weights=weights, k=1)[0]

        player_main_rank_val, _ = convert_rank(user_level)
        if player_main_rank_val is None:
            player_main_rank_val = 0

        item_base_rank = max(player_main_rank_val - random.randint(15, 20), 5)
        item_final_rank = random.randint(item_base_rank, min(item_base_rank + random.randint(5, 10), 54))

        if item_final_rank <= 10 and random.random() < 0.2:
            item_final_rank = random.randint(11, 20)

        items_id = item_s.get_random_id_list_by_rank_and_item_type((item_final_rank), selected_item)
        if not items_id:
            item_id = 0
        else:
            item_id = random.choice(items_id)

        return item_id

    def creating_monsters(self, user_level, user_exp, monsters_info, monster_type="minion"):
        """
        根据玩家境界和经验创建怪物。
        """
        if not self.current_dungeon:
            return {}

        player_main_rank_val, player_sub_rank_val = convert_rank(user_level)
        if player_main_rank_val is None:
            player_main_rank_val = convert_rank("江湖好手")[0] if convert_rank("江湖好手")[0] is not None else 0
            player_sub_rank_val = 0

        available_main_jinjie = [
            j for j in jinjie_list
            if j not in ["江湖好手", "至高"] and
            (convert_rank(f"{j}初期")[0] or 0) <= (player_main_rank_val + 5)
        ]

        if not available_main_jinjie:
            chosen_main_jj = "感气境"
        else:
            player_current_main_jj = ""
            for jj in jinjie_list:
                if user_level.startswith(jj):
                    player_current_main_jj = jj
                    break

            weighted_choices = []
            for jj in available_main_jinjie:
                jj_rank_val, _ = convert_rank(f"{jj}初期")
                if jj_rank_val is None:
                    continue

                weight = 1
                rank_diff = abs(player_main_rank_val - jj_rank_val)

                if rank_diff <= 2:
                    weight = 5
                elif rank_diff <= 5:
                    weight = 3

                if jj_rank_val < player_main_rank_val - 5:
                    weight = 0.5

                weighted_choices.extend([jj] * int(weight * 10))

            if not weighted_choices:
                chosen_main_jj = random.choice(available_main_jinjie)
            else:
                chosen_main_jj = random.choice(weighted_choices)

        if monster_type == "boss" and player_current_main_jj:
            chosen_main_jj = player_current_main_jj

        monsters_jj_main = chosen_main_jj

        monster_rank_val, _ = convert_rank(f"{monsters_jj_main}初期")
        if monster_rank_val is None:
            monster_rank_val = convert_rank("感气境初期")[0]

        monster_base_exp_for_level = int(jsondata.level_data().get(f"{monsters_jj_main}初期", {}).get("power", 100))
        if monster_base_exp_for_level == 0:
            monster_base_exp_for_level = 100

        min_scaling = 0.8
        max_scaling = 1.2

        player_standard_exp_for_level_val = int(jsondata.level_data().get(f"{player_main_rank_val}初期", {}).get("power", 100))
        if player_standard_exp_for_level_val == 0:
            player_standard_exp_for_level_val = 100

        min_exp_for_player_phase = int(jsondata.level_data().get(user_level, {}).get("power", 100))
        next_level_info = jsondata.level_data().get(f"{user_level}下一境界", None)
        if next_level_info:
            max_exp_for_player_phase = int(next_level_info.get("power", min_exp_for_player_phase * 2))
        else:
            max_exp_for_player_phase = min_exp_for_player_phase * 2

        player_exp_progress_in_phase = 0.0
        if max_exp_for_player_phase > min_exp_for_player_phase:
            player_exp_progress_in_phase = (user_exp - min_exp_for_player_phase) / (max_exp_for_player_phase - min_exp_for_player_phase)
            player_exp_progress_in_phase = max(0.0, min(1.0, player_exp_progress_in_phase))

        monster_attr_scaling = min_scaling + (max_scaling - min_scaling) * player_exp_progress_in_phase
        monster_attr_scaling *= random.uniform(0.95, 1.05)
        monster_attr_scaling = max(min_scaling * 0.9, min(max_scaling * 1.1, monster_attr_scaling))

        if monster_type == "boss":
            final_monster_power_base = max(monster_base_exp_for_level, int(user_exp))
        else:
            final_monster_power_base = int(monster_base_exp_for_level * monster_attr_scaling)

        hp = int(final_monster_power_base * monsters_info["hp_base_multiplier"] * random.uniform(0.9, 1.1))
        mp = int(final_monster_power_base * monsters_info["mp_base_multiplier"] * random.uniform(0.9, 1.1))
        attack = int(final_monster_power_base * monsters_info["attack_base_multiplier"] * random.uniform(0.9, 1.1))

        hp = max(hp, 100)
        mp = max(mp, 10)
        attack = max(attack, 20)

        spirit_stone_base_from_config = monsters_info["reward"].get("spirit_stone", [1, 2])
        random_stone_multiplier = random.uniform(spirit_stone_base_from_config[0], spirit_stone_base_from_config[1])

        initial_base_stone_per_rank = 10000
        rank_difference_for_stone = monster_rank_val - (convert_rank("江湖好手")[0] or 0)
        if rank_difference_for_stone < 0:
            rank_difference_for_stone = 0

        stone_scaling_factor = (1.25 ** rank_difference_for_stone)
        final_stone_value = int(initial_base_stone_per_rank * random_stone_multiplier * stone_scaling_factor * random.uniform(80, 150))

        exp_per_monster_base_power_unit = 200
        base_exp_reward = final_monster_power_base * exp_per_monster_base_power_unit * monsters_info["reward"]["experience"] * random.uniform(1.0, 2.0)

        if monster_type == "boss":
            base_exp_reward *= 3
            final_stone_value *= 2

        final_exp_reward = int(base_exp_reward)

        item_id = 0
        if random.random() < monsters_info["reward"]["drop_chance"]:
            drop_items = monsters_info["reward"]["drop_items"]
            item_id = self.generate_drop_item(user_level, drop_items)

        return {
            "name": monsters_info["name"],
            "jj": monsters_jj_main,
            "气血": hp,
            "总血量": hp,
            "真元": mp,
            "攻击": attack,
            "skills": monsters_info["skills"],
            "experience": final_exp_reward,
            "stone": final_stone_value,
            "item_id": item_id,
            "monster_type": monster_type
        }

    def trigger_event(self, user_level, user_exp):
        """触发一个随机事件"""
        event = self.current_dungeon.get_random_event()

        result = {
            "type": event.event_type,
            "description": event.description
        }

        if event.event_type == "trap":
            result["damage"] = random.uniform(event.damage[0], event.damage[1])

        elif event.event_type == "monster":
            battle_config = event.battle
            template_type_choices = battle_config.get("monster_templates", ["common"])
            num_monsters_range = battle_config.get("num_monsters", [1, 3])
            minion_count = random.randint(num_monsters_range[0], num_monsters_range[1])
            enemy_data = []

            elite_chance = battle_config.get("elite_chance", 0.2)
            for i in range(minion_count):
                actual_template_type = "common"
                if random.random() < elite_chance and "elite" in template_type_choices:
                    actual_template_type = "elite"
                elif len(template_type_choices) > 1:
                    actual_template_type = random.choice([t for t in template_type_choices if t != "elite"])

                minion_info = self.current_dungeon.get_minion_info(actual_template_type)
                minion = self.creating_monsters(user_level, user_exp, minion_info, monster_type="minion")
                enemy_data.append(minion)
            result["monster_data"] = enemy_data

        elif event.event_type == "treasure":
            item_id = self.generate_drop_item(user_level, event.drop_items)
            result["drop_items"] = item_id

        elif event.event_type == "spirit_stone":
            spirit_stone_base_from_config = event.stones
            random_stone_multiplier = random.uniform(spirit_stone_base_from_config[0], spirit_stone_base_from_config[1])

            initial_base_stone_per_rank = 10000

            player_main_rank_val, _ = convert_rank(user_level)
            if player_main_rank_val is None:
                player_main_rank_val = 0

            rank_difference = player_main_rank_val - (convert_rank("江湖好手")[0] or 0)
            if rank_difference < 0:
                rank_difference = 0

            stone_scaling_factor = (1.25 ** rank_difference)
            final_stone_value = int(initial_base_stone_per_rank * random_stone_multiplier * stone_scaling_factor * random.uniform(100, 200))
            result["stones"] = final_stone_value

        return result

    def get_boss_data(self, user_level, user_exp):
        """获取BOSS和小怪信息（1个BOSS + 2个小怪）"""
        enemy_data = []

        boss_info = self.current_dungeon.get_boss_info()
        boss = self.creating_monsters(user_level, user_exp, boss_info, monster_type="boss")
        enemy_data.append(boss)

        minion_count = 2
        for i in range(minion_count):
            minion_info = self.current_dungeon.get_minion_info()
            minion = self.creating_monsters(user_level, user_exp, minion_info, monster_type="minion")
            enemy_data.append(minion)

        return enemy_data