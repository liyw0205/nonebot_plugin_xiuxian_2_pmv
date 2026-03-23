import json
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager # 导入 PlayerDataManager
from ..xiuxian_utils.utils import number_to # 导入 number_to 

item_s = Items()
player_data = PlayerDataManager() # PlayerDataManager实例

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
            "hp_base_multiplier": random.uniform(hp_range[0], hp_range[1]), # 改为基数倍率
            "mp_base_multiplier": random.uniform(mp_range[0], mp_range[1]), # 改为基数倍率
            "attack_base_multiplier": random.uniform(attack_range[0], attack_range[1]), # 改为基数倍率
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
            "hp_base_multiplier": random.uniform(hp_range[0], hp_range[1]), # 改为基数倍率
            "mp_base_multiplier": random.uniform(mp_range[0], mp_range[1]), # 改为基数倍率
            "attack_base_multiplier": random.uniform(attack_range[0], attack_range[1]), # 改为基数倍率
            "skills": boss_config.get("skills", []),
            "reward": boss_config.get("reward", {})
        }


class DungeonManager:
    """副本管理器"""

    DUNGEON_GLOBAL_STATE_TABLE = "dungeon_global_state" # 全局副本状态表
    PLAYER_DUNGEON_STATUS_TABLE = "player_dungeon_status" # 玩家副本状态表
    GLOBAL_USER_ID = "0" # 用于存储全局信息的伪user_id

    def __init__(self):
        self.data_path = Path(__file__).parent.absolute()
        self.dungeon_data_path = self.data_path / "data"
        self.config_file = self.dungeon_data_path / "副本.json" # 副本模板配置

        # 创建目录
        self.dungeon_data_path.mkdir(parents=True, exist_ok=True)

        # 加载副本模板 (从本地JSON文件加载)
        self.dungeon_templates = self._load_dungeon_templates()

        # 当前活跃副本 (从数据库加载或生成)
        self.current_dungeon: Optional[DungeonTemplate] = None
        
        # 初始化或加载副本状态
        self.reset_dungeon() # 启动时调用一次以确保状态最新

    def _get_current_date(self) -> str:
        """获取当前日期字符串（YYYY-MM-DD）"""
        return datetime.now().strftime("%Y-%m-%d")

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
            if not found_template: # 如果saved_dungeon_id对应的模板没找到，则重新随机
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
            if not self.current_dungeon: # 如果仍然没有，说明有问题
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
        # 1. 玩家记录不存在
        # 2. 玩家记录中的副本ID与当前副本ID不符 (可能是新副本)
        # 3. 玩家记录中的最后重置日期与当前日期不符 (新的一天)
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
            # 保存到数据库
            # 使用 update_or_write_data 确保记录存在，并且所有字段都更新
            for key, value in new_status.items():
                player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, key, value)
            return player_data.get_fields(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE) # 重新获取完整记录
        
        return player_status_record


    def update_player_progress(self, user_id, layer_increment=1, status: Optional[str] = None):
        """
        更新玩家副本进度。
        :param user_id: 玩家QQ号。
        :param layer_increment: 层数增量，默认为1。
        :param status: 强制设置的副本状态（如 "completed"）。
        """
        user_id_str = str(user_id)
        player_data_record = self.get_player_status(user_id) # 确保获取到最新的状态

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
        
        # 将更新后的数据保存回数据库
        for key, value in player_data_record.items():
            player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, key, value)


    def clear_all_player_status(self) -> None:
        """
        重置所有玩家的副本状态到“未开始”和第0层。
        这应该在每日副本重置时调用，以确保所有玩家的进度都被重置。
        """
        player_data._ensure_table_exists(self.PLAYER_DUNGEON_STATUS_TABLE) # 确保表存在
        conn = player_data._get_connection()
        cursor = conn.cursor()
        current_date = self._get_current_date()
        current_dungeon_id = self.current_dungeon.id if self.current_dungeon else "unknown"
        current_dungeon_name = self.current_dungeon.name if self.current_dungeon else "未知副本"
        current_dungeon_layers = self.current_dungeon.total_layers if self.current_dungeon else 0
        
        # 使用 UPDATE 语句来重置所有玩家的状态
        # 如果玩家记录不存在，则不会有任何更新。get_player_status 会在首次查询时创建。
        cursor.execute(
            f"""
            UPDATE {self.PLAYER_DUNGEON_STATUS_TABLE}
            SET dungeon_status = ?, current_layer = ?, last_reset_date = ?,
                dungeon_id = ?, dungeon_name = ?, total_layers = ?
            WHERE user_id IS NOT NULL
            """,
            ("not_started", 0, current_date, current_dungeon_id, current_dungeon_name, current_dungeon_layers)
        )
        conn.commit()
        conn.close()


    def generate_drop_item(self, user_level, drop_items):
        """根据用户等级和掉落权重生成随机物品ID"""
        items_list = list(drop_items.keys())  # ['功法', '神通', '药材', '辅修功法', '法器', '防具']
        weights = list(drop_items.values())  # [55, 15, 15, 5, 5, 5]
        selected_item = random.choices(items_list, weights=weights, k=1)[0]
        # 获取玩家的大境界名称，用于判断掉落等级
        player_main_rank_val, _ = convert_rank(user_level)
        
        # 确保 player_main_rank_val 不为 None
        if player_main_rank_val is None:
            player_main_rank_val = 0  # 默认最低等级
        
        # 根据玩家境界计算物品掉落的基准等级
        # 调整基准等级，使其与玩家境界更匹配，通常比玩家低一些，但不会太低
        item_base_rank = max(player_main_rank_val - random.randint(15, 20), 5) # 随机降低一些等级，但至少是5级
        
        # 随机掉落物品的境界范围
        # 确保物品等级不会超过当前最高境界（54级）
        item_final_rank = random.randint(item_base_rank, min(item_base_rank + random.randint(5, 10), 54)) # 在基准上浮动5-10级
        
        # 如果最终等级很低，有小概率提升
        if item_final_rank <= 10 and random.random() < 0.2: # 20%几率提升
            item_final_rank = random.randint(11, 20)
        
        items_id = item_s.get_random_id_list_by_rank_and_item_type((item_final_rank), selected_item)
        if not items_id:
            item_id = 0
        else:
            item_id = random.choice(items_id)

        return item_id

    def creating_monsters(self, user_level, user_exp, monsters_info, monster_type="minion"): # monster_type默认为minion
        """
        根据玩家境界和经验创建怪物。
        现在 monsters_info 中的 hp, mp, attack 都是倍率 (例如 55, 1, 0.1)。
        :param user_level: 玩家的境界字符串 (e.g., "至尊境初期")
        :param user_exp: 玩家的经验值
        :param monsters_info: 怪物模板信息 (从副本.json读取)
        :param monster_type: 怪物类型 ("minion" 或 "boss")
        """
        if not self.current_dungeon:
            return {}

        # 玩家当前境界的数值表示，例如 "至尊境初期" -> (35, 1)
        player_main_rank_val, player_sub_rank_val = convert_rank(user_level)
        
        # 确保玩家境界数值有效，否则默认为最低境界
        if player_main_rank_val is None:
            player_main_rank_val = convert_rank("江湖好手")[0] if convert_rank("江湖好手")[0] is not None else 0
            player_sub_rank_val = 0

        # 根据玩家境界来确定怪物的“标准”境界
        # 随机生成一个与玩家当前境界接近的怪物大境界，但不会高于玩家境界太多
        
        # 可选的怪物大境界列表 (排除特殊境界)
        available_main_jinjie = [
            j for j in jinjie_list 
            if j not in ["江湖好手", "至高"] and 
            (convert_rank(f"{j}初期")[0] or 0) <= (player_main_rank_val + 5) # 怪物境界不超过玩家5个大境界
        ]
        
        if not available_main_jinjie: # 兜底，如果玩家境界太低，直接选感气境
            chosen_main_jj = "感气境"
        else:
            # 优先选择与玩家境界相同或稍低的怪物境界，增加挑战性
            # 将玩家当前大境界作为中心，进行随机选择
            player_current_main_jj = ""
            for jj in jinjie_list:
                if user_level.startswith(jj):
                    player_current_main_jj = jj
                    break
            
            # 构造权重列表，使其更倾向于玩家当前境界附近的怪物
            weighted_choices = []
            for jj in available_main_jinjie:
                jj_rank_val, _ = convert_rank(f"{jj}初期")
                if jj_rank_val is None: continue

                weight = 1 # 基础权重
                rank_diff = abs(player_main_rank_val - jj_rank_val)

                if rank_diff <= 2: # 玩家境界上下2个大境界，权重更高
                    weight = 5
                elif rank_diff <= 5: # 玩家境界上下5个大境界，次高
                    weight = 3
                
                # 确保怪物境界不低于玩家太多
                if jj_rank_val < player_main_rank_val - 5: # 怪物境界低于玩家5个大境界的，权重降低
                    weight = 0.5
                
                weighted_choices.extend([jj] * int(weight * 10)) # 增加重复项以体现权重

            if not weighted_choices: # 如果权重选择后为空，则退回普通随机
                chosen_main_jj = random.choice(available_main_jinjie)
            else:
                chosen_main_jj = random.choice(weighted_choices)

        # 如果是BOSS，强制对标玩家当前的大境界，避免BOSS境界随到太低
        if monster_type == "boss" and player_current_main_jj:
            chosen_main_jj = player_current_main_jj
            
        monsters_jj_main = chosen_main_jj
        
        # 获取怪物大境界的数值表示 (用于怪物强度计算)
        monster_rank_val, _ = convert_rank(f"{monsters_jj_main}初期")
        if monster_rank_val is None:
            monster_rank_val = convert_rank("感气境初期")[0] # 兜底
        
        # 获取怪物的基础经验（作为其强度基数）
        # 这里用怪物境界对应的“初期”经验作为基数，来计算怪物属性
        monster_base_exp_for_level = int(jsondata.level_data().get(f"{monsters_jj_main}初期", {}).get("power", 100))
        if monster_base_exp_for_level == 0: monster_base_exp_for_level = 100

        # ==== 怪物属性缩放因子 (exp_rate) ====
        # 这是一个关键的调整，使怪物强度与玩家经验而非境界更紧密挂钩
        # 目标：让怪物强度在玩家当前境界的 `[0.8, 1.2]` 倍之间波动，同时考虑玩家实际经验
        min_scaling = 0.8  # 怪物属性最低为玩家当前境界标准属性的 80%
        max_scaling = 1.2  # 怪物属性最高为玩家当前境界标准属性的 120%
        
        # 玩家当前境界的“标准经验”
        player_standard_exp_for_level_val = int(jsondata.level_data().get(f"{player_main_rank_val}初期", {}).get("power", 100))
        if player_standard_exp_for_level_val == 0: player_standard_exp_for_level_val = 100

        # 根据玩家实际经验与标准经验的比例，调整怪物强度
        # 如果玩家经验远超境界标准，怪物会更强，反之则弱
        
        # 计算玩家经验在当前境界的进度 (0.0 - 1.0)
        # 例如，玩家在练气境初期，经验刚达到练气境初期，进度接近0
        # 经验达到练气境中期，进度接近0.5
        # 经验达到练气境圆满，进度接近1.0
        # 如果是满级玩家，user_exp / player_standard_exp_for_level_val 会很高，需要做限制
        
        # 获取玩家当前阶段的经验范围
        min_exp_for_player_phase = int(jsondata.level_data().get(user_level, {}).get("power", 100))
        # 获取玩家下一阶段的经验范围
        # 尝试获取下一阶段的经验值，如果没有，就用当前阶段的作为上限 (或一个较大值)
        next_level_info = jsondata.level_data().get(f"{user_level}下一境界", None) # 假设有这个字段
        if next_level_info:
            max_exp_for_player_phase = int(next_level_info.get("power", min_exp_for_player_phase * 2))
        else:
            max_exp_for_player_phase = min_exp_for_player_phase * 2 # 兜底

        # 计算玩家在当前"阶段"的经验百分比
        # 这是一个0到1的值，代表玩家距离当前阶段满经验的进度
        player_exp_progress_in_phase = 0.0
        if max_exp_for_player_phase > min_exp_for_player_phase:
            player_exp_progress_in_phase = (user_exp - min_exp_for_player_phase) / (max_exp_for_player_phase - min_exp_for_player_phase)
            player_exp_progress_in_phase = max(0.0, min(1.0, player_exp_progress_in_phase)) # 限制在0-1之间

        # 根据玩家在该境界的进度，线性调整怪物强度缩放
        # 例如，进度0时怪物强度为 min_scaling，进度1时为 max_scaling
        monster_attr_scaling = min_scaling + (max_scaling - min_scaling) * player_exp_progress_in_phase
        
        # 随机浮动，增加不确定性
        monster_attr_scaling *= random.uniform(0.95, 1.05) # 额外5%的随机浮动
        monster_attr_scaling = max(min_scaling * 0.9, min(max_scaling * 1.1, monster_attr_scaling)) # 整体缩放范围再扩大一点

        # 最终怪物强度基数，用于计算HP/MP/Attack
        if monster_type == "boss":
            # Boss的强度基准极高，直接对标玩家真实修为并结合特有倍率，且不受缩放进度惩罚（参考塔与世界BOSS）
            final_monster_power_base = max(monster_base_exp_for_level, int(user_exp))
        else:
            final_monster_power_base = int(monster_base_exp_for_level * monster_attr_scaling)

        # ==== 属性计算 ====
        # HP, MP, Attack 现在是 monsters_info 中 hp_base_multiplier 等乘以 final_monster_power_base
        hp = int(final_monster_power_base * monsters_info["hp_base_multiplier"] * random.uniform(0.9, 1.1))
        mp = int(final_monster_power_base * monsters_info["mp_base_multiplier"] * random.uniform(0.9, 1.1))
        attack = int(final_monster_power_base * monsters_info["attack_base_multiplier"] * random.uniform(0.9, 1.1))

        # 确保属性不会过低
        hp = max(hp, 100) # 至少100血
        mp = max(mp, 10)  # 至少10真元
        attack = max(attack, 20) # 至少20攻击

        # ==== 灵石奖励调整 ====
        spirit_stone_base_from_config = monsters_info["reward"].get("spirit_stone", [1, 2])
        random_stone_multiplier = random.uniform(spirit_stone_base_from_config[0], spirit_stone_base_from_config[1])
        
        # 灵石基数：与怪物基础强度（与玩家境界匹配）挂钩，并乘以一个更大的常数
        initial_base_stone_per_rank = 10000 # 每提升一个大境界，基础灵石增加
        
        # 将 "江湖好手" 对应的 rank 值定为 0
        rank_difference_for_stone = monster_rank_val - (convert_rank("江湖好手")[0] or 0)
        if rank_difference_for_stone < 0: rank_difference_for_stone = 0 # 避免负数
            
        # 使用指数增长，例如每层境界基础灵石增加 25%
        stone_scaling_factor = (1.25 ** rank_difference_for_stone) # 1.25的rank_difference次方
        
        # 乘以一个较大的常数，确保灵石足够多
        final_stone_value = int(initial_base_stone_per_rank * random_stone_multiplier * stone_scaling_factor * random.uniform(80, 150)) # 额外随机80-150倍，确保数量可观

        # ==== 经验奖励调整 ====
        # 经验奖励不再是玩家经验的乘数，而是基于怪物基础强度的一个具体数值
        # `monsters_info["reward"]["experience"]` (副本.json中的0.001, 0.005等) 作为经验奖励的“倍率”
        # 假设 1点怪物基础强度 = X 修为
        exp_per_monster_base_power_unit = 200 # 1单位强度给 200 经验

        # 怪物掉落的基础经验值
        base_exp_reward = final_monster_power_base * exp_per_monster_base_power_unit * monsters_info["reward"]["experience"] * random.uniform(1.0, 2.0) # 额外随机1-2倍

        # Boss的经验奖励应该显著高于小怪
        if monster_type == "boss":
            base_exp_reward *= 3 # Boss经验是小怪的3倍 (可调整)
            final_stone_value *= 2 # Boss灵石是小怪的2倍 (可调整)
            
        final_exp_reward = int(base_exp_reward)


        item_id = 0
        if random.random() < monsters_info["reward"]["drop_chance"]:
            drop_items = monsters_info["reward"]["drop_items"]
            item_id = self.generate_drop_item(user_level, drop_items)

        return {
            "name": monsters_info["name"],
            "jj": monsters_jj_main,  # 这里存储大境界字符串，符合player_fight.py的generate_boss_buff的预期
            "气血": hp,
            "总血量": hp, # 初始总血量等于气血
            "真元": mp,
            "攻击": attack,
            "skills": monsters_info["skills"],
            "experience": final_exp_reward, # 调整为实际经验值
            "stone": final_stone_value, # 调整为实际灵石值
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
            result["damage"] = random.uniform(event.damage[0], event.damage[1])

        elif event.event_type == "monster":
            battle_config = event.battle
            template_type_choices = battle_config.get("monster_templates", ["common"])
            num_monsters_range = battle_config.get("num_monsters", [1, 3])
            minion_count = random.randint(num_monsters_range[0], num_monsters_range[1])
            enemy_data = []  # 存储所有敌人数据

            elite_chance = battle_config.get("elite_chance", 0.2)
            for i in range(minion_count):
                actual_template_type = "common"
                if random.random() < elite_chance and "elite" in template_type_choices:
                    actual_template_type = "elite"
                elif len(template_type_choices) > 1: # 如果有多种普通模板可选
                    actual_template_type = random.choice([t for t in template_type_choices if t != "elite"]) # 排除精英模板随机
                
                minion_info = self.current_dungeon.get_minion_info(actual_template_type)
                minion = self.creating_monsters(user_level, user_exp, minion_info, monster_type="minion")
                enemy_data.append(minion)
            result["monster_data"] = enemy_data

        elif event.event_type == "treasure":
            item_id = self.generate_drop_item(user_level, event.drop_items)
            result["drop_items"] = item_id

        elif event.event_type == "spirit_stone":
            # ==== 灵石奖励调整 ====
            spirit_stone_base_from_config = event.stones # e.g., [1, 2]
            random_stone_multiplier = random.uniform(spirit_stone_base_from_config[0], spirit_stone_base_from_config[1])
            
            # 灵石基数：与玩家境界挂钩，并乘以一个更大的常数
            initial_base_stone_per_rank = 10000 # 每提升一个大境界，基础灵石增加
            
            player_main_rank_val, _ = convert_rank(user_level)
            if player_main_rank_val is None: player_main_rank_val = 0 # 兜底
            
            rank_difference = player_main_rank_val - (convert_rank("江湖好手")[0] or 0)
            if rank_difference < 0: rank_difference = 0
            
            # 使用指数增长，比如每层境界基础灵石增加 25%
            stone_scaling_factor = (1.25 ** rank_difference) 
            
            final_stone_value = int(initial_base_stone_per_rank * random_stone_multiplier * stone_scaling_factor * random.uniform(100, 200)) # 额外随机100-200倍，比怪物掉落更多
            
            result["stones"] = final_stone_value

        return result

    def get_boss_data(self, user_level, user_exp):
        """获取BOSS和小怪信息（1个BOSS + 2个小怪）"""
        enemy_data = []  # 存储所有敌人数据

        boss_info = self.current_dungeon.get_boss_info()
        boss = self.creating_monsters(user_level, user_exp, boss_info, monster_type="boss") # 标记为boss类型
        enemy_data.append(boss)

        minion_count = 2  # 生成2个小怪
        for i in range(minion_count):
            minion_info = self.current_dungeon.get_minion_info()
            minion = self.creating_monsters(user_level, user_exp, minion_info, monster_type="minion") # 标记为minion类型
            enemy_data.append(minion)

        return enemy_data