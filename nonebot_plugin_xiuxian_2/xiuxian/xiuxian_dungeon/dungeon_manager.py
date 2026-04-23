import json
import random
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from nonebot.log import logger

from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import convert_rank
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager

item_s = Items()
player_data = PlayerDataManager()


# 只保留大境界（用于怪物jj）
jinjie_list = [
    "江湖好手",
    "感气境", "练气境", "筑基境", "结丹境", "金丹境",
    "元神境", "化神境", "炼神境", "返虚境", "大乘境",
    "虚道境", "斩我境", "遁一境", "至尊境", "微光境",
    "星芒境", "月华境", "耀日境", "祭道境", "自在境",
    "破虚境", "无界境", "混元境", "造化境", "永恒境",
    "至高"
]

# 兜底神通池（确保boss/minion有技能可用）
DEFAULT_BOSS_SKILLS = [14001, 14002, 14003, 14004]
DEFAULT_MINION_SKILLS = [14003, 14004]


class DungeonEvent:
    """副本事件类"""

    def __init__(self, event_data: Dict):
        self.event_id = event_data.get("event_id")
        self.event_type = self.event_id
        self.weight = event_data.get("weight", 0)
        self.description = event_data.get("description", "")

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
        self.type = template_data.get("type", "explore")
        self.description = template_data.get("description")
        self.total_layers = template_data.get("total_layers", 5)

        self.events: List[DungeonEvent] = []
        for event_data in template_data.get("events", []):
            self.events.append(DungeonEvent(event_data))

        self.monster_templates = template_data.get("monster_templates", {})
        self.boss_config = template_data.get("boss", {})

    def get_event_map(self) -> Dict[str, DungeonEvent]:
        return {e.event_id: e for e in self.events}

    def get_minion_info(self, template_type: str = "common") -> Dict[str, Any]:
        """根据模板生成小怪基准数据"""
        if template_type not in self.monster_templates:
            template_type = "common"

        template = self.monster_templates.get(template_type, {})
        prefix = random.choice(template.get("name_prefix", [""]))
        base_name = random.choice(template.get("base_names", ["怪物"]))
        name = f"{prefix}·{base_name}" if prefix else base_name

        hp_range = template.get("hp_range", [50, 100])
        mp_range = template.get("mp_range", [1, 2])
        attack_range = template.get("attack_range", [0.1, 0.5])

        skill_pool = template.get("skill_pool", [])
        if not isinstance(skill_pool, list):
            skill_pool = []

        if skill_pool:
            pick_n = 1 if template_type == "common" else min(2, len(skill_pool))
            skills = random.sample(skill_pool, pick_n)
        else:
            skills = DEFAULT_MINION_SKILLS[:1]

        return {
            "name": name,
            "hp_base_multiplier": random.uniform(hp_range[0], hp_range[1]),
            "mp_base_multiplier": random.uniform(mp_range[0], mp_range[1]),
            "attack_base_multiplier": random.uniform(attack_range[0], attack_range[1]),
            "skills": skills,
            "reward": template.get("reward", {}),
        }

    def get_boss_info(self) -> Dict[str, Any]:
        """获取BOSS基准数据（保证skills有效）"""
        boss_config = self.boss_config
        hp_range = boss_config.get("hp_range", [100, 200])
        mp_range = boss_config.get("mp_range", [1, 2])
        attack_range = boss_config.get("attack_range", [0.3, 0.5])

        skills = boss_config.get("skills", [])
        if not isinstance(skills, list):
            skills = []
        if len(skills) == 0:
            skills = DEFAULT_BOSS_SKILLS[:2]

        return {
            "name": boss_config.get("name", "副本BOSS"),
            "hp_base_multiplier": random.uniform(hp_range[0], hp_range[1]),
            "mp_base_multiplier": random.uniform(mp_range[0], mp_range[1]),
            "attack_base_multiplier": random.uniform(attack_range[0], attack_range[1]),
            "skills": skills,
            "reward": boss_config.get("reward", {})
        }


class DungeonManager:
    """副本管理器"""

    _instance = None
    _has_init = False
    _lock = threading.RLock()

    DUNGEON_GLOBAL_STATE_TABLE = "dungeon_global_state"
    PLAYER_DUNGEON_STATUS_TABLE = "player_dungeon_status"
    GLOBAL_USER_ID = "0"

    TYPE_EVENT_WEIGHTS = {
        "explore": {
            "trap": 10,
            "monster": 40,
            "treasure": 20,
            "spirit_stone": 20,
            "nothing": 10
        },
        "challenge": {
            "trap": 10,
            "monster": 65,
            "treasure": 10,
            "spirit_stone": 10,
            "nothing": 5
        },
        "puzzle": {
            "trap": 30,
            "monster": 25,
            "treasure": 20,
            "spirit_stone": 15,
            "nothing": 10
        }
    }

    TYPE_DIFFICULTY_MULT = {
        "explore": (0.90, 1.10),
        "challenge": (1.20, 1.60),
        "puzzle": (1.00, 1.25),
    }

    TYPE_REWARD_MULT = {
        "explore": 1.00,
        "challenge": 1.60,
        "puzzle": 1.25,
    }

    TYPE_NON_BATTLE_REWARD_MULT = {
        "explore": 1.00,
        "challenge": 1.50,
        "puzzle": 1.20
    }

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DungeonManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        with self._lock:
            if self.__class__._has_init:
                return
            self.__class__._has_init = True

            self.data_path = Path(__file__).parent.absolute()
            self.dungeon_data_path = self.data_path / "data"
            self.config_file = self.dungeon_data_path / "副本.json"

            self.dungeon_data_path.mkdir(parents=True, exist_ok=True)

            self.dungeon_templates = self._load_dungeon_templates()
            self._init_dungeon_tables()

            self.current_dungeon: Optional[DungeonTemplate] = None
            self._load_or_init_today_dungeon()

            logger.info("DungeonManager 初始化完成")

    def _get_current_date(self) -> str:
        return datetime.now().strftime("%Y-%m-%d")

    def _init_dungeon_tables(self):
        player_data._ensure_table_exists(self.DUNGEON_GLOBAL_STATE_TABLE)
        player_data._ensure_table_exists(self.PLAYER_DUNGEON_STATUS_TABLE)

        global_fields = {
            "dungeon_id": "TEXT",
            "dungeon_name": "TEXT",
            "date": "TEXT",
        }
        for f, t in global_fields.items():
            player_data._ensure_field_exists(self.DUNGEON_GLOBAL_STATE_TABLE, f, t)

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
        templates = []
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                for template_data in config_data:
                    templates.append(DungeonTemplate(template_data))
            except Exception as e:
                logger.error(f"加载副本模板失败: {e}")
        else:
            logger.warning(f"副本配置文件不存在: {self.config_file}")
        return templates

    def _get_global_state(self) -> dict | None:
        return player_data.get_fields(self.GLOBAL_USER_ID, self.DUNGEON_GLOBAL_STATE_TABLE)

    def _save_global_state(self, dungeon: DungeonTemplate, date_str: str):
        data = {
            "dungeon_id": dungeon.id,
            "dungeon_name": dungeon.name,
            "date": date_str
        }
        for key, value in data.items():
            player_data.update_or_write_data(self.GLOBAL_USER_ID, self.DUNGEON_GLOBAL_STATE_TABLE, key, value)

    def _find_template_by_id(self, dungeon_id: str) -> Optional[DungeonTemplate]:
        for template in self.dungeon_templates:
            if template.id == dungeon_id:
                return template
        return None

    def _load_or_init_today_dungeon(self):
        """
        仅初始化或同步今日副本。
        不清空玩家状态。
        """
        with self._lock:
            current_date = self._get_current_date()
            global_state = self._get_global_state()

            if global_state and global_state.get("date") == current_date:
                template = self._find_template_by_id(global_state.get("dungeon_id"))
                if template:
                    self.current_dungeon = template
                    return

            if not self.dungeon_templates:
                self.current_dungeon = None
                logger.error("未加载到任何副本模板")
                return

            self.current_dungeon = random.choice(self.dungeon_templates)
            self._save_global_state(self.current_dungeon, current_date)
            logger.info(
                f"初始化今日副本成功: {self.current_dungeon.id} - {self.current_dungeon.name} - {current_date}"
            )

    def sync_current_dungeon(self):
        """
        从全局状态同步当前副本到内存。
        如果跨天则自动刷新当天副本。
        """
        with self._lock:
            current_date = self._get_current_date()
            global_state = self._get_global_state()

            if not global_state:
                logger.warning("未找到副本全局状态，自动初始化")
                self._load_or_init_today_dungeon()
                return

            if global_state.get("date") != current_date:
                logger.info("检测到跨天，自动刷新今日副本")
                self.reset_dungeon()
                return

            dungeon_id = global_state.get("dungeon_id")
            if self.current_dungeon and self.current_dungeon.id == dungeon_id:
                return

            template = self._find_template_by_id(dungeon_id)
            if template:
                self.current_dungeon = template
            else:
                logger.warning(f"全局副本ID无效: {dungeon_id}，重新初始化今日副本")
                self._load_or_init_today_dungeon()

    def reset_dungeon(self) -> None:
        """
        每日刷新副本。
        只有这里才会重置所有玩家副本状态。
        """
        with self._lock:
            current_date = self._get_current_date()

            if not self.dungeon_templates:
                self.current_dungeon = None
                logger.error("副本刷新失败：没有可用副本模板")
                return

            self.current_dungeon = random.choice(self.dungeon_templates)
            self._save_global_state(self.current_dungeon, current_date)
            self.clear_all_player_status()

            logger.info(
                f"每日副本已刷新: {self.current_dungeon.id} - {self.current_dungeon.name} - {current_date}"
            )

    def get_dungeon_progress(self) -> Dict[str, Any]:
        self.sync_current_dungeon()

        if not self.current_dungeon:
            return {
                "name": "未知副本",
                "description": "副本数据加载失败",
                "total_layers": 0,
                "date": self._get_current_date(),
                "type": "explore"
            }

        return {
            "name": self.current_dungeon.name,
            "description": self.current_dungeon.description,
            "total_layers": self.current_dungeon.total_layers,
            "date": self._get_current_date(),
            "type": self.current_dungeon.type
        }

    def get_player_status(self, user_id) -> Dict[str, Any]:
        """
        获取玩家副本状态。
        仅在“当天无记录”或“跨天”时初始化。
        不再因为旧实例 current_dungeon 不一致而刷状态。
        """
        self.sync_current_dungeon()

        user_id_str = str(user_id)
        player_status_record = player_data.get_fields(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE)

        current_dungeon_id = self.current_dungeon.id if self.current_dungeon else "unknown"
        current_dungeon_name = self.current_dungeon.name if self.current_dungeon else "未知副本"
        current_dungeon_layers = self.current_dungeon.total_layers if self.current_dungeon else 0
        current_date = self._get_current_date()

        need_init = False

        if not player_status_record:
            need_init = True
        elif player_status_record.get("last_reset_date") != current_date:
            need_init = True

        if need_init:
            new_status = {
                "dungeon_id": current_dungeon_id,
                "dungeon_name": current_dungeon_name,
                "dungeon_status": "not_started",
                "current_layer": 0,
                "total_layers": current_dungeon_layers,
                "last_reset_date": current_date
            }
            for key, value in new_status.items():
                if key in ("current_layer", "total_layers"):
                    player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, key, value, data_type="INTEGER")
                else:
                    player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, key, value, data_type="TEXT")

            logger.info(f"初始化玩家副本状态: user_id={user_id_str}, dungeon_id={current_dungeon_id}")
            return player_data.get_fields(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE)

        # 如果今天记录存在，但副本名称/总层数不同，则温和同步，不重置进度
        patched = False
        if player_status_record.get("dungeon_id") != current_dungeon_id:
            player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, "dungeon_id", current_dungeon_id, data_type="TEXT")
            patched = True
        if player_status_record.get("dungeon_name") != current_dungeon_name:
            player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, "dungeon_name", current_dungeon_name, data_type="TEXT")
            patched = True
        if int(player_status_record.get("total_layers", 0) or 0) != int(current_dungeon_layers):
            player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, "total_layers", current_dungeon_layers, data_type="INTEGER")
            patched = True

        if patched:
            logger.warning(f"玩家副本状态字段已同步但未重置进度: user_id={user_id_str}")
            return player_data.get_fields(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE)

        return player_status_record

    def update_player_progress(self, user_id, layer_increment=1, status: Optional[str] = None):
        self.sync_current_dungeon()

        user_id_str = str(user_id)
        player_data_record = self.get_player_status(user_id)

        if status:
            player_data_record["dungeon_status"] = status

        if player_data_record["dungeon_status"] != "completed":
            if layer_increment > 0:
                player_data_record["dungeon_status"] = "exploring"
                player_data_record["current_layer"] += layer_increment

                if player_data_record["current_layer"] >= player_data_record["total_layers"]:
                    player_data_record["dungeon_status"] = "completed"
                    player_data_record["current_layer"] = player_data_record["total_layers"]

        for key, value in player_data_record.items():
            if key in ("current_layer", "total_layers"):
                player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, key, value, data_type="INTEGER")
            else:
                player_data.update_or_write_data(user_id_str, self.PLAYER_DUNGEON_STATUS_TABLE, key, value, data_type="TEXT")

    def clear_all_player_status(self) -> None:
        """
        重置所有玩家副本状态。
        仅应在 reset_dungeon() 时调用。
        """
        self._init_dungeon_tables()

        current_date = self._get_current_date()
        current_dungeon_id = self.current_dungeon.id if self.current_dungeon else "unknown"
        current_dungeon_name = self.current_dungeon.name if self.current_dungeon else "未知副本"
        current_dungeon_layers = self.current_dungeon.total_layers if self.current_dungeon else 0

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

        logger.info(f"已重置全部玩家副本状态，共 {len(all_records)} 条")

    # =========================
    # 业务辅助
    # =========================

    def _get_dungeon_type(self) -> str:
        self.sync_current_dungeon()
        if not self.current_dungeon:
            return "explore"
        t = str(self.current_dungeon.type or "explore").lower()
        if t not in ("explore", "challenge", "puzzle"):
            return "explore"
        return t

    def _safe_level_power(self, level_name: str) -> int:
        try:
            val = int(jsondata.level_data().get(level_name, {}).get("power", 100))
            return max(100, val)
        except Exception:
            return 100

    def _choose_main_jinjie_by_player(self, user_level: str, prefer_player_level_for_boss=False):
        player_rank_val, _ = convert_rank(user_level)
        if player_rank_val is None:
            player_rank_val = convert_rank("江湖好手")[0] or 0

        player_main_jj = ""
        for jj in jinjie_list:
            if user_level.startswith(jj):
                player_main_jj = jj
                break
        if not player_main_jj:
            player_main_jj = "感气境"

        if prefer_player_level_for_boss:
            return player_main_jj

        available = []
        for jj in jinjie_list:
            if jj in ("江湖好手", "至高"):
                continue
            rank_val, _ = convert_rank(f"{jj}初期")
            if rank_val is None:
                continue
            if rank_val <= player_rank_val + 5:
                available.append((jj, rank_val))

        if not available:
            return "感气境"

        weighted = []
        for jj, rv in available:
            diff = abs(player_rank_val - rv)
            if diff <= 2:
                w = 6
            elif diff <= 5:
                w = 3
            else:
                w = 1
            weighted.extend([jj] * w)

        return random.choice(weighted) if weighted else random.choice([x[0] for x in available])

    def _get_type_multipliers(self):
        t = self._get_dungeon_type()
        diff_low, diff_high = self.TYPE_DIFFICULTY_MULT.get(t, (1.0, 1.0))
        reward_mult = self.TYPE_REWARD_MULT.get(t, 1.0)
        non_battle_reward_mult = self.TYPE_NON_BATTLE_REWARD_MULT.get(t, 1.0)
        return diff_low, diff_high, reward_mult, non_battle_reward_mult

    # =========================
    # 掉落与怪物创建
    # =========================

    def generate_drop_item(self, user_level, drop_items):
        if not drop_items:
            return 0

        items_list = list(drop_items.keys())
        weights = list(drop_items.values())
        selected_item = random.choices(items_list, weights=weights, k=1)[0]

        player_rank_val, _ = convert_rank(user_level)
        if player_rank_val is None:
            player_rank_val = convert_rank("江湖好手")[0] or 0

        max_rank = convert_rank("江湖好手")[0] or 55
        item_base_rank = max(player_rank_val - random.randint(15, 20), 5)
        item_final_rank = random.randint(item_base_rank, min(item_base_rank + random.randint(5, 10), max_rank))

        if item_final_rank <= 10 and random.random() < 0.2:
            item_final_rank = random.randint(11, 20)

        items_id = item_s.get_random_id_list_by_rank_and_item_type(item_final_rank, selected_item)
        if not items_id:
            return 0
        return random.choice(items_id)

    def creating_monsters(self, user_level, user_exp, monsters_info, monster_type="minion"):
        self.sync_current_dungeon()
        if not self.current_dungeon:
            return {}

        dungeon_type = self._get_dungeon_type()
        diff_low, diff_high, reward_mult, _ = self._get_type_multipliers()

        prefer_player_level_for_boss = monster_type == "boss"
        monsters_jj_main = self._choose_main_jinjie_by_player(user_level, prefer_player_level_for_boss)

        if monsters_jj_main in ("江湖好手", "至高"):
            monster_level_key = monsters_jj_main
        else:
            monster_level_key = f"{monsters_jj_main}初期"

        monster_base_exp_for_level = self._safe_level_power(monster_level_key)
        player_level_power = self._safe_level_power(user_level)
        player_exp_factor = max(0.8, min(1.3, (user_exp / max(1, player_level_power)) ** 0.2))
        type_diff_factor = random.uniform(diff_low, diff_high)

        if monster_type == "boss":
            boss_bonus = 1.25 if dungeon_type == "challenge" else 1.10
            power_anchor = int(max(monster_base_exp_for_level, user_exp) * player_exp_factor * boss_bonus)
        else:
            power_anchor = int(monster_base_exp_for_level * player_exp_factor * type_diff_factor)

        power_anchor = max(100, power_anchor)

        hp = int(power_anchor * monsters_info.get("hp_base_multiplier", 60) * random.uniform(0.92, 1.08))
        mp = int(power_anchor * monsters_info.get("mp_base_multiplier", 1.5) * random.uniform(0.92, 1.08))
        attack = int(power_anchor * monsters_info.get("attack_base_multiplier", 0.15) * random.uniform(0.92, 1.08))

        hp = max(hp, 100)
        mp = max(mp, 10)
        attack = max(attack, 20)

        reward_cfg = monsters_info.get("reward", {})

        spirit_stone_cfg = reward_cfg.get("spirit_stone", [0.2, 0.6])
        if not isinstance(spirit_stone_cfg, list) or len(spirit_stone_cfg) != 2:
            spirit_stone_cfg = [0.2, 0.6]
        rand_stone_mul = random.uniform(spirit_stone_cfg[0], spirit_stone_cfg[1])
        stone_base = 12000 if monster_type == "boss" else 8000
        final_stone_value = int(stone_base * rand_stone_mul * reward_mult * random.uniform(90, 150))

        exp_ratio = float(reward_cfg.get("experience", 0.002))
        base_exp_reward = power_anchor * 180 * exp_ratio * reward_mult * random.uniform(1.0, 1.8)
        if monster_type == "boss":
            base_exp_reward *= 1.6
        final_exp_reward = int(base_exp_reward)

        drop_chance = float(reward_cfg.get("drop_chance", 0.01))
        if dungeon_type == "challenge":
            drop_chance *= 1.20
        elif dungeon_type == "puzzle":
            drop_chance *= 1.10
        drop_chance = min(drop_chance, 0.95)

        item_id = 0
        if random.random() < drop_chance:
            drop_items = reward_cfg.get("drop_items", {})
            item_id = self.generate_drop_item(user_level, drop_items)

        skills = monsters_info.get("skills", [])
        if not isinstance(skills, list):
            skills = []
        if len(skills) == 0:
            skills = DEFAULT_BOSS_SKILLS[:2] if monster_type == "boss" else DEFAULT_MINION_SKILLS[:1]

        return {
            "name": monsters_info.get("name", "怪物"),
            "jj": monsters_jj_main,
            "气血": hp,
            "总血量": hp,
            "真元": mp,
            "攻击": attack,
            "skills": skills,
            "experience": max(1, final_exp_reward),
            "stone": max(1, final_stone_value),
            "item_id": item_id,
            "monster_type": monster_type
        }

    # =========================
    # 事件触发
    # =========================

    def _pick_event_by_type(self) -> DungeonEvent:
        self.sync_current_dungeon()

        e_map = self.current_dungeon.get_event_map()
        dungeon_type = self._get_dungeon_type()
        cfg = self.TYPE_EVENT_WEIGHTS.get(dungeon_type, self.TYPE_EVENT_WEIGHTS["explore"])

        candidates = []
        weights = []
        for eid, w in cfg.items():
            if eid in e_map:
                candidates.append(e_map[eid])
                weights.append(w)

        if not candidates:
            if not self.current_dungeon.events:
                return DungeonEvent({"event_id": "nothing", "description": "无事发生", "weight": 1})
            ws = [max(1, e.weight) for e in self.current_dungeon.events]
            return random.choices(self.current_dungeon.events, weights=ws, k=1)[0]

        return random.choices(candidates, weights=weights, k=1)[0]

    def trigger_event(self, user_level, user_exp):
        self.sync_current_dungeon()

        event = self._pick_event_by_type()
        dungeon_type = self._get_dungeon_type()
        _, _, _, non_battle_reward_mult = self._get_type_multipliers()

        result = {
            "type": event.event_type,
            "description": event.description
        }

        if event.event_type == "trap":
            low, high = event.damage if hasattr(event, "damage") else (0.1, 0.2)
            if dungeon_type == "puzzle":
                low *= 1.10
                high *= 1.20
            elif dungeon_type == "challenge":
                low *= 1.05
                high *= 1.10
            result["damage"] = random.uniform(low, high)

        elif event.event_type == "monster":
            battle_config = getattr(event, "battle", {}) or {}
            template_type_choices = battle_config.get("monster_templates", ["common"])
            elite_chance = float(battle_config.get("elite_chance", 0.2))

            enemy_data = []

            if dungeon_type == "challenge":
                minion_count = random.randint(1, 3)
                elite_chance = min(0.5, elite_chance + 0.15)
            else:
                minion_count = 1
                if dungeon_type == "puzzle":
                    elite_chance = min(0.35, elite_chance + 0.05)

            for _ in range(minion_count):
                actual_template_type = "common"
                if random.random() < elite_chance and "elite" in template_type_choices:
                    actual_template_type = "elite"
                elif len(template_type_choices) > 1:
                    pool = [t for t in template_type_choices if t != "elite"]
                    actual_template_type = random.choice(pool) if pool else "common"

                minion_info = self.current_dungeon.get_minion_info(actual_template_type)
                minion = self.creating_monsters(user_level, user_exp, minion_info, monster_type="minion")
                enemy_data.append(minion)

            result["monster_data"] = enemy_data

        elif event.event_type == "treasure":
            drop_items = getattr(event, "drop_items", {}) or {}
            roll_times = 1
            if dungeon_type == "puzzle":
                roll_times = 2
            elif dungeon_type == "challenge":
                roll_times = 3

            best_item = 0
            best_rank = -1
            for _ in range(roll_times):
                iid = self.generate_drop_item(user_level, drop_items)
                if iid == 0:
                    continue
                data = item_s.get_data_by_item_id(iid)
                rank = int(data.get("rank", 0)) if data else 0
                if rank > best_rank:
                    best_rank = rank
                    best_item = iid
            result["drop_items"] = best_item

        elif event.event_type == "spirit_stone":
            spirit_stone_cfg = getattr(event, "stones", [1, 2])
            if not isinstance(spirit_stone_cfg, list) or len(spirit_stone_cfg) != 2:
                spirit_stone_cfg = [1, 2]

            rand_mul = random.uniform(spirit_stone_cfg[0], spirit_stone_cfg[1])

            player_rank_val, _ = convert_rank(user_level)
            if player_rank_val is None:
                player_rank_val = 0
            base_rank = convert_rank("江湖好手")[0] or 0
            rank_diff = max(0, player_rank_val - base_rank)

            base_stone = 12000
            final_stone_value = int(base_stone * rand_mul * (1.18 ** rank_diff) * non_battle_reward_mult * random.uniform(90, 140))
            result["stones"] = max(1, final_stone_value)

        return result

    # =========================
    # Boss层生成
    # =========================

    def get_boss_data(self, user_level, user_exp):
        self.sync_current_dungeon()

        enemy_data = []
        dungeon_type = self._get_dungeon_type()

        boss_info = self.current_dungeon.get_boss_info()
        boss = self.creating_monsters(user_level, user_exp, boss_info, monster_type="boss")
        enemy_data.append(boss)

        if dungeon_type == "challenge":
            minion_count = random.randint(1, 3)
            minion_template_type = "elite" if random.random() < 0.6 else "common"
        else:
            minion_count = 1
            minion_template_type = "common"

        for _ in range(minion_count):
            minion_info = self.current_dungeon.get_minion_info(minion_template_type)
            minion = self.creating_monsters(user_level, user_exp, minion_info, monster_type="minion")
            enemy_data.append(minion)

        return enemy_data