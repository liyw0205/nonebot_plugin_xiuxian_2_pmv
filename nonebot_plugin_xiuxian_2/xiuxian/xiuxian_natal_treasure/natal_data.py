# natal_data.py
import random
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from .natal_config import *

sql_message = XiuxianDateManage()
player_data = PlayerDataManager()

# ======================
#   本命法宝数据管理类
# ======================
class NatalTreasure:
    """
    本命法宝数据封装与操作类。
    负责法宝的觉醒、养成、升阶、数据存取等功能。
    """

    def __init__(self, user_id: int | str):
        self.user_id = str(user_id)
        self.table = "natal_treasure"
        self.max_treasure_level = MAX_TREASURE_LEVEL  # 法宝总等级上限
        self.max_effect_level_single = MAX_EFFECT_LEVEL_SINGLE  # 单效果等级上限
        self.max_effect_level_double = MAX_EFFECT_LEVEL_DOUBLE  # 双效果等级上限
        # 内部缓存法宝数据，减少数据库查询
        self._natal_data_cache = None

    def _ensure_record(self):
        """
        确保用户在本命法宝表中的记录存在，并检查/添加所有必要的字段。
        此方法会在每次访问法宝数据前调用，以兼容旧数据结构更新。
        """
        # 定义所有可能的字段及其默认值
        default_data = {
            "form": 0,          # 法宝形态 (0表示未觉醒)
            "name": "",         # 法宝名称
            "level": 0,         # 法宝总等级
            "exp": 0,           # 法宝养成经验
            "max_exp": 100,     # 法宝升级所需经验
            "effect1_type": 0,  # 效果1类型 (NatalEffectType枚举值)
            "effect1_base_value": 0.0, # 效果1的基础值 (随机生成，对应配置中的min/max_single/double)
            "effect1_level": 0, # 效果1的等级
            "effect2_type": 0,  # 效果2类型
            "effect2_base_value": 0.0, # 效果2的基础值
            "effect2_level": 0, # 效果2的等级
            # 新增效果需要额外字段来追踪战斗中的使用次数，这些在战斗结束后会同步回数据库
            "fate_revive_count": 0,    # 天命复活已使用次数
            "immortal_revive_count": 0, # 不灭复活已使用次数
            "invincible_gain_count": 0, # 无敌已获取次数（周期性生效时获得）
        }
        
        # 逐个检查字段是否存在并添加，以兼容旧数据
        record = player_data.get_fields(self.user_id, self.table)
        if not record: # 如果用户在表中完全没有记录
            for field, default_value in default_data.items():
                player_data.update_or_write_data(self.user_id, self.table, field, default_value, data_type=type(default_value).__name__.upper())
        else: # 如果记录已存在，检查并添加新字段
            for field, default_value in default_data.items():
                if field not in record: # 如果新版本添加了字段，旧数据中没有，则添加
                    player_data.update_or_write_data(self.user_id, self.table, field, default_value, data_type=type(default_value).__name__.upper())
        self._natal_data_cache = None # 清除缓存，以便下次从数据库加载最新数据

    def exists(self) -> bool:
        """
        检查本命法宝是否已觉醒。
        :return: True如果已觉醒 (form不为0)，False否则。
        """
        self._ensure_record() # 确保字段存在
        form = player_data.get_field_data(self.user_id, self.table, "form")
        return form is not None and form != 0

    def awaken(self, force_new: bool = False):
        """
        觉醒或重塑本命法宝。
        :param force_new: 如果为True，强制重新生成所有属性（重塑）。
        """
        self._ensure_record()
        current_form = player_data.get_field_data(self.user_id, self.table, "form")

        if not current_form or force_new:
            # 随机法宝形态 (例如，1到4代表不同的形态图片或描述)
            form = random.randint(1, 4)
            player_data.update_or_write_data(self.user_id, self.table, "form", form)
        
        # 决定单/双效果（单效果75%，双效果25%）
        is_double = random.random() < 0.25 

        available_effect_types = list(NatalEffectType) # 所有可能的法宝效果类型
        selected_types = []
        if is_double:
            selected_types = random.sample(available_effect_types, 2) # 随机选择两个不同效果
        else:
            selected_types = [random.choice(available_effect_types)] # 随机选择一个效果

        # 名字以第一个效果类型为基准，从对应的名字池中随机选择
        first_type = selected_types[0]
        name_pool = NATAL_TREASURE_NAMES.get(first_type, ["未知法宝"])
        selected_name = random.choice(name_pool)
        player_data.update_or_write_data(self.user_id, self.table, "name", selected_name)

        # 重置所有效果相关字段为默认值，以便写入新效果
        for i in [1, 2]:
            player_data.update_or_write_data(self.user_id, self.table, f"effect{i}_type", 0)
            player_data.update_or_write_data(self.user_id, self.table, f"effect{i}_base_value", 0.0, data_type='REAL')
            player_data.update_or_write_data(self.user_id, self.table, f"effect{i}_level", 0)
        
        # 重置新的次数统计字段，重塑时这些都归零
        player_data.update_or_write_data(self.user_id, self.table, "fate_revive_count", 0)
        player_data.update_or_write_data(self.user_id, self.table, "immortal_revive_count", 0)
        player_data.update_or_write_data(self.user_id, self.table, "invincible_gain_count", 0)

        # 写入新生成的法宝效果
        for idx, etype in enumerate(selected_types, 1):
            field_type = f"effect{idx}_type"
            field_base_value = f"effect{idx}_base_value"
            field_level = f"effect{idx}_level"
            
            # 获取效果的基础范围配置
            config = EFFECT_BASE_AND_GROWTH.get(etype)
            if config:
                if not is_double: # 单效果时使用单效果的基值范围
                    base_value_min = config["min_single"]
                    base_value_max = config["max_single"]
                else: # 双效果时使用双效果的基值范围
                    base_value_min = config["min_double"]
                    base_value_max = config["max_double"]
                
                # 特殊处理无敌和双生，其base_value存储的是概率
                if etype == NatalEffectType.INVINCIBLE or etype == NatalEffectType.TWIN_STRIKE:
                    base_value = base_value_min # base_value存基础概率
                else:
                    # 对于其他效果，随机生成一个基础数值
                    base_value = round(random.uniform(base_value_min, base_value_max), 3) # 增加精度
                
                player_data.update_or_write_data(self.user_id, self.table, field_type, etype.value)
                player_data.update_or_write_data(self.user_id, self.table, field_base_value, base_value, data_type='REAL')
                player_data.update_or_write_data(self.user_id, self.table, field_level, 1) # 初始等级为1

        # 重置法宝总等级和经验为初始状态
        player_data.update_or_write_data(self.user_id, self.table, "level", 0)
        player_data.update_or_write_data(self.user_id, self.table, "exp", 0)
        player_data.update_or_write_data(self.user_id, self.table, "max_exp", 100) # 初始100经验升级
        self._natal_data_cache = None # 清除缓存

    def add_exp(self, amount: int) -> tuple[bool, str]:
        """
        增加法宝养成经验，并处理升级。
        :param amount: 增加的经验值数量。
        :return: (是否至少升级一次, 升级信息或失败信息)。
        """
        self._ensure_record()
        self._natal_data_cache = None # 经验改变，清除缓存

        current_level = player_data.get_field_data(self.user_id, self.table, "level")
        current_exp = player_data.get_field_data(self.user_id, self.table, "exp")
        max_exp = player_data.get_field_data(self.user_id, self.table, "max_exp")

        if current_level >= self.max_treasure_level:
            return False, f"你的本命法宝已达最高等级 {self.max_treasure_level}，无法继续养成。"

        initial_level = current_level
        current_exp += amount
        level_up_messages = []

        # 循环处理升级，直到经验不足或达到最高等级
        while current_level < self.max_treasure_level and current_exp >= max_exp:
            current_level += 1
            current_exp -= max_exp
            
            player_data.update_or_write_data(self.user_id, self.table, "level", current_level)

            # 更新新的max_exp，防止最后一级经验溢出
            if current_level < self.max_treasure_level:
                max_exp += (current_level * 10) # 升级所需经验递增
                player_data.update_or_write_data(self.user_id, self.table, "max_exp", max_exp)
            else:
                current_exp = max_exp # 如果达到最高等级，经验值封顶为max_exp
                player_data.update_or_write_data(self.user_id, self.table, "exp", current_exp) # 经验封顶
                level_up_messages.append(f"本命法宝已达到最高等级 {current_level}！")
                break # 达到最高等级，停止升级循环

            level_up_messages.append(f"本命法宝等级提升至 {current_level}！")
        
        # 保存最终经验 (如果循环中没有保存，或者有剩余经验)
        player_data.update_or_write_data(self.user_id, self.table, "exp", current_exp)

        if level_up_messages:
            return True, "\n".join(level_up_messages)
        else:
            return False, f"法宝经验 +{amount}，当前经验 {current_exp}/{max_exp}。"

    def upgrade_single_effect_level(self) -> tuple[bool, str]:
        """
        提升一个法宝效果的等级，优先等级低的，消耗神秘经书。
        :return: (True如果成功升阶，False否则, 升阶信息或失败信息)。
        """
        data = self.get_data() # 从缓存或数据库获取数据
        self._natal_data_cache = None # 效果等级改变，清除缓存
        
        e1_type = data.get("effect1_type")
        e1_level = data.get("effect1_level", 0)
        e2_type = data.get("effect2_type")
        e2_level = data.get("effect2_level", 0)

        upgradable_effects = []
        
        # 判断是单效果还是双效果，并设置对应的效果等级上限
        is_double_effect_setup = (e2_type and e2_type > 0)
        max_effect_level = self.max_effect_level_double if is_double_effect_setup else self.max_effect_level_single

        # 检查效果1是否可升阶
        if e1_type and e1_type > 0 and e1_level < max_effect_level:
            upgradable_effects.append({"field": "effect1_level", "type": e1_type, "level": e1_level})
        # 检查效果2是否可升阶
        if is_double_effect_setup and e2_type and e2_type > 0 and e2_level < max_effect_level: # 只有双效果才有效果2
            upgradable_effects.append({"field": "effect2_level", "type": e2_type, "level": e2_level})

        if not upgradable_effects:
            return False, "所有效果已达最高等级，无法继续升阶。"

        # 优先选择等级较低的效果进行升阶
        upgradable_effects.sort(key=lambda x: x["level"])
        
        # 如果有多个效果等级相同且都最低，则随机选择一个
        min_level = upgradable_effects[0]["level"]
        lowest_level_effects = [e for e in upgradable_effects if e["level"] == min_level]
        
        selected_effect = random.choice(lowest_level_effects) # 随机选择一个最低等级效果
        
        field_to_upgrade = selected_effect["field"]
        current_level = selected_effect["level"]
        effect_type_value = selected_effect["type"]
        effect_name = EFFECT_NAME_MAP.get(NatalEffectType(effect_type_value), "未知效果")

        # 更新选定效果的等级
        player_data.update_or_write_data(self.user_id, self.table, field_to_upgrade, current_level + 1)
        
        return True, f"效果【{effect_name}】等级提升至 {current_level + 1}。"

    def get_data(self) -> dict | None:
        """
        获取法宝所有数据。
        :return: 包含法宝所有信息的字典，如果不存在则返回 None。
        """
        if self._natal_data_cache:
            return self._natal_data_cache # 返回缓存数据

        self._ensure_record() # 确保字段存在
        data = player_data.get_fields(self.user_id, self.table)
        if not data:
            return None
        
        # 将从数据库读取的字符串类型数字转换为实际的int/float类型
        processed_data = {}
        for k, v in data.items():
            if k in ["effect1_base_value", "effect2_base_value"]:
                processed_data[k] = float(v) if isinstance(v, str) else v
            elif k in ["form", "level", "exp", "max_exp", "effect1_type", "effect1_level", "effect2_type", "effect2_level",
                        "fate_revive_count", "immortal_revive_count", "invincible_gain_count"]:
                processed_data[k] = int(v) if isinstance(v, str) else v
            else:
                processed_data[k] = v
        
        self._natal_data_cache = {k: v for k, v in processed_data.items() if v is not None} # 过滤掉值为None的字段并缓存
        return self._natal_data_cache

    def update_data(self, field: str, value):
        """
        更新法宝特定字段的数据。
        :param field: 要更新的字段名。
        :param value: 字段的新值。
        """
        self._ensure_record()
        player_data.update_or_write_data(self.user_id, self.table, field, value)
        self._natal_data_cache = None # 清除缓存，以便下次从数据库加载最新数据

    def get_effect_value(self, effect_type: NatalEffectType, natal_treasure_level: int = 0, is_first_gain: bool = False) -> float | tuple[float, float]:
        """
        计算某个效果的最终数值。
        对于双生效果，返回 (触发概率, 伤害倍率) 元组。
        对于破盾效果，返回 (无视护盾百分比)。
        :param effect_type: 效果类型。
        :param natal_treasure_level: 法宝总等级 (用于无敌等效果的计算)。
        :param is_first_gain: 是否是本场战斗首次获得无敌 (用于无敌效果计算)。
        :return: 最终数值 (float) 或 (触发概率, 伤害倍率) 元组。
        """
        data = self.get_data()
        if not data:
            return 0.0

        for i in [1, 2]: # 遍历法宝的两个效果位
            etype = data.get(f"effect{i}_type")
            if etype == effect_type.value: # 找到匹配的效果类型
                base_value = data.get(f"effect{i}_base_value", 0.0) # 效果的基础值
                effect_level = data.get(f"effect{i}_level", 0) # 效果的等级
                
                config = EFFECT_BASE_AND_GROWTH.get(effect_type)
                if not config:
                    return 0.0
                
                growth_per_level = config.get("growth", 0.0) # 效果每级成长的数值
                
                # 特殊处理无敌效果的获得概率
                if effect_type == NatalEffectType.INVINCIBLE:
                    # base_value 存储的是配置里的 min_single/double (基础概率)
                    # total_level_growth 是法宝总等级带来的额外概率
                    total_level_growth = natal_treasure_level * growth_per_level
                    
                    if is_first_gain: # 首次获得无敌时，使用首次获得的基础概率
                        return INVINCIBLE_FIRST_GAIN_CHANCE + total_level_growth
                    else: # 后续获得无敌时，使用后续获得的基础概率
                        return INVINCIBLE_SUBSEQUENT_GAIN_CHANCE + total_level_growth
                
                # 特殊处理双生效果，base_value存概率，max_single/double存额外伤害倍率
                elif effect_type == NatalEffectType.TWIN_STRIKE:
                    # 获取触发概率 (base_value是存储的min_single/double) + 效果等级成长
                    trigger_chance = base_value + (effect_level - 1) * growth_per_level
                    
                    # 获取伤害倍率 (固定100%，即1.0)
                    damage_multiplier = 1.0 # 伤害倍率固定100%
                    
                    return trigger_chance, damage_multiplier # 返回概率和倍率元组
                
                # 其他常规效果的计算方式：基础值 + (效果等级-1) * 每级成长值
                return base_value + (effect_level - 1) * growth_per_level # 初始等级1已包含基础值

        return 0.0 # 如果没有找到对应的效果类型，返回0.0

    def get_effect_desc(self) -> str:
        """
        获取法宝的详细描述信息。
        :return: 格式化的法宝描述字符串。
        """
        data = self.get_data()
        if not data or not data.get("form"):
            return "你尚未觉醒本命法宝！\n发送【觉醒本命法宝】进行首次觉醒"

        lines = []
        name = data.get("name", "未知法宝")
        level = data.get("level", 0)
        exp = data.get("exp", 0)
        max_exp = data.get("max_exp", 100)

        lines.append(f"【{name}】  总等级：{level}/{self.max_treasure_level}")
        if level < self.max_treasure_level:
            lines.append(f"  └─ 养成进度：{exp}/{max_exp}")
        
        effect_count = 0
        for i in [1, 2]: # 遍历两个效果位
            etype = data.get(f"effect{i}_type")
            if etype and etype > 0:
                effect_count += 1
                natal_effect_type = NatalEffectType(etype)
                effect_name = EFFECT_NAME_MAP.get(natal_effect_type, "未知效果")
                effect_level = data.get(f"effect{i}_level", 0)
                
                # 根据是否为双效果配置，确定当前效果等级上限
                current_max_effect_level = self.max_effect_level_double if data.get("effect2_type", 0) not in (0, None) else self.max_effect_level_single

                if natal_effect_type == NatalEffectType.FATE:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：{round(final_value * 100, 2)}% 复活概率")
                elif natal_effect_type == NatalEffectType.IMMORTAL:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：{round(final_value * 100, 2)}% 恢复血量")
                elif natal_effect_type == NatalEffectType.DEATH_STRIKE:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：对生命低于{round(final_value * 100, 2)}%的敌人斩杀")
                elif natal_effect_type == NatalEffectType.INVINCIBLE:
                    # 无敌不直接有值，只在 periodic_effect 中计算次数和概率
                    # 这里显示基础获得概率和总等级成长概率
                    config = EFFECT_BASE_AND_GROWTH.get(natal_effect_type)
                    total_level_growth = level * config.get("growth", 0.0) # 法宝总等级带来的额外概率
                    
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：首次获得概率{round((INVINCIBLE_FIRST_GAIN_CHANCE + total_level_growth) * 100, 2)}%，后续获得概率{round((INVINCIBLE_SUBSEQUENT_GAIN_CHANCE + total_level_growth) * 100, 2)}%")
                elif natal_effect_type == NatalEffectType.TWIN_STRIKE:
                    trigger_chance, damage_multiplier = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：普通攻击时有{round(trigger_chance * 100, 2)}% 概率连击，额外造成 {round(damage_multiplier * 100, 2)}% 伤害")
                elif natal_effect_type == NatalEffectType.SHIELD_BREAK:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击有护盾的敌人时，无视其{round(final_value * 100, 2)}%护盾并额外造成{round(SHIELD_BREAK_BONUS_DAMAGE * 100, 2)}%伤害")
                else: # 其他常规百分比效果
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：{round(final_value * 100, 2)}%")

        if effect_count == 0:
            lines.append("  └─ 暂无效果（请尝试重塑）")
        
        # 补充周期性真伤描述 (此效果与法宝总等级相关)
        periodic_true_dmg_rate = PERIODIC_TRUE_DAMAGE_BASE + level * PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL
        lines.append(f"\n  ◎ 道韵：每4回合对所有敌方造成当前生命 {round(periodic_true_dmg_rate * 100, 2)}% 的真实伤害。")


        return "\n".join(lines)