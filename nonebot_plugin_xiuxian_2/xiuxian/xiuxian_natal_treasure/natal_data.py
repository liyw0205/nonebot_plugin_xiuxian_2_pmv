import random
import json
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
        self.max_effect_level_all_effects = MAX_EFFECT_LEVEL_ALL_EFFECTS # 所有效果的等级上限
        self.max_effect_slots = MAX_EFFECT_SLOTS # 效果槽位上限
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
            "effect1_base_value": 0.0, # 效果1的基础值 (随机生成，对应配置中的min/max_value)
            "effect1_level": 0, # 效果1的等级
            "effect2_type": 0,  # 效果2类型
            "effect2_base_value": 0.0, # 效果2的基础值
            "effect2_level": 0, # 效果2的等级
            "effect3_type": 0,  # 效果3类型 (新增槽位)
            "effect3_base_value": 0.0, # 效果3的基础值
            "effect3_level": 0, # 效果3的等级
            # 新增效果需要额外字段来追踪战斗中的使用次数，这些在战斗结束后会同步回数据库
            "fate_revive_count": 0,    # 天命复活已使用次数
            "immortal_revive_count": 0, # 不灭复活已使用次数
            "invincible_gain_count": 0, # 无敌已获取次数（周期性生效时获得）
            # 新增涅槃和魂返次数
            "nirvana_revive_count": 0,  # 涅槃复活已使用次数
            "soul_return_revive_count": 0, # 魂返复活已使用次数
            "charge_status": 0, # 蓄力状态 (0:未蓄力, 1:蓄力中)
            "soul_summon_count": {}, # 招魂已使用次数 {ally_id: count}
            "enlightenment_count": {}, # 启明已使用次数 {ally_id: count}
        }
        
        # 逐个检查字段是否存在并添加，以兼容旧数据
        record = player_data.get_fields(self.user_id, self.table)
        
        # 记录是否存在（判断是否是首次创建）
        is_new_record = not bool(record)

        if is_new_record: # 如果用户在表中完全没有记录，则创建
            for field, default_value in default_data.items():
                # 只有当default_value为字典时，才使用JSON字符串存储，否则直接存储
                store_value = json.dumps(default_value) if isinstance(default_value, dict) else default_value
                player_data.update_or_write_data(self.user_id, self.table, field, store_value, data_type=type(default_value).__name__.upper() if not isinstance(default_value, dict) else 'TEXT')
        else: # 如果记录已存在，检查并添加新字段
            for field, default_value in default_data.items():
                # 如果这个字段在数据库中不存在或值为None，则设置默认值
                # 这里get_fields会尝试反序列化，所以如果存的是JSON字符串，会返回字典
                if field not in record or record.get(field) is None: 
                    # 只有当字段不存在于record中，或者其值为None时，才设置默认值
                    store_value = json.dumps(default_value) if isinstance(default_value, dict) else default_value
                    player_data.update_or_write_data(self.user_id, self.table, field, store_value, data_type=type(default_value).__name__.upper() if not isinstance(default_value, dict) else 'TEXT')
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
        首次觉醒默认只生成一个效果。重塑时清空所有效果槽位，然后重新生成一个效果。
        :param force_new: 如果为True，强制重新生成所有属性（重塑）。
        """
        self._ensure_record()
        current_form = player_data.get_field_data(self.user_id, self.table, "form")

        if not current_form or force_new:
            # 随机法宝形态 (例如，1到4代表不同的形态图片或描述)
            form = random.randint(1, 4)
            player_data.update_or_write_data(self.user_id, self.table, "form", form)
        
        # 觉醒/重塑时，现在默认只随机生成1个效果
        available_effect_types = list(NatalEffectType) # 所有可能的法宝效果类型
        selected_type = random.choice(available_effect_types) # 随机选择一个效果

        # 名字以这个效果类型为基准，从对应的名字池中随机选择
        name_pool = NATAL_TREASURE_NAMES.get(selected_type, ["未知法宝"])
        selected_name = random.choice(name_pool)
        player_data.update_or_write_data(self.user_id, self.table, "name", selected_name)

        # 重置所有效果相关字段为默认值，以便写入新效果
        for i in range(1, self.max_effect_slots + 1): # 遍历所有效果槽位
            player_data.update_or_write_data(self.user_id, self.table, f"effect{i}_type", 0)
            player_data.update_or_write_data(self.user_id, self.table, f"effect{i}_base_value", 0.0, data_type='REAL')
            player_data.update_or_write_data(self.user_id, self.table, f"effect{i}_level", 0)
        
        # 重置新的次数统计字段，重塑时这些都归零
        player_data.update_or_write_data(self.user_id, self.table, "fate_revive_count", 0)
        player_data.update_or_write_data(self.user_id, self.table, "immortal_revive_count", 0)
        player_data.update_or_write_data(self.user_id, self.table, "invincible_gain_count", 0)
        player_data.update_or_write_data(self.user_id, self.table, "nirvana_revive_count", 0) # 新增涅槃次数重置
        player_data.update_or_write_data(self.user_id, self.table, "soul_return_revive_count", 0) # 新增魂返次数重置
        player_data.update_or_write_data(self.user_id, self.table, "charge_status", 0) # 蓄力状态重置
        player_data.update_or_write_data(self.user_id, self.table, "soul_summon_count", json.dumps({}), data_type='TEXT') # 招魂次数重置
        player_data.update_or_write_data(self.user_id, self.table, "enlightenment_count", json.dumps({}), data_type='TEXT') # 启明次数重置


        # 写入新生成的法宝效果 (只写effect1)
        config = EFFECT_BASE_AND_GROWTH.get(selected_type)
        if config:
            base_value_min = config["min_value"]
            base_value_max = config["max_value"]
            
            # 特殊处理概率类效果，base_value存储的是配置中的min_value（基础概率）
            if selected_type in [NatalEffectType.INVINCIBLE, NatalEffectType.TWIN_STRIKE,
                                NatalEffectType.SLEEP, NatalEffectType.PETRIFY, NatalEffectType.STUN,
                                NatalEffectType.FATIGUE, NatalEffectType.SILENCE,
                                NatalEffectType.NIRVANA, NatalEffectType.SOUL_RETURN,
                                NatalEffectType.SOUL_SUMMON, NatalEffectType.ENLIGHTENMENT]:
                base_value = base_value_min 
            else:
                # 对于其他效果，随机生成一个基础数值
                base_value = round(random.uniform(base_value_min, base_value_max), 3) # 增加精度
            
            player_data.update_or_write_data(self.user_id, self.table, "effect1_type", selected_type.value)
            player_data.update_or_write_data(self.user_id, self.table, "effect1_base_value", base_value, data_type='REAL')
            player_data.update_or_write_data(self.user_id, self.table, "effect1_level", 1) # 初始等级为1

        # 重置法宝总等级和经验为初始状态
        player_data.update_or_write_data(self.user_id, self.table, "level", 0)
        player_data.update_or_write_data(self.user_id, self.table, "exp", 0)
        player_data.update_or_write_data(self.user_id, self.table, "max_exp", 100) # 初始100经验升级
        self._natal_data_cache = None # 清除缓存

    def engrave_effect(self) -> tuple[bool, str]:
        """
        为本命法宝铭刻一个新的道纹（效果）。
        :return: (True如果成功铭刻，False否则, 铭刻信息或失败信息)。
        """
        data = self.get_data()
        if not data:
            return False, "你尚未觉醒本命法宝，无法铭刻道纹！"
        
        current_effect_count = 0
        current_effect_types = set()
        empty_slot_idx = -1

        for i in range(1, self.max_effect_slots + 1):
            etype = data.get(f"effect{i}_type", 0)
            if etype > 0:
                current_effect_count += 1
                current_effect_types.add(NatalEffectType(etype))
            elif empty_slot_idx == -1: # 找到第一个空槽位
                empty_slot_idx = i

        if current_effect_count >= self.max_effect_slots:
            return False, f"你的本命法宝效果槽位已满 ({self.max_effect_slots}个)，无法继续铭刻新的道纹。"
        
        if empty_slot_idx == -1: # 理论上不会发生，但以防万一
            return False, "未找到可用的铭刻槽位。"

        available_effect_types = list(NatalEffectType)
        # 排除已有的效果类型
        potential_new_effects = [e for e in available_effect_types if e not in current_effect_types]

        if not potential_new_effects:
            return False, "所有效果类型都已被你的法宝拥有，无法铭刻新的道纹。"
        
        new_effect_type = random.choice(potential_new_effects)

        # 获取效果的基础范围配置
        config = EFFECT_BASE_AND_GROWTH.get(new_effect_type)
        if not config:
            return False, "新效果配置异常，铭刻失败。"
        
        base_value_min = config["min_value"]
        base_value_max = config["max_value"]
        
        # 特殊处理概率类效果，base_value存储的是配置中的min_value（基础概率）
        if new_effect_type in [NatalEffectType.INVINCIBLE, NatalEffectType.TWIN_STRIKE,
                             NatalEffectType.SLEEP, NatalEffectType.PETRIFY, NatalEffectType.STUN,
                             NatalEffectType.FATIGUE, NatalEffectType.SILENCE,
                             NatalEffectType.NIRVANA, NatalEffectType.SOUL_RETURN,
                             NatalEffectType.SOUL_SUMMON, NatalEffectType.ENLIGHTENMENT]:
            base_value = base_value_min 
        else:
            base_value = round(random.uniform(base_value_min, base_value_max), 3) # 增加精度
        
        # 将新效果写入找到的空槽位
        player_data.update_or_write_data(self.user_id, self.table, f"effect{empty_slot_idx}_type", new_effect_type.value)
        player_data.update_or_write_data(self.user_id, self.table, f"effect{empty_slot_idx}_base_value", base_value, data_type='REAL')
        player_data.update_or_write_data(self.user_id, self.table, f"effect{empty_slot_idx}_level", 1) # 初始等级为1
        self._natal_data_cache = None # 清除缓存

        effect_name_cn = EFFECT_NAME_MAP.get(new_effect_type, "未知效果")
        return True, f"成功铭刻道纹：【{effect_name_cn}】，等级1。"

    def forget_effect(self, effect_type_to_forget: NatalEffectType) -> tuple[bool, str]:
        """
        遗忘本命法宝上的一个道纹（效果）。
        :param effect_type_to_forget: 要遗忘的效果类型。
        :return: (True如果成功遗忘，False否则, 遗忘信息或失败信息)。
        """
        data = self.get_data()
        if not data:
            return False, "你尚未觉醒本命法宝，无法遗忘道纹！"
        
        current_effect_count = 0
        effect_slot_to_clear = -1
        
        for i in range(1, self.max_effect_slots + 1):
            etype_val = data.get(f"effect{i}_type", 0)
            if etype_val > 0:
                current_effect_count += 1
                if NatalEffectType(etype_val) == effect_type_to_forget:
                    effect_slot_to_clear = i
        
        if effect_slot_to_clear == -1:
            effect_name_cn = EFFECT_NAME_MAP.get(effect_type_to_forget, "未知效果")
            return False, f"你的本命法宝上没有【{effect_name_cn}】这个道纹，无法遗忘。"

        if current_effect_count <= 1:
            return False, "你的本命法宝至少需要保留一个道纹，无法遗忘！"

        # 清空对应槽位的数据
        player_data.update_or_write_data(self.user_id, self.table, f"effect{effect_slot_to_clear}_type", 0)
        player_data.update_or_write_data(self.user_id, self.table, f"effect{effect_slot_to_clear}_base_value", 0.0, data_type='REAL')
        player_data.update_or_write_data(self.user_id, self.table, f"effect{effect_slot_to_clear}_level", 0)
        self._natal_data_cache = None # 清除缓存

        effect_name_cn = EFFECT_NAME_MAP.get(effect_type_to_forget, "未知效果")
        return True, f"成功遗忘道纹：【{effect_name_cn}】。"


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
                max_exp = int(MAX_EXP_BASE + current_level * MAX_EXP_GROWTH_PER_LEVEL) # 更新经验计算公式
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
        
        e_effects = []
        for i in range(1, self.max_effect_slots + 1): # 遍历所有效果槽位
            etype = data.get(f"effect{i}_type")
            if etype and etype > 0:
                e_effects.append({"field": f"effect{i}_level", "type": etype, "level": data.get(f"effect{i}_level", 0)})

        upgradable_effects = []
        # 所有效果等级上限统一
        max_effect_level = self.max_effect_level_all_effects

        for effect_data in e_effects:
            if effect_data["level"] < max_effect_level:
                upgradable_effects.append(effect_data)

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
            if k in [f"effect{i}_base_value" for i in range(1, self.max_effect_slots + 1)]: # 所有效果的基础值
                processed_data[k] = float(v) if isinstance(v, str) else v
            elif k in ["form", "level", "exp", "max_exp",
                       *[f"effect{i}_type" for i in range(1, self.max_effect_slots + 1)],
                       *[f"effect{i}_level" for i in range(1, self.max_effect_slots + 1)],
                        "fate_revive_count", "immortal_revive_count", "invincible_gain_count",
                        "nirvana_revive_count", "soul_return_revive_count", "charge_status"]: # 新增次数和状态
                processed_data[k] = int(v) if isinstance(v, str) and v.isdigit() else v # 确保是数字字符串才转int
            elif k in ["soul_summon_count", "enlightenment_count"]: # 新增的次数统计字段是字典，需要JSON反序列化
                processed_data[k] = json.loads(v) if isinstance(v, str) else v
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
        # 根据字段类型，给update_or_write_data传递正确的数据类型
        data_type = 'TEXT' # 默认TEXT
        if field in [f"effect{i}_base_value" for i in range(1, self.max_effect_slots + 1)]:
            data_type = 'REAL'
        elif field in ["form", "level", "exp", "max_exp",
                       *[f"effect{i}_type" for i in range(1, self.max_effect_slots + 1)],
                       *[f"effect{i}_level" for i in range(1, self.max_effect_slots + 1)],
                       "fate_revive_count", "immortal_revive_count", "invincible_gain_count",
                       "nirvana_revive_count", "soul_return_revive_count", "charge_status"]:
            data_type = 'INTEGER'
        elif field in ["soul_summon_count", "enlightenment_count"]:
            value = json.dumps(value) # 字典类型存储为JSON字符串
            data_type = 'TEXT'
        
        player_data.update_or_write_data(self.user_id, self.table, field, value, data_type=data_type)
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

        for i in range(1, self.max_effect_slots + 1): # 遍历法宝的所有效果位
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
                    # total_level_growth 是法宝总等级带来的额外概率
                    total_level_growth = natal_treasure_level * INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE # 使用专门的无敌总等级成长系数
                    
                    if is_first_gain: # 首次获得无敌时，使用首次获得的基础概率
                        return INVINCIBLE_FIRST_GAIN_CHANCE + total_level_growth
                    else: # 后续获得无敌时，使用后续获得的基础概率
                        return INVINCIBLE_SUBSEQUENT_GAIN_CHANCE + total_level_growth
                
                # 特殊处理双生效果，base_value存概率，max_value存额外伤害倍率
                elif effect_type == NatalEffectType.TWIN_STRIKE:
                    # 获取触发概率 (base_value是存储的min_value) + 效果等级成长
                    trigger_chance = base_value + (effect_level - 1) * growth_per_level
                    
                    # 获取伤害倍率 (固定100%，即1.0)
                    damage_multiplier = 1.0 # 伤害倍率固定100%
                    
                    return trigger_chance, damage_multiplier # 返回概率和倍率元组
                
                # 新增蓄力和神力
                elif effect_type == NatalEffectType.CHARGE:
                    # 蓄力效果直接返回增加的伤害百分比
                    return base_value + (effect_level - 1) * growth_per_level

                elif effect_type == NatalEffectType.DIVINE_POWER:
                    # 神力效果直接返回增加的伤害百分比
                    return base_value + (effect_level - 1) * growth_per_level
                
                # 其他概率类效果 (睡眠、石化、眩晕、疲劳、沉默、涅槃、魂返、招魂、启明)
                elif effect_type in [NatalEffectType.SLEEP, NatalEffectType.PETRIFY, NatalEffectType.STUN,
                                     NatalEffectType.FATIGUE, NatalEffectType.SILENCE,
                                     NatalEffectType.NIRVANA, NatalEffectType.SOUL_RETURN,
                                     NatalEffectType.SOUL_SUMMON, NatalEffectType.ENLIGHTENMENT]:
                    return base_value + (effect_level - 1) * growth_per_level

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
        for i in range(1, self.max_effect_slots + 1): # 遍历所有效果槽位
            etype = data.get(f"effect{i}_type")
            if etype and etype > 0:
                effect_count += 1
                natal_effect_type = NatalEffectType(etype)
                effect_name = EFFECT_NAME_MAP.get(natal_effect_type, "未知效果")
                effect_level = data.get(f"effect{i}_level", 0)
                
                # 效果等级上限统一为MAX_EFFECT_LEVEL_ALL_EFFECTS
                current_max_effect_level = self.max_effect_level_all_effects

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
                    
                    # 获取当前法宝总等级（用于显示描述）
                    current_natal_treasure_level_for_desc = data.get("level", 0)
                    total_level_growth = current_natal_treasure_level_for_desc * INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE # 法宝总等级带来的额外概率
                    
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：首次获得概率{round((INVINCIBLE_FIRST_GAIN_CHANCE + total_level_growth) * 100, 2)}%，后续获得概率{round((INVINCIBLE_SUBSEQUENT_GAIN_CHANCE + total_level_growth) * 100, 2)}%")
                elif natal_effect_type == NatalEffectType.TWIN_STRIKE:
                    trigger_chance, damage_multiplier = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：普通攻击时有{round(trigger_chance * 100, 2)}% 概率连击，额外造成 {round(damage_multiplier * 100, 2)}% 伤害")
                elif natal_effect_type == NatalEffectType.SHIELD_BREAK:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击有护盾的敌人时，无视其{round(final_value * 100, 2)}%护盾并额外造成{round(SHIELD_BREAK_BONUS_DAMAGE * 100, 2)}%伤害")
                # 新增效果描述
                elif natal_effect_type == NatalEffectType.SLEEP:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击时有{round(final_value * 100, 2)}%概率使目标睡眠{SLEEP_DURATION}回合")
                elif natal_effect_type == NatalEffectType.PETRIFY:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击时有{round(final_value * 100, 2)}%概率使目标石化{PETRIFY_DURATION}回合")
                elif natal_effect_type == NatalEffectType.STUN:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击时有{round(final_value * 100, 2)}%概率使目标眩晕{STUN_DURATION}回合")
                elif natal_effect_type == NatalEffectType.FATIGUE:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击时有{round(final_value * 100, 2)}%概率使目标疲劳{FATIGUE_DURATION}回合，攻击力降低{round(FATIGUE_ATTACK_REDUCTION * 100, 2)}%")
                elif natal_effect_type == NatalEffectType.SILENCE:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击时有{round(final_value * 100, 2)}%概率使目标沉默{SILENCE_DURATION}回合")
                elif natal_effect_type == NatalEffectType.CHARGE:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：本回合不攻击，下回合伤害额外提升{round(CHARGE_BONUS_DAMAGE * 100 + final_value * 100, 2)}% (固定{round(CHARGE_BONUS_DAMAGE * 100, 2)}% + 等级提升)") # 蓄力效果固定值+等级加成
                elif natal_effect_type == NatalEffectType.DIVINE_POWER:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击力额外提升{round(final_value * 100, 2)}%")
                elif natal_effect_type == NatalEffectType.NIRVANA:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：阵亡时有队友在场，进入涅槃{NIRVANA_DURATION}回合后满血复活，并获得最大生命{(NIRVANA_SHIELD_BASE + final_value) * 100:.2f}%护盾，仅{NIRVANA_REVIVE_LIMIT}次")
                elif natal_effect_type == NatalEffectType.SOUL_RETURN:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：阵亡时有队友在场，进入灵体{SOUL_RETURN_DURATION}回合后回复最大生命{(SOUL_RETURN_HP_BASE + final_value) * 100:.2f}%复活，期间可正常攻击且不会被攻击，仅{SOUL_RETURN_REVIVE_LIMIT}次")
                elif natal_effect_type == NatalEffectType.SOUL_SUMMON:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击时有{round(final_value * 100, 2)}%概率让已死亡的队友进入魂返状态，每个队友仅可触发{SOUL_SUMMON_LIMIT}次。")
                elif natal_effect_type == NatalEffectType.ENLIGHTENMENT:
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：攻击时有{round(final_value * 100, 2)}%概率让已死亡的队友回复{ENLIGHTENMENT_REVIVE_HP_PERCENT * 100:.2f}%生命值复活，每个队友仅可触发{ENLIGHTENMENT_LIMIT}次。")
                else: # 其他常规百分比效果
                    final_value = self.get_effect_value(natal_effect_type)
                    lines.append(f"  └─ {effect_name} Lv.{effect_level}/{current_max_effect_level}：{round(final_value * 100, 2)}%")

        if effect_count == 0:
            lines.append("  └─ 暂无效果（请尝试觉醒）")
        
        # 补充周期性真伤描述 (此效果与法宝总等级相关)
        periodic_true_dmg_rate = PERIODIC_TRUE_DAMAGE_BASE + level * PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL
        lines.append(f"\n  ◎ 道韵：每4回合对所有敌方造成当前生命 {round(periodic_true_dmg_rate * 100, 2)}% 的真实伤害。")


        return "\n".join(lines)