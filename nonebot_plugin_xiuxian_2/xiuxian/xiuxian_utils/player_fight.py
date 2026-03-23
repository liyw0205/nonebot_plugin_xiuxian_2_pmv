import json
import random
from enum import IntEnum
from pathlib import Path
from nonebot.log import logger

# 导入自定义模块和配置
from .xiuxian2_handle import XiuxianDateManage, OtherSet, UserBuffDate, XIUXIAN_IMPART_BUFF
from ..xiuxian_config import convert_rank
from .utils import number_to
from .item_json import Items
from ..xiuxian_natal_treasure import NatalTreasure, NatalEffectType, EFFECT_NAME_MAP
from ..xiuxian_natal_treasure.natal_config import FATE_REVIVE_COUNT_LIMIT, IMMORTAL_REVIVE_COUNT_LIMIT, INVINCIBLE_COUNT_LIMIT, PERIODIC_TRUE_DAMAGE_BASE, PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL, SHIELD_BREAK_BONUS_DAMAGE # 导入次数限制和道韵的配置

items = Items()  # 物品管理类实例
sql_message = XiuxianDateManage()  # SQL数据管理类实例
xiuxian_impart = XIUXIAN_IMPART_BUFF() # 虚神界传承Buff管理类实例


async def pve_fight(user, monster, type_in=2, bot_id=0, level_ratios=None):
    """
    PVE战斗主函数，处理玩家 vs. 多个怪物或BOSS的战斗。
    :param user: 玩家列表，每个元素是一个用户ID。
    :param monster: 怪物或BOSS列表，每个元素是一个字典，包含怪物属性。
    :param type_in: 战斗类型，1=不更新数据，2=更新数据（默认）。
    :param bot_id: 机器人ID。
    :param level_ratios: 等级系数，用于调整玩家属性。
    :return: 战斗日志列表，胜利方，最终状态列表。
    """
    user_data = []
    monster_data = []

    # 初始化玩家实体
    for u in user:
        player_data = get_players_attributes(u, level_ratios)
        player = Entity(player_data["属性"], team_id=0)  # 玩家队伍ID为0
        apply_player_buffs(player, player_data)  # 添加玩家Buff和技能
        user_data.append(player)
    
    # 初始化怪物实体
    for m in monster:
        enemy_data = get_boss_attributes(m, bot_id)
        enemy = Entity(enemy_data["属性"], team_id=1, is_boss=True)  # 怪物队伍ID为1，标记为BOSS
        enemy.start_skills.extend(generate_boss_buff(m))  # 添加BOSS Buff
        generate_boss_skill(enemy, m.get("skills", []))  # 添加BOSS技能
        monster_data.append(enemy)

    battle = BattleSystem(user_data, monster_data, bot_id)  # 创建战斗系统实例
    play_list, winner, status_list = battle.run_battle()  # 运行战斗

    # 根据战斗类型更新玩家数据
    if type_in == 2:
        update_all_user_status(status_list, bot_id, level_ratios)  # 更新玩家数据

    return play_list, winner, status_list


def Player_fight(user1, user2, type_in=1, bot_id=0):
    """
    玩家PVP战斗主函数。
    :param user1: 玩家1的用户ID。
    :param user2: 玩家2的用户ID。
    :param type_in: 战斗类型，1=不更新数据，2=更新数据（默认）。
    :param bot_id: 机器人ID。
    :return: 战斗日志列表，胜利者昵称，最终状态列表。
    """
    player1_data = get_players_attributes(user1)  # 获取玩家1数据
    player2_data = get_players_attributes(user2)  # 获取玩家2数据

    player1 = Entity(player1_data["属性"], team_id=0)  # 玩家1队伍ID为0
    player2 = Entity(player2_data["属性"], team_id=1)  # 玩家2队伍ID为1

    apply_player_buffs(player1, player1_data)  # 添加玩家1 Buff和技能
    apply_player_buffs(player2, player2_data)  # 添加玩家2 Buff和技能

    battle = BattleSystem([player1], [player2], bot_id)  # 创建战斗系统实例
    play_list, winner, status_list = battle.run_battle()  # 运行战斗

    # 判断胜利者
    if winner == 0:
        suc = player1_data["属性"]["nickname"]
    elif winner == 1:
        suc = player2_data["属性"]["nickname"]
    else:  # 平局处理
        suc = "没有人"

    # 根据战斗类型更新玩家数据
    if type_in == 2:
        update_all_user_status(status_list, bot_id)

    return play_list, suc


async def Boss_fight(user1, boss: dict, type_in=2, bot_id=0):
    """
    BOSS战斗主函数。
    :param user1: 玩家1的用户ID。
    :param boss: BOSS数据字典。
    :param type_in: 战斗类型，1=不更新数据，2=更新数据（默认）。
    :param bot_id: 机器人ID。
    :return: 战斗日志列表，胜利者消息，更新后的BOSS数据。
    """
    # --- 1. 获取数据 ---
    player1_data = get_players_attributes(user1)  # 获取玩家数据
    boss_data = get_boss_attributes(boss, bot_id)  # 获取BOSS数据

    # --- 2. 初始化 ---
    player1 = Entity(player1_data["属性"], team_id=0)  # 玩家实体
    boss1 = Entity(boss_data["属性"], team_id=1, is_boss=True)  # BOSS实体

    apply_player_buffs(player1, player1_data)  # 添加玩家Buff和技能

    # boss添加buff
    boss1.start_skills.extend(generate_boss_buff(boss))

    if not boss['name'] == "稻草人":  # 稻草人不加技能
        # boss添加技能
        generate_boss_skill(boss1, [14001, 14002])  # 添加技能

    # --- 3. 运行 ---
    battle = BattleSystem([player1], [boss1], bot_id)  # 创建战斗系统实例
    play_list, winner, status_list = battle.run_battle()  # 运行战斗

    # 更新boss数据（主要是血量和真元）
    update_data_boss_status(boss, status_list)

    # 判断胜利者
    if winner == 0:
        suc = "群友赢了"
    else:
        suc = "Boss赢了"

    # 根据战斗类型更新玩家数据
    if type_in == 2:
        update_all_user_status(status_list, bot_id)  # 更新玩家数据

    return play_list, suc, boss


# ---------- 玩家数据部分 ----------
def get_players_attributes(user_id, level_ratios=None):
    """
    获取玩家所有属性（基础属性、装备、功法、buff等加成）。
    :param user_id: 玩家用户ID。
    :param level_ratios: 等级系数，用于调整玩家属性。
    :return: 包含玩家属性和Buff信息的字典。
    """
    # 获取用户所有装备功法buff数据
    buff_data_info = UserBuffDate(user_id).BuffInfo
    buffs = {}
    ratio = 1
    if level_ratios:
        ratio = level_ratios.get(user_id, 1)

    # 定义buff类型映射，用于从buff_data_info中提取物品数据
    buff_types = {
        'main_buff': '主功法',
        'sub_buff': '辅修功法',
        'sec_buff': '神通技能',
        'effect1_buff': '身法',
        'effect2_buff': '瞳术',
        'faqi_buff': '法器',
        'armor_buff': '防具'
    }

    # 遍历buff类型，获取对应的物品数据
    for key, display_name in buff_types.items():
        item_id = buff_data_info.get(key, 0)
        if item_id != 0:
            item_data = items.get_data_by_item_id(item_id)
            buffs[display_name] = item_data

    # 玩家基础信息和虚神界传承信息
    user_info = sql_message.get_user_info_with_id(user_id)
    user_impart_info = xiuxian_impart.get_user_impart_info_with_id(user_id)

    # 法器防具加成 - 使用.get()方法避免KeyError
    faqi_data = buffs.get('法器', {})
    armor_data = buffs.get('防具', {})
    main_gongfa_data = buffs.get('主功法', {})

    weapon_mp_cost_modifier = faqi_data.get('mp_buff', 0)
    weapon_atk_buff = faqi_data.get('atk_buff', 0)
    armor_atk_buff = armor_data.get('atk_buff', 0)
    weapon_crit_buff = faqi_data.get('crit_buff', 0)
    armor_crit_buff = armor_data.get('crit_buff', 0)
    weapon_critatk = faqi_data.get('critatk', 0)
    weapon_def = faqi_data.get('def_buff', 0)
    armor_def = armor_data.get('def_buff', 0)

    # 功法加成
    main_hp_buff = main_gongfa_data.get('hpbuff', 0)
    main_mp_buff = main_gongfa_data.get('mpbuff', 0)
    main_atk_buff = main_gongfa_data.get('atkbuff', 0)
    main_crit_buff = main_gongfa_data.get('crit_buff', 0)
    main_critatk = main_gongfa_data.get('critatk', 0)
    main_def = main_gongfa_data.get('def_buff', 0)

    # 宗门修炼加成
    hppractice = user_info['hppractice'] * 0.05
    mppractice = user_info['mppractice'] * 0.05
    atkpractice = user_info['atkpractice'] * 0.04

    # 虚神界加成
    impart_hp_per = user_impart_info['impart_hp_per']
    impart_mp_per = user_impart_info['impart_mp_per']
    impart_atk_per = user_impart_info['impart_atk_per']
    impart_know_per = user_impart_info['impart_know_per']
    impart_burst_per = user_impart_info['impart_burst_per']
    boss_atk = user_impart_info['boss_atk']

    # 计算最终属性
    max_hp = int((user_info['exp'] / 2) * (1 + main_hp_buff + impart_hp_per + hppractice))
    hp = int(user_info['hp'] * (1 + main_hp_buff + impart_hp_per + hppractice))
    max_mp = int(user_info['exp'] * (1 + main_mp_buff + impart_mp_per + mppractice))
    mp = int(user_info['mp'] * (1 + main_mp_buff + impart_mp_per + mppractice))
    atk = int((user_info['atk'] * (atkpractice + 1) * (1 + main_atk_buff) * (
            1 + weapon_atk_buff) * (1 + armor_atk_buff)) * (1 + impart_atk_per)) + int(buff_data_info.get('atk_buff', 0))
    crit = max(0, min(1, weapon_crit_buff + armor_crit_buff + main_crit_buff + impart_know_per))
    critatk = 1.5 + impart_burst_per + weapon_critatk + main_critatk
    dr = armor_def + weapon_def + main_def
    hit = 100  # 基础命中
    dodge = 0  # 基础闪避
    ap = 0  # 基础穿甲
    speed = 10  # 玩家基础速度

    # 玩家属性字典
    attributes = {
        "user_id": user_id,  # 用户唯一标识符
        "nickname": user_info['user_name'],  # 用户昵称
        "max_hp": int(max_hp * ratio),  # 生命值上限
        "current_hp": int(hp * ratio),  # 当前生命值
        "max_mp": int(max_mp * ratio),  # 最大真元值
        "current_mp": int(mp * ratio),  # 当前真元值
        "mp_cost_modifier": weapon_mp_cost_modifier,  # 真元消耗修正
        "attack": int(atk * ratio),  # 攻击力
        "exp": int(user_info['exp'] * ratio),  # 当前经验值
        "critical_rate": crit,  # 暴击率 (0-1范围)
        "critical_damage": critatk,  # 暴击伤害倍数
        "boss_damage_bonus": boss_atk,  # 对BOSS伤害加成
        "damage_reduction": dr,  # 伤害减免率
        "armor_penetration": ap,  # 护甲穿透值
        "accuracy": hit,  # 命中率 (百分比)
        "dodge": dodge,  # 闪避率 (百分比)
        "speed": speed,  # 速度系数
        "start_skills": []  # 初始技能
    }

    # 将三个数据合成一个列表输出
    buffs["属性"] = attributes
    buffs["其他"] = buff_data_info

    # 返回结果
    return buffs


def apply_player_buffs(player, player_data):
    """
    根据 player_data 自动生成并添加各种 buff和技能。
    :param player: Entity 实例，表示玩家。
    :param player_data: 包含玩家属性和Buff信息的字典。
    """
    # --- 定义 buff 生成配置 ---
    buff_config = [
        ("主功法", generate_main_buff, lambda d: (d, player_data.get("其他", {}).get("faqi_buff", 0))),
        ("辅修功法", generate_sub_buff, lambda d: (d, buff_type_mapping)),
        ("身法", generate_effect_buff, lambda d: (d,)),
        ("瞳术", generate_effect_buff, lambda d: (d,))
    ]

    # --- 通用 buff 添加 ---
    for key, generator, args_builder in buff_config:
        if data := player_data.get(key):
            args = args_builder(data)
            buffs = generator(*args)
            player.start_skills.extend(buffs)

    # --- 神通技能（非 buff，独立逻辑）---
    if st_data := player_data.get("神通技能"):
        player.skills.append(Skill(st_data))


def generate_sub_buff(skill, buff_type_mapping_param):
    """
    根据辅修功法技能配置自动生成 buff 列表。
    :param skill: 技能数据字典。
    :param buff_type_mapping_param: Buff类型映射字典。
    :return: Buff字典列表。
    """

    name = skill["name"]
    buff_type_id = int(skill["buff_type"])
    
    # 原始的 buff 和 buff2 值
    raw_buff_value = float(skill["buff"])
    raw_buff2_value = float(skill["buff2"])

    v1 = raw_buff_value / 100 # 大多数百分比属性需要除以100
    v2 = raw_buff2_value / 100

    is_debuff = False
    
    # 特殊处理：斗战或穿甲，其值是直接的破甲值 (0-1范围)，不需要除以100
    if buff_type_id == 13 or buff_type_id == 14: 
        v1 = float(skill["break"]) # 'break'字段已经是0-1的小数
        
    # 特殊处理：中毒或禁止吸取，这些是状态而非数值buff，value可能用于持续时间或强度
    if buff_type_id == 8 or buff_type_id == 10: 
        is_debuff = True
        # 对于中毒，v1是伤害百分比，通常是0-1的小数，所以这里需要除以100
        if buff_type_id == 8:
            v1 = raw_buff_value / 100
        # 对于禁止吸取，v1和v2是固定的1，不需要除以100
        elif buff_type_id == 10:
            v1, v2 = 1, 1


    mapped = buff_type_mapping_param.get(buff_type_id)

    buffs = []

    # 映射不存在
    if not mapped:
        return buffs

    # 情况 1：只是一种 buff（不是 list）
    if not isinstance(mapped, list):
        buffs.append({
            "name": name,
            "type": mapped,
            "value": v1,
            "coefficient": 1,
            "is_debuff": is_debuff,
            "duration": 99,
            "skill_type": 0
        })
        return buffs

    # 情况 2：多个 buff（例如双吸、双禁止）
    values = [v1, v2] # 已经处理过除以100的v1和v2

    for i, t in enumerate(mapped):
        # 避免 value = 0 的无效 buff
        if i < len(values) and values[i] > 0:
            buffs.append({
                "name": name,
                "type": t,
                "value": values[i],
                "coefficient": 1,
                "is_debuff": is_debuff,
                "duration": 99,
                "skill_type": 0
            })

    return buffs


def generate_effect_buff(data: dict):
    """
    根据瞳术和身法配置自动生成 buff 列表。
    :param data: 瞳术或身法数据字典。
    :return: Buff字典列表。
    """
    buff_type_map = {
        "1": BuffType.EVASION_UP,  # 闪避
        "2": BuffType.ACCURACY_UP  # 命中
    }

    low = float(data["buff"]) / 100  # 假设原始buff值是百分比，转为0-1小数
    high = float(data["buff2"]) / 100

    if low > high: 
        low, high = high, low 

    return [{
        "name": data["name"],
        "type": buff_type_map[data["buff_type"]],
        "value": random.uniform(low, high), # 随机一个0-1范围内的值
        "coefficient": 1,
        "is_debuff": False,
        "duration": 99,
        "skill_type": 0
    }]


def generate_main_buff(data, weapon_id):
    """
    生成主功法相关的buff。
    :param data: 主功法数据字典。
    :param weapon_id: 玩家当前装备的法器ID。
    :return: Buff字典列表。
    """
    buffs = []

    # 判断ew（专属武器ID）是否大于0且等于当前装备的武器ID
    if data.get("ew", 0) > 0 and data["ew"] == weapon_id:
        buffs.append({
            'name': data["name"],
            'type': BuffType.ATTACK_UP,
            'value': 0.5, # 攻击力提升50%
            'coefficient': 1,
            'is_debuff': False,
            'duration': 99,
            'skill_type': 0
        })

    # 判断random_buff是否为1，如果是则随机一个属性Buff
    if data.get("random_buff") == 1:
        configs = [
            (BuffType.ARMOR_PENETRATION_UP, (15, 40)), # 护甲穿透
            (BuffType.LIFESTEAL_UP, (2, 10)), # 生命偷取
            (BuffType.CRIT_RATE_UP, (5, 40)), # 暴击率
            (BuffType.DAMAGE_REDUCTION_UP, (5, 15)) # 伤害减免
        ]

        index = random.randint(0, 3) # 随机选择一个属性
        buff_type, (min_val, max_val) = configs[index]

        buffs.append({
            "name": "无上战意",
            "type": buff_type,
            "value": random.uniform(min_val, max_val) / 100, # 将百分比转换为0-1的小数
            "coefficient": 1,
            "is_debuff": False,
            "duration": 99,
            "skill_type": 0
        })

    return buffs


def update_all_user_status(status_list, bot_id, level_ratios=None):
    """
    遍历 status_list 更新所有玩家的hp/mp。
    排除 user_id=0 和 user_id=bot_id，hp/mp < 1 则替换为 1。
    同时更新本命法宝的次数统计。
    :param status_list: 战斗结束后的所有实体状态列表。
    :param bot_id: 机器人ID。
    :param level_ratios: 等级系数，用于反向计算真实HP/MP。
    """
    for item in status_list:
        for name, attr in item.items():
            user_id = attr.get("user_id", 0)

            # 排除无效用户ID和机器人
            if user_id == 0 or user_id == bot_id:
                continue

            ratio = 1
            if level_ratios:
                ratio = level_ratios.get(user_id, 1)

            # 获取HP/MP的乘数，用于反向还原到基础值
            hp_multiplier = attr.get("hp_multiplier", 1)
            mp_multiplier = attr.get("mp_multiplier", 1)
            # 确保除数不为0，如果为0则使用1
            safe_hp_multiplier = hp_multiplier if hp_multiplier != 0 else 1
            safe_mp_multiplier = mp_multiplier if mp_multiplier != 0 else 1
            safe_ratio = ratio if ratio != 0 else 1

            # 反向计算玩家实际HP/MP
            hp = attr.get("hp", 1) / safe_hp_multiplier / safe_ratio
            mp = attr.get("mp", 1) / safe_mp_multiplier / safe_ratio

            # hp/mp 最小为 1
            if hp < 1:
                hp = 1
            if mp < 1:
                mp = 1

            # 更新数据库
            sql_message.update_user_hp_mp(
                user_id,
                int(hp),
                int(mp)
            )
            # 更新本命法宝的次数统计
            natal_data = attr.get("natal_data", {})
            if natal_data and user_id:
                nt_instance = NatalTreasure(user_id)
                if nt_instance.exists(): # 确保法宝存在才更新
                    for field in ["fate_revive_count", "immortal_revive_count", "invincible_gain_count"]:
                        if field in natal_data:
                            nt_instance.update_data(field, natal_data[field])


# ---------- BOSS数据部分 ----------
def get_boss_attributes(boss, bot_id):
    """
    获取BOSS数据。
    :param boss: BOSS数据字典。
    :param bot_id: 机器人ID。
    :return: 包含BOSS属性和信息的字典。
    """
    buffs = {}

    attributes = {
        "user_id": bot_id,  # BOSS的user_id通常设置为bot_id
        "nickname": boss['name'],  # BOSS昵称
        "max_hp": boss['总血量'],  # 总血量
        "current_hp": boss['气血'],  # 当前气血
        "max_mp": boss['真元'],  # 总真元
        "current_mp": boss['真元'],  # 当前真元
        "attack": boss['攻击'],  # 攻击力
        "exp": 2,  # BOSS的经验值，可能用于特定计算
        "critical_rate": 0,  # 暴击率
        "critical_damage": 1.5,  # 暴击伤害倍数
        "boss_damage_bonus": 0,  # 对BOSS伤害加成
        "damage_reduction": 0,  # 伤害减免
        "armor_penetration": 0,  # 护甲穿透
        "accuracy": 100,  # 命中率
        "dodge": 0,  # 闪避率
        "speed": 0,  # 速度
        "start_skills": [],  # 初始技能
        'monster_type': boss.get("monster_type", "boss") # 怪物类型
    }

    buffs["属性"] = attributes
    buffs["其他"] = boss

    return buffs


def generate_boss_buff(boss):
    """
    初始化BOSS的特殊buff。
    :param boss: BOSS数据字典。
    :return: Buff字典列表。
    """
    # 初始化buff字典，所有属性默认0
    boss_buff = {
        'boss_zs': 0, # 真龙九变 (攻击力提升)
        'boss_hx': 0, # 无瑕七绝剑 (暴击率提升)
        'boss_bs': 0, # 太乙剑诀 (暴击伤害提升)
        'boss_xx': 0, # 七煞灭魂聚血杀阵 (生命偷取降低)
        'boss_jg': 0, # 子午安息香 (攻击力降低)
        'boss_jh': 0, # 玄冥剑气 (暴击率降低)
        'boss_jb': 0, # 大德琉璃金刚身 (暴击伤害降低)
        'boss_xl': 0, # 千煌锁灵阵 (真元偷取降低)
        'boss_cj': 0, # 钉头七箭书 (护甲穿透提升)
        'boss_js': 0, # 护身罡气 (伤害减免提升)
        'boss_sb': 0  # 虚无道则残片 (闪避率提升)
    }

    # BOSS Buff类型映射
    boss_buff_map = {
        'boss_zs': [BuffType.ATTACK_UP, "真龙九变"],
        'boss_hx': [BuffType.CRIT_RATE_UP, "无瑕七绝剑"],
        'boss_bs': [BuffType.CRIT_DAMAGE_UP, "太乙剑诀"],
        'boss_xx': [DebuffType.LIFESTEAL_DOWN, "七煞灭魂聚血杀阵"],
        'boss_jg': [DebuffType.ATTACK_DOWN, "子午安息香"],
        'boss_jh': [DebuffType.CRIT_RATE_DOWN, "玄冥剑气"],
        'boss_jb': [DebuffType.CRIT_DAMAGE_DOWN, "大德琉璃金刚身"],
        'boss_xl': [DebuffType.MANA_STEAL_DOWN, "千煌锁灵阵"],
        'boss_cj': [BuffType.ARMOR_PENETRATION_UP, "钉头七箭书"],
        'boss_js': [BuffType.DAMAGE_REDUCTION_UP, "护身罡气"],
        'boss_sb': [BuffType.EVASION_UP, "虚无道则残片"]
    }

    boss_level = boss["jj"] # BOSS境界

    # 1. 预计算当前BOSS的境界值，简化后续判断
    current_rank_val = convert_rank(boss_level + '中期')[0]

    def get_rank_val(name):
        """获取境界的数值表示"""
        return convert_rank(name)[0]

    # 2. 定义辅助函数：处理随机属性组 (每组4个选项，等概率随机一个)
    def apply_random_group(attr_names, value_options):
        """
        attr_names: 属性名列表 ['boss_zs', 'boss_hx', 'boss_bs', 'boss_xx']
        value_options: 对应的值列表，支持固定值(0-1比例)或函数(lambda，返回0-1比例)
        """
        # 随机选中一个属性名及其对应的索引
        selected_attr = random.choice(attr_names)
        idx = attr_names.index(selected_attr)

        # 获取值（如果是函数则调用它生成随机数，否则直接使用）
        val = value_options[idx]
        final_val = val() if callable(val) else val

        # 设置到 boss_buff 字典上
        boss_buff[selected_attr] = final_val

    # 3. 定义各境界的配置数据 (数据驱动)
    cfg = None

    # --- 境界判断逻辑 ---
    # 从最高境界开始向下判断
    # 祭道境 (最高级)
    if boss_level == "祭道境" or current_rank_val < get_rank_val('祭道境初期'): # 修正判断逻辑，祭道境及以上
        cfg = {
            'js': 0.95, # 伤害减免 95%
            'cj': (0.25, 0.50), # 护甲穿透 25%-50%
            # 对应: zs(攻击提升), hx(暴击率提升), bs(暴击伤害提升), xx(生命偷取降低)
            'g1': [1.0, 0.7, 2.0, 1.0], # 攻击提升100%, 暴击率70%, 暴击伤害200%, 生命偷取降低100%
            # 对应: jg(攻击力降低), jh(暴击率降低), jb(暴击伤害降低), xl(真元偷取降低)
            'g2': [0.7, 0.7, 1.5, 1.0] # 攻击力降低70%, 暴击率70%, 暴击伤害50%, 真元偷取降低100%
        }

    # 至尊 ~ 斩我 (中级)
    elif get_rank_val('至尊境初期') <= current_rank_val <= get_rank_val('斩我境圆满'):
        cfg = {
            'js': (0.50, 0.55), # 伤害减免 50%-55%
            'cj': (0.15, 0.30), # 护甲穿透 15%-30%
            'g1': [0.3, 0.1, 0.5, lambda: random.randint(5, 100) / 100], # 攻击提升30%, 暴击率10%, 暴击伤害50%, 生命偷取降低5%-100%
            'g2': [0.3, 0.3, 0.5, lambda: random.randint(5, 100) / 100] # 攻击降低30%, 暴击率30%, 暴击伤害50%, 真元偷取降低5%-100%
        }

    # 微光 ~ 遁一
    elif get_rank_val('微光境初期') <= current_rank_val <= get_rank_val('遁一境圆满'):
        cfg = {
            'js': (0.40, 0.45),
            'cj': (0.20, 0.40),
            'g1': [0.4, 0.2, 0.7, lambda: random.randint(10, 100) / 100],
            'g2': [0.4, 0.4, 0.7, lambda: random.randint(10, 100) / 100]
        }

    # 星芒 ~ 至尊
    elif get_rank_val('星芒境初期') <= current_rank_val <= get_rank_val('至尊境圆满'):
        cfg = {
            'js': (0.30, 0.35),
            'cj': (0.20, 0.40),
            'g1': [0.6, 0.35, 1.1, lambda: random.randint(30, 100) / 100],
            'g2': [0.5, 0.5, 0.9, lambda: random.randint(30, 100) / 100]
        }

    # 月华 ~ 微光
    elif get_rank_val('月华境初期') <= current_rank_val <= get_rank_val('微光境圆满'):
        cfg = {
            'js': (0.20, 0.25),
            'cj': (0.20, 0.40),
            'g1': [0.7, 0.45, 1.3, lambda: random.randint(40, 100) / 100],
            'g2': [0.55, 0.6, 1.0, lambda: random.randint(40, 100) / 100]
        }

    # 耀日 ~ 星芒
    elif get_rank_val('耀日境初期') <= current_rank_val <= get_rank_val('星芒境圆满'):
        cfg = {
            'js': (0.10, 0.15),
            'cj': (0.25, 0.45),
            'g1': [0.85, 0.5, 1.5, lambda: random.randint(50, 100) / 100],
            'g2': [0.6, 0.65, 1.1, lambda: random.randint(50, 100) / 100]
        }

    # 祭道 ~ 月华 (此处的“祭道”应是指比耀日境更低，但名字和上方重复，姑且保留，但逻辑上会被上面的祭道境判断优先匹配)
    # 按照境界从高到低匹配，如果这里写的“祭道”是指低境界，则需要调整
    # 假设这里是最低的境界范围，低于耀日境初期
    else: # 默认最低境界
        cfg = {
            'js': (0.05, 0.10), # 更低的减伤
            'cj': (0.20, 0.40), # 更低的穿透
            'g1': [0.9, 0.6, 1.7, lambda: random.randint(60, 100) / 100],
            'g2': [0.62, 0.67, 1.2, lambda: random.randint(60, 100) / 100]
        }

    # 4. 统一应用配置
    if cfg:
        # 应用减伤 (JS) - 支持固定值或随机范围
        if isinstance(cfg['js'], tuple):
            boss_buff['boss_js'] = random.uniform(*cfg['js']) # 直接取0-1范围内的浮点数
        else:
            boss_buff['boss_js'] = cfg['js'] # 直接使用0-1比例值

        # 应用护甲穿透 (CJ)
        # 这里钉头七箭书的护甲穿透是29.0%，那么原始值是0.29
        # 如果cfg['cj']是 (25, 50)，那么这里是 random.randint(25, 50) / 100，即0.25-0.5
        # 这样和日志的29.0%一致
        if isinstance(cfg['cj'], tuple):
            boss_buff['boss_cj'] = random.uniform(*cfg['cj']) # 直接取0-1范围内的浮点数
        else:
            boss_buff['boss_cj'] = cfg['cj'] # 直接使用0-1比例值


        # 应用两组随机属性
        apply_random_group(['boss_zs', 'boss_hx', 'boss_bs', 'boss_xx'], cfg['g1'])
        apply_random_group(['boss_jg', 'boss_jh', 'boss_jb', 'boss_xl'], cfg['g2'])

    else:
        # 如果没有匹配到任何境界配置，使用默认值
        boss_buff['boss_js'] = 0.0 # 默认无减伤
        boss_buff['boss_cj'] = 0.0 # 默认无穿透
        # 其他属性默认为0，已初始化

    # 计算BOSS闪避率
    # 这里的 boss_sb 应该是一个百分比值，而不是一个固定值
    # 如果 cfg['sb'] 是 (10, 20)，那么 boss_buff['boss_sb'] = random.randint(10, 20) / 100
    # 原始代码是 int((1 - boss_buff['boss_js']) * 100 * random.uniform(0.1, 0.5))
    # 假设闪避率也是一个0-1的浮点数
    boss_buff['boss_sb'] = random.uniform(0.1, 0.5) # 0.1-0.5 的随机浮点数，代表10%-50%闪避
    # boss_sb 是直接的0-1浮点数，用于内部计算，外部显示时转换为百分比

    result = []

    for key, value in boss_buff.items():
        if value == 0:
            continue  # 跳过无效果的Buff

        if key not in boss_buff_map:
            continue

        effect_type, effect_name = boss_buff_map[key]

        # 判断是否是DebuffType枚举
        is_debuff = isinstance(effect_type, DebuffType)

        result.append({
            "name": effect_name,
            "type": effect_type,
            "value": value, # 值已经是0-1的浮点数
            "is_debuff": is_debuff,
            "coefficient": 1, # BOSS Buff通常没有系数
            "duration": 99,   # BOSS Buff通常持续整场战斗
            "skill_type": 0   # BOSS Buff通常不是由技能触发
        })
    return result


def load_json_file(filename="data.json"):
    """
    加载JSON文件，用于BOSS神通。
    :param filename: JSON文件名。
    :return: JSON数据。
    """
    filepath = Path() / "data" / "xiuxian" / "功法" / filename

    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


skill_data_cache = None  # 全局缓存BOSS神通数据


def get_skill_data():
    """
    获取技能数据（带缓存）。
    :return: BOSS神通数据字典。
    """
    global skill_data_cache
    if skill_data_cache is None:
        skill_data_cache = load_json_file("boss神通.json")
    return skill_data_cache


def generate_boss_skill(enemy, skills):
    """
    为BOSS添加技能。
    :param enemy: BOSS实体。
    :param skills: 技能ID列表。
    """
    skill_data = get_skill_data()  # 第一次加载，后续使用缓存
    for skill_id in skills:
        skill_str = str(skill_id)
        if skill_str not in skill_data:
            continue
        enemy.skills.append(Skill(skill_data[skill_str]))


def update_data_boss_status(data, status_list):
    """
    更新BOSS的血量和真元数据。
    :param data: 原始BOSS数据字典。
    :param status_list: 战斗结束后的所有实体状态列表。
    :return: True如果找到并更新，False否则。
    """
    target_name = data["name"]

    for item in status_list:
        for name, attr in item.items():
            if name == target_name:
                # 将 status 中的 hp/mp 写回 data
                data["气血"] = attr.get("hp", data.get("气血"))
                data["真元"] = attr.get("mp", data.get("mp")) # 修复了原始代码中的一个潜在错误：Boss的真元应该更新，而不是总是写回初始真元
                return True
    return False


# ---------- 战斗部分 ----------
class SkillType(IntEnum):
    """技能类型枚举"""
    MULTI_HIT = 1  # 连续攻击
    DOT = 2  # 持续伤害 (毒/火)
    BUFF_STAT = 3  # 属性增益
    CONTROL = 4  # 封印/控制
    RANDOM_HIT = 5  # 波动伤害
    STACK_BUFF = 6  # 叠加Buff
    RANDOM_ACQUIRE = 7  # 随机获取技能

    # ====== BOSS特殊技能 ======
    MULTIPLIER_PERCENT_HP = 101  # 倍数伤害+目标百分比生命值伤害
    MULTIPLIER_DEF_IGNORE = 102  # 倍数伤害+无视防御
    CC = 103  # 控制类型（眩晕、沉默、定身等）
    SUMMON = 104  # 召唤类型技能
    FIELD = 105  # 领域类型


class TargetType(IntEnum):
    """目标类型枚举"""
    SINGLE = 1  # 单体
    AOE = 2  # 群体
    MULTI = 3  # 固定数量多目标


class BuffType(IntEnum):
    """增益效果类型枚举类"""
    # 基础属性增益
    ATTACK_UP = 1  # 攻击提升
    DEFENSE_UP = 2  # 防御提升 (注意：这里是防御力提升，与伤害减免不同)
    CRIT_RATE_UP = 3  # 暴击率提升
    CRIT_DAMAGE_UP = 4  # 暴击伤害提升
    DAMAGE_REDUCTION_UP = 5  # 伤害减免提升
    ARMOR_PENETRATION_UP = 6  # 护甲穿透提升
    ACCURACY_UP = 7  # 命中率提升
    EVASION_UP = 8  # 闪避率提升
    LIFESTEAL_UP = 9  # 生命偷取提升
    MANA_STEAL_UP = 10  # 法力偷取提升
    DEBUFF_IMMUNITY = 11  # 免疫减益
    HP_REGEN_PERCENT = 12  # 百分比回血
    MP_REGEN_PERCENT = 13  # 百分比回蓝
    REFLECT_DAMAGE = 14  # 伤害反弹
    SHIELD = 15  # 护盾
    INVINCIBLE = 16 # 无敌


class DebuffType(IntEnum):
    """减益效果类型枚举类"""
    # 属性降低类
    ATTACK_DOWN = 1  # 攻击力降低
    CRIT_RATE_DOWN = 2  # 暴击率降低
    CRIT_DAMAGE_DOWN = 3  # 暴击伤害降低
    DEFENSE_DOWN = 4  # 防御降低 (此防御降低会直接影响受到的伤害)
    ACCURACY_DOWN = 5  # 命中率降低
    EVASION_DOWN = 6  # 闪避率降低
    LIFESTEAL_DOWN = 7  # 生命偷取降低
    MANA_STEAL_DOWN = 8  # 真元偷取降低
    LIFESTEAL_BLOCK = 9  # 禁止生命吸取
    MANA_STEAL_BLOCK = 10  # 禁止法力吸取
    POISON_DOT = 11  # 中毒
    SKILL_DOT = 12  # 技能持续伤害
    BLEED_DOT = 13  # 流血
    BURN_DOT = 14  # 灼烧

    # 控制类
    FATIGUE = 15  # 疲劳
    STUN = 16  # 眩晕
    FREEZE = 17  # 冰冻
    PETRIFY = 18  # 石化
    SLEEP = 19  # 睡眠
    ROOT = 20  # 定身
    FEAR = 21  # 恐惧
    SEAL = 22  # 封印
    PARALYSIS = 23  # 麻痹
    SILENCE = 24  # 沉默


buff_type_mapping = {
    1: BuffType.ATTACK_UP,  # 攻击提升
    2: BuffType.CRIT_RATE_UP,  # 暴击率提升
    3: BuffType.CRIT_DAMAGE_UP,  # 暴击伤害提升
    4: BuffType.HP_REGEN_PERCENT,  # 气血回复 (百分比)
    5: BuffType.MP_REGEN_PERCENT,  # 真元回复 (百分比)
    6: BuffType.LIFESTEAL_UP,  # 吸气血
    7: BuffType.MANA_STEAL_UP,  # 吸真元
    8: DebuffType.POISON_DOT,  # 中毒
    9: [BuffType.LIFESTEAL_UP, BuffType.MANA_STEAL_UP],  # 双吸（同时提升两种偷取）
    10: [DebuffType.LIFESTEAL_BLOCK, DebuffType.MANA_STEAL_BLOCK],  # 禁止吸取（同时禁止两种偷取）
    11: BuffType.DEBUFF_IMMUNITY,  # 抵消 (免疫减益)
    12: "",  # 聚宝 (暂无实际战斗效果)
    13: BuffType.ARMOR_PENETRATION_UP,  # 斗战 (护甲穿透)
    14: BuffType.ARMOR_PENETRATION_UP  # 穿甲 (护甲穿透)
}

# 统一BUFF/DEBUFF显示模板，确保百分号在模板中
# Values are expected to be in 0-1 range for percentage-based buffs,
# or raw values for non-percentage buffs like crit damage multiplier, shield points, duration.
BUFF_DESC_TEMPLATES = {
    BuffType.ATTACK_UP: "攻击力提升 {value_display}",
    BuffType.DEFENSE_UP: "防御力提升 {value_display}",
    BuffType.CRIT_RATE_UP: "暴击率提升 {value_display}", 
    BuffType.CRIT_DAMAGE_UP: "暴击伤害提升 {value_display_raw} 倍", # 暴击伤害是倍数
    BuffType.DAMAGE_REDUCTION_UP: "伤害减免提升 {value_display}",
    BuffType.ARMOR_PENETRATION_UP: "护甲穿透提升 {value_display}",
    BuffType.ACCURACY_UP: "命中率提升 {value_display}",
    BuffType.EVASION_UP: "闪避率提升 {value_display}",
    BuffType.LIFESTEAL_UP: "生命偷取提升 {value_display}",
    BuffType.MANA_STEAL_UP: "真元偷取提升 {value_display}",
    BuffType.DEBUFF_IMMUNITY: "免疫所有减益效果",
    BuffType.HP_REGEN_PERCENT: "每回合回复最大生命 {value_display}", # 模板正确，使用 value_display
    BuffType.MP_REGEN_PERCENT: "每回合回复最大真元 {value_display}",
    BuffType.REFLECT_DAMAGE: "受到伤害时反弹 {value_display}",
    BuffType.SHIELD: "获得 {value_display_raw} 点护盾", # 护盾是点数
    BuffType.INVINCIBLE: "获得无敌效果", 
}

DEBUFF_DESC_TEMPLATES = {
    DebuffType.ATTACK_DOWN: "攻击力降低 {value_display}",
    DebuffType.CRIT_RATE_DOWN: "暴击率降低 {value_display}",
    DebuffType.CRIT_DAMAGE_DOWN: "暴击伤害降低 {value_display_raw} 倍",
    DebuffType.DEFENSE_DOWN: "防御力降低 {value_display}", 
    DebuffType.ACCURACY_DOWN: "命中率降低 {value_display}",
    DebuffType.EVASION_DOWN: "闪避率降低 {value_display}",
    DebuffType.LIFESTEAL_DOWN: "生命偷取降低 {value_display}",
    DebuffType.MANA_STEAL_DOWN: "真元偷取降低 {value_display}",
    DebuffType.LIFESTEAL_BLOCK: "无法进行生命偷取",
    DebuffType.MANA_STEAL_BLOCK: "无法进行真元偷取",

    DebuffType.POISON_DOT: "中毒，每回合损失当前生命 {value_display}", # 之前是最大生命，但实际代码是当前生命
    DebuffType.SKILL_DOT: "持续受到 {value_display_raw} 倍攻击的技能伤害", # 技能DoT是倍数
    DebuffType.BLEED_DOT: "流血，每回合损失最大生命 {value_display}",
    DebuffType.BURN_DOT: "灼烧，每回合损失最大生命 {value_display}",

    DebuffType.FATIGUE: "力竭，需休息 {value_display_raw} 回合", # 持续时间是回合数
    DebuffType.STUN: "眩晕，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.FREEZE: "冰冻，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.PETRIFY: "石化，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.SLEEP: "沉睡，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.ROOT: "被定身，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.FEAR: "陷入恐惧，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.SEAL: "被封印，无法使用技能，剩余 {value_display_raw} 回合",
    DebuffType.PARALYSIS: "麻痹，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.SILENCE: "被沉默，无法施放法术，剩余 {value_display_raw} 回合",
}

VALID_FIELDS = {"name", "type", "value", "coefficient", "is_debuff", "duration", "skill_type"} # 状态效果的有效字段


class StatusEffect:
    """
    表示战斗中的一个状态效果（Buff或Debuff）。
    """
    def __init__(self, name, effect_type, value, coefficient, is_debuff, duration=99, skill_type=0):
        self.name = name  # 技能名称或效果名称
        self.type = effect_type  # 效果类型 (BuffType或DebuffType枚举)
        self.value = value  # 效果数值 (例如，攻击力提升的百分比)
        self.coefficient = coefficient  # 效果系数 (用于特殊计算，例如谁施加的DoT)
        self.is_debuff = is_debuff  # 是否为负面效果（True为负面，False为正面）
        self.duration = duration  # 效果持续回合数
        self.skill_type = skill_type  # 技能类型 (例如，由哪种技能触发的Buff)

    def __repr__(self):  # 定义对象的字符串表示形式
        # 返回可读的状态效果信息
        return f"[{'Debuff' if self.is_debuff else 'Buff'}:{self.name}|{self.type}|{self.value}|{self.duration}|{self.skill_type}]"


class Skill:
    """
    表示战斗中的一个技能。
    """
    def __init__(self, data):
        self.name = data.get("name")  # 技能名称
        self.desc = data.get("desc", "")  # 技能介绍
        self.skill_type = int(data.get("skill_type", 1))  # 技能类型 (SkillType枚举)
        self.target_type = int(data.get("target_type", 1))  # 目标类型 (TargetType枚举)
        self.multi_count = int(data.get("multi_count", 1))  # 目标数量 (多目标技能使用)
        self.hp_condition = float(data.get("hp_condition", 1))  # 触发血量条件 (自身HP百分比低于此值时触发)

        # 消耗
        self.hp_cost_rate = float(data.get("hpcost", 0))  # 消耗气血百分比
        self.mp_cost_rate = float(data.get("mpcost", 0))  # 消耗真元百分比

        # 通用参数
        self.turn_cost = int(data.get("turncost", 0))  # 持续回合 或 休息回合
        self.rate = float(data.get("rate", 100))  # 触发率 (技能本身的概率，默认100%)
        self.cd = float(data.get("cd", 0))  # 冷却时间
        self.remain_cd = float(data.get("remain_cd", 0))  # 剩余冷却（回合）

        # 类型特定参数
        self.atk_values = data.get("atkvalue", [])  # 攻击参数 1 (可能是倍率列表，也可能是单一倍率)
        self.atk_coefficient = float(data.get("atkvalue2", 0))  # 攻击参数 2 (例如，百分比伤害的基数，随机伤害的上限)
        self.skill_buff_type = int(data.get("bufftype", 0))  # BUFF类型 (例如，属性提升的类型)
        self.skill_buff_value = float(data.get("buffvalue", 0))  # BUFF参数 (例如，提升的数值)
        self.success_rate = float(data.get("success", 0))  # 概率参数 (例如，控制技能的成功率)
        self.skill_content = data.get("skill_content", [])  # 随机神通参数 (例如，随机获取技能的ID列表)

    def is_available(self):
        """检查技能是否可用（冷却完成）"""
        return self.remain_cd <= 0

    def trigger_cd(self):
        """触发技能冷却"""
        self.remain_cd = self.cd

    def tick_cd(self):
        """冷却计数减少（每回合调用）"""
        if self.remain_cd > 0:
            self.remain_cd -= 1

    def __str__(self):
        """字符串表示"""
        return f"{self.name}(cd:{self.cd},rem:{self.remain_cd})"


# --- 实体类 (角色/怪物) ---
class Entity:
    """
    表示战斗中的一个实体（玩家或怪物）。
    包含其属性、状态、技能和本命法宝。
    """
    def __init__(self, data, team_id, is_boss=False):
        self.data = data # 原始数据
        self.id = data.get("user_id") # 实体ID
        self.name = data.get("nickname", "Unknown") # 昵称
        self.team_id = team_id # 队伍ID (0或1)
        self.is_boss = is_boss # 是否是BOSS
        self.type = data.get("monster_type", "player") # 实体类型 (player, boss, minion)

        # 基础属性
        self.max_hp = float(data.get("max_hp", 1)) # 最大生命值
        self.hp = float(data.get("current_hp", 1)) # 当前生命值
        self.max_mp = float(data.get("max_mp", 1)) # 最大真元值
        self.mp = float(data.get("current_mp", 1)) # 当前真元值
        self.mp_cost_modifier = float(data.get("mp_cost_modifier", 0)) # 真元消耗修正
        self.exp = float(data.get("exp", 1)) # 经验值 (此处可能作为计算基础值使用)
        self.boss_damage = float(data.get("boss_damage_bonus", 0)) # 对BOSS伤害加成

        # ── 新增：护盾属性 ──
        self.shield = 0.0 # 当前护盾值
        # ── 新增：无敌次数 ──
        self.invincible_count = 0 # 记录无敌次数（不计入buff列表，单独管理）

        # 进阶属性
        self.base_atk = float(data.get("attack", 1)) # 基础攻击力
        self.base_crit = float(data.get("critical_rate", 0)) # 基础暴击率
        self.base_crit_dmg = float(data.get("critical_damage", 1.5)) # 基础暴击伤害倍数
        self.base_damage_reduction = float(data.get("damage_reduction", 0)) # 基础伤害减免
        self.base_armor_pen = float(data.get("armor_penetration", 0)) # 基础护甲穿透
        self.base_accuracy = float(data.get("accuracy", 100)) # 基础命中率
        self.base_dodge = float(data.get("dodge", 0)) # 基础闪避率
        self.base_speed = float(data.get("speed", 10)) # 基础速度

        # 状态管理
        self.buffs = [] # Buff列表
        self.debuffs = [] # Debuff列表
        self.start_skills = data.get("start_skills", []) # 初始技能 (开局触发)
        self.skills = data.get("skills", []) # 可主动使用的技能
        self.total_dmg = 0 # 累计造成伤害

        # 本命法宝
        self.natal = None # NatalTreasure实例
        self.natal_data = None # 本命法宝数据
        # 如果是玩家且ID不为0，则初始化本命法宝
        if not is_boss and self.id and self.id != 0:
            self.natal = NatalTreasure(self.id)
            if self.natal.exists(): # 只有觉醒了本命法宝才加载数据
                self.natal_data = self.natal.get_data()
                # 从数据库加载已使用的本命法宝次数，确保在战斗中能正确统计
                self.fate_revive_count = self.natal_data.get("fate_revive_count", 0)
                self.immortal_revive_count = self.natal_data.get("immortal_revive_count", 0)
                self.invincible_count = self.natal_data.get("invincible_gain_count", 0) # 初始无敌次数
            # 如果本命法宝不存在，natal 和 natal_data 保持为 None


    # ======================
    #   本命法宝相关效果
    # ======================
    def apply_natal_periodic_effect(self, battle):
        """
        战斗中周期性触发本命法宝效果（每4回合，或首回合）。
        包括破甲、闪避、护盾、无敌的施加/刷新。
        :param battle: BattleSystem实例，用于发送战斗消息。
        """
        # 确保 natal_data 存在才进行后续操作
        if not self.natal_data:
            return

        name = self.natal_data.get("name", "本命法宝")
        
        enemies = battle._get_all_enemies(self) # 获取所有敌方单位

        # 1. 施加/刷新效果 (破甲, 闪避, 护盾, 无敌)
        for i in [1, 2]: # 遍历本命法宝的两个效果位
            etype_val = self.natal_data.get(f"effect{i}_type")
            if not etype_val or etype_val <= 0:
                continue

            etype = NatalEffectType(etype_val)
            effect_name = EFFECT_NAME_MAP.get(etype, "未知效果")
            
            # 以下效果不在这里周期性处理
            if etype in [NatalEffectType.BLEED, NatalEffectType.FATE, NatalEffectType.IMMORTAL, NatalEffectType.DEATH_STRIKE,
                         NatalEffectType.SHIELD_BREAK, NatalEffectType.REFLECT_DAMAGE, NatalEffectType.TRUE_DAMAGE,
                         NatalEffectType.CRIT_RESIST, NatalEffectType.TWIN_STRIKE]:
                continue


            # 破甲效果 (DebuffType.DEFENSE_DOWN)
            elif etype == NatalEffectType.ARMOR_BREAK:
                if not enemies: continue # 没有敌人则不施加
                value = self.natal.get_effect_value(NatalEffectType.ARMOR_BREAK)
                for enemy in enemies:
                    # 检查是否已存在同名Debuff，如果存在则刷新 duration
                    existing_debuff = enemy.get_debuff("name", f"{name}·{effect_name}")
                    if existing_debuff:
                        existing_debuff.duration = 99 # 刷新为新的持续时间
                    else:
                        effect = StatusEffect(
                            name=f"{name}·{effect_name}", effect_type=DebuffType.DEFENSE_DOWN,
                            value=value, coefficient=1, is_debuff=True, duration=99
                        )
                        enemy.add_status(effect)
                    battle.add_message(self, f"→ 对 {enemy.name} 施加了【{effect_name}】，降低其防御！")

            # 闪避效果 (BuffType.EVASION_UP)
            elif etype == NatalEffectType.EVASION:
                value = self.natal.get_effect_value(NatalEffectType.EVASION)
                existing_buff = self.get_buff("name", f"{name}·{effect_name}")
                if existing_buff:
                    existing_buff.duration = 99 # 刷新为新的持续时间
                else:
                    effect = StatusEffect(
                        name=f"{name}·{effect_name}", effect_type=BuffType.EVASION_UP,
                        value=value, coefficient=1, is_debuff=False, duration=99 # 闪避值是0-1的小数
                    )
                    self.add_status(effect)
                battle.add_message(self, f"→ 获得【{effect_name}】，提升了自身闪避！")
            
            # 护盾效果 (BuffType.SHIELD)
            elif etype == NatalEffectType.SHIELD:
                value = self.natal.get_effect_value(NatalEffectType.SHIELD)
                shield_value = int(self.max_hp * value)
                self.shield += shield_value
                battle.add_message(self, f"→ 重新凝聚【{effect_name}】，获得护盾 {number_to(int(shield_value))} 点 (当前总护盾: {number_to(int(self.shield))})")
            
            # 无敌效果 (BuffType.INVINCIBLE)
            elif etype == NatalEffectType.INVINCIBLE:
                # 获取当前战斗回合数，用于判断是否首次获得无敌
                is_first_gain_in_battle = (battle.round == 1)
                
                # 获取获得无敌的概率
                gain_chance = self.natal.get_effect_value(NatalEffectType.INVINCIBLE, self.natal_data.get("level", 0), is_first_gain_in_battle)
                
                if random.random() < gain_chance: # 概率判定
                    if self.invincible_count < INVINCIBLE_COUNT_LIMIT:
                        self.invincible_count += 1
                        battle.add_message(self, f"→ 凝聚【{effect_name}】，获得一次无敌效果！(当前拥有{self.invincible_count}次)")
                    else:
                        battle.add_message(self, f"→ 无法凝聚更多【{effect_name}】，无敌次数已达上限！(当前拥有{self.invincible_count}次)")

    def apply_natal_bleed_proc(self, battle):
        """
        处理本命法宝的流血效果（每回合概率触发）。
        :param battle: BattleSystem实例，用于发送战斗消息。
        """
        # 确保 natal_data 存在才进行后续操作
        if not self.natal_data:
            return

        for i in [1, 2]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if not etype_val or NatalEffectType(etype_val) != NatalEffectType.BLEED:
                continue

            # 25% 概率触发 (可调整)
            if random.random() > 0.25:
                continue
                
            enemies = battle._get_all_enemies(self)
            if not enemies:
                return
            
            target = random.choice(enemies) # 随机选择一个敌人
            name = self.natal_data.get("name", "本命法宝")
            effect_name = f"{name}·{EFFECT_NAME_MAP[NatalEffectType.BLEED]}"
            
            # 获取当前目标身上的流血层数
            bleed_debuffs = target.get_debuffs("name", effect_name)
            
            value = self.natal.get_effect_value(NatalEffectType.BLEED)
            
            # 层数上限为3 (可调整)
            if len(bleed_debuffs) < 3:
                # 直接添加新的一层
                effect = StatusEffect(
                    name=effect_name, effect_type=DebuffType.BLEED_DOT,
                    value=value, coefficient=1, is_debuff=True, duration=3 # 持续时间可调
                )
                target.add_status(effect)
                battle.add_message(self, f"→ {name} 对 {target.name} 施加了一层【流血】！(当前{len(bleed_debuffs) + 1}层)")
            else:
                # 刷新持续时间最短的一层
                bleed_debuffs.sort(key=lambda s: s.duration)
                bleed_debuffs[0].duration = 3 # 刷新为新的持续时间
                battle.add_message(self, f"→ {name} 刷新了 {target.name} 的一层【流血】效果！(仍为{len(bleed_debuffs)}层)")

    def has_buff(self, field: str, value):
        """
        检查实体是否拥有某个Buff。
        :param field: Buff对象的属性名 (例如 "type", "name")。
        :param value: 属性值。
        :return: True如果拥有，False否则。
        """
        if field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{field}'")
        return any(getattr(buff, field, None) == value for buff in self.buffs)

    def has_debuff(self, field: str, value):
        """
        检查实体是否拥有某个Debuff。
        :param field: Debuff对象的属性名 (例如 "type", "name")。
        :param value: 属性值。
        :return: True如果拥有，False否则。
        """
        if field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{field}'")
        return any(getattr(debuff, field, None) == value for debuff in self.debuffs)

    def add_status(self, effect):
        """
        向实体添加一个状态效果。
        :param effect: StatusEffect实例。
        """
        if effect.is_debuff:
            self.debuffs.append(effect)
        else:
            self.buffs.append(effect)

    def update_status_effects(self):
        """
        更新所有状态效果的持续时间，并移除过期的效果。
        同时处理技能的冷却时间。
        """
        for skill in self.skills[:]: # 遍历技能列表并减少冷却
            skill.tick_cd()

        for buff in self.buffs[:]: # 遍历Buff列表，更新持续时间并移除过期Buff
            buff.duration -= 1
            if buff.duration < 0:
                self.buffs.remove(buff)

        for debuff in self.debuffs[:]: # 遍历Debuff列表，更新持续时间并移除过期Debuff
            debuff.duration -= 1
            if debuff.duration < 0:
                self.debuffs.remove(debuff)

    # -------- buff管理函数 (更多通用方法) --------

    def get_buff_field(self, match_field: str, return_field: str, match_value):
        """
        在 buffs 中查找 match_field == match_value 的效果，
        找到后返回 return_field 的值。
        :param match_field: 用于匹配的Buff属性名。
        :param return_field: 需要返回的Buff属性名。
        :param match_value: 用于匹配的属性值。
        :return: 匹配Buff的指定属性值，如果未找到则返回 None。
        """
        if match_field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{match_field}'")
        if return_field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{return_field}'")

        for buff in self.buffs:
            if getattr(buff, match_field, None) == match_value:
                return getattr(buff, return_field, None)

        return None  # 找不到则返回 None

    def get_debuff_field(self, match_field: str, return_field: str, match_value):
        """
        在 debuffs 中查找 match_field == match_value 的效果，
        找到后返回 return_field 的值。
        :param match_field: 用于匹配的Debuff属性名。
        :param return_field: 需要返回的Debuff属性名。
        :param match_value: 用于匹配的属性值。
        :return: 匹配Debuff的指定属性值，如果未找到则返回 None。
        """
        if match_field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{match_field}'")
        if return_field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{return_field}'")

        for debuff in self.debuffs:
            if getattr(debuff, match_field, None) == match_value:
                return getattr(debuff, return_field, None)

        return None  # 找不到则返回 None

    def set_buff_field(self, match_field: str, target_field: str, match_value, new_value) -> bool:
        """
        在 buffs 中查找 match_field == match_value 的效果，
        并将 target_field 的值修改为 new_value。
        :param match_field: 用于匹配的Buff属性名。
        :param target_field: 需要修改的Buff属性名。
        :param match_value: 用于匹配的属性值。
        :param new_value: 新的属性值。
        :return: True 表示修改成功，False 表示未找到。
        """
        if match_field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{match_field}'")
        if target_field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{target_field}'")

        for buff in self.buffs:
            if getattr(buff, match_field, None) == match_value:
                setattr(buff, target_field, new_value)
                return True
        return False  # 没找到

    def set_debuff_field(self, match_field: str, target_field: str, match_value, new_value) -> bool:
        """
        在 debuffs 中查找 match_field == match_value 的效果，
        并将 target_field 的值修改为 new_value。
        :param match_field: 用于匹配的Debuff属性名。
        :param target_field: 需要修改的Debuff属性名。
        :param match_value: 用于匹配的属性值。
        :param new_value: 新的属性值。
        :return: True 表示修改成功，False 表示未找到。
        """
        if match_field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{match_field}'")
        if target_field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{target_field}'")

        for debuff in self.debuffs:
            if getattr(debuff, match_field, None) == match_value:
                setattr(debuff, target_field, new_value)
                return True
        return False  # 没找到

    def get_buffs(self, field: str, value):
        """
        根据任意字段获取所有匹配的 buff 列表。
        :param field: Buff对象的属性名。
        :param value: 属性值。
        :return: 匹配的Buff列表。
        """
        if field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{field}'. 有效字段: {VALID_FIELDS}")

        return [b for b in self.buffs if getattr(b, field, None) == value]

    def get_debuffs(self, field: str, value):
        """
        根据任意字段获取所有匹配的 debuff 列表。
        :param field: Debuff对象的属性名。
        :param value: 属性值。
        :return: 匹配的Debuff列表。
        """
        if field not in VALID_FIELDS:
            raise ValueError(f"不支持的字段 '{field}'. 有效字段: {VALID_FIELDS}")

        return [d for d in self.debuffs if getattr(d, field, None) == value]

    def get_buff(self, field: str, value):
        """
        返回第一个匹配的 buff，没有则返回 None。
        :param field: Buff对象的属性名。
        :param value: 属性值。
        :return: 匹配的Buff实例或 None。
        """
        buffs = self.get_buffs(field, value)
        return buffs[0] if buffs else None

    def get_debuff(self, field: str, value):
        """
        返回第一个匹配的 debuff，没有则返回 None。
        :param field: Debuff对象的属性名。
        :param value: 属性值。
        :return: 匹配的Debuff实例或 None。
        """
        debuffs = self.get_debuffs(field, value)
        return debuffs[0] if debuffs else None

    # -------- 数值类计算 --------

    def _get_effect_value(self, buff_type, debuff_type=None):
        """
        计算 (所有增益值 - 所有减益值)。
        :param buff_type: Buff类型。
        :param debuff_type: Debuff类型。
        :return: 最终效果值。
        """
        val = 0.0
        # 加 Buff
        for b in self.buffs:
            if b.type == buff_type: val += b.value
        # 减 Debuff
        if debuff_type:
            for d in self.debuffs:
                if d.type == debuff_type: val -= d.value
        return val

    def _get_effect_value_mixed(self, buff_type, debuff_type=None):
        """
        混合计算：增益加法叠加，减益乘法叠加。
        用于生命偷取/法力偷取等效果。
        :param buff_type: Buff类型。
        :param debuff_type: Debuff类型。
        :return: 最终效果值。
        """
        # 增益部分：加法叠加
        buff_sum = 0.0
        for b in self.buffs:
            if b.type == buff_type:
                buff_sum += b.value

        multiplier = 0 + buff_sum 

        # 减益部分：乘法叠加 (假设减益是减少multiplier)
        if debuff_type:
            for d in self.debuffs:
                if d.type == debuff_type:
                    multiplier *= (1 - d.value)  # 乘法叠加

        return multiplier

    def update_stat(self, stat: str, op: int, value: float, bypass_shield: bool = False):
        """
        更新HP或MP。
        :param stat: "hp" 或 "mp"。
        :param op: 1=加，2=减。
        :param value: 数值。
        :param bypass_shield: 是否绕过护盾直接造成伤害。
        """
        if stat not in ("hp", "mp"):
            raise ValueError("stat 必须是 'hp' 或 'mp'")

        # 对于非HP扣减，或者明确要绕过护盾的情况，直接更新属性
        # 如果是MP扣减，或者HP增加，或者明确绕过护盾的HP扣减，直接更新属性
        if stat == "mp" or op == 1 or bypass_shield:
            current = getattr(self, stat)
            max_value = getattr(self, f"max_{stat}")
            if op == 1:
                current += value
            elif op == 2:
                current -= value
            current = min(current, max_value) if op == 1 else current # 增加不能超过最大值
            setattr(self, stat, current)
            return
        
        # 以下是需要处理护盾的HP扣减逻辑
        # (此部分逻辑已移至 BattleSystem._apply_damage 中，这里保留一个简化的兜底，但正常流程不会走到)
        absorbed = 0
        if self.shield > 0:
            absorbed = min(value, self.shield)
            self.shield -= absorbed
            value -= absorbed
        
        if value > 0:
            self.hp -= value


    def pay_cost(self, hp_cost, mp_cost, deduct=False):
        """
        支付技能消耗。
        :param hp_cost: 气血消耗。
        :param mp_cost: 真元消耗。
        :param deduct: 是否实际扣除资源。
        :return: True如果可以支付，False否则。
        """
        # 修复：pay_cost的判断逻辑应该是 hp <= hp_cost 才返回False，而不是 hp < hp_cost
        # 如果 hp_cost 是0，那么 hp < 0 会返回 True，这不符合逻辑
        if self.hp < hp_cost or self.mp < mp_cost:
            return False
        if deduct:
            self.hp -= hp_cost
            self.mp -= mp_cost
        return True

    def show_bar(self, stat: str, length: int = 10):
        """
        显示一个血条或蓝条的字符串表示。
        :param stat: 'hp' 或 'mp'。
        :param length: 血条长度（单位：字符）。
        :return: 血条字符串。
        """
        if stat not in ("hp", "mp"):
            raise ValueError("stat 必须是 'hp' 或 'mp'")
        current_data = getattr(self, stat)
        current = max(0, current_data)
        max_value = getattr(self, f"max_{stat}")

        ratio = current / max_value if max_value > 0 else 0
        filled = int(ratio * length)
        empty = length - filled
        # 进度条
        bar = "▬" * filled + "▭" * empty
        
        return f"{self.name}剩余{number_to(int(current_data))}\n{stat.upper()} {bar} {int(ratio * 100)}%"


    @property
    def is_alive(self):
        """判断实体是否存活。"""
        return self.hp > 0

    @property
    def atk_rate(self):
        """计算最终攻击力。"""
        # 攻击力 = 基础 * (1 + 攻击提升Buff - 攻击降低Debuff)
        pct = self._get_effect_value(BuffType.ATTACK_UP, DebuffType.ATTACK_DOWN)
        return max(0, self.base_atk * (1 + pct))

    @property
    def crit_rate(self):
        """计算最终暴击率。"""
        # 暴击率 = 基础 + 暴击Buff - 暴击Debuff
        val = self.base_crit + self._get_effect_value(BuffType.CRIT_RATE_UP, DebuffType.CRIT_RATE_DOWN)
        # impart_know_per 已经包含在 get_players_attributes 的 crit 中，这里不再重复加
        return max(0, val)

    @property
    def crit_dmg_rate(self):
        """计算最终暴击伤害倍数。"""
        # 暴击伤害 = 基础 + 暴击伤害Buff - 暴击伤害Debuff
        val = self.base_crit_dmg + self._get_effect_value(BuffType.CRIT_DAMAGE_UP, DebuffType.CRIT_DAMAGE_DOWN)
        # impart_burst_per 已经包含在 get_players_attributes 的 critatk 中，这里不再重复加
        return max(0, val)

    @property
    def damage_reduction_rate(self):
        """计算最终伤害减免率。"""
        # 减伤率 = 基础 + 减伤Buff - 防御降低Debuff(来自本命法宝破甲)
        # 这里的防御降低Debuff直接影响的是Damage Reduction
        val = self.base_damage_reduction + self._get_effect_value(BuffType.DAMAGE_REDUCTION_UP, DebuffType.DEFENSE_DOWN)
        return min(0.95, max(-1, val)) # 允许负减伤（增伤），但有上限和下限

    @property
    def armor_pen_rate(self):
        """计算最终护甲穿透率。"""
        # 穿甲
        # 加上本命法宝的破甲效果 (NatalEffectType.ARMOR_BREAK)
        natal_armor_break = self.natal.get_effect_value(NatalEffectType.ARMOR_BREAK) if self.natal and self.natal_data else 0
        val = self.base_armor_pen + self._get_effect_value(BuffType.ARMOR_PENETRATION_UP) + natal_armor_break
        return max(0, val)

    @property
    def accuracy_rate(self):
        """计算最终命中率。"""
        # 命中率
        val = self.base_accuracy + self._get_effect_value(BuffType.ACCURACY_UP)
        return max(0, val)

    @property
    def dodge_rate(self):
        """计算最终闪避率。"""
        # 闪避
        # 加上本命法宝的闪避效果 (NatalEffectType.EVASION)
        natal_evasion = self.natal.get_effect_value(NatalEffectType.EVASION) if self.natal and self.natal_data else 0
        # 闪避率是0-1的小数，EffectValue也是0-1，所以直接相加即可
        val = self.base_dodge + self._get_effect_value(BuffType.EVASION_UP) + natal_evasion * 100 # base_dodge是百分比，effect_value是0-1的小数，所以这里需要乘以100
        return min(180, max(0, val)) # 闪避率有上限

    @property
    def lifesteal_rate(self):
        """计算最终生命偷取率。"""
        # 基础生命偷取假设为0，完全靠Buff
        if self.has_debuff("type", DebuffType.LIFESTEAL_BLOCK): # 如果被禁止生命吸取
            return 0
        val = self._get_effect_value_mixed(BuffType.LIFESTEAL_UP, DebuffType.LIFESTEAL_DOWN)
        return max(0, val)

    @property
    def mana_steal_rate(self):
        """计算最终法力偷取率。"""
        # 基础法力偷取假设为0，完全靠Buff
        if self.has_debuff("type", DebuffType.MANA_STEAL_BLOCK): # 如果被禁止法力吸取
            return 0
        val = self._get_effect_value_mixed(BuffType.MANA_STEAL_UP, DebuffType.MANA_STEAL_DOWN)
        return max(0, val)
    
    @property
    def shield_break_rate(self):
        """本命法宝的破盾效果 (攻击时无视部分护盾)。"""
        # 检查是否拥有SHIELD_BREAK效果
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.SHIELD_BREAK:
                return self.natal.get_effect_value(NatalEffectType.SHIELD_BREAK)
        return 0.0
    
    @property
    def shield_break_bonus_damage(self):
        """本命法宝的破盾效果 (攻击时额外伤害)。"""
        # 破盾效果的额外伤害加成是固定的，不随效果等级变化，只检查是否拥有此效果
        if self.natal and self.natal_data and any(self.natal_data.get(f"effect{i}_type") == NatalEffectType.SHIELD_BREAK.value for i in [1, 2]):
            return SHIELD_BREAK_BONUS_DAMAGE
        return 0.0

    @property
    def reflect_damage_rate(self):
        """本命法宝的反伤效果 (受到攻击时反弹)。"""
        return self.natal.get_effect_value(NatalEffectType.REFLECT_DAMAGE) if self.natal and self.natal_data else 0.0
    
    @property
    def true_damage_bonus(self):
        """本命法宝的真伤效果 (攻击时额外造成真实伤害)。"""
        return self.natal.get_effect_value(NatalEffectType.TRUE_DAMAGE) if self.natal and self.natal_data else 0.0
    
    @property
    def crit_resist_rate(self):
        """本命法宝的抗暴效果 (减少被暴击伤害)。"""
        return self.natal.get_effect_value(NatalEffectType.CRIT_RESIST) if self.natal and self.natal_data else 0.0

    @property
    def fate_revive_chance(self):
        """本命法宝的天命复活概率。"""
        return self.natal.get_effect_value(NatalEffectType.FATE) if self.natal and self.natal_data else 0.0
    
    @property
    def immortal_revive_hp_percent(self):
        """本命法宝的不灭复活血量百分比。"""
        return self.natal.get_effect_value(NatalEffectType.IMMORTAL) if self.natal and self.natal_data else 0.0

    @property
    def death_strike_threshold(self):
        """本命法宝的斩命触发血量阈值。"""
        return self.natal.get_effect_value(NatalEffectType.DEATH_STRIKE) if self.natal and self.natal_data else 0.0
        
    @property
    def has_death_strike(self):
        """判断是否拥有斩命效果。"""
        # 兼容 self.natal_data 为 None 的情况
        if not self.natal or not self.natal_data: return False
        for i in [1, 2]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.DEATH_STRIKE:
                return True
        return False
        
    @property
    def has_fate_effect(self):
        """判断是否拥有天命效果。"""
        # 兼容 self.natal_data 为 None 的情况
        if not self.natal or not self.natal_data: return False
        for i in [1, 2]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.FATE:
                return True
        return False

    @property
    def twin_strike_effect(self) -> tuple[float, float] | None:
        """本命法宝的双生效果 (触发概率, 伤害倍率)。"""
        # 兼容 self.natal_data 为 None 的情况
        if not self.natal or not self.natal_data:
            return None
        for i in [1, 2]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.TWIN_STRIKE:
                return self.natal.get_effect_value(NatalEffectType.TWIN_STRIKE)
        return None

    @property
    def bleed_dot_dmg_list(self):
        """返回所有流血伤害的列表 (每层流血造成的伤害)。"""
        damages = []
        for debuff in self.debuffs:
            if debuff.type == DebuffType.BLEED_DOT:
                # 流血基础伤害 = 最大生命值 / 2 * 流血值 (流血值是百分比)
                base_bleed_dmg = (self.max_hp / 2) * debuff.value
                # 计算受减伤影响后的伤害
                final_bleed_dmg = base_bleed_dmg * (1 - self.damage_reduction_rate)
                damages.append(int(final_bleed_dmg))
        return damages

    @property
    def poison_dot_dmg(self):
        """所有中毒伤害的总和（基于当前生命值）。"""
        total = 0.0
        for debuff in self.debuffs:
            if debuff.type == DebuffType.POISON_DOT:
                # 假设debuff.value是百分比（如0.05表示5%）
                total += self.hp * debuff.value
        return int(total)

    @property
    def hp_regen_rate(self):
        """所有HP恢复的总和（基于最大生命值）。"""
        total = 0.0
        for buff in self.buffs:
            if buff.type == BuffType.HP_REGEN_PERCENT:
                # 假设buff.value是百分比（如0.05表示5%）
                total += self.max_hp * buff.value
        return int(total)

    @property
    def mp_regen_rate(self):
        """所有MP恢复的总和（基于最大真元值）。"""
        total = 0.0
        for buff in self.buffs:
            if buff.type == BuffType.MP_REGEN_PERCENT:
                # 假设buff.value是百分比（如0.05表示5%）
                total += self.max_mp * buff.value
        return int(total)

    # --- 状态管理 ---
    def remove_skill_by_name(self, skill_name):
        """
        删除指定名称的技能。
        :param skill_name: 技能名称。
        :return: True如果成功删除，False否则。
        """
        for i, skill in enumerate(self.skills):
            if skill.name == skill_name:
                del self.skills[i]
                return True
        return False

    def has_skill(self, skill_name):
        """
        检查是否拥有某个技能。
        :param skill_name: 技能名称。
        :return: True如果拥有，False否则。
        """
        return any(skill.name == skill_name for skill in self.skills)

    def check_and_clear_debuffs_by_immunity(self):
        """
        检查是否有debuff免疫效果，如果有则清空所有debuffs。
        """
        if self.has_buff("type", BuffType.DEBUFF_IMMUNITY):
            self.debuffs.clear()


# --- 战斗引擎 (核心逻辑整合) ---
class BattleSystem:
    """
    战斗系统核心类，管理战斗回合、单位行动、状态更新、伤害计算和胜负判定。
    """
    def __init__(self, team_a, team_b, bot_id):
        self.bot_id = bot_id # 机器人ID
        self.team_a = team_a # 队伍A的实体列表
        self.team_b = team_b # 队伍B的实体列表
        self.play_list = [] # 战斗日志列表
        self.round = 0 # 当前回合数
        self.max_rounds = 50 # 最大回合数

    def add_message(self, unit, message):
        """
        添加战斗消息到日志。
        :param unit: 发送消息的实体。
        :param message: 消息内容。
        """
        # 确保消息内容不为空
        if not message.strip():
            return

        msg_dict = {
            "type": "node",
            "data": {
                "name": unit.name, # 发送消息的实体名称
                "uin": int(unit.id),
                "content": message
            }
        }
        self.play_list.append(msg_dict)
        
    def add_shield_log(self, defender, absorbed_damage):
        """
        为护盾吸收伤害单独添加日志条目。
        :param defender: 防御者实体。
        :param absorbed_damage: 被护盾吸收的伤害量。
        """
        # 仅在护盾实际吸收伤害时才添加日志
        if absorbed_damage > 0:
            msg_dict = {
                "type": "node",
                "data": {
                    "name": defender.name,
                    "uin": int(defender.id),
                    "content": f"🛡️ {defender.name}的护盾抵挡了 {number_to(int(absorbed_damage))} 点伤害！(剩余护盾: {number_to(int(defender.shield))})"
                }
            }
            self.play_list.append(msg_dict)

    def add_unit_status_message(self, unit):
        """
        添加单位的HP/MP状态栏消息。
        只在HP或MP发生变化后调用，避免重复显示。
        :param unit: 实体。
        """
        hp_bar = unit.show_bar("hp")

        shield_info = f"护盾:{number_to(int(unit.shield))}" if unit.shield > 0 else ""
        invincible_info = f"无敌:{unit.invincible_count}" if unit.invincible_count > 0 else ""
        
        extra_info_parts = [info for info in [shield_info, invincible_info] if info]
        extra_info = f" | {' | '.join(extra_info_parts)}" if extra_info_parts else ""
        
        status_msg = f"{hp_bar}{extra_info}"
        
        # 确保消息不为空才添加
        if status_msg.strip(): # 再次检查status_msg是否为空或只包含空格
            # 如果上一个消息就是这个单位的血条，则不重复添加
            if self.play_list and self.play_list[-1]["data"]["uin"] == int(unit.id) and \
               self.play_list[-1]["data"]["content"].startswith(f"{unit.name}剩余"):
                # 如果是相同的血条消息，直接更新，而不是追加
                self.play_list[-1]["data"]["content"] = status_msg
            else:
                self.play_list.append({
                    "type": "node",
                    "data": {
                        "name": unit.name,
                        "uin": int(unit.id),
                        "content": status_msg
                    }
                })

    def add_system_message(self, message):
        """
        添加系统消息到日志。
        :param message: 系统消息内容。
        """
        msg_dict = {
            "type": "node",
            "data": {
                "name": "系统", # 系统消息的发送者
                "uin": int(self.bot_id),
                "content": message
            }
        }
        self.play_list.append(msg_dict)

    def get_effect_desc(self, effect_type, is_debuff=False, value=None):
        """
        获取状态效果的描述字符串。
        :param effect_type: 效果类型 (BuffType或DebuffType枚举)。
        :param is_debuff: 是否为负面效果。
        :param value: 效果值 (预期为0-1的浮点数，或整数)。
        :return: 格式化的效果描述字符串。
        """
        if value is None:
            return "未知效果"
    
        try:
            val = float(value)
        except (TypeError, ValueError):
            val = 0.0
        
        # Determine the template based on effect type
        template = ""
        if is_debuff:
            template = DEBUFF_DESC_TEMPLATES.get(effect_type, "未知减益")
        else:
            template = BUFF_DESC_TEMPLATES.get(effect_type, "未知增益")
        
        # Format raw value for display in some templates
        value_display_raw = f"{val:.2f}" if val != int(val) else f"{int(val)}"

        # Default percentage formatting
        value_display = ""
        # Check if template explicitly uses value_display_raw for certain types
        # and has a specific format like "倍" or "回合"
        if effect_type in {BuffType.CRIT_DAMAGE_UP, DebuffType.CRIT_DAMAGE_DOWN, # 暴击伤害是倍数
                           DebuffType.SKILL_DOT, # 技能持续伤害是倍数
                           BuffType.SHIELD, # 护盾是点数
                           DebuffType.FATIGUE, DebuffType.STUN, DebuffType.FREEZE, DebuffType.PETRIFY,
                           DebuffType.SLEEP, DebuffType.ROOT, DebuffType.FEAR, DebuffType.SEAL,
                           DebuffType.PARALYSIS, DebuffType.SILENCE # 控制效果持续时间是回合数
                          }:
            value_display = value_display_raw
        else: # Most other buffs are percentages (0-1 -> X%)
            # Convert to percentage and format
            display_val = val * 100
            if display_val == int(display_val):
                value_display = f"{int(display_val)}%"
            else:
                value_display = f"{display_val:.1f}%"
        
        # Fill the template, passing both raw and percentage-formatted values
        # This allows templates to choose which format to use
        return template.format(value_display=value_display, value_display_raw=value_display_raw)

    def add_after_last_damage(self, msg, add_text):
        """
        在最后一个"伤害！"后面添加指定字符串。
        :param msg: 原始消息。
        :param add_text: 需要添加的文本。
        :return: 修改后的消息。
        """
        # 使用partition从右边分割，避免索引错误
        before_last, separator, after_last = msg.rpartition("伤害！")

        if separator:  # 找到了"伤害！"
            return before_last + "伤害！" + add_text + after_last
        else:  # 没有找到"伤害！"
            return msg

    def _calc_raw_damage(self, attacker, defender, multiplier, penetration=False):
        """
        基础伤害计算公式。
        :param attacker: 攻击者实体。
        :param defender: 防御者实体。
        :param multiplier: 技能伤害倍数。
        :param penetration: 是否无视防御。
        :return: 原始伤害值、真实伤害值、是否暴击、命中状态。
        """
        # 命中判定
        status = "Hit"
        # base_accuracy是100，dodge_rate是0-1的小数（代表百分比，但未经转换），所以这里需要统一转换为百分比进行比较
        if random.uniform(0, 100) > (attacker.accuracy_rate - defender.dodge_rate):
            status = "Miss"

        # 暴击判定
        is_crit = random.random() < attacker.crit_rate
        
        # 计算暴击伤害倍数
        crit_mult = attacker.crit_dmg_rate if is_crit else 1.0
        
        # 应用防御方的抗暴效果 (NatalEffectType.CRIT_RESIST)
        if is_crit and defender.natal_data and defender.crit_resist_rate > 0:
            crit_resist = defender.crit_resist_rate
            crit_mult *= (1 - crit_resist) # 减少暴击伤害的乘数
            if crit_resist > 0:
                self.add_message(defender, f"『{defender.natal_data.get('name','本命法宝')}』触发抗暴，减少了暴击伤害！")

        # defender.damage_reduction_rate 可能为负数（易伤）
        if penetration:
            dr_eff = 0  # 完全无视减伤
        else:
            dr_eff = max(0, defender.damage_reduction_rate - attacker.armor_pen_rate)  # 减伤率减去穿透，且不低于0
        
        # 伤害公式: 攻击 * 倍率 * 暴击 * (1 - 有效减伤率)
        damage = attacker.atk_rate * multiplier * crit_mult * (1 - dr_eff)

        # BOSS加成
        if defender.is_boss:
            damage *= (1 + attacker.boss_damage)
            
        # 本命法宝真伤 (NatalEffectType.TRUE_DAMAGE)
        # 真伤是额外造成，且无视护盾和减伤，所以在 raw_damage 之外单独计算
        true_damage = 0
        if attacker.natal_data and attacker.true_damage_bonus > 0:
            true_damage_rate = attacker.true_damage_bonus
            if true_damage_rate > 0:
                true_damage = attacker.atk_rate * true_damage_rate * multiplier
                self.add_message(attacker, f"『{attacker.natal_data.get('name','本命法宝')}』触发真伤，额外造成 {number_to(int(true_damage))} 真实伤害！")

        # 本命法宝破盾额外伤害 (NatalEffectType.SHIELD_BREAK)
        # 当目标有护盾时，造成额外伤害
        if attacker.natal_data and attacker.shield_break_bonus_damage > 0 and defender.shield > 0:
            bonus_damage_amount = attacker.atk_rate * attacker.shield_break_bonus_damage * multiplier
            damage += bonus_damage_amount # 额外伤害直接加到普通伤害中
            self.add_message(attacker, f"『{attacker.natal_data.get('name','本命法宝')}』触发破盾，对有护盾的{defender.name}额外造成 {number_to(int(bonus_damage_amount))} 点伤害！")

        # 伤害浮动 - 添加0.95到1.05的随机浮动，使伤害结果更自然
        damage *= random.uniform(0.95, 1.05)

        return int(damage), int(true_damage), is_crit, status
        
    def _apply_damage(self, caster, defender, raw_damage_value, true_damage_value=0):
        """
        处理伤害和护盾吸收，返回实际HP伤害和被吸收的伤害。
        :param caster: 攻击者实体。
        :param defender: 防御者实体。
        :param raw_damage_value: 原始（未被护盾吸收）的伤害值。
        :param true_damage_value: 额外真实伤害值。
        :return: 实际对HP造成的伤害，被护盾吸收的伤害，反弹的伤害。
        """
        if not isinstance(raw_damage_value, (int, float)) or raw_damage_value < 0:
            raw_damage_value = 0
        if not isinstance(true_damage_value, (int, float)) or true_damage_value < 0:
            true_damage_value = 0

        # --- 无敌效果判定 (NatalEffectType.INVINCIBLE) ---
        # 如果防御方拥有无敌次数，则本次伤害归零，并消耗一次无敌
        if defender.invincible_count > 0:
            self.add_message(defender, f"✨『{defender.natal_data.get('name','本命法宝')}』触发【无敌】，本次伤害完全免疫！(剩余无敌次数: {defender.invincible_count - 1})")
            defender.invincible_count -= 1
            # 将无敌次数同步到natal_data，以便战斗结束后更新数据库
            if defender.natal_data: defender.natal_data["invincible_gain_count"] = defender.invincible_count
            return 0, 0, 0 # 实际HP伤害, 吸收伤害, 反弹伤害

        # 1. 优先处理护盾抵挡和破盾效果
        absorbed_by_shield = 0
        damage_to_be_absorbed = raw_damage_value # 需要被护盾抵挡的伤害
        
        if defender.shield > 0:
            if caster.natal_data and caster.shield_break_rate > 0:
                # 计算无视的护盾量，这部分伤害直接穿透护盾
                ignored_shield_amount = damage_to_be_absorbed * caster.shield_break_rate
                
                self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』触发破盾，无视了 {number_to(int(ignored_shield_amount))} 点护盾！")
                
                # 剩余伤害，这部分尝试被护盾抵挡
                damage_to_be_absorbed -= ignored_shield_amount
                
            # 护盾吸收剩余的伤害
            absorbed_by_shield = min(damage_to_be_absorbed, defender.shield)
            defender.shield -= absorbed_by_shield
            raw_damage_value -= absorbed_by_shield # 从原始伤害中扣除被护盾吸收的部分
            
        # 确保护盾不为负
        defender.shield = max(0, defender.shield)
        
        # 2. 实际对HP造成的伤害 (普通伤害 + 真实伤害)
        final_hp_damage = int(raw_damage_value + true_damage_value)
        
        # 3. 反伤效果 (defender.reflect_damage_rate)
        reflected_damage = 0
        if defender.natal_data and defender.reflect_damage_rate > 0:
            # 反伤是基于实际造成的HP伤害 (即final_hp_damage)
            reflected_damage = int(final_hp_damage * defender.reflect_damage_rate)
            if reflected_damage > 0:
                # 反伤是真实伤害，无视护盾和减伤
                # 这里不直接扣除攻击者血量，而是返回，让BattleSystem处理
                self.add_message(defender, f"『{defender.natal_data.get('name','本命法宝')}』触发反伤，反弹 {number_to(reflected_damage)} 真实伤害！")

        # 4. 更新目标HP
        if final_hp_damage > 0:
            defender.update_stat("hp", 2, final_hp_damage, bypass_shield=True) # bypass_shield=True因为护盾已在此处计算
            
        # 5. 记录护盾吸收日志
        self.add_shield_log(defender, absorbed_by_shield) # 即使 absorbed_by_shield 为 0， add_shield_log 内部也会检查
            
        return final_hp_damage, int(absorbed_by_shield), reflected_damage

    def _check_and_apply_revive_effects(self, defender, attacker_has_death_strike):
        """
        检查并应用复活类效果 (天命, 不灭)。
        :param defender: 防御者实体。
        :param attacker_has_death_strike: 攻击者是否拥有斩命效果。
        :return: True 如果目标复活, False 否则。
        """
        if defender.is_alive: # 活着不需要复活
            return False

        # --- 斩命效果判定 (NatalEffectType.DEATH_STRIKE) ---
        # 如果攻击方拥有斩命效果，并且目标拥有天命效果，则天命效果被禁止
        if attacker_has_death_strike and defender.has_fate_effect:
            self.add_message(defender, f"💀你的『{defender.natal_data.get('name','本命法宝')}』【天命】效果被【斩命】禁止！")
            return False # 天命被禁止，无法复活

        # --- 天命效果判定 (NatalEffectType.FATE) ---
        if defender.natal_data and defender.fate_revive_chance > 0 and defender.fate_revive_count < FATE_REVIVE_COUNT_LIMIT:
            if random.random() < defender.fate_revive_chance:
                defender.hp = defender.max_hp # 恢复满血
                defender.fate_revive_count += 1
                # 将使用次数同步到natal_data
                if defender.natal_data: defender.natal_data["fate_revive_count"] = defender.fate_revive_count
                self.add_message(defender, f"✨『{defender.natal_data.get('name','本命法宝')}』触发【天命】，恢复全部生命！(已使用{defender.fate_revive_count}/{FATE_REVIVE_COUNT_LIMIT}次)")
                # self.add_unit_status_message(defender) # 复活后更新血条 -- 统一在回合末更新
                return True

        # --- 不灭效果判定 (NatalEffectType.IMMORTAL) ---
        if defender.natal_data and defender.immortal_revive_hp_percent > 0 and defender.immortal_revive_count < IMMORTAL_REVIVE_COUNT_LIMIT:
            # 不灭有固定50%概率触发，随效果等级提升恢复血量百分比
            if random.randint(1, 100) <= 50: # 固定50%触发概率
                revive_amount = defender.max_hp * defender.immortal_revive_hp_percent
                defender.hp = revive_amount
                defender.immortal_revive_count += 1
                # 将使用次数同步到natal_data
                if defender.natal_data: defender.natal_data["immortal_revive_count"] = defender.immortal_revive_count
                self.add_message(defender, f"✨『{defender.natal_data.get('name','本命法宝')}』触发【不灭】，恢复 {number_to(int(revive_amount))} 生命！(已使用{defender.immortal_revive_count}/{IMMORTAL_REVIVE_COUNT_LIMIT}次)")
                # self.add_unit_status_message(defender) # 复活后更新血条 -- 统一在回合末更新
                return True

        return False

    def _check_and_apply_death_strike(self, attacker, defender, final_hp_damage):
        """
        检查并应用斩命效果 (NatalEffectType.DEATH_STRIKE)。
        :param attacker: 攻击者实体。
        :param defender: 防御者实体。
        :param final_hp_damage: 本次攻击造成的最终HP伤害。
        :return: True 如果目标被斩杀, False 否则。
        """
        if attacker.natal_data and attacker.has_death_strike:
            threshold = attacker.death_strike_threshold
            # 如果目标因本次伤害死亡，或者血量低于阈值
            if not defender.is_alive or (defender.hp / defender.max_hp) <= threshold:
                # 直接斩杀
                remaining_hp_to_kill = defender.hp # 斩杀时实际扣除的血量
                if remaining_hp_to_kill > 0:
                    defender.update_stat("hp", 2, remaining_hp_to_kill, bypass_shield=True) # 直接扣除剩余全部生命值
                    self.add_message(attacker, f"💀『{attacker.natal_data.get('name','本命法宝')}』触发【斩命】，对{defender.name}造成【{number_to(int(remaining_hp_to_kill))}】点额外伤害并直接斩杀！")
                    # self.add_unit_status_message(defender) # 斩杀后更新血条 -- 统一在回合末更新
                    return True
                elif not defender.is_alive: # 本次攻击就打死了，斩命只是确认死亡
                     self.add_message(attacker, f"💀『{attacker.natal_data.get('name','本命法宝')}』触发【斩命】，将{defender.name}直接斩杀！")
                     return True
        return False

    def _get_all_enemies(self, entity):
        """
        获取指定实体的所有敌方存活单位。
        :param entity: 实体。
        :return: 敌方存活单位列表。
        """
        if entity.team_id == 0:
            # 如果实体在队伍0，返回队伍1的所有存活单位
            return [e for e in self.team_b if e.is_alive]
        else:
            # 如果实体在队伍1，返回队伍0的所有存活单位
            return [e for e in self.team_a if e.is_alive]

    def _get_all_allies(self, entity):
        """
        获取指定实体的所有友方存活单位（不包括自己）。
        :param entity: 实体。
        :return: 友方存活单位列表。
        """
        if entity.team_id == 0:
            # 队伍0的所有存活单位，排除自己
            return [e for e in self.team_a if e.is_alive and e.id != entity.id]
        else:
            # 队伍1的所有存活单位，排除自己
            return [e for e in self.team_b if e.is_alive and e.id != entity.id]

    def _apply_round_one_skills(self, caster, targets, skills_dict):
        """
        处理开局技能字典。
        :param caster: 施法者实体。
        :param targets: 目标列表（单个或多个）。
        :param skills_dict: 技能字典 {{'type':..., 'value':..., 'is_debuff':...}}。
        """
        if not skills_dict:
            return

        for data in skills_dict:
            name = data['name']
            b_type = data['type']
            val = float(data['value'])
            is_db = data['is_debuff']

            # 小兵不施加Debuff
            if is_db and caster.type == "minion":
                continue

            # 创建效果对象
            effect = StatusEffect(name, b_type, val, 1, is_db, duration=99, skill_type=0)

            if is_db: # Debuff施加给敌人
                for target in targets:
                    target.add_status(effect)
            else: # Buff施加给自己
                caster.add_status(effect)

            # Pass the raw numeric value 'val' to get_effect_desc for formatting
            buff_msg = self.get_effect_desc(b_type, is_db, val)
            msg = f"{caster.name}使用{name}，{buff_msg}"
            if caster.type != "minion": # 小兵不发送消息
                self.add_message(caster, msg)
            
    def choose_skill(self, caster, skills, enemies):
        """
        选择一个可用的技能。
        :param caster: 施法者实体。
        :param skills: 施法者拥有的技能列表。
        :param enemies: 敌方存活单位列表。
        :return: 选定的Skill实例或None。
        """
        usable_skills = []
        # ---------- 先过滤不可用技能 ----------
        for sk in skills:
            if sk.skill_type == SkillType.RANDOM_ACQUIRE:  # Type 7: 随机获取技能
                # 随机选择一个技能ID并创建技能实例
                skill_id = random.choice(sk.skill_content)
                skill_data = items.get_data_by_item_id(skill_id)
                sk_data = Skill(skill_data)
                caster.skills.append(sk_data)  # 添加随机的技能
                caster.remove_skill_by_name(sk.name)  # 删除当前技能 (因为它已经被随机替换)
                skill_data_name = skill_data["name"]
                self.add_message(caster, f"{sk.desc} 随机获得了{skill_data_name}神通!")
                if self._skill_available(caster, sk_data, enemies): # 检查新获得的技能是否可用
                    usable_skills.append(sk_data)
            elif self._skill_available(caster, sk, enemies): # 检查普通技能是否可用
                usable_skills.append(sk)
        if not usable_skills:
            return None  # 没技能可用，将进行普攻或跳过

        # ---------- 触发血量类型技能优先 ----------
        not_hp1_skills = [sk for sk in usable_skills if sk.hp_condition != 1]
        if not_hp1_skills:
            return not_hp1_skills[0]  # 优先返回hp_condition不等于1的技能

        # ---------- BUFF 技能优先 ----------
        buff_list = [sk for sk in usable_skills if sk.skill_type == SkillType.BUFF_STAT]
        if buff_list:
            return buff_list[0]  # 或按权重选择第一个Buff技能

        # ---------- 随机技能 ----------
        return random.choice(usable_skills)

    def _skill_available(self, caster, skill, enemies):
        """
        判断技能是否可以被使用。
        :param caster: 施法者实体。
        :param skill: 技能实例。
        :param enemies: 敌方存活单位列表。
        :return: True如果技能可用，False否则。
        """
        # ---------- 1. 冷却 ----------
        if not skill.is_available():
            return False

        # ---------- 2. hp触发条件 ----------
        hp_percentage = caster.hp / caster.max_hp
        if hp_percentage > skill.hp_condition:
            return False

        # ---------- 3. 资源消耗检查 ----------
        hp_cost = caster.hp * skill.hp_cost_rate
        mp_cost = caster.exp * skill.mp_cost_rate * (1 - caster.mp_cost_modifier)
        if not caster.pay_cost(hp_cost, mp_cost, deduct=False): # 只检查是否能支付，不实际扣除
            return False

        # ---------- 4. 技能：检查是否所有敌人都已经有这个debuff ----------
        if skill.skill_type in (SkillType.DOT, SkillType.CC, SkillType.CONTROL):
            enemies_without_debuff = [e for e in enemies if not e.has_debuff("name", skill.name)]
            if not enemies_without_debuff: # If all enemies already have this Debuff, do not cast
                return False

        # ---------- 5. BUFF 技能：不能重复施放相同 Buff ----------
        if skill.skill_type == SkillType.BUFF_STAT or skill.skill_type == SkillType.STACK_BUFF:
            # For STACK_BUFF, check if there's already a buff with the same name and skill_type,
            # indicating it's already active and stacking (or just needs to be refreshed, not recast).
            # The current logic assumes a simple buff without complex stacking management here.
            # A more robust stacking system might check for 'stacks' property of the buff.
            if caster.has_buff("name", skill.name): 
                return False

        return True


    def _select_targets(self, enemies, skill, is_boss=False):
        """
        根据技能目标类型选择目标。
        :param enemies: 敌方存活单位列表。
        :param skill: 技能实例。
        :param is_boss: 是否是BOSS在选择目标。
        :return: 目标实体列表。
        """
        alive = [e for e in enemies if e.is_alive]
        if not alive: return [] # 如果没有存活的敌人，返回空列表

        if skill.target_type == TargetType.SINGLE:
            if skill.skill_type == SkillType.DOT: # 如果是DoT技能，优先选择没有该Debuff的敌人
                alive_without_dot = [a for a in alive if not a.has_debuff("name", skill.name)]
                if alive_without_dot:
                    alive = alive_without_dot # 如果有，则只从这些敌人中选择
                if not alive: return [] # 如果都没有，则无目标

            if is_boss:
                return random.sample(alive, k=1)  # boss攻击随机挑选目标
            return [min(alive, key=lambda x: x.hp)]  # 玩家攻击选择血量最少的敌人


        elif skill.target_type == TargetType.AOE:
            return alive  # 所有敌人

        elif skill.target_type == TargetType.MULTI:
            if skill.skill_type == SkillType.DOT: # 多目标DoT技能，同样优先选择没有该Debuff的敌人
                alive_without_dot = [a for a in alive if not a.has_debuff("name", skill.name)]
                if alive_without_dot:
                    alive = alive_without_dot
                if not alive: return []
            
            n = min(getattr(skill, 'multi_count', 2), len(alive)) # 目标数不能超过存活敌人
            if n == 0: return []
            
            if is_boss:
                return random.sample(alive, k=n)  # boss攻击随机挑选
            return sorted(alive, key=lambda x: x.hp)[:n]  # 玩家攻击选择血量最少的N个敌人

        return []

    def _execute_skill(self, caster, targets, skill):
        """
        执行一个技能。
        :param caster: 施法者实体。
        :param targets: 目标实体列表。
        :param skill: 技能实例。
        :return: 技能执行结果消息字符串，造成的总伤害。
        """
        total_dmg = 0  # 记录总伤害
        skill_activated = False

        # 记录攻击者行动前的HP/MP，用于后续判断是否需要更新其状态栏
        caster_hp_before_action = caster.hp
        caster_mp_before_action = caster.mp

        # 计算释放概率 (技能本身的触发率)
        if random.uniform(0, 100) <= skill.rate:
            skill_activated = True

            # 计算消耗并扣除
            hp_cost = caster.hp * skill.hp_cost_rate
            mp_cost = caster.exp * skill.mp_cost_rate * (1 - caster.mp_cost_modifier)
            caster.pay_cost(hp_cost, mp_cost, deduct=True)  # 扣除消耗

            parts = []
            if hp_cost > 0:
                parts.append(f"气血{number_to(int(hp_cost))}点")
            if mp_cost > 0:
                parts.append(f"真元{number_to(int(mp_cost))}点")
            
            cost_msg = f"消耗{'、'.join(parts)}，" if parts else ""
            self.add_message(caster, f"{caster.name}使用{skill.name}！{cost_msg}")
            
            skill.trigger_cd()  # 技能进入冷却

            # --- 核心逻辑分支 (根据SkillType处理不同技能效果) ---
            # Type 1: 连续攻击 (Multi-Hit)
            if skill.skill_type == SkillType.MULTI_HIT:
                hits = skill.atk_values if isinstance(skill.atk_values, list) else [skill.atk_values] # 攻击倍率列表
                if not targets: return "", 0
                target = targets[0] # 连续攻击通常只针对一个目标
                
                hit_dmgs = []
                for mult in hits:  # 遍历每一次攻击
                    # 计算单次攻击伤害
                    dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, target, float(mult))
                    if status == "Hit":
                        hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, dmg, true_dmg)
                        # 处理反伤
                        if reflected_dmg > 0:
                            self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                            caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True) # 反弹伤害对攻击者造成
                        total_dmg += hp_dmg
                        crit_str = "💥" if is_crit else ""
                        hit_dmgs.append(f"{crit_str}{number_to(int(hp_dmg))}伤害")
                        # 斩命和复活判定
                        killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                        if not target.is_alive and not killed_by_death_strike: # 只有没被斩杀才检查复活
                            if self._check_and_apply_revive_effects(target, caster.has_death_strike):
                                total_dmg -= hp_dmg # 复活后抵消掉的伤害

                    else:
                        hit_dmgs.append("miss")
                
                self.add_message(caster, f"→ 对{target.name}造成" + "、".join(hit_dmgs) + "！")
                # self.add_unit_status_message(target) # 更新目标血条 -- 统一在回合末更新

                # 连续攻击后可能需要休息
                if skill.turn_cost > 0:
                    effect = StatusEffect(skill.name, DebuffType.FATIGUE, 0, 1, True, skill.turn_cost, skill.skill_type)
                    caster.add_status(effect)
                    self.add_message(caster, f"→ {caster.name}力竭，需休息{skill.turn_cost}回合")
                return "", total_dmg

            # Type 2: 持续伤害 (DoT)
            elif skill.skill_type == SkillType.DOT:
                if not targets: return "", 0
                target_names = []
                for target in targets:
                    target_names.append(target.name)
                    # 施加持续伤害Debuff，coefficient用于记录施法者，以便计算后续伤害
                    effect = StatusEffect(skill.name, DebuffType.SKILL_DOT, skill.atk_values, caster.name, True,
                                        skill.turn_cost,
                                        skill.skill_type)
                    target.add_status(effect)
                target_name_msg = "、".join(target_names)
                # 使用 get_effect_desc 格式化显示伤害倍率
                damage_desc = self.get_effect_desc(DebuffType.SKILL_DOT, True, skill.atk_values[0] if isinstance(skill.atk_values, list) else skill.atk_values)
                self.add_message(caster, f"→ 对{target_name_msg}施加持续伤害，{damage_desc}，持续{skill.turn_cost}回合")
                return "", total_dmg

            # Type 3: 属性增益 (Stat Buff)
            elif skill.skill_type == SkillType.BUFF_STAT:
                if not targets: return "", 0
                # Use raw numeric value for buffvalue
                buff_value_for_display = skill.skill_buff_value
                
                # 根据buff类型施加不同的Buff效果
                if skill.skill_buff_type == 1:  # 攻击力增加 (BuffType.ATTACK_UP)
                    effect = StatusEffect(skill.name, BuffType.ATTACK_UP, buff_value_for_display, 1, False, skill.turn_cost,
                                        skill.skill_type)
                    caster.add_status(effect)  # 给自己添加Buff
                    self.add_message(caster, f"→ 提升了{self.get_effect_desc(BuffType.ATTACK_UP, False, buff_value_for_display)}，持续{skill.turn_cost}回合")
                elif skill.skill_buff_type == 2:  # 减伤加成 (BuffType.DAMAGE_REDUCTION_UP)
                    effect = StatusEffect(skill.name, BuffType.DAMAGE_REDUCTION_UP, buff_value_for_display, 1, False,
                                        skill.turn_cost, skill.skill_type)
                    caster.add_status(effect)  # 给自己添加Buff
                    self.add_message(caster, f"→ 提升了{self.get_effect_desc(BuffType.DAMAGE_REDUCTION_UP, False, buff_value_for_display)}，持续{skill.turn_cost}回合")
                
                # Buff技能通常伴随一次普攻，但不触发双生
                attack_msg, total_dmg = self._normal_attack_and_process(caster, targets[0], skip_twin_strike=True, is_skill_proc=True)
                return attack_msg, total_dmg

            # Type 4: 封印/控制 (Control)
            elif skill.skill_type == SkillType.CONTROL:
                if not targets: return "", 0
                chance = skill.success_rate # 控制成功率
                target_names_success = []
                target_names_failure = []
                for target in targets:
                    if random.uniform(0, 100) <= chance: # 概率判定
                        effect = StatusEffect(skill.name, DebuffType.SEAL, skill.turn_cost, 1, True, skill.turn_cost, skill.skill_type) # Value is duration
                        target.add_status(effect)
                        target_names_success.append(target.name)
                    else:  # 封印失败
                        target_names_failure.append(target.name)
                
                # 使用 get_effect_desc 格式化显示控制效果
                control_desc = self.get_effect_desc(DebuffType.SEAL, True, skill.turn_cost)
                if target_names_success:
                    target_name_msg = "、".join(target_names_success)
                    self.add_message(caster, f"→ {target_name_msg}{control_desc}！")
                if target_names_failure:
                    target_name_msg = "、".join(target_names_failure)
                    self.add_message(caster, f"→ 封印失败，被{target_name_msg}抵抗了！")
                
                # 控制技能通常伴随一次普攻，但不触发双生
                attack_msg, total_dmg = self._normal_attack_and_process(caster, targets[0], skip_twin_strike=True, is_skill_proc=True)
                return attack_msg, total_dmg

            # Type 5: 随机波动伤害 (Random Hit)
            elif skill.skill_type == SkillType.RANDOM_HIT:
                if not targets: return "", 0
                target = targets[0]
                min_mult = float(skill.atk_values[0]) if isinstance(skill.atk_values, list) else float(skill.atk_values) # 最小倍率
                max_mult = float(skill.atk_coefficient) # 最大倍率
                rand_mult = random.uniform(min_mult, max_mult) # 随机一个倍率
                rand_mult = round(rand_mult, 2)  # 保留两位小数
                dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, target, rand_mult)

                if status == "Hit":
                    hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, dmg, true_dmg)
                    # 处理反伤
                    if reflected_dmg > 0:
                        self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                        caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
                    total_dmg = hp_dmg
                    crit_str = "💥并且发生了会心一击，" if is_crit else ""
                    self.add_message(caster, f"→ 获得{rand_mult}倍加成，{crit_str}对{target.name}造成{number_to(int(total_dmg))}伤害！")
                    # self.add_unit_status_message(target) # 更新目标血条 -- 统一在回合末更新
                    # 斩命和复活判定
                    killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                    if not target.is_alive and not killed_by_death_strike:
                        if self._check_and_apply_revive_effects(target, caster.has_death_strike):
                            total_dmg -= hp_dmg
                else:
                    self.add_message(caster, f"→ 技能被{target.name}闪避了！")

                # 波动伤害后可能需要休息
                if skill.turn_cost > 0:
                    effect = StatusEffect(skill.name, DebuffType.FATIGUE, skill.turn_cost, 1, True, skill.turn_cost, skill.skill_type) # Value is duration
                    caster.add_status(effect)
                    self.add_message(caster, f"→ {caster.name}力竭，需休息{skill.turn_cost}回合")
                return "", total_dmg

            # Type 6: 叠加 Buff (Stacking)
            elif skill.skill_type == SkillType.STACK_BUFF:
                if not targets: return "", 0
                # 施加一个可叠加的Buff，通常是攻击力
                effect = StatusEffect(skill.name, BuffType.ATTACK_UP, skill.skill_buff_value, 1, False, skill.turn_cost,
                                    skill.skill_type)
                caster.add_status(effect)  # 给自己添加Buff
                # 使用 get_effect_desc 格式化显示叠加Buff效果
                buff_desc = self.get_effect_desc(BuffType.ATTACK_UP, False, skill.skill_buff_value)
                self.add_message(caster, f"→ 每回合叠加{buff_desc}，持续{skill.turn_cost}回合")
                # 叠加Buff后通常伴随一次普攻，但不触发双生
                attack_msg, total_dmg = self._normal_attack_and_process(caster, targets[0], skip_twin_strike=True, is_skill_proc=True)
                return attack_msg, total_dmg


            # Type 101: BOSS专属技能 紫玄掌 (倍数伤害+目标百分比生命值伤害)
            elif skill.skill_type == SkillType.MULTIPLIER_PERCENT_HP:
                if not targets: return "", 0
                current_total_dmg = 0 # 临时变量，记录本次技能的总伤害
                for target in targets:
                    # 造成固定倍率伤害
                    dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, target, skill.atk_values[0] if isinstance(skill.atk_values, list) else skill.atk_values)
                    if status == "Hit":
                        crit_str = "💥并且发生了会心一击，" if is_crit else ""
                        # 附加目标最大生命值的百分比伤害
                        raw_dmg_with_hp_percent = dmg + (target.max_hp * skill.atk_coefficient)
                        hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, raw_dmg_with_hp_percent, true_dmg)
                        # 处理反伤
                        if reflected_dmg > 0:
                            self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                            caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
                        current_total_dmg += hp_dmg
                        self.add_message(caster, f"→ {crit_str}对{target.name}造成{number_to(int(hp_dmg))}伤害！")
                        # self.add_unit_status_message(target) # 更新目标血条 -- 统一在回合末更新
                        # 斩命和复活判定
                        killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                        if not target.is_alive and not killed_by_death_strike:
                            if self._check_and_apply_revive_effects(target, caster.has_death_strike):
                                current_total_dmg -= hp_dmg
                    else:
                        self.add_message(caster, f"→ {target.name}躲开了{caster.name}的攻击！")
                total_dmg = current_total_dmg # 更新总伤害
                return "", total_dmg

            # Type 102: BOSS专属技能 子龙朱雀 (倍数伤害+无视防御)
            elif skill.skill_type == SkillType.MULTIPLIER_DEF_IGNORE:
                if not targets: return "", 0
                current_total_dmg = 0
                for target in targets:
                    # 穿透护甲，造成固定倍率伤害
                    dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, target, skill.atk_values[0] if isinstance(skill.atk_values, list) else skill.atk_values, True) # 穿透护甲
                    if status == "Hit":
                        crit_str = "💥并且发生了会心一击，" if is_crit else ""
                        hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, dmg, true_dmg)
                        # 处理反伤
                        if reflected_dmg > 0:
                            self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                            caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
                        current_total_dmg += hp_dmg
                        self.add_message(caster, f"→ {crit_str}对{target.name}造成{number_to(int(hp_dmg))}伤害！")
                        # self.add_unit_status_message(target) # 更新目标血条 --统一在回合末更新
                        # 斩命和复活判定
                        killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                        if not target.is_alive and not killed_by_death_strike:
                            if self._check_and_apply_revive_effects(target, caster.has_death_strike):
                                current_total_dmg -= hp_dmg
                    else:
                        self.add_message(caster, f"→ {target.name}躲开了{caster.name}的攻击！")
                total_dmg = current_total_dmg
                return "", total_dmg

            # Type 103: 控制类型 (CC)
            elif skill.skill_type == SkillType.CC:
                if not targets: return "", 0
                chance = skill.success_rate
                target_names_success = []
                target_names_failure = []
                for target in targets:
                    if random.uniform(0, 100) <= chance: # 概率判定
                        effect = StatusEffect(skill.name, skill.skill_buff_type, skill.turn_cost, 1, True, skill.turn_cost, # Value is duration
                                            skill.skill_type)
                        target.add_status(effect)
                        target_names_success.append(target.name)
                    else:  # 控制失败
                        target_names_failure.append(target.name)
                
                # 使用 get_effect_desc 格式化显示控制效果
                control_desc = self.get_effect_desc(skill.skill_buff_type, True, skill.turn_cost)
                if target_names_success:
                    target_name_msg = "、".join(target_names_success)
                    self.add_message(caster, f"→ {target_name_msg}{control_desc}！")
                if target_names_failure:
                    target_name_msg = "、".join(target_names_failure)
                    self.add_message(caster, f"→ {skill.name}对{target_name_msg}的控制被抵抗了！")
                return "", total_dmg

            # Type 104: 召唤类型 (SUMMON)
            elif skill.skill_type == SkillType.SUMMON:
                copy_ratio = skill.atk_values[0] if isinstance(skill.atk_values, list) else skill.atk_values # 召唤物属性倍率
                summon_count = int(skill.atk_coefficient)  # 召唤数量

                for i in range(summon_count):
                    # 创建召唤物的数据字典，继承施法者部分属性并进行缩放
                    summon_data = {}

                    # 1. 复制基础信息
                    summon_data["user_id"] = self.bot_id # 召唤物ID也设置为bot_id
                    summon_data["nickname"] = f"{caster.name}的召唤物"
                    summon_data["monster_type"] = "summon"

                    # 2. 复制并缩放基础属性
                    summon_data["max_hp"] = caster.max_hp * copy_ratio
                    summon_data["current_hp"] = caster.max_hp * copy_ratio
                    summon_data["max_mp"] = caster.max_mp * copy_ratio
                    summon_data["current_mp"] = caster.max_mp * copy_ratio
                    summon_data["attack"] = caster.base_atk * copy_ratio
                    summon_data["armor_penetration"] = caster.base_armor_pen * copy_ratio
                    summon_data["damage_reduction"] = caster.base_damage_reduction * copy_ratio
                    summon_data["critical_rate"] = caster.base_crit
                    summon_data["accuracy"] = caster.base_accuracy
                    summon_data["dodge"] = caster.base_dodge
                    summon_data["speed"] = caster.base_speed

                    # 3. 召唤物特有设置
                    summon_data["start_skills"] = []  # 召唤物没有初始技能
                    summon_data["skills"] = []  # 召唤物没有技能，只能普通攻击

                    # 如果是BOSS的召唤物，保留BOSS标识
                    if hasattr(caster, 'is_boss') and caster.is_boss:
                        summon_data["is_boss"] = True
                    else:
                        summon_data["is_boss"] = False

                    summon = Entity(
                        data=summon_data,
                        team_id=caster.team_id,  # 与召唤者同队
                        is_boss=summon_data.get("is_boss", False)
                    )

                    # 将召唤物加入对应的队伍
                    if caster.team_id == 0:
                        self.team_a.append(summon)
                    else:
                        self.team_b.append(summon)

                self.add_message(caster, f"→ 生成{summon_count}个召唤物！")
                return "", total_dmg

            else:
                self.add_message(caster, f"→ {skill.name}技能效果未知！")
                return "", total_dmg
        
        # If the skill didn't activate, perform a normal attack
        target = min(targets, key=lambda x: x.hp) if targets else None
        if not target: return "", 0
        
        # Skill did not activate, fall back to normal attack logic
        attack_msg, total_dmg = self._normal_attack_and_process(caster, target, skip_twin_strike=True)
        
        return attack_msg, total_dmg


    def _normal_attack_and_process(self, caster, target, skip_twin_strike=False, is_skill_proc=False):
        """
        执行普通攻击并处理伤害、反伤、斩命、复活。
        :param caster: 攻击者实体。
        :param target: 目标实体。
        :param skip_twin_strike: 是否跳过双生效果判定，用于技能未触发后的普攻。
        :param is_skill_proc: 是否是技能触发的普攻（用于消息区分）。
        :return: 普攻结果消息字符串，造成的总伤害。
        """
        total_dmg = 0
        
        # 记录攻击者行动前的HP/MP，用于后续判断是否需要更新其状态栏
        caster_hp_before_action = caster.hp
        caster_mp_before_action = caster.mp

        raw_dmg, true_dmg, is_crit, accuracy = self._calc_raw_damage(caster, target, 1) # 普攻倍率为1
        
        if accuracy == "Hit":
            hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, raw_dmg, true_dmg)
            # 处理反伤
            if reflected_dmg > 0:
                self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
            total_dmg = hp_dmg
            
            crit_str = "💥并且发生了会心一击，" if is_crit else ""
            if is_skill_proc: # 技能后普攻
                self.add_message(caster, f"→ {crit_str}对{target.name}造成{number_to(int(total_dmg))}伤害！")
            else: # 纯普攻
                self.add_message(caster, f"{caster.name}发起攻击，{crit_str}对{target.name}造成{number_to(int(total_dmg))}伤害！")

            # self.add_unit_status_message(target) # 更新目标血条 -- 统一在回合末更新

            # 斩命和复活判定
            killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
            if not target.is_alive and not killed_by_death_strike: # 只有没被斩杀才检查复活
                if self._check_and_apply_revive_effects(target, caster.has_death_strike):
                    total_dmg -= hp_dmg # 复活后抵消掉的伤害
            
            # --- 双生效果判定 (NatalEffectType.TWIN_STRIKE) ---
            # 仅限普通攻击触发，且未被跳过
            if not skip_twin_strike and caster.natal_data and caster.twin_strike_effect:
                trigger_chance, damage_multiplier = caster.twin_strike_effect
                if random.random() < trigger_chance:
                    # 触发连击，再次造成额外伤害
                    twin_strike_dmg, twin_strike_true_dmg, twin_strike_is_crit, twin_strike_accuracy = self._calc_raw_damage(caster, target, damage_multiplier)
                    
                    if twin_strike_accuracy == "Hit":
                        twin_hp_dmg, _, twin_reflected_dmg = self._apply_damage(caster, target, twin_strike_dmg, twin_strike_true_dmg)
                        if twin_reflected_dmg > 0:
                            self.add_message(target, f"对{caster.name}反弹{number_to(twin_reflected_dmg)}真实伤害！")
                            caster.update_stat("hp", 2, twin_reflected_dmg, bypass_shield=True)
                        total_dmg += twin_hp_dmg # 累计双生伤害
                        twin_crit_str = "💥" if twin_strike_is_crit else ""
                        self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』触发【双生】，{twin_crit_str}再次对{target.name}造成{number_to(int(twin_hp_dmg))}伤害！")
                        # self.add_unit_status_message(target) # 更新目标血条 -- 统一在回合末更新
                        
                        # 斩命和复活判定 (双生攻击后也进行判定)
                        killed_by_death_strike = self._check_and_apply_death_strike(caster, target, twin_hp_dmg)
                        if not target.is_alive and not killed_by_death_strike:
                            if self._check_and_apply_revive_effects(target, caster.has_death_strike):
                                total_dmg -= twin_hp_dmg # 复活后抵消掉的伤害
                    else:
                        self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』触发【双生】，但被{target.name}躲开了！")
                
            # 如果攻击者HP/MP因吸血/吸蓝/反伤等发生变化，则更新其血条 -- 统一在回合末更新
            # if caster.hp != caster_hp_before_action or caster.mp != caster_mp_before_action:
            #     self.add_unit_status_message(caster)

        else: # 普攻Miss
            if is_skill_proc: # 技能后普攻
                self.add_message(caster, f"→ 攻击被{target.name}躲开了")
            else: # 纯普攻
                self.add_message(caster, f"{caster.name}使用普通攻击，被{target.name}躲开了")

        return "", total_dmg # 返回空消息，因为消息已经在内部add_message了


    def check_unit_control(self, unit):
        """
        检查单位的控制状态，并返回控制消息。
        :param unit: 实体。
        :return: 控制消息字符串或None。
        """
        # 所有会导致跳过回合的控制效果
        SKIP_TURN_CONTROLS = {
            DebuffType.FATIGUE: ("😫", "正在调息，跳过回合"),
            DebuffType.STUN: ("🌀", "被眩晕，跳过回合"),
            DebuffType.FREEZE: ("❄️", "被冰冻，跳过回合"),
            DebuffType.PETRIFY: ("🗿", "被石化，跳过回合"),
            DebuffType.SLEEP: ("💤", "正在沉睡，跳过回合"),
            DebuffType.ROOT: ("🌿", "被定身，跳过回合"),
            DebuffType.FEAR: ("😱", "陷入恐惧，跳过回合"),
            DebuffType.SEAL: ("🔒", "被封印，无法使用技能"), # 封印可能只禁技能，这里统一处理为跳过回合
            DebuffType.PARALYSIS: ("⚡", "全身麻痹，跳过回合"),
            DebuffType.SILENCE: ("🔇", "被沉默，无法施法") # 沉默只禁法术，不一定跳过回合，但为了简化，这里也视为控制
        }

        # 检查每种控制效果
        for debuff_type, (emoji, description) in SKIP_TURN_CONTROLS.items():
            if unit.has_debuff("type", debuff_type):
                duration = unit.get_debuff_field("type", "duration", debuff_type)
                return f"{emoji}{unit.name}{description}（剩余{duration}回合）"

        return None

    def process_turn(self):
        """
        处理单个战斗回合的逻辑。
        包括回合开始、单位行动、状态更新、伤害结算、胜负判定等。
        """
        self.round += 1
        # 获取所有存活单位并按速度排序
        # 修复：将 'u for u u in' 改为 'u for u in'
        units = sorted([u for u in self.team_a + self.team_b if u.is_alive], key=lambda x: x.base_speed, reverse=True)
        
        # 记录所有单位在回合开始时的HP/MP，用于回合结束时统一更新状态条
        units_initial_hp_mp = {unit.id: {'hp': unit.hp, 'mp': unit.mp} for unit in units}

        # 战斗开始时（第1回合），应用开局技能
        if self.round == 1:
            for unit in units:
                enemies = self._get_all_enemies(unit)
                self._apply_round_one_skills(unit, enemies, unit.start_skills)
            # 在第一回合结束时统一更新所有单位状态，避免开局技能导致重复消息
            # 这里的更新放在回合开始的周期性效果处理之后，确保只显示一次
            # 移除此处的 add_unit_status_message，统一由下面的周期性效果处理后的更新取代

        # 回合开始时的周期性效果 (如本命法宝)
        # 每4回合触发一次，包括第1回合 ((1-1)%4 == 0)
        if (self.round - 1) % 4 == 0:
            for unit in units:
                if not unit.is_alive: continue
                # 确保 unit.natal_data 为 None 时不会尝试获取名称和触发效果
                if unit.natal_data: 
                    self.add_message(unit, f"『{unit.natal_data.get('name','本命法宝')}』道韵流转，威能再现！")
                    unit.apply_natal_periodic_effect(self) # 此方法内部不再打印 "道韵流转"

                    # === 新增：道韵真伤逻辑 ===
                    # 仅玩家角色（非BOSS）的本命法宝触发此效果
                    if not unit.is_boss:
                        natal_treasure_level = unit.natal_data.get("level", 0) # 从natal_data获取level
                        periodic_true_dmg_rate = PERIODIC_TRUE_DAMAGE_BASE + natal_treasure_level * PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL
                        
                        if periodic_true_dmg_rate > 0:
                            enemies_to_damage = self._get_all_enemies(unit)
                            if enemies_to_damage:
                                self.add_message(unit, f"◎ 『{unit.natal_data.get('name', '本命法宝')}』道韵生效！")
                                for enemy in enemies_to_damage:
                                    if enemy.is_alive:
                                        true_damage = int(enemy.hp * periodic_true_dmg_rate)
                                        if true_damage > 0:
                                            enemy.update_stat("hp", 2, true_damage, bypass_shield=True) # 真实伤害无视护盾和减伤，直接扣除HP
                                            self.add_message(unit, f"→ 对 {enemy.name} 造成 {number_to(true_damage)} 点真实伤害！")
                                            # self.add_unit_status_message(enemy) # 移除：统一在回合末更新
                                            
                                            # 检查目标是否死亡并尝试复活
                                            if not enemy.is_alive:
                                                # 道韵真伤不带斩命属性，所以第二个参数为False
                                                if not self._check_and_apply_revive_effects(enemy, False):
                                                    self.add_message(enemy, f"💀{enemy.name}倒下了！")
        
            # 在周期性效果处理后统一更新所有单位状态，避免重复消息
            # 这里统一更新所有单位的状态条
            for unit in units:
                if unit.is_alive:
                    self.add_unit_status_message(unit) # 确保所有单位在回合开始（包括周期性效果后）有一次状态更新


        # 按单位行动
        for unit in units:
            if not unit.is_alive: continue
            
            # 回合开始头部信息 (当前行动单位)
            self.add_message(unit, f"☆------{unit.name}的回合 (第 {self.round} 回合)------☆")
            # 移除此处的 add_unit_status_message，统一由回合末或周期性更新处理
            # self.add_unit_status_message(unit)

            # 更新Buff/Debuff持续时间，技能冷却
            unit.update_status_effects()  

            # 记录HP和MP，用于后续判断是否需要更新状态栏
            # 这一段移到外层循环，记录所有单位的初始HP/MP，便于统一在回合末处理
            # initial_hp_in_turn = unit.hp
            # initial_mp_in_turn = unit.mp

            # 本命法宝流血概率触发
            if unit.natal_data: # 确保存在本命法宝数据
                unit.apply_natal_bleed_proc(self)
                
            # DoT 伤害结算 (中毒、流血、技能持续伤害)
            if unit.poison_dot_dmg > 0:
                self.add_message(unit, f"☠️{unit.name}中毒消耗气血{number_to(int(unit.poison_dot_dmg))}点")
                hp_dmg_from_dot, _, _ = self._apply_damage(unit, unit, unit.poison_dot_dmg, 0) # 自己给自己造成伤害，不反弹
                # 复活判定 (在hp_dmg_from_dot 计算后进行，避免重复扣除生命)
                if not unit.is_alive:
                    if not self._check_and_apply_revive_effects(unit, False): # 中毒无法触发斩命
                        self.add_message(unit, f"💀{unit.name}倒下了！")
            
            bleed_damages = unit.bleed_dot_dmg_list
            if bleed_damages:
                total_bleed_dmg = sum(bleed_damages)
                self.add_message(unit, f"🩸{unit.name}因流血消耗气血{number_to(int(total_bleed_dmg))}点 ({len(bleed_damages)}层)")
                hp_dmg_from_dot, _, _ = self._apply_damage(unit, unit, total_bleed_dmg, 0) # 自己给自己造成伤害，不反弹
                # 复活判定
                if not unit.is_alive:
                    if not self._check_and_apply_revive_effects(unit, False): # 流血无法触发斩命
                        self.add_message(unit, f"💀{unit.name}倒下了！")

            # 技能DoT结算
            for skill_dot_info in unit.get_debuffs("type", DebuffType.SKILL_DOT):
                caster = next((u for u in self.team_a + self.team_b if u.name == skill_dot_info.coefficient), None) # 找到施加DoT的施法者
                if not caster: continue
                raw_dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, unit, skill_dot_info.value) # DoT通常没有true_dmg
                if status == "Hit":
                    hp_dmg, _, reflected_dmg = self._apply_damage(caster, unit, raw_dmg, true_dmg)
                    # 处理反伤
                    if reflected_dmg > 0:
                        self.add_message(unit, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                        caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
                    
                    crit_str = "💥会心一击，" if is_crit else ""
                    self.add_message(unit, f"{skill_dot_info.name}{crit_str}造成{number_to(int(hp_dmg))}伤害！"
                                           f"（剩余{skill_dot_info.duration}回合）")
                    # self.add_unit_status_message(unit) # 移除：统一在回合末更新
                    # 斩命和复活判定
                    killed_by_death_strike = self._check_and_apply_death_strike(caster, unit, hp_dmg)
                    if not unit.is_alive and not killed_by_death_strike:
                        if not self._check_and_apply_revive_effects(unit, caster.has_death_strike):
                            self.add_message(unit, f"💀{unit.name}倒下了！")


            # HoT 结算 (HP和MP恢复)
            if unit.hp_regen_rate > 0:
                self.add_message(unit, f"❤️{unit.name}回复气血{number_to(int(unit.hp_regen_rate))}点")
                unit.update_stat("hp", 1, unit.hp_regen_rate)

            if unit.mp_regen_rate > 0:
                self.add_message(unit, f"💙{unit.name}回复真元{number_to(int(unit.mp_regen_rate))}点")
                unit.update_stat("mp", 1, unit.mp_regen_rate)
            
            # 如果HP或MP有变化，则更新当前单位的状态显示 (在所有DoT/HoT/吸血/吸蓝后一次性更新)
            # 移除此处的 add_unit_status_message，统一由回合末处理
            # if unit.hp != initial_hp_in_turn or unit.mp != initial_mp_in_turn:
            #     self.add_unit_status_message(unit)


            # Buff 状态显示 (例如，持续Buff的剩余回合数)
            if unit.has_buff("skill_type", 3): # 属性增益型Buff
                for skill_buff in unit.get_buffs("skill_type", 3):
                    # Pass raw numeric value to get_effect_desc
                    buff_msg = self.get_effect_desc(skill_buff.type, False, skill_buff.value)
                    self.add_message(unit, f"『{skill_buff.name}』{buff_msg}，剩余{skill_buff.duration}回合")

            if unit.has_buff("skill_type", 6): # 叠加型Buff
                skill_buff = unit.get_buff("skill_type", 6)
                # 叠加Buff的逻辑：每次叠加基础值乘以系数
                # 假设 coefficient 记录已叠加层数
                # 原始逻辑是 skill_buff.value + skill_buff.value / skill_buff.coefficient，可能不符合通常的叠加逻辑
                # 修正为：每次效果触发，直接增加skill_buff.value到某个累加属性上
                # 由于此处没有累加属性，直接修改buff的value不太直观。
                # 暂不修改，如果后续有问题，这里需要重新设计叠加buff的机制。
                # Use skill_buff.value for display as it's the base value of each stack
                buff_msg = self.get_effect_desc(BuffType.ATTACK_UP, False, skill_buff.value)
                # The actual effect value in the buff should reflect the sum of stacks
                # Assuming 'coefficient' is the current stack count, and 'value' is the base per stack
                # When applying, you'd iterate through buffs and sum up values for the total effect.
                # For display here, we show the 'per stack' value.
                self.add_message(unit, f"『{skill_buff.name}』{buff_msg}，剩余{skill_buff.duration}回合")


            # 结算后检查死亡，如果死亡则跳过后续行动
            if not unit.is_alive:
                # 只有在之前的处理中没有打印过死亡消息时才打印
                if not any(f"💀{unit.name}倒下了！" in entry["data"]["content"] for entry in self.play_list[-5:]): # 检查最近5条消息
                    self.add_message(unit, f"💀{unit.name}倒下了！")
                continue # 跳过当前单位的行动


            # 控制状态检查，如果被控制则跳过行动
            control_message = self.check_unit_control(unit)
            if control_message:
                self.add_message(unit, control_message)
                continue # 跳过当前单位的行动

            enemies = self._get_all_enemies(unit)
            if not enemies: break # 没有敌人则战斗结束

            # --- 攻击流程 ---
            skill = self.choose_skill(unit, unit.skills, enemies) # 选择技能
            
            if skill:
                targets = self._select_targets(enemies, skill, unit.is_boss) # 选择目标
                if not targets: # 如果技能有目标但没选到，则进行普攻
                    target = min(enemies, key=lambda x: x.hp)
                    _, total_dmg = self._normal_attack_and_process(unit, target)
                else:
                    _, total_dmg = self._execute_skill(unit, targets, skill) # 执行技能
            else: # 如果没有可用技能，则进行普攻
                target = min(enemies, key=lambda x: x.hp) # 普攻选择血量最少的敌人
                _, total_dmg = self._normal_attack_and_process(unit, target)

            # 处理吸血/吸蓝
            if total_dmg > 0:
                lifesteal_msg, mana_steal_msg = "", ""
                # old_unit_hp_after_attack = unit.hp # 再次记录，用于吸血后的状态更新 -- 统一在回合末更新
                # old_unit_mp_after_attack = unit.mp

                if unit.lifesteal_rate > 0:
                    lifesteal = int(total_dmg * unit.lifesteal_rate)
                    if lifesteal > 0:
                        lifesteal_msg = f"❤️吸取气血{number_to(lifesteal)}点"
                        unit.update_stat("hp", 1, lifesteal)
                
                if unit.mana_steal_rate > 0:
                    mana_steal = int(total_dmg * unit.mana_steal_rate)
                    if mana_steal > 0:
                        mana_steal_msg = f"💙吸取真元{number_to(mana_steal)}点"
                        unit.update_stat("mp", 1, mana_steal)

                if lifesteal_msg or mana_steal_msg:
                    messages = []
                    if lifesteal_msg: messages.append(lifesteal_msg)
                    if mana_steal_msg: messages.append(mana_steal_msg)
                    self.add_message(unit, f"（{'、'.join(messages)}）")
                
                # 如果吸血/吸蓝导致HP或MP变化，则更新当前单位的状态显示 -- 统一在回合末更新
                # if unit.hp != old_unit_hp_after_attack or unit.mp != old_unit_mp_in_action:
                #     self.add_unit_status_message(unit)

            unit.total_dmg += total_dmg
            # 攻击完成后检查敌人是否死亡，并打印死亡消息
            for enemy in enemies:
                if not enemy.is_alive:
                    # 只有在之前的处理中没有打印过死亡消息时才打印
                    if not any(f"💀{enemy.name}倒下了！" in entry["data"]["content"] for entry in self.play_list[-5:]):
                        self.add_message(enemy, f"💀{enemy.name}倒下了！")
            
            # --- 回合结束，统一更新状态条 ---
            # 遍历所有单位，检查其HP/MP是否与回合开始时不同，如果不同则更新状态条
            for u in self.team_a + self.team_b:
                if u.id in units_initial_hp_mp:
                    if u.hp != units_initial_hp_mp[u.id]['hp'] or u.mp != units_initial_hp_mp[u.id]['mp']:
                        self.add_unit_status_message(u) # 只有发生变化的单位才更新状态条


    def get_final_status_list(self):
        """
        获取战斗结束时所有实体的最终状态列表。
        包括HP、MP、用户ID、HP/MP乘数、队伍ID、总伤害和本命法宝次数统计。
        :return: 最终状态列表。
        """
        status = []
        for u in self.team_a + self.team_b:
            natal_counts = {}
            if u.natal_data:
                natal_counts = {
                    "fate_revive_count": u.fate_revive_count,
                    "immortal_revive_count": u.immortal_revive_count,
                    "invincible_gain_count": u.invincible_count,
                }
            status.append({
                u.name: {
                    "hp": int(u.hp),
                    "mp": int(u.mp),
                    "user_id": u.id,
                    "hp_multiplier": u.max_hp / (u.exp / 2 if u.exp > 0 else 1), # 用于反向还原HP
                    "mp_multiplier": u.max_mp / (u.exp if u.exp > 0 else 1), # 用于反向还原MP
                    "team_id": u.team_id,
                    "total_dmg": int(u.total_dmg),
                    "natal_data": natal_counts # 传回本命法宝的次数统计
                }
            })
        return status

    def run_battle(self):
        """
        运行整个战斗过程，直到一方胜利或达到最大回合数。
        :return: 战斗日志列表，胜利方ID (0/1/2平局)，最终状态列表。
        """
        # 战斗开始前检查单位的Debuff免疫
        for unit in self.team_a + self.team_b:
            if unit.is_alive:
                unit.check_and_clear_debuffs_by_immunity()

        while self.round < self.max_rounds:
            self.process_turn() # 处理一个回合

            # 检查队伍A和队伍B是否有存活单位
            alive_a = any(u.is_alive for u in self.team_a)
            alive_b = any(u.is_alive for u in self.team_b)

            if not alive_a: # 队伍A全灭，队伍B胜利
                winner_name = next((u.name for u in self.team_b if u.is_alive), "B队")
                self.add_system_message(f"战斗结束: {winner_name} 方获胜!")
                return self.play_list, 1, self.get_final_status_list()

            if not alive_b: # 队伍B全灭，队伍A胜利
                winner_name = next((u.name for u in self.team_a if u.is_alive), "A队")
                self.add_system_message(f"战斗结束: {winner_name} 方获胜!")
                return self.play_list, 0, self.get_final_status_list()

        self.add_system_message("战斗超过最大回合数，平局！")
        return self.play_list, 2, self.get_final_status_list() # 达到最大回合数则平局
