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
from ..xiuxian_natal_treasure.natal_config import *
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
    max_mp = int(user_info['mp'] * (1 + main_mp_buff + impart_mp_per + mppractice))
    mp = int(user_info['mp'] * (1 + main_mp_buff + impart_mp_per + mppractice))
    atk = int((user_info['atk'] * (atkpractice + 1) * (1 + main_atk_buff) * (
            1 + weapon_atk_buff) * (1 + armor_atk_buff)) * (1 + impart_atk_per)) + int(buff_data_info.get('atk_buff', 0))
    crit = max(0, min(1, weapon_crit_buff + armor_crit_buff + main_crit_buff + impart_know_per))
    critatk = 1.5 + impart_burst_per + weapon_critatk + main_critatk
    dr = armor_def + weapon_def + main_def
    hit = 100
    dodge = 0
    ap = 0
    speed = 10

    # 玩家属性字典
    attributes = {
        "user_id": user_id,
        "nickname": user_info['user_name'],
        "max_hp": int(max_hp * ratio),
        "current_hp": int(hp * ratio),
        "max_mp": int(max_mp * ratio),
        "current_mp": int(mp * ratio),
        "mp_cost_modifier": weapon_mp_cost_modifier,
        "attack": int(atk * ratio),
        "exp": int(user_info['exp'] * ratio),
        "critical_rate": crit,
        "critical_damage": critatk,
        "boss_damage_bonus": boss_atk,
        "damage_reduction": dr,
        "armor_penetration": ap,
        "accuracy": hit,
        "dodge": dodge,
        "speed": speed,
        "start_skills": []
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

    v1 = raw_buff_value / 100
    v2 = raw_buff2_value / 100

    is_debuff = False
    
    # 特殊处理：斗战或穿甲，其值是直接的破甲值 (0-1范围)，不需要除以100
    if buff_type_id == 13 or buff_type_id == 14: 
        v1 = float(skill["break"])
        
    # 特殊处理：中毒或禁止吸取，这些是状态而非数值buff，value可能用于持续时间或强度
    if buff_type_id == 8 or buff_type_id == 10: 
        is_debuff = True
        if buff_type_id == 8:
            v1 = raw_buff_value / 100
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
    values = [v1, v2]

    for i, t in enumerate(mapped):
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
        "1": BuffType.EVASION_UP,
        "2": BuffType.ACCURACY_UP
    }

    low = float(data["buff"]) / 100
    high = float(data["buff2"]) / 100

    if low > high: 
        low, high = high, low 

    return [{
        "name": data["name"],
        "type": buff_type_map[data["buff_type"]],
        "value": random.uniform(low, high),
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
            'value': 0.5,
            'coefficient': 1,
            'is_debuff': False,
            'duration': 99,
            'skill_type': 0
        })

    # 判断random_buff是否为1，如果是则随机一个属性Buff
    if data.get("random_buff") == 1:
        configs = [
            (BuffType.ARMOR_PENETRATION_UP, (15, 40)),
            (BuffType.LIFESTEAL_UP, (2, 10)),
            (BuffType.CRIT_RATE_UP, (5, 40)),
            (BuffType.DAMAGE_REDUCTION_UP, (5, 15))
        ]

        index = random.randint(0, 3)
        buff_type, (min_val, max_val) = configs[index]

        buffs.append({
            "name": "无上战意",
            "type": buff_type,
            "value": random.uniform(min_val, max_val) / 100,
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
            
            # 确保 safe_hp_multiplier 和 safe_mp_multiplier 在使用前已被定义
            safe_hp_multiplier = hp_multiplier if hp_multiplier != 0 else 1
            safe_mp_multiplier = mp_multiplier if mp_multiplier != 0 else 1
            safe_ratio = ratio if ratio != 0 else 1

            # 反向计算玩家实际HP/MP
            hp = int(attr.get("hp", 1) / safe_ratio / safe_hp_multiplier)
            mp = int(attr.get("mp", 1) / safe_ratio / safe_mp_multiplier)

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
                    for field in ["fate_revive_count", "immortal_revive_count", "invincible_gain_count",
                                  "nirvana_revive_count", "soul_return_revive_count", "charge_status",
                                  "soul_summon_count", "enlightenment_count"]: # 新增招魂、启明次数
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
        "user_id": bot_id,
        "nickname": boss['name'],
        "max_hp": boss['总血量'],
        "current_hp": boss['气血'],
        "max_mp": boss['真元'],
        "current_mp": boss['真元'],
        "attack": boss['攻击'],
        "exp": 2,
        "critical_rate": 0,
        "critical_damage": 1.5,
        "boss_damage_bonus": 0,
        "damage_reduction": 0,
        "armor_penetration": 0,
        "accuracy": 100,
        "dodge": 0,
        "speed": 0,
        "start_skills": [],
        'monster_type': boss.get("monster_type", "boss")
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
    boss_buff_values = {
        'boss_zs': 0,
        'boss_hx': 0,
        'boss_bs': 0,
        'boss_xx': 0,
        'boss_jg': 0,
        'boss_jh': 0,
        'boss_jb': 0,
        'boss_xl': 0,
        'boss_cj': 0,
        'boss_js': 0,
        'boss_sb': 0,
        'boss_jl': 0,
        'boss_hd': 0,
        'boss_zs_boss': 0,
        'boss_sz': 0
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
        'boss_sb': [BuffType.EVASION_UP, "虚妄无相"],
        'boss_jl': [DebuffType.HEALING_BLOCK, "枯血断脉"],
        'boss_hd': [BuffType.SHIELD_BUFF, "不灭金身"],
        'boss_zs_boss': [BuffType.EXECUTE_EFFECT, "绝命斩杀"],
        'boss_sz': [BuffType.REGENERATION, "生生不息"]
    }

    boss_level = boss["jj"]

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

        # 设置到 boss_buff_values 字典上
        boss_buff_values[selected_attr] = final_val

    # 3. 定义各境界的配置数据 (数据驱动)
    cfg = None

    # --- 境界判断逻辑 ---
    # 从最高境界开始向下判断
    # 祭道境 (最高级)
    if boss_level == "祭道境" or current_rank_val <= get_rank_val('祭道境初期'):
        cfg = {
            'js': 0.95,
            'cj': (0.25, 0.50),
            'g1': [1.0, 0.7, 2.0, 1.0],
            'g2': [0.7, 0.7, 1.5, 1.0],
            'new_buffs_chance': 0.5,
            'new_buffs': {
                'boss_jl': (random.randint(HEALING_BLOCK_DURATION_MIN, HEALING_BLOCK_DURATION_MAX), DebuffType.HEALING_BLOCK),
                'boss_hd': (random.uniform(0.05, 0.20), BuffType.SHIELD_BUFF),
                'boss_zs_boss': (0.30, BuffType.EXECUTE_EFFECT),
                'boss_sz': (random.uniform(0.02, 0.05), BuffType.REGENERATION)
            }
        }

    # 至尊 ~ 斩我 (中级)
    elif get_rank_val('至尊境初期') <= current_rank_val <= get_rank_val('斩我境圆满'):
        cfg = {
            'js': (0.50, 0.55),
            'cj': (0.15, 0.30),
            'g1': [0.3, 0.1, 0.5, lambda: random.randint(5, 100) / 100],
            'g2': [0.3, 0.3, 1.5, lambda: random.randint(5, 100) / 100],
            'new_buffs_chance': 0.4,
            'new_buffs': {
                'boss_jl': (random.randint(HEALING_BLOCK_DURATION_MIN, HEALING_BLOCK_DURATION_MAX), DebuffType.HEALING_BLOCK),
                'boss_hd': (random.uniform(0.03, 0.15), BuffType.SHIELD_BUFF),
                'boss_zs_boss': (0.25, BuffType.EXECUTE_EFFECT),
                'boss_sz': (random.uniform(0.01, 0.03), BuffType.REGENERATION)
            }
        }

    # 微光 ~ 遁一
    elif get_rank_val('微光境初期') <= current_rank_val <= get_rank_val('遁一境圆满'):
        cfg = {
            'js': (0.40, 0.45),
            'cj': (0.20, 0.40),
            'g1': [0.4, 0.2, 0.7, lambda: random.randint(10, 100) / 100],
            'g2': [0.4, 0.4, 0.7, lambda: random.randint(10, 100) / 100],
            'new_buffs_chance': 0.3,
            'new_buffs': {
                'boss_jl': (HEALING_BLOCK_DURATION_MIN, DebuffType.HEALING_BLOCK),
                'boss_hd': (random.uniform(0.02, 0.10), BuffType.SHIELD_BUFF),
                'boss_zs_boss': (0.20, BuffType.EXECUTE_EFFECT),
                'boss_sz': (random.uniform(0.005, 0.02), BuffType.REGENERATION)
            }
        }

    # 星芒 ~ 至尊
    elif get_rank_val('星芒境初期') <= current_rank_val <= get_rank_val('至尊境圆满'):
        cfg = {
            'js': (0.30, 0.35),
            'cj': (0.20, 0.40),
            'g1': [0.6, 0.35, 1.1, lambda: random.randint(30, 100) / 100],
            'g2': [0.5, 0.5, 0.9, lambda: random.randint(30, 100) / 100],
            'new_buffs_chance': 0.2,
            'new_buffs': {
                'boss_hd': (random.uniform(0.01, 0.05), BuffType.SHIELD_BUFF),
                'boss_zs_boss': (0.15, BuffType.EXECUTE_EFFECT),
                'boss_sz': (random.uniform(0.001, 0.01), BuffType.REGENERATION)
            }
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

    # 祭道 ~ 月华
    else:
        cfg = {
            'js': (0.05, 0.10),
            'cj': (0.20, 0.40),
            'g1': [0.9, 0.6, 1.7, lambda: random.randint(60, 100) / 100],
            'g2': [0.62, 0.67, 1.2, lambda: random.randint(60, 100) / 100]
        }

    # 4. 统一应用配置
    if cfg:
        # 应用减伤 (JS) - 支持固定值或随机范围
        if isinstance(cfg['js'], tuple):
            boss_buff_values['boss_js'] = random.uniform(*cfg['js'])
        else:
            boss_buff_values['boss_js'] = cfg['js']

        # 应用护甲穿透 (CJ)
        if isinstance(cfg['cj'], tuple):
            boss_buff_values['boss_cj'] = random.uniform(*cfg['cj'])
        else:
            boss_buff_values['boss_cj'] = cfg['cj']


        # 应用两组随机属性
        apply_random_group(['boss_zs', 'boss_hx', 'boss_bs', 'boss_xx'], cfg['g1'])
        apply_random_group(['boss_jg', 'boss_jh', 'boss_jb', 'boss_xl'], cfg['g2'])

        # 应用新的BOSS随机BUFF (如果有)
        if 'new_buffs_chance' in cfg and random.random() < cfg['new_buffs_chance']:
            new_buff_key = random.choice(list(cfg['new_buffs'].keys()))
            value, buff_type = cfg['new_buffs'][new_buff_key]
            # 根据buff类型更新boss_buff_values
            if buff_type == DebuffType.HEALING_BLOCK:
                boss_buff_values['boss_jl'] = value 
            elif buff_type == BuffType.SHIELD_BUFF:
                boss_buff_values['boss_hd'] = value
            elif buff_type == BuffType.EXECUTE_EFFECT:
                boss_buff_values['boss_zs_boss'] = value
            elif buff_type == BuffType.REGENERATION:
                boss_buff_values['boss_sz'] = value

    else:
        # 如果没有匹配到任何境界配置，使用默认值
        boss_buff_values['boss_js'] = 0.0
        boss_buff_values['boss_cj'] = 0.0

    boss_buff_values['boss_sb'] = random.uniform(0.1, 0.5)

    result = []

    for key, value in boss_buff_values.items():
        if value == 0:
            continue

        if key not in boss_buff_map:
            continue

        effect_type, effect_name = boss_buff_map[key]

        is_debuff = isinstance(effect_type, DebuffType)

        result.append({
            "name": effect_name,
            "type": effect_type,
            "value": value,
            "is_debuff": is_debuff,
            "coefficient": 1,
            "duration": 99 if effect_type not in [DebuffType.HEALING_BLOCK] else value,
            "skill_type": 0
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


skill_data_cache = None


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
    skill_data = get_skill_data()
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
                data["真元"] = attr.get("mp", data.get("mp"))
                return True
    return False


# ---------- 战斗部分 ----------
class SkillType(IntEnum):
    """技能类型枚举"""
    MULTI_HIT = 1
    DOT = 2
    BUFF_STAT = 3
    CONTROL = 4
    RANDOM_HIT = 5
    STACK_BUFF = 6
    RANDOM_ACQUIRE = 7

    # ====== BOSS特殊技能 ======
    MULTIPLIER_PERCENT_HP = 101
    MULTIPLIER_DEF_IGNORE = 102
    CC = 103
    SUMMON = 104
    FIELD = 105


class TargetType(IntEnum):
    """目标类型枚举"""
    SINGLE = 1
    AOE = 2
    MULTI = 3


class BuffType(IntEnum):
    """增益效果类型枚举类"""
    ATTACK_UP = 1
    DEFENSE_UP = 2
    CRIT_RATE_UP = 3
    CRIT_DAMAGE_UP = 4
    DAMAGE_REDUCTION_UP = 5
    ARMOR_PENETRATION_UP = 6
    ACCURACY_UP = 7
    EVASION_UP = 8
    LIFESTEAL_UP = 9
    MANA_STEAL_UP = 10
    DEBUFF_IMMUNITY = 11
    HP_REGEN_PERCENT = 12
    MP_REGEN_PERCENT = 13
    REFLECT_DAMAGE = 14
    SHIELD = 15
    INVINCIBLE = 16
    SHIELD_BUFF = 17
    EXECUTE_EFFECT = 18
    REGENERATION = 19
    CHARGE_DAMAGE_UP = 20
    DIVINE_POWER_DAMAGE_UP = 21
    ALLY_SOUL_RETURN = 22 # 招魂效果：让已死亡的队友进入魂返状态
    ALLY_REVIVE_HP = 23 # 启明效果：让已死亡的队友回复生命复活


class DebuffType(IntEnum):
    """减益效果类型枚举类"""
    ATTACK_DOWN = 1
    CRIT_RATE_DOWN = 2
    CRIT_DAMAGE_DOWN = 3
    DEFENSE_DOWN = 4
    ACCURACY_DOWN = 5
    EVASION_DOWN = 6
    LIFESTEAL_DOWN = 7
    MANA_STEAL_DOWN = 8
    LIFESTEAL_BLOCK = 9
    MANA_STEAL_BLOCK = 10
    POISON_DOT = 11
    SKILL_DOT = 12
    BLEED_DOT = 13
    BURN_DOT = 14

    FATIGUE = 15
    STUN = 16
    FREEZE = 17
    PETRIFY = 18
    SLEEP = 19
    ROOT = 20
    FEAR = 21
    SEAL = 22
    PARALYSIS = 23
    SILENCE = 24
    HEALING_BLOCK = 25


buff_type_mapping = {
    1: BuffType.ATTACK_UP,
    2: BuffType.CRIT_RATE_UP,
    3: BuffType.CRIT_DAMAGE_UP,
    4: BuffType.HP_REGEN_PERCENT,
    5: BuffType.MP_REGEN_PERCENT,
    6: BuffType.LIFESTEAL_UP,
    7: BuffType.MANA_STEAL_UP,
    8: DebuffType.POISON_DOT,
    9: [BuffType.LIFESTEAL_UP, BuffType.MANA_STEAL_UP],
    10: [DebuffType.LIFESTEAL_BLOCK, DebuffType.MANA_STEAL_BLOCK],
    11: BuffType.DEBUFF_IMMUNITY,
    12: "",
    13: BuffType.ARMOR_PENETRATION_UP,
    14: BuffType.ARMOR_PENETRATION_UP
}

# 统一BUFF/DEBUFF显示模板，确保百分号在模板中
# Values are expected to be in 0-1 range for percentage-based buffs,
# or raw values for non-percentage buffs like crit damage multiplier, shield points, duration.
BUFF_DESC_TEMPLATES = {
    BuffType.ATTACK_UP: "攻击力提升 {value_display}",
    BuffType.DEFENSE_UP: "防御力提升 {value_display}",
    BuffType.CRIT_RATE_UP: "暴击率提升 {value_display}", 
    BuffType.CRIT_DAMAGE_UP: "暴击伤害提升 {value_display_raw} 倍",
    BuffType.DAMAGE_REDUCTION_UP: "伤害减免提升 {value_display}",
    BuffType.ARMOR_PENETRATION_UP: "护甲穿透提升 {value_display}",
    BuffType.ACCURACY_UP: "命中率提升 {value_display}",
    BuffType.EVASION_UP: "闪避率提升 {value_display}",
    BuffType.LIFESTEAL_UP: "生命偷取提升 {value_display}",
    BuffType.MANA_STEAL_UP: "真元偷取提升 {value_display}",
    BuffType.DEBUFF_IMMUNITY: "免疫所有减益效果",
    BuffType.HP_REGEN_PERCENT: "每回合回复最大生命 {value_display}",
    BuffType.MP_REGEN_PERCENT: "每回合回复最大真元 {value_display}",
    BuffType.REFLECT_DAMAGE: "受到伤害时反弹 {value_display}",
    BuffType.SHIELD: "获得 {value_display_raw} 点护盾",
    BuffType.INVINCIBLE: "获得无敌效果",
    BuffType.SHIELD_BUFF: "获得 {value_display} 的护盾",
    BuffType.EXECUTE_EFFECT: "拥有斩杀效果 (血量低于 {value_display} 直接斩杀)",
    BuffType.REGENERATION: "每回合回复最大生命 {value_display}",
    BuffType.CHARGE_DAMAGE_UP: "攻击力额外提升 {value_display}",
    BuffType.DIVINE_POWER_DAMAGE_UP: "攻击力额外提升 {value_display}",
    BuffType.ALLY_SOUL_RETURN: "招魂，使死亡队友进入魂返状态", # 新增招魂描述
    BuffType.ALLY_REVIVE_HP: "启明，使死亡队友回复生命复活", # 新增启明描述
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

    DebuffType.POISON_DOT: "中毒，每回合损失当前生命 {value_display}",
    DebuffType.SKILL_DOT: "持续受到 {value_display_raw} 倍攻击的技能伤害",
    DebuffType.BLEED_DOT: "流血，每回合损失最大生命 {value_display}",
    DebuffType.BURN_DOT: "灼烧，每回合损失最大生命 {value_display}",

    DebuffType.FATIGUE: "疲劳，攻击力降低 {value_display}，剩余 {value_display_raw} 回合",
    DebuffType.STUN: "眩晕，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.FREEZE: "冰冻，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.PETRIFY: "石化，无法行动，被攻击伤害减免 {value_display}，剩余 {value_display_raw} 回合",
    DebuffType.SLEEP: "睡眠，无法行动，被攻击或 {value_display_raw} 回合后苏醒",
    DebuffType.ROOT: "被定身，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.FEAR: "陷入恐惧，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.SEAL: "被封印，无法使用技能，剩余 {value_display_raw} 回合",
    DebuffType.PARALYSIS: "麻痹，无法行动，剩余 {value_display_raw} 回合",
    DebuffType.SILENCE: "被沉默，无法施放神通，剩余 {value_display_raw} 回合",
    DebuffType.HEALING_BLOCK: "被禁疗，无法回复生命或真元，剩余 {value_display_raw} 回合",
}

VALID_FIELDS = {"name", "type", "value", "coefficient", "is_debuff", "duration", "skill_type"}


class StatusEffect:
    """
    表示战斗中的一个状态效果（Buff或Debuff）。
    """
    def __init__(self, name, effect_type, value, coefficient, is_debuff, duration=99, skill_type=0):
        self.name = name
        self.type = effect_type
        self.value = value
        self.coefficient = coefficient
        self.is_debuff = is_debuff
        self.duration = duration
        self.skill_type = skill_type

    def __repr__(self):
        return f"[{'Debuff' if self.is_debuff else 'Buff'}:{self.name}|{self.type}|{self.value}|{self.duration}|{self.skill_type}]"


class Skill:
    """
    表示战斗中的一个技能。
    """
    def __init__(self, data):
        self.name = data.get("name")
        self.desc = data.get("desc", "")
        self.skill_type = int(data.get("skill_type", 1))
        self.target_type = int(data.get("target_type", 1))
        self.multi_count = int(data.get("multi_count", 1))
        self.hp_condition = float(data.get("hp_condition", 1))

        # 消耗
        self.hp_cost_rate = float(data.get("hpcost", 0))
        self.mp_cost_rate = float(data.get("mpcost", 0))

        # 通用参数
        self.turn_cost = int(data.get("turncost", 0))
        self.rate = float(data.get("rate", 100))
        self.cd = float(data.get("cd", 0))
        self.remain_cd = float(data.get("remain_cd", 0))

        # 类型特定参数
        self.atk_values = data.get("atkvalue", [])
        self.atk_coefficient = float(data.get("atkvalue2", 0))
        self.skill_buff_type = int(data.get("bufftype", 0))
        self.skill_buff_value = float(data.get("buffvalue", 0))
        self.success_rate = float(data.get("success", 0))
        self.skill_content = data.get("skill_content", [])

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
        self.data = data
        self.id = data.get("user_id")
        self.name = data.get("nickname", "Unknown")
        self.team_id = team_id
        self.is_boss = is_boss
        self.type = data.get("monster_type", "player")

        # 基础属性
        self.max_hp = float(data.get("max_hp", 1))
        self.hp = float(data.get("current_hp", 1))
        self.max_mp = float(data.get("max_mp", 1))
        self.mp = float(data.get("current_mp", 1))
        self.mp_cost_modifier = float(data.get("mp_cost_modifier", 0))
        self.exp = float(data.get("exp", 1))
        self.boss_damage = float(data.get("boss_damage_bonus", 0))

        self.shield = 0.0
        self.invincible_count = 0
        self.is_charging_turn = False
        self.charge_bonus = 0.0
        self.is_nirvana_state = False # 是否处于涅槃状态 (假死但未被移除)
        self.nirvana_revive_turn = 0 # 涅槃状态剩余回合
        self.is_soul_return_state = False # 是否处于魂返状态 (假死但未被移除)
        self.soul_return_revive_turn = 0 # 魂返状态剩余回合


        # 进阶属性
        self.base_atk = float(data.get("attack", 1))
        self.base_crit = float(data.get("critical_rate", 0))
        self.base_crit_dmg = float(data.get("critical_damage", 1.5))
        self.base_damage_reduction = float(data.get("damage_reduction", 0))
        self.base_armor_pen = float(data.get("armor_penetration", 0))
        self.base_accuracy = float(data.get("accuracy", 100))
        self.base_dodge = float(data.get("dodge", 0))
        self.base_speed = float(data.get("speed", 10))

        # 状态管理
        self.buffs = []
        self.debuffs = []
        self.start_skills = data.get("start_skills", [])
        self.skills = data.get("skills", [])
        self.total_dmg = 0

        # 本命法宝
        self.natal = None
        self.natal_data = None
        if not is_boss and self.id and self.id != 0:
            self.natal = NatalTreasure(self.id)
            if self.natal.exists():
                self.natal_data = self.natal.get_data()
                self.fate_revive_count = self.natal_data.get("fate_revive_count", 0)
                self.immortal_revive_count = self.natal_data.get("immortal_revive_count", 0)
                self.invincible_count = self.natal_data.get("invincible_gain_count", 0)
                self.nirvana_revive_count = self.natal_data.get("nirvana_revive_count", 0)
                self.soul_return_revive_count = self.natal_data.get("soul_return_revive_count", 0)
                self.natal_charge_status = self.natal_data.get("charge_status", 0)
                self.soul_summon_counts = self.natal_data.get("soul_summon_count", {}) # 招魂计数，key为队友user_id
                self.enlightenment_counts = self.natal_data.get("enlightenment_count", {}) # 启明计数，key为队友user_id


    # ======================
    #   本命法宝相关效果
    # ======================
    def apply_natal_periodic_effect(self, battle):
        """
        战斗中周期性触发本命法宝效果（每4回合，或首回合）。
        包括破甲、闪避、护盾、无敌、神力的施加/刷新。
        :param battle: BattleSystem实例，用于发送战斗消息。
        """
        if not self.natal_data:
            return

        name = self.natal_data.get("name", "本命法宝")
        
        enemies = battle._get_all_enemies(self)

        # 1. 施加/刷新效果 (破甲, 闪避, 护盾, 无敌, 神力)
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if not etype_val or etype_val <= 0:
                continue

            etype = NatalEffectType(etype_val)
            effect_name = EFFECT_NAME_MAP.get(etype, "未知效果")
            
            # 跳过非周期性或不需要主动施加的效果
            if etype in [NatalEffectType.BLEED, NatalEffectType.FATE, NatalEffectType.IMMORTAL, NatalEffectType.DEATH_STRIKE,
                         NatalEffectType.SHIELD_BREAK, NatalEffectType.REFLECT_DAMAGE, NatalEffectType.TRUE_DAMAGE,
                         NatalEffectType.CRIT_RESIST, NatalEffectType.TWIN_STRIKE,
                         NatalEffectType.SLEEP, NatalEffectType.PETRIFY, NatalEffectType.STUN,
                         NatalEffectType.FATIGUE, NatalEffectType.SILENCE, NatalEffectType.CHARGE,
                         NatalEffectType.NIRVANA, NatalEffectType.SOUL_RETURN, NatalEffectType.DIVINE_POWER,
                         NatalEffectType.SOUL_SUMMON, NatalEffectType.ENLIGHTENMENT]:
                continue


            # 破甲效果 (DebuffType.DEFENSE_DOWN)
            elif etype == NatalEffectType.ARMOR_BREAK:
                if not enemies: continue
                value = self.natal.get_effect_value(NatalEffectType.ARMOR_BREAK)
                for enemy in enemies:
                    existing_debuff = enemy.get_debuff("name", f"{name}·{effect_name}")
                    if existing_debuff:
                        existing_debuff.duration = 99
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
                    existing_buff.duration = 99
                else:
                    effect = StatusEffect(
                        name=f"{name}·{effect_name}", effect_type=BuffType.EVASION_UP,
                        value=value, coefficient=1, is_debuff=False, duration=99
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
                gain_chance = self.natal.get_effect_value(NatalEffectType.INVINCIBLE, self.natal_data.get("level", 0), True)
                
                if random.random() < gain_chance:
                    if self.invincible_count < INVINCIBLE_COUNT_LIMIT:
                        self.invincible_count += 1
                        battle.add_message(self, f"✨『{name}』【{effect_name}】凝聚成功，获得一次无敌效果！(当前拥有{self.invincible_count}次)")
                    else:
                        battle.add_message(self, f"→ 无法凝聚更多『{name}』【{effect_name}】，无敌次数已达上限！(当前拥有{self.invincible_count}次)")

    def apply_natal_bleed_proc(self, battle):
        """
        处理本命法宝的流血效果（每回合概率触发）。
        :param battle: BattleSystem实例，用于发送战斗消息。
        """
        if not self.natal_data:
            return

        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if not etype_val or NatalEffectType(etype_val) != NatalEffectType.BLEED:
                continue

            # 每回合有25%概率触发流血
            if random.random() > 0.25:
                continue
                
            enemies = battle._get_all_enemies(self)
            if not enemies:
                return
            
            target = random.choice(enemies)
            name = self.natal_data.get("name", "本命法宝")
            effect_name = EFFECT_NAME_MAP[NatalEffectType.BLEED]
            
            # 获取当前目标身上的流血层数
            bleed_debuffs = target.get_debuffs("name", f"{name}·{effect_name}")
            
            value = self.natal.get_effect_value(NatalEffectType.BLEED)
            
            if len(bleed_debuffs) < 3: # 最多叠加3层
                effect = StatusEffect(
                    name=f"{name}·{effect_name}", effect_type=DebuffType.BLEED_DOT,
                    value=value, coefficient=1, is_debuff=True, duration=3
                )
                target.add_status(effect)
                battle.add_message(self, f"→ 『{name}』 对 {target.name} 施加了一层【流血】！(当前{len(bleed_debuffs) + 1}层)")
            else:
                # 刷新持续时间最久的流血层数
                bleed_debuffs.sort(key=lambda s: s.duration)
                bleed_debuffs[0].duration = 3
                battle.add_message(self, f"→ 『{name}』 刷新了 {target.name} 的一层【流血】效果！(仍为{len(bleed_debuffs)}层)")

    def check_for_natal_charge_effect(self):
        """
        检查实体是否拥有蓄力效果。
        :return: 蓄力效果的数值 (伤害提升百分比)，如果没有则返回 0.0。
        """
        if not self.natal_data:
            return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.CHARGE:
                return self.natal.get_effect_value(NatalEffectType.CHARGE)
        return 0.0
    
    def init_charge_status(self):
        """
        初始化蓄力状态，将Entity的is_charging_turn与natal_charge_status同步。
        这个方法在回合开始时调用，确保Entity的瞬时状态与数据库持久状态一致。
        """
        if self.natal_charge_status == 1:
            self.charge_bonus = CHARGE_BONUS_DAMAGE + self.check_for_natal_charge_effect()
            self.natal_charge_status = 2
            self.is_charging_turn = False
        elif self.natal_charge_status == 2:
            self.charge_bonus = 0.0
            self.natal_charge_status = 0
            self.is_charging_turn = False
        else:
            self.charge_bonus = 0.0
            self.is_charging_turn = False


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
        for skill in self.skills[:]:
            skill.tick_cd()

        expired_debuff_messages = []

        # 检查Buff并移除过期的
        for buff in self.buffs[:]:
            buff.duration -= 1
            if buff.duration < 0:
                self.buffs.remove(buff)

        # 检查Debuff并移除过期的，同时收集移除信息
        for debuff in self.debuffs[:]:
            debuff.duration -= 1
            if debuff.duration < 0:
                # 根据Debuff类型生成不同的过期消息
                if debuff.type == DebuffType.SLEEP:
                    expired_debuff_messages.append(f"💤{self.name}从睡眠中苏醒！")
                elif debuff.type == DebuffType.PETRIFY:
                    expired_debuff_messages.append(f"🗿{self.name}解除了石化状态！")
                elif debuff.type == DebuffType.STUN:
                    expired_debuff_messages.append(f"🌀{self.name}从眩晕中恢复！")
                elif debuff.type == DebuffType.FATIGUE:
                    expired_debuff_messages.append(f"😫{self.name}解除了疲劳状态！")
                elif debuff.type == DebuffType.SILENCE:
                    expired_debuff_messages.append(f"🔇{self.name}解除了沉默状态！")
                elif debuff.type == DebuffType.HEALING_BLOCK:
                    expired_debuff_messages.append(f"✅{self.name}解除了禁疗状态！")
                self.debuffs.remove(debuff)
            
        return expired_debuff_messages


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

        return None

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

        return None

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
        return False

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
        return False

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
                    multiplier *= (1 - d.value)

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
        
        # 禁疗Debuff检查 (只禁止HP/MP的增加操作)
        if self.has_debuff("type", DebuffType.HEALING_BLOCK) and op == 1:
            return

        # 获取当前值和最大值
        current = getattr(self, stat)
        max_value = getattr(self, f"max_{stat}")

        if stat == "mp" or op == 1 or bypass_shield: # MP, 增加操作, 或绕过护盾的HP扣减
            if op == 1:
                current += value
                current = min(current, max_value) # 增加不能超过最大值
            elif op == 2:
                current -= value
            setattr(self, stat, current)
            return
        
        # HP扣减且不绕过护盾
        absorbed = 0
        if self.shield > 0:
            absorbed = min(value, self.shield)
            self.shield -= absorbed
            value -= absorbed # 剩余伤害

        if value > 0:
            self.hp -= value # 实际扣减HP
        
        self.shield = max(0, self.shield) # 护盾不能为负


    def pay_cost(self, hp_cost, mp_cost, deduct=False):
        """
        支付技能消耗。
        :param hp_cost: 气血消耗。
        :param mp_cost: 真元消耗。
        :param deduct: 是否实际扣除资源。
        :return: True如果可以支付，False否则。
        """
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
        max_value = getattr(self, f"max_{stat}")

        display_current = max(0, min(current_data, max_value))
        
        ratio = display_current / max_value if max_value > 0 else 0
        filled = int(ratio * length)
        empty = length - filled
        bar = "▬" * filled + "▭" * empty
        
        return f"{self.name}剩余{number_to(int(display_current))}\n{stat.upper()} {bar} {int(ratio * 100)}%"


    @property
    def is_alive(self):
        """判断实体是否存活（包括涅槃/魂返状态）。"""
        return self.hp > 0 or self.is_nirvana_state or self.is_soul_return_state

    @property
    def is_truly_alive(self):
        """判断实体是否真正存活 (非涅槃/魂返状态)。"""
        return self.hp > 0

    @property
    def atk_rate(self):
        """计算最终攻击力。"""
        pct = self._get_effect_value(BuffType.ATTACK_UP, DebuffType.ATTACK_DOWN)
        
        # 疲劳Debuff
        if self.has_debuff("type", DebuffType.FATIGUE):
            pct -= FATIGUE_ATTACK_REDUCTION

        # 蓄力加成 (蓄力成功后，只在爆发回合有加成)
        if self.charge_bonus > 0:
            pct += self.charge_bonus
        
        # 神力加成
        if self.natal_data:
            divine_power_bonus = 0
            for i in [1, 2, 3]:
                etype_val = self.natal_data.get(f"effect{i}_type")
                if etype_val and NatalEffectType(etype_val) == NatalEffectType.DIVINE_POWER:
                    divine_power_bonus = self.natal.get_effect_value(NatalEffectType.DIVINE_POWER)
                    break
            pct += divine_power_bonus

        return max(0, self.base_atk * (1 + pct))

    @property
    def crit_rate(self):
        """计算最终暴击率。"""
        val = self.base_crit + self._get_effect_value(BuffType.CRIT_RATE_UP, DebuffType.CRIT_RATE_DOWN)
        return max(0, val)

    @property
    def crit_dmg_rate(self):
        """计算最终暴击伤害倍数。"""
        val = self.base_crit_dmg + self._get_effect_value(BuffType.CRIT_DAMAGE_UP, DebuffType.CRIT_DAMAGE_DOWN)
        return max(0, val)

    @property
    def damage_reduction_rate(self):
        """计算最终伤害减免率。"""
        val = self.base_damage_reduction + self._get_effect_value(BuffType.DAMAGE_REDUCTION_UP, DebuffType.DEFENSE_DOWN)
        
        # 石化Debuff额外减伤
        if self.has_debuff("type", DebuffType.PETRIFY):
            val += PETRIFY_DAMAGE_REDUCTION_PERCENT

        return min(0.95, max(-1, val))

    @property
    def armor_pen_rate(self):
        """计算最终护甲穿透率。"""
        natal_armor_break = 0.0
        if self.natal and self.natal_data:
            for i in [1, 2, 3]:
                etype_val = self.natal_data.get(f"effect{i}_type")
                if etype_val and NatalEffectType(etype_val) == NatalEffectType.ARMOR_BREAK:
                    natal_armor_break = self.natal.get_effect_value(NatalEffectType.ARMOR_BREAK)
                    break
        val = self.base_armor_pen + self._get_effect_value(BuffType.ARMOR_PENETRATION_UP) + natal_armor_break
        return max(0, val)

    @property
    def accuracy_rate(self):
        """计算最终命中率。"""
        val = self.base_accuracy + self._get_effect_value(BuffType.ACCURACY_UP, DebuffType.ACCURACY_DOWN)
        return max(0, val)

    @property
    def dodge_rate(self):
        """计算最终闪避率。"""
        natal_evasion = 0.0
        if self.natal and self.natal_data:
            for i in [1, 2, 3]:
                etype_val = self.natal_data.get(f"effect{i}_type")
                if etype_val and NatalEffectType(etype_val) == NatalEffectType.EVASION:
                    natal_evasion = self.natal.get_effect_value(NatalEffectType.EVASION)
                    break
        val = self.base_dodge + self._get_effect_value(BuffType.EVASION_UP, DebuffType.EVASION_DOWN) + natal_evasion * 100 
        return min(180, max(0, val))

    @property
    def lifesteal_rate(self):
        """计算最终生命偷取率。"""
        if self.has_debuff("type", DebuffType.LIFESTEAL_BLOCK):
            return 0
        val = self._get_effect_value_mixed(BuffType.LIFESTEAL_UP, DebuffType.LIFESTEAL_DOWN)
        return max(0, val)

    @property
    def mana_steal_rate(self):
        """计算最终法力偷取率。"""
        if self.has_debuff("type", DebuffType.MANA_STEAL_BLOCK):
            return 0
        val = self._get_effect_value_mixed(BuffType.MANA_STEAL_UP, DebuffType.MANA_STEAL_DOWN)
        return max(0, val)
    
    @property
    def shield_break_rate(self):
        """本命法宝的破盾效果 (攻击时无视部分护盾)。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.SHIELD_BREAK:
                return self.natal.get_effect_value(NatalEffectType.SHIELD_BREAK)
        return 0.0
    
    @property
    def shield_break_bonus_damage(self):
        """本命法宝的破盾效果 (攻击时额外伤害)。"""
        if self.natal and self.natal_data and any(self.natal_data.get(f"effect{i}_type") == NatalEffectType.SHIELD_BREAK.value for i in [1, 2, 3]):
            return SHIELD_BREAK_BONUS_DAMAGE
        return 0.0

    @property
    def reflect_damage_rate(self):
        """本命法宝的反伤效果 (受到攻击时反弹)。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.REFLECT_DAMAGE:
                return self.natal.get_effect_value(NatalEffectType.REFLECT_DAMAGE)
        return 0.0
    
    @property
    def true_damage_bonus(self):
        """本命法宝的真伤效果 (攻击时额外造成真实伤害)。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.TRUE_DAMAGE:
                return self.natal.get_effect_value(NatalEffectType.TRUE_DAMAGE)
        return 0.0
    
    @property
    def crit_resist_rate(self):
        """本命法宝的抗暴效果 (减少被暴击伤害)。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.CRIT_RESIST:
                return self.natal.get_effect_value(NatalEffectType.CRIT_RESIST)
        return 0.0

    @property
    def fate_revive_chance(self):
        """本命法宝的天命复活概率。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.FATE:
                return self.natal.get_effect_value(NatalEffectType.FATE)
        return 0.0
    
    @property
    def immortal_revive_hp_percent(self):
        """本命法宝的不灭复活血量百分比。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.IMMORTAL:
                return self.natal.get_effect_value(NatalEffectType.IMMORTAL)
        return 0.0

    @property
    def death_strike_threshold(self):
        """本命法宝的斩命触发血量阈值。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.DEATH_STRIKE:
                return self.natal.get_effect_value(NatalEffectType.DEATH_STRIKE)
        return 0.0
        
    @property
    def has_death_strike(self):
        """判断是否拥有斩命效果。"""
        if not self.natal or not self.natal_data: return False
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.DEATH_STRIKE:
                return True
        return False
        
    @property
    def has_fate_effect(self):
        """判断是否拥有天命效果。"""
        if not self.natal or not self.natal_data: return False
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.FATE:
                return True
        return False

    @property
    def twin_strike_effect(self) -> tuple[float, float] | None:
        """本命法宝的双生效果 (触发概率, 伤害倍率)。"""
        if not self.natal or not self.natal_data:
            return None
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.TWIN_STRIKE:
                return self.natal.get_effect_value(NatalEffectType.TWIN_STRIKE)
        return None

    @property
    def sleep_chance(self):
        """本命法宝睡眠效果的触发概率。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.SLEEP:
                return self.natal.get_effect_value(NatalEffectType.SLEEP)
        return 0.0

    @property
    def petrify_chance(self):
        """本命法宝石化效果的触发概率。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.PETRIFY:
                return self.natal.get_effect_value(NatalEffectType.PETRIFY)
        return 0.0

    @property
    def stun_chance(self):
        """本命法宝眩晕效果的触发概率。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.STUN:
                return self.natal.get_effect_value(NatalEffectType.STUN)
        return 0.0

    @property
    def fatigue_chance(self):
        """本命法宝疲劳效果的触发概率。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.FATIGUE:
                return self.natal.get_effect_value(NatalEffectType.FATIGUE)
        return 0.0

    @property
    def silence_chance(self):
        """本命法宝沉默效果的触发概率。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.SILENCE:
                return self.natal.get_effect_value(NatalEffectType.SILENCE)
        return 0.0
    
    @property
    def nirvana_effect(self):
        """本命法宝涅槃效果的护盾加成。"""
        if not self.natal or not self.natal_data: return None
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.NIRVANA:
                return self.natal.get_effect_value(NatalEffectType.NIRVANA)
        return None
    
    @property
    def soul_return_effect(self):
        """本命法宝魂返效果的生命回复加成。"""
        if not self.natal or not self.natal_data: return None
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.SOUL_RETURN:
                return self.natal.get_effect_value(NatalEffectType.SOUL_RETURN)
        return None

    @property
    def soul_summon_chance(self):
        """本命法宝招魂效果的触发概率。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.SOUL_SUMMON:
                return self.natal.get_effect_value(NatalEffectType.SOUL_SUMMON)
        return 0.0
    
    @property
    def enlightenment_chance(self):
        """本命法宝启明效果的触发概率。"""
        if not self.natal or not self.natal_data: return 0.0
        for i in [1, 2, 3]:
            etype_val = self.natal_data.get(f"effect{i}_type")
            if etype_val and NatalEffectType(etype_val) == NatalEffectType.ENLIGHTENMENT:
                return self.natal.get_effect_value(NatalEffectType.ENLIGHTENMENT)
        return 0.0

    @property
    def bleed_dot_dmg_list(self):
        """返回所有流血伤害的列表 (每层流血造成的伤害)。"""
        damages = []
        for debuff in self.debuffs:
            if debuff.type == DebuffType.BLEED_DOT:
                base_bleed_dmg = self.max_hp * debuff.value
                final_bleed_dmg = base_bleed_dmg * (1 - self.damage_reduction_rate)
                damages.append(int(final_bleed_dmg))
        return damages

    @property
    def poison_dot_dmg(self):
        """所有中毒伤害的总和（基于当前生命值）。"""
        total = 0.0
        for debuff in self.debuffs:
            if debuff.type == DebuffType.POISON_DOT:
                total += self.hp * debuff.value
        return int(total)

    @property
    def hp_regen_rate(self):
        """所有HP恢复的总和（基于最大生命值）。"""
        total = 0.0
        for buff in self.buffs:
            if buff.type == BuffType.HP_REGEN_PERCENT:
                total += self.max_hp * buff.value
        # BOSS再生效果
        if self.has_buff("type", BuffType.REGENERATION):
            regen_buff = self.get_buff("type", BuffType.REGENERATION)
            total += self.max_hp * regen_buff.value
        
        return int(total)

    @property
    def mp_regen_rate(self):
        """所有MP恢复的总和（基于最大真元值）。"""
        total = 0.0
        for buff in self.buffs:
            if buff.type == BuffType.MP_REGEN_PERCENT:
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
        self.bot_id = bot_id
        self.team_a = team_a
        self.team_b = team_b
        self.play_list = []
        self.round = 0
        self.max_rounds = 50
        self.last_status_messages = {}


    def add_message(self, unit, message):
        """
        添加战斗消息到日志。
        :param unit: 发送消息的实体。
        :param message: 消息内容。
        """
        if not message.strip():
            return

        msg_dict = {
            "type": "node",
            "data": {
                "name": unit.name,
                "uin": int(unit.id) if unit.id else 0,
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
        if absorbed_damage > 0:
            msg_dict = {
                "type": "node",
                "data": {
                    "name": defender.name,
                    "uin": int(defender.id) if defender.id else 0,
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
        unit_id = unit.id if unit.id else unit.name
        current_status_msg = ""
        
        if unit.is_soul_return_state:
            if unit.soul_return_revive_turn > 0:
                current_status_msg = f"👻{unit.name}处于灵体状态，免疫所有伤害！(魂返剩余{unit.soul_return_revive_turn}回合)"
        
        elif unit.is_nirvana_state:
            if unit.nirvana_revive_turn > 0:
                current_status_msg = f"💀{unit.name}处于涅槃状态，免疫所有伤害！(涅槃剩余{unit.nirvana_revive_turn}回合)"
        
        elif unit.is_truly_alive: # 真正存活才显示血条和护盾/无敌
            hp_bar = unit.show_bar("hp")

            shield_info = f"护盾:{number_to(int(unit.shield))}" if unit.shield > 0 else ""
            invincible_info = f"无敌:{unit.invincible_count}" if unit.invincible_count > 0 else ""
            
            extra_info_parts = [info for info in [shield_info, invincible_info] if info]
            extra_info = f" | {' | '.join(extra_info_parts)}" if extra_info_parts else ""
            
            current_status_msg = f"{hp_bar}{extra_info}"
        
        # 只有状态消息实际改变了才添加，避免刷屏
        if current_status_msg.strip() and self.last_status_messages.get(unit_id) != current_status_msg:
            self.play_list.append({
                "type": "node",
                "data": {
                    "name": unit.name,
                    "uin": int(unit.id) if unit.id else 0,
                    "content": current_status_msg
                }
            })
            self.last_status_messages[unit_id] = current_status_msg


    def add_system_message(self, message):
        """
        添加系统消息到日志。
        :param message: 系统消息内容。
        """
        msg_dict = {
            "type": "node",
            "data": {
                "name": "系统",
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
        
        template = ""
        if is_debuff:
            template = DEBUFF_DESC_TEMPLATES.get(effect_type, "未知减益")
        else:
            template = BUFF_DESC_TEMPLATES.get(effect_type, "未知增益")
        
        value_display_raw = f"{val:.2f}" if val != int(val) else f"{int(val)}"

        value_display = ""
        if effect_type in {BuffType.CRIT_DAMAGE_UP, DebuffType.CRIT_DAMAGE_DOWN,
                           DebuffType.SKILL_DOT,
                           BuffType.SHIELD, BuffType.SHIELD_BUFF,
                           DebuffType.FATIGUE, DebuffType.STUN, DebuffType.FREEZE, DebuffType.PETRIFY,
                           DebuffType.SLEEP, DebuffType.ROOT, DebuffType.FEAR, DebuffType.SEAL,
                           DebuffType.PARALYSIS, DebuffType.SILENCE, DebuffType.HEALING_BLOCK
                          }:
            if effect_type == DebuffType.FATIGUE:
                value_display = f"{FATIGUE_ATTACK_REDUCTION*100}%"
            elif effect_type == DebuffType.PETRIFY:
                value_display = f"{PETRIFY_DAMAGE_REDUCTION_PERCENT*100}%"
            elif effect_type == DebuffType.SLEEP:
                value_display = f"{value_display_raw}"
            else:
                value_display = value_display_raw
        elif effect_type in {BuffType.ALLY_SOUL_RETURN, BuffType.ALLY_REVIVE_HP}: # 招魂/启明不需要百分比显示
             value_display = ""
        else:
            display_val = val * 100
            if display_val == int(display_val):
                value_display = f"{int(display_val)}%"
            else:
                value_display = f"{display_val:.1f}%"
        
        return template.format(value_display=value_display, value_display_raw=value_display_raw)

    def add_after_last_damage(self, msg, add_text):
        """
        在最后一个"伤害！"后面添加指定字符串。
        :param msg: 原始消息。
        :param add_text: 需要添加的文本。
        :return: 修改后的消息。
        """
        before_last, separator, after_last = msg.rpartition("伤害！")

        if separator:
            return before_last + "伤害！" + add_text + after_last
        else:
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
        # 魂返状态下，免疫所有伤害
        if defender.is_soul_return_state:
            return 0, 0, False, "Immune"
        # 涅槃状态下，免疫所有伤害
        if defender.is_nirvana_state:
            return 0, 0, False, "Immune"

        # 命中判定
        status = "Hit"
        # 闪避率最高不超过180%
        if random.uniform(0, 100) > (attacker.accuracy_rate - min(defender.dodge_rate, 180)):
            status = "Miss"

        # 暴击判定
        is_crit = random.random() < attacker.crit_rate
        
        # 计算暴击伤害倍数
        crit_mult = attacker.crit_dmg_rate if is_crit else 1.0
        
        # 应用防御方的抗暴效果 (NatalEffectType.CRIT_RESIST)
        if is_crit and defender.natal_data and defender.crit_resist_rate > 0:
            crit_resist = defender.crit_resist_rate
            crit_mult *= (1 - crit_resist)
            if crit_resist > 0:
                self.add_message(defender, f"『{defender.natal_data.get('name','本命法宝')}』触发抗暴，减少了暴击伤害！")

        current_dr = defender.damage_reduction_rate
        
        if penetration:
            dr_eff = 0
        else:
            dr_eff = max(0, current_dr - attacker.armor_pen_rate)
        
        # 伤害公式: 攻击 * 倍率 * 暴击 * (1 - 有效减伤率)
        damage = attacker.atk_rate * multiplier * crit_mult * (1 - dr_eff)

        # BOSS加成
        if defender.is_boss:
            damage *= (1 + attacker.boss_damage)
            
        # 本命法宝真伤 (NatalEffectType.TRUE_DAMAGE)
        true_damage = 0
        if attacker.natal_data and attacker.true_damage_bonus > 0:
            true_damage_rate = attacker.true_damage_bonus
            if true_damage_rate > 0:
                true_damage = attacker.atk_rate * true_damage_rate * multiplier

        # 本命法宝破盾额外伤害 (NatalEffectType.SHIELD_BREAK)
        if attacker.natal_data and attacker.shield_break_bonus_damage > 0 and defender.shield > 0:
            bonus_damage_amount = attacker.atk_rate * attacker.shield_break_bonus_damage * multiplier
            damage += bonus_damage_amount

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
        
        # 魂返状态下，免疫所有伤害
        if defender.is_soul_return_state:
            return 0, 0, 0
        # 涅槃状态下，免疫所有伤害
        if defender.is_nirvana_state:
            return 0, 0, 0

        # --- 无敌效果判定 (NatalEffectType.INVINCIBLE) ---
        if defender.invincible_count > 0:
            self.add_message(defender, f"✨『{defender.natal_data.get('name','本命法宝')}』触发【无敌】，本次伤害完全免疫！(剩余无敌次数: {defender.invincible_count - 1})")
            defender.invincible_count -= 1
            if defender.natal_data: defender.natal_data["invincible_gain_count"] = defender.invincible_count
            return 0, 0, 0

        # 1. 优先处理护盾抵挡和破盾效果
        absorbed_by_shield = 0
        damage_to_be_absorbed = raw_damage_value
        
        if defender.shield > 0 and caster and caster.natal_data and caster.shield_break_rate > 0:
            ignored_shield_amount = damage_to_be_absorbed * caster.shield_break_rate
            self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』触发破盾，无视了 {number_to(int(ignored_shield_amount))} 点护盾！")
            damage_to_be_absorbed -= ignored_shield_amount
            
        if defender.shield > 0 and damage_to_be_absorbed > 0:
            absorbed_by_shield = min(damage_to_be_absorbed, defender.shield)
            defender.shield -= absorbed_by_shield
            raw_damage_value -= absorbed_by_shield
            
        defender.shield = max(0, defender.shield)
        
        # 2. 实际对HP造成的伤害 (普通伤害 + 真实伤害)
        final_hp_damage = int(raw_damage_value + true_damage_value)
        
        # 本命法宝真伤 (NatalEffectType.TRUE_DAMAGE) 消息
        if caster and caster.natal_data and caster.true_damage_bonus > 0 and true_damage_value > 0:
            self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』触发真伤，额外造成 {number_to(int(true_damage_value))} 真实伤害！")

        # 3. 反伤效果 (defender.reflect_damage_rate)
        reflected_damage = 0
        if defender.natal_data and defender.reflect_damage_rate > 0:
            reflected_damage = int(final_hp_damage * defender.reflect_damage_rate)
            if reflected_damage > 0:
                self.add_message(defender, f"『{defender.natal_data.get('name','本命法宝')}』触发反伤，反弹 {number_to(reflected_damage)} 真实伤害！")

        # 4. 更新目标HP
        if final_hp_damage > 0:
            defender.hp -= final_hp_damage # 直接扣血，不通过update_stat，因为后面要检查是否致死
            if caster and caster.id:
                caster.total_dmg += final_hp_damage

        # 5. 记录护盾吸收日志
        self.add_shield_log(defender, absorbed_by_shield)
            
        return final_hp_damage, int(absorbed_by_shield), reflected_damage

    def _check_and_apply_revive_effects(self, defender, attacker_has_death_strike):
        """
        检查并应用复活类效果 (天命, 不灭, 涅槃, 魂返)。
        :param defender: 防御者实体。
        :param attacker_has_death_strike: 攻击者是否拥有斩命效果。
        :return: True 如果目标复活, False 否则。
        """
        # 魂返状态下，不能重复触发涅槃/魂返
        if defender.is_soul_return_state:
            return False
        # 涅槃状态下，不能重复触发涅槃/魂返
        if defender.is_nirvana_state:
            return False

        # 检查是否有队友存活，涅槃和魂返需要队友在场
        # is_truly_alive 可以是自身，也可以是队友，但这里涅槃魂返自身已经hp <= 0，所以只检查队友
        has_alive_allies = any(u.is_truly_alive for u in self._get_all_allies(defender)) 
        
        # --- 涅槃效果判定 (NatalEffectType.NIRVANA) ---
        if defender.natal_data and defender.nirvana_effect is not None \
            and defender.nirvana_revive_count < NIRVANA_REVIVE_LIMIT and has_alive_allies:
            defender.is_nirvana_state = True
            defender.nirvana_revive_turn = NIRVANA_DURATION
            defender.nirvana_revive_count += 1
            if defender.natal_data: defender.natal_data["nirvana_revive_count"] = defender.nirvana_revive_count
            self.add_message(defender, f"✨『{defender.natal_data.get('name','本命法宝')}』触发【涅槃】，{defender.name}进入涅槃状态，{NIRVANA_DURATION}回合后满血复活！(已使用{defender.nirvana_revive_count}/{NIRVANA_REVIVE_LIMIT}次)")
            defender.hp = 1 # 保证HP大于0，避免被当做死亡单位，但处于假死状态
            return True

        # --- 魂返效果判定 (NatalEffectType.SOUL_RETURN) ---
        if defender.natal_data and defender.soul_return_effect is not None \
            and defender.soul_return_revive_count < SOUL_RETURN_REVIVE_LIMIT and has_alive_allies:
            defender.is_soul_return_state = True
            defender.soul_return_revive_turn = SOUL_RETURN_DURATION
            defender.soul_return_revive_count += 1
            if defender.natal_data: defender.natal_data["soul_return_revive_count"] = defender.soul_return_revive_count
            self.add_message(defender, f"👻『{defender.natal_data.get('name','本命法宝')}』触发【魂返】，{defender.name}进入灵体状态，{SOUL_RETURN_DURATION}回合后回复部分生命值复活！(已使用{defender.soul_return_revive_count}/{SOUL_RETURN_REVIVE_LIMIT}次)")
            defender.hp = 1 # 保证HP大于0，避免被当做死亡单位，但处于假死状态
            return True

        # --- 天命效果判定 (NatalEffectType.FATE) ---
        if attacker_has_death_strike and defender.has_fate_effect:
            self.add_message(defender, f"💀你的『{defender.natal_data.get('name','本命法宝')}』【天命】效果被【斩命】禁止！")
            return False

        if defender.natal_data and defender.fate_revive_chance > 0 and defender.fate_revive_count < FATE_REVIVE_COUNT_LIMIT:
            if random.random() < defender.fate_revive_chance:
                defender.hp = defender.max_hp # 恢复满血
                defender.fate_revive_count += 1
                if defender.natal_data: defender.natal_data["fate_revive_count"] = defender.fate_revive_count
                self.add_message(defender, f"✨『{defender.natal_data.get('name','本命法宝')}』触发【天命】，恢复全部生命！(已使用{defender.fate_revive_count}/{FATE_REVIVE_COUNT_LIMIT}次)")
                return True

        # --- 不灭效果判定 (NatalEffectType.IMMORTAL) ---
        if defender.natal_data and defender.immortal_revive_hp_percent > 0 and defender.immortal_revive_count < IMMORTAL_REVIVE_COUNT_LIMIT:
            # 不灭有50%固定概率触发
            if random.random() < 0.5: 
                revive_amount = defender.max_hp * defender.immortal_revive_hp_percent
                defender.hp = min(defender.max_hp, defender.hp + revive_amount)
                defender.immortal_revive_count += 1
                if defender.natal_data: defender.natal_data["immortal_revive_count"] = defender.immortal_revive_count
                self.add_message(defender, f"✨『{defender.natal_data.get('name','本命法宝')}』触发【不灭】，恢复 {number_to(int(revive_amount))} 生命！(已使用{defender.immortal_revive_count}/{IMMORTAL_REVIVE_COUNT_LIMIT}次)")
                return True

        return False

    def _check_and_apply_death_strike(self, attacker, defender, final_hp_damage):
        """
        检查并应用斩命效果 (NatalEffectType.DEATH_STRIKE) 和 BOSS斩杀。
        :param attacker: 攻击者实体。
        :param defender: 防御者实体。
        :param final_hp_damage: 本次攻击造成的最终HP伤害。(注意：这里传入的final_hp_damage是实际扣除的伤害，不是用来计算斩杀的当前血量)
        :return: True 如果目标被斩杀, False 否则。
        """
        # 魂返状态下无法被斩杀
        if defender.is_soul_return_state:
            return False
        # 涅槃状态下无法被斩杀
        if defender.is_nirvana_state:
            return False

        # BOSS斩杀效果
        boss_execute_buff = defender.get_buff("type", BuffType.EXECUTE_EFFECT)
        boss_execute_threshold = boss_execute_buff.value if boss_execute_buff else 0.0

        is_executed = False
        if boss_execute_threshold > 0 and (defender.hp / defender.max_hp) <= boss_execute_threshold:
            remaining_hp = defender.hp
            if remaining_hp > 0:
                defender.hp = 0 # 直接斩杀
                self.add_message(attacker, f"💀【{boss_execute_buff.name}】触发斩杀，对{defender.name}造成【{number_to(int(remaining_hp))}】点额外伤害！")
                is_executed = True

        # 本命法宝斩命效果
        if attacker.natal_data and attacker.has_death_strike:
            threshold = attacker.death_strike_threshold
            # 注意：这里的defender.hp已经是本次攻击后的血量
            if defender.hp <= 0 or (defender.hp / defender.max_hp) <= threshold:
                remaining_hp_to_kill = defender.hp # 可能已经 <= 0
                if remaining_hp_to_kill > 0: # 如果还有血，但低于斩杀线，直接斩杀
                    defender.hp = 0
                    self.add_message(attacker, f"💀『{attacker.natal_data.get('name','本命法宝')}』触发【斩命】，对{defender.name}造成【{number_to(int(remaining_hp_to_kill))}】点额外伤害并直接斩杀！")
                    return True
                elif defender.hp <= 0: # 已经死亡，补刀
                     self.add_message(attacker, f"💀『{attacker.natal_data.get('name','本命法宝')}』触发【斩命】，将{defender.name}直接斩杀！")
                     return True
        return is_executed

    def _get_all_enemies(self, entity):
        """
        获取指定实体的所有敌方存活单位。
        :param entity: 实体。
        :return: 敌方存活单位列表。
        """
        if entity.team_id == 0:
            return [e for e in self.team_b if e.is_alive]
        else:
            return [e for e in self.team_a if e.is_alive]

    def _get_all_allies(self, entity, include_self=False):
        """
        获取指定实体的所有友方存活单位。
        :param entity: 实体。
        :param include_self: 是否包含自身。
        :return: 友方存活单位列表。
        """
        allies = []
        if entity.team_id == 0:
            allies = [e for e in self.team_a if e.is_alive]
        else:
            allies = [e for e in self.team_b if e.is_alive]
        
        if not include_self:
            allies = [e for e in allies if e.id != entity.id]
        return allies
            
    def _get_all_truly_alive_allies(self, entity, include_self=False):
        """
        获取指定实体的所有友方真正存活单位（不包括涅槃/魂返状态）。
        :param entity: 实体。
        :param include_self: 是否包含自身。
        :return: 友方真正存活单位列表。
        """
        allies = []
        if entity.team_id == 0:
            allies = [e for e in self.team_a if e.is_truly_alive]
        else:
            allies = [e for e in self.team_b if e.is_truly_alive]
        
        if not include_self:
            allies = [e for e in allies if e.id != entity.id]
        return allies

    def _get_all_dead_allies(self, entity):
        """
        获取指定实体的所有已死亡的队友 (不包括自己，且非假死状态)。
        :param entity: 实体。
        :return: 死亡队友列表。
        """
        dead_allies = []
        if entity.team_id == 0:
            dead_allies = [e for e in self.team_a if not e.is_alive and e.id != entity.id]
        else:
            dead_allies = [e for e in self.team_b if not e.is_alive and e.id != entity.id]
        return dead_allies

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
            
            # BOSS护盾效果 (BuffType.SHIELD_BUFF)
            if b_type == BuffType.SHIELD_BUFF:
                shield_value = int(caster.max_hp * val)
                caster.shield += shield_value
                self.add_message(caster, f"{caster.name}使用{name}，为自身施加了 {number_to(shield_value)} 点护盾！")
                continue

            # BOSS斩杀效果 (BuffType.EXECUTE_EFFECT)
            if b_type == BuffType.EXECUTE_EFFECT:
                effect = StatusEffect(name, b_type, val, 1, False, duration=99)
                caster.add_status(effect)
                self.add_message(caster, f"{caster.name}使用{name}，激活斩杀效果 (血量低于 {round(val * 100, 2)}% 直接斩杀)！")
                continue

            # BOSS再生效果 (BuffType.REGENERATION)
            if b_type == BuffType.REGENERATION:
                effect = StatusEffect(name, b_type, val, 1, False, duration=99)
                caster.add_status(effect)
                self.add_message(caster, f"{caster.name}使用{name}，获得再生效果 (每回合回复最大生命 {round(val * 100, 2)}%)！")
                continue

            effect = StatusEffect(name, b_type, val, 1, is_db, duration=99 if b_type != DebuffType.HEALING_BLOCK else val, skill_type=0)
            
            if is_db:
                for target in targets:
                    target.add_status(effect)
            else:
                caster.add_status(effect)

            buff_msg = self.get_effect_desc(b_type, is_db, val)
            msg = f"{caster.name}使用{name}，{buff_msg}"
            if caster.type != "minion":
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
            if sk.skill_type == SkillType.RANDOM_ACQUIRE:
                skill_id = random.choice(sk.skill_content)
                skill_data = items.get_data_by_item_id(skill_id)
                sk_data = Skill(skill_data)
                caster.skills.append(sk_data)
                caster.remove_skill_by_name(sk.name)
                skill_data_name = skill_data["name"]
                self.add_message(caster, f"{sk.desc} 随机获得了{skill_data_name}神通!")
                if self._skill_available(caster, sk_data, enemies):
                    usable_skills.append(sk_data)
            elif self._skill_available(caster, sk, enemies):
                usable_skills.append(sk)
        if not usable_skills:
            return None

        # ---------- 触发血量类型技能优先 ----------
        not_hp1_skills = [sk for sk in usable_skills if sk.hp_condition != 1]
        if not_hp1_skills:
            return not_hp1_skills[0]

        # ---------- BUFF 技能优先 ----------
        buff_list = [sk for sk in usable_skills if sk.skill_type == SkillType.BUFF_STAT]
        if buff_list:
            return buff_list[0]

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
        mp_cost = caster.max_mp * skill.mp_cost_rate * (1 - caster.mp_cost_modifier)
        if not caster.pay_cost(hp_cost, mp_cost, deduct=False):
            return False
        
        # ---------- 4. 沉默状态无法使用神通 ----------
        if caster.has_debuff("type", DebuffType.SILENCE):
            return False

        # ---------- 5. 技能：检查是否所有敌人都已经有这个debuff ----------
        if skill.skill_type in (SkillType.DOT, SkillType.CC, SkillType.CONTROL):
            enemies_without_debuff = [e for e in enemies if not e.has_debuff("name", skill.name)]
            if not enemies_without_debuff:
                return False

        # ---------- 6. BUFF 技能：不能重复施放相同 Buff ----------
        if skill.skill_type == SkillType.BUFF_STAT or skill.skill_type == SkillType.STACK_BUFF:
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
        alive = [e for e in enemies if e.is_truly_alive] # 技能的目标必须是真正存活的单位
        if not alive: return []

        if skill.target_type == TargetType.SINGLE:
            if skill.skill_type in (SkillType.DOT, SkillType.CC, SkillType.CONTROL):
                alive_without_debuff = [e for e in alive if not e.has_debuff("name", skill.name)]
                if alive_without_debuff:
                    alive = alive_without_debuff
                if not alive: return []

            if is_boss:
                return random.sample(alive, k=1)
            return [min(alive, key=lambda x: x.hp)]


        elif skill.target_type == TargetType.AOE:
            return alive

        elif skill.target_type == TargetType.MULTI:
            if skill.skill_type in (SkillType.DOT, SkillType.CC, SkillType.CONTROL):
                alive_without_debuff = [e for e in alive if not e.has_debuff("name", skill.name)]
                if alive_without_debuff:
                    alive = alive_without_debuff
                if not alive: return []
            
            n = min(getattr(skill, 'multi_count', 2), len(alive))
            if n == 0: return []
            
            if is_boss:
                return random.sample(alive, k=n)
            return sorted(alive, key=lambda x: x.hp)[:n]

        return []

    def _execute_skill(self, caster, targets, skill):
        """
        执行一个技能。
        :param caster: 施法者实体。
        :param targets: 目标实体列表。
        :param skill: 技能实例。
        :return: 技能执行结果消息字符串，造成的总伤害。
        """
        total_dmg = 0
        skill_activated = False

        if random.uniform(0, 100) <= skill.rate:
            skill_activated = True

            # 计算消耗并扣除
            hp_cost = caster.hp * skill.hp_cost_rate
            mp_cost = caster.max_mp * skill.mp_cost_rate * (1 - caster.mp_cost_modifier)
            caster.pay_cost(hp_cost, mp_cost, deduct=True)

            parts = []
            if hp_cost > 0:
                parts.append(f"气血{number_to(int(hp_cost))}点")
            if mp_cost > 0:
                parts.append(f"真元{number_to(int(mp_cost))}点")
            
            cost_msg = f"消耗{'、'.join(parts)}，" if parts else ""
            self.add_message(caster, f"{caster.name}使用{skill.name}！{cost_msg}")
            
            skill.trigger_cd()

            # --- 核心逻辑分支 (根据SkillType处理不同技能效果) ---
            # Type 1: 连续攻击 (Multi-Hit)
            if skill.skill_type == SkillType.MULTI_HIT:
                hits = skill.atk_values if isinstance(skill.atk_values, list) else [skill.atk_values]
                if not targets: return "", 0
                target = targets[0] # 连续攻击一般只针对一个目标
                
                hit_dmgs = []
                for mult in hits:
                    dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, target, float(mult))
                    if status == "Hit":
                        hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, dmg, true_dmg)
                        
                        # 检查目标是否死亡并尝试复活
                        if target.hp <= 0:
                            revived = self._check_and_apply_revive_effects(target, caster.has_death_strike)
                            if not revived: # 如果没有复活成功，则目标真正死亡
                                killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                                if not killed_by_death_strike:
                                    self.add_message(target, f"💀{target.name}倒下了！")
                        
                        if reflected_dmg > 0:
                            self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                            caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True) # 反弹伤害绕过护盾
                        total_dmg += hp_dmg
                        crit_str = "💥" if is_crit else ""
                        hit_dmgs.append(f"{crit_str}{number_to(int(hp_dmg))}伤害")
                    else:
                        hit_dmgs.append("miss")
                
                self.add_message(caster, f"→ 对{target.name}造成" + "、".join(hit_dmgs) + "！")

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
                    effect = StatusEffect(skill.name, DebuffType.SKILL_DOT, skill.atk_values, caster.name, True, # coefficient存储施加者名称
                                        skill.turn_cost, skill.skill_type)
                    target.add_status(effect)
                target_name_msg = "、".join(target_names)
                damage_desc = self.get_effect_desc(DebuffType.SKILL_DOT, True, skill.atk_values[0] if isinstance(skill.atk_values, list) else skill.atk_values)
                self.add_message(caster, f"→ 对{target_name_msg}施加持续伤害，{damage_desc}，持续{skill.turn_cost}回合")
                return "", total_dmg

            # Type 3: 属性增益 (Stat Buff)
            elif skill.skill_type == SkillType.BUFF_STAT:
                if not targets: return "", 0 # BUFF技能没有直接目标，但为了兼容普攻逻辑，需要一个目标
                buff_value_for_display = skill.skill_buff_value
                
                if skill.skill_buff_type == 1: # 攻击力提升
                    effect = StatusEffect(skill.name, BuffType.ATTACK_UP, buff_value_for_display, 1, False, skill.turn_cost,
                                        skill.skill_type)
                    caster.add_status(effect)
                    self.add_message(caster, f"→ 提升了{self.get_effect_desc(BuffType.ATTACK_UP, False, buff_value_for_display)}，持续{skill.turn_cost}回合")
                elif skill.skill_buff_type == 2: # 伤害减免提升
                    effect = StatusEffect(skill.name, BuffType.DAMAGE_REDUCTION_UP, buff_value_for_display, 1, False, skill.turn_cost, skill.skill_type)
                    caster.add_status(effect)
                    self.add_message(caster, f"→ 提升了{self.get_effect_desc(BuffType.DAMAGE_REDUCTION_UP, False, buff_value_for_display)}，持续{skill.turn_cost}回合")
                
                # BUFF技能释放后，可能还会进行一次普通攻击
                attack_msg, current_dmg = self._normal_attack_and_process(caster, targets[0], skip_twin_strike=True, is_skill_proc=True)
                total_dmg += current_dmg
                return attack_msg, total_dmg

            # Type 4: 封印/控制 (Control)
            elif skill.skill_type == SkillType.CONTROL:
                if not targets: return "", 0
                chance = skill.success_rate
                target_names_success = []
                target_names_failure = []
                for target in targets:
                    if random.uniform(0, 100) <= chance:
                        control_type = DebuffType(skill.skill_buff_type)
                        
                        effect = StatusEffect(skill.name, control_type, skill.turn_cost, 1, True, skill.turn_cost)
                        target.add_status(effect)
                        target_names_success.append(target.name)
                    else:
                        target_names_failure.append(target.name)
                
                control_desc = self.get_effect_desc(DebuffType(skill.skill_buff_type), True, skill.turn_cost)
                if target_names_success:
                    target_name_msg = "、".join(target_names_success)
                    self.add_message(caster, f"→ {target_name_msg}{control_desc}！")
                if target_names_failure:
                    target_name_msg = "、".join(target_names_failure)
                    self.add_message(caster, f"→ 封印失败，被{target_name_msg}抵抗了！")
                
                attack_msg, current_dmg = self._normal_attack_and_process(caster, targets[0], skip_twin_strike=True, is_skill_proc=True)
                total_dmg += current_dmg
                return attack_msg, total_dmg

            # Type 5: 随机波动伤害 (Random Hit)
            elif skill.skill_type == SkillType.RANDOM_HIT:
                if not targets: return "", 0
                target = targets[0]
                min_mult = float(skill.atk_values[0]) if isinstance(skill.atk_values, list) else float(skill.atk_values)
                max_mult = float(skill.atk_coefficient)
                rand_mult = random.uniform(min_mult, max_mult)
                rand_mult = round(rand_mult, 2)
                dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, target, rand_mult)

                if status == "Hit":
                    hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, dmg, true_dmg)
                    
                    # 检查目标是否死亡并尝试复活
                    if target.hp <= 0:
                        revived = self._check_and_apply_revive_effects(target, caster.has_death_strike)
                        if not revived:
                            killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                            if not killed_by_death_strike:
                                self.add_message(target, f"💀{target.name}倒下了！")

                    if reflected_dmg > 0:
                        self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                        caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
                    total_dmg += hp_dmg
                    crit_str = "💥并且发生了会心一击，" if is_crit else ""
                    self.add_message(caster, f"→ 获得{rand_mult}倍加成，{crit_str}对{target.name}造成{number_to(int(total_dmg))}伤害！")
                else:
                    self.add_message(caster, f"→ 技能被{target.name}闪避了！")

                if skill.turn_cost > 0:
                    effect = StatusEffect(skill.name, DebuffType.FATIGUE, skill.turn_cost, 1, True, skill.turn_cost, skill.skill_type)
                    caster.add_status(effect)
                    self.add_message(caster, f"→ {caster.name}力竭，需休息{skill.turn_cost}回合")
                return "", total_dmg

            # Type 6: 叠加 Buff (Stacking)
            elif skill.skill_type == SkillType.STACK_BUFF:
                if not targets: return "", 0
                effect = StatusEffect(skill.name, BuffType.ATTACK_UP, skill.skill_buff_value, 1, False, skill.turn_cost,
                                    skill.skill_type)
                caster.add_status(effect)
                buff_desc = self.get_effect_desc(BuffType.ATTACK_UP, False, skill.skill_buff_value)
                self.add_message(caster, f"→ 每回合叠加{buff_desc}，持续{skill.turn_cost}回合")
                attack_msg, current_dmg = self._normal_attack_and_process(caster, targets[0], skip_twin_strike=True, is_skill_proc=True)
                total_dmg += current_dmg
                return attack_msg, total_dmg


            # Type 101: BOSS专属技能 紫玄掌 (倍数伤害+目标百分比生命值伤害)
            elif skill.skill_type == SkillType.MULTIPLIER_PERCENT_HP:
                if not targets: return "", 0
                current_total_dmg = 0
                for target in targets:
                    dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, target, skill.atk_values[0] if isinstance(skill.atk_values, list) else skill.atk_values)
                    if status == "Hit":
                        crit_str = "💥并且发生了会心一击，" if is_crit else ""
                        raw_dmg_with_hp_percent = dmg + (target.max_hp * skill.atk_coefficient)
                        hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, raw_dmg_with_hp_percent, true_dmg)
                        
                        # 检查目标是否死亡并尝试复活
                        if target.hp <= 0:
                            revived = self._check_and_apply_revive_effects(target, caster.has_death_strike)
                            if not revived:
                                killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                                if not killed_by_death_strike:
                                    self.add_message(target, f"💀{target.name}倒下了！")

                        if reflected_dmg > 0:
                            self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                            caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
                        current_total_dmg += hp_dmg
                        self.add_message(caster, f"→ {crit_str}对{target.name}造成{number_to(int(hp_dmg))}伤害！")
                    else:
                        self.add_message(caster, f"→ {target.name}躲开了{caster.name}的攻击！")
                total_dmg = current_total_dmg
                return "", total_dmg

            # Type 102: BOSS专属技能 子龙朱雀 (倍数伤害+无视防御)
            elif skill.skill_type == SkillType.MULTIPLIER_DEF_IGNORE:
                if not targets: return "", 0
                current_total_dmg = 0
                for target in targets:
                    dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster, target, skill.atk_values[0] if isinstance(skill.atk_values, list) else skill.atk_values, True)
                    if status == "Hit":
                        crit_str = "💥并且发生了会心一击，" if is_crit else ""
                        hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, dmg, true_dmg)
                        
                        # 检查目标是否死亡并尝试复活
                        if target.hp <= 0:
                            revived = self._check_and_apply_revive_effects(target, caster.has_death_strike)
                            if not revived:
                                killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                                if not killed_by_death_strike:
                                    self.add_message(target, f"💀{target.name}倒下了！")

                        if reflected_dmg > 0:
                            self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                            caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
                        current_total_dmg += hp_dmg
                        self.add_message(caster, f"→ {crit_str}对{target.name}造成{number_to(int(hp_dmg))}伤害！")
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
                    if random.uniform(0, 100) <= chance:
                        effect = StatusEffect(skill.name, skill.skill_buff_type, skill.turn_cost, 1, True, skill.turn_cost)
                        target.add_status(effect)
                        target_names_success.append(target.name)
                    else:
                        target_names_failure.append(target.name)
                
                control_desc = self.get_effect_desc(DebuffType(skill.skill_buff_type), True, skill.turn_cost)
                if target_names_success:
                    target_name_msg = "、".join(target_names_success)
                    self.add_message(caster, f"→ {target_name_msg}{control_desc}！")
                if target_names_failure:
                    target_name_msg = "、".join(target_names_failure)
                    self.add_message(caster, f"→ {skill.name}对{target_name_msg}的控制被抵抗了！")
                return "", total_dmg

            # Type 104: 召唤类型 (SUMMON)
            elif skill.skill_type == SkillType.SUMMON:
                copy_ratio = skill.atk_values[0] if isinstance(skill.atk_values, list) else skill.atk_values
                summon_count = int(skill.atk_coefficient)

                for i in range(summon_count):
                    summon_data = {}

                    summon_data["user_id"] = self.bot_id
                    summon_data["nickname"] = f"{caster.name}的召唤物"
                    summon_data["monster_type"] = "summon"

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

                    summon_data["start_skills"] = []
                    summon_data["skills"] = []

                    if hasattr(caster, 'is_boss') and caster.is_boss:
                        summon_data["is_boss"] = True
                    else:
                        summon_data["is_boss"] = False

                    summon = Entity(
                        data=summon_data,
                        team_id=caster.team_id,
                        is_boss=summon_data.get("is_boss", False)
                    )

                    if caster.team_id == 0:
                        self.team_a.append(summon)
                    else:
                        self.team_b.append(summon)

                self.add_message(caster, f"→ 生成{summon_count}个召唤物！")
                return "", total_dmg

            else:
                self.add_message(caster, f"→ {skill.name}技能效果未知！")
                return "", total_dmg
        
        target = min(targets, key=lambda x: x.hp) if targets else None # 技能未触发，选择普攻目标
        if not target: return "", 0
        
        attack_msg, current_dmg = self._normal_attack_and_process(caster, target, skip_twin_strike=True)
        total_dmg += current_dmg
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
        total_dmg_from_this_attack = 0
        
        attack_multiplier = 1.0
        if caster.charge_bonus > 0:
            attack_multiplier += caster.charge_bonus
            self.add_message(caster, f"✨{caster.name}的蓄力攻击爆发，攻击力额外提升{round(caster.charge_bonus * 100, 2)}%！")
            caster.charge_bonus = 0.0
            caster.natal_charge_status = 0
            if caster.natal_data: caster.natal_data["charge_status"] = caster.natal_charge_status


        raw_dmg, true_dmg, is_crit, accuracy = self._calc_raw_damage(caster, target, attack_multiplier)
        
        if accuracy == "Hit":
            # 睡眠状态下被攻击会苏醒
            if target.has_debuff("type", DebuffType.SLEEP):
                self.add_message(target, f"💤{target.name}被攻击，从睡眠中苏醒！")
                target.debuffs = [d for d in target.debuffs if d.type != DebuffType.SLEEP]

            hp_dmg, _, reflected_dmg = self._apply_damage(caster, target, raw_dmg, true_dmg)
            
            # 检查目标是否死亡并尝试复活
            if target.hp <= 0:
                revived = self._check_and_apply_revive_effects(target, caster.has_death_strike)
                if not revived: # 如果没有复活成功，则目标真正死亡
                    killed_by_death_strike = self._check_and_apply_death_strike(caster, target, hp_dmg)
                    if not killed_by_death_strike:
                        self.add_message(target, f"💀{target.name}倒下了！")

            # 处理反伤
            if reflected_dmg > 0:
                self.add_message(target, f"对{caster.name}反弹{number_to(reflected_dmg)}真实伤害！")
                caster.update_stat("hp", 2, reflected_dmg, bypass_shield=True) # 反弹伤害绕过护盾
            total_dmg_from_this_attack += hp_dmg
            
            crit_str = "💥并且发生了会心一击，" if is_crit else ""
            if is_skill_proc:
                self.add_message(caster, f"→ {crit_str}对{target.name}造成{number_to(int(hp_dmg))}伤害！")
            else:
                self.add_message(caster, f"{caster.name}发起攻击，{crit_str}对{target.name}造成{number_to(int(hp_dmg))}伤害！")

            
            # --- 双生效果判定 (NatalEffectType.TWIN_STRIKE) ---
            if not skip_twin_strike and caster.natal_data and caster.twin_strike_effect:
                trigger_chance, damage_multiplier = caster.twin_strike_effect
                if random.random() < trigger_chance:
                    twin_strike_dmg, twin_strike_true_dmg, twin_strike_is_crit, twin_strike_accuracy = self._calc_raw_damage(caster, target, damage_multiplier)
                    
                    if twin_strike_accuracy == "Hit":
                        twin_hp_dmg, _, twin_reflected_dmg = self._apply_damage(caster, target, twin_strike_dmg, twin_strike_true_dmg)
                        
                        # 检查目标是否死亡并尝试复活
                        if target.hp <= 0:
                            revived = self._check_and_apply_revive_effects(target, caster.has_death_strike)
                            if not revived:
                                killed_by_death_strike = self._check_and_apply_death_strike(caster, target, twin_hp_dmg)
                                if not killed_by_death_strike:
                                    self.add_message(target, f"💀{target.name}倒下了！")
                        
                        if twin_reflected_dmg > 0:
                            self.add_message(target, f"对{caster.name}反弹{number_to(twin_reflected_dmg)}真实伤害！")
                            caster.update_stat("hp", 2, twin_reflected_dmg, bypass_shield=True)
                        total_dmg_from_this_attack += twin_hp_dmg
                        twin_crit_str = "💥" if twin_strike_is_crit else ""
                        self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』触发【双生】，{twin_crit_str}再次对{target.name}造成{number_to(int(twin_hp_dmg))}伤害！")
                    else:
                        self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』触发【双生】，但被{target.name}躲开了！")
                
            # --- 新增控制效果触发 ---
            # 睡眠
            if caster.natal_data and caster.sleep_chance > 0 and random.random() < caster.sleep_chance:
                if not target.has_debuff("type", DebuffType.SLEEP):
                    effect = StatusEffect(f"{caster.natal_data.get('name','本命法宝')}·睡眠", DebuffType.SLEEP, SLEEP_DURATION, 1, True, SLEEP_DURATION)
                    target.add_status(effect)
                    self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』对{target.name}施加了【睡眠】效果！")
            
            # 石化
            if caster.natal_data and caster.petrify_chance > 0 and random.random() < caster.petrify_chance:
                if not target.has_debuff("type", DebuffType.PETRIFY):
                    effect = StatusEffect(f"{caster.natal_data.get('name','本命法宝')}·石化", DebuffType.PETRIFY, PETRIFY_DURATION, 1, True, PETRIFY_DURATION)
                    target.add_status(effect)
                    self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』对{target.name}施加了【石化】效果！")
            
            # 眩晕
            if caster.natal_data and caster.stun_chance > 0 and random.random() < caster.stun_chance:
                if not target.has_debuff("type", DebuffType.STUN):
                    effect = StatusEffect(f"{caster.natal_data.get('name','本命法宝')}·眩晕", DebuffType.STUN, STUN_DURATION, 1, True, STUN_DURATION)
                    target.add_status(effect)
                    self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』对{target.name}施加了【眩晕】效果！")
            
            # 疲劳
            if caster.natal_data and caster.fatigue_chance > 0 and random.random() < caster.fatigue_chance:
                if not target.has_debuff("type", DebuffType.FATIGUE):
                    effect = StatusEffect(f"{caster.natal_data.get('name','本命法宝')}·疲劳", DebuffType.FATIGUE, FATIGUE_DURATION, 1, True, FATIGUE_DURATION)
                    target.add_status(effect)
                    self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』对{target.name}施加了【疲劳】效果！")

            # 沉默
            if caster.natal_data and caster.silence_chance > 0 and random.random() < caster.silence_chance:
                if not target.has_debuff("type", DebuffType.SILENCE):
                    effect = StatusEffect(f"{caster.natal_data.get('name','本命法宝')}·沉默", DebuffType.SILENCE, SILENCE_DURATION, 1, True, SILENCE_DURATION)
                    target.add_status(effect)
                    self.add_message(caster, f"『{caster.natal_data.get('name','本命法宝')}』对{target.name}施加了【沉默】效果！")

            # 招魂 (仅在有多人战斗且有死亡队友时触发)
            dead_allies = self._get_all_dead_allies(caster)
            if caster.natal_data and caster.soul_summon_chance > 0 and dead_allies and len(dead_allies) > 0 and random.random() < caster.soul_summon_chance:
                # 随机选择一个死亡队友尝试招魂
                ally_to_summon = random.choice(dead_allies)
                # 检查该队友是否已触发过招魂
                if caster.soul_summon_counts.get(str(ally_to_summon.id), 0) < SOUL_SUMMON_LIMIT:
                    ally_to_summon.is_soul_return_state = True
                    ally_to_summon.soul_return_revive_turn = SOUL_RETURN_DURATION
                    # 增加招魂次数
                    caster.soul_summon_counts[str(ally_to_summon.id)] = caster.soul_summon_counts.get(str(ally_to_summon.id), 0) + 1
                    if caster.natal_data: caster.natal_data["soul_summon_count"] = caster.soul_summon_counts # 更新回 natal_data
                    self.add_message(caster, f"✨『{caster.natal_data.get('name','本命法宝')}』触发【招魂】，将已逝队友 {ally_to_summon.name} 召唤回战场，进入灵体状态！")
                    ally_to_summon.hp = 1 # 确保在灵体状态下不被移除
            
            # 启明 (仅在有多人战斗且有死亡队友时触发)
            dead_allies_for_enlightenment = self._get_all_dead_allies(caster) # 重新获取可能已被招魂的队友
            if caster.natal_data and caster.enlightenment_chance > 0 and dead_allies_for_enlightenment and len(dead_allies_for_enlightenment) > 0 and random.random() < caster.enlightenment_chance:
                # 随机选择一个死亡队友尝试启明
                ally_to_enlighten = random.choice(dead_allies_for_enlightenment)
                # 检查该队友是否已触发过启明
                if caster.enlightenment_counts.get(str(ally_to_enlighten.id), 0) < ENLIGHTENMENT_LIMIT:
                    revive_amount = ally_to_enlighten.max_hp * ENLIGHTENMENT_REVIVE_HP_PERCENT
                    ally_to_enlighten.hp = revive_amount
                    # 增加启明次数
                    caster.enlightenment_counts[str(ally_to_enlighten.id)] = caster.enlightenment_counts.get(str(ally_to_enlighten.id), 0) + 1
                    if caster.natal_data: caster.natal_data["enlightenment_count"] = caster.enlightenment_counts # 更新回 natal_data
                    self.add_message(caster, f"✨『{caster.natal_data.get('name','本命法宝')}』触发【启明】，将已逝队友 {ally_to_enlighten.name} 复活，回复 {number_to(int(revive_amount))} 生命！")


        else:
            if is_skill_proc:
                self.add_message(caster, f"→ 攻击被{target.name}躲开了")
            else:
                self.add_message(caster, f"{caster.name}使用普通攻击，被{target.name}躲开了")

        return "", total_dmg_from_this_attack


    def check_unit_control(self, unit):
        """
        检查单位的控制状态，并返回控制消息。
        :param unit: 实体。
        :return: True如果单位被控制且需要跳过行动，False否则。
        """
        SKIP_TURN_CONTROLS = {
            DebuffType.STUN: ("🌀", "被眩晕，跳过行动"),
            DebuffType.FREEZE: ("❄️", "被冰冻，跳过行动"),
            DebuffType.PETRIFY: ("🗿", "被石化，跳过行动"),
            DebuffType.SLEEP: ("💤", "正在睡眠，跳过行动"),
            DebuffType.ROOT: ("🌿", "被定身，跳过行动"),
            DebuffType.FEAR: ("😱", "陷入恐惧，跳过行动"),
            DebuffType.PARALYSIS: ("⚡", "全身麻痹，跳过行动"),
        }
        
        SKIP_SKILL_CONTROLS = {
            DebuffType.SEAL: ("🔒", "被封印，无法使用技能"),
            DebuffType.SILENCE: ("🔇", "被沉默，无法使用神通"),
        }
        
        if unit.has_debuff("type", DebuffType.FATIGUE):
            duration = unit.get_debuff_field("type", "duration", DebuffType.FATIGUE)
            self.add_message(unit, f"😫{unit.name}处于疲劳状态（攻击力降低{round(FATIGUE_ATTACK_REDUCTION*100,2)}%），剩余{duration}回合")

        for debuff_type, (emoji, description) in SKIP_TURN_CONTROLS.items():
            if unit.has_debuff("type", debuff_type):
                duration = unit.get_debuff_field("type", "duration", debuff_type)
                if debuff_type == DebuffType.SLEEP:
                    self.add_message(unit, f"{emoji}{unit.name}{description}（剩余{duration}回合），但被攻击可能会苏醒！")
                    return True
                elif debuff_type == DebuffType.PETRIFY:
                    self.add_message(unit, f"{emoji}{unit.name}{description}（被攻击伤害减免{round(PETRIFY_DAMAGE_REDUCTION_PERCENT*100,2)}%），剩余{duration}回合")
                    return True
                else:
                    self.add_message(unit, f"{emoji}{unit.name}{description}（剩余{duration}回合）")
                    return True
        
        for debuff_type, (emoji, description) in SKIP_SKILL_CONTROLS.items():
            if unit.has_debuff("type", debuff_type):
                duration = unit.get_debuff_field("type", "duration", debuff_type)
                self.add_message(unit, f"{emoji}{unit.name}{description}（剩余{duration}回合）")
        
        return False

    def process_turn(self):
        """
        处理单个战斗回合的逻辑。
        包括回合开始、单位行动、状态更新、伤害结算、胜负判定等。
        """
        self.round += 1
        # 获取所有存活单位并按速度排序 (包括假死状态的单位)
        units = sorted([u for u in self.team_a + self.team_b if u.is_alive], key=lambda x: x.base_speed, reverse=True)
        
        self_id_for_bot = self.bot_id # 机器人id

        self.last_status_messages.clear()

        # 战斗开始时（第1回合），应用开局技能
        if self.round == 1:
            for unit in units:
                enemies = self._get_all_enemies(unit)
                self._apply_round_one_skills(unit, enemies, unit.start_skills)

        # 回合开始时的周期性效果 (如本命法宝)
        if (self.round - 1) % 4 == 0:
            for unit in units:
                if not unit.is_alive: continue # 只有存活单位才触发周期性效果
                if unit.natal_data: 
                    self.add_message(unit, f"『{unit.natal_data.get('name','本命法宝')}』道韵流转，威能再现！")
                    unit.apply_natal_periodic_effect(self)

                    if not unit.is_boss: # 玩家单位才触发周期性真实伤害
                        natal_treasure_level = unit.natal_data.get("level", 0)
                        periodic_true_dmg_rate = PERIODIC_TRUE_DAMAGE_BASE + natal_treasure_level * PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL
                        
                        if periodic_true_dmg_rate > 0:
                            enemies_to_damage = self._get_all_enemies(unit)
                            if enemies_to_damage:
                                self.add_message(unit, f"◎ 『{unit.natal_data.get('name', '本命法宝')}』道韵生效！")
                                for enemy in enemies_to_damage:
                                    # 只有真正存活的敌人才能受到周期伤害
                                    if enemy.is_truly_alive: 
                                        true_damage = int(enemy.hp * periodic_true_dmg_rate)
                                        if true_damage > 0:
                                            hp_dmg, _, _ = self._apply_damage(unit, enemy, 0, true_damage) # 真实伤害绕过护盾，只计算HP扣减
                                            self.add_message(unit, f"→ 对 {enemy.name} 造成 {number_to(hp_dmg)} 点真实伤害！")
                                            
                                            # 周期伤害也可能导致死亡，需要检查复活
                                            if enemy.hp <= 0:
                                                revived = self._check_and_apply_revive_effects(enemy, False) # 周期性伤害不视为斩命
                                                if not revived:
                                                    self.add_message(enemy, f"💀{enemy.name}倒下了！")
        
        for unit in units: # 显示所有存活单位的状态，包括假死
            if unit.is_alive:
                self.add_unit_status_message(unit)

        alive_a = any(u.is_truly_alive for u in self.team_a) # 只有真正存活的单位才能判定队伍是否存活
        alive_b = any(u.is_truly_alive for u in self.team_b)
        if not alive_a or not alive_b: return


        # 按单位行动
        for unit in units:
            if not unit.is_alive: continue # 如果单位在前面的结算中死亡，则跳过行动
            
            self.add_message(unit, f"☆------{unit.name}的回合 (第 {self.round} 回合)------☆")

            # 更新所有状态效果的持续时间，并移除过期的
            expired_debuff_messages = unit.update_status_effects()
            for msg in expired_debuff_messages:
                self.add_message(unit, msg)

            # 涅槃/魂返状态处理
            if unit.is_nirvana_state:
                unit.nirvana_revive_turn -= 1
                if unit.nirvana_revive_turn > 0:
                    # 涅槃期间检查队友是否全部死亡
                    if not self._get_all_truly_alive_allies(unit, include_self=True): # 检查所有友方单位是否全部真正死亡（包括自己）
                        self.add_message(unit, f"💀{unit.name}的队友全部阵亡，涅槃失败！")
                        unit.hp = 0 # 真正死亡
                        unit.is_nirvana_state = False
                        self.add_unit_status_message(unit)
                        continue # 跳过当前单位行动
                    self.add_unit_status_message(unit)
                    continue # 跳过当前单位行动
                else: # 涅槃结束，复活
                    unit.is_nirvana_state = False
                    unit.hp = min(unit.max_hp, unit.max_hp) # 满血复活
                    # 计算护盾值并给所有队友（包括自己）施加护盾
                    shield_value = unit.max_hp * (NIRVANA_SHIELD_BASE + (unit.nirvana_effect if unit.nirvana_effect else 0))
                    allies_and_self = self._get_all_allies(unit, include_self=True) # 包括自己
                    for ally in allies_and_self:
                        if ally.is_truly_alive: # 确保只给真正活着的队友加护盾
                            ally.shield += shield_value
                            self.add_message(ally, f"✨{ally.name}受到涅槃之力庇护，获得{number_to(int(shield_value))}点护盾！")
                    self.add_message(unit, f"✨{unit.name}涅槃重生，满血复活！")

            if unit.is_soul_return_state:
                unit.soul_return_revive_turn -= 1
                if unit.soul_return_revive_turn > 0:
                    # 魂返期间检查队友是否全部死亡
                    if not self._get_all_truly_alive_allies(unit, include_self=True): # 检查所有友方单位是否全部真正死亡（包括自己）
                        self.add_message(unit, f"💀{unit.name}的队友全部阵亡，魂返失败！")
                        unit.hp = 0 # 真正死亡
                        unit.is_soul_return_state = False
                        self.add_unit_status_message(unit)
                        continue # 跳过当前单位行动
                    self.add_unit_status_message(unit)
                else: # 魂返结束，复活
                    unit.is_soul_return_state = False
                    revive_amount = unit.max_hp * (SOUL_RETURN_HP_BASE + (unit.soul_return_effect if unit.soul_return_effect else 0))
                    unit.hp = min(unit.max_hp, unit.hp + revive_amount)
                    self.add_message(unit, f"✨{unit.name}魂返成功，回复{number_to(int(revive_amount))}生命值复活！")

            # 结算后检查死亡，如果死亡则跳过后续行动
            if not unit.is_truly_alive:
                if not any(f"💀{unit.name}倒下了！" in entry["data"]["content"] for entry in self.play_list[-5:]): # 避免重复显示死亡信息
                    self.add_message(unit, f"💀{unit.name}倒下了！")
                self.add_unit_status_message(unit)
                continue

            enemies = self._get_all_enemies(unit)
            if not enemies: break
            
            # 本命法宝流血概率触发
            if unit.natal_data:
                unit.apply_natal_bleed_proc(self)
                
            # DoT 伤害结算 (中毒、流血、技能持续伤害)
            if unit.poison_dot_dmg > 0:
                self.add_message(unit, f"☠️{unit.name}中毒消耗气血{number_to(int(unit.poison_dot_dmg))}点")
                hp_dmg_from_dot, _, _ = self._apply_damage(unit, unit, unit.poison_dot_dmg, 0)
                if unit.hp <= 0: # 死亡检查
                    revived = self._check_and_apply_revive_effects(unit, False)
                    if not revived:
                        self.add_message(unit, f"💀{unit.name}倒下了！")
                self.add_unit_status_message(unit)
            
            bleed_damages = unit.bleed_dot_dmg_list
            if bleed_damages:
                total_bleed_dmg = sum(bleed_damages)
                self.add_message(unit, f"🩸{unit.name}因流血消耗气血{number_to(int(total_bleed_dmg))}点 ({len(bleed_damages)}层)")
                hp_dmg_from_dot, _, _ = self._apply_damage(unit, unit, total_bleed_dmg, 0)
                if unit.hp <= 0: # 死亡检查
                    revived = self._check_and_apply_revive_effects(unit, False)
                    if not revived:
                        self.add_message(unit, f"💀{unit.name}倒下了！")
                self.add_unit_status_message(unit)

            for skill_dot_info in unit.get_debuffs("type", DebuffType.SKILL_DOT):
                caster_entity = next((u for u in self.team_a + self.team_b if u.name == skill_dot_info.coefficient), None)
                if not caster_entity: continue
                raw_dmg, true_dmg, is_crit, status = self._calc_raw_damage(caster_entity, unit, skill_dot_info.value)
                if status == "Hit":
                    hp_dmg, _, reflected_dmg = self._apply_damage(caster_entity, unit, raw_dmg, true_dmg)
                    if reflected_dmg > 0:
                        self.add_message(unit, f"对{caster_entity.name}反弹{number_to(reflected_dmg)}真实伤害！")
                        caster_entity.update_stat("hp", 2, reflected_dmg, bypass_shield=True)
                    
                    crit_str = "💥会心一击，" if is_crit else ""
                    self.add_message(unit, f"{skill_dot_info.name}{crit_str}造成{number_to(int(hp_dmg))}伤害！"
                                           f"（剩余{skill_dot_info.duration}回合）")
                    if unit.hp <= 0: # 死亡检查
                        revived = self._check_and_apply_revive_effects(unit, caster_entity.has_death_strike)
                        if not revived:
                            self.add_message(unit, f"💀{unit.name}倒下了！")
                self.add_unit_status_message(unit)


            # HoT 结算 (HP和MP恢复)
            if unit.hp_regen_rate > 0:
                self.add_message(unit, f"❤️{unit.name}回复气血{number_to(int(unit.hp_regen_rate))}点")
                unit.update_stat("hp", 1, unit.hp_regen_rate)
                self.add_unit_status_message(unit)

            if unit.mp_regen_rate > 0:
                self.add_message(unit, f"💙{unit.name}回复真元{number_to(int(unit.mp_regen_rate))}点")
                unit.update_stat("mp", 1, unit.mp_regen_rate)
                self.add_unit_status_message(unit)
            
            # Buff 状态显示 (例如，持续Buff的剩余回合数)
            if unit.has_buff("skill_type", 3):
                for skill_buff in unit.get_buffs("skill_type", 3):
                    buff_msg = self.get_effect_desc(skill_buff.type, False, skill_buff.value)
                    self.add_message(unit, f"『{skill_buff.name}』{buff_msg}，剩余{skill_buff.duration}回合")

            if unit.has_buff("skill_type", 6):
                skill_buff = unit.get_buff("skill_type", 6)
                buff_msg = self.get_effect_desc(BuffType.ATTACK_UP, False, skill_buff.value)
                self.add_message(unit, f"『{skill_buff.name}』{buff_msg}，剩余{skill_buff.duration}回合")
            
            # 结算后检查死亡，如果死亡则跳过后续行动
            if not unit.is_truly_alive: # 真正死亡才跳过行动
                if not any(f"💀{unit.name}倒下了！" in entry["data"]["content"] for entry in self.play_list[-5:]):
                    self.add_message(unit, f"💀{unit.name}倒下了！")
                self.add_unit_status_message(unit)
                continue

            enemies = self._get_all_enemies(unit)
            if not enemies: break
            
            # --- 蓄力效果判定 (NatalEffectType.CHARGE) ---
            if unit.natal_data and unit.check_for_natal_charge_effect() > 0:
                if unit.natal_charge_status == 0:
                    unit.is_charging_turn = True
                    unit.natal_charge_status = 1
                    self.add_message(unit, f"✨『{unit.natal_data.get('name','本命法宝')}』【蓄力】激活，本回合将不攻击，下回合攻击力将大幅提升！")
                elif unit.natal_charge_status == 1: # 蓄力回合结束，进入待爆发状态
                    self.add_message(unit, f"✨『{unit.natal_data.get('name','本命法宝')}』【蓄力】爆发！")
                    unit.natal_charge_status = 2 # 状态更新为蓄力后爆发
                elif unit.natal_charge_status == 2: # 爆发回合结束，重置
                    pass # 实际重置在_normal_attack_and_process中进行

            # 控制状态检查
            controlled_and_skip_turn = self.check_unit_control(unit)
            if controlled_and_skip_turn:
                if unit.is_charging_turn: # 如果被控制时处于蓄力状态，则中断蓄力
                    self.add_message(unit, f"❌{unit.name}蓄力被中断！")
                    unit.is_charging_turn = False
                    unit.natal_charge_status = 0
                self.add_unit_status_message(unit)
                continue
            
            if unit.is_charging_turn: # 如果处于蓄力状态，跳过本回合攻击
                unit.is_charging_turn = False
                self.add_unit_status_message(unit)
                continue

            # --- 攻击流程 ---
            skill = None
            if unit.has_debuff("type", DebuffType.SILENCE):
                self.add_message(unit, f"🔇{unit.name}被沉默，无法使用神通！")
                skill = None
            elif unit.is_soul_return_state:
                self.add_message(unit, f"👻{unit.name}处于灵体状态，只进行普通攻击！")
                skill = None
            else:
                skill = self.choose_skill(unit, unit.skills, enemies)
            
            total_dmg_this_action = 0
            if skill:
                targets = self._select_targets(enemies, skill, unit.is_boss)
                if not targets: # 如果技能没有可选目标，则进行普攻
                    current_enemies = self._get_all_enemies(unit)
                    if not current_enemies: break
                    target = min(current_enemies, key=lambda x: x.hp)
                    _, total_dmg_this_action = self._normal_attack_and_process(unit, target)
                else:
                    _, total_dmg_this_action = self._execute_skill(unit, targets, skill)
            else: # 没有可选技能或被沉默，进行普攻
                current_enemies = self._get_all_enemies(unit)
                if not current_enemies: break
                target = min(current_enemies, key=lambda x: x.hp)
                _, total_dmg_this_action = self._normal_attack_and_process(unit, target)

            # 处理吸血/吸蓝
            if total_dmg_this_action > 0:
                lifesteal_msg, mana_steal_msg = "", ""
                
                if unit.lifesteal_rate > 0:
                    lifesteal = int(total_dmg_this_action * unit.lifesteal_rate)
                    if lifesteal > 0:
                        lifesteal_msg = f"❤️吸取气血{number_to(lifesteal)}点"
                        unit.update_stat("hp", 1, lifesteal)
                
                if unit.mana_steal_rate > 0:
                    mana_steal = int(total_dmg_this_action * unit.mana_steal_rate)
                    if mana_steal > 0:
                        mana_steal_msg = f"💙吸取真元{number_to(mana_steal)}点"
                        unit.update_stat("mp", 1, mana_steal)

                if lifesteal_msg or mana_steal_msg:
                    messages = []
                    if lifesteal_msg: messages.append(lifesteal_msg)
                    if mana_steal_msg: messages.append(mana_steal_msg)
                    self.add_message(unit, f"（{'、'.join(messages)}）")
            
            # 攻击完成后检查敌人是否死亡，并打印死亡消息
            for enemy in enemies:
                if not enemy.is_truly_alive and not enemy.is_nirvana_state and not enemy.is_soul_return_state:
                    if not any(f"💀{enemy.name}倒下了！" in entry["data"]["content"] for entry in self.play_list[-5:]): # 避免重复打印
                        self.add_message(enemy, f"💀{enemy.name}倒下了！")
                self.add_unit_status_message(enemy) # 更新敌人状态条
            
            self.add_unit_status_message(unit) # 更新自身状态条

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
                    "nirvana_revive_count": u.nirvana_revive_count,
                    "soul_return_revive_count": u.soul_return_revive_count,
                    "charge_status": u.natal_charge_status,
                    "soul_summon_count": u.soul_summon_counts, # 招魂次数
                    "enlightenment_count": u.enlightenment_counts # 启明次数
                }
            status.append({
                u.name: {
                    "hp": int(u.hp),
                    "mp": int(u.mp),
                    "user_id": u.id,
                    "hp_multiplier": u.max_hp / (u.data.get("max_hp", 1) if u.data.get("max_hp", 1) > 0 else 1),
                    "mp_multiplier": u.max_mp / (u.data.get("max_mp", 1) if u.data.get("max_mp", 1) > 0 else 1),
                    "team_id": u.team_id,
                    "total_dmg": int(u.total_dmg),
                    "natal_data": natal_counts
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
            if unit.natal_data:
                unit.init_charge_status()

        while self.round < self.max_rounds:
            self.process_turn()

            # 检查队伍是否仍然存活
            alive_a = any(u.is_truly_alive for u in self.team_a)
            alive_b = any(u.is_truly_alive for u in self.team_b)

            if not alive_a:
                winner_name = next((u.name for u in self.team_b if u.is_truly_alive), "B队")
                self.add_system_message(f"战斗结束: {winner_name} 方获胜!")
                return self.play_list, 1, self.get_final_status_list()

            if not alive_b:
                winner_name = next((u.name for u in self.team_a if u.is_truly_alive), "A队")
                self.add_system_message(f"战斗结束: {winner_name} 方获胜!")
                return self.play_list, 0, self.get_final_status_list()

        self.add_system_message("战斗超过最大回合数，平局！")
        return self.play_list, 2, self.get_final_status_list()