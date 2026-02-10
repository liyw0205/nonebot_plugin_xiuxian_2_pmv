import json
import random
from enum import IntEnum
from pathlib import Path
from nonebot.log import logger

from .xiuxian2_handle import XiuxianDateManage, OtherSet, UserBuffDate, XIUXIAN_IMPART_BUFF
from ..xiuxian_config import convert_rank
from .utils import number_to
from .item_json import Items

items = Items()
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()


async def pve_fight(user, monster, type_in=2, bot_id=0, level_ratios=None):
    user_data = []
    monster_data = []

    for u in user:
        player_data = get_players_attributes(u, level_ratios)
        player = Entity(player_data["属性"], team_id=0)
        apply_player_buffs(player, player_data)  # 添加buff和技能
        user_data.append(player)  # 添加列表
    for m in monster:
        enemy_data = get_boss_attributes(m, bot_id)
        enemy = Entity(enemy_data["属性"], team_id=1, is_boss=True)
        enemy.start_skills.extend(generate_boss_buff(m))  # 添加buff
        generate_boss_skill(enemy, m.get("skills", []))  # 添加技能
        monster_data.append(enemy)

    battle = BattleSystem(user_data, monster_data, bot_id)
    play_list, winner, status_list = battle.run_battle()

    if type_in == 2:
        update_all_user_status(status_list, bot_id, level_ratios)  # 更新玩家数据

    return play_list, winner, status_list


def Player_fight(user1, user2, type_in=1, bot_id=0):
    player1_data = get_players_attributes(user1)  # 获取玩家数据
    player2_data = get_players_attributes(user2)

    player1 = Entity(player1_data["属性"], team_id=0)
    player2 = Entity(player2_data["属性"], team_id=1)

    apply_player_buffs(player1, player1_data)
    apply_player_buffs(player2, player2_data)

    battle = BattleSystem([player1], [player2], bot_id)
    play_list, winner, status_list = battle.run_battle()

    if winner == 0:
        suc = player1_data["属性"]["nickname"]
    elif winner == 1:
        suc = player2_data["属性"]["nickname"]
    else:    # 平局处理
        suc = "没有人"

    if type_in == 2:
        update_all_user_status(status_list, bot_id)

    return play_list, suc


async def Boss_fight(user1, boss: dict, type_in=2, bot_id=0):
    """BOSS战斗"""
    # --- 1. 获取数据 ---
    player1_data = get_players_attributes(user1)  # 获取玩家数据
    boss_data = get_boss_attributes(boss, bot_id)  # 获取BOSS数据

    # --- 2. 初始化 ---
    player1 = Entity(player1_data["属性"], team_id=0)
    boss1 = Entity(boss_data["属性"], team_id=1, is_boss=True)

    apply_player_buffs(player1, player1_data)  # 添加buff和技能

    # boss添加buff
    boss1.start_skills.extend(generate_boss_buff(boss))

    if not boss['name'] == "稻草人":  # 稻草人不加技能
        # boss添加技能
        generate_boss_skill(boss1, [14001, 14002])  # 添加技能

    # --- 3. 运行 ---
    battle = BattleSystem([player1], [boss1], bot_id)
    play_list, winner, status_list = battle.run_battle()

    # 更新boss数据
    update_data_boss_status(boss, status_list)

    if winner == 0:
        suc = "群友赢了"
    else:
        suc = "Boss赢了"

    if type_in == 2:
        update_all_user_status(status_list, bot_id)  # 更新玩家数据

    return play_list, suc, boss


# ---------- 玩家数据部分 ----------
def get_players_attributes(user_id, level_ratios=None):
    """获取玩家数据"""
    # 获取用户所有装备功法buff数据
    buff_data_info = UserBuffDate(user_id).BuffInfo
    buffs = {}
    ratio = 1
    if level_ratios:
        ratio = level_ratios.get(user_id, 1)

    # 定义buff类型映射
    buff_types = {
        'main_buff': '主功法',
        'sub_buff': '辅修功法',
        'sec_buff': '神通技能',
        'effect1_buff': '身法',
        'effect2_buff': '瞳术',
        'faqi_buff': '法器',
        'armor_buff': '防具'
    }

    for key, display_name in buff_types.items():
        item_id = buff_data_info.get(key, 0)
        if item_id != 0:
            item_data = items.get_data_by_item_id(item_id)
            buffs[display_name] = item_data

    # 玩家属性
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
    atk = int((user_info['atk'] * (atkpractice + 1) * (1 + main_atk_buff) * (1 + weapon_atk_buff) * (
            1 + armor_atk_buff)) * (1 + impart_atk_per)) + int(buff_data_info.get('atk_buff', 0))
    crit = max(0, min(1, weapon_crit_buff + armor_crit_buff + main_crit_buff + impart_know_per))
    critatk = 1.5 + impart_burst_per + weapon_critatk + main_critatk
    dr = armor_def + weapon_def + main_def
    hit = 100  # 基础命中
    dodge = 0  # 基础闪避
    ap = 0  # 基础穿甲
    speed = 10  # 玩家基础速度

    attributes = {
        "user_id": user_id,  # 用户唯一标识符
        "nickname": user_info['user_name'],  # 用户昵称
        "max_hp": int(max_hp * ratio),  # 生命值上限
        "current_hp": int(hp * ratio),  # 当前生命值
        "max_mp": int(max_mp * ratio),  # 真元值上限
        "current_mp": int(mp * ratio),  # 当前真元值
        "mp_cost_modifier": weapon_mp_cost_modifier,  # 真元消耗修正
        "attack": int(atk * ratio),  # 攻击力
        "exp": int(user_info['exp'] * ratio),  # 当前经验值
        "critical_rate": crit,  # 暴击率 (0-1范围)
        "critical_damage": critatk,  # 暴击伤害倍数
        "boss_damage_bonus": boss_atk,  # 对BOSS伤害加成
        "damage_reduction": dr,  # 伤害减免率
        "armor_penetration": ap,  # 穿甲值
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
    根据 player_data 自动生成并添加各种 buff。
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


def generate_sub_buff(skill, buff_type_mapping):
    """根据技能配置自动生成 buff 列表"""

    name = skill["name"]
    buff_type_id = int(skill["buff_type"])
    v1 = float(skill["buff"]) / 100
    v2 = float(skill["buff2"]) / 100
    is_debuff = False
    if buff_type_id == 13 or buff_type_id == 14:
        v1 = skill["break"]
    if buff_type_id == 8 or buff_type_id == 10:
        is_debuff = True

    mapped = buff_type_mapping.get(buff_type_id)

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
    # 多个 buff 需要同时从 buff / buff2 取值
    if buff_type_id == 10:
        v1, v2 = 1, 1
    values = [v1, v2]

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
    """根据瞳术和身法配置自动生成 buff 列表"""
    buff_type_map = {
        "1": BuffType.EVASION_UP,
        "2": BuffType.ACCURACY_UP
    }

    low = int(data["buff"])
    high = int(data["buff2"])
    if low > high:
        low, high = high, low

    return [{
        "name": data["name"],
        "type": buff_type_map[data["buff_type"]],
        "value": random.randint(low, high),
        "coefficient": 1,
        "is_debuff": False,
        "duration": 99,
        "skill_type": 0
    }]


def generate_main_buff(data, weapon_id):
    """生成主功法相关的buff"""
    buffs = []

    # 判断ew是否大于0且等于武器ID
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

    # 判断random_buff是否为1
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
    遍历 status_list 更新所有玩家 hp/mp
    排除 user_id=0 和 user_id=bot_id
    hp/mp < 1 则替换为 1
    """
    for item in status_list:
        for name, attr in item.items():
            user_id = attr.get("user_id", 0)

            # 排除无效与机器人
            if user_id == 0 or user_id == bot_id:
                continue

            ratio = 1
            if level_ratios:
                ratio = level_ratios.get(user_id, 1)

            hp_multiplier = attr.get("hp_multiplier", 1)
            mp_multiplier = attr.get("mp_multiplier", 1)
            # 确保除数不为0，如果为0则使用1
            safe_hp_multiplier = hp_multiplier if hp_multiplier != 0 else 1
            safe_mp_multiplier = mp_multiplier if mp_multiplier != 0 else 1
            safe_ratio = ratio if ratio != 0 else 1

            hp = attr.get("hp", 1) / safe_hp_multiplier / safe_ratio
            mp = attr.get("mp", 1) / safe_mp_multiplier / safe_ratio

            # hp/mp 最小为 1
            if hp < 1:
                hp = 1
            if mp < 1:
                mp = 1

            # 更新数据库
            # print("test",user_id,int(hp),int(mp))
            sql_message.update_user_hp_mp(
                user_id,
                int(hp),
                int(mp)
            )


# ---------- BOSS数据部分 ----------
def get_boss_attributes(boss, bot_id):
    """获取boss数据"""
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
    """初始化BOSS的特殊buff (优化版)"""
    # 初始化buff字典
    boss_buff = {
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
        'boss_sb': 0
    }

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

    boss_level = boss["jj"]

    # 1. 预计算当前BOSS的境界值，简化后续判断
    current_rank_val = convert_rank(boss_level + '中期')[0]

    def get_rank_val(name):
        return convert_rank(name)[0]

    # 2. 定义辅助函数：处理随机属性组 (每组4个选项，等概率随机一个)
    def apply_random_group(attr_names, value_options):
        """
        attr_names: 属性名列表 ['boss_zs', 'boss_hx', 'boss_bs', 'boss_xx']
        value_options: 对应的值列表，支持固定值或函数(lambda)
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

    # 祭道境 (最高级)
    if boss_level == "祭道境" or current_rank_val < get_rank_val('祭道境初期'):
        cfg = {
            'js': 0.05,
            'cj': (25, 50),
            # 对应: zs, hx, bs, xx
            'g1': [1, 0.7, 2, 1],
            # 对应: jg, jh, jb, xl
            'g2': [0.7, 0.7, 1.5, 1]
        }

    # 至尊 ~ 斩我 (中级)
    elif get_rank_val('至尊境初期') < current_rank_val < get_rank_val('斩我境圆满'):
        cfg = {
            'js': (50, 55),
            'cj': (15, 30),
            'g1': [0.3, 0.1, 0.5, lambda: random.randint(5, 100) / 100],
            'g2': [0.3, 0.3, 0.5, lambda: random.randint(5, 100) / 100]
        }

    # 微光 ~ 遁一
    elif get_rank_val('微光境初期') < current_rank_val < get_rank_val('遁一境圆满'):
        cfg = {
            'js': (40, 45),
            'cj': (20, 40),
            'g1': [0.4, 0.2, 0.7, lambda: random.randint(10, 100) / 100],
            'g2': [0.4, 0.4, 0.7, lambda: random.randint(10, 100) / 100]
        }

    # 星芒 ~ 至尊
    elif get_rank_val('星芒境初期') < current_rank_val < get_rank_val('至尊境圆满'):
        cfg = {
            'js': (30, 35),
            'cj': (20, 40),
            'g1': [0.6, 0.35, 1.1, lambda: random.randint(30, 100) / 100],
            'g2': [0.5, 0.5, 0.9, lambda: random.randint(30, 100) / 100]
        }

    # 月华 ~ 微光
    elif get_rank_val('月华境初期') < current_rank_val < get_rank_val('微光境圆满'):
        cfg = {
            'js': (20, 25),
            'cj': (20, 40),
            'g1': [0.7, 0.45, 1.3, lambda: random.randint(40, 100) / 100],
            'g2': [0.55, 0.6, 1.0, lambda: random.randint(40, 100) / 100]
        }

    # 耀日 ~ 星芒
    elif get_rank_val('耀日境初期') < current_rank_val < get_rank_val('星芒境圆满'):
        cfg = {
            'js': (10, 15),
            'cj': (25, 45),
            'g1': [0.85, 0.5, 1.5, lambda: random.randint(50, 100) / 100],
            'g2': [0.6, 0.65, 1.1, lambda: random.randint(50, 100) / 100]
        }

    # 祭道 ~ 月华
    elif get_rank_val('祭道境初期') < current_rank_val < get_rank_val('月华境圆满'):
        cfg = {
            'js': 0.1,
            'cj': (25, 45),
            'g1': [0.9, 0.6, 1.7, lambda: random.randint(60, 100) / 100],
            'g2': [0.62, 0.67, 1.2, lambda: random.randint(60, 100) / 100]
        }

    # 4. 统一应用配置
    if cfg:
        # 应用减伤 (JS) - 支持固定值或随机范围
        if isinstance(cfg['js'], tuple):
            boss_buff['boss_js'] = random.randint(*cfg['js']) / 100
        else:
            boss_buff['boss_js'] = cfg['js']

        # 应用暴击 (CJ)
        boss_buff['boss_cj'] = random.randint(*cfg['cj']) / 100

        # 应用两组随机属性
        apply_random_group(['boss_zs', 'boss_hx', 'boss_bs', 'boss_xx'], cfg['g1'])
        apply_random_group(['boss_jg', 'boss_jh', 'boss_jb', 'boss_xl'], cfg['g2'])

    else:
        # 低级BOSS / 默认处理
        boss_buff['boss_js'] = 1.0
        boss_buff['boss_cj'] = 0
        # 其他属性默认为0，已初始化

    # 计算BOSS闪避率 和 减伤率
    boss_buff['boss_sb'] = int((1 - boss_buff['boss_js']) * 100 * random.uniform(0.1, 0.5))
    boss_buff['boss_js'] = 1 - boss_buff['boss_js']

    result = []

    for key, value in boss_buff.items():
        if value == 0:
            continue  # 跳过无效果

        if key not in boss_buff:
            continue

        effect_type, effect_name = boss_buff_map[key]

        # 修复判断逻辑：判断是否是DebuffType枚举
        is_debuff = isinstance(effect_type, DebuffType)

        result.append({
            "name": effect_name,
            "type": effect_type,
            "value": value,
            "is_debuff": is_debuff
        })

    return result


def load_json_file(filename="data.json"):
    """加载BOSS神通"""
    filepath = Path() / "data" / "xiuxian" / "功法" / filename

    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


skill_data_cache = None  # 全局缓存


def get_skill_data():
    """获取技能数据（带缓存）"""
    global skill_data_cache
    if skill_data_cache is None:
        skill_data_cache = load_json_file("boss神通.json")
    return skill_data_cache


def generate_boss_skill(enemy, skills):
    skill_data = get_skill_data()  # 第一次加载，后续使用缓存
    for skill in skills:
        skill_str = str(skill)
        if skill_str not in skill_data:
            continue
        enemy.skills.append(Skill(skill_data[skill_str]))


def update_data_boss_status(data, status_list):
    target_name = data["name"]

    for item in status_list:
        for name, attr in item.items():
            if name == target_name:
                # 将 status 中的 hp/mp 写回 data
                data["气血"] = attr.get("hp", data.get("气血"))
                data["真元"] = attr.get("mp", data.get("真元"))
                return True
    return False


# ---------- 战斗部分 ----------
class SkillType(IntEnum):
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

    TRIGGER_HP_BELOW = 104  # 血量低于阈值时触发
    FIELD = 105  # 领域类型


class TargetType(IntEnum):
    SINGLE = 1  # 单体
    AOE = 2  # 群体
    MULTI = 3  # 固定数量多目标


class BuffType(IntEnum):
    """增益效果类型枚举类"""
    # 基础属性增益
    ATTACK_UP = 1  # 攻击提升
    DEFENSE_UP = 2  # 防御提升
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


class DebuffType(IntEnum):
    """减益效果类型枚举类"""
    # 属性降低类
    ATTACK_DOWN = 1  # 攻击力降低
    CRIT_RATE_DOWN = 2  # 暴击率降低
    CRIT_DAMAGE_DOWN = 3  # 暴击伤害降低
    DEFENSE_DOWN = 4  # 防御降低
    ACCURACY_DOWN = 5  # 命中率降低
    EVASION_DOWN = 6  # 闪避率降低
    LIFESTEAL_DOWN = 7  # 生命偷取降低
    MANA_STEAL_DOWN = 8  # 法力偷取降低
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
    4: BuffType.HP_REGEN_PERCENT,  # 气血回复
    5: BuffType.MP_REGEN_PERCENT,  # 真元回复
    6: BuffType.LIFESTEAL_UP,  # 吸气血
    7: BuffType.MANA_STEAL_UP,  # 吸真元
    8: DebuffType.POISON_DOT,  # 中毒
    9: [BuffType.LIFESTEAL_UP, BuffType.MANA_STEAL_UP],  # 双吸（同时提升两种偷取）
    10: [DebuffType.LIFESTEAL_BLOCK, DebuffType.MANA_STEAL_BLOCK],  # 禁止吸取（同时禁止两种偷取）
    11: BuffType.DEBUFF_IMMUNITY,  # 抵消
    12: "",  # 聚宝
    13: BuffType.ARMOR_PENETRATION_UP,  # 斗战
    14: BuffType.ARMOR_PENETRATION_UP  # 穿甲
}

BUFF_DESC_TEMPLATES = {
    BuffType.ATTACK_UP: "攻击力提升 {value}",
    BuffType.DEFENSE_UP: "防御力提升 {value}",
    BuffType.CRIT_RATE_UP: "会心率提升 {value}",
    BuffType.CRIT_DAMAGE_UP: "会心伤害提升 {value}",
    BuffType.DAMAGE_REDUCTION_UP: "伤害减免提升 {value}",
    BuffType.ARMOR_PENETRATION_UP: "护甲穿透提升 {value}",
    BuffType.ACCURACY_UP: "命中率提升 {value}",
    BuffType.EVASION_UP: "闪避率提升 {value}",
    BuffType.LIFESTEAL_UP: "生命偷取提升 {value}",
    BuffType.MANA_STEAL_UP: "法力偷取提升 {value}",
    BuffType.DEBUFF_IMMUNITY: "获得免疫减益效果",
    BuffType.HP_REGEN_PERCENT: "每回合回复 {value} 生命值",
    BuffType.MP_REGEN_PERCENT: "每回合回复 {value} 法力值",
    BuffType.REFLECT_DAMAGE: "反弹 {value} 伤害",
    BuffType.SHIELD: "获得 {value} 点护盾",
}

DEBUFF_DESC_TEMPLATES = {
    DebuffType.ATTACK_DOWN: "攻击力降低 {value}",
    DebuffType.CRIT_RATE_DOWN: "会心率降低 {value}",
    DebuffType.CRIT_DAMAGE_DOWN: "会心伤害降低 {value}",
    DebuffType.DEFENSE_DOWN: "防御力降低 {value}",
    DebuffType.ACCURACY_DOWN: "命中率降低 {value}",
    DebuffType.EVASION_DOWN: "闪避率降低 {value}",
    DebuffType.LIFESTEAL_DOWN: "生命偷取降低 {value}",
    DebuffType.MANA_STEAL_DOWN: "法力偷取降低 {value}",
    DebuffType.LIFESTEAL_BLOCK: "无法进行生命偷取",
    DebuffType.MANA_STEAL_BLOCK: "无法进行法力偷取",

    DebuffType.POISON_DOT: "中毒，每回合受到 {value} 点伤害",
    DebuffType.SKILL_DOT: "持续技能伤害，每回合受到 {value} 点伤害",
    DebuffType.BLEED_DOT: "流血，每回合受到 {value} 点伤害",
    DebuffType.BURN_DOT: "灼烧，每回合受到 {value} 点伤害",

    DebuffType.FATIGUE: "陷入疲劳状态",
    DebuffType.STUN: "眩晕，无法行动",
    DebuffType.FREEZE: "冰冻，无法行动",
    DebuffType.PETRIFY: "石化，无法行动",
    DebuffType.SLEEP: "睡眠，无法行动",
    DebuffType.ROOT: "定身，无法行动",
    DebuffType.FEAR: "恐惧，无法行动",
    DebuffType.SEAL: "封印，无法行动",
    DebuffType.PARALYSIS: "麻痹，无法行动",
    DebuffType.SILENCE: "沉默，无法施放法术",
}

VALID_FIELDS = {"name", "type", "value", "coefficient", "is_debuff", "duration", "skill_type"}


class StatusEffect:
    def __init__(self, name, effect_type, value, coefficient, is_debuff, duration=99, skill_type=0):
        self.name = name  # 技能名称
        self.type = effect_type  # 效果类型
        self.value = value  # 效果数值
        self.coefficient = coefficient  # 效果系数
        self.is_debuff = is_debuff  # 是否为负面效果（True为负面，False为正面）
        self.duration = duration  # 效果持续回合数
        self.skill_type = skill_type  # 技能类型

    def __repr__(self):  # 定义对象的字符串表示形式
        # 返回可读的状态效果信息
        return f"[{'Debuff' if self.is_debuff else 'Buff'}:{self.name}|{self.type}|{self.value}|{self.duration}|{self.skill_type}]"


class Skill:
    def __init__(self, data):
        self.name = data.get("name")  # 技能名称
        self.desc = data.get("desc", "")  # 技能介绍
        self.skill_type = int(data.get("skill_type", 1))  # 技能类型
        self.target_type = int(data.get("target_type", 1))  # 目标类型
        self.multi_count = int(data.get("multi_count", 1))  # 目标数量
        self.hp_condition = float(data.get("hp_condition", 1))  # 触发血量

        # 消耗
        self.hp_cost_rate = float(data.get("hpcost", 0))  # 消耗气血
        self.mp_cost_rate = float(data.get("mpcost", 0))  # 消耗真元

        # 通用参数
        self.turn_cost = int(data.get("turncost", 0))  # 持续回合 或 休息回合
        self.rate = float(data.get("rate", 0))  # 触发率
        self.cd = float(data.get("cd", 0))  # 触发率
        self.remain_cd = float(data.get("remain_cd", 0))  # 剩余冷却（回合）

        # 类型特定参数
        self.atk_values = data.get("atkvalue", [])  # 攻击参数 1
        self.atk_coefficient = float(data.get("atkvalue2", 0))  # 攻击参数 2
        self.skill_buff_type = int(data.get("bufftype", 0))  # BUFF类型
        self.skill_buff_value = float(data.get("buffvalue", 0))  # BUFF参数
        self.success_rate = float(data.get("success", 0))  # 概率参数
        self.skill_content = data.get("skill_content", [])  # 随机神通参数

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

        # 进阶属性
        self.base_atk = float(data.get("attack", 1))  # 基础攻击
        self.base_crit = float(data.get("critical_rate", 0))  # 基础暴击率
        self.base_crit_dmg = float(data.get("critical_damage", 1.5))  # 基础暴击伤害倍数
        self.base_damage_reduction = float(data.get("damage_reduction", 0))  # 基础减伤
        self.base_armor_pen = float(data.get("armor_penetration", 0))  # 基础穿甲
        self.base_accuracy = float(data.get("accuracy", 100))  # 基础命中率
        self.base_dodge = float(data.get("dodge", 0))  # 基础闪避率
        self.base_speed = float(data.get("speed", 10))  # 基础速度

        # 状态管理
        self.buffs = []
        self.debuffs = []
        # 初始buff配置 (用于Round One)
        self.start_skills = data.get("start_skills", [])
        self.skills = data.get("skills", [])  # 存放技能参数
        self.total_dmg = 0

    # -------- buff管理函数 --------

    def has_buff(self, field: str, value) -> bool:
        """
        检查 buffs 中是否存在某个字段等于指定值的 buff
        """
        if field not in VALID_FIELDS:
            raise ValueError(f"unsupported field '{field}'. valid fields: {VALID_FIELDS}")

        return any(getattr(buff, field, None) == value for buff in self.buffs)

    def has_debuff(self, field: str, value) -> bool:
        """
        检查 debuffs 中是否存在某个字段等于指定值的 debuff
        """
        if field not in VALID_FIELDS:
            raise ValueError(f"unsupported field '{field}'. valid fields: {VALID_FIELDS}")

        return any(getattr(debuff, field, None) == value for debuff in self.debuffs)

    def get_buff_field(self, match_field: str, return_field: str, match_value):
        """
        在 buffs 中查找 match_field == match_value 的效果，
        找到后返回 return_field 的值。
        """
        if match_field not in VALID_FIELDS or return_field not in VALID_FIELDS:
            raise ValueError(f"unsupported field. valid fields: {VALID_FIELDS}")

        for buff in self.buffs:
            if getattr(buff, match_field, None) == match_value:
                return getattr(buff, return_field, None)

        return None  # 找不到则返回 None

    def get_debuff_field(self, match_field: str, return_field: str, match_value):
        """
        在 debuffs 中查找 match_field == match_value 的效果，
        找到后返回 return_field 的值。
        """
        if match_field not in VALID_FIELDS or return_field not in VALID_FIELDS:
            raise ValueError(f"unsupported field. valid fields: {VALID_FIELDS}")

        for debuff in self.debuffs:
            if getattr(debuff, match_field, None) == match_value:
                return getattr(debuff, return_field, None)

        return None  # 找不到则返回 None

    def set_buff_field(self, match_field: str, target_field: str, match_value, new_value) -> bool:
        """
        在 buffs 中查找 match_field == match_value 的效果，
        并将 target_field 的值修改为 new_value。
        返回 True 表示修改成功，False 表示未找到。
        """
        if match_field not in VALID_FIELDS or target_field not in VALID_FIELDS:
            raise ValueError(f"unsupported field. valid fields: {VALID_FIELDS}")

        for buff in self.buffs:
            if getattr(buff, match_field, None) == match_value:
                setattr(buff, target_field, new_value)
                return True
        return False  # 没找到

    def set_debuff_field(self, match_field: str, target_field: str, match_value, new_value) -> bool:
        """
        在 debuffs 中查找 match_field == match_value 的效果，
        并将 target_field 的值修改为 new_value。
        返回 True 表示修改成功，False 表示未找到。
        """
        if match_field not in VALID_FIELDS or target_field not in VALID_FIELDS:
            raise ValueError(f"unsupported field. valid fields: {VALID_FIELDS}")

        for debuff in self.debuffs:
            if getattr(debuff, match_field, None) == match_value:
                setattr(debuff, target_field, new_value)
                return True
        return False  # 没找到

    def get_buffs(self, field: str, value):
        """
        根据任意字段获取所有匹配的 buff 列表。
        field 必须属于 VALID_FIELDS。
        """
        if field not in VALID_FIELDS:
            raise ValueError(f"unsupported field '{field}'. valid fields: {VALID_FIELDS}")

        return [b for b in self.buffs if getattr(b, field, None) == value]

    def get_debuffs(self, field: str, value):
        """
        根据任意字段获取所有匹配的 debuff 列表。
        """
        if field not in VALID_FIELDS:
            raise ValueError(f"unsupported field '{field}'. valid fields: {VALID_FIELDS}")

        return [d for d in self.debuffs if getattr(d, field, None) == value]

    def get_buff(self, field: str, value):
        """返回第一个匹配的 buff，没有则返回 None"""
        buffs = self.get_buffs(field, value)
        return buffs[0] if buffs else None

    def get_debuff(self, field: str, value):
        """返回第一个匹配的 debuff，没有则返回 None"""
        debuffs = self.get_debuffs(field, value)
        return debuffs[0] if debuffs else None

    # -------- 数值类计算 --------

    def _get_effect_value(self, buff_type, debuff_type=None):
        """计算 (所有增益值 - 所有减益值)"""
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
        """混合计算：增益加法叠加，减益乘法叠加"""
        # 增益部分：加法叠加
        buff_sum = 0.0
        for b in self.buffs:
            if b.type == buff_type:
                buff_sum += b.value  # 直接相加，假设value是百分比

        # 计算基础系数 (1 + 总增益百分比)
        multiplier = 0 + buff_sum

        # 减益部分：乘法叠加
        if debuff_type:
            for d in self.debuffs:
                if d.type == debuff_type:
                    multiplier *= (1 - d.value)  # 乘法叠加

        return multiplier

    def update_stat(self, stat: str, op: int, value: float):
        """
        stat: "hp" 或 "mp"
        op: 1=加，2=减
        value: 数值
        """
        if stat not in ("hp", "mp"):
            raise ValueError("stat 必须是 'hp' 或 'mp'")
        # 选择对应属性
        current = getattr(self, stat)
        max_value = getattr(self, f"max_{stat}")
        # 操作加减
        if op == 1:  # 加
            current += value
        elif op == 2:  # 减
            current -= value
        else:
            raise ValueError("op 必须是 1(加) 或 2(减)")
        # 限制范围：0 ~ 最大值
        current = min(current, max_value)
        # 更新属性
        setattr(self, stat, current)

    def pay_cost(self, hp_cost, mp_cost, deduct=False):
        if self.hp <= hp_cost or self.mp < mp_cost:
            return False
        if deduct:
            self.hp -= hp_cost
            self.mp -= mp_cost
        return True

    def show_bar(self, stat: str, length: int = 10):
        """
        显示一个血条或蓝条
        stat: 'hp' 或 'mp'
        length: 血条长度（单位：字符）
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
        # 打印
        return f"{self.name}剩余血量{number_to(int(current_data))}\n{stat.upper()} {bar} {int(ratio * 100)}%"

    @property
    def is_alive(self):
        return self.hp > 0

    @property
    def atk_rate(self):
        # 攻击力 = 基础 * (1 + 攻击提升Buff - 攻击降低Debuff)
        pct = self._get_effect_value(BuffType.ATTACK_UP, DebuffType.ATTACK_DOWN)
        return max(0, self.base_atk * (1 + pct))

    @property
    def crit_rate(self):
        # 暴击率 = 基础 + 暴击Buff - 暴击Debuff
        val = self.base_crit + self._get_effect_value(BuffType.CRIT_RATE_UP, DebuffType.CRIT_RATE_DOWN)
        return max(0, val)

    @property
    def crit_dmg_rate(self):
        # 暴击伤害 = 基础 + 暴击伤害Buff - 暴击伤害Debuff
        val = self.base_crit_dmg + self._get_effect_value(BuffType.CRIT_DAMAGE_UP, DebuffType.CRIT_DAMAGE_DOWN)
        return max(0, val)

    @property
    def damage_reduction_rate(self):
        # 减伤率 = 基础 + 减伤Buff
        # 注意：这里假设 defense_down 会减少减伤率
        val = self.base_damage_reduction + self._get_effect_value(BuffType.DAMAGE_REDUCTION_UP)
        return min(0.95, val)  # 限制范围 0% - 95%

    @property
    def armor_pen_rate(self):
        # 穿甲
        val = self.base_armor_pen + self._get_effect_value(BuffType.ARMOR_PENETRATION_UP)
        return max(0, val)

    @property
    def accuracy_rate(self):
        # 命中率
        val = self.base_accuracy + self._get_effect_value(BuffType.ACCURACY_UP)
        return max(0, val)

    @property
    def dodge_rate(self):
        # 闪避
        val = self.base_dodge + self._get_effect_value(BuffType.EVASION_UP)
        return min(180, max(0, val))

    @property
    def lifesteal_rate(self):
        # 基础生命偷取假设为0，完全靠Buff
        if self.has_debuff("type", DebuffType.LIFESTEAL_BLOCK):
            return 0
        val = self._get_effect_value_mixed(BuffType.LIFESTEAL_UP, DebuffType.LIFESTEAL_DOWN)
        return max(0, val)

    @property
    def mana_steal_rate(self):
        # 基础法力偷取假设为0，完全靠Buff
        if self.has_debuff("type", DebuffType.MANA_STEAL_BLOCK):
            return 0
        val = self._get_effect_value_mixed(BuffType.MANA_STEAL_UP, DebuffType.MANA_STEAL_DOWN)
        return max(0, val)

    @property
    def poison_dot_dmg(self):
        """所有中毒伤害的总和（基于当前生命值）"""
        total = 0.0
        for debuff in self.debuffs:
            if debuff.type == DebuffType.POISON_DOT:
                # 假设debuff.value是百分比（如0.05表示5%）
                total += self.hp * debuff.value
        return int(total)

    @property
    def hp_regen_rate(self):
        """所有HP恢复的总和（基于最大生命值）"""
        total = 0.0
        for buff in self.buffs:
            if buff.type == BuffType.HP_REGEN_PERCENT:
                # 假设debuff.value是百分比（如0.05表示5%）
                total += self.max_hp * buff.value
        return int(total)

    @property
    def mp_regen_rate(self):
        """所有MP恢复的总和（基于最大生命值）"""
        total = 0.0
        for buff in self.buffs:
            if buff.type == BuffType.MP_REGEN_PERCENT:
                # 假设debuff.value是百分比（如0.05表示5%）
                total += self.max_mp * buff.value
        return int(total)

    # --- 状态管理 ---
    def remove_skill_by_name(self, skill_name):
        """删除指定名称的技能"""
        for i, skill in enumerate(self.skills):
            if skill.name == skill_name:
                del self.skills[i]
                return True
        return False

    def has_skill(self, skill_name):
        """检查是否拥有某个技能"""
        return any(skill.name == skill_name for skill in self.skills)

    def check_and_clear_debuffs_by_immunity(self):
        # 检查是否有debuff免疫效果，如果有则清空所有debuffs
        if self.has_buff("type", BuffType.DEBUFF_IMMUNITY):
            self.debuffs.clear()

    def add_status(self, effect):
        if effect.is_debuff:
            self.debuffs.append(effect)
        else:
            self.buffs.append(effect)

    def update_status_effects(self):
        # 处理技能CD
        for skill in self.skills[:]:
            skill.tick_cd()

        # 处理 Buff
        for buff in self.buffs[:]:  # 用 [:] 防止删除时影响遍历
            buff.duration -= 1
            if buff.duration < 0:
                self.buffs.remove(buff)

        # 处理 Debuff
        for debuff in self.debuffs[:]:
            debuff.duration -= 1
            if debuff.duration < 0:
                self.debuffs.remove(debuff)


# --- 战斗引擎 (核心逻辑整合) ---
class BattleSystem:
    def __init__(self, team_a, team_b, bot_id):
        self.bot_id = bot_id
        self.team_a = team_a
        self.team_b = team_b
        self.play_list = []
        self.round = 0
        self.max_rounds = 50

    def add_message(self, unit, message):
        """添加战斗消息"""
        msg_dict = {
            "type": "node",
            "data": {
                "name": f"{unit.name} 当前血量：{number_to(int(unit.hp))} / {number_to(int(unit.max_hp))}",
                "uin": int(unit.id),
                "content": message
            }
        }
        self.play_list.append(msg_dict)

    def add_system_message(self, message):
        """添加系统消息"""
        msg_dict = {
            "type": "node",
            "data": {
                "name": "Bot",
                "uin": int(self.bot_id),
                "content": message
            }
        }
        self.play_list.append(msg_dict)

    def get_effect_desc(self, effect_type, is_db, value=None):
        """buff统一生成显示文本"""
        if effect_type in BUFF_DESC_TEMPLATES and not is_db:
            template = BUFF_DESC_TEMPLATES[effect_type]
        elif effect_type in DEBUFF_DESC_TEMPLATES:
            template = DEBUFF_DESC_TEMPLATES[effect_type]
        else:
            return "未知效果"

        if value is None:
            return template

        return template.format(value=value)

    def add_after_last_damage(self, msg, add_text):
        """
        在最后一个"伤害！"后面添加指定字符串
        """
        # 使用partition从右边分割，避免索引错误
        before_last, separator, after_last = msg.rpartition("伤害！")

        if separator:  # 找到了"伤害！"
            return before_last + "伤害！" + add_text + after_last
        else:  # 没有找到"伤害！"
            return msg

    def _calc_raw_damage(self, attacker, defender, multiplier, penetration=False):
        """基础伤害计算公式"""
        # 命中判定
        status = "Hit"
        if random.uniform(0, 100) > (attacker.accuracy_rate - defender.dodge_rate):
            status = "Miss"

        # 暴击判定
        is_crit = random.random() < attacker.crit_rate
        crit_mult = attacker.crit_dmg_rate if is_crit else 1.0

        if defender.damage_reduction_rate < 0:
            dr_eff = defender.damage_reduction_rate  # 负减伤（伤害加深）
        elif penetration:
            dr_eff = 0  # 完全无视减伤
        else:
            dr_eff = max(0, defender.damage_reduction_rate - attacker.armor_pen_rate)  # 减伤率减去穿透，且不低于0
        # 伤害公式: 攻击 * 倍率 * 暴击 * (1 - (敌方减伤 - 我方穿甲))
        damage = attacker.atk_rate * multiplier * crit_mult * (1 - dr_eff)

        # BOSS加成
        if defender.is_boss:
            damage *= (1 + attacker.boss_damage)

        # 伤害浮动 - 添加0.95到1.05的随机浮动，使伤害结果更自然
        damage *= random.uniform(0.95, 1.05)

        return int(damage), is_crit, status

    def _get_all_enemies(self, entity):
        """获取指定实体的所有敌方单位（存活状态）"""
        if entity.team_id == 0:
            # 如果实体在队伍0，返回队伍1的所有存活单位
            return [e for e in self.team_b if e.is_alive]
        else:
            # 如果实体在队伍1，返回队伍0的所有存活单位
            return [e for e in self.team_a if e.is_alive]

    def _get_all_allies(self, entity):
        """获取指定实体的所有友方单位（存活状态，不包括自己）"""
        if entity.team_id == 0:
            # 队伍0的所有存活单位，排除自己
            return [e for e in self.team_a if e.is_alive and e.id != entity.id]
        else:
            # 队伍1的所有存活单位，排除自己
            return [e for e in self.team_b if e.is_alive and e.id != entity.id]

    def _apply_round_one_skills(self, caster, targets, skills_dict):
        """
        处理开局技能字典
        caster: 施法者
        targets: 目标列表（单个或多个）
        skills_dict: 技能字典 {{'type':..., 'value':..., 'is_debuff':...}}
        """
        if not skills_dict:
            return

        for data in skills_dict:
            name = data['name']
            b_type = data['type']
            val = data['value']
            is_db = data['is_debuff']

            if is_db and caster.type == "minion":
                continue

            # 创建效果对象
            effect = StatusEffect(name, b_type, val, 1, is_db, duration=99, skill_type=0)

            if is_db:
                for target in targets:
                    target.add_status(effect)
            else:
                # 是 Buff -> 给自己
                caster.add_status(effect)

            val_msg = None
            if val > 0:
                val_msg = f"{val * 100:.0f}%"
            if not is_db and (data['type'] == BuffType.ACCURACY_UP or data['type'] == BuffType.EVASION_UP):
                val_msg = f"{val:.0f}%"

            buff_msg = self.get_effect_desc(b_type, is_db, val_msg)
            msg = f"{caster.name}使用{name}，{buff_msg}"
            if caster.type != "minion":
                self.add_message(caster, msg)

    def choose_skill(self, caster, skills, enemies):
        usable_skills = []
        # ---------- 先过滤不可用技能 ----------
        for sk in skills:
            if sk.skill_type == SkillType.RANDOM_ACQUIRE:  # Type 7: 随机获取技能
                skill_id = random.choice(sk.skill_content)
                skill_data = items.get_data_by_item_id(skill_id)
                sk_data = Skill(skill_data)
                caster.skills.append(sk_data)  # 添加随机的技能
                caster.remove_skill_by_name(sk.name)  # 删除当前技能
                skill_data_name = skill_data["name"]
                self.add_message(caster, f"{sk.desc} 随机获得了{skill_data_name}神通!")
                if self._skill_available(caster, sk_data, enemies):
                    usable_skills.append(sk_data)
            elif self._skill_available(caster, sk, enemies):
                usable_skills.append(sk)
        if not usable_skills:
            return None  # 没技能可用，普攻/跳过

        # ---------- 触发血量类型技能优先 ----------
        not_hp1_skills = [sk for sk in usable_skills if sk.hp_condition != 1]
        if not_hp1_skills:
            return not_hp1_skills[0]  # 优先返回hp_condition不等于1的技能

        # ---------- BUFF 技能优先 ----------
        buff_list = [sk for sk in usable_skills if sk.skill_type == SkillType.BUFF_STAT]
        if buff_list:
            return buff_list[0]  # 或按权重选择

        # ---------- 随机技能 ----------
        return random.choice(usable_skills)

    def _skill_available(self, caster, skill, enemies):
        """
        判断技能是否可以被使用：
        1. 冷却
        2. HP/MP 消耗
        3. DOT 技能是否重复
        4. BUFF 是否重复
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
        if not caster.pay_cost(hp_cost, mp_cost, deduct=False):
            return False

        # ---------- 4. 技能：检查是否所有敌人都已经有这个debuff ----------
        if skill.skill_type in (SkillType.DOT, SkillType.CC, SkillType.CONTROL):
            enemies_without_debuff = [e for e in enemies if not e.has_debuff("name", skill.name)]
            if not enemies_without_debuff:
                return False

        # ---------- 5. BUFF 技能：不能重复施放相同 Buff ----------
        if skill.skill_type == SkillType.BUFF_STAT or skill.skill_type == SkillType.STACK_BUFF:
            if caster.has_buff("name", skill.name):
                return False

        return True

    def _select_targets(self, enemies, skill, is_boss=False):
        alive = [e for e in enemies if e.is_alive]

        if skill.target_type == TargetType.SINGLE:
            if skill.skill_type == SkillType.DOT:
                alive = [a for a in alive if not a.has_debuff("name", skill.name)]
            if is_boss:
                return random.sample(alive, k=1)  # boss攻击随机挑选
            return [min(alive, key=lambda x: x.hp)]  # 玩家攻击选血最少


        elif skill.target_type == TargetType.AOE:
            return alive  # 所有敌人

        elif skill.target_type == TargetType.MULTI:
            if skill.skill_type == SkillType.DOT:
                alive = [a for a in alive if not a.has_debuff("name", skill.name)]
            n = getattr(skill, 'multi_count', 2)
            # 按血量排序取前 N 个

            if is_boss:
                return random.sample(alive, k=n)  # boss攻击随机挑选
            return sorted(alive, key=lambda x: x.hp)[:n]  # 玩家攻击选血最少

        return []

    def _execute_skill(self, caster, targets, skill):
        """
        处理开局技能字典
        caster: 施法者
        targets: 目标列表（单个或多个）
        skills_dict: 技能字典 {'name': {'type':..., 'value':..., 'is_debuff':...}}
        """

        # 计算释放概率
        if not random.uniform(0, 100) <= skill.rate:
            skill_msg, total_dmg = self._normal_attack(caster, min(targets, key=lambda x: x.hp))
            return skill_msg, total_dmg

        # 计算消耗
        hp_cost = caster.hp * skill.hp_cost_rate
        mp_cost = caster.exp * skill.mp_cost_rate * (1 - caster.mp_cost_modifier)
        caster.pay_cost(hp_cost, mp_cost, deduct=True)  # 扣除消耗

        parts = []
        if hp_cost > 0:
            parts.append(f"气血{number_to(int(hp_cost))}点")
        if mp_cost > 0:
            parts.append(f"真元{number_to(int(mp_cost))}点")
        if parts:  # 如果有消耗
            cost_msg = f"消耗{'、'.join(parts)}，"
        else:  # 没有消耗
            cost_msg = ""

        skill_msg = f"{skill.desc} {cost_msg}"
        total_dmg = 0  # 记录总伤害
        skill.trigger_cd()  # 添加cd

        # --- 核心逻辑分支 (对应你的6种类型) ---
        # Type 1: 连续攻击 (Multi-Hit)
        if skill.skill_type == SkillType.MULTI_HIT:
            hits = skill.atk_values if isinstance(skill.atk_values, list) else [skill.atk_values]
            skill_msg += f"对{targets[0].name}造成"
            for mult in hits:  # 遍历每一次攻击
                # 计算单次攻击伤害
                dmg, is_crit, status = self._calc_raw_damage(caster, targets[0], float(mult))
                if status == "Hit":
                    crit_str = "💥" if is_crit else ""
                    skill_msg += f"{crit_str}{number_to(int(dmg))}伤害、"
                    targets[0].update_stat("hp", 2, dmg)
                    total_dmg += dmg
                else:
                    skill_msg += f"miss、"

            if total_dmg > 0:
                skill_msg = skill_msg[:-1] + "！"
            else:
                skill_msg = f"{caster.name}的技能被{targets[0].name}闪避了！"

            if skill.turn_cost > 0:  # 释放后回气休息
                effect = StatusEffect(skill.name, DebuffType.FATIGUE, 0, 1, True, skill.turn_cost, skill.skill_type)
                caster.add_status(effect)
                skill_msg += f"\n{caster.name}力竭，需休息{skill.turn_cost}回合"
            return skill_msg, total_dmg

        # Type 2: 持续伤害 (DoT)
        elif skill.skill_type == SkillType.DOT:
            # dot_damage = skill.atk_values * caster.atk_rate  # 攻击倍率 × 实时攻击力

            target_names = []
            for target in targets:
                target_names.append(target.name)
                effect = StatusEffect(skill.name, DebuffType.SKILL_DOT, skill.atk_values, caster.name, True,
                                      skill.turn_cost,
                                      skill.skill_type)
                target.add_status(effect)
            target_name_msg = "、".join(target_names)
            skill_msg += f"对{target_name_msg}造成每回合{skill.atk_values}倍攻击力持续伤害，持续{skill.turn_cost}回合"
            return skill_msg, total_dmg

        # Type 3: 属性增益 (Stat Buff / Damage Reduction)
        elif skill.skill_type == SkillType.BUFF_STAT:
            if skill.skill_buff_type == 1:  # 攻击力增加
                effect = StatusEffect(skill.name, BuffType.ATTACK_UP, skill.skill_buff_value, 1, False, skill.turn_cost,
                                      skill.skill_type)
                caster.add_status(effect)  # 给自己添加Buff
                skill_msg += f"提升了{skill.skill_buff_value * 100:.0f}%攻击力，持续{skill.turn_cost}回合（剩余{skill.turn_cost - 1}回合）\n"
            elif skill.skill_buff_type == 2:  # 减伤加成
                effect = StatusEffect(skill.name, BuffType.DAMAGE_REDUCTION_UP, skill.skill_buff_value, 1, False,
                                      skill.turn_cost, skill.skill_type)
                caster.add_status(effect)  # 给自己添加Buff
                skill_msg += f"提升了{skill.skill_buff_value * 100:.0f}%伤害减免，持续{skill.turn_cost}回合（剩余{skill.turn_cost - 1}回合）\n"
            attack_msg, total_dmg = self._normal_attack(caster, targets[0])
            skill_msg += attack_msg
            return skill_msg, total_dmg

        # Type 4: 封印/控制 (Control)
        elif skill.skill_type == SkillType.CONTROL:
            chance = skill.success_rate
            target_names_success = []
            target_names_failure = []
            for target in targets:
                if random.uniform(0, 100) <= chance:
                    effect = StatusEffect(skill.name, DebuffType.SEAL, 0, 1, True, skill.turn_cost, skill.skill_type)
                    target.add_status(effect)
                    target_names_success.append(target.name)
                else:  # 封印失败
                    target_names_failure.append(target.name)
            if target_names_success:
                target_name_msg = "、".join(target_names_success)
                skill_msg += f"{target_name_msg}被封印了！动弹不得，持续{skill.turn_cost}回合\n"
            if target_names_failure:
                target_name_msg = "、".join(target_names_failure)
                skill_msg += f"封印失败，被{target_name_msg}抵抗了！\n"
            attack_msg, total_dmg = self._normal_attack(caster, targets[0])
            skill_msg += attack_msg
            return skill_msg, total_dmg

        # Type 5: 随机波动伤害 (Random Hit)
        elif skill.skill_type == SkillType.RANDOM_HIT:
            min_mult = float(skill.atk_values)
            max_mult = float(skill.atk_coefficient)
            rand_mult = random.uniform(min_mult, max_mult)
            rand_mult = round(rand_mult, 2)  # 保留两位小数
            dmg, is_crit, status = self._calc_raw_damage(caster, targets[0], rand_mult)

            if status == "Hit":
                crit_str = "💥并且发生了会心一击，" if is_crit else ""
                total_dmg = dmg
                skill_msg += f"获得{rand_mult}倍加成，{crit_str}造成{number_to(int(total_dmg))}伤害！"
                targets[0].update_stat("hp", 2, total_dmg)
            else:
                skill_msg = f"{caster.name}的技能被{targets[0].name}闪避了！"

            if skill.turn_cost > 0:  # 释放后回气休息
                effect = StatusEffect(skill.name, DebuffType.FATIGUE, 0, 1, True, skill.turn_cost, skill.skill_type)
                caster.add_status(effect)
                skill_msg += f"\n{caster.name}力竭，需休息{skill.turn_cost}回合"
            return skill_msg, total_dmg

        # Type 6: 叠加 Buff (Stacking)
        elif skill.skill_type == SkillType.STACK_BUFF:
            effect = StatusEffect(skill.name, BuffType.ATTACK_UP, skill.skill_buff_value, 1, False, skill.turn_cost - 1,
                                  skill.skill_type)
            caster.add_status(effect)  # 给自己添加Buff
            skill_msg += f"每回合叠加{skill.skill_buff_value}倍攻击力，持续{skill.turn_cost}回合（剩余{skill.turn_cost - 1}回合）\n"
            attack_msg, total_dmg = self._normal_attack(caster, targets[0])
            skill_msg += attack_msg
            return skill_msg, total_dmg


        # Type 101: BOSS专属技能紫玄掌
        elif skill.skill_type == SkillType.MULTIPLIER_PERCENT_HP:
            # 特殊技能1：造成5倍伤害并附加30%最大生命值的伤害
            skill_miss_msg = ""
            for target in targets:
                dmg, is_crit, status = self._calc_raw_damage(caster, target, skill.atk_values)
                if status == "Hit":
                    crit_str = "💥并且发生了会心一击，" if is_crit else ""
                    dmg = dmg + (target.max_hp * skill.atk_coefficient)
                    skill_msg += f"{crit_str}对{target.name}造成{number_to(int(dmg))}伤害！"
                    target.update_stat("hp", 2, dmg)
                    total_dmg += dmg
                else:
                    skill_miss_msg += f"{caster.name}的技能被{target.name}闪避了！"
            if total_dmg > 0:
                if skill_miss_msg:
                    skill_msg += f"\n{skill_miss_msg}"
            else:
                skill_msg = f"{caster.name}的技能被敌人闪避了！"
            return skill_msg, total_dmg

        # Type 102: BOSS专属技能子龙朱雀
        elif skill.skill_type == SkillType.MULTIPLIER_DEF_IGNORE:
            # 特殊技能2：穿透护甲，造成3倍伤害
            skill_miss_msg = ""
            for target in targets:
                dmg, is_crit, status = self._calc_raw_damage(caster, target, skill.atk_values, True)
                if status == "Hit":
                    crit_str = "💥并且发生了会心一击，" if is_crit else ""
                    skill_msg += f"{crit_str}对{target.name}造成{number_to(int(dmg))}伤害！"
                    target.update_stat("hp", 2, dmg)
                    total_dmg += dmg
                else:
                    skill_msg = f"{caster.name}的技能被{target.name}闪避了！"
            if total_dmg > 0:
                if skill_miss_msg:
                    skill_msg += f"\n{skill_miss_msg}"
            else:
                skill_msg = f"{caster.name}的技能被敌人闪避了！"
            return skill_msg, total_dmg

        # Type 103: 控制类型
        elif skill.skill_type == SkillType.CC:
            buff_msg = self.get_effect_desc(skill.skill_buff_type, True)
            chance = skill.success_rate
            target_names_success = []
            target_names_failure = []
            for target in targets:
                if random.uniform(0, 100) <= chance:
                    effect = StatusEffect(skill.name, skill.skill_buff_type, 0, 1, True, skill.turn_cost,
                                          skill.skill_type)
                    target.add_status(effect)
                    target_names_success.append(target.name)
                else:  # 封印失败
                    target_names_failure.append(target.name)
            if target_names_success:
                target_name_msg = "、".join(target_names_success)
                skill_msg += f"{target_name_msg}被{buff_msg}！持续{skill.turn_cost}回合\n"
            if target_names_failure:
                target_name_msg = "、".join(target_names_failure)
                skill_msg += f"{skill.name}被{target_name_msg}抵抗了！\n"
            return skill_msg, total_dmg

        # Type 104: 召唤类型
        elif skill.skill_type == SkillType.SUMMON:
            copy_ratio = skill.atk_values  # 召唤物属性倍率
            summon_count = int(skill.atk_coefficient)  # 召唤数量

            for i in range(summon_count):
                # 创建召唤物的数据字典
                summon_data = {}

                # 1. 复制基础信息
                summon_data["user_id"] = self.bot_id
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

                if caster.team_id == 0:
                    self.team_a.append(summon)
                else:
                    self.team_b.append(summon)

            skill_msg += f"生成{summon_count}个召唤物！"
            return skill_msg, total_dmg

        else:
            return skill_msg, total_dmg

    def _normal_attack(self, caster, targets):
        skill_msg = ""
        total_dmg = 0
        dmg, is_crit, accuracy = self._calc_raw_damage(caster, targets, 1)
        if accuracy == "Hit":
            total_dmg = dmg
            if is_crit:
                skill_msg += f"{caster.name}发起攻击，💥并且发生了会心一击，对{targets.name}造成{number_to(int(total_dmg))}伤害！"
            else:
                skill_msg += f"{caster.name}发起攻击，对{targets.name}造成{number_to(int(total_dmg))}伤害！"
            targets.update_stat("hp", 2, total_dmg)
        else:
            skill_msg += f"{caster.name}使用普通攻击，被{targets.name}躲开了"

        return skill_msg, total_dmg

    def check_unit_control(self, unit):
        """检查单位的控制状态"""
        # 所有会导致跳过回合的控制效果
        SKIP_TURN_CONTROLS = {
            DebuffType.FATIGUE: ("😫", "正在调息，跳过回合"),
            DebuffType.STUN: ("🌀", "被眩晕，跳过回合"),
            DebuffType.FREEZE: ("❄️", "被冰冻，跳过回合"),
            DebuffType.PETRIFY: ("🗿", "被石化，跳过回合"),
            DebuffType.SLEEP: ("💤", "正在沉睡，跳过回合"),
            DebuffType.ROOT: ("🌿", "被定身，跳过回合"),
            DebuffType.FEAR: ("😱", "陷入恐惧，跳过回合"),
            DebuffType.SEAL: ("🔒", "被封印，跳过回合"),
            DebuffType.PARALYSIS: ("⚡", "全身麻痹，跳过回合"),
        }

        # 检查每种控制效果
        for debuff_type, (emoji, description) in SKIP_TURN_CONTROLS.items():
            if unit.has_debuff("type", debuff_type):
                duration = unit.get_debuff_field("type", "duration", debuff_type)
                return f"{emoji}{unit.name}{description}（剩余{duration}回合）"

        return None

    def process_turn(self):
        self.round += 1
        # 获取所有存活单位并按速度排序
        units = [u for u in self.team_a + self.team_b if u.is_alive]
        units.sort(key=lambda x: x.base_speed, reverse=True)

        if self.round == 1:  # 开局释放buff
            for unit in units:
                enemies = self._get_all_enemies(unit)  # 获取全部敌人
                self._apply_round_one_skills(unit, enemies, unit.start_skills)

        # print(f"\n----- 第 {self.round} 回合 -----")
        for unit in units:
            if not unit.is_alive: continue  # 如果死亡跳过
            enemies = self._get_all_enemies(unit)  # 获取全部敌人
            if not enemies: break  # 没有敌人推出循环

            if self.round == 1:
                unit.check_and_clear_debuffs_by_immunity()  # 检查是否有debuff免疫效果

            self.add_message(unit, f"☆------{unit.name}的回合------☆")
            unit.update_status_effects()  # 更新buff状态

            if unit.poison_dot_dmg > 0:
                self.add_message(unit, f"{unit.name}☠️中毒消耗气血{number_to(int(unit.poison_dot_dmg))}点")
                unit.update_stat("hp", 2, unit.poison_dot_dmg)

            if unit.has_debuff("type", DebuffType.SKILL_DOT):
                for skill_dot_info in unit.get_debuffs("type", DebuffType.SKILL_DOT):
                    for enemy in enemies:
                        if enemy.name == skill_dot_info.coefficient:
                            dmg, is_crit, status = self._calc_raw_damage(enemy, unit, skill_dot_info.value)
                            unit.update_stat("hp", 2, dmg)
                            crit_str = "💥会心一击，" if is_crit else ""
                            self.add_message(unit, f"{skill_dot_info.name}{crit_str}造成{number_to(int(dmg))}伤害！"
                                                   f"（剩余{skill_dot_info.duration}回合）")

            # 扣血deBuff结算后检查是否死亡
            if not unit.is_alive:
                self.add_message(unit, f"{unit.name}💀倒下了！")
                continue

            if unit.hp_regen_rate > 0:
                self.add_message(unit, f"{unit.name}❤️回复气血{number_to(int(unit.hp_regen_rate))}点")
                unit.update_stat("hp", 1, unit.hp_regen_rate)

            if unit.mp_regen_rate > 0:
                self.add_message(unit, f"{unit.name}💙回复真元{number_to(int(unit.mp_regen_rate))}点")
                unit.update_stat("mp", 1, unit.mp_regen_rate)

            if unit.has_buff("skill_type", 3):
                skill_buffs = unit.get_buffs("skill_type", 3)
                for skill_buff in skill_buffs:
                    buff_msg = self.get_effect_desc(skill_buff.type, False, f"{skill_buff.value * 100:.0f}%")
                    self.add_message(unit, f"{skill_buff.name}{buff_msg}，剩余{skill_buff.duration}回合")

            if unit.has_buff("skill_type", 6):
                skill_buff = unit.get_buff("skill_type", 6)
                skill_value = skill_buff.value + skill_buff.value / skill_buff.coefficient
                unit.set_buff_field("name", "value", skill_buff.name, skill_value)
                unit.set_buff_field("name", "coefficient", skill_buff.name, (skill_buff.coefficient + 1))
                self.add_message(unit,
                                 f"{skill_buff.name}提升了{skill_value:.2f}倍攻击力，剩余{skill_buff.duration}回合")

            control_message = self.check_unit_control(unit)  # 控制类debuff
            if control_message:
                self.add_message(unit, control_message)
                continue  # 跳过这个单位的回合

            # --- 攻击流程 ---
            skill_msg = ""
            total_dmg = 0
            # 1. 选择技能（BUFF 优先）
            skill = self.choose_skill(unit, unit.skills, enemies)
            if skill:  # 释放技能
                targets = self._select_targets(enemies, skill, unit.is_boss)
                skill_msg, total_dmg = self._execute_skill(unit, targets, skill)  # 释放技能
            else:  # 普通攻击
                targets = min(enemies, key=lambda x: x.hp)  # 选择血最少的
                skill_msg, total_dmg = self._normal_attack(unit, targets)

            if total_dmg > 0:
                lifesteal_msg = ""
                if unit.has_buff("type", BuffType.LIFESTEAL_UP) and unit.lifesteal_rate > 0:
                    lifesteal = int(total_dmg * unit.lifesteal_rate)
                    lifesteal_msg = f"（❤️吸取气血{number_to(int(lifesteal))}点）"
                    unit.update_stat("hp", 1, lifesteal)

                mana_steal_msg = ""
                if unit.has_buff("type", BuffType.MANA_STEAL_UP) and unit.mana_steal_rate > 0:
                    mana_steal = int(total_dmg * unit.mana_steal_rate)
                    mana_steal_msg = f"（💙吸取真元{number_to(int(mana_steal))}点）"
                    unit.update_stat("mp", 1, mana_steal)

                skill_msg = self.add_after_last_damage(skill_msg, f"{lifesteal_msg}{mana_steal_msg}")

            self.add_message(unit, skill_msg)
            unit.total_dmg += total_dmg
            if total_dmg > 0:
                if isinstance(targets, list):
                    hp_msgs = [t.show_bar("hp") for t in targets]
                    self.add_message(unit, "\n".join(hp_msgs))
                else:
                    self.add_message(unit, targets.show_bar("hp"))

    def get_final_status_list(self):
        status = []
        for u in self.team_a + self.team_b:
            status.append({
                u.name: {
                    "hp": int(u.hp),
                    "mp": int(u.mp),
                    "user_id": u.id,
                    "hp_multiplier": u.max_hp / (u.exp / 2 if u.exp > 0 else 1),
                    "mp_multiplier": u.max_mp / (u.exp if u.exp > 0 else 1),
                    "team_id": u.team_id,
                    "total_dmg": int(u.total_dmg)
                }
            })
        return status

    def run_battle(self):
        while self.round < self.max_rounds:

            alive_a_units = [u for u in self.team_a if u.is_alive]
            alive_b_units = [u for u in self.team_b if u.is_alive]

            alive_a = len(alive_a_units) > 0
            alive_b = len(alive_b_units) > 0

            if not alive_a:
                winner_name = alive_b_units[0].name if alive_b_units else "未知"
                winner = 1
                self.add_system_message(f"战斗结束: {winner_name} 方获胜!")
                return self.play_list, winner, self.get_final_status_list()

            if not alive_b:
                winner_name = alive_a_units[0].name if alive_a_units else "未知"
                winner = 0
                self.add_system_message(f"战斗结束: {winner_name} 方获胜!")
                return self.play_list, winner, self.get_final_status_list()

            self.process_turn()

        self.add_system_message("平局")
        winner = 2
        return self.play_list, winner, self.get_final_status_list()
