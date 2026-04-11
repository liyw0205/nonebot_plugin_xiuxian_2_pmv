import json
import random
from enum import IntEnum
from pathlib import Path
from nonebot.log import logger

from .xiuxian2_handle import (
    XiuxianDateManage, OtherSet, UserBuffDate, XIUXIAN_IMPART_BUFF,
    get_final_attributes
)
from ..xiuxian_config import convert_rank
from .utils import number_to
from .item_json import Items

# 本命法宝相关导入
from ..xiuxian_natal_treasure.natal_data import NatalTreasure
from ..xiuxian_natal_treasure.natal_config import (
    NatalEffectType,
    EFFECT_BASE_AND_GROWTH,
    PERIODIC_TRUE_DAMAGE_BASE,
    PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL,
    SHIELD_BREAK_BONUS_DAMAGE,
    FATE_REVIVE_COUNT_LIMIT,
    IMMORTAL_REVIVE_COUNT_LIMIT,
    INVINCIBLE_COUNT_LIMIT,
    INVINCIBLE_FIRST_GAIN_CHANCE,
    INVINCIBLE_SUBSEQUENT_GAIN_CHANCE,
    INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE,
    SLEEP_DURATION,
    PETRIFY_DURATION,
    STUN_DURATION,
    FATIGUE_DURATION,
    FATIGUE_ATTACK_REDUCTION,
    SILENCE_DURATION,
    NIRVANA_DURATION,
    NIRVANA_SHIELD_BASE,
    NIRVANA_REVIVE_LIMIT,
    SOUL_RETURN_DURATION,
    SOUL_RETURN_HP_BASE,
    SOUL_RETURN_REVIVE_LIMIT,
    SOUL_SUMMON_LIMIT,
    ENLIGHTENMENT_LIMIT,
    ENLIGHTENMENT_REVIVE_HP_PERCENT,
    CHARGE_BONUS_DAMAGE,
    PETRIFY_DAMAGE_REDUCTION_PERCENT,
    HEALING_BLOCK_DURATION_MIN,
    HEALING_BLOCK_DURATION_MAX
)

items = Items()
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()


async def pve_fight(user, monster, type_in=2, bot_id=0, level_ratios=None):
    user_data = []
    monster_data = []

    for u in user:
        player_data = get_players_attributes(u, level_ratios)
        player_attr = player_data["属性"]
        player_attr["natal_data"] = player_data.get("本命法宝")
        player = Entity(player_attr, team_id=0)
        apply_player_buffs(player, player_data)
        user_data.append(player)

    for m in monster:
        enemy_data = get_boss_attributes(m, bot_id)
        enemy = Entity(enemy_data["属性"], team_id=1, is_boss=True)
        enemy.start_skills.extend(generate_boss_buff(m))
        generate_boss_skill(enemy, m.get("skills", []))
        monster_data.append(enemy)

    battle = BattleSystem(user_data, monster_data, bot_id)
    play_list, winner, status_list = battle.run_battle()

    if type_in == 2:
        update_all_user_status(status_list, bot_id, level_ratios)

    return play_list, winner, status_list


def Player_fight(user1, user2, type_in=1, bot_id=0):
    player1_data = get_players_attributes(user1)
    player2_data = get_players_attributes(user2)

    player1_attr = player1_data["属性"]
    player2_attr = player2_data["属性"]
    player1_attr["natal_data"] = player1_data.get("本命法宝")
    player2_attr["natal_data"] = player2_data.get("本命法宝")

    player1 = Entity(player1_attr, team_id=0)
    player2 = Entity(player2_attr, team_id=1)

    apply_player_buffs(player1, player1_data)
    apply_player_buffs(player2, player2_data)

    battle = BattleSystem([player1], [player2], bot_id)
    play_list, winner, status_list = battle.run_battle()

    if winner == 0:
        suc = player1_data["属性"]["nickname"]
    elif winner == 1:
        suc = player2_data["属性"]["nickname"]
    else:
        suc = "没有人"

    if type_in == 2:
        update_all_user_status(status_list, bot_id)

    return play_list, suc


async def Boss_fight(user1, boss: dict, type_in=2, bot_id=0):
    player1_data = get_players_attributes(user1)
    boss_data = get_boss_attributes(boss, bot_id)

    player1_attr = player1_data["属性"]
    player1_attr["natal_data"] = player1_data.get("本命法宝")
    player1 = Entity(player1_attr, team_id=0)
    boss1 = Entity(boss_data["属性"], team_id=1, is_boss=True)

    apply_player_buffs(player1, player1_data)
    boss1.start_skills.extend(generate_boss_buff(boss))

    if boss['name'] != "稻草人":
        generate_boss_skill(boss1, [14001, 14002])

    battle = BattleSystem([player1], [boss1], bot_id)
    play_list, winner, status_list = battle.run_battle()

    update_data_boss_status(boss, status_list)

    suc = "群友赢了" if winner == 0 else "Boss赢了"

    if type_in == 2:
        update_all_user_status(status_list, bot_id)

    return play_list, suc, boss


def get_players_attributes(user_id, level_ratios=None):
    buff_data_info = UserBuffDate(user_id).BuffInfo
    buffs = {}
    ratio = 1
    if level_ratios:
        ratio = level_ratios.get(user_id, 1)

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

    weapon_data = buffs.get('法器', {})

    final_attr = get_final_attributes(user_id, ratio=ratio, include_current=True)
    if not final_attr:
        return {"属性": {}, "其他": buff_data_info, "本命法宝": None}

    # ===== 套装固定闪避点数 =====
    set_bonus_effects = final_attr.get("set_bonus_effects", []) or []
    set_dodge = 0
    for sb in set_bonus_effects:
        if sb.get("type") == "dodge":
            set_dodge += float(sb.get("value", 0))

    attributes = {
        "user_id": final_attr["user_id"],
        "nickname": final_attr["nickname"],

        "max_hp": final_attr["max_hp"],
        "current_hp": final_attr["current_hp"],
        "max_mp": final_attr["max_mp"],
        "current_mp": final_attr["current_mp"],

        "mp_cost_modifier": weapon_data.get('mp_buff', 0),
        "attack": final_attr["final_atk"],
        "exp": final_attr["exp"],

        # ===== 暴击体系 =====
        "critical_rate": final_attr["crit_rate"],
        "critical_damage": final_attr["crit_damage"],

        # 抗暴：乘法区
        "crit_resist": final_attr.get("crit_resist", 0),

        # 减会心伤害：减法区
        "crit_damage_reduction": final_attr.get("crit_damage_reduction", 0),

        "boss_damage_bonus": final_attr["boss_damage_bonus"],
        "damage_reduction": final_attr["damage_reduction"],
        "armor_penetration": final_attr["armor_penetration"],

        "accuracy": 100,
        "dodge": set_dodge,
        "speed": 10,
        "start_skills": [],

        # 套装扩展，交给战斗层继续处理
        "set_bonus_effects": set_bonus_effects
    }

    natal_treasure = NatalTreasure(user_id)
    natal_data = natal_treasure.get_data() if natal_treasure.exists() else None

    buffs["本命法宝"] = natal_data
    buffs["属性"] = attributes
    buffs["其他"] = buff_data_info

    return buffs

def apply_player_buffs(player, player_data):
    buff_config = [
        ("主功法", generate_main_buff, lambda d: (d, player_data.get("其他", {}).get("faqi_buff", 0))),
        ("辅修功法", generate_sub_buff, lambda d: (d, buff_type_mapping)),
        ("身法", generate_effect_buff, lambda d: (d,)),
        ("瞳术", generate_effect_buff, lambda d: (d,))
    ]

    for key, generator, args_builder in buff_config:
        if data := player_data.get(key):
            args = args_builder(data)
            buffs = generator(*args)
            player.start_skills.extend(buffs)

    if st_data := player_data.get("神通技能"):
        player.skills.append(Skill(st_data))

    natal_data = player_data.get("本命法宝")
    if natal_data:
        player.load_natal_effects(natal_data)


def generate_sub_buff(skill, buff_type_mapping):
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

    if not mapped:
        return buffs

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

    if buff_type_id == 10:
        v1, v2 = 1, 1
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
    buffs = []

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
    for item in status_list:
        for name, attr in item.items():
            user_id = attr.get("user_id", 0)
            if user_id == 0 or user_id == bot_id:
                continue

            ratio = 1
            if level_ratios:
                ratio = level_ratios.get(user_id, 1)

            hp_multiplier = attr.get("hp_multiplier", 1)
            mp_multiplier = attr.get("mp_multiplier", 1)
            safe_hp_multiplier = hp_multiplier if hp_multiplier != 0 else 1
            safe_mp_multiplier = mp_multiplier if mp_multiplier != 0 else 1
            safe_ratio = ratio if ratio != 0 else 1

            hp = attr.get("hp", 1) / safe_hp_multiplier / safe_ratio
            mp = attr.get("mp", 1) / safe_mp_multiplier / safe_ratio

            if hp < 1:
                hp = 1
            if mp < 1:
                mp = 1

            sql_message.update_user_hp_mp(user_id, int(hp), int(mp))


def get_boss_attributes(boss, bot_id):
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
        "crit_resist": 0,  # 抗暴
        "crit_damage_reduction": 0,  # 减会伤
        "boss_damage_bonus": 0,
        "damage_reduction": 0,
        "armor_penetration": 0,
        "accuracy": 100,
        "dodge": 0,
        "speed": 0,
        "start_skills": [],
        "set_bonus_effects": [],
        'monster_type': boss.get("monster_type", "boss")
    }

    buffs["属性"] = attributes
    buffs["其他"] = boss
    return buffs


def generate_boss_buff(boss):
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
        'boss_sb': 0,
        'boss_jl': 0,
        'boss_hd': 0,
        'boss_zs_boss': 0,
        'boss_sz': 0,
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
        'boss_sb': [BuffType.EVASION_UP, "虚妄无相"],
        'boss_jl': [DebuffType.HEALING_BLOCK, "枯血断脉"],
        'boss_hd': [BuffType.SHIELD_BUFF, "不灭金身"],
        'boss_zs_boss': [BuffType.EXECUTE_EFFECT, "绝命斩杀"],
        'boss_sz': [BuffType.REGENERATION, "生生不息"],
    }

    boss_level = boss["jj"]
    current_rank_val = convert_rank(boss_level + '中期')[0]

    def get_rank_val(name):
        return convert_rank(name)[0]

    def apply_random_group(attr_names, value_options):
        selected_attr = random.choice(attr_names)
        idx = attr_names.index(selected_attr)
        val = value_options[idx]
        final_val = val() if callable(val) else val
        boss_buff[selected_attr] = final_val

    cfg = None

    if boss_level == "祭道境" or current_rank_val < get_rank_val('祭道境初期'):
        cfg = {'js': 0.05, 'cj': (25, 50), 'g1': [1, 0.7, 2, 1], 'g2': [0.7, 0.7, 1.5, 1]}
    elif get_rank_val('至尊境初期') < current_rank_val < get_rank_val('斩我境圆满'):
        cfg = {'js': (50, 55), 'cj': (15, 30), 'g1': [0.3, 0.1, 0.5, lambda: random.randint(5, 100) / 100], 'g2': [0.3, 0.3, 0.5, lambda: random.randint(5, 100) / 100]}
    elif get_rank_val('微光境初期') < current_rank_val < get_rank_val('遁一境圆满'):
        cfg = {'js': (40, 45), 'cj': (20, 40), 'g1': [0.4, 0.2, 0.7, lambda: random.randint(10, 100) / 100], 'g2': [0.4, 0.4, 0.7, lambda: random.randint(10, 100) / 100]}
    elif get_rank_val('星芒境初期') < current_rank_val < get_rank_val('至尊境圆满'):
        cfg = {'js': (30, 35), 'cj': (20, 40), 'g1': [0.6, 0.35, 1.1, lambda: random.randint(30, 100) / 100], 'g2': [0.5, 0.5, 0.9, lambda: random.randint(30, 100) / 100]}
    elif get_rank_val('月华境初期') < current_rank_val < get_rank_val('微光境圆满'):
        cfg = {'js': (20, 25), 'cj': (20, 40), 'g1': [0.7, 0.45, 1.3, lambda: random.randint(40, 100) / 100], 'g2': [0.55, 0.6, 1.0, lambda: random.randint(40, 100) / 100]}
    elif get_rank_val('耀日境初期') < current_rank_val < get_rank_val('星芒境圆满'):
        cfg = {'js': (10, 15), 'cj': (25, 45), 'g1': [0.85, 0.5, 1.5, lambda: random.randint(50, 100) / 100], 'g2': [0.6, 0.65, 1.1, lambda: random.randint(50, 100) / 100]}
    elif get_rank_val('祭道境初期') < current_rank_val < get_rank_val('月华境圆满'):
        cfg = {'js': 0.1, 'cj': (25, 45), 'g1': [0.9, 0.6, 1.7, lambda: random.randint(60, 100) / 100], 'g2': [0.62, 0.67, 1.2, lambda: random.randint(60, 100) / 100]}

    if cfg:
        boss_buff['boss_js'] = random.randint(*cfg['js']) / 100 if isinstance(cfg['js'], tuple) else cfg['js']
        boss_buff['boss_cj'] = random.randint(*cfg['cj']) / 100
        apply_random_group(['boss_zs', 'boss_hx', 'boss_bs', 'boss_xx'], cfg['g1'])
        apply_random_group(['boss_jg', 'boss_jh', 'boss_jb', 'boss_xl'], cfg['g2'])
    else:
        boss_buff['boss_js'] = 1.0
        boss_buff['boss_cj'] = 0

    boss_buff['boss_sb'] = int((1 - boss_buff['boss_js']) * 100 * random.uniform(0.1, 0.5))
    boss_buff['boss_js'] = 1 - boss_buff['boss_js']

    extra_candidates = [
        ("boss_jl", random.randint(1, 5)),
        ("boss_hd", random.uniform(0.15, 0.35)),
        ("boss_zs_boss", random.uniform(0.15, 0.25)),
        ("boss_sz", random.uniform(0.01, 0.05)),
    ]
    chosen_key, chosen_val = random.choice(extra_candidates)
    boss_buff[chosen_key] = chosen_val

    result = []
    for key, value in boss_buff.items():
        if value == 0 or key not in boss_buff_map:
            continue
        effect_type, effect_name = boss_buff_map[key]
        is_debuff = isinstance(effect_type, DebuffType)
        result.append({"name": effect_name, "type": effect_type, "value": value, "is_debuff": is_debuff})

    return result


def load_json_file(filename="data.json"):
    filepath = Path() / "data" / "xiuxian" / "功法" / filename
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


skill_data_cache = None


def get_skill_data():
    global skill_data_cache
    if skill_data_cache is None:
        skill_data_cache = load_json_file("boss神通.json")
    return skill_data_cache


def generate_boss_skill(enemy, skills):
    skill_data = get_skill_data()
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
                data["气血"] = attr.get("hp", data.get("气血"))
                data["真元"] = attr.get("mp", data.get("真元"))
                return True
    return False


class SkillType(IntEnum):
    MULTI_HIT = 1
    DOT = 2
    BUFF_STAT = 3
    CONTROL = 4
    RANDOM_HIT = 5
    STACK_BUFF = 6
    RANDOM_ACQUIRE = 7
    MULTIPLIER_PERCENT_HP = 101
    MULTIPLIER_DEF_IGNORE = 102
    CC = 103
    SUMMON = 104
    TRIGGER_HP_BELOW = 104
    FIELD = 105


class TargetType(IntEnum):
    SINGLE = 1
    AOE = 2
    MULTI = 3


class BuffType(IntEnum):
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
    SHIELD_BUFF = 16
    EXECUTE_EFFECT = 17
    REGENERATION = 18


class DebuffType(IntEnum):
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
    BuffType.SHIELD_BUFF: "获得 {value} 点护盾",
    BuffType.EXECUTE_EFFECT: "激活斩杀效果 (血量低于 {value} 直接斩杀)",
    BuffType.REGENERATION: "获得再生效果 (每回合回复最大生命 {value})",
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
    DebuffType.HEALING_BLOCK: "陷入禁疗，无法恢复生命",
}

VALID_FIELDS = {"name", "type", "value", "coefficient", "is_debuff", "duration", "skill_type"}


class StatusEffect:
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
    def __init__(self, data):
        self.name = data.get("name")
        self.desc = data.get("desc", "")
        self.skill_type = int(data.get("skill_type", 1))
        self.target_type = int(data.get("target_type", 1))
        self.multi_count = int(data.get("multi_count", 1))
        self.hp_condition = float(data.get("hp_condition", 1))
        self.hp_cost_rate = float(data.get("hpcost", 0))
        self.mp_cost_rate = float(data.get("mpcost", 0))
        self.turn_cost = int(data.get("turncost", 0))
        self.rate = float(data.get("rate", 0))
        self.cd = float(data.get("cd", 0))
        self.remain_cd = float(data.get("remain_cd", 0))
        self.atk_values = data.get("atkvalue", [])
        self.atk_coefficient = float(data.get("atkvalue2", 0))
        self.skill_buff_type = int(data.get("bufftype", 0))
        self.skill_buff_value = float(data.get("buffvalue", 0))
        self.success_rate = float(data.get("success", 0))
        self.skill_content = data.get("skill_content", [])

    def is_available(self):
        return self.remain_cd <= 0

    def trigger_cd(self):
        self.remain_cd = self.cd

    def tick_cd(self):
        if self.remain_cd > 0:
            self.remain_cd -= 1

    def __str__(self):
        return f"{self.name}(cd:{self.cd},rem:{self.remain_cd})"


class Entity:
    def __init__(self, data, team_id, is_boss=False):
        self.data = data
        self.id = data.get("user_id")
        self.name = data.get("nickname", "Unknown")
        self.team_id = team_id
        self.is_boss = is_boss
        self.type = data.get("monster_type", "player")

        self.max_hp = float(data.get("max_hp", 1))
        self.hp = float(data.get("current_hp", 1))
        self.max_mp = float(data.get("max_mp", 1))
        self.mp = float(data.get("current_mp", 1))
        self.mp_cost_modifier = float(data.get("mp_cost_modifier", 0))
        self.exp = float(data.get("exp", 1))
        self.boss_damage = float(data.get("boss_damage_bonus", 0))

        self.base_atk = float(data.get("attack", 1))
        self.base_crit = float(data.get("critical_rate", 0))
        self.base_crit_dmg = float(data.get("critical_damage", 1.5))

        # ===== 关键修复：分离两个防暴属性 =====
        self.base_crit_resist = float(data.get("crit_resist", 0))  # 抗暴（乘法）
        self.base_crit_damage_reduction = float(data.get("crit_damage_reduction", 0))  # 减会伤（减法）

        self.base_damage_reduction = float(data.get("damage_reduction", 0))
        self.base_armor_pen = float(data.get("armor_penetration", 0))
        self.base_accuracy = float(data.get("accuracy", 100))
        self.base_dodge = float(data.get("dodge", 0))
        self.base_speed = float(data.get("speed", 10))

        self.set_bonus_effects = data.get("set_bonus_effects", []) or []

        self.buffs = []
        self.debuffs = []
        self.start_skills = data.get("start_skills", [])
        self.skills = data.get("skills", [])
        self.total_dmg = 0

        self.natal_data = data.get("natal_data")
        self.natal_effects = {}
        self.natal_name = ""
        self.natal_level = 0

        self.natal_runtime = {
            "fate_revive_count": 0,
            "immortal_revive_count": 0,
            "invincible_gain_count": 0,
            "nirvana_revive_count": 0,
            "soul_return_revive_count": 0,
            "charge_status": 0,
            "soul_summon_count": {},
            "enlightenment_count": {},
            "invincible_active": 0,
            "nirvana_turns": 0,
            "soul_return_turns": 0,
            "is_soul_form": False,
            "is_nirvana_waiting": False
        }

        self.healing_block_turns = 0

    def load_natal_effects(self, natal_data):
        if not natal_data:
            return

        self.natal_data = natal_data
        self.natal_name = natal_data.get("name", "")
        self.natal_level = int(natal_data.get("level", 0))

        for i in range(1, 4):
            effect_type = natal_data.get(f"effect{i}_type", 0)
            if effect_type and effect_type > 0:
                self.natal_effects[int(effect_type)] = {
                    "level": int(natal_data.get(f"effect{i}_level", 0)),
                    "base_value": float(natal_data.get(f"effect{i}_base_value", 0.0))
                }

    def has_natal_effect(self, effect_type: NatalEffectType):
        return effect_type.value in self.natal_effects

    def get_natal_effect_level(self, effect_type: NatalEffectType):
        effect = self.natal_effects.get(effect_type.value)
        return effect["level"] if effect else 0

    def get_natal_effect_base(self, effect_type: NatalEffectType):
        effect = self.natal_effects.get(effect_type.value)
        return effect["base_value"] if effect else 0.0

    def get_natal_effect_value(self, effect_type: NatalEffectType):
        if not self.has_natal_effect(effect_type):
            return 0.0

        effect = self.natal_effects[effect_type.value]
        base_value = effect["base_value"]
        effect_level = effect["level"]
        config = EFFECT_BASE_AND_GROWTH.get(effect_type)

        if not config:
            return 0.0

        growth = config.get("growth", 0.0)

        if effect_type == NatalEffectType.INVINCIBLE:
            return 0.0

        if effect_type == NatalEffectType.TWIN_STRIKE:
            trigger_chance = base_value + (effect_level - 1) * growth
            return trigger_chance

        return base_value + (effect_level - 1) * growth

    def has_buff(self, field: str, value) -> bool:
        if field not in VALID_FIELDS:
            raise ValueError(f"unsupported field '{field}'. valid fields: {VALID_FIELDS}")
        return any(getattr(buff, field, None) == value for buff in self.buffs)

    def has_debuff(self, field: str, value) -> bool:
        if field not in VALID_FIELDS:
            raise ValueError(f"unsupported field '{field}'. valid fields: {VALID_FIELDS}")
        return any(getattr(debuff, field, None) == value for debuff in self.debuffs)

    def get_buff_field(self, match_field: str, return_field: str, match_value):
        if match_field not in VALID_FIELDS or return_field not in VALID_FIELDS:
            raise ValueError(f"unsupported field. valid fields: {VALID_FIELDS}")
        for buff in self.buffs:
            if getattr(buff, match_field, None) == match_value:
                return getattr(buff, return_field, None)
        return None

    def get_debuff_field(self, match_field: str, return_field: str, match_value):
        if match_field not in VALID_FIELDS or return_field not in VALID_FIELDS:
            raise ValueError(f"unsupported field. valid fields: {VALID_FIELDS}")
        for debuff in self.debuffs:
            if getattr(debuff, match_field, None) == match_value:
                return getattr(debuff, return_field, None)
        return None

    def set_buff_field(self, match_field: str, target_field: str, match_value, new_value) -> bool:
        if match_field not in VALID_FIELDS or target_field not in VALID_FIELDS:
            raise ValueError(f"unsupported field. valid fields: {VALID_FIELDS}")
        for buff in self.buffs:
            if getattr(buff, match_field, None) == match_value:
                setattr(buff, target_field, new_value)
                return True
        return False

    def set_debuff_field(self, match_field: str, target_field: str, match_value, new_value) -> bool:
        if match_field not in VALID_FIELDS or target_field not in VALID_FIELDS:
            raise ValueError(f"unsupported field. valid fields: {VALID_FIELDS}")
        for debuff in self.debuffs:
            if getattr(debuff, match_field, None) == match_value:
                setattr(debuff, target_field, new_value)
                return True
        return False

    def get_buffs(self, field: str, value):
        if field not in VALID_FIELDS:
            raise ValueError(f"unsupported field '{field}'. valid fields: {VALID_FIELDS}")
        return [b for b in self.buffs if getattr(b, field, None) == value]

    def get_debuffs(self, field: str, value):
        if field not in VALID_FIELDS:
            raise ValueError(f"unsupported field '{field}'. valid fields: {VALID_FIELDS}")
        return [d for d in self.debuffs if getattr(d, field, None) == value]

    def get_buff(self, field: str, value):
        buffs = self.get_buffs(field, value)
        return buffs[0] if buffs else None

    def get_debuff(self, field: str, value):
        debuffs = self.get_debuffs(field, value)
        return debuffs[0] if debuffs else None

    def _get_effect_value(self, buff_type, debuff_type=None):
        val = 0.0
        for b in self.buffs:
            if b.type == buff_type:
                val += b.value
        if debuff_type:
            for d in self.debuffs:
                if d.type == debuff_type:
                    val -= d.value
        return val

    def _get_effect_value_mixed(self, buff_type, debuff_type=None):
        buff_sum = 0.0
        for b in self.buffs:
            if b.type == buff_type:
                buff_sum += b.value
        multiplier = 0 + buff_sum
        if debuff_type:
            for d in self.debuffs:
                if d.type == debuff_type:
                    multiplier *= (1 - d.value)
        return multiplier

    def update_stat(self, stat: str, op: int, value: float):
        if stat not in ("hp", "mp"):
            raise ValueError("stat 必须是 'hp' 或 'mp'")
        current = getattr(self, stat)
        max_value = getattr(self, f"max_{stat}")

        if op == 1:
            if stat == "hp" and self.healing_block_turns > 0:
                return
            current += value
        elif op == 2:
            current -= value
        else:
            raise ValueError("op 必须是 1(加) 或 2(减)")

        current = min(current, max_value)
        setattr(self, stat, current)

    def pay_cost(self, hp_cost, mp_cost, deduct=False):
        if self.hp <= hp_cost or self.mp < mp_cost:
            return False
        if deduct:
            self.hp -= hp_cost
            self.mp -= mp_cost
        return True

    @property
    def total_shield(self):
        return int(sum(max(0, b.value) for b in self.buffs if b.type == BuffType.SHIELD))

    @property
    def invincible_count(self):
        return int(self.natal_runtime.get("invincible_active", 0))

    def show_bar(self, stat: str, length: int = 10):
        if stat not in ("hp", "mp"):
            raise ValueError("stat 必须是 'hp' 或 'mp'")
        current_data = getattr(self, stat)
        current = max(0, current_data)
        max_value = getattr(self, f"max_{stat}")

        ratio = current / max_value if max_value > 0 else 0
        filled = int(ratio * length)
        empty = length - filled
        bar = "▬" * filled + "▭" * empty

        extra = []
        if stat == "hp":
            if self.total_shield > 0:
                extra.append(f"护盾 {number_to(self.total_shield)}")
            if self.invincible_count > 0:
                extra.append(f"无敌 {self.invincible_count}")
        suffix = f" | {' | '.join(extra)}" if extra else ""

        return f"{self.name}剩余血量{number_to(int(current_data))}\n{stat.upper()} {bar} {int(ratio * 100)}%{suffix}"

    @property
    def is_alive(self):
        return self.hp > 0

    @property
    def atk_rate(self):
        pct = self._get_effect_value(BuffType.ATTACK_UP, DebuffType.ATTACK_DOWN)
        return max(0, self.base_atk * (1 + pct))

    @property
    def crit_rate(self):
        val = self.base_crit + self._get_effect_value(BuffType.CRIT_RATE_UP, DebuffType.CRIT_RATE_DOWN)
        return max(0, val)

    @property
    def crit_dmg_rate(self):
        val = self.base_crit_dmg + self._get_effect_value(BuffType.CRIT_DAMAGE_UP, DebuffType.CRIT_DAMAGE_DOWN)
        return max(1.0, val)

    @property
    def damage_reduction_rate(self):
        val = self.base_damage_reduction + self._get_effect_value(BuffType.DAMAGE_REDUCTION_UP)
        return min(0.95, val)

    @property
    def armor_pen_rate(self):
        val = self.base_armor_pen + self._get_effect_value(BuffType.ARMOR_PENETRATION_UP)
        return max(0, val)

    @property
    def accuracy_rate(self):
        val = self.base_accuracy + self._get_effect_value(BuffType.ACCURACY_UP)
        return max(0, val)

    @property
    def dodge_rate(self):
        val = self.base_dodge + self._get_effect_value(BuffType.EVASION_UP)
        return min(180, max(0, val))

    @property
    def lifesteal_rate(self):
        if self.has_debuff("type", DebuffType.LIFESTEAL_BLOCK):
            return 0
        val = self._get_effect_value_mixed(BuffType.LIFESTEAL_UP, DebuffType.LIFESTEAL_DOWN)
        return max(0, val)

    @property
    def mana_steal_rate(self):
        if self.has_debuff("type", DebuffType.MANA_STEAL_BLOCK):
            return 0
        val = self._get_effect_value_mixed(BuffType.MANA_STEAL_UP, DebuffType.MANA_STEAL_DOWN)
        return max(0, val)

    @property
    def poison_dot_dmg(self):
        total = 0.0
        for debuff in self.debuffs:
            if debuff.type == DebuffType.POISON_DOT:
                total += self.max_hp * debuff.value
        return int(total)

    @property
    def bleed_dot_dmg(self):
        total = 0.0
        for debuff in self.debuffs:
            if debuff.type == DebuffType.BLEED_DOT:
                total += self.max_hp * debuff.value
        return int(total)

    @property
    def hp_regen_rate(self):
        total = 0.0
        for buff in self.buffs:
            if buff.type == BuffType.HP_REGEN_PERCENT:
                total += self.max_hp * buff.value
        return int(total)

    @property
    def mp_regen_rate(self):
        total = 0.0
        for buff in self.buffs:
            if buff.type == BuffType.MP_REGEN_PERCENT:
                total += self.max_mp * buff.value
        return int(total)

    def remove_skill_by_name(self, skill_name):
        for i, skill in enumerate(self.skills):
            if skill.name == skill_name:
                del self.skills[i]
                return True
        return False

    def has_skill(self, skill_name):
        return any(skill.name == skill_name for skill in self.skills)

    def check_and_clear_debuffs_by_immunity(self):
        if self.has_buff("type", BuffType.DEBUFF_IMMUNITY):
            self.debuffs.clear()

    def add_status(self, effect):
        if effect.is_debuff:
            self.debuffs.append(effect)
            if effect.type == DebuffType.HEALING_BLOCK:
                self.healing_block_turns = max(self.healing_block_turns, effect.duration if effect.duration > 0 else 1)
        else:
            self.buffs.append(effect)

    def update_status_effects(self):
        for skill in self.skills[:]:
            skill.tick_cd()

        for buff in self.buffs[:]:
            buff.duration -= 1
            if buff.duration < 0:
                self.buffs.remove(buff)

        for debuff in self.debuffs[:]:
            debuff.duration -= 1
            if debuff.duration < 0:
                self.debuffs.remove(debuff)

        if self.healing_block_turns > 0:
            self.healing_block_turns -= 1

class BattleSystem:
    def __init__(self, team_a, team_b, bot_id):
        self.bot_id = bot_id
        self.team_a = team_a
        self.team_b = team_b
        self.play_list = []
        self.round = 0
        self.max_rounds = 50

    def add_message(self, unit, message):
        msg_dict = {
            "type": "node",
            "data": {
                "name": f"{unit.name} 当前血量：{number_to(int(unit.hp))} / {number_to(int(unit.max_hp))}",
                "uin": str(unit.id),
                "content": message
            }
        }
        self.play_list.append(msg_dict)

    def add_system_message(self, message):
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
        before_last, separator, after_last = msg.rpartition("伤害！")
        if separator:
            return before_last + "伤害！" + add_text + after_last
        return msg

    def _get_all_enemies(self, entity):
        def valid_target(e):
            if not e.is_alive:
                return False
            if e.natal_runtime.get("is_soul_form", False):
                return False
            if e.natal_runtime.get("is_nirvana_waiting", False):
                return False
            return True

        if entity.team_id == 0:
            return [e for e in self.team_b if valid_target(e)]
        return [e for e in self.team_a if valid_target(e)]

    def _get_all_allies(self, entity):
        if entity.team_id == 0:
            return [e for e in self.team_a if e.is_alive and e.id != entity.id]
        return [e for e in self.team_b if e.is_alive and e.id != entity.id]

    def _get_alive_allies_include_self_team(self, entity):
        if entity.team_id == 0:
            return [e for e in self.team_a if e.is_alive]
        return [e for e in self.team_b if e.is_alive]

    def _get_dead_allies(self, entity):
        if entity.team_id == 0:
            return [e for e in self.team_a if (not e.is_alive) and e.id != entity.id]
        return [e for e in self.team_b if (not e.is_alive) and e.id != entity.id]

    def _get_set_bonus_total(self, entity, effect_type: str) -> float:
        total = 0.0
        for sb in getattr(entity, "set_bonus_effects", []) or []:
            if sb.get("type") == effect_type:
                total += float(sb.get("value", 0))
        return total

    def _apply_set_bonus_start_effects(self, unit):
        # 开场护盾
        shield_rate = self._get_set_bonus_total(unit, "shield")
        if shield_rate > 0:
            shield_amount = int(unit.max_hp * shield_rate)
            if shield_amount > 0:
                shield_effect = StatusEffect("套装护盾", BuffType.SHIELD, shield_amount, 1, False, duration=99, skill_type=0)
                unit.add_status(shield_effect)
                self.add_message(unit, f"【套装效果】开场获得{number_to(shield_amount)}点护盾！")

    def _calc_raw_damage(self, attacker, defender, multiplier, penetration=False):
        status = "Hit"
        if random.uniform(0, 100) > (attacker.accuracy_rate - defender.dodge_rate):
            status = "Miss"

        is_crit = random.random() < attacker.crit_rate
        crit_mult = attacker.crit_dmg_rate if is_crit else 1.0

        # ===== 修复点：减会伤(减法) + 抗暴(乘法) 分离 =====
        if is_crit:
            # 1. 减会心伤害（减法）
            crit_dmg_reduce = float(getattr(defender, "base_crit_damage_reduction", 0.0))

            # 如果以后你有 debuff/buff 想影响减会伤，可以继续往这里叠
            # 例如：crit_dmg_reduce += xxx

            # 2. 抗暴（乘法）
            crit_resist_mul = float(getattr(defender, "base_crit_resist", 0.0))

            # 本命法宝抗暴 -> 并入抗暴乘区
            if defender.has_natal_effect(NatalEffectType.CRIT_RESIST):
                crit_resist_mul += defender.get_natal_effect_value(NatalEffectType.CRIT_RESIST)

            # 上限保护
            crit_resist_mul = max(0.0, min(0.95, crit_resist_mul))

            # 最终暴击倍率：先减法，再乘法
            crit_mult = max(1.0, (crit_mult - crit_dmg_reduce) * (1 - crit_resist_mul))

        # 减伤 / 穿透
        if defender.damage_reduction_rate < 0:
            dr_eff = defender.damage_reduction_rate
        elif penetration:
            dr_eff = 0
        else:
            dr_eff = max(0, defender.damage_reduction_rate - attacker.armor_pen_rate)

        damage = attacker.atk_rate * multiplier * crit_mult * (1 - dr_eff)

        if defender.is_boss:
            damage *= (1 + attacker.boss_damage)

        if defender.has_debuff("type", DebuffType.PETRIFY):
            damage *= (1 - PETRIFY_DAMAGE_REDUCTION_PERCENT)

        damage *= random.uniform(0.95, 1.05)
        return int(max(1, damage)), is_crit, status

    def _apply_damage_with_layers(self, attacker, target, dmg, damage_type="normal", shield_penetration=0.0):
        """
        统一伤害结算：
        - 无敌优先
        - true/dot：无视护盾直接打血
        - normal：走护盾 + 护盾穿透
        """
        if dmg <= 0:
            return 0, 0, False

        if target.natal_runtime.get("invincible_active", 0) > 0:
            target.natal_runtime["invincible_active"] -= 1
            return 0, 0, True

        remain = int(dmg)
        absorbed = 0
        hp_loss = 0

        if damage_type in ("true", "dot"):
            dr_eff = max(0, min(0.95, target.damage_reduction_rate))
            hp_loss = int(max(1, remain * (1 - dr_eff)))
            if hp_loss > 0:
                target.update_stat("hp", 2, hp_loss)
            return hp_loss, 0, False

        pen = max(0.0, min(1.0, float(shield_penetration)))
        direct_hp = int(remain * pen)
        shield_part = remain - direct_hp

        if shield_part > 0:
            shields = target.get_buffs("type", BuffType.SHIELD)
            if shields:
                left = shield_part
                for sh in shields[:]:
                    if left <= 0:
                        break
                    if sh.value <= 0:
                        continue
                    take = min(left, int(sh.value))
                    sh.value -= take
                    left -= take
                    absorbed += take

                for sh in shields[:]:
                    if sh.value <= 0 and sh in target.buffs:
                        target.buffs.remove(sh)

                shield_part = left

        hp_loss = max(0, direct_hp + shield_part)
        if hp_loss > 0:
            target.update_stat("hp", 2, hp_loss)

        return hp_loss, absorbed, False

    def _apply_round_one_skills(self, caster, targets, skills_dict):
        if not skills_dict:
            return

        for data in skills_dict:
            name = data['name']
            b_type = data['type']
            val = data['value']
            is_db = data['is_debuff']

            if is_db and caster.type == "minion":
                continue

            if b_type == BuffType.SHIELD_BUFF:
                shield_value = int(caster.max_hp * val)
                shield_effect = StatusEffect(name, BuffType.SHIELD, shield_value, 1, False, duration=99, skill_type=0)
                caster.add_status(shield_effect)
                self.add_message(caster, f"{caster.name}使用{name}，为自身施加了 {number_to(shield_value)} 点护盾！")
                continue

            if b_type == BuffType.EXECUTE_EFFECT:
                effect = StatusEffect(name, BuffType.EXECUTE_EFFECT, val, 1, False, duration=99, skill_type=0)
                caster.add_status(effect)
                self.add_message(caster, f"{caster.name}使用{name}，激活斩杀效果 (血量低于 {round(val * 100, 2)}% 直接斩杀)！")
                continue

            if b_type == BuffType.REGENERATION:
                effect = StatusEffect(name, BuffType.REGENERATION, val, 1, False, duration=99, skill_type=0)
                caster.add_status(effect)
                self.add_message(caster, f"{caster.name}使用{name}，获得再生效果 (每回合回复最大生命 {round(val * 100, 2)}%)！")
                continue

            if b_type == DebuffType.HEALING_BLOCK:
                heal_block_duration = random.randint(HEALING_BLOCK_DURATION_MIN, HEALING_BLOCK_DURATION_MAX)
                for target in targets:
                    effect = StatusEffect(name, DebuffType.HEALING_BLOCK, val, 1, True, duration=heal_block_duration, skill_type=0)
                    target.add_status(effect)
                self.add_message(caster, f"{caster.name}使用{name}，对敌方施加禁疗{heal_block_duration}回合")
                continue

            effect = StatusEffect(name, b_type, val, 1, is_db, duration=99, skill_type=0)

            if is_db:
                for target in targets:
                    target.add_status(effect)
            else:
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

    def _apply_natal_periodic_effects(self, unit, force=False):
        if not unit.natal_effects:
            return

        if not force and self.round % 4 != 0:
            return

        enemies = self._get_all_enemies(unit)
        if not enemies:
            return

        periodic_true_dmg_rate = PERIODIC_TRUE_DAMAGE_BASE + unit.natal_level * PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL
        for enemy in enemies:
            if not enemy.is_alive:
                continue
            dmg = max(1, int(enemy.hp * periodic_true_dmg_rate))
            hp_loss, absorbed, blocked = self._apply_damage_with_layers(unit, enemy, dmg, damage_type="true")

            if blocked:
                self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】道韵发动，被{enemy.name}的无敌抵挡！")
            else:
                msg = f"【{unit.natal_name or '本命法宝'}】道韵发动，对{enemy.name}造成{number_to(hp_loss)}点真实伤害！"
                self.add_message(unit, msg)

            if enemy.hp <= 0:
                revived = self._try_handle_natal_revive(enemy, unit)
                if not revived:
                    self.add_message(enemy, f"{enemy.name}💀倒下了！")

        if unit.has_natal_effect(NatalEffectType.SHIELD):
            shield_value = unit.get_natal_effect_value(NatalEffectType.SHIELD)
            shield_amount = int(unit.max_hp * shield_value)
            shield_effect = StatusEffect("本命法宝护盾", BuffType.SHIELD, shield_amount, 1, False, 3, 0)
            unit.add_status(shield_effect)
            self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】生成护盾，获得{number_to(shield_amount)}点护盾！")

        if unit.has_natal_effect(NatalEffectType.BLEED):
            bleed_value = unit.get_natal_effect_value(NatalEffectType.BLEED)
            for enemy in enemies:
                effect = StatusEffect("本命法宝流血", DebuffType.BLEED_DOT, bleed_value, 1, True, 2, 0)
                enemy.add_status(effect)
                self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】使{enemy.name}进入流血状态！")

        if unit.has_natal_effect(NatalEffectType.ARMOR_BREAK):
            armor_break_value = unit.get_natal_effect_value(NatalEffectType.ARMOR_BREAK)
            for enemy in enemies:
                effect = StatusEffect("本命法宝破甲", DebuffType.DEFENSE_DOWN, armor_break_value, 1, True, 2, 0)
                enemy.add_status(effect)
                self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】削弱了{enemy.name}的防御！")

        if unit.has_natal_effect(NatalEffectType.EVASION):
            evasion_value = unit.get_natal_effect_value(NatalEffectType.EVASION)
            effect = StatusEffect("本命法宝闪避", BuffType.EVASION_UP, evasion_value * 100, 1, False, 2, 0)
            unit.add_status(effect)
            self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】提升了闪避能力！")

        if unit.has_natal_effect(NatalEffectType.INVINCIBLE):
            if unit.natal_runtime["invincible_gain_count"] < INVINCIBLE_COUNT_LIMIT:
                base_chance = INVINCIBLE_FIRST_GAIN_CHANCE if unit.natal_runtime["invincible_gain_count"] == 0 else INVINCIBLE_SUBSEQUENT_GAIN_CHANCE
                chance = base_chance + unit.natal_level * INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE
                if random.random() < chance:
                    unit.natal_runtime["invincible_gain_count"] += 1
                    unit.natal_runtime["invincible_active"] += 1
                    self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】触发无敌，本回合可抵挡一次伤害！")

    def _apply_natal_attack_effects(self, attacker, defender, damage):
        extra_true_damage = 0
        append_msgs = []

        if attacker.has_natal_effect(NatalEffectType.TRUE_DAMAGE):
            true_dmg_rate = attacker.get_natal_effect_value(NatalEffectType.TRUE_DAMAGE)
            extra_true_damage = int(attacker.atk_rate * true_dmg_rate)
            if extra_true_damage > 0:
                hp_loss, absorbed, blocked = self._apply_damage_with_layers(attacker, defender, extra_true_damage, damage_type="true")
                extra_true_damage = hp_loss
                if blocked:
                    append_msgs.append("真实伤害被无敌抵挡")
                else:
                    if hp_loss > 0:
                        append_msgs.append(f"附加真实伤害{number_to(hp_loss)}")

        control_effects = [
            (NatalEffectType.SLEEP, DebuffType.SLEEP, SLEEP_DURATION, "睡眠"),
            (NatalEffectType.PETRIFY, DebuffType.PETRIFY, PETRIFY_DURATION, "石化"),
            (NatalEffectType.STUN, DebuffType.STUN, STUN_DURATION, "眩晕"),
            (NatalEffectType.FATIGUE, DebuffType.FATIGUE, FATIGUE_DURATION, "疲劳"),
            (NatalEffectType.SILENCE, DebuffType.SILENCE, SILENCE_DURATION, "沉默"),
        ]
        for natal_type, debuff_type, duration, desc in control_effects:
            if attacker.has_natal_effect(natal_type):
                chance = attacker.get_natal_effect_value(natal_type)
                if random.random() < chance:
                    if natal_type == NatalEffectType.FATIGUE:
                        debuff_main = StatusEffect(f"本命法宝{desc}", DebuffType.FATIGUE, 0, 1, True, duration, 0)
                        defender.add_status(debuff_main)
                        debuff_atk = StatusEffect(f"本命法宝{desc}攻击降低", DebuffType.ATTACK_DOWN, FATIGUE_ATTACK_REDUCTION, 1, True, duration, 0)
                        defender.add_status(debuff_atk)
                    else:
                        effect = StatusEffect(f"本命法宝{desc}", debuff_type, 0, 1, True, duration, 0)
                        defender.add_status(effect)
                    append_msgs.append(f"触发{desc}")

        if attacker.has_natal_effect(NatalEffectType.SOUL_SUMMON):
            dead_allies = self._get_dead_allies(attacker)
            if dead_allies:
                chance = attacker.get_natal_effect_value(NatalEffectType.SOUL_SUMMON)
                if random.random() < chance:
                    target = random.choice(dead_allies)
                    current_count = attacker.natal_runtime["soul_summon_count"].get(str(target.id), 0)
                    if current_count < SOUL_SUMMON_LIMIT:
                        attacker.natal_runtime["soul_summon_count"][str(target.id)] = current_count + 1
                        target.natal_runtime["is_soul_form"] = True
                        target.natal_runtime["soul_return_turns"] = SOUL_RETURN_DURATION
                        target.hp = 1
                        append_msgs.append(f"招魂唤回了{target.name}的残魂")

        if attacker.has_natal_effect(NatalEffectType.ENLIGHTENMENT):
            dead_allies = self._get_dead_allies(attacker)
            if dead_allies:
                chance = attacker.get_natal_effect_value(NatalEffectType.ENLIGHTENMENT)
                if random.random() < chance:
                    target = random.choice(dead_allies)
                    current_count = attacker.natal_runtime["enlightenment_count"].get(str(target.id), 0)
                    if current_count < ENLIGHTENMENT_LIMIT:
                        attacker.natal_runtime["enlightenment_count"][str(target.id)] = current_count + 1
                        target.hp = max(1, int(target.max_hp * ENLIGHTENMENT_REVIVE_HP_PERCENT))
                        target.natal_runtime["is_soul_form"] = False
                        target.natal_runtime["is_nirvana_waiting"] = False
                        append_msgs.append(f"启明复活了{target.name}")

        return extra_true_damage, append_msgs

    def _apply_set_bonus_attack_effects(self, attacker, defender, base_damage):
        extra_true_damage = 0
        append_msgs = []

        # 套装真伤
        true_damage_rate = self._get_set_bonus_total(attacker, "true_damage")
        if true_damage_rate > 0:
            td = int(attacker.atk_rate * true_damage_rate)
            if td > 0:
                hp_loss, absorbed, blocked = self._apply_damage_with_layers(attacker, defender, td, damage_type="true")
                extra_true_damage += hp_loss
                if blocked:
                    append_msgs.append("套装真伤被无敌抵挡")
                elif hp_loss > 0:
                    append_msgs.append(f"套装附加真伤{number_to(hp_loss)}")

        return extra_true_damage, append_msgs

    def _try_handle_natal_revive(self, unit, killer=None):
        if unit.hp > 0:
            return False

        death_strike_block_fate = False
        if killer and killer.has_natal_effect(NatalEffectType.DEATH_STRIKE):
            death_strike_block_fate = True

        if unit.has_natal_effect(NatalEffectType.FATE) and not death_strike_block_fate:
            if unit.natal_runtime["fate_revive_count"] < FATE_REVIVE_COUNT_LIMIT:
                chance = unit.get_natal_effect_value(NatalEffectType.FATE)
                if random.random() < chance:
                    unit.natal_runtime["fate_revive_count"] += 1
                    unit.hp = unit.max_hp
                    self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】天命发动，满血复活！")
                    return True

        if unit.has_natal_effect(NatalEffectType.IMMORTAL):
            if unit.natal_runtime["immortal_revive_count"] < IMMORTAL_REVIVE_COUNT_LIMIT:
                if random.random() < 0.5:
                    heal_rate = unit.get_natal_effect_value(NatalEffectType.IMMORTAL)
                    unit.natal_runtime["immortal_revive_count"] += 1
                    unit.hp = max(1, int(unit.max_hp * heal_rate))
                    self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】不灭发动，恢复{round(heal_rate * 100, 2)}%生命！")
                    return True

        if unit.has_natal_effect(NatalEffectType.NIRVANA):
            if unit.natal_runtime["nirvana_revive_count"] < NIRVANA_REVIVE_LIMIT:
                allies = self._get_all_allies(unit)
                if any(a.is_alive for a in allies):
                    unit.natal_runtime["nirvana_revive_count"] += 1
                    unit.natal_runtime["is_nirvana_waiting"] = True
                    unit.natal_runtime["nirvana_turns"] = NIRVANA_DURATION
                    unit.hp = 1
                    self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】进入涅槃，将于{NIRVANA_DURATION}回合后重生！")
                    return True

        if unit.has_natal_effect(NatalEffectType.SOUL_RETURN):
            if unit.natal_runtime["soul_return_revive_count"] < SOUL_RETURN_REVIVE_LIMIT:
                allies = self._get_all_allies(unit)
                if any(a.is_alive for a in allies):
                    unit.natal_runtime["soul_return_revive_count"] += 1
                    unit.natal_runtime["is_soul_form"] = True
                    unit.natal_runtime["soul_return_turns"] = SOUL_RETURN_DURATION
                    unit.hp = 1
                    self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】魂返发动，灵体持续{SOUL_RETURN_DURATION}回合！")
                    return True

        return False

    def _process_natal_special_states(self, unit):
        if unit.natal_runtime["is_nirvana_waiting"]:
            unit.natal_runtime["nirvana_turns"] -= 1
            if unit.natal_runtime["nirvana_turns"] <= 0:
                allies = self._get_all_allies(unit)
                if any(a.is_alive for a in allies):
                    unit.natal_runtime["is_nirvana_waiting"] = False
                    unit.hp = unit.max_hp
                    shield_percent = NIRVANA_SHIELD_BASE + unit.get_natal_effect_value(NatalEffectType.NIRVANA)
                    shield_amount = int(unit.max_hp * shield_percent)
                    for ally in self._get_alive_allies_include_self_team(unit):
                        ally.add_status(StatusEffect("涅槃护盾", BuffType.SHIELD, shield_amount, 1, False, 3, 0))
                    self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】涅槃完成，满血复活并为全队附加护盾！")
                else:
                    unit.hp = 0
                    unit.natal_runtime["is_nirvana_waiting"] = False
                    self.add_message(unit, f"{unit.name}的涅槃失败，因全队已灭。")

        if unit.natal_runtime["is_soul_form"]:
            unit.natal_runtime["soul_return_turns"] -= 1
            if unit.natal_runtime["soul_return_turns"] <= 0:
                allies = self._get_all_allies(unit)
                if any(a.is_alive for a in allies):
                    unit.natal_runtime["is_soul_form"] = False
                    hp_percent = SOUL_RETURN_HP_BASE + unit.get_natal_effect_value(NatalEffectType.SOUL_RETURN)
                    unit.hp = max(1, int(unit.max_hp * hp_percent))
                    self.add_message(unit, f"【{unit.natal_name or '本命法宝'}】魂返完成，恢复{round(hp_percent * 100, 2)}%生命复活！")
                else:
                    unit.hp = 0
                    unit.natal_runtime["is_soul_form"] = False
                    self.add_message(unit, f"{unit.name}的魂返失败，因全队已灭。")

    def choose_skill(self, caster, skills, enemies):
        usable_skills = []
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

        not_hp1_skills = [sk for sk in usable_skills if sk.hp_condition != 1]
        if not_hp1_skills:
            return not_hp1_skills[0]

        buff_list = [sk for sk in usable_skills if sk.skill_type == SkillType.BUFF_STAT]
        if buff_list:
            return buff_list[0]

        return random.choice(usable_skills)

    def _skill_available(self, caster, skill, enemies):
        if not skill.is_available():
            return False

        hp_percentage = caster.hp / caster.max_hp if caster.max_hp > 0 else 0
        if hp_percentage > skill.hp_condition:
            return False

        hp_cost = caster.hp * skill.hp_cost_rate
        mp_cost = caster.exp * skill.mp_cost_rate * (1 - caster.mp_cost_modifier)
        if not caster.pay_cost(hp_cost, mp_cost, deduct=False):
            return False

        if skill.skill_type in (SkillType.DOT, SkillType.CC, SkillType.CONTROL):
            enemies_without_debuff = [e for e in enemies if not e.has_debuff("name", skill.name)]
            if not enemies_without_debuff:
                return False

        if skill.skill_type in (SkillType.BUFF_STAT, SkillType.STACK_BUFF):
            if caster.has_buff("name", skill.name):
                return False

        if caster.has_debuff("type", DebuffType.SILENCE):
            return False

        return True

    def _select_targets(self, enemies, skill, is_boss=False):
        alive = [e for e in enemies if e.is_alive]
        if not alive:
            return []

        if skill.target_type == TargetType.SINGLE:
            if skill.skill_type == SkillType.DOT:
                alive = [a for a in alive if not a.has_debuff("name", skill.name)] or alive
            if is_boss:
                return random.sample(alive, k=1)
            return [min(alive, key=lambda x: x.hp)]

        elif skill.target_type == TargetType.AOE:
            return alive

        elif skill.target_type == TargetType.MULTI:
            if skill.skill_type == SkillType.DOT:
                alive = [a for a in alive if not a.has_debuff("name", skill.name)] or alive
            n = max(1, int(getattr(skill, 'multi_count', 2)))
            n = min(n, len(alive))
            if is_boss:
                return random.sample(alive, k=n)
            return sorted(alive, key=lambda x: x.hp)[:n]

        return []

    def _execute_skill(self, caster, targets, skill):
        if not targets:
            return f"{caster.name}未找到目标，回合结束。", 0

        if not random.uniform(0, 100) <= skill.rate:
            skill_msg, total_dmg = self._normal_attack(caster, min(targets, key=lambda x: x.hp))
            return skill_msg, total_dmg

        hp_cost = caster.hp * skill.hp_cost_rate
        mp_cost = caster.exp * skill.mp_cost_rate * (1 - caster.mp_cost_modifier)
        caster.pay_cost(hp_cost, mp_cost, deduct=True)

        parts = []
        if hp_cost > 0:
            parts.append(f"气血{number_to(int(hp_cost))}点")
        if mp_cost > 0:
            parts.append(f"真元{number_to(int(mp_cost))}点")
        cost_msg = f"消耗{'、'.join(parts)}，" if parts else ""

        skill_msg = f"{skill.desc} {cost_msg}"
        total_dmg = 0
        skill.trigger_cd()

        if skill.skill_type == SkillType.MULTI_HIT:
            hits = skill.atk_values if isinstance(skill.atk_values, list) else [skill.atk_values]
            target = targets[0]
            skill_msg += f"对{target.name}造成"
            for mult in hits:
                dmg, is_crit, status = self._calc_raw_damage(caster, target, float(mult))
                if status == "Hit":
                    hp_loss, absorbed, blocked = self._apply_damage_with_layers(caster, target, dmg, damage_type="normal")
                    if blocked:
                        skill_msg += "无敌抵挡、"
                        continue
                    if hp_loss > 0:
                        crit_str = "💥" if is_crit else ""
                        skill_msg += f"{crit_str}{number_to(int(hp_loss))}伤害、"
                        total_dmg += hp_loss
                    elif absorbed > 0:
                        skill_msg += f"护盾吸收{number_to(int(absorbed))}、"
                else:
                    skill_msg += "miss、"

            if total_dmg > 0 or "护盾吸收" in skill_msg:
                skill_msg = skill_msg[:-1] + "！"
            else:
                skill_msg = f"{caster.name}的技能被{target.name}闪避了！"

            if total_dmg > 0 and target.is_alive:
                extra_true_damage1, extra_msgs1 = self._apply_natal_attack_effects(caster, target, total_dmg)
                extra_true_damage2, extra_msgs2 = self._apply_set_bonus_attack_effects(caster, target, total_dmg)
                total_dmg += extra_true_damage1 + extra_true_damage2
                all_msgs = extra_msgs1 + extra_msgs2
                if all_msgs:
                    skill_msg += "（" + "，".join(all_msgs) + "）"

            if target.hp <= 0:
                revived = self._try_handle_natal_revive(target, caster)
                if not revived:
                    skill_msg += f"\n{target.name}💀倒下了！"

            if skill.turn_cost > 0:
                effect = StatusEffect(skill.name, DebuffType.FATIGUE, 0, 1, True, skill.turn_cost, skill.skill_type)
                caster.add_status(effect)
                skill_msg += f"\n{caster.name}力竭，需休息{skill.turn_cost}回合"
            return skill_msg, total_dmg

        elif skill.skill_type == SkillType.DOT:
            target_names = []
            for target in targets:
                target_names.append(target.name)
                effect = StatusEffect(skill.name, DebuffType.SKILL_DOT, skill.atk_values, caster.name, True, skill.turn_cost, skill.skill_type)
                target.add_status(effect)
            target_name_msg = "、".join(target_names)
            skill_msg += f"对{target_name_msg}造成每回合{skill.atk_values}倍攻击力持续伤害，持续{skill.turn_cost}回合"
            return skill_msg, total_dmg

        elif skill.skill_type == SkillType.BUFF_STAT:
            if skill.skill_buff_type == 1:
                effect = StatusEffect(skill.name, BuffType.ATTACK_UP, skill.skill_buff_value, 1, False, skill.turn_cost, skill.skill_type)
                caster.add_status(effect)
                skill_msg += f"提升了{skill.skill_buff_value * 100:.0f}%攻击力，持续{skill.turn_cost}回合（剩余{skill.turn_cost - 1}回合）\n"
            elif skill.skill_buff_type == 2:
                effect = StatusEffect(skill.name, BuffType.DAMAGE_REDUCTION_UP, skill.skill_buff_value, 1, False, skill.turn_cost, skill.skill_type)
                caster.add_status(effect)
                skill_msg += f"提升了{skill.skill_buff_value * 100:.0f}%伤害减免，持续{skill.turn_cost}回合（剩余{skill.turn_cost - 1}回合）\n"

            attack_msg, total_dmg = self._normal_attack(caster, targets[0])
            skill_msg += attack_msg
            return skill_msg, total_dmg

        elif skill.skill_type == SkillType.CONTROL:
            chance = skill.success_rate
            target_names_success = []
            target_names_failure = []
            for target in targets:
                if random.uniform(0, 100) <= chance:
                    effect = StatusEffect(skill.name, DebuffType.SEAL, 0, 1, True, skill.turn_cost, skill.skill_type)
                    target.add_status(effect)
                    target_names_success.append(target.name)
                else:
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

        elif skill.skill_type == SkillType.RANDOM_HIT:
            min_mult = float(skill.atk_values)
            max_mult = float(skill.atk_coefficient)
            rand_mult = round(random.uniform(min_mult, max_mult), 2)
            dmg, is_crit, status = self._calc_raw_damage(caster, targets[0], rand_mult)

            if status == "Hit":
                target = targets[0]
                hp_loss, absorbed, blocked = self._apply_damage_with_layers(caster, target, dmg, damage_type="normal")
                if blocked:
                    skill_msg += f"获得{rand_mult}倍加成，但被{target.name}无敌抵挡！"
                    total_dmg = 0
                else:
                    crit_str = "💥并且发生了会心一击，" if is_crit else ""
                    total_dmg = hp_loss
                    if hp_loss > 0:
                        skill_msg += f"获得{rand_mult}倍加成，{crit_str}造成{number_to(int(total_dmg))}伤害！"
                    else:
                        skill_msg += f"获得{rand_mult}倍加成，但伤害被护盾吸收{number_to(int(absorbed))}！"

                    extra_true_damage1, extra_msgs1 = self._apply_natal_attack_effects(caster, target, max(total_dmg, 0))
                    extra_true_damage2, extra_msgs2 = self._apply_set_bonus_attack_effects(caster, target, max(total_dmg, 0))
                    total_dmg += extra_true_damage1 + extra_true_damage2
                    all_msgs = extra_msgs1 + extra_msgs2
                    if all_msgs:
                        skill_msg += "（" + "，".join(all_msgs) + "）"

                    if target.hp <= 0:
                        revived = self._try_handle_natal_revive(target, caster)
                        if not revived:
                            skill_msg += f"\n{target.name}💀倒下了！"
            else:
                skill_msg = f"{caster.name}的技能被{targets[0].name}闪避了！"

            if skill.turn_cost > 0:
                effect = StatusEffect(skill.name, DebuffType.FATIGUE, 0, 1, True, skill.turn_cost, skill.skill_type)
                caster.add_status(effect)
                skill_msg += f"\n{caster.name}力竭，需休息{skill.turn_cost}回合"
            return skill_msg, total_dmg

        elif skill.skill_type == SkillType.STACK_BUFF:
            effect = StatusEffect(skill.name, BuffType.ATTACK_UP, skill.skill_buff_value, 1, False, skill.turn_cost - 1, skill.skill_type)
            caster.add_status(effect)
            skill_msg += f"每回合叠加{skill.skill_buff_value}倍攻击力，持续{skill.turn_cost}回合（剩余{skill.turn_cost - 1}回合）\n"
            attack_msg, total_dmg = self._normal_attack(caster, targets[0])
            skill_msg += attack_msg
            return skill_msg, total_dmg

        elif skill.skill_type == SkillType.MULTIPLIER_PERCENT_HP:
            skill_miss_msg = ""
            for target in targets:
                dmg, is_crit, status = self._calc_raw_damage(caster, target, skill.atk_values)
                if status == "Hit":
                    dmg = dmg + int(target.max_hp * skill.atk_coefficient)
                    hp_loss, absorbed, blocked = self._apply_damage_with_layers(caster, target, dmg, damage_type="normal")
                    if blocked:
                        skill_miss_msg += f"{target.name}的无敌抵挡了技能！"
                        continue

                    crit_str = "💥并且发生了会心一击，" if is_crit else ""
                    if hp_loss > 0:
                        skill_msg += f"{crit_str}对{target.name}造成{number_to(int(hp_loss))}伤害！"
                        total_dmg += hp_loss
                    elif absorbed > 0:
                        skill_msg += f"对{target.name}造成的伤害被护盾吸收{number_to(int(absorbed))}！"

                    if target.hp <= 0:
                        revived = self._try_handle_natal_revive(target, caster)
                        if not revived:
                            skill_msg += f"\n{target.name}💀倒下了！"
                else:
                    skill_miss_msg += f"{caster.name}的技能被{target.name}闪避了！"

            if total_dmg <= 0 and not skill_msg:
                skill_msg = f"{caster.name}的技能被敌人闪避了！"
            if skill_miss_msg:
                skill_msg += ("\n" + skill_miss_msg if skill_msg else skill_miss_msg)
            return skill_msg, total_dmg

        elif skill.skill_type == SkillType.MULTIPLIER_DEF_IGNORE:
            skill_miss_msg = ""
            for target in targets:
                dmg, is_crit, status = self._calc_raw_damage(caster, target, skill.atk_values, True)
                if status == "Hit":
                    hp_loss, absorbed, blocked = self._apply_damage_with_layers(caster, target, dmg, damage_type="normal")
                    if blocked:
                        skill_miss_msg += f"{target.name}的无敌抵挡了技能！"
                        continue

                    crit_str = "💥并且发生了会心一击，" if is_crit else ""
                    if hp_loss > 0:
                        skill_msg += f"{crit_str}对{target.name}造成{number_to(int(hp_loss))}伤害！"
                        total_dmg += hp_loss
                    elif absorbed > 0:
                        skill_msg += f"对{target.name}造成的伤害被护盾吸收{number_to(int(absorbed))}！"

                    if target.hp <= 0:
                        revived = self._try_handle_natal_revive(target, caster)
                        if not revived:
                            skill_msg += f"\n{target.name}💀倒下了！"
                else:
                    skill_miss_msg += f"{caster.name}的技能被{target.name}闪避了！"

            if total_dmg <= 0 and not skill_msg:
                skill_msg = f"{caster.name}的技能被敌人闪避了！"
            if skill_miss_msg:
                skill_msg += ("\n" + skill_miss_msg if skill_msg else skill_miss_msg)
            return skill_msg, total_dmg

        elif skill.skill_type == SkillType.CC:
            buff_msg = self.get_effect_desc(skill.skill_buff_type, True)
            chance = skill.success_rate
            target_names_success = []
            target_names_failure = []
            for target in targets:
                if random.uniform(0, 100) <= chance:
                    effect = StatusEffect(skill.name, skill.skill_buff_type, 0, 1, True, skill.turn_cost, skill.skill_type)
                    target.add_status(effect)
                    target_names_success.append(target.name)
                else:
                    target_names_failure.append(target.name)
            if target_names_success:
                target_name_msg = "、".join(target_names_success)
                skill_msg += f"{target_name_msg}被{buff_msg}！持续{skill.turn_cost}回合\n"
            if target_names_failure:
                target_name_msg = "、".join(target_names_failure)
                skill_msg += f"{skill.name}被{target_name_msg}抵抗了！\n"
            return skill_msg, total_dmg

        elif skill.skill_type == SkillType.SUMMON:
            copy_ratio = skill.atk_values
            summon_count = int(skill.atk_coefficient)

            for i in range(summon_count):
                summon_data = {
                    "user_id": self.bot_id,
                    "nickname": f"{caster.name}的召唤物",
                    "monster_type": "summon",
                    "max_hp": caster.max_hp * copy_ratio,
                    "current_hp": caster.max_hp * copy_ratio,
                    "max_mp": caster.max_mp * copy_ratio,
                    "current_mp": caster.max_mp * copy_ratio,
                    "attack": caster.base_atk * copy_ratio,
                    "armor_penetration": caster.base_armor_pen * copy_ratio,
                    "damage_reduction": caster.base_damage_reduction * copy_ratio,
                    "critical_rate": caster.base_crit,
                    "critical_damage": caster.base_crit_dmg,
                    "crit_resist": caster.base_crit_resist,
                    "crit_damage_reduction": caster.base_crit_damage_reduction,
                    "boss_damage_bonus": 0,
                    "accuracy": caster.base_accuracy,
                    "dodge": caster.base_dodge,
                    "speed": caster.base_speed,
                    "start_skills": [],
                    "skills": [],
                    "set_bonus_effects": caster.set_bonus_effects,
                    "is_boss": bool(getattr(caster, "is_boss", False))
                }

                summon = Entity(data=summon_data, team_id=caster.team_id, is_boss=summon_data.get("is_boss", False))

                if caster.team_id == 0:
                    self.team_a.append(summon)
                else:
                    self.team_b.append(summon)

            skill_msg += f"生成{summon_count}个召唤物！"
            return skill_msg, total_dmg

        return skill_msg, total_dmg

    def _normal_attack(self, caster, target):
        skill_msg = ""
        total_dmg = 0

        if caster.has_natal_effect(NatalEffectType.CHARGE):
            if caster.natal_runtime["charge_status"] == 0 and random.random() < 0.2:
                caster.natal_runtime["charge_status"] = 1
                return f"{caster.name}正在蓄力，下回合将爆发更强一击！", 0

        attack_multiplier = 1.0

        if caster.has_natal_effect(NatalEffectType.DIVINE_POWER):
            attack_multiplier += caster.get_natal_effect_value(NatalEffectType.DIVINE_POWER)

        if caster.natal_runtime["charge_status"] == 1:
            charge_bonus = CHARGE_BONUS_DAMAGE
            if caster.has_natal_effect(NatalEffectType.CHARGE):
                charge_bonus += caster.get_natal_effect_value(NatalEffectType.CHARGE)
            attack_multiplier += charge_bonus
            caster.natal_runtime["charge_status"] = 0

        dmg, is_crit, accuracy = self._calc_raw_damage(caster, target, attack_multiplier)

        if accuracy == "Hit":
            shield_pen = 0.0

            # 本命法宝破盾
            if caster.has_natal_effect(NatalEffectType.SHIELD_BREAK):
                if target.get_buffs("type", BuffType.SHIELD):
                    shield_pen = caster.get_natal_effect_value(NatalEffectType.SHIELD_BREAK)

            # 套装破盾
            set_shield_break = self._get_set_bonus_total(caster, "shield_break")
            if set_shield_break > 0 and target.get_buffs("type", BuffType.SHIELD):
                shield_pen = max(shield_pen, set_shield_break)

            hp_loss, absorbed, blocked = self._apply_damage_with_layers(
                caster, target, dmg, damage_type="normal", shield_penetration=shield_pen
            )
            if blocked:
                return f"{caster.name}发起攻击，但被{target.name}的无敌效果抵挡了！", 0

            if hp_loss > 0:
                total_dmg = hp_loss
                if is_crit:
                    skill_msg += f"{caster.name}发起攻击，💥并且发生了会心一击，对{target.name}造成{number_to(int(total_dmg))}伤害！"
                else:
                    skill_msg += f"{caster.name}发起攻击，对{target.name}造成{number_to(int(total_dmg))}伤害！"
            else:
                skill_msg += f"{caster.name}发起攻击，但伤害被{target.name}的护盾吸收{number_to(int(absorbed))}！"

            if shield_pen > 0:
                skill_msg += f"（破盾生效：{round(shield_pen * 100, 2)}%伤害穿透护盾）"

            extra_true_damage1, extra_msgs1 = self._apply_natal_attack_effects(caster, target, max(total_dmg, 0))
            extra_true_damage2, extra_msgs2 = self._apply_set_bonus_attack_effects(caster, target, max(total_dmg, 0))
            total_dmg += extra_true_damage1 + extra_true_damage2
            all_msgs = extra_msgs1 + extra_msgs2
            if all_msgs:
                skill_msg += "（" + "，".join(all_msgs) + "）"

            # 本命法宝反伤
            reflect_rate = 0.0
            if target.has_natal_effect(NatalEffectType.REFLECT_DAMAGE):
                reflect_rate += target.get_natal_effect_value(NatalEffectType.REFLECT_DAMAGE)

            # 套装反伤
            reflect_rate += self._get_set_bonus_total(target, "reflect")

            if reflect_rate > 0:
                reflect_dmg = int(max(total_dmg, 0) * reflect_rate)
                if reflect_dmg > 0:
                    r_hp_loss, r_absorbed, r_blocked = self._apply_damage_with_layers(target, caster, reflect_dmg, damage_type="true")
                    if r_blocked:
                        skill_msg += f"\n{caster.name}的无敌抵挡了反伤！"
                    elif r_hp_loss > 0:
                        skill_msg += f"\n{target.name}反伤{number_to(r_hp_loss)}点！"

            if caster.has_natal_effect(NatalEffectType.DEATH_STRIKE) and target.hp > 0:
                death_rate = caster.get_natal_effect_value(NatalEffectType.DEATH_STRIKE)
                if target.hp / target.max_hp <= death_rate:
                    target.hp = 0
                    skill_msg += f"\n{target.name}被斩命直接诛灭！"

            execute_buffs = caster.get_buffs("type", BuffType.EXECUTE_EFFECT)
            if execute_buffs and target.hp > 0:
                execute_rate = max(b.value for b in execute_buffs)
                if target.hp / target.max_hp <= execute_rate:
                    target.hp = 0
                    skill_msg += f"\n{target.name}触发斩杀线，被直接终结！"

            if target.hp <= 0:
                revived = self._try_handle_natal_revive(target, caster)
                if not revived:
                    skill_msg += f"\n{target.name}💀倒下了！"

            if caster.has_natal_effect(NatalEffectType.TWIN_STRIKE) and target.is_alive:
                twin_chance = caster.get_natal_effect_value(NatalEffectType.TWIN_STRIKE)
                if random.random() < twin_chance:
                    twin_dmg, twin_crit, twin_status = self._calc_raw_damage(caster, target, 1.0)
                    if twin_status == "Hit":
                        t_hp_loss, t_absorbed, t_blocked = self._apply_damage_with_layers(caster, target, twin_dmg, damage_type="normal")
                        if t_blocked:
                            skill_msg += f"\n{target.name}的无敌抵挡了双生连击！"
                        else:
                            if t_hp_loss > 0:
                                total_dmg += t_hp_loss
                                skill_msg += f"\n双生发动，额外对{target.name}造成{number_to(int(t_hp_loss))}伤害！"
                            elif t_absorbed > 0:
                                skill_msg += f"\n双生发动，但伤害被护盾吸收{number_to(int(t_absorbed))}！"

                            if target.hp <= 0:
                                revived = self._try_handle_natal_revive(target, caster)
                                if not revived:
                                    skill_msg += f"\n{target.name}💀倒下了！"

        else:
            skill_msg += f"{caster.name}使用普通攻击，被{target.name}躲开了"

        return skill_msg, total_dmg

    def check_unit_control(self, unit):
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

        for debuff_type, (emoji, description) in SKIP_TURN_CONTROLS.items():
            if unit.has_debuff("type", debuff_type):
                duration = unit.get_debuff_field("type", "duration", debuff_type)
                return f"{emoji}{unit.name}{description}（剩余{duration}回合）"

        return None

    def process_turn(self):
        self.round += 1
        units = [u for u in self.team_a + self.team_b if u.is_alive]
        units.sort(key=lambda x: x.base_speed, reverse=True)

        if self.round == 1:
            for unit in units:
                enemies = self._get_all_enemies(unit)
                self._apply_round_one_skills(unit, enemies, unit.start_skills)
                self._apply_set_bonus_start_effects(unit)
                self._apply_natal_periodic_effects(unit, force=True)

        for unit in units:
            if not unit.is_alive:
                continue

            self._process_natal_special_states(unit)

            if unit.natal_runtime["is_nirvana_waiting"]:
                self.add_message(unit, f"{unit.name}正在涅槃中，无法行动。")
                continue

            enemies = self._get_all_enemies(unit)
            if not enemies:
                break

            if self.round == 1:
                unit.check_and_clear_debuffs_by_immunity()

            self.add_message(unit, f"☆------{unit.name}的第{self.round}回合------☆")
            unit.update_status_effects()

            self._apply_natal_periodic_effects(unit)

            if unit.poison_dot_dmg > 0:
                hp_loss, absorbed, blocked = self._apply_damage_with_layers(None, unit, unit.poison_dot_dmg, damage_type="dot")
                if blocked:
                    self.add_message(unit, f"{unit.name}☠️中毒伤害被无敌抵挡")
                else:
                    self.add_message(unit, f"{unit.name}☠️中毒消耗气血{number_to(int(hp_loss))}点")

            if unit.bleed_dot_dmg > 0:
                hp_loss, absorbed, blocked = self._apply_damage_with_layers(None, unit, unit.bleed_dot_dmg, damage_type="dot")
                if blocked:
                    self.add_message(unit, f"{unit.name}🩸流血伤害被无敌抵挡")
                else:
                    self.add_message(unit, f"{unit.name}🩸流血损失气血{number_to(int(hp_loss))}点")

            if unit.has_debuff("type", DebuffType.SKILL_DOT):
                for skill_dot_info in unit.get_debuffs("type", DebuffType.SKILL_DOT):
                    for enemy in enemies:
                        if enemy.name == skill_dot_info.coefficient:
                            dmg, is_crit, status = self._calc_raw_damage(enemy, unit, skill_dot_info.value)
                            if status != "Hit":
                                self.add_message(unit, f"{skill_dot_info.name}的持续伤害被闪避！（剩余{skill_dot_info.duration}回合）")
                                continue

                            hp_loss, absorbed, blocked = self._apply_damage_with_layers(enemy, unit, dmg, damage_type="dot")
                            if blocked:
                                self.add_message(unit, f"{skill_dot_info.name}的持续伤害被无敌抵挡！（剩余{skill_dot_info.duration}回合）")
                            else:
                                crit_str = "💥会心一击，" if is_crit else ""
                                msg = f"{skill_dot_info.name}{crit_str}造成{number_to(int(hp_loss))}伤害！（剩余{skill_dot_info.duration}回合）"
                                self.add_message(unit, msg)

            if not unit.is_alive:
                revived = self._try_handle_natal_revive(unit, None)
                if not revived:
                    self.add_message(unit, f"{unit.name}💀倒下了！")
                    continue

            regen_buffs = unit.get_buffs("type", BuffType.REGENERATION)
            if regen_buffs:
                regen_rate = sum(b.value for b in regen_buffs)
                regen_val = int(unit.max_hp * regen_rate)
                if regen_val > 0:
                    if unit.healing_block_turns > 0:
                        self.add_message(unit, f"{unit.name}处于禁疗状态，再生无效（剩余{unit.healing_block_turns}回合）")
                    else:
                        unit.update_stat("hp", 1, regen_val)
                        self.add_message(unit, f"{unit.name}触发再生，回复气血{number_to(regen_val)}点")

            if unit.hp_regen_rate > 0:
                if unit.healing_block_turns > 0:
                    self.add_message(unit, f"{unit.name}处于禁疗状态，无法回血（剩余{unit.healing_block_turns}回合）")
                else:
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
                self.add_message(unit, f"{skill_buff.name}提升了{skill_value:.2f}倍攻击力，剩余{skill_buff.duration}回合")

            control_message = self.check_unit_control(unit)
            if control_message:
                self.add_message(unit, control_message)
                continue

            is_soul_form = unit.natal_runtime["is_soul_form"]

            skill_msg = ""
            total_dmg = 0
            targets = None

            if is_soul_form:
                target = min(enemies, key=lambda x: x.hp)
                skill_msg, total_dmg = self._normal_attack(unit, target)
                targets = target
            else:
                if unit.has_debuff("type", DebuffType.SILENCE):
                    target = min(enemies, key=lambda x: x.hp)
                    skill_msg, total_dmg = self._normal_attack(unit, target)
                    targets = target
                else:
                    skill = self.choose_skill(unit, unit.skills, enemies)
                    if skill:
                        targets = self._select_targets(enemies, skill, unit.is_boss)
                        if not targets:
                            skill_msg = f"{unit.name}没有可选目标，回合结束。"
                            total_dmg = 0
                        else:
                            skill_msg, total_dmg = self._execute_skill(unit, targets, skill)
                    else:
                        target = min(enemies, key=lambda x: x.hp)
                        skill_msg, total_dmg = self._normal_attack(unit, target)
                        targets = target

            if total_dmg > 0:
                lifesteal_msg = ""
                if unit.has_buff("type", BuffType.LIFESTEAL_UP) and unit.lifesteal_rate > 0:
                    lifesteal = int(total_dmg * unit.lifesteal_rate)
                    if unit.healing_block_turns > 0:
                        lifesteal_msg = "（禁疗中，吸血无效）"
                    else:
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

            if targets is not None:
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
