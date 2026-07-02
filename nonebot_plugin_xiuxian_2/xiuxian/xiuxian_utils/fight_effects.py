from enum import IntEnum

from .pet_system import PET_SKILL_ATTACK, PET_SKILL_BUFF, PET_SKILL_PROTECT


PET_ACTIVE_TRIGGER_RULES = {
    PET_SKILL_ATTACK: (0.38, 0.12, 0.68),
    PET_SKILL_BUFF: (0.30, 0.08, 0.55),
    PET_SKILL_PROTECT: (0.32, 0.08, 0.58),
}
PET_EXCLUSIVE_TRIGGER_BONUS = 0.05
PET_PROTECT_TRIGGER_LIMIT = 3


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
    SPEED_UP = 19


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
    SPEED_DOWN = 26


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
    14: BuffType.ARMOR_PENETRATION_UP,
    15: BuffType.SPEED_UP,
    16: DebuffType.SPEED_DOWN,
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
    BuffType.SPEED_UP: "速度提升 {value}",
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
    DebuffType.SPEED_DOWN: "速度降低 {value}",
}

PET_BUFF_TYPE_MAP = {
    "attack": BuffType.ATTACK_UP,
    "crit_rate": BuffType.CRIT_RATE_UP,
    "crit_damage": BuffType.CRIT_DAMAGE_UP,
    "damage_reduction": BuffType.DAMAGE_REDUCTION_UP,
    "armor_penetration": BuffType.ARMOR_PENETRATION_UP,
    "accuracy": BuffType.ACCURACY_UP,
    "evasion": BuffType.EVASION_UP,
    "lifesteal": BuffType.LIFESTEAL_UP,
    "mana_steal": BuffType.MANA_STEAL_UP,
    "debuff_immunity": BuffType.DEBUFF_IMMUNITY,
    "hp_regen": BuffType.HP_REGEN_PERCENT,
    "mp_regen": BuffType.MP_REGEN_PERCENT,
    "reflect": BuffType.REFLECT_DAMAGE,
    "shield": BuffType.SHIELD,
    "speed": BuffType.SPEED_UP,
}

PET_CONTROL_TYPE_MAP = {
    "fatigue": DebuffType.FATIGUE,
    "stun": DebuffType.STUN,
    "freeze": DebuffType.FREEZE,
    "petrify": DebuffType.PETRIFY,
    "sleep": DebuffType.SLEEP,
    "root": DebuffType.ROOT,
    "fear": DebuffType.FEAR,
    "seal": DebuffType.SEAL,
    "paralysis": DebuffType.PARALYSIS,
    "silence": DebuffType.SILENCE,
    "healing_block": DebuffType.HEALING_BLOCK,
}

VALID_FIELDS = {"name", "type", "value", "coefficient", "is_debuff", "duration", "skill_type"}


def format_effect_desc(effect_type, is_db, value=None):
    if effect_type in BUFF_DESC_TEMPLATES and not is_db:
        template = BUFF_DESC_TEMPLATES[effect_type]
    elif effect_type in DEBUFF_DESC_TEMPLATES:
        template = DEBUFF_DESC_TEMPLATES[effect_type]
    else:
        return "未知效果"

    if value is None:
        return template

    return template.format(value=value)


def add_after_last_damage(msg, add_text):
    before_last, separator, after_last = msg.rpartition("伤害！")
    if separator:
        return before_last + "伤害！" + add_text + after_last
    return msg
