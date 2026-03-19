import random
from enum import IntEnum

# ======================
#   本命法宝相关枚举与名字池
# ======================
class NatalEffectType(IntEnum):
    """本命法宝效果类型枚举"""
    BLEED       = 1     # 流血 (每回合对敌方造成持续伤害)
    ARMOR_BREAK = 2     # 破甲 (降低敌方防御，提升自身穿甲)
    EVASION     = 3     # 闪避 (提升自身闪避率)
    SHIELD      = 4     # 护盾 (开局获得护盾，周期性刷新)
    SHIELD_BREAK = 5    # 破盾 (攻击时部分伤害无视护盾，且当对方有护盾时攻击额外造成伤害)
    REFLECT_DAMAGE = 6  # 反伤 (被攻击时反还部分伤害)
    TRUE_DAMAGE = 7     # 真伤 (攻击时额外造成真实伤害，无视减伤和护盾)
    CRIT_RESIST = 8     # 抗暴 (减少被暴击时受到的伤害)
    FATE        = 9     # 天命 (生命值低于0时低概率恢复满血)
    IMMORTAL    = 10    # 不灭 (生命值低于0时50%概率恢复部分血量)
    DEATH_STRIKE = 11   # 斩命 (禁止天命效果，且对低血量敌人直接斩杀)
    INVINCIBLE  = 12    # 无敌 (周期性获得无敌效果，抵挡伤害)
    TWIN_STRIKE = 13    # 双生 (普通攻击时有概率造成连击，再造成一次额外伤害)


NATAL_TREASURE_NAMES = {
    # 各效果类型对应的法宝名称池
    NatalEffectType.BLEED: [
        "噬血魔刃", "赤焰血镰", "幽冥泣血", "血祭之瞳", "荆棘之环"
    ],
    NatalEffectType.ARMOR_BREAK: [
        "裂空碎甲", "灭地魔戟", "破军星刃", "穿魂箭", "断魂枪"
    ],
    NatalEffectType.EVASION: [
        "幻影流光", "瞬息千影", "虚空遁形", "风神羽衣", "魅影披风"
    ],
    NatalEffectType.SHIELD: [
        "玄龟镇岳", "不灭金钟", "太初护魂", "磐石壁垒", "真武宝甲"
    ],
    NatalEffectType.SHIELD_BREAK: [
        "裂盾锥", "破障刃", "虚无法珠", "穿甲弹", "洞察之眼"
    ],
    NatalEffectType.REFLECT_DAMAGE: [
        "荆棘甲", "反噬之镜", "回响战鼓", "业火莲台", "绝境反击"
    ],
    NatalEffectType.TRUE_DAMAGE: [
        "湮灭之光", "无量真诀", "混沌虚剑", "裁决之镰", "净世法印"
    ],
    NatalEffectType.CRIT_RESIST: [
        "坚韧护心镜", "不屈战纹", "铁壁符文", "金刚体", "龙鳞甲"
    ],
    NatalEffectType.FATE: [
        "天命神符", "涅槃圣印", "不朽道果", "轮回之盘", "鸿蒙紫气"
    ],
    NatalEffectType.IMMORTAL: [
        "不灭金身", "残生续命", "化血神术", "回天玉露", "万劫不灭"
    ],
    NatalEffectType.DEATH_STRIKE: [
        "斩命刀", "绝杀令", "灭魂幡", "审判之镰", "破界神枪"
    ],
    NatalEffectType.INVINCIBLE: [
        "无敌金钟", "不坏之身", "虚无道衣", "绝对防御", "不朽符文"
    ],
    NatalEffectType.TWIN_STRIKE: [
        "双生幻刃", "镜像重击", "分影疾刺", "轮回分身", "重影剑匣"
    ]
}


# 效果类型的中文显示名称
EFFECT_NAME_MAP = {
    # 将枚举类型映射到中文名称，用于显示
    NatalEffectType.BLEED:       "流血",
    NatalEffectType.ARMOR_BREAK: "破甲",
    NatalEffectType.EVASION:     "闪避",
    NatalEffectType.SHIELD:      "护盾",
    NatalEffectType.SHIELD_BREAK: "破盾",
    NatalEffectType.REFLECT_DAMAGE: "反伤",
    NatalEffectType.TRUE_DAMAGE: "真伤",
    NatalEffectType.CRIT_RESIST: "抗暴",
    NatalEffectType.FATE:        "天命",
    NatalEffectType.IMMORTAL:    "不灭",
    NatalEffectType.DEATH_STRIKE: "斩命",
    NatalEffectType.INVINCIBLE:  "无敌",
    NatalEffectType.TWIN_STRIKE: "双生"
}

# 效果的基础值和每级成长值 (百分比值，0.01 = 1%)
# 对于概率类效果，值代表概率 (如0.05=5%)
# min_single/max_single: 单效果法宝觉醒时，该效果基础值的随机范围
# min_double/max_double: 双效果法宝觉醒时，该效果基础值的随机范围
# growth: 效果每提升1级，增加的数值 (Effect Level Growth)
EFFECT_BASE_AND_GROWTH = {
    NatalEffectType.BLEED:        {"min_single": 0.05, "max_single": 0.10, "min_double": 0.03, "max_double": 0.07, "growth": 0.015},
    NatalEffectType.ARMOR_BREAK:  {"min_single": 0.08, "max_single": 0.15, "min_double": 0.05, "max_double": 0.10, "growth": 0.02},
    NatalEffectType.EVASION:      {"min_single": 0.07, "max_single": 0.12, "min_double": 0.04, "max_double": 0.08, "growth": 0.015},
    NatalEffectType.SHIELD:       {"min_single": 0.15, "max_single": 0.35, "min_double": 0.05, "max_double": 0.10, "growth": 0.025},
    NatalEffectType.SHIELD_BREAK: {"min_single": 0.10, "max_single": 0.20, "min_double": 0.06, "max_double": 0.12, "growth": 0.02}, # 护盾无视百分比
    NatalEffectType.REFLECT_DAMAGE: {"min_single": 0.05, "max_single": 0.10, "min_double": 0.03, "max_double": 0.07, "growth": 0.015},
    NatalEffectType.TRUE_DAMAGE:  {"min_single": 0.03, "max_single": 0.07, "min_double": 0.02, "max_double": 0.05, "growth": 0.01},
    NatalEffectType.CRIT_RESIST:  {"min_single": 0.08, "max_single": 0.15, "min_double": 0.05, "max_double": 0.10, "growth": 0.02},
    NatalEffectType.FATE:         {"min_single": 0.01, "max_single": 0.03, "min_double": 0.005, "max_double": 0.015, "growth": 0.005}, # 概率
    NatalEffectType.IMMORTAL:     {"min_single": 0.10, "max_single": 0.20, "min_double": 0.05, "max_double": 0.10, "growth": 0.02}, # 恢复百分比
    NatalEffectType.DEATH_STRIKE: {"min_single": 0.10, "max_single": 0.15, "min_double": 0.07, "max_double": 0.10, "growth": 0.01}, # 低血量阈值，值代表低于X%触发
    NatalEffectType.INVINCIBLE:   {"min_single": 0.50, "max_single": 0.00, "min_double": 0.25, "max_double": 0.00, "growth": 0.01}, # min_single/double 为基础触发概率, growth 为效果等级增加的概率
    # 双生效果：min_single/double 为触发概率, max_single/double 为额外伤害百分比 (固定100%), growth 为每效果等级增加的概率
    NatalEffectType.TWIN_STRIKE:  {"min_single": 0.12, "max_single": 1.0, "min_double": 0.08, "max_double": 1.0, "growth": 0.005},
}

# 神秘经书ID，用于法宝重塑和效果升阶
MYSTERIOUS_SCRIPTURE_ID = 20009

# 法宝总等级上限
MAX_TREASURE_LEVEL = 10
# 单效果等级上限
MAX_EFFECT_LEVEL_SINGLE = 10
# 双效果等级上限 (当法宝拥有两个效果时，每个效果的等级上限)
MAX_EFFECT_LEVEL_DOUBLE = 8
# 天命复活次数上限 (每场战斗)
FATE_REVIVE_COUNT_LIMIT = 1
# 不灭复活次数上限 (每场战斗)
IMMORTAL_REVIVE_COUNT_LIMIT = 3
# 无敌次数上限 (每次生效周期获得，每场战斗累计)
INVINCIBLE_COUNT_LIMIT = 1
# 无敌首次获得概率 (基准值，不包括法宝总等级成长)
INVINCIBLE_FIRST_GAIN_CHANCE = 0.50 # 50%
# 无敌后续获得概率 (基准值，不包括法宝总等级成长)
INVINCIBLE_SUBSEQUENT_GAIN_CHANCE = 0.25 # 25%
# 无敌效果，法宝总等级每提升1级，获得概率增加值 (Natal Treasure Level Growth for Invincible)
INVINCIBLE_GROWTH_PER_LEVEL_NATAL_TREASURE = 0.01 # 1%

# 周期性真实伤害的基础值和每法宝总等级成长值
PERIODIC_TRUE_DAMAGE_BASE = 0.01 # 1% (法宝0级时)
PERIODIC_TRUE_DAMAGE_GROWTH_PER_LEVEL = 0.005 # 0.5% (法宝总等级每提升1级，周期道韵伤害增加0.5%)

# 破盾效果的额外伤害加成 (当对方有护盾时)
SHIELD_BREAK_BONUS_DAMAGE = 0.20 # 额外造成20%伤害