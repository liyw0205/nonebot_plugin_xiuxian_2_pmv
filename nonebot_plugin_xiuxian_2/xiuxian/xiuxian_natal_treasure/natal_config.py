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
    # 新增效果
    SLEEP       = 14    # 睡眠 (攻击时有概率让对方陷入睡眠)
    PETRIFY     = 15    # 石化 (攻击时有概率让对方陷入石化)
    STUN        = 16    # 眩晕 (攻击时有概率让对方陷入眩晕)
    FATIGUE     = 17    # 疲劳 (攻击时有概率让对方攻击力下降)
    SILENCE     = 18    # 沉默 (攻击时有概率让对方无法使用神通)
    CHARGE      = 19    # 蓄力 (这回合不攻击，下回合伤害增加)
    DIVINE_POWER = 20   # 神力 (伤害增加)
    NIRVANA     = 21    # 涅槃 (生命值低于0时进入涅槃，三回合后满血复活并获得护盾)
    SOUL_RETURN = 22    # 魂返 (生命值低于0时进入灵体，三回合后回复部分生命值复活)
    SOUL_SUMMON = 23    # 招魂 (攻击时有概率让已死亡的队友进入魂返状态)
    ENLIGHTENMENT = 24  # 启明 (攻击时有概率让已死亡的队友回复10%生命值复活)


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
    ],
    # 新增效果名称
    NatalEffectType.SLEEP: [
        "幽梦铃", "迷魂香", "幻眠灯", "沉睡之眼", "安息符"
    ],
    NatalEffectType.PETRIFY: [
        "石化之瞳", "定形咒", "磐石印", "化石珠", "凝滞之镜"
    ],
    NatalEffectType.STUN: [
        "震魂锣", "镇神钟", "破势鼓", "雷霆一击", "惊神符"
    ],
    NatalEffectType.FATIGUE: [
        "疲惫之刃", "倦怠之雾", "虚弱之链", "散力咒", "无力之手"
    ],
    NatalEffectType.SILENCE: [
        "禁言符", "锁咒匣", "无声之刃", "断音琴", "噤声铃"
    ],
    NatalEffectType.CHARGE: [
        "蓄力符", "冲锋号角", "破晓之光", "决战之心", "战意凝结"
    ],
    NatalEffectType.DIVINE_POWER: [
        "神力之斧", "天神之怒", "巨力符", "狂暴之血", "战神之魂"
    ],
    NatalEffectType.NIRVANA: [
        "涅槃火羽", "不死鸟蛋", "凤凰心", "重生莲", "不灭金身"
    ],
    NatalEffectType.SOUL_RETURN: [
        "魂体玉佩", "灵影披风", "幽魂灯", "归墟石", "万魂幡"
    ],
    NatalEffectType.SOUL_SUMMON: [
        "招魂幡", "引魂灯", "还魂丹", "聚灵阵", "唤灵笛"
    ],
    NatalEffectType.ENLIGHTENMENT: [
        "启明灯", "回春符", "生机泉", "复苏草", "涅槃金丹"
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
    NatalEffectType.TWIN_STRIKE: "双生",
    # 新增效果中文名称
    NatalEffectType.SLEEP:       "睡眠",
    NatalEffectType.PETRIFY:     "石化",
    NatalEffectType.STUN:        "眩晕",
    NatalEffectType.FATIGUE:     "疲劳",
    NatalEffectType.SILENCE:     "沉默",
    NatalEffectType.CHARGE:      "蓄力",
    NatalEffectType.DIVINE_POWER: "神力",
    NatalEffectType.NIRVANA:     "涅槃",
    NatalEffectType.SOUL_RETURN: "魂返",
    NatalEffectType.SOUL_SUMMON: "招魂",
    NatalEffectType.ENLIGHTENMENT: "启明"
}

# 反向映射，用于通过中文名称查找枚举
EFFECT_NAME_TO_TYPE = {v: k for k, v in EFFECT_NAME_MAP.items()}

# 效果的基础值和每级成长值 (百分比值，0.01 = 1%)
# 对于概率类效果，值代表概率 (如0.05=5%)
# min_value/max_value: 觉醒/铭刻时，该效果基础值的随机范围
# growth: 效果每提升1级，增加的数值 (Effect Level Growth)
EFFECT_BASE_AND_GROWTH = {
    NatalEffectType.BLEED:        {"min_value": 0.025, "max_value": 0.05, "growth": 0.005},
    NatalEffectType.ARMOR_BREAK:  {"min_value": 0.08, "max_value": 0.15, "growth": 0.02},
    NatalEffectType.EVASION:      {"min_value": 0.07, "max_value": 0.12, "growth": 0.015},
    NatalEffectType.SHIELD:       {"min_value": 0.15, "max_value": 0.35, "growth": 0.025},
    NatalEffectType.SHIELD_BREAK: {"min_value": 0.10, "max_value": 0.20, "growth": 0.02}, # 护盾无视百分比
    NatalEffectType.REFLECT_DAMAGE: {"min_value": 0.05, "max_value": 0.10, "growth": 0.015},
    NatalEffectType.TRUE_DAMAGE:  {"min_value": 0.03, "max_value": 0.07, "growth": 0.01},
    NatalEffectType.CRIT_RESIST:  {"min_value": 0.08, "max_value": 0.15, "growth": 0.02},
    NatalEffectType.FATE:         {"min_value": 0.01, "max_value": 0.03, "growth": 0.005}, # 概率
    NatalEffectType.IMMORTAL:     {"min_value": 0.10, "max_value": 0.20, "growth": 0.02}, # 恢复百分比
    NatalEffectType.DEATH_STRIKE: {"min_value": 0.10, "max_value": 0.15, "growth": 0.01}, # 低血量阈值，值代表低于X%触发
    NatalEffectType.INVINCIBLE:   {"min_value": 0.50, "max_value": 0.00, "growth": 0.01}, # min_value 为基础触发概率, growth 为效果等级增加的概率
    # 双生效果：min_value 为触发概率, max_value 为额外伤害百分比 (固定100%)
    NatalEffectType.TWIN_STRIKE:  {"min_value": 0.12, "max_value": 1.0, "growth": 0.005},
    # 新增效果配置
    NatalEffectType.SLEEP:        {"min_value": 0.05, "max_value": 0.10, "growth": 0.01}, # 触发概率
    NatalEffectType.PETRIFY:      {"min_value": 0.05, "max_value": 0.10, "growth": 0.01}, # 触发概率
    NatalEffectType.STUN:         {"min_value": 0.03, "max_value": 0.07, "growth": 0.005}, # 触发概率
    NatalEffectType.FATIGUE:      {"min_value": 0.08, "max_value": 0.15, "growth": 0.015}, # 触发概率
    NatalEffectType.SILENCE:      {"min_value": 0.08, "max_value": 0.15, "growth": 0.015}, # 触发概率
    NatalEffectType.CHARGE:       {"min_value": 0.20, "max_value": 0.35, "growth": 0.02}, # 伤害增加百分比
    NatalEffectType.DIVINE_POWER: {"min_value": 0.10, "max_value": 0.20, "growth": 0.015}, # 伤害增加百分比
    NatalEffectType.NIRVANA:      {"min_value": 0.20, "max_value": 0.30, "growth": 0.02}, # 护盾百分比 (基于最大生命)
    NatalEffectType.SOUL_RETURN:  {"min_value": 0.10, "max_value": 0.15, "growth": 0.01}, # 回复生命百分比
    NatalEffectType.SOUL_SUMMON:  {"min_value": 0.08, "max_value": 0.15, "growth": 0.015}, # 招魂触发概率
    NatalEffectType.ENLIGHTENMENT: {"min_value": 0.05, "max_value": 0.10, "growth": 0.01}, # 启明触发概率
}

# 神秘经书ID，用于法宝重塑和效果升阶
MYSTERIOUS_SCRIPTURE_ID = 20009

# 铭刻道纹费用
MYSTERIOUS_SCRIPTURE_COST_ENGRAVE = 1
# 遗忘道纹费用
MYSTERIOUS_SCRIPTURE_COST_FORGET = 3


# 法宝总等级上限
MAX_TREASURE_LEVEL = 10
# 效果等级上限 (所有效果统一)
MAX_EFFECT_LEVEL_ALL_EFFECTS = 10
# 效果槽位上限
MAX_EFFECT_SLOTS = 3

# 本命法宝养成经验配置
MAX_EXP_BASE = 100 # 初始等级升级所需经验
MAX_EXP_GROWTH_PER_LEVEL = 100 # 每提升1级法宝总等级，所需经验增加量

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

# 新增效果持续时间等配置
SLEEP_DURATION = 2 # 睡眠持续回合数
PETRIFY_DURATION = 2 # 石化持续回合数
STUN_DURATION = 1 # 眩晕持续回合数
FATIGUE_DURATION = 2 # 疲劳持续回合数
FATIGUE_ATTACK_REDUCTION = 0.30 # 疲劳攻击力降低百分比
SILENCE_DURATION = 2 # 沉默持续回合数

NIRVANA_DURATION = 3 # 涅槃持续回合数
NIRVANA_SHIELD_BASE = 0.20 # 涅槃复活后基础护盾百分比 (基于最大生命)
NIRVANA_REVIVE_LIMIT = 1 # 涅槃触发次数上限

SOUL_RETURN_DURATION = 3 # 魂返持续回合数
SOUL_RETURN_HP_BASE = 0.10 # 魂返复活后基础HP百分比 (基于最大生命)
SOUL_RETURN_REVIVE_LIMIT = 1 # 魂返触发次数上限

# 招魂和启明效果次数限制
SOUL_SUMMON_LIMIT = 1 # 招魂触发次数上限 (每个队友)
ENLIGHTENMENT_LIMIT = 1 # 启明触发次数上限 (每个队友)
ENLIGHTENMENT_REVIVE_HP_PERCENT = 0.10 # 启明复活时回复的基础生命百分比

# 蓄力效果额外伤害
CHARGE_BONUS_DAMAGE = 0.50 # 蓄力后额外增加50%伤害 (这个值是基准，实际会从法宝效果中获取)

# 石化Debuff对被攻击伤害的减免
PETRIFY_DAMAGE_REDUCTION_PERCENT = 0.30 # 石化状态下被攻击伤害减免30%

# 禁疗Debuff禁止回复持续回合随机范围
HEALING_BLOCK_DURATION_MIN = 1
HEALING_BLOCK_DURATION_MAX = 3