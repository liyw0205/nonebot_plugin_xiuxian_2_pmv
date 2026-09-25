"""Pure accessory rule tables shared by readers and legacy commands."""


AFFIX_KEY_MAP = {
    "气血": "hp_pct",
    "抗暴": "crit_resist",
    "防御": "dmg_reduction",
    "会心": "crit_rate",
    "会心伤害": "crit_damage",
    "攻击": "atk_pct",
    "速度": "speed",
}


SET_BONUS = {
    "烈阳": {
        2: {"type": "attack", "value": 0.08},
        4: {"type": "true_damage", "value": 0.06},
    },
    "玄渊": {
        2: {"type": "shield", "value": 0.12},
        4: {"type": "reflect", "value": 0.12},
    },
    "天衡": {
        2: {"type": "armor_pen", "value": 0.08},
        4: {"type": "dmg_reduction", "value": 0.10},
    },
    "星痕": {
        2: {"type": "crit_rate", "value": 0.06},
        4: {"type": "dodge", "value": 12},
    },
    "龙魄": {
        2: {"type": "attack", "value": 0.06},
        4: {"type": "shield_break", "value": 0.10},
    },
    "踏风": {
        2: {"type": "speed_pct", "value": 0.08},
        4: {"type": "speed_pct", "value": 0.18},
    },
}
