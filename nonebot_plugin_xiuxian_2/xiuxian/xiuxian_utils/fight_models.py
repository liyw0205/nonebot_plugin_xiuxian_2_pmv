from ..xiuxian_natal_treasure.natal_config import EFFECT_BASE_AND_GROWTH, NatalEffectType
from .fight_effects import BuffType, DebuffType, VALID_FIELDS
from .utils import number_to


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
        self.is_scarecrow = bool(data.get("is_scarecrow") or self.name == "稻草人")

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
        self.pet = data.get("pet")
        self.pet_runtime = {
            "active_round": 0,
            "guard_round": 0,
            "control_rescue_round": 0,
            "resonance_round": 0,
            "protect_trigger_count": 0,
        }
        self.pet_stats = {
            "trigger": 0,
            "damage": 0,
            "damage_boost": 0,
            "damage_reduced": 0,
            "shield": 0,
            "support": 0,
            "guard": 0,
            "resonance": 0,
            "healing": 0,
            "cleanse": 0,
            "control": 0,
            "rescue": 0,
        }
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

        current = max(0, min(current, max_value))
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

        return f"{self.name}剩余血量{number_to(int(current))}\n{stat.upper()} {bar} {int(ratio * 100)}%{suffix}"

    @property
    def is_alive(self):
        return self.hp > 0

    @property
    def atk_rate(self):
        pct = self._get_effect_value(BuffType.ATTACK_UP, DebuffType.ATTACK_DOWN)
        return max(0, self.base_atk * (1 + pct))

    @property
    def crit_rate(self):
        """
        最终会心率：
        - base_crit 可以超过100%
        - 战斗中的会心提升/降低在这里统一结算
        - 最终参与判定时限制在 0%~100%
        """
        val = self.base_crit + self._get_effect_value(
            BuffType.CRIT_RATE_UP,
            DebuffType.CRIT_RATE_DOWN
        )
        return max(0.0, min(1.0, val))

    @property
    def crit_dmg_rate(self):
        val = self.base_crit_dmg + self._get_effect_value(BuffType.CRIT_DAMAGE_UP, DebuffType.CRIT_DAMAGE_DOWN)
        return max(1.0, val)

    @property
    def damage_reduction_rate(self):
        """
        原始减伤率，不在这里做95%上限截断。

        原因：
        - 减伤可能通过功法/装备/饰品/套装堆到 100% 以上。
        - 穿甲/破甲需要先从原始减伤中扣除。
        - 最终参与伤害计算时，再限制到最高95%。
        """
        val = self.base_damage_reduction + self._get_effect_value(
            BuffType.DAMAGE_REDUCTION_UP,
            DebuffType.DEFENSE_DOWN
        )
        return val

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
    def speed_rate(self):
        speed_pct = self._get_effect_value(BuffType.SPEED_UP, DebuffType.SPEED_DOWN)
        return max(1.0, self.base_speed * max(0.1, 1 + speed_pct))

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
        if self.is_scarecrow:
            return 0
        total = 0.0
        for debuff in self.debuffs:
            if debuff.type == DebuffType.POISON_DOT:
                total += self.max_hp * debuff.value
        return int(total)

    @property
    def bleed_dot_dmg(self):
        if self.is_scarecrow:
            return 0
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

    def sync_healing_block_turns(self):
        turns = [
            debuff.duration if debuff.duration > 0 else 1
            for debuff in self.debuffs
            if debuff.type == DebuffType.HEALING_BLOCK
        ]
        self.healing_block_turns = max(turns) if turns else 0

    def check_and_clear_debuffs_by_immunity(self):
        if self.has_buff("type", BuffType.DEBUFF_IMMUNITY):
            self.debuffs.clear()
            self.healing_block_turns = 0

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
