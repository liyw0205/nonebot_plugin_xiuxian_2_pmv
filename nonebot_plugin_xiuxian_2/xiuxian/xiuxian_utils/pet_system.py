try:
    import ujson as json
except ImportError:
    import json
import random
import time
from pathlib import Path

from nonebot.log import logger

from .xiuxian2_handle import PlayerDataManager

DATABASE = Path() / "data" / "xiuxian"
PET_CONFIG_PATH = DATABASE / "宠物" / "宠物.json"
PET_SKILL_CONFIG_PATH = DATABASE / "宠物" / "宠物技能.json"

TABLE = "player_pet"
FIELDS = ["active", "bag", "egg_pity_count", "egg_pity_no_mythic_count"]

EGG_COST = 1000000
EGG_PITY_THRESHOLD = 1000
EGG_PITY_RARITY_WEIGHTS = {
    "卓越": 50,
    "传说": 35,
    "神话": 15,
}

RARITIES = ["常见", "普通", "卓越", "传说", "神话"]
RACES = ["仙兽", "妖兽", "鬼怪", "凡兽"]
PET_TYPES = ["攻击", "增益", "保护"]
RARITY_INDEX = {name: idx for idx, name in enumerate(RARITIES)}
RARITY_ROLL_WEIGHTS = {
    "常见": 589,
    "普通": 300,
    "卓越": 100,
    "传说": 10,
    "神话": 1,
}

RARITY_MAX_STARS = {
    "常见": 5,
    "普通": 10,
    "卓越": 15,
    "传说": 20,
    "神话": 25,
}

FORM_NAMES = ["初始形态", "成长期", "完全期", "巅峰期", "超越形态"]
FORM_STAR_FLOOR = [1, 5, 10, 15, 20]
FORM_POWER = [1.0, 1.08, 1.18, 1.3, 1.45]
RARITY_POWER = {
    "常见": 0.9,
    "普通": 1.0,
    "卓越": 1.08,
    "传说": 1.18,
    "神话": 1.3,
}
PET_EXCLUSIVE_POWER_RATE = 0.45

PET_SKILL_ATTACK = "攻击"
PET_SKILL_BUFF = "增益"
PET_SKILL_PROTECT = "保护"

RACE_POWER = {
    "仙兽": {"攻击": 1.0, "增益": 1.08, "保护": 1.06},
    "妖兽": {"攻击": 1.10, "增益": 0.97, "保护": 0.97},
    "鬼怪": {"攻击": 1.04, "增益": 1.05, "保护": 1.02},
    "凡兽": {"攻击": 1.0, "增益": 1.0, "保护": 1.03},
}

TALENT_NAMES = {
    ("仙兽", "攻击"): "仙魄灵击",
    ("仙兽", "增益"): "瑞光加持",
    ("仙兽", "保护"): "灵华护主",
    ("妖兽", "攻击"): "凶魄扑杀",
    ("妖兽", "增益"): "妖血沸腾",
    ("妖兽", "保护"): "妖骨守御",
    ("鬼怪", "攻击"): "幽冥噬魂",
    ("鬼怪", "增益"): "鬼火燃心",
    ("鬼怪", "保护"): "阴雾护形",
    ("凡兽", "攻击"): "灵爪突袭",
    ("凡兽", "增益"): "灵息鼓舞",
    ("凡兽", "保护"): "护主灵盾",
}

QIMING_STONE_ID = 20032
PET_RELEASE_REFUND_ITEM_ID = 20027
PET_RELEASE_REFUND_RATE = 80
PET_EGG_IDS = {
    20033: "常见",
    20034: "普通",
    20035: "卓越",
    20036: "传说",
    20037: "神话",
}
PET_FEED_EXP_KEY = "pet_feed_exp"
PET_FEED_STAR_TIER_KEY = "pet_feed_star_tier"
PET_EGG_RARITY_KEY = "pet_egg_rarity"
FEED_BASE_EXP = {
    "药材": 80,
}
HERB_TIER_NAMES = {
    "一品": 1,
    "二品": 2,
    "三品": 3,
    "四品": 4,
    "五品": 5,
    "六品": 6,
    "七品": 7,
    "八品": 8,
    "九品": 9,
}
EXCLUSIVE_SKILL_ROLL_BASE_CHANCE = 0.12
EXCLUSIVE_SKILL_ROLL_FORM_BONUS = 0.03
EXCLUSIVE_SKILL_ROLL_RARITY_BONUS = 0.02
EXCLUSIVE_SKILL_ROLL_MAX_CHANCE = 0.35
EXCLUSIVE_SKILL_HIGH_STAR_CHANCE = 0.50
EXCLUSIVE_SKILL_HIGH_STAR_TARGETS = {20, 25}
PET_BREAKTHROUGH_RULES = {
    "传说": {
        15: "普通",
        20: "普通",
    },
    "神话": {
        20: "卓越",
        25: "卓越",
    },
}

player_data_manager = PlayerDataManager()
_PET_POOL_CACHE = None
_PET_SKILL_CACHE = None


def _default_pet_doc():
    return {
        "active": None,
        "bag": [],
        "egg_pity_count": 0,
        "egg_pity_no_mythic_count": 0,
    }


def _normalize_pet_doc(doc: dict):
    if not isinstance(doc, dict):
        doc = _default_pet_doc()

    active = doc.get("active")
    if active is not None and not isinstance(active, dict):
        active = None

    bag = doc.get("bag")
    if not isinstance(bag, list):
        bag = []
    bag = [x for x in bag if isinstance(x, dict)]

    try:
        egg_pity_count = int(doc.get("egg_pity_count", 0) or 0)
    except Exception:
        egg_pity_count = 0
    try:
        egg_pity_no_mythic_count = int(doc.get("egg_pity_no_mythic_count", 0) or 0)
    except Exception:
        egg_pity_no_mythic_count = 0

    doc["active"] = _normalize_pet(active) if active else None
    doc["bag"] = [_normalize_pet(x) for x in bag]
    doc["egg_pity_count"] = max(0, egg_pity_count)
    doc["egg_pity_no_mythic_count"] = min(9, max(0, egg_pity_no_mythic_count))
    return doc


def _normalize_pet(pet: dict):
    if not isinstance(pet, dict):
        return pet

    pet["uid"] = str(pet.get("uid") or f"pet_{int(time.time())}_{random.randint(1000, 9999)}")
    pet["pet_id"] = str(pet.get("pet_id", ""))
    pet["name"] = str(pet.get("name", "未知宠物"))
    pet["rarity"] = str(pet.get("rarity", "常见"))
    pet["race"] = str(pet.get("race", "凡兽"))
    pet["type"] = str(pet.get("type", "攻击"))
    if pet["rarity"] not in RARITIES:
        pet["rarity"] = "常见"
    if pet["race"] not in RACES:
        pet["race"] = "凡兽"
    if pet["type"] not in PET_TYPES:
        pet["type"] = PET_SKILL_ATTACK

    try:
        pet["stars"] = int(pet.get("stars", 1))
    except Exception:
        pet["stars"] = 1
    pet["stars"] = max(1, min(25, pet["stars"]))
    pet["max_stars"] = get_rarity_max_stars(pet["rarity"])
    pet["stars"] = min(pet["stars"], pet["max_stars"])

    try:
        pet["exp"] = int(pet.get("exp", 0))
    except Exception:
        pet["exp"] = 0
    pet["exp"] = max(0, pet["exp"])

    forms = pet.get("forms")
    if not isinstance(forms, list) or len(forms) < 5:
        forms = [pet["name"]] * 5
    pet["forms"] = [str(x) for x in forms[:5]]
    pet["form_index"] = get_form_index(pet["stars"])
    pet["form_name"] = pet["forms"][pet["form_index"]]

    skills = pet.get("skills")
    if not isinstance(skills, list):
        old_skill = pet.get("skill")
        skills = [old_skill] if isinstance(old_skill, dict) else []
    skills = [
        dict(skill)
        for skill in skills
        if isinstance(skill, dict) and skill.get("type") == pet["type"]
    ]
    if not skills:
        skills = [roll_basic_pet_skill(pet)]
    pet["skills"] = skills[:1]
    pet["skill"] = pet["skills"][0]
    return pet


def get_pet_doc(user_id: str | int):
    doc = player_data_manager.get_doc(
        user_id=str(user_id),
        table_name=TABLE,
        fields=FIELDS,
        default_factory=_default_pet_doc,
    )
    return _normalize_pet_doc(doc)


def save_pet_doc(user_id: str | int, data: dict):
    data = _normalize_pet_doc(data)
    player_data_manager.save_doc(
        user_id=str(user_id),
        table_name=TABLE,
        data=data,
        fields=FIELDS,
        dirty_check=True,
    )


def get_active_pet(user_id: str | int):
    return get_pet_doc(user_id).get("active")


def has_any_pet(user_id: str | int) -> bool:
    data = get_pet_doc(user_id)
    return bool(data.get("active") or data.get("bag"))


def get_pet_bag_rows(data: dict):
    data = _normalize_pet_doc(data)
    rows = []
    active_uid = ""

    active = data.get("active")
    if active:
        active_uid = str(active.get("uid", ""))
        row = dict(active)
        row["is_active"] = True
        rows.append(row)

    bag_rows = []
    for pet in data.get("bag", []):
        if active_uid and str(pet.get("uid", "")) == active_uid:
            continue
        row = dict(pet)
        row["is_active"] = False
        bag_rows.append(row)

    bag_rows.sort(key=lambda pet: (
        -RARITY_INDEX.get(str(pet.get("rarity", "常见")), 0),
        -int(pet.get("stars", 1)),
        str(pet.get("form_name", pet.get("name", ""))),
        str(pet.get("uid", "")),
    ))
    rows.extend(bag_rows)

    return rows


def load_pet_pool():
    global _PET_POOL_CACHE
    if _PET_POOL_CACHE is not None:
        return _PET_POOL_CACHE

    try:
        with open(PET_CONFIG_PATH, "r", encoding="utf-8") as f:
            raw = json.loads(f.read() or "{}")
    except Exception as e:
        logger.warning(f"宠物池加载失败: {e}")
        raw = {}

    pool = {}
    if isinstance(raw, dict):
        for pet_id, item in raw.items():
            if not isinstance(item, dict):
                continue

            rarity = str(item.get("rarity", "常见"))
            race = str(item.get("race", "凡兽"))
            pet_type = str(item.get("type", "攻击"))
            forms = item.get("forms")

            if rarity not in RARITIES or race not in RACES or pet_type not in PET_TYPES:
                continue
            if not isinstance(forms, list) or len(forms) < 5:
                continue

            try:
                weight = max(1, int(item.get("weight", 1)))
            except Exception:
                weight = 1

            pool[str(pet_id)] = {
                "pet_id": str(pet_id),
                "name": str(item.get("name", forms[0])),
                "rarity": rarity,
                "race": race,
                "type": pet_type,
                "forms": [str(x) for x in forms[:5]],
                "weight": weight,
            }

    _PET_POOL_CACHE = pool
    return _PET_POOL_CACHE


def get_pet_template(pet_id: str):
    return load_pet_pool().get(str(pet_id))


def _normalize_skill(raw: dict, skill_id: str, category: str):
    if not isinstance(raw, dict):
        return None

    skill_type = str(raw.get("type", ""))
    if skill_type not in PET_TYPES:
        return None

    min_rarity = str(raw.get("min_rarity", "常见"))
    if min_rarity not in RARITIES:
        min_rarity = "常见"

    try:
        base_power = float(raw.get("base_power", 0))
    except Exception:
        base_power = 0
    if base_power <= 0:
        return None

    try:
        min_form = int(raw.get("min_form", 0))
    except Exception:
        min_form = 0
    try:
        min_stars = int(raw.get("min_stars", 1))
    except Exception:
        min_stars = 1
    try:
        weight = int(raw.get("weight", 1))
    except Exception:
        weight = 1

    exclusive_pet_ids = raw.get("exclusive_pet_ids", [])
    if not isinstance(exclusive_pet_ids, list):
        exclusive_pet_ids = []

    default_effect = {
        PET_SKILL_ATTACK: "damage",
        PET_SKILL_BUFF: "attack_buff",
        PET_SKILL_PROTECT: "shield",
    }.get(skill_type, "damage")

    def get_float(key: str, default=0.0):
        try:
            return float(raw.get(key, default))
        except Exception:
            return default

    def get_int(key: str, default=0):
        try:
            return int(raw.get(key, default))
        except Exception:
            return default

    return {
        "skill_id": str(skill_id),
        "name": str(raw.get("name", "未知宠物技能")),
        "type": skill_type,
        "scope": str(raw.get("scope", "通用")),
        "category": str(raw.get("category", category)),
        "min_rarity": min_rarity,
        "min_form": max(0, min(4, min_form)),
        "min_stars": max(1, min(25, min_stars)),
        "base_power": base_power,
        "star_bonus": float(raw.get("star_bonus", 0)),
        "form_bonus": float(raw.get("form_bonus", 0)),
        "rarity_bonus": float(raw.get("rarity_bonus", 0)),
        "weight": max(1, weight),
        "exclusive_pet_ids": [str(x) for x in exclusive_pet_ids],
        "desc": str(raw.get("desc", "")),
        "effect": str(raw.get("effect", default_effect)),
        "target_scope": str(raw.get("target_scope", "single")),
        "target_count": max(1, get_int("target_count", 1)),
        "hit_count": max(1, get_int("hit_count", 1)),
        "duration": max(1, get_int("duration", raw.get("turncost", 1))),
        "success": max(0.0, min(100.0, get_float("success", 100.0))),
        "min_power": get_float("min_power", 0.85),
        "max_power": get_float("max_power", 1.25),
        "hp_percent": max(0.0, get_float("hp_percent", 0.0)),
        "shield_penetration": max(0.0, min(1.0, get_float("shield_penetration", 0.0))),
        "dot_power": max(0.0, get_float("dot_power", 0.0)),
        "buff_scale": max(0.0, get_float("buff_scale", 1.0)),
        "shield_scale": max(0.0, get_float("shield_scale", 1.0)),
        "reflect_scale": max(0.0, get_float("reflect_scale", 1.0)),
        "buff_type": str(raw.get("buff_type", "")),
        "debuff_type": str(raw.get("debuff_type", "")),
        "control_type": str(raw.get("control_type", "")),
        "dot_type": str(raw.get("dot_type", "")),
    }


def load_pet_skill_pool():
    global _PET_SKILL_CACHE
    if _PET_SKILL_CACHE is not None:
        return _PET_SKILL_CACHE

    try:
        with open(PET_SKILL_CONFIG_PATH, "r", encoding="utf-8") as f:
            raw = json.loads(f.read() or "{}")
    except Exception as e:
        logger.warning(f"宠物技能池加载失败: {e}")
        raw = {}

    pool = {"basic": {}, "exclusive": {}}
    for group, category in (("basic", "基础"), ("exclusive", "专属")):
        group_data = raw.get(group, {}) if isinstance(raw, dict) else {}
        if not isinstance(group_data, dict):
            continue
        for skill_id, skill_data in group_data.items():
            skill = _normalize_skill(skill_data, str(skill_id), category)
            if skill:
                pool[group][str(skill_id)] = skill

    _PET_SKILL_CACHE = pool
    return _PET_SKILL_CACHE


def _skill_available_for_pet(skill: dict, pet: dict, exclusive: bool = False):
    if skill.get("type") != pet.get("type"):
        return False

    pet_rarity = pet.get("rarity", "常见")
    if RARITY_INDEX.get(pet_rarity, 0) < RARITY_INDEX.get(skill.get("min_rarity", "常见"), 0):
        return False
    if int(pet.get("form_index", 0)) < int(skill.get("min_form", 0)):
        return False
    if int(pet.get("stars", 1)) < int(skill.get("min_stars", 1)):
        return False

    if exclusive:
        ids = skill.get("exclusive_pet_ids") or []
        return str(pet.get("pet_id", "")) in ids

    return True


def get_available_basic_skills(pet: dict):
    pet = dict(pet)
    skills = load_pet_skill_pool().get("basic", {})
    return [
        dict(skill)
        for skill in skills.values()
        if _skill_available_for_pet(skill, pet, exclusive=False)
    ]


def get_available_exclusive_skills(pet: dict):
    pet = dict(pet)
    skills = load_pet_skill_pool().get("exclusive", {})
    return [
        dict(skill)
        for skill in skills.values()
        if _skill_available_for_pet(skill, pet, exclusive=True)
    ]


def _fallback_basic_skill(pet: dict):
    pet_type = pet.get("type", PET_SKILL_ATTACK)
    talent_name = TALENT_NAMES.get((pet.get("race", "凡兽"), pet_type), TALENT_NAMES[("凡兽", pet_type)])
    if pet_type == PET_SKILL_ATTACK:
        desc = "出手造成直接伤害"
        base_power = 0.42
    elif pet_type == PET_SKILL_BUFF:
        desc = "为主人提升攻击"
        base_power = 0.36
    else:
        desc = "为主人施加护盾"
        base_power = 0.38

    return {
        "skill_id": f"fallback_{pet_type}",
        "name": talent_name,
        "type": pet_type,
        "scope": "通用",
        "category": "基础",
        "base_power": base_power,
        "star_bonus": 0.01,
        "form_bonus": 0.04,
        "rarity_bonus": 0.02,
        "desc": desc,
    }


def roll_basic_pet_skill(pet: dict, exclude_skill_ids: set[str] | None = None):
    candidates = get_available_basic_skills(pet)
    if exclude_skill_ids:
        filtered = [
            skill
            for skill in candidates
            if str(skill.get("skill_id", "")) not in exclude_skill_ids
        ]
        if filtered:
            candidates = filtered

    if not candidates:
        return _fallback_basic_skill(pet)

    rarity_idx = RARITY_INDEX.get(pet.get("rarity", "常见"), 0)
    form_index = int(pet.get("form_index", 0))
    stars = int(pet.get("stars", 1))

    weights = []
    for skill in candidates:
        min_rarity_idx = RARITY_INDEX.get(skill.get("min_rarity", "常见"), 0)
        rarity_gap = max(0, rarity_idx - min_rarity_idx)
        weight = int(skill.get("weight", 1))
        weight += rarity_gap * 8
        weight += form_index * 4
        weight += max(0, stars - int(skill.get("min_stars", 1)))
        weights.append(max(1, weight))

    return dict(random.choices(candidates, weights=weights, k=1)[0])


def _exclusive_skill_roll_chance(pet: dict, override_chance: float | None = None):
    if not get_available_exclusive_skills(pet):
        return 0.0

    if override_chance is not None:
        return max(0.0, min(1.0, float(override_chance)))

    form_index = int(pet.get("form_index", 0))
    rarity_idx = RARITY_INDEX.get(pet.get("rarity", "常见"), 0)
    chance = (
        EXCLUSIVE_SKILL_ROLL_BASE_CHANCE
        + form_index * EXCLUSIVE_SKILL_ROLL_FORM_BONUS
        + rarity_idx * EXCLUSIVE_SKILL_ROLL_RARITY_BONUS
    )
    return max(0.0, min(EXCLUSIVE_SKILL_ROLL_MAX_CHANCE, chance))


def get_star_up_exclusive_chance(stars: int):
    try:
        stars = int(stars)
    except Exception:
        return None
    if stars in EXCLUSIVE_SKILL_HIGH_STAR_TARGETS:
        return EXCLUSIVE_SKILL_HIGH_STAR_CHANCE
    return None


def roll_pet_skill(
    pet: dict,
    include_exclusive: bool = False,
    exclude_skill_ids: set[str] | None = None,
    exclusive_chance: float | None = None,
):
    if include_exclusive:
        exclusive_candidates = get_available_exclusive_skills(pet)
        if exclude_skill_ids:
            exclusive_candidates = [
                skill
                for skill in exclusive_candidates
                if str(skill.get("skill_id", "")) not in exclude_skill_ids
            ]

        chance = _exclusive_skill_roll_chance(pet, override_chance=exclusive_chance)
        if exclusive_candidates and random.random() < chance:
            weights = [max(1, int(skill.get("weight", 1))) for skill in exclusive_candidates]
            return dict(random.choices(exclusive_candidates, weights=weights, k=1)[0])

    return roll_basic_pet_skill(pet, exclude_skill_ids=exclude_skill_ids)


def roll_replacement_pet_skill(pet: dict, exclusive_chance: float | None = None):
    current_skill = pet.get("skill") or (pet.get("skills") or [{}])[0]
    exclude = set()
    if isinstance(current_skill, dict) and current_skill.get("skill_id"):
        exclude.add(str(current_skill.get("skill_id")))
    return roll_pet_skill(
        pet,
        include_exclusive=True,
        exclude_skill_ids=exclude,
        exclusive_chance=exclusive_chance,
    )


def get_pet_runtime_skill(pet: dict):
    skill = pet.get("skill") or (pet.get("skills") or [{}])[0]
    if not isinstance(skill, dict) or skill.get("type") not in PET_TYPES:
        return roll_basic_pet_skill(pet)

    return dict(skill)


def roll_pet_template():
    pool = list(load_pet_pool().values())
    if not pool:
        raise RuntimeError("宠物池为空，请检查 data/xiuxian/宠物/宠物.json")

    available_rarities = {pet.get("rarity") for pet in pool}
    rarities = [
        rarity
        for rarity in RARITIES
        if rarity in available_rarities and RARITY_ROLL_WEIGHTS.get(rarity, 0) > 0
    ]
    rarity = random.choices(
        rarities,
        weights=[RARITY_ROLL_WEIGHTS[rarity] for rarity in rarities],
        k=1,
    )[0]
    return roll_pet_template_by_rarity(rarity)


def roll_pet_template_by_rarity(rarity: str):
    rarity = str(rarity)
    pool = [
        pet
        for pet in load_pet_pool().values()
        if pet.get("rarity") == rarity
    ]
    if not pool:
        raise RuntimeError(f"未找到{rarity}稀有度宠物，请检查 data/xiuxian/宠物/宠物.json")
    return random.choices(pool, weights=[p.get("weight", 1) for p in pool], k=1)[0]


def create_pet_instance(template: dict | None = None):
    if template is None:
        template = roll_pet_template()

    pet = {
        "uid": f"pet_{int(time.time())}_{random.randint(1000, 9999)}",
        "pet_id": template["pet_id"],
        "name": template["name"],
        "rarity": template["rarity"],
        "race": template["race"],
        "type": template["type"],
        "forms": template["forms"],
        "stars": 1,
        "exp": 0,
        "total_exp": 0,
    }
    return _normalize_pet(pet)


def _put_pet_into_doc(data: dict, pet: dict):
    pet = _normalize_pet(pet)
    if data.get("active"):
        data["bag"].append(pet)
        return pet, "bag"

    data["active"] = pet
    return pet, "active"


def roll_egg_pity_rarity(no_mythic_count: int = 0):
    no_mythic_count = max(0, int(no_mythic_count))
    if no_mythic_count >= 9:
        return "神话", True

    rarities = list(EGG_PITY_RARITY_WEIGHTS.keys())
    rarity = random.choices(
        rarities,
        weights=[EGG_PITY_RARITY_WEIGHTS[rarity] for rarity in rarities],
        k=1,
    )[0]
    return rarity, False


def grant_pet_egg_pity_rewards(user_id: str | int, draw_count: int):
    draw_count = max(0, int(draw_count))
    data = get_pet_doc(user_id)
    pity_count = int(data.get("egg_pity_count", 0)) + draw_count
    no_mythic_count = int(data.get("egg_pity_no_mythic_count", 0))
    rewards = []

    while pity_count >= EGG_PITY_THRESHOLD:
        pity_count -= EGG_PITY_THRESHOLD
        rarity, forced_mythic = roll_egg_pity_rarity(no_mythic_count)
        template = roll_pet_template_by_rarity(rarity)
        pet, location = _put_pet_into_doc(data, create_pet_instance(template))

        if rarity == "神话":
            no_mythic_count = 0
        else:
            no_mythic_count += 1

        rewards.append({
            "pet": pet,
            "location": location,
            "rarity": rarity,
            "forced_mythic": forced_mythic,
            "no_mythic_count": no_mythic_count,
        })

    data["egg_pity_count"] = pity_count
    data["egg_pity_no_mythic_count"] = no_mythic_count
    save_pet_doc(user_id, data)
    return rewards, pity_count, no_mythic_count


def grant_pet_as_active(user_id: str | int, pet: dict | None = None):
    pet, _ = grant_pet(user_id, pet)
    return pet


def grant_pet(user_id: str | int, pet: dict | None = None):
    data = get_pet_doc(user_id)
    new_pet = create_pet_instance() if pet is None else _normalize_pet(pet)
    new_pet, location = _put_pet_into_doc(data, new_pet)
    save_pet_doc(user_id, data)
    return new_pet, location


def grant_pet_by_rarity(user_id: str | int, rarity: str):
    template = roll_pet_template_by_rarity(rarity)
    return grant_pet(user_id, create_pet_instance(template))


def find_pet_anywhere(data: dict, token: str):
    token = str(token).strip()
    if not token:
        return None, None, None

    active = data.get("active")
    if active and str(active.get("uid")) == token:
        return "active", None, active

    for idx, pet in enumerate(data.get("bag", [])):
        if str(pet.get("uid")) == token:
            return "bag", idx, pet

    return None, None, None


def set_active_pet(user_id: str | int, uid: str):
    data = get_pet_doc(user_id)
    where, key, pet = find_pet_anywhere(data, uid)
    if not pet:
        return False, "未找到该宠物UID。", None
    if where == "active":
        return True, f"{pet.get('form_name', pet.get('name', '宠物'))}已在出战。", pet

    current = data.get("active")
    if current:
        data["bag"].append(current)

    data["active"] = pet
    del data["bag"][key]
    save_pet_doc(user_id, data)
    return True, f"已设置出战宠物：{pet.get('form_name', pet.get('name', '宠物'))}（UID:{pet.get('uid')}）", pet


def remove_pet(user_id: str | int, token: str | None = None):
    data = get_pet_doc(user_id)
    removed = None

    if not token:
        removed = data.get("active")
        data["active"] = None
    else:
        where, key, pet = find_pet_anywhere(data, token)
        if where == "active":
            removed = pet
            data["active"] = None
        elif where == "bag":
            removed = pet
            del data["bag"][key]

    if removed:
        save_pet_doc(user_id, data)
    return removed


def _pet_matches_release_keyword(pet: dict, keyword: str):
    keyword = str(keyword).strip()
    if not keyword:
        return False

    if keyword in RARITIES:
        return pet.get("rarity") == keyword

    names = {str(pet.get("name", "")), str(pet.get("form_name", ""))}
    forms = pet.get("forms")
    if isinstance(forms, list):
        names.update(str(name) for name in forms)
    return keyword in names


def remove_pets_by_keyword(user_id: str | int, keyword: str, include_active: bool = False):
    data = get_pet_doc(user_id)
    keyword = str(keyword).strip()
    removed = []
    skipped_active = False

    if not keyword:
        return removed, skipped_active

    active = data.get("active")
    if active and _pet_matches_release_keyword(active, keyword):
        if include_active:
            removed.append(active)
            data["active"] = None
        else:
            skipped_active = True

    kept_bag = []
    for pet in data.get("bag", []):
        if _pet_matches_release_keyword(pet, keyword):
            removed.append(pet)
        else:
            kept_bag.append(pet)
    data["bag"] = kept_bag

    if removed:
        save_pet_doc(user_id, data)
    return removed, skipped_active


def get_rarity_max_stars(rarity: str) -> int:
    return RARITY_MAX_STARS.get(str(rarity), 5)


def get_form_index(stars: int) -> int:
    stars = max(1, min(25, int(stars)))
    idx = 0
    for i, floor in enumerate(FORM_STAR_FLOOR):
        if stars >= floor:
            idx = i
    return idx


def get_star_ratio(stars: int) -> float:
    stars = max(1, min(25, int(stars)))
    return min(0.52, 0.10 + (stars - 1) * 0.018)


def get_pet_star_tier(stars: int) -> int:
    stars = max(1, min(25, int(stars)))
    return max(1, min(5, (stars + 4) // 5))


def format_stars(stars: int) -> str:
    stars = max(1, min(25, int(stars)))
    big = stars // 5
    small = stars % 5
    return ("★" * big + "☆" * small) or "☆"


def requires_fusion_for_next_star(stars: int) -> bool:
    stars = max(1, min(25, int(stars)))
    return stars < 25 and stars % 5 == 4


def get_pet_exp_stage_multiplier(stars: int) -> int:
    stars = max(1, min(25, int(stars)))
    star_count = stars // 5
    if requires_fusion_for_next_star(stars):
        star_count += 1
    return 1 + star_count * 2


def exp_to_next_star(stars: int) -> int:
    stars = max(1, min(25, int(stars)))
    return stars * 150 * get_pet_exp_stage_multiplier(stars)


def calc_pet_total_exp(pet: dict) -> int:
    try:
        if "total_exp" in pet and pet.get("total_exp") is not None:
            return max(0, int(pet.get("total_exp", 0)))
    except Exception:
        pass

    pet = _normalize_pet(dict(pet))
    stars = int(pet.get("stars", 1))
    current_exp = int(pet.get("exp", 0))
    used_exp = sum(exp_to_next_star(star) for star in range(1, stars))
    return max(0, used_exp + current_exp)


def calc_pet_release_refund(pet: dict, item_info: dict | None = None):
    total_exp = calc_pet_total_exp(pet)
    refund_base_exp = total_exp * PET_RELEASE_REFUND_RATE // 100
    refund_exp = 800

    if isinstance(item_info, dict):
        try:
            refund_exp = int(item_info.get(PET_FEED_EXP_KEY, refund_exp) or refund_exp)
        except Exception:
            refund_exp = 800

    refund_exp = max(1, refund_exp)
    return refund_base_exp // refund_exp, total_exp, refund_base_exp, refund_exp


def is_pet_feed_item(item_info: dict) -> bool:
    if not isinstance(item_info, dict):
        return False

    item_type = item_info.get("item_type", item_info.get("type", ""))
    if item_type == "药材":
        return True

    try:
        pet_feed_exp = int(item_info.get(PET_FEED_EXP_KEY, 0) or 0)
    except Exception:
        pet_feed_exp = 0

    return (
        item_type == "特殊物品"
        and item_info.get("type") == "特殊道具"
        and pet_feed_exp > 0
    )


def validate_pet_feed_item(pet: dict, item_info: dict):
    if not is_pet_feed_item(item_info):
        return False, "宠物只能喂食药材，或专用的宠物特殊道具。"

    try:
        item_star_tier = int(item_info.get(PET_FEED_STAR_TIER_KEY, 0) or 0)
    except Exception:
        item_star_tier = 0

    if item_star_tier > 0:
        pet_star_tier = get_pet_star_tier(int(pet.get("stars", 1)))
        if pet_star_tier > item_star_tier:
            return (
                False,
                f"{item_info.get('name', '该道具')}最高只能喂食{format_stars(item_star_tier * 5)}及以下宠物，"
                f"当前宠物为{format_stars(pet.get('stars', 1))}。",
            )

    return True, ""


def calc_feed_exp(item_info: dict, count: int = 1):
    if not isinstance(item_info, dict):
        return 0

    item_type = item_info.get("item_type", item_info.get("type", ""))
    if not is_pet_feed_item(item_info):
        return 0

    try:
        special_exp = int(item_info.get(PET_FEED_EXP_KEY, 0) or 0)
    except Exception:
        special_exp = 0
    if special_exp > 0:
        return special_exp * max(1, int(count))

    base = FEED_BASE_EXP.get(item_type, 0)

    if item_type == "药材":
        base *= get_herb_tier(item_info)
        return max(0, int(base) * max(1, int(count)))

    try:
        rank = int(item_info.get("rank", 0))
    except Exception:
        rank = 0
    if rank > 0:
        base += max(0, 60 - rank) * 5

    try:
        buff = int(float(item_info.get("buff", 0)))
    except Exception:
        buff = 0
    if buff > 0:
        base += min(1200, max(0, buff // 10000000) * 20)

    return max(0, int(base) * max(1, int(count)))


def get_herb_tier(item_info: dict) -> int:
    if not isinstance(item_info, dict):
        return 1

    level = str(item_info.get("level", ""))
    for tier_name, tier in HERB_TIER_NAMES.items():
        if tier_name in level:
            return tier

    try:
        rank = int(item_info.get("rank", 0))
    except Exception:
        rank = 0
    if rank > 0:
        if rank >= 48:
            return 1
        return max(1, min(9, (51 - rank) // 3))

    return 1


def feed_active_pet(user_id: str | int, feed_exp: int):
    data = get_pet_doc(user_id)
    pet = data.get("active")
    if not pet:
        return None, 0, [], []

    old_form = int(pet.get("form_index", 0))
    max_stars = get_rarity_max_stars(pet.get("rarity", "常见"))

    if int(pet.get("stars", 1)) >= max_stars:
        return pet, 0, [], []

    gained_exp = max(0, int(feed_exp))
    try:
        if "total_exp" in pet and pet.get("total_exp") is not None:
            current_total_exp = int(pet.get("total_exp", 0))
        else:
            current_total_exp = calc_pet_total_exp(pet)
    except Exception:
        current_total_exp = calc_pet_total_exp(pet)

    pet["total_exp"] = max(0, current_total_exp) + gained_exp
    pet["exp"] = int(pet.get("exp", 0)) + gained_exp
    upgraded = 0
    form_changes = []
    skill_offers = []

    while int(pet.get("stars", 1)) < max_stars:
        need = exp_to_next_star(int(pet["stars"]))
        if int(pet["exp"]) < need:
            break
        if requires_fusion_for_next_star(int(pet["stars"])):
            pet["exp"] = need
            break
        pet["exp"] -= need
        pet["stars"] += 1
        upgraded += 1

        new_form = get_form_index(int(pet["stars"]))
        if new_form != old_form:
            old_form = new_form
            form_changes.append(pet["forms"][new_form])

        if int(pet["stars"]) % 5 == 0:
            current_pet = _normalize_pet(dict(pet))
            exclusive_chance = get_star_up_exclusive_chance(
                int(current_pet.get("stars", pet["stars"]))
            )
            skill_offers.append({
                "stars": int(current_pet.get("stars", pet["stars"])),
                "skill": roll_replacement_pet_skill(
                    current_pet,
                    exclusive_chance=exclusive_chance,
                ),
            })

    pet = _normalize_pet(pet)
    data["active"] = pet
    save_pet_doc(user_id, data)
    return pet, upgraded, form_changes, skill_offers


def fusion_need(stars: int) -> int:
    stars = max(1, min(25, int(stars)))
    return min(5, max(1, ((stars - 1) // 5) + 1))


def get_pet_breakthrough_requirement(pet: dict):
    if not isinstance(pet, dict):
        return None

    try:
        next_stars = int(pet.get("stars", 1)) + 1
    except Exception:
        return None

    if next_stars % 5 != 0:
        return None

    required_rarity = PET_BREAKTHROUGH_RULES.get(str(pet.get("rarity", "")), {}).get(next_stars)
    if not required_rarity:
        return None

    return {
        "target_stars": next_stars,
        "rarity": required_rarity,
        "required_stars": get_rarity_max_stars(required_rarity),
    }


def _is_valid_breakthrough_material(pet: dict, requirement: dict | None):
    if not requirement or not isinstance(pet, dict):
        return False
    required_rarity = str(requirement.get("rarity", ""))
    required_stars = int(requirement.get("required_stars", get_rarity_max_stars(required_rarity)))
    return (
        str(pet.get("rarity", "")) == required_rarity
        and int(pet.get("stars", 1)) >= required_stars
    )


def fuse_pet(user_id: str | int, material_tokens: list[str]):
    data = get_pet_doc(user_id)
    main_pet = data.get("active")
    if not main_pet:
        return False, "融合失败：当前没有出战宠物，请先使用【出战宠物 UID】设置主宠。", None, None

    max_stars = get_rarity_max_stars(main_pet.get("rarity", "常见"))
    current_stars = int(main_pet.get("stars", 1))
    if current_stars >= max_stars:
        return False, f"{main_pet.get('form_name', main_pet.get('name', '宠物'))}已达到{main_pet.get('rarity', '')}稀有度上限（{format_stars(max_stars)}）。", main_pet, None

    if not requires_fusion_for_next_star(current_stars):
        return (
            False,
            f"融合失败：当前品阶为{format_stars(current_stars)}，尚未到四☆突破关口，请先通过【宠物喂食】提升☆。",
            main_pet,
            None,
        )

    fusion_exp_need = exp_to_next_star(current_stars)
    current_exp = int(main_pet.get("exp", 0))
    if current_exp < fusion_exp_need:
        return (
            False,
            f"融合失败：四☆突破经验不足（{current_exp} / {fusion_exp_need}），请先通过【宠物喂食】补足经验。",
            main_pet,
            None,
        )

    need = fusion_need(current_stars)
    breakthrough_requirement = get_pet_breakthrough_requirement(main_pet)

    hit_indexes = []
    breakthrough_index = None
    breakthrough_pet = None
    main_uid = str(main_pet.get("uid", ""))
    used_uids = {main_uid} if main_uid else set()
    for token in material_tokens:
        w, idx, pet = find_pet_anywhere(data, token)
        if w != "bag" or pet is None:
            continue
        pet_uid = str(pet.get("uid", ""))
        if pet_uid in used_uids:
            continue

        is_same_body = str(pet.get("pet_id")) == str(main_pet.get("pet_id"))
        is_breakthrough = _is_valid_breakthrough_material(pet, breakthrough_requirement)

        if is_same_body and len(hit_indexes) < need:
            hit_indexes.append(idx)
            used_uids.add(pet_uid)
            continue

        if is_breakthrough and breakthrough_index is None:
            breakthrough_index = idx
            breakthrough_pet = pet
            used_uids.add(pet_uid)
            continue

    missing_msgs = []
    if len(hit_indexes) < need:
        missing_msgs.append(f"同名本体不足：已匹配{len(hit_indexes)}/{need}只")
    if breakthrough_requirement and breakthrough_index is None:
        missing_msgs.append(
            f"破阶宠不足：破入{format_stars(breakthrough_requirement['target_stars'])}"
            f"需要1只满★{breakthrough_requirement['rarity']}宠物"
        )
    if missing_msgs:
        return False, "融合失败：\n" + "\n".join(missing_msgs), main_pet, None

    consume_indexes = hit_indexes[:need]
    if breakthrough_index is not None:
        consume_indexes.append(breakthrough_index)

    for idx in sorted(set(consume_indexes), reverse=True):
        del data["bag"][idx]

    old_form = int(main_pet.get("form_index", 0))
    main_pet["stars"] = current_stars + 1
    main_pet["exp"] = max(0, current_exp - fusion_exp_need)
    main_pet = _normalize_pet(main_pet)
    skill_offer = None
    if int(main_pet.get("stars", 1)) % 5 == 0:
        exclusive_chance = get_star_up_exclusive_chance(int(main_pet.get("stars", 1)))
        skill_offer = {
            "stars": int(main_pet.get("stars", 1)),
            "skill": roll_replacement_pet_skill(
                main_pet,
                exclusive_chance=exclusive_chance,
            ),
        }

    data["active"] = main_pet

    save_pet_doc(user_id, data)

    form_msg = ""
    if int(main_pet.get("form_index", 0)) != old_form:
        form_msg = f"\n形态进化：{main_pet.get('form_name')}"

    breakthrough_msg = ""
    if breakthrough_pet:
        breakthrough_msg = (
            f"\n破阶消耗：满★{breakthrough_requirement['rarity']}"
            f"【{breakthrough_pet.get('form_name', breakthrough_pet.get('name', '宠物'))}】"
        )

    return True, f"融合成功：消耗四☆突破经验{fusion_exp_need}，{main_pet.get('name')}提升至{format_stars(main_pet.get('stars', 1))}。{form_msg}{breakthrough_msg}", main_pet, skill_offer


def replace_pet_skill(user_id: str | int, uid: str, skill: dict):
    if not isinstance(skill, dict):
        return None

    data = get_pet_doc(user_id)
    where, key, pet = find_pet_anywhere(data, uid)
    if not pet:
        return None
    if skill.get("type") != pet.get("type"):
        return None

    pet["skills"] = [dict(skill)]
    pet["skill"] = dict(skill)
    pet = _normalize_pet(pet)

    if where == "active":
        data["active"] = pet
    else:
        data["bag"][key] = pet

    save_pet_doc(user_id, data)
    return pet


def reroll_pet_skill(user_id: str | int, uid: str | None = None):
    data = get_pet_doc(user_id)
    if uid:
        where, key, pet = find_pet_anywhere(data, uid)
    else:
        where, key, pet = ("active", None, data.get("active"))

    if not pet:
        return None, None

    new_skill = roll_replacement_pet_skill(pet)
    pet["skills"] = [new_skill]
    pet["skill"] = new_skill
    pet = _normalize_pet(pet)

    if where == "active":
        data["active"] = pet
    else:
        data["bag"][key] = pet

    save_pet_doc(user_id, data)
    return pet, new_skill


def build_pet_detail(pet: dict):
    pet = _normalize_pet(dict(pet))
    ratio = get_star_ratio(pet.get("stars", 1))
    max_stars = get_rarity_max_stars(pet.get("rarity", "常见"))
    need = exp_to_next_star(pet.get("stars", 1)) if pet.get("stars", 1) < max_stars else 0
    skill = build_pet_battle_skill(pet)
    exclusive_skills = get_available_exclusive_skills(pet)

    lines = [
        f"名称：{pet.get('form_name', pet.get('name', '未知宠物'))}",
        f"本体：{pet.get('name', '未知宠物')}",
        f"UID：{pet.get('uid', '')}",
        f"稀有度：{pet.get('rarity', '常见')}",
        f"种族：{pet.get('race', '凡兽')}",
        f"类型：{pet.get('type', '攻击')}",
        f"品阶：{format_stars(pet.get('stars', 1))} / {format_stars(max_stars)}",
        f"形态：{FORM_NAMES[pet.get('form_index', 0)]}",
        f"主人属性继承：{ratio * 100:.0f}%",
        f"当前技能：{skill['name']}（{skill.get('scope', '通用')}·{skill.get('category', '基础')}，{skill['desc']}）",
    ]
    if exclusive_skills:
        if skill.get("scope") == "专属":
            lines.append(f"已领悟专属：{skill.get('raw_name', '未知专属')}")
        else:
            lines.append("可随机领悟专属：" + "、".join(s.get("name", "未知专属") for s in exclusive_skills[:3]))

    if need:
        current_exp = min(int(pet.get("exp", 0)), need)
        lines.append(f"经验：{current_exp} / {need}")
        if requires_fusion_for_next_star(pet.get("stars", 1)):
            lines.append(f"四☆突破所需本体：{fusion_need(pet.get('stars', 1))}只")
            if current_exp >= need:
                lines.append("四☆突破经验已满，可使用【宠物融合】突破。")
            else:
                lines.append("四☆突破前需先喂食补满经验。")
            breakthrough_requirement = get_pet_breakthrough_requirement(pet)
            if breakthrough_requirement:
                lines.append(
                    f"破阶所需：满★{breakthrough_requirement['rarity']}宠物1只"
                    f"（破入{format_stars(breakthrough_requirement['target_stars'])}）"
                )
        else:
            lines.append("升☆方式：宠物喂食")
    else:
        lines.append("经验：已达当前稀有度上限")

    return "\n".join(lines)


def build_pet_battle_skill(pet: dict):
    pet = _normalize_pet(dict(pet))
    skill = get_pet_runtime_skill(pet)

    pet_type = skill.get("type", pet.get("type", PET_SKILL_ATTACK))
    race = pet.get("race", "凡兽")
    stars = int(pet.get("stars", 1))
    form_index = int(pet.get("form_index", get_form_index(stars)))

    ratio = get_star_ratio(stars)
    race_power = RACE_POWER.get(race, RACE_POWER["凡兽"]).get(pet_type, 1.0)
    form_power = FORM_POWER[form_index]
    rarity = pet.get("rarity", "常见")
    rarity_power = RARITY_POWER.get(rarity, 1.0)
    rarity_idx = RARITY_INDEX.get(rarity, 0)
    base_power = float(skill.get("base_power", 0.4))
    star_growth = 1 + max(0, stars - 1) * float(skill.get("star_bonus", 0))
    form_growth = 1 + form_index * float(skill.get("form_bonus", 0))
    rarity_growth = 1 + rarity_idx * float(skill.get("rarity_bonus", 0))
    power = ratio * race_power * form_power * rarity_power * base_power * star_growth * form_growth * rarity_growth
    if skill.get("category") == "专属" or skill.get("scope") == "专属":
        power *= PET_EXCLUSIVE_POWER_RATE

    skill_name = skill.get("name") or TALENT_NAMES.get((race, pet_type), TALENT_NAMES[("凡兽", pet_type)])
    name = f"{pet.get('form_name', pet.get('name', '宠物'))}·{skill_name}"

    if pet_type == PET_SKILL_ATTACK:
        desc = skill.get("desc") or "出手造成直接伤害"
    elif pet_type == PET_SKILL_BUFF:
        desc = skill.get("desc") or "为主人提升攻击"
    else:
        desc = skill.get("desc") or "为主人施加护盾"

    return {
        "skill_id": skill.get("skill_id", ""),
        "type": pet_type,
        "name": name,
        "raw_name": skill_name,
        "scope": skill.get("scope", "通用"),
        "category": skill.get("category", "基础"),
        "power": power,
        "base_power": base_power,
        "ratio": ratio,
        "desc": desc,
        "race": race,
        "form_index": form_index,
        "effect": skill.get("effect", "damage" if pet_type == PET_SKILL_ATTACK else "attack_buff"),
        "target_scope": skill.get("target_scope", "single"),
        "target_count": int(skill.get("target_count", 1)),
        "hit_count": int(skill.get("hit_count", 1)),
        "duration": int(skill.get("duration", 1)),
        "success": float(skill.get("success", 100.0)),
        "min_power": float(skill.get("min_power", 0.85)),
        "max_power": float(skill.get("max_power", 1.25)),
        "hp_percent": float(skill.get("hp_percent", 0.0)),
        "shield_penetration": float(skill.get("shield_penetration", 0.0)),
        "dot_power": float(skill.get("dot_power", 0.0)),
        "buff_scale": float(skill.get("buff_scale", 1.0)),
        "shield_scale": float(skill.get("shield_scale", 1.0)),
        "reflect_scale": float(skill.get("reflect_scale", 1.0)),
        "buff_type": skill.get("buff_type", ""),
        "debuff_type": skill.get("debuff_type", ""),
        "control_type": skill.get("control_type", ""),
        "dot_type": skill.get("dot_type", ""),
    }


def get_user_pet_for_battle(user_id: str | int):
    pet = get_active_pet(user_id)
    if not pet:
        return None
    return {
        "uid": pet.get("uid", ""),
        "name": pet.get("name", ""),
        "form_name": pet.get("form_name", pet.get("name", "")),
        "rarity": pet.get("rarity", "常见"),
        "race": pet.get("race", "凡兽"),
        "type": pet.get("type", "攻击"),
        "stars": int(pet.get("stars", 1)),
        "skills": pet.get("skills", []),
        "skill": build_pet_battle_skill(pet),
    }
