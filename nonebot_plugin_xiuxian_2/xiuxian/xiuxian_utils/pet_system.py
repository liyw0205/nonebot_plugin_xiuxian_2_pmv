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
HERB_CONFIG_PATH = DATABASE / "丹药" / "药材.json"

TABLE = "player_pet"
PET_ITEM_TABLE = "player_pet_item"
PET_BAG_LIMIT = 1000
LEGACY_JSON_FIELDS = ["active", "bag"]
FIELDS = ["active_uid", "egg_pity_count", "egg_pity_no_mythic_count", "travel"]
PET_META_STORAGE_FIELDS = FIELDS + LEGACY_JSON_FIELDS
PET_ITEM_FIELDS = [
    "id",
    "user_id",
    "uid",
    "is_active",
    "pet_id",
    "stars",
    "exp",
    "total_exp",
    "skill_id",
    "created_at",
    "updated_at",
]
PET_ITEM_LEGACY_FIELDS = ["name", "rarity", "race", "pet_type", "forms", "skills", "skill"]

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

PET_TRAVEL_MIN_HOURS = 1
PET_TRAVEL_MAX_HOURS = 12
PET_TRAVEL_ITEM_POOLS = {
    "forage": {
        "name": "灵草谷",
        "desc": "采集低阶药材和灵髓，适合稳定补充宠物养成材料。",
        "items": [
            {"herb_levels": ["一品药材"], "min": 8, "max": 18, "weight": 40},
            {"herb_levels": ["二品药材"], "min": 6, "max": 14, "weight": 32},
            {"herb_levels": ["三品药材"], "min": 4, "max": 10, "weight": 18},
        ],
        "pet_egg_drop": {
            "chance_per_4_hours": 0.015,
            "cap": 0.045,
            "items": [
                {"id": 20033, "weight": 85},
                {"id": 20034, "weight": 15},
            ],
        },
        "marrow_drop": {
            "chance_per_4_hours": 0.04,
            "cap": 0.12,
            "items": [
                {"id": 20027, "weight": 70},
                {"id": 20028, "weight": 25},
                {"id": 20029, "weight": 5},
            ],
        },
    },
    "training": {
        "name": "妖兽岭",
        "desc": "磨砺宠物并搜寻中阶药材，宠物资源掉落更高。",
        "items": [
            {"herb_levels": ["三品药材"], "min": 6, "max": 14, "weight": 28},
            {"herb_levels": ["四品药材"], "min": 5, "max": 12, "weight": 32},
            {"herb_levels": ["五品药材"], "min": 3, "max": 8, "weight": 24},
            {"herb_levels": ["六品药材"], "min": 2, "max": 5, "weight": 12, "amount_multiplier_cap": 1.5},
        ],
        "pet_egg_drop": {
            "chance_per_4_hours": 0.03,
            "cap": 0.09,
            "items": [
                {"id": 20033, "weight": 55},
                {"id": 20034, "weight": 32},
                {"id": 20035, "weight": 12},
                {"id": 20036, "weight": 1},
            ],
        },
        "marrow_drop": {
            "chance_per_4_hours": 0.06,
            "cap": 0.18,
            "items": [
                {"id": 20027, "weight": 50},
                {"id": 20028, "weight": 35},
                {"id": 20029, "weight": 15},
            ],
        },
    },
    "rift": {
        "name": "秘境边缘",
        "desc": "风险更高，可能带回高阶药材和稀有灵宠蛋。",
        "items": [
            {"id": 18076, "min": 1, "max": 1, "weight": 5, "amount_multiplier_cap": 1.0},
            {"herb_levels": ["七品药材"], "min": 2, "max": 5, "weight": 28, "amount_multiplier_cap": 1.2},
            {"herb_levels": ["八品药材"], "min": 1, "max": 4, "weight": 30, "amount_multiplier_cap": 1.0},
            {"herb_levels": ["九品药材"], "min": 1, "max": 3, "weight": 24, "amount_multiplier_cap": 1.0},
        ],
        "pet_egg_drop": {
            "chance_per_4_hours": 0.05,
            "cap": 0.15,
            "items": [
                {"id": 20034, "weight": 42},
                {"id": 20035, "weight": 34},
                {"id": 20036, "weight": 18},
                {"id": 20037, "weight": 6},
            ],
        },
        "marrow_drop": {
            "chance_per_4_hours": 0.08,
            "cap": 0.24,
            "items": [
                {"id": 20027, "weight": 35},
                {"id": 20028, "weight": 40},
                {"id": 20029, "weight": 25},
            ],
        },
    },
}
PET_TRAVEL_SCENE_ALIASES = {
    "灵草谷": "forage",
    "采药": "forage",
    "药材": "forage",
    "妖兽岭": "training",
    "历练": "training",
    "磨砺": "training",
    "秘境边缘": "rift",
    "秘境": "rift",
    "探秘": "rift",
}
PET_TRAVEL_STORY_TEXTS = (
    "沿溪寻得一处灵草地，带回几缕温润灵气。",
    "避开山间妖气后折返，途中拾得散落材料。",
    "循着灵脉残响游走半日，归来时灵袋微沉。",
    "在古道旁守候机缘，带回些许可用资粮。",
    "踏过荒岭雾气，归来时精神更显灵动。",
)

player_data_manager = PlayerDataManager()
_PET_POOL_CACHE = None
_PET_SKILL_CACHE = None
_HERB_IDS_BY_LEVEL_CACHE = None
_PET_STORAGE_READY = False
_PET_STORAGE_MIGRATION_ATTEMPTED = False


def reset_pet_storage_state():
    global _PET_STORAGE_READY, _PET_STORAGE_MIGRATION_ATTEMPTED
    _PET_STORAGE_READY = False
    _PET_STORAGE_MIGRATION_ATTEMPTED = False


def _default_pet_doc():
    return {
        "active": None,
        "bag": [],
        "egg_pity_count": 0,
        "egg_pity_no_mythic_count": 0,
        "travel": None,
    }


def _normalize_pet_travel_state(travel: dict | None):
    if not isinstance(travel, dict):
        return None

    pet_uid = _clean_uid(travel.get("pet_uid", ""))
    if not pet_uid:
        return None

    scene = str(travel.get("scene", "forage") or "forage")
    if scene not in PET_TRAVEL_ITEM_POOLS:
        scene = "forage"

    try:
        start_at = int(float(travel.get("start_at", 0) or 0))
    except Exception:
        start_at = 0
    try:
        end_at = int(float(travel.get("end_at", 0) or 0))
    except Exception:
        end_at = 0
    try:
        duration_hours = int(float(travel.get("duration_hours", 1) or 1))
    except Exception:
        duration_hours = 1

    duration_hours = max(PET_TRAVEL_MIN_HOURS, min(PET_TRAVEL_MAX_HOURS, duration_hours))
    if start_at <= 0:
        start_at = int(time.time())
    if end_at <= start_at:
        end_at = start_at + duration_hours * 3600

    rarity = str(travel.get("pet_rarity", "常见") or "常见")
    if rarity not in RARITIES:
        rarity = "常见"
    try:
        stars = int(travel.get("pet_stars", 1) or 1)
    except Exception:
        stars = 1

    scene_info = PET_TRAVEL_ITEM_POOLS[scene]
    return {
        "pet_uid": pet_uid,
        "pet_name": str(travel.get("pet_name", "未知宠物") or "未知宠物"),
        "pet_rarity": rarity,
        "pet_stars": max(1, min(25, stars)),
        "scene": scene,
        "scene_name": scene_info["name"],
        "start_at": start_at,
        "end_at": end_at,
        "duration_hours": duration_hours,
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
    travel = _normalize_pet_travel_state(doc.get("travel"))
    if travel:
        pet_uids = set()
        if doc["active"]:
            pet_uids.add(str(doc["active"].get("uid", "")))
        pet_uids.update(str(pet.get("uid", "")) for pet in doc["bag"])
        if travel["pet_uid"] not in pet_uids:
            travel = None
    doc["travel"] = travel
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


def _json_dump(value):
    return json.dumps(value, ensure_ascii=False)


def _json_load(value, default=None):
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return default


def _to_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _clean_uid(value):
    value = "" if value is None else str(value)
    if value.lower() in {"none", "null"}:
        return ""
    return value


def _pet_storage_id(user_id: str, uid: str):
    return f"{user_id}:{uid}"


def _ensure_pet_storage():
    global _PET_STORAGE_READY
    if _PET_STORAGE_READY:
        return

    q = player_data_manager._quote_ident
    with player_data_manager._conn_lock:
        if _PET_STORAGE_READY:
            return

        player_data_manager._ensure_table_exists(TABLE)
        for field in PET_META_STORAGE_FIELDS:
            player_data_manager._ensure_field_exists(TABLE, field, "TEXT")

        cursor = player_data_manager._get_cursor()
        item_table_sql = q(PET_ITEM_TABLE)
        if not player_data_manager.conn.table_exists(PET_ITEM_TABLE):
            cursor.execute(
                f"""
                CREATE TABLE {item_table_sql} (
                    {q("id")} TEXT PRIMARY KEY,
                    {q("user_id")} TEXT NOT NULL,
                    {q("uid")} TEXT NOT NULL,
                    {q("is_active")} INTEGER DEFAULT 0,
                    {q("pet_id")} TEXT DEFAULT NULL,
                    {q("stars")} INTEGER DEFAULT 1,
                    {q("exp")} INTEGER DEFAULT 0,
                    {q("total_exp")} INTEGER DEFAULT 0,
                    {q("skill_id")} TEXT DEFAULT NULL,
                    {q("created_at")} INTEGER DEFAULT 0,
                    {q("updated_at")} INTEGER DEFAULT 0
                )
                """
            )
        else:
            existing_fields = set(player_data_manager.conn.column_names(PET_ITEM_TABLE))
            column_defs = {
                "id": "TEXT",
                "user_id": "TEXT",
                "uid": "TEXT",
                "is_active": "INTEGER DEFAULT 0",
                "pet_id": "TEXT DEFAULT NULL",
                "stars": "INTEGER DEFAULT 1",
                "exp": "INTEGER DEFAULT 0",
                "total_exp": "INTEGER DEFAULT 0",
                "skill_id": "TEXT DEFAULT NULL",
                "created_at": "INTEGER DEFAULT 0",
                "updated_at": "INTEGER DEFAULT 0",
            }
            for field, data_type in column_defs.items():
                if field not in existing_fields:
                    cursor.execute(f"ALTER TABLE {item_table_sql} ADD COLUMN {q(field)} {data_type}")

        cursor.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {q('idx_player_pet_item_user_uid')} "
            f"ON {item_table_sql} ({q('user_id')}, {q('uid')})"
        )
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {q('idx_player_pet_item_user_active')} "
            f"ON {item_table_sql} ({q('user_id')}, {q('is_active')})"
        )
        player_data_manager._commit_write()
        _PET_STORAGE_READY = True


def _row_to_dict(cursor, row):
    columns = [col[0] for col in cursor.description]
    return {column: value for column, value in zip(columns, row)}


def _get_pet_meta(user_id: str):
    meta = player_data_manager.get_doc(
        user_id=user_id,
        table_name=TABLE,
        fields=PET_META_STORAGE_FIELDS,
        default_factory=_default_pet_doc,
    )
    if meta is None:
        meta = _default_pet_doc()
        meta["user_id"] = user_id
    return meta


def _meta_active_uid(meta: dict):
    active_uid = _clean_uid(meta.get("active_uid", ""))
    if active_uid:
        return active_uid

    legacy_active = meta.get("active")
    if isinstance(legacy_active, str):
        return _clean_uid(legacy_active)
    return ""


def _legacy_payload_exists(meta: dict):
    return isinstance(meta.get("active"), dict) or isinstance(meta.get("bag"), list)


def _legacy_doc_from_meta(meta: dict):
    return {
        "active": meta.get("active") if isinstance(meta.get("active"), dict) else None,
        "bag": meta.get("bag") if isinstance(meta.get("bag"), list) else [],
        "egg_pity_count": _to_int(meta.get("egg_pity_count", 0), 0),
        "egg_pity_no_mythic_count": _to_int(meta.get("egg_pity_no_mythic_count", 0), 0),
        "travel": _normalize_pet_travel_state(meta.get("travel")),
    }


def get_pet_skill_by_id(skill_id: str | None, pet: dict | None = None):
    skill_id = str(skill_id or "").strip()
    if not skill_id:
        return None

    if skill_id.startswith("fallback_") and isinstance(pet, dict):
        return _fallback_basic_skill(pet)

    pool = load_pet_skill_pool()
    for group in ("basic", "exclusive"):
        skill = pool.get(group, {}).get(skill_id)
        if skill:
            return dict(skill)
    return None


def _legacy_skill_from_row(row: dict):
    skill = _json_load(row.get("skill"), default=None)
    if isinstance(skill, dict):
        return skill

    skills = _json_load(row.get("skills"), default=[])
    if isinstance(skills, list):
        for item in skills:
            if isinstance(item, dict):
                return item
    return None


def _row_to_pet(row: dict):
    template = get_pet_template(str(row.get("pet_id", "")))
    legacy_forms = _json_load(row.get("forms"), default=[])

    pet = {
        "uid": row.get("uid", ""),
        "pet_id": row.get("pet_id", "") or (template or {}).get("pet_id", ""),
        "name": (template or {}).get("name", row.get("name", "未知宠物")),
        "rarity": (template or {}).get("rarity", row.get("rarity", "常见")),
        "race": (template or {}).get("race", row.get("race", "凡兽")),
        "type": (template or {}).get("type", row.get("pet_type", PET_SKILL_ATTACK)),
        "forms": (template or {}).get("forms", legacy_forms),
        "stars": _to_int(row.get("stars", 1), 1),
        "exp": _to_int(row.get("exp", 0), 0),
        "total_exp": _to_int(row.get("total_exp", 0), 0),
    }

    skill_id = str(row.get("skill_id", "") or "").strip()
    legacy_skill = _legacy_skill_from_row(row)
    if not skill_id and isinstance(legacy_skill, dict):
        skill_id = str(legacy_skill.get("skill_id", "") or "").strip()

    skill = get_pet_skill_by_id(skill_id, pet)
    if skill is None:
        skill = legacy_skill

    if isinstance(skill, dict):
        pet["skill"] = skill
        pet["skills"] = [skill]
    return _normalize_pet(pet)


def _fetch_pet_rows(user_id: str):
    _ensure_pet_storage()
    q = player_data_manager._quote_ident
    with player_data_manager._conn_lock:
        cursor = player_data_manager._get_cursor()
        existing_fields = set(player_data_manager.conn.column_names(PET_ITEM_TABLE))
        select_fields = PET_ITEM_FIELDS + [
            field
            for field in PET_ITEM_LEGACY_FIELDS
            if field in existing_fields and field not in PET_ITEM_FIELDS
        ]
        col_sql = ", ".join(q(field) for field in select_fields)
        cursor.execute(
            f"""
            SELECT {col_sql}
            FROM {q(PET_ITEM_TABLE)}
            WHERE {q("user_id")}=%s
            ORDER BY {q("is_active")} DESC, {q("created_at")} ASC, {q("id")} ASC
            """,
            (user_id,),
        )
        return [_row_to_dict(cursor, row) for row in cursor.fetchall()]


def _doc_from_rows(meta: dict, rows: list[dict]):
    active_uid = _meta_active_uid(meta)
    active_candidates = []
    bag = []

    for row in rows:
        pet = _row_to_pet(row)
        row_active = _to_int(row.get("is_active", 0), 0) == 1
        if row_active or (active_uid and str(pet.get("uid", "")) == active_uid):
            active_candidates.append(pet)
        else:
            bag.append(pet)

    active = active_candidates[0] if active_candidates else None
    if len(active_candidates) > 1:
        bag = active_candidates[1:] + bag

    return {
        "active": active,
        "bag": bag,
        "egg_pity_count": _to_int(meta.get("egg_pity_count", 0), 0),
        "egg_pity_no_mythic_count": _to_int(meta.get("egg_pity_no_mythic_count", 0), 0),
        "travel": _normalize_pet_travel_state(meta.get("travel")),
    }


def _iter_unique_pet_rows(data: dict):
    seen = set()

    active = data.get("active")
    if active:
        active = _normalize_pet(dict(active))
        uid = str(active.get("uid", ""))
        if uid:
            seen.add(uid)
            yield active, True

    for pet in data.get("bag", []):
        if not isinstance(pet, dict):
            continue
        pet = _normalize_pet(dict(pet))
        uid = str(pet.get("uid", ""))
        if not uid or uid in seen:
            continue
        seen.add(uid)
        yield pet, False


def _save_pet_meta(user_id: str, data: dict, pet_count: int):
    active = data.get("active")
    active_uid = _clean_uid(active.get("uid", "")) if isinstance(active, dict) else ""
    travel = _normalize_pet_travel_state(data.get("travel"))
    player_data_manager.save_doc(
        user_id=user_id,
        table_name=TABLE,
        data={
            "active_uid": active_uid,
            "egg_pity_count": _to_int(data.get("egg_pity_count", 0), 0),
            "egg_pity_no_mythic_count": _to_int(data.get("egg_pity_no_mythic_count", 0), 0),
            "travel": travel,
            "active": active_uid,
            "bag": f"items:{pet_count}",
        },
        fields=PET_META_STORAGE_FIELDS,
        dirty_check=True,
    )


def _pet_to_item_values(user_id: str, pet: dict, is_active: bool, now: int):
    uid = str(pet.get("uid", ""))
    skill = pet.get("skill") or ((pet.get("skills") or [None])[0])
    skill_id = ""
    if isinstance(skill, dict):
        skill_id = str(skill.get("skill_id", "") or "")
    values = {
        "id": _pet_storage_id(user_id, uid),
        "user_id": user_id,
        "uid": uid,
        "is_active": 1 if is_active else 0,
        "pet_id": str(pet.get("pet_id", "")),
        "stars": _to_int(pet.get("stars", 1), 1),
        "exp": _to_int(pet.get("exp", 0), 0),
        "total_exp": _to_int(pet.get("total_exp", 0), 0),
        "skill_id": skill_id or None,
        "created_at": now,
        "updated_at": now,
    }
    return [values[field] for field in PET_ITEM_FIELDS]


def _clear_legacy_pet_item_columns(cursor, user_id: str):
    existing_fields = set(player_data_manager.conn.column_names(PET_ITEM_TABLE))
    legacy_fields = [field for field in PET_ITEM_LEGACY_FIELDS if field in existing_fields]
    if not legacy_fields:
        return

    q = player_data_manager._quote_ident
    set_sql = ", ".join(f"{q(field)}=NULL" for field in legacy_fields)
    cursor.execute(
        f"UPDATE {q(PET_ITEM_TABLE)} SET {set_sql} WHERE {q('user_id')}=%s",
        (user_id,),
    )


def _save_pet_items(user_id: str, data: dict):
    _ensure_pet_storage()
    q = player_data_manager._quote_ident
    now = int(time.time())
    pet_rows = list(_iter_unique_pet_rows(data))
    keep_ids = [_pet_storage_id(user_id, str(pet.get("uid", ""))) for pet, _ in pet_rows]

    with player_data_manager._conn_lock:
        cursor = player_data_manager._get_cursor()
        item_table_sql = q(PET_ITEM_TABLE)
        if keep_ids:
            placeholders = ", ".join(["%s"] * len(keep_ids))
            cursor.execute(
                f"""
                DELETE FROM {item_table_sql}
                WHERE {q("user_id")}=%s AND {q("id")} NOT IN ({placeholders})
                """,
                [user_id] + keep_ids,
            )
        else:
            cursor.execute(f"DELETE FROM {item_table_sql} WHERE {q('user_id')}=%s", (user_id,))

        col_sql = ", ".join(q(field) for field in PET_ITEM_FIELDS)
        placeholders = ", ".join(["%s"] * len(PET_ITEM_FIELDS))
        update_fields = [field for field in PET_ITEM_FIELDS if field not in {"id", "created_at"}]
        update_sql = ", ".join(f"{q(field)}=EXCLUDED.{q(field)}" for field in update_fields)
        insert_sql = (
            f"INSERT INTO {item_table_sql} ({col_sql}) VALUES ({placeholders}) "
            f"ON CONFLICT ({q('user_id')}, {q('uid')}) DO UPDATE SET {update_sql}"
        )

        for pet, is_active in pet_rows:
            cursor.execute(insert_sql, _pet_to_item_values(user_id, pet, is_active, now))

        _clear_legacy_pet_item_columns(cursor, user_id)
        player_data_manager._commit_write()
    return len(pet_rows)


def _save_pet_doc_to_storage(user_id: str, data: dict):
    data = _normalize_pet_doc(data)
    pet_count = _save_pet_items(user_id, data)
    _save_pet_meta(user_id, data, pet_count)


def get_pet_doc(user_id: str | int):
    user_id = str(user_id)
    _ensure_pet_storage()
    meta = _get_pet_meta(user_id)
    rows = _fetch_pet_rows(user_id)

    if _legacy_payload_exists(meta):
        if not rows:
            _save_pet_doc_to_storage(user_id, _legacy_doc_from_meta(meta))
            meta = _get_pet_meta(user_id)
            rows = _fetch_pet_rows(user_id)
        else:
            doc = _doc_from_rows(meta, rows)
            _save_pet_meta(user_id, doc, len(rows))
            meta = _get_pet_meta(user_id)

    return _normalize_pet_doc(_doc_from_rows(meta, rows))


def save_pet_doc(user_id: str | int, data: dict):
    _save_pet_doc_to_storage(str(user_id), data)


def migrate_all_legacy_pet_docs():
    _ensure_pet_storage()
    compacted = 0
    for record in player_data_manager.get_all_records(TABLE):
        user_id = record.get("user_id")
        if not user_id:
            continue
        if not _legacy_payload_exists(record) and not _pet_item_has_legacy_payload(str(user_id)):
            continue
        doc = get_pet_doc(str(user_id))
        save_pet_doc(str(user_id), doc)
        compacted += 1
    return compacted


def _pet_item_has_legacy_payload(user_id: str):
    q = player_data_manager._quote_ident
    with player_data_manager._conn_lock:
        if not player_data_manager.conn.table_exists(PET_ITEM_TABLE):
            return False
        existing_fields = set(player_data_manager.conn.column_names(PET_ITEM_TABLE))
        legacy_fields = [field for field in PET_ITEM_LEGACY_FIELDS if field in existing_fields]
        if not legacy_fields:
            return False

        cursor = player_data_manager._get_cursor()
        clauses = [f"({q(field)} IS NOT NULL AND CAST({q(field)} AS TEXT) <> '')" for field in legacy_fields]
        cursor.execute(
            f"""
            SELECT 1
            FROM {q(PET_ITEM_TABLE)}
            WHERE {q("user_id")}=%s AND ({" OR ".join(clauses)})
            LIMIT 1
            """,
            (user_id,),
        )
        return cursor.fetchone() is not None


def migrate_pet_storage_once():
    global _PET_STORAGE_MIGRATION_ATTEMPTED
    if _PET_STORAGE_MIGRATION_ATTEMPTED:
        return 0
    try:
        migrated = migrate_all_legacy_pet_docs()
    except Exception:
        _PET_STORAGE_MIGRATION_ATTEMPTED = False
        raise
    _PET_STORAGE_MIGRATION_ATTEMPTED = True
    return migrated


def _sqlite_quote_ident(name: str):
    return '"' + str(name).replace('"', '""') + '"'


def _sqlite_column_names(conn, table_name: str):
    cursor = conn.execute(f"PRAGMA table_info({_sqlite_quote_ident(table_name)})")
    return [str(row[1]) for row in cursor.fetchall()]


def _sqlite_table_exists(conn, table_name: str):
    cursor = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _ensure_sqlite_pet_storage(conn):
    q = _sqlite_quote_ident
    if not _sqlite_table_exists(conn, TABLE):
        return False

    meta_columns = set(_sqlite_column_names(conn, TABLE))
    for field in PET_META_STORAGE_FIELDS:
        if field not in meta_columns:
            conn.execute(f"ALTER TABLE {q(TABLE)} ADD COLUMN {q(field)} TEXT DEFAULT NULL")

    if not _sqlite_table_exists(conn, PET_ITEM_TABLE):
        conn.execute(
            f"""
            CREATE TABLE {q(PET_ITEM_TABLE)} (
                {q("id")} TEXT PRIMARY KEY,
                {q("user_id")} TEXT NOT NULL,
                {q("uid")} TEXT NOT NULL,
                {q("is_active")} INTEGER DEFAULT 0,
                {q("pet_id")} TEXT DEFAULT NULL,
                {q("stars")} INTEGER DEFAULT 1,
                {q("exp")} INTEGER DEFAULT 0,
                {q("total_exp")} INTEGER DEFAULT 0,
                {q("skill_id")} TEXT DEFAULT NULL,
                {q("created_at")} INTEGER DEFAULT 0,
                {q("updated_at")} INTEGER DEFAULT 0
            )
            """
        )
    else:
        item_columns = set(_sqlite_column_names(conn, PET_ITEM_TABLE))
        column_defs = {
            "id": "TEXT",
            "user_id": "TEXT",
            "uid": "TEXT",
            "is_active": "INTEGER DEFAULT 0",
            "pet_id": "TEXT DEFAULT NULL",
            "stars": "INTEGER DEFAULT 1",
            "exp": "INTEGER DEFAULT 0",
            "total_exp": "INTEGER DEFAULT 0",
            "skill_id": "TEXT DEFAULT NULL",
            "created_at": "INTEGER DEFAULT 0",
            "updated_at": "INTEGER DEFAULT 0",
        }
        for field, data_type in column_defs.items():
            if field not in item_columns:
                conn.execute(f"ALTER TABLE {q(PET_ITEM_TABLE)} ADD COLUMN {q(field)} {data_type}")

    conn.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {q('idx_player_pet_item_user_uid')} "
        f"ON {q(PET_ITEM_TABLE)} ({q('user_id')}, {q('uid')})"
    )
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {q('idx_player_pet_item_user_active')} "
        f"ON {q(PET_ITEM_TABLE)} ({q('user_id')}, {q('is_active')})"
    )
    return True


def _sqlite_row_to_legacy_doc(row: dict):
    active = _json_load(row.get("active"), default=None)
    bag = _json_load(row.get("bag"), default=[])
    return {
        "active": active if isinstance(active, dict) else None,
        "bag": bag if isinstance(bag, list) else [],
        "egg_pity_count": _to_int(row.get("egg_pity_count", 0), 0),
        "egg_pity_no_mythic_count": _to_int(row.get("egg_pity_no_mythic_count", 0), 0),
        "travel": _normalize_pet_travel_state(_json_load(row.get("travel"), default=None)),
    }


def _sqlite_legacy_payload_exists(row: dict):
    active = _json_load(row.get("active"), default=None)
    bag = _json_load(row.get("bag"), default=None)
    return isinstance(active, dict) or isinstance(bag, list)


def _save_sqlite_pet_doc(conn, user_id: str, data: dict):
    q = _sqlite_quote_ident
    data = _normalize_pet_doc(data)
    pet_rows = list(_iter_unique_pet_rows(data))
    keep_ids = [_pet_storage_id(user_id, str(pet.get("uid", ""))) for pet, _ in pet_rows]
    now = int(time.time())

    if keep_ids:
        placeholders = ", ".join(["?"] * len(keep_ids))
        conn.execute(
            f"""
            DELETE FROM {q(PET_ITEM_TABLE)}
            WHERE {q("user_id")}=? AND {q("id")} NOT IN ({placeholders})
            """,
            [user_id] + keep_ids,
        )
    else:
        conn.execute(f"DELETE FROM {q(PET_ITEM_TABLE)} WHERE {q('user_id')}=?", (user_id,))

    col_sql = ", ".join(q(field) for field in PET_ITEM_FIELDS)
    placeholders = ", ".join(["?"] * len(PET_ITEM_FIELDS))
    insert_sql = f"INSERT OR REPLACE INTO {q(PET_ITEM_TABLE)} ({col_sql}) VALUES ({placeholders})"
    for pet, is_active in pet_rows:
        conn.execute(insert_sql, _pet_to_item_values(user_id, pet, is_active, now))

    item_columns = set(_sqlite_column_names(conn, PET_ITEM_TABLE))
    legacy_fields = [field for field in PET_ITEM_LEGACY_FIELDS if field in item_columns]
    if legacy_fields:
        set_sql = ", ".join(f"{q(field)}=NULL" for field in legacy_fields)
        conn.execute(
            f"UPDATE {q(PET_ITEM_TABLE)} SET {set_sql} WHERE {q('user_id')}=?",
            (user_id,),
        )

    active = data.get("active")
    active_uid = _clean_uid(active.get("uid", "")) if isinstance(active, dict) else ""
    travel = _normalize_pet_travel_state(data.get("travel"))
    conn.execute(
        f"""
        UPDATE {q(TABLE)}
        SET {q("active_uid")}=?,
            {q("egg_pity_count")}=?,
            {q("egg_pity_no_mythic_count")}=?,
            {q("travel")}=?,
            {q("active")}=?,
            {q("bag")}=?
        WHERE {q("user_id")}=?
        """,
        (
            active_uid,
            str(_to_int(data.get("egg_pity_count", 0), 0)),
            str(_to_int(data.get("egg_pity_no_mythic_count", 0), 0)),
            _json_dump(travel) if travel else None,
            active_uid,
            f"items:{len(pet_rows)}",
            user_id,
        ),
    )


def _fetch_sqlite_pet_rows(conn, user_id: str):
    q = _sqlite_quote_ident
    if not _sqlite_table_exists(conn, PET_ITEM_TABLE):
        return []

    existing_fields = set(_sqlite_column_names(conn, PET_ITEM_TABLE))
    select_fields = PET_ITEM_FIELDS + [
        field
        for field in PET_ITEM_LEGACY_FIELDS
        if field in existing_fields and field not in PET_ITEM_FIELDS
    ]
    cursor = conn.execute(
        f"""
        SELECT {', '.join(q(field) for field in select_fields)}
        FROM {q(PET_ITEM_TABLE)}
        WHERE {q("user_id")}=?
        ORDER BY {q("is_active")} DESC, {q("created_at")} ASC, {q("id")} ASC
        """,
        (user_id,),
    )
    return [dict(row) for row in cursor.fetchall()]


def _sqlite_item_users_with_legacy_payload(conn):
    q = _sqlite_quote_ident
    if not _sqlite_table_exists(conn, PET_ITEM_TABLE):
        return set()

    item_columns = set(_sqlite_column_names(conn, PET_ITEM_TABLE))
    legacy_fields = [field for field in PET_ITEM_LEGACY_FIELDS if field in item_columns]
    if not legacy_fields:
        return set()

    clauses = [f"({q(field)} IS NOT NULL AND CAST({q(field)} AS TEXT) <> '')" for field in legacy_fields]
    cursor = conn.execute(
        f"""
        SELECT DISTINCT {q("user_id")}
        FROM {q(PET_ITEM_TABLE)}
        WHERE {" OR ".join(clauses)}
        """
    )
    return {str(row[0]) for row in cursor.fetchall() if row[0]}


def _sqlite_pet_user_ids(conn):
    q = _sqlite_quote_ident
    user_ids = set()

    if _sqlite_table_exists(conn, TABLE):
        cursor = conn.execute(
            f"""
            SELECT {q("user_id")}
            FROM {q(TABLE)}
            WHERE {q("user_id")} IS NOT NULL AND CAST({q("user_id")} AS TEXT) <> ''
            """
        )
        user_ids.update(str(row[0]) for row in cursor.fetchall() if row[0])

    if _sqlite_table_exists(conn, PET_ITEM_TABLE):
        item_columns = set(_sqlite_column_names(conn, PET_ITEM_TABLE))
        if "user_id" in item_columns:
            cursor = conn.execute(
                f"""
                SELECT DISTINCT {q("user_id")}
                FROM {q(PET_ITEM_TABLE)}
                WHERE {q("user_id")} IS NOT NULL AND CAST({q("user_id")} AS TEXT) <> ''
                """
            )
            user_ids.update(str(row[0]) for row in cursor.fetchall() if row[0])

    return user_ids


def _sqlite_get_pet_meta(conn, user_id: str):
    q = _sqlite_quote_ident
    if not _sqlite_table_exists(conn, TABLE):
        return None

    cursor = conn.execute(
        f"""
        SELECT {', '.join(q(field) for field in ["user_id"] + PET_META_STORAGE_FIELDS)}
        FROM {q(TABLE)}
        WHERE {q("user_id")}=?
        LIMIT 1
        """,
        (user_id,),
    )
    row = cursor.fetchone()
    return dict(row) if row else None


def _sqlite_pet_doc_for_user(conn, user_id: str):
    meta = _sqlite_get_pet_meta(conn, user_id) or {
        "user_id": user_id,
        **_default_pet_doc(),
    }
    if _sqlite_legacy_payload_exists(meta):
        return _sqlite_row_to_legacy_doc(meta)

    rows = _fetch_sqlite_pet_rows(conn, user_id)
    return _doc_from_rows(meta, rows)


def _active_pet_user_has_items(user_id: str):
    _ensure_pet_storage()
    q = player_data_manager._quote_ident
    with player_data_manager._conn_lock:
        if not player_data_manager.conn.table_exists(PET_ITEM_TABLE):
            return False
        cursor = player_data_manager._get_cursor()
        cursor.execute(
            f"""
            SELECT 1
            FROM {q(PET_ITEM_TABLE)}
            WHERE {q("user_id")}=%s
            LIMIT 1
            """,
            (user_id,),
        )
        return cursor.fetchone() is not None


def _active_pet_meta_exists(user_id: str):
    _ensure_pet_storage()
    q = player_data_manager._quote_ident
    with player_data_manager._conn_lock:
        cursor = player_data_manager._get_cursor()
        cursor.execute(
            f"""
            SELECT 1
            FROM {q(TABLE)}
            WHERE {q("user_id")}=%s
            LIMIT 1
            """,
            (user_id,),
        )
        return cursor.fetchone() is not None


def _pet_doc_has_pet(data: dict):
    data = _normalize_pet_doc(data)
    return bool(data.get("active") or data.get("bag"))


def merge_missing_sqlite_pet_users(sqlite_path: str | Path | None = None):
    sqlite_path = Path(sqlite_path) if sqlite_path else DATABASE / "player.db"
    if not sqlite_path.exists():
        return 0

    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        has_pet_storage = _ensure_sqlite_pet_storage(conn)
        if has_pet_storage:
            conn.commit()
        elif not _sqlite_table_exists(conn, PET_ITEM_TABLE):
            return 0

        merged = 0
        for user_id in sorted(_sqlite_pet_user_ids(conn)):
            if _active_pet_user_has_items(user_id):
                continue

            doc = _sqlite_pet_doc_for_user(conn, user_id)
            if not _pet_doc_has_pet(doc) and _active_pet_meta_exists(user_id):
                continue

            save_pet_doc(user_id, doc)
            merged += 1

        return merged
    finally:
        conn.close()


def migrate_local_sqlite_pet_storage(sqlite_path: str | Path | None = None):
    sqlite_path = Path(sqlite_path) if sqlite_path else DATABASE / "player.db"
    if not sqlite_path.exists():
        return 0

    import sqlite3

    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    try:
        if not _ensure_sqlite_pet_storage(conn):
            conn.commit()
            return 0

        cursor = conn.execute(
            f"""
            SELECT {', '.join(_sqlite_quote_ident(field) for field in ["user_id"] + PET_META_STORAGE_FIELDS)}
            FROM {_sqlite_quote_ident(TABLE)}
            """
        )
        migrated = 0
        migrated_users = set()
        for row in cursor.fetchall():
            record = dict(row)
            user_id = record.get("user_id")
            if not user_id or not _sqlite_legacy_payload_exists(record):
                continue
            _save_sqlite_pet_doc(conn, str(user_id), _sqlite_row_to_legacy_doc(record))
            migrated += 1
            migrated_users.add(str(user_id))

        for user_id in _sqlite_item_users_with_legacy_payload(conn) - migrated_users:
            meta = _sqlite_get_pet_meta(conn, user_id)
            if not meta:
                continue
            rows = _fetch_sqlite_pet_rows(conn, user_id)
            _save_sqlite_pet_doc(conn, user_id, _doc_from_rows(meta, rows))
            migrated += 1

        conn.commit()
        return migrated
    finally:
        conn.close()


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


def get_pet_total_count(data: dict) -> int:
    data = _normalize_pet_doc(data)
    seen = set()
    total = 0

    active = data.get("active")
    if active:
        uid = str(active.get("uid", ""))
        if uid:
            seen.add(uid)
        total += 1

    for pet in data.get("bag", []):
        if not isinstance(pet, dict):
            continue
        uid = str(pet.get("uid", ""))
        if uid and uid in seen:
            continue
        if uid:
            seen.add(uid)
        total += 1

    return total


def get_pet_count(user_id: str | int) -> int:
    return get_pet_total_count(get_pet_doc(user_id))


def get_pet_remaining_capacity(user_id: str | int) -> int:
    return max(0, PET_BAG_LIMIT - get_pet_count(user_id))


def can_add_pets(user_id: str | int, count: int = 1) -> tuple[bool, int, int]:
    owned = get_pet_count(user_id)
    remaining = max(0, PET_BAG_LIMIT - owned)
    return max(0, int(count)) <= remaining, owned, remaining


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


def load_herb_ids_by_level():
    global _HERB_IDS_BY_LEVEL_CACHE
    if _HERB_IDS_BY_LEVEL_CACHE is not None:
        return _HERB_IDS_BY_LEVEL_CACHE

    result = {}
    try:
        with open(HERB_CONFIG_PATH, "r", encoding="utf-8") as f:
            raw = json.loads(f.read() or "{}")
    except Exception as e:
        logger.warning(f"药材池加载失败: {e}")
        raw = {}

    if isinstance(raw, dict):
        for item_id, item in raw.items():
            if not isinstance(item, dict):
                continue
            if item.get("type") != "药材" and item.get("item_type") != "药材":
                continue
            level = str(item.get("level", "") or "")
            if not level:
                continue
            try:
                item_id_int = int(item_id)
            except Exception:
                continue
            result.setdefault(level, []).append(item_id_int)

    _HERB_IDS_BY_LEVEL_CACHE = result
    return _HERB_IDS_BY_LEVEL_CACHE


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
    if get_pet_total_count(data) >= PET_BAG_LIMIT:
        raise RuntimeError(f"宠物持有数量已达上限{PET_BAG_LIMIT}，请先放生或整理宠物。")

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
        if get_pet_total_count(data) >= PET_BAG_LIMIT:
            break
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


def get_pet_travel_scene_key(text: str | None = None):
    text = str(text or "").strip()
    if not text:
        return "forage"
    if text in PET_TRAVEL_ITEM_POOLS:
        return text
    return PET_TRAVEL_SCENE_ALIASES.get(text)


def get_pet_travel_scenes():
    return {
        key: {
            "name": value["name"],
            "desc": value["desc"],
        }
        for key, value in PET_TRAVEL_ITEM_POOLS.items()
    }


def _travel_pet_power(pet: dict):
    rarity_idx = RARITY_INDEX.get(str(pet.get("rarity", "常见")), 0)
    stars = max(1, min(25, _to_int(pet.get("stars", 1), 1)))
    form_index = get_form_index(stars)
    return 1.0 + rarity_idx * 0.16 + max(0, stars - 1) * 0.035 + form_index * 0.08


def _travel_remaining_seconds(travel: dict, now: int | None = None):
    if now is None:
        now = int(time.time())
    return max(0, int(travel.get("end_at", 0)) - now)


def format_pet_travel_time(seconds: int):
    seconds = max(0, int(seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours > 0 and minutes > 0:
        return f"{hours}小时{minutes}分钟"
    if hours > 0:
        return f"{hours}小时"
    if minutes > 0:
        return f"{minutes}分钟"
    return "不足1分钟"


def start_pet_travel(
    user_id: str | int,
    scene: str | None = None,
    duration_hours: int | str = 4,
    pet_uid: str | None = None,
):
    data = get_pet_doc(user_id)
    if data.get("travel"):
        return False, "已有宠物正在游历，请先等待返回并领取游历收获。", data.get("travel"), None

    if pet_uid:
        _, _, pet = find_pet_anywhere(data, pet_uid)
    else:
        pet = data.get("active")
    if not pet:
        return False, "当前没有可派遣的宠物，请先获得宠物并设置出战。", None, None

    scene_key = get_pet_travel_scene_key(scene)
    if not scene_key:
        return False, "未知游历地点，可选：灵草谷、妖兽岭、秘境边缘。", None, None

    try:
        duration_hours = int(duration_hours)
    except Exception:
        duration_hours = 4
    duration_hours = max(PET_TRAVEL_MIN_HOURS, min(PET_TRAVEL_MAX_HOURS, duration_hours))
    now = int(time.time())
    travel = {
        "pet_uid": str(pet.get("uid", "")),
        "pet_name": str(pet.get("form_name", pet.get("name", "未知宠物"))),
        "pet_rarity": str(pet.get("rarity", "常见")),
        "pet_stars": _to_int(pet.get("stars", 1), 1),
        "scene": scene_key,
        "scene_name": PET_TRAVEL_ITEM_POOLS[scene_key]["name"],
        "start_at": now,
        "end_at": now + duration_hours * 3600,
        "duration_hours": duration_hours,
    }
    data["travel"] = travel
    save_pet_doc(user_id, data)
    return True, "派遣成功。", _normalize_pet_travel_state(travel), pet


def get_pet_travel_status(user_id: str | int):
    data = get_pet_doc(user_id)
    return data.get("travel")


def is_pet_travel_done(travel: dict | None):
    travel = _normalize_pet_travel_state(travel)
    if not travel:
        return False
    return _travel_remaining_seconds(travel) <= 0


def _resolve_travel_reward_item_id(selected: dict):
    if "id" in selected:
        try:
            return int(selected["id"])
        except Exception:
            return None

    ids = selected.get("ids")
    if isinstance(ids, list) and ids:
        try:
            return int(random.choice(ids))
        except Exception:
            return None

    herb_levels = selected.get("herb_levels")
    if isinstance(herb_levels, str):
        herb_levels = [herb_levels]
    if isinstance(herb_levels, list) and herb_levels:
        herb_pool = load_herb_ids_by_level()
        candidates = []
        for level in herb_levels:
            candidates.extend(herb_pool.get(str(level), []))
        if candidates:
            return int(random.choice(candidates))

    return None


def _roll_travel_drop_group(drop_config: dict | None, duration_hours: int, multiplier: float = 1.0):
    if not isinstance(drop_config, dict):
        return []

    pool = drop_config.get("items", [])
    if not isinstance(pool, list) or not pool:
        return []

    try:
        chance_per_4_hours = float(drop_config.get("chance_per_4_hours", 0) or 0)
    except Exception:
        chance_per_4_hours = 0
    try:
        chance_cap = float(drop_config.get("cap", chance_per_4_hours) or chance_per_4_hours)
    except Exception:
        chance_cap = chance_per_4_hours

    chance = min(chance_cap, chance_per_4_hours * max(1, int(duration_hours)) / 4)
    if chance <= 0 or random.random() > chance:
        return []

    selected = random.choices(pool, weights=[max(1, int(item.get("weight", 1) or 1)) for item in pool], k=1)[0]
    item_id = _resolve_travel_reward_item_id(selected)
    if item_id is None:
        return []

    amount_min = int(selected.get("min", 1) or 1)
    amount_max = int(selected.get("max", amount_min) or amount_min)
    amount = random.randint(amount_min, max(amount_min, amount_max))
    amount_multiplier = min(multiplier, float(selected.get("amount_multiplier_cap", multiplier)))
    amount = max(1, int(amount * amount_multiplier))
    return [{"id": item_id, "amount": amount}]


def _roll_travel_item_reward(scene_key: str, multiplier: float, duration_hours: int):
    scene_pool = PET_TRAVEL_ITEM_POOLS.get(scene_key, PET_TRAVEL_ITEM_POOLS["forage"])
    pool = scene_pool["items"]
    reward_count = 1
    if duration_hours >= 4:
        reward_count += 1
    if duration_hours >= 8:
        reward_count += 1
    if random.random() < min(0.45, max(0.0, (multiplier - 1.0) * 0.16)):
        reward_count += 1

    rewards_by_id = {}
    for _ in range(reward_count):
        selected = random.choices(pool, weights=[max(1, item.get("weight", 1)) for item in pool], k=1)[0]
        amount_min = int(selected.get("min", 1))
        amount_max = int(selected.get("max", amount_min))
        amount = random.randint(amount_min, max(amount_min, amount_max))
        amount_multiplier = min(multiplier, float(selected.get("amount_multiplier_cap", multiplier)))
        amount = max(1, int(amount * amount_multiplier))
        item_id = _resolve_travel_reward_item_id(selected)
        if item_id is None:
            continue
        rewards_by_id[item_id] = rewards_by_id.get(item_id, 0) + amount

    extra_rewards = []
    extra_rewards.extend(_roll_travel_drop_group(scene_pool.get("pet_egg_drop"), duration_hours))
    extra_rewards.extend(_roll_travel_drop_group(scene_pool.get("marrow_drop"), duration_hours))
    for reward in extra_rewards:
        item_id = int(reward["id"])
        rewards_by_id[item_id] = rewards_by_id.get(item_id, 0) + int(reward["amount"])

    return [
        {"id": item_id, "amount": amount}
        for item_id, amount in rewards_by_id.items()
        if amount > 0
    ]


def complete_pet_travel(user_id: str | int):
    data = get_pet_doc(user_id)
    travel = data.get("travel")
    if not travel:
        return False, "当前没有正在游历的宠物。", None

    remaining = _travel_remaining_seconds(travel)
    if remaining > 0:
        return False, f"宠物尚未返回，剩余{format_pet_travel_time(remaining)}。", travel

    _, _, pet = find_pet_anywhere(data, travel["pet_uid"])
    if not pet:
        data["travel"] = None
        save_pet_doc(user_id, data)
        return False, "游历宠物已不存在，本次游历状态已清理。", None

    multiplier = _travel_pet_power(pet)
    duration_hours = int(travel.get("duration_hours", 1))
    scene_key = travel.get("scene", "forage")
    scene_bonus = {
        "forage": 1.0,
        "training": 1.08,
        "rift": 1.18,
    }.get(scene_key, 1.0)
    reward_rate = multiplier * scene_bonus

    stone = 0
    exp = 0
    item_rewards = _roll_travel_item_reward(scene_key, reward_rate, duration_hours)
    result = {
        "travel": travel,
        "pet": pet,
        "stone": stone,
        "exp": exp,
        "items": item_rewards,
        "story": random.choice(PET_TRAVEL_STORY_TEXTS),
        "reward_rate": reward_rate,
    }

    data["travel"] = None
    save_pet_doc(user_id, data)
    return True, "游历完成。", result


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
