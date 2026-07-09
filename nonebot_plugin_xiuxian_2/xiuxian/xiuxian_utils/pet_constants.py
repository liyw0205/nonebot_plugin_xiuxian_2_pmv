from nonebot_plugin_xiuxian_2.paths import get_paths

DATABASE = get_paths().data
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
PET_TRAVEL_HERB_AMOUNT_MULTIPLIER_CAP = 2.5
PET_TRAVEL_HERB_AMOUNT_PER_ROLL_CAP = 5
PET_TRAVEL_ITEM_POOLS = {
    "forage": {
        "name": "灵草谷",
        "desc": "采集低阶药材和灵髓，适合稳定补充宠物养成材料。",
        "items": [
            {"herb_levels": ["一品药材"], "min": 1, "max": 5, "weight": 40},
            {"herb_levels": ["二品药材"], "min": 1, "max": 4, "weight": 32},
            {"herb_levels": ["三品药材"], "min": 1, "max": 3, "weight": 18},
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
            {"herb_levels": ["三品药材"], "min": 1, "max": 4, "weight": 28},
            {"herb_levels": ["四品药材"], "min": 1, "max": 3, "weight": 32},
            {"herb_levels": ["五品药材"], "min": 1, "max": 2, "weight": 24},
            {"herb_levels": ["六品药材"], "min": 1, "max": 2, "weight": 12},
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
            {"herb_levels": ["七品药材"], "min": 1, "max": 2, "weight": 28},
            {"herb_levels": ["八品药材"], "min": 1, "max": 2, "weight": 30},
            {"herb_levels": ["九品药材"], "min": 1, "max": 2, "weight": 24},
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
