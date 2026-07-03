import random
import time
from urllib.parse import quote

from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.xiuxian2_handle import PlayerDataManager

items = Items()
player_data_manager = PlayerDataManager()

def _paginate_sections(sections, page: int, per_page: int = 15):
    total_items = sum(len(rows) for _, rows in sections)
    total_pages = max(1, (total_items + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))

    start = (page - 1) * per_page
    end = start + per_page

    out = []
    cursor = 0
    for title, rows in sections:
        next_cursor = cursor + len(rows)
        if next_cursor <= start:
            cursor = next_cursor
            continue
        if cursor >= end:
            break

        local_start = max(0, start - cursor)
        local_end = min(len(rows), end - cursor)
        part = rows[local_start:local_end]
        if part:
            out.append((title, part))
        cursor = next_cursor

    return out, page, total_pages


TABLE = "player_accessory"
ACCESSORY_BAG_LIMIT = 1000

AFFIX_TYPES = ["气血", "抗暴", "防御", "会心", "会心伤害", "攻击", "速度"]

# 品阶1-5洗练区间
WASH_RANGE = {
    1: {"气血": (0.02, 0.05), "抗暴": (0.01, 0.03), "防御": (0.01, 0.03), "会心": (0.01, 0.03), "会心伤害": (0.02, 0.05), "攻击": (0.02, 0.05), "速度": (4, 9)},
    2: {"气血": (0.04, 0.08), "抗暴": (0.02, 0.05), "防御": (0.02, 0.05), "会心": (0.02, 0.05), "会心伤害": (0.04, 0.08), "攻击": (0.04, 0.08), "速度": (8, 16)},
    3: {"气血": (0.06, 0.12), "抗暴": (0.03, 0.07), "防御": (0.03, 0.07), "会心": (0.03, 0.07), "会心伤害": (0.06, 0.12), "攻击": (0.06, 0.12), "速度": (14, 26)},
    4: {"气血": (0.08, 0.16), "抗暴": (0.04, 0.10), "防御": (0.04, 0.10), "会心": (0.04, 0.10), "会心伤害": (0.08, 0.16), "攻击": (0.08, 0.16), "速度": (22, 40)},
    5: {"气血": (0.10, 0.20), "抗暴": (0.05, 0.12), "防御": (0.05, 0.12), "会心": (0.05, 0.12), "会心伤害": (0.10, 0.20), "攻击": (0.10, 0.20), "速度": (34, 60)},
}

SLOTS = ["手镯", "戒指", "手环", "项链"]

# 你的饰品词条中文 -> 统一属性键
AFFIX_KEY_MAP = {
    "气血": "hp_pct",              # 最大生命百分比
    "抗暴": "crit_resist",         # 抗暴
    "防御": "dmg_reduction",       # 伤害减免
    "会心": "crit_rate",           # 会心率
    "会心伤害": "crit_damage",     # 会心伤害
    "攻击": "atk_pct",             # 攻击百分比
    "速度": "speed",               # 固定速度
}

# 套装效果（2件 / 4件）
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

def quality_to_cn(q: int) -> str:
    return {
        1: "一阶",
        2: "二阶",
        3: "三阶",
        4: "四阶",
        5: "五阶",
    }.get(int(q), f"{q}阶")

SET_TYPE_CN = {
    "attack": "攻击提升",
    "true_damage": "附加真伤",
    "shield": "开场护盾",
    "reflect": "反伤",
    "armor_pen": "护甲穿透",
    "dmg_reduction": "伤害减免",
    "crit_rate": "会心率",
    "dodge": "闪避",
    "shield_break": "护盾穿透",
    "speed_pct": "速度提升",
}

SET_VALUE_POINT_TYPES = {"dodge"}

ACCESSORY_SETS = ["烈阳", "玄渊", "天衡", "星痕", "龙魄", "踏风"]
ACCESSORY_PARTS = ["手镯", "戒指", "手环", "项链"]
QUALITY_RANGE = [1, 2, 3, 4, 5]

WASH_STONE_ID = 20023
WASH_STONE_NAME = "洗练石"
LOCKED_AFFIX_KEY = "locked_affixes"

WASH_STONE_COST = {
    1: 1,
    2: 2,
    3: 4,
    4: 8,
    5: 12
}

ACCESSORY_DECOMPOSE_GAIN = {
    1: 1,
    2: 3,
    3: 8,
    4: 20,
    5: 50
}


def _default_accessory_doc():
    return {
        "equipped": {"手镯": None, "戒指": None, "手环": None, "项链": None},
        "bag": []
    }

def _normalize_accessory_doc(doc: dict):
    if not isinstance(doc, dict):
        doc = _default_accessory_doc()

    eq = doc.get("equipped")
    if not isinstance(eq, dict):
        eq = {"手镯": None, "戒指": None, "手环": None, "项链": None}
    for s in SLOTS:
        if s not in eq:
            eq[s] = None

    bag = doc.get("bag")
    if not isinstance(bag, list):
        bag = []

    doc["equipped"] = eq
    doc["bag"] = bag
    return doc

def _get_data(user_id: str):
    doc = player_data_manager.get_doc(
        user_id=user_id,
        table_name=TABLE,
        fields=["equipped", "bag"],
        default_factory=_default_accessory_doc
    )
    return _normalize_accessory_doc(doc)

def _save_data(user_id: str, data: dict):
    data = _normalize_accessory_doc(data)
    player_data_manager.save_doc(
        user_id=user_id,
        table_name=TABLE,
        data=data,
        fields=["equipped", "bag"],
        dirty_check=True
    )

def get_accessory_total_count(data: dict) -> int:
    data = _normalize_accessory_doc(data)
    total = len(data.get("bag", []) or [])
    total += sum(1 for item in (data.get("equipped", {}) or {}).values() if item)
    return total

def get_accessory_count(user_id: str) -> int:
    return get_accessory_total_count(_get_data(str(user_id)))

def get_accessory_remaining_capacity(user_id: str) -> int:
    return max(0, ACCESSORY_BAG_LIMIT - get_accessory_count(str(user_id)))

def can_add_accessories(user_id: str, count: int = 1) -> tuple[bool, int, int]:
    owned = get_accessory_count(str(user_id))
    remaining = max(0, ACCESSORY_BAG_LIMIT - owned)
    return int(count) <= remaining, owned, remaining

def _target_affix_count_for_quality(quality: int) -> int:
    quality = max(1, min(5, int(quality)))
    return 3 if quality >= 4 else 2

def roll_affixes(quality: int, count: int = 2):
    quality = max(1, min(5, int(quality)))
    count = max(1, min(4, count))
    pool = random.sample(AFFIX_TYPES, count)
    out = []
    for t in pool:
        lo, hi = WASH_RANGE[quality][t]
        value = round(random.uniform(lo, hi)) if t == "速度" else round(random.uniform(lo, hi), 4)
        out.append({"type": t, "value": value})
    return out

def roll_affixes_with_pity(quality: int, count: int = 2, pity_reached: bool = False, exclude_types=None):
    quality = max(1, min(5, int(quality)))
    count = max(0, min(4, count))
    if count <= 0:
        return []
    excluded = set(exclude_types or [])
    candidates = [t for t in AFFIX_TYPES if t not in excluded]
    if len(candidates) < count:
        candidates = AFFIX_TYPES[:]
    pool = random.sample(candidates, count)
    out = []
    for t in pool:
        lo, hi = WASH_RANGE[quality][t]
        if t == "速度":
            v = hi if pity_reached else round(random.uniform(lo, hi))
        else:
            v = hi if pity_reached else round(random.uniform(lo, hi), 4)
        out.append({"type": t, "value": v})
    return out

def _format_affix_value(affix: dict) -> str:
    t = affix.get("type", "未知")
    v = float(affix.get("value", 0))
    if t == "速度":
        return f"+{round(v)}点"
    return f"+{round(v * 100, 2)}%"

def _normalize_locked_affixes(acc: dict, affix_count: int | None = None) -> list[int]:
    if affix_count is None:
        affixes = acc.get("affixes", []) if isinstance(acc, dict) else []
        affix_count = len(affixes) if isinstance(affixes, list) else 0
    raw = acc.get(LOCKED_AFFIX_KEY, []) if isinstance(acc, dict) else []
    if not isinstance(raw, list):
        raw = []

    locked = []
    for idx in raw:
        try:
            idx = int(idx)
        except Exception:
            continue
        if 0 <= idx < affix_count and idx not in locked:
            locked.append(idx)
    return sorted(locked)

def _set_locked_affixes(acc: dict, locked_indexes: list[int]):
    locked = sorted({int(i) for i in locked_indexes})
    if locked:
        acc[LOCKED_AFFIX_KEY] = locked
    else:
        acc.pop(LOCKED_AFFIX_KEY, None)

def _split_affix_index_tokens(tokens: list[str]) -> list[str]:
    out = []
    for token in tokens:
        for part in str(token).replace("，", ",").replace("、", ",").split(","):
            part = part.strip().lstrip("#")
            if part:
                out.append(part)
    return out

def _parse_affix_indexes(tokens: list[str], affix_count: int):
    parts = _split_affix_index_tokens(tokens)
    if not parts:
        return None, "请指定词条序号，例如：1 或 1 2"

    indexes = []
    for part in parts:
        try:
            idx = int(part)
        except Exception:
            return None, f"词条序号错误：{part}"
        if idx < 1 or idx > affix_count:
            return None, f"词条序号必须在1到{affix_count}之间"
        zero_idx = idx - 1
        if zero_idx not in indexes:
            indexes.append(zero_idx)
    return sorted(indexes), ""

def _format_locked_positions(locked_indexes: list[int]) -> str:
    if not locked_indexes:
        return "无"
    return "、".join(str(i + 1) for i in sorted(locked_indexes))

def _wash_stone_need(quality: int, locked_count: int = 0) -> int:
    base = WASH_STONE_COST.get(max(1, min(5, int(quality))), 1)
    return base * (1 + max(0, int(locked_count)))

def _fit_affixes_to_quality(quality: int, affixes: list[dict], pity_reached: bool = False):
    target_count = _target_affix_count_for_quality(quality)
    current = list(affixes or [])[:target_count] if isinstance(affixes, list) else []
    if len(current) >= target_count:
        return current

    existing_types = {
        str(af.get("type", ""))
        for af in current
        if isinstance(af, dict)
    }
    current.extend(
        roll_affixes_with_pity(
            quality,
            target_count - len(current),
            pity_reached=pity_reached,
            exclude_types=existing_types
        )
    )
    return current

def _reroll_affixes_preserving_locked(quality: int, old_affixes: list[dict], locked_indexes: list[int], pity_reached: bool = False):
    target_count = _target_affix_count_for_quality(quality)
    old_affixes = list(old_affixes or [])[:target_count] if isinstance(old_affixes, list) else []
    locked_set = {i for i in locked_indexes if 0 <= i < len(old_affixes) and i < target_count}
    locked_types = {
        str(old_affixes[i].get("type", ""))
        for i in locked_set
        if i < len(old_affixes) and isinstance(old_affixes[i], dict)
    }
    new_affixes = roll_affixes_with_pity(
        quality,
        target_count - len(locked_set),
        pity_reached=pity_reached,
        exclude_types=locked_types
    )
    new_iter = iter(new_affixes)

    result = []
    for idx in range(target_count):
        if idx in locked_set and idx < len(old_affixes):
            result.append(old_affixes[idx])
        else:
            result.append(next(new_iter))
    return result

def create_accessory_instance(item_id: int, quality: int = 1):
    item = items.get_data_by_item_id(item_id)
    quality = max(1, min(5, int(quality)))
    uid = f"acc_{int(time.time())}_{random.randint(1,9999)}"
    return {
        "uid": uid,
        "item_id": item_id,
        "name": item["name"],
        "part": item["part"],
        "set_type": item["set_type"],
        "quality": quality,
        "affixes": roll_affixes(quality, _target_affix_count_for_quality(quality)),
        LOCKED_AFFIX_KEY: [],
        "wash_count": 0
    }

def add_accessory_to_bag(user_id: str, item_id: int, quality: int = 1):
    data = _get_data(user_id)
    if get_accessory_total_count(data) >= ACCESSORY_BAG_LIMIT:
        raise RuntimeError(f"饰品持有数量已达上限{ACCESSORY_BAG_LIMIT}，请先分解或整理饰品。")
    ins = create_accessory_instance(item_id, quality)
    data["bag"].append(ins)
    _save_data(user_id, data)
    return ins

def _find_accessory_in_bag(data: dict, uid: str):
    bag = data.get("bag", [])
    for i, x in enumerate(bag):
        if str(x.get("uid", "")) == str(uid):
            return i, x
    return -1, None

def _parse_quality_arg(q_text: str):
    q_text = str(q_text).strip()
    mapping = {
        "1": 1, "2": 2, "3": 3, "4": 4, "5": 5,
        "一阶": 1, "二阶": 2, "三阶": 3, "四阶": 4, "五阶": 5,
        "q1": 1, "q2": 2, "q3": 3, "q4": 4, "q5": 5,
        "Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "Q5": 5,
    }
    return mapping.get(q_text, None)

def _match_accessory_type(acc: dict, t: str):
    t = str(t).strip()
    if t == "全部":
        return True
    if t in ["烈阳", "玄渊", "天衡", "星痕", "龙魄"]:
        return acc.get("set_type") == t
    if t in ["手镯", "戒指", "手环", "项链"]:
        return acc.get("part") == t
    return False

def _find_accessory_anywhere(data: dict, uid: str):
    for i, x in enumerate(data.get("bag", [])):
        if str(x.get("uid", "")) == str(uid):
            return "bag", i, x
    for s in SLOTS:
        it = data.get("equipped", {}).get(s)
        if it and str(it.get("uid", "")) == str(uid):
            return "equipped", s, it
    return None, None, None

def _default_accessory_preset():
    return {"手镯": None, "戒指": None, "手环": None, "项链": None}

def _get_accessory_preset(user_id: str, preset_idx: int):
    field = f"preset_{preset_idx}"
    raw = player_data_manager.get_field_data(str(user_id), TABLE, field)

    if not isinstance(raw, dict):
        raw = _default_accessory_preset()

    for s in SLOTS:
        if s not in raw:
            raw[s] = None

    return raw

def _save_accessory_preset(user_id: str, preset_idx: int, preset_data: dict):
    field = f"preset_{preset_idx}"
    normalized = _default_accessory_preset()
    if isinstance(preset_data, dict):
        for s in SLOTS:
            normalized[s] = preset_data.get(s)
    player_data_manager.update_or_write_data(str(user_id), TABLE, field, normalized, data_type="TEXT")

def _accessory_uid_exists(data: dict, uid: str):
    if not uid:
        return False

    # 查背包
    for x in data.get("bag", []):
        if str(x.get("uid", "")) == str(uid):
            return True

    # 查已装备
    for s in SLOTS:
        it = data.get("equipped", {}).get(s)
        if it and str(it.get("uid", "")) == str(uid):
            return True

    return False

def _clean_accessory_preset(user_id: str, preset_idx: int):
    data = _get_data(str(user_id))
    preset = _get_accessory_preset(str(user_id), preset_idx)

    changed = False
    result = _default_accessory_preset()

    for s in SLOTS:
        uid = preset.get(s)
        if uid and _accessory_uid_exists(data, uid):
            result[s] = uid
        else:
            if uid is not None:
                changed = True
            result[s] = None

    if changed:
        _save_accessory_preset(str(user_id), preset_idx, result)

    return result

def _get_accessory_by_uid(data: dict, uid: str):
    if not uid:
        return None

    for x in data.get("bag", []):
        if str(x.get("uid", "")) == str(uid):
            return x

    for s in SLOTS:
        it = data.get("equipped", {}).get(s)
        if it and str(it.get("uid", "")) == str(uid):
            return it

    return None

def _format_accessory_preset(user_id: str, preset_idx: int):
    data = _get_data(str(user_id))
    preset = _clean_accessory_preset(str(user_id), preset_idx)

    lines = [f"【预设{preset_idx}】"]
    empty = True

    for s in SLOTS:
        uid = preset.get(s)
        if not uid:
            lines.append(f"{s}：未记录")
            continue

        acc = _get_accessory_by_uid(data, uid)
        if not acc:
            lines.append(f"{s}：未记录")
            continue

        empty = False
        lines.append(
            f"{s}：{acc.get('name', '未知饰品')}[{quality_to_cn(acc.get('quality', 1))}] "
            f"({acc.get('set_type', '未知')}·UID:{uid})"
        )

    if empty:
        lines.append("（当前预设为空）")

    return "\n".join(lines)


def _iter_all_accessories(data: dict):
    for acc in data.get("bag", []) or []:
        if acc:
            yield acc, "背包"
    for slot in SLOTS:
        acc = data.get("equipped", {}).get(slot)
        if acc:
            yield acc, f"已装备:{slot}"


def _build_accessory_collection(user_id: str) -> dict:
    data = _get_data(str(user_id))
    collection = {
        set_name: {
            quality: {slot: [] for slot in SLOTS}
            for quality in QUALITY_RANGE
        }
        for set_name in ACCESSORY_SETS
    }

    for acc, where in _iter_all_accessories(data):
        set_name = str(acc.get("set_type", ""))
        part = str(acc.get("part", ""))
        quality = max(1, min(5, int(acc.get("quality", 1) or 1)))
        if set_name not in collection or part not in SLOTS:
            continue
        collection[set_name][quality][part].append(
            {
                "uid": acc.get("uid", ""),
                "name": acc.get("name", "未知饰品"),
                "where": where,
            }
        )
    return collection


def _summarize_accessory_collection(collection: dict) -> dict:
    result = {}
    for set_name in ACCESSORY_SETS:
        quality_map = collection.get(set_name, {})
        result[set_name] = {
            "owned_slots": set(),
            "complete_qualities": [],
            "best_quality": 0,
            "total_owned": 0,
        }
        for quality in QUALITY_RANGE:
            slot_map = quality_map.get(quality, {})
            owned_slots = {slot for slot in SLOTS if slot_map.get(slot)}
            result[set_name]["owned_slots"].update(owned_slots)
            result[set_name]["total_owned"] += sum(len(slot_map.get(slot, [])) for slot in SLOTS)
            if len(owned_slots) == len(SLOTS):
                result[set_name]["complete_qualities"].append(quality)
                result[set_name]["best_quality"] = max(result[set_name]["best_quality"], quality)
    return result


def _empty_set_detail() -> dict:
    return {
        "owned_by_slot": {slot: 0 for slot in SLOTS},
        "equipped_by_slot": {slot: 0 for slot in SLOTS},
        "bag_by_slot": {slot: 0 for slot in SLOTS},
        "owned_total": 0,
        "equipped_total": 0,
        "bag_total": 0,
        "duplicate_total": 0,
        "missing_slots": list(SLOTS),
        "equipped_slots": [],
        "bag_slots": [],
    }


def _build_accessory_set_details(user_id: str) -> dict:
    data = _get_data(str(user_id))
    details = {set_name: _empty_set_detail() for set_name in ACCESSORY_SETS}

    for acc, where in _iter_all_accessories(data):
        set_name = str(acc.get("set_type", ""))
        part = str(acc.get("part", ""))
        if set_name not in details or part not in SLOTS:
            continue

        info = details[set_name]
        info["owned_by_slot"][part] += 1
        info["owned_total"] += 1

        if str(where).startswith("已装备"):
            info["equipped_by_slot"][part] += 1
            info["equipped_total"] += 1
        else:
            info["bag_by_slot"][part] += 1
            info["bag_total"] += 1

    for info in details.values():
        info["missing_slots"] = [slot for slot in SLOTS if info["owned_by_slot"].get(slot, 0) <= 0]
        info["equipped_slots"] = [slot for slot in SLOTS if info["equipped_by_slot"].get(slot, 0) > 0]
        info["bag_slots"] = [slot for slot in SLOTS if info["bag_by_slot"].get(slot, 0) > 0]
        info["duplicate_total"] = sum(max(0, cnt - 1) for cnt in info["owned_by_slot"].values())

    return details


def _format_slots(slots: list[str], empty_text: str = "无") -> str:
    return "、".join(slots) if slots else empty_text


def _format_owned_slot_counts(slot_counts: dict) -> str:
    return "、".join(f"{slot}{int(slot_counts.get(slot, 0))}" for slot in SLOTS)


def _format_set_progress(info: dict) -> str:
    if "owned_slots" in info:
        owned = min(len(SLOTS), len(info.get("owned_slots", set())))
    else:
        owned = sum(1 for slot in SLOTS if info.get("owned_by_slot", {}).get(slot, 0) > 0)
    missing = info.get("missing_slots", [])
    if not missing:
        return f"部位{owned}/4，缺失：无"
    return f"部位{owned}/4，缺失：{_format_slots(missing)}"


def _format_active_set_bonus_lines(set_name: str, equipped_total: int) -> list[str]:
    lines = []
    for pieces in (2, 4):
        if equipped_total < pieces:
            continue
        bonus = SET_BONUS.get(set_name, {}).get(pieces)
        if not bonus:
            continue
        bonus_type = bonus.get("type")
        bonus_name = SET_TYPE_CN.get(bonus_type, bonus_type)
        bonus_value = float(bonus.get("value", 0))
        if bonus_type in SET_VALUE_POINT_TYPES:
            value_text = f"+{bonus_value:.0f}点"
        else:
            value_text = f"+{bonus_value * 100:.2f}%"
        lines.append(f"{pieces}件：{bonus_name}{value_text}")
    return lines


def _next_set_bonus_hint(set_name: str, detail: dict) -> str:
    equipped_total = int(detail.get("equipped_total", 0))
    if equipped_total >= 4:
        return "已激活最高4件效果"

    next_pieces = 2 if equipped_total < 2 else 4
    missing_count = max(0, next_pieces - equipped_total)
    equipped_slots = set(detail.get("equipped_slots", []))
    candidate_slots = [
        slot
        for slot in SLOTS
        if slot not in equipped_slots and detail.get("owned_by_slot", {}).get(slot, 0) > 0
    ]
    if candidate_slots:
        return f"距离{next_pieces}件效果还差{missing_count}件，可补装备部位：{_format_slots(candidate_slots)}"

    owned_missing = [
        slot
        for slot in SLOTS
        if slot not in equipped_slots and detail.get("owned_by_slot", {}).get(slot, 0) <= 0
    ]
    if owned_missing:
        return f"距离{next_pieces}件效果还差{missing_count}件，缺少部位：{_format_slots(owned_missing)}"

    return f"距离{next_pieces}件效果还差{missing_count}件"


def _format_set_bonus_lines(set_name: str) -> list[str]:
    lines = []
    for pieces in (2, 4):
        bonus = SET_BONUS.get(set_name, {}).get(pieces)
        if not bonus:
            continue
        bonus_type = bonus.get("type")
        bonus_name = SET_TYPE_CN.get(bonus_type, bonus_type)
        bonus_value = float(bonus.get("value", 0))
        if bonus_type in SET_VALUE_POINT_TYPES:
            value_text = f"+{bonus_value:.0f}点"
        else:
            value_text = f"+{bonus_value * 100:.2f}%"
        lines.append(f"{pieces}件：{bonus_name}{value_text}")
    return lines or ["暂无套装效果"]


def _resolve_collection_set_filter(arg: str) -> str | None:
    text = str(arg or "").strip()
    text = text.lstrip("/!！.")
    for suffix in ("图鉴", "套装", "套"):
        if text.endswith(suffix):
            text = text[:-len(suffix)].strip()
    if not text or text in {"全部", "总览"}:
        return None
    for set_name in ACCESSORY_SETS:
        if text == set_name:
            return set_name
    return None


def _extract_plain_text_from_event(event) -> str:
    try:
        return str(event.get_plaintext()).strip()
    except Exception:
        pass
    for attr in ("raw_message", "plaintext", "content"):
        value = getattr(event, attr, None)
        if value:
            return str(value).strip()
    return ""


def _accessory_collection_buttons(set_filter: str | None = None) -> dict:
    target = set_filter or "烈阳"
    return {
        "md_type": "背包",
        "k1": f"{target}图鉴",
        "v1": f"饰品图鉴 {target}",
        "k2": "饰品背包",
        "v2": "饰品背包",
        "k3": "我的饰品",
        "v3": "我的饰品",
        "k4": "快速装备",
        "v4": "快速装备饰品",
    }


def _format_collection_slot_detail(owned: list[dict]) -> str:
    if not owned:
        return "×"

    equipped_count = sum(1 for item in owned if str(item.get("where", "")).startswith("已装备"))
    bag_count = len(owned) - equipped_count
    parts = []
    if equipped_count:
        parts.append(f"已装备{equipped_count}")
    if bag_count:
        parts.append(f"背包{bag_count}")
    return f"√({len(owned)}件/{'、'.join(parts)})"

def _get_upgrade_cost(cur_quality: int) -> int:
    if cur_quality <= 1:
        return 1
    return cur_quality - 1

def _is_same_accessory_for_upgrade(main_acc: dict, material_acc: dict) -> bool:
    if not main_acc or not material_acc:
        return False
    return (
        int(main_acc.get("item_id", 0)) == int(material_acc.get("item_id", -1))
        and str(main_acc.get("part", "")) == str(material_acc.get("part", ""))
        and str(main_acc.get("set_type", "")) == str(material_acc.get("set_type", ""))
        and int(main_acc.get("quality", 1)) == int(material_acc.get("quality", 0))
    )

def _build_accessory_sections_for_md(user_id: str):
    data = _get_data(str(user_id))
    if not data:
        return []

    bag = data.get("bag", [])
    equipped = data.get("equipped", {})

    set_order = ["烈阳", "玄渊", "天衡", "星痕", "龙魄", "踏风", "其他"]
    buckets = {k: [] for k in set_order}

    equipped_rows = []
    for s in SLOTS:
        it = equipped.get(s)
        if not it:
            continue
        row = {
            "name": it.get("name", "未知饰品"),
            "count": 1,
            "bind": 0,
            "goods_type": "饰品",
            "uid": it.get("uid", ""),
            "quality": int(it.get("quality", 1)),
            "part": it.get("part", s),
            "set_type": it.get("set_type", "其他"),
            "is_equipped": True
        }
        equipped_rows.append(row)

    bag_rows = []
    for x in bag:
        row = {
            "name": x.get("name", "未知饰品"),
            "count": 1,
            "bind": 0,
            "goods_type": "饰品",
            "uid": x.get("uid", ""),
            "quality": int(x.get("quality", 1)),
            "part": x.get("part", ""),
            "set_type": x.get("set_type", "其他"),
            "is_equipped": False
        }
        bag_rows.append(row)

    all_rows = equipped_rows + bag_rows

    for row in all_rows:
        st = row.get("set_type", "其他")
        if st not in buckets:
            st = "其他"
        buckets[st].append(row)

    sections = []
    for st in set_order:
        rows = buckets.get(st, [])
        if not rows:
            continue

        rows = sorted(
            rows,
            key=lambda r: (
                0 if r.get("is_equipped") else 1,
                -r.get("quality", 1),
                r.get("part", ""),
                r.get("name", "")
            )
        )
        sections.append((f"{st}套装", rows))

    return sections

def _build_accessory_md_text(
    title: str,
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    total_pages: int,
    next_cmd: str = "",
    capacity_text: str = "",
) -> str:
    lines = [f"【{title}】", ""]
    if capacity_text:
        lines.extend([capacity_text, ""])

    for sec_title, rows in sections:
        if not rows:
            continue

        lines.append(f"【{sec_title}】")
        lines.append("")

        for row in rows:
            name = row.get("name", "未知饰品")
            uid = row.get("uid", "")
            q = int(row.get("quality", 1))
            part = row.get("part", "")
            set_type = row.get("set_type", "未知")

            view_cmd = quote(f"查看饰品 {uid}", safe="")
            view_md = f"[{name}](mqqapi://aio/inlinecmd?command={view_cmd}&enter=false&reply=false)"

            equip_cmd = quote(f"装备饰品 {uid}", safe="")
            wash_cmd = quote(f"饰品洗练 {uid}", safe="")
            decompose_cmd = quote(f"饰品分解 {uid}", safe="")
            op_md = (
                f"[装备](mqqapi://aio/inlinecmd?command={equip_cmd}&enter=false&reply=false) "
                f"[洗练](mqqapi://aio/inlinecmd?command={wash_cmd}&enter=false&reply=false) "
                f"[分解](mqqapi://aio/inlinecmd?command={decompose_cmd}&enter=false&reply=false)"
            )

            eq_flag = "【已装备】" if row.get("is_equipped") else ""
            lines.append(
                f"> - {eq_flag}{view_md} | {part} | {set_type} | {quality_to_cn(q)} | UID:{uid} | {op_md}"
            )
            lines.append("\r")

    lines.append("")
    lines.append(f"第 {current_page}/{total_pages} 页")
    if current_page < total_pages and next_cmd:
        next_q = quote(next_cmd, safe="")
        lines.append(f"[下一页](mqqapi://aio/inlinecmd?command={next_q}&enter=false&reply=false)")

    return "\r".join(lines)


def _build_accessory_plain_text(
    title: str,
    sections: list[tuple[str, list[dict]]],
    current_page: int,
    total_pages: int,
    next_cmd: str = "",
    capacity_text: str = "",
) -> str:
    lines = [f"【{title}】"]
    if capacity_text:
        lines.append(capacity_text)

    for sec_title, rows in sections:
        if not rows:
            continue

        lines.extend(["", f"【{sec_title}】"])
        for row in rows:
            eq_flag = "【已装备】" if row.get("is_equipped") else ""
            q = int(row.get("quality", 1))
            lines.append(
                f"- {eq_flag}{row.get('name', '未知饰品')}"
                f" | {row.get('part', '')}"
                f" | {row.get('set_type', '未知')}"
                f" | {quality_to_cn(q)}"
                f" | UID:{row.get('uid', '')}"
            )

    lines.extend(["", f"第 {current_page}/{total_pages} 页"])
    if current_page < total_pages and next_cmd:
        lines.append(f"输入 {next_cmd} 查看下一页")
    lines.append("可用命令：查看饰品 UID / 装备饰品 UID / 饰品洗练 UID / 饰品分解 UID")

    return "\n".join(lines)


__all__ = [
    name for name in globals()
    if name.isupper() or name == "quality_to_cn" or (name.startswith("_") and not name.startswith("__"))
]
