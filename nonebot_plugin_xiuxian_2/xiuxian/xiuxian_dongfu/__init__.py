try:
    import ujson as json
except ImportError:
    import json

import random
from datetime import datetime, timedelta
from pathlib import Path
from ..on_compat import on_command
from nonebot.params import CommandArg

from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, handle_send, number_to, send_help_message
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.item_json import Items

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()
items = Items()

MAP_TABLE = "map_status"
DONGFU_TABLE = "dongfu_status"
MAP_FILE = Path() / "data" / "xiuxian" / "地图.json"

SEED_CONFIG = {
    21001: {"name": "青灵草种", "price": 500000, "pool": "herb_low", "minutes": 60},
    21002: {"name": "玄木灵种", "price": 3000000, "pool": "herb_mid", "minutes": 180},
    21003: {"name": "星砂神种", "price": 15000000, "pool": "god_low", "minutes": 360},
    21004: {"name": "混元神种", "price": 80000000, "pool": "god_low", "minutes": 720},
}

HERB_LOW = list(range(3001, 3037))
HERB_MID = list(range(3037, 3109))
GOD_LOW = [15000, 15001, 15002, 15003, 15004]
GOD_HIGH = [15010, 15011]

ARRAY_LEVEL_MAX = 10
VISIT_STAMINA = 5
INFILTRATE_STAMINA = 8
INFILTRATE_DAILY_LIMIT = 3
DONGFU_PLOT_COUNT = 3
DONGFU_PLOT_MAX = 6
DONGFU_ITEM_ACCELERATE = 21005
DONGFU_ITEM_FERTILIZER = 21006
DONGFU_ITEM_ARRAY_STONE = 21007
DONGFU_ITEM_DEED = 21008
DONGFU_ACCELERATE_MINUTES = 60
DONGFU_FERTILIZER_MAX = 3
DONGFU_PATROL_STAMINA = 8
DONGFU_PATROL_DAILY_LIMIT = 3

DONGFU_GEOMANCY = {
    "水域": {
        "name": "水润灵脉",
        "grow_speed": 0.08,
        "accelerate_bonus": 20,
        "desc": "灵田成熟时间小幅缩短，灵息露催熟效果增强。",
    },
    "灵林": {
        "name": "草木灵脉",
        "grow_speed": 0.05,
        "harvest_bonus": 0.25,
        "desc": "药材类灵田收获时有概率额外产出。",
    },
    "仙山": {
        "name": "仙山灵脉",
        "grow_speed": 0.04,
        "god_bonus": 0.18,
        "desc": "神种收获时有概率额外产出，普通灵田也略微加速。",
    },
    "矿脉": {
        "name": "玄矿地脉",
        "array_discount": 0.10,
        "array_stone_reduce": 1,
        "desc": "洞府布阵灵石消耗降低，高阶布阵少量节省玄铁阵石。",
    },
    "险地": {
        "name": "险煞灵脉",
        "harvest_bonus": 0.35,
        "intrude_success_bonus": 0.08,
        "desc": "灵田收获更易额外产出，但洞府更容易被潜入得手。",
    },
}

my_dongfu = on_command("我的洞府", priority=8, block=True)
dongfu_plant = on_command("洞府种植", priority=8, block=True)
dongfu_harvest = on_command("洞府收获", aliases={"收获洞府"}, priority=8, block=True)
dongfu_accelerate = on_command("洞府催熟", priority=8, block=True)
dongfu_fertilize = on_command("洞府施肥", priority=8, block=True)
dongfu_expand = on_command("洞府扩建", priority=8, block=True)
dongfu_geomancy = on_command("洞府地脉", aliases={"地脉查看"}, priority=8, block=True)
dongfu_patrol = on_command("洞府巡山", aliases={"巡山护府"}, priority=8, block=True)
visit_friend = on_command("拜访道友", priority=8, block=True)
dongfu_array = on_command("洞府布阵", priority=8, block=True)
dongfu_help = on_command("洞府帮助", priority=8, block=True)
infiltrate_dongfu = on_command("潜入洞府", aliases={"随机潜入洞府", "随机潜入"}, priority=8, block=True)


def _now():
    return datetime.now()


def _fmt_dt(dt: datetime):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt(s: str):
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(str(s), fmt)
        except Exception:
            pass
    return None


def _today_str():
    return _now().strftime("%Y-%m-%d")


def _load_map_data():
    if not MAP_FILE.exists():
        return None
    try:
        with open(MAP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _find_map_node(realm: str, heaven: str, node_id: str):
    map_data = _load_map_data()
    if not map_data or realm not in map_data:
        return None
    heavens = map_data.get(realm, {}).get("heavens", {})
    for node in heavens.get(heaven, []):
        if str(node.get("id")) == str(node_id):
            return node
    return None


def _get_dongfu_node_type(d: dict):
    node_type = str(d.get("node_type") or "")
    if node_type:
        return node_type
    node = _find_map_node(d.get("realm", ""), d.get("heaven", ""), d.get("node_id", ""))
    if node:
        node_type = str(node.get("type") or "")
        d["node_type"] = node_type
    return node_type


def _get_geomancy(d: dict):
    return DONGFU_GEOMANCY.get(_get_dongfu_node_type(d), {})


def _format_geomancy(d: dict):
    node_type = _get_dongfu_node_type(d) or "未知"
    geomancy = _get_geomancy(d)
    if not geomancy:
        return f"地脉：普通地势（节点类型：{node_type}）\n效果：无额外加成"
    lines = [
        f"地脉：{geomancy['name']}（节点类型：{node_type}）",
        f"效果：{geomancy['desc']}",
    ]
    details = []
    if geomancy.get("grow_speed"):
        details.append(f"成熟时间-{int(geomancy['grow_speed'] * 100)}%")
    if geomancy.get("harvest_bonus"):
        details.append(f"额外产出概率+{int(geomancy['harvest_bonus'] * 100)}%")
    if geomancy.get("god_bonus"):
        details.append(f"神种额外产出概率+{int(geomancy['god_bonus'] * 100)}%")
    if geomancy.get("array_discount"):
        details.append(f"布阵灵石-{int(geomancy['array_discount'] * 100)}%")
    if geomancy.get("array_stone_reduce"):
        details.append(f"高阶布阵阵石-{geomancy['array_stone_reduce']}")
    if geomancy.get("accelerate_bonus"):
        details.append(f"催熟额外+{geomancy['accelerate_bonus']}分钟")
    if geomancy.get("intrude_success_bonus"):
        details.append(f"被潜入成功率+{int(geomancy['intrude_success_bonus'] * 100)}%")
    if details:
        lines.append("数值：" + "、".join(details))
    return "\n".join(lines)


def _empty_plant_slot(slot_no: int):
    return {
        "slot": slot_no,
        "seed_id": 0,
        "seed_name": "",
        "plant_start": "",
        "plant_finish": "",
        "fertilizer": 0,
    }


def _default_plant_slots():
    return [_empty_plant_slot(i) for i in range(1, DONGFU_PLOT_COUNT + 1)]


def _default_dongfu():
    return {
        "built": 0,
        "realm": "",
        "heaven": "",
        "node_id": "",
        "node_name": "",
        "node_type": "",
        "array_level": 0,
        "planting": 0,
        "plant_seed_id": 0,
        "plant_start": "",
        "plant_finish": "",
        "plant_slots": _default_plant_slots(),
        "plot_count": DONGFU_PLOT_COUNT,
        "intrude_date": "",
        "intrude_count": 0,
        "patrol_date": "",
        "patrol_count": 0,
        "patrol_guard": 0,
    }


def _to_int(value, default=0):
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_plant_slots(d: dict):
    plot_count = min(DONGFU_PLOT_MAX, max(DONGFU_PLOT_COUNT, _to_int(d.get("plot_count"), DONGFU_PLOT_COUNT)))
    d["plot_count"] = plot_count
    raw_slots = d.get("plant_slots")
    if isinstance(raw_slots, str):
        try:
            raw_slots = json.loads(raw_slots)
        except Exception:
            raw_slots = []
    if not isinstance(raw_slots, list):
        raw_slots = []

    slots = []
    for index in range(plot_count):
        raw = raw_slots[index] if index < len(raw_slots) and isinstance(raw_slots[index], dict) else {}
        seed_id = _to_int(raw.get("seed_id"))
        seed_name = str(raw.get("seed_name") or "")
        if seed_id and not seed_name:
            seed_name = SEED_CONFIG.get(seed_id, {}).get("name", "")
        slots.append({
            "slot": index + 1,
            "seed_id": seed_id,
            "seed_name": seed_name,
            "plant_start": str(raw.get("plant_start") or ""),
            "plant_finish": str(raw.get("plant_finish") or ""),
            "fertilizer": min(DONGFU_FERTILIZER_MAX, max(0, _to_int(raw.get("fertilizer")))),
        })

    legacy_seed_id = _to_int(d.get("plant_seed_id"))
    if not any(_to_int(slot.get("seed_id")) for slot in slots) and _to_int(d.get("planting")) == 1 and legacy_seed_id:
        slots[0] = {
            "slot": 1,
            "seed_id": legacy_seed_id,
            "seed_name": SEED_CONFIG.get(legacy_seed_id, {}).get("name", ""),
            "plant_start": str(d.get("plant_start") or ""),
            "plant_finish": str(d.get("plant_finish") or ""),
            "fertilizer": 0,
        }

    d["plant_slots"] = slots
    return slots


def _active_plant_slots(d: dict):
    return [slot for slot in _normalize_plant_slots(d) if _to_int(slot.get("seed_id")) in SEED_CONFIG]


def _sync_plant_fields(d: dict):
    active = next(iter(_active_plant_slots(d)), None)
    if active:
        d["planting"] = 1
        d["plant_seed_id"] = _to_int(active.get("seed_id"))
        d["plant_start"] = active.get("plant_start", "")
        d["plant_finish"] = active.get("plant_finish", "")
    else:
        d["planting"] = 0
        d["plant_seed_id"] = 0
        d["plant_start"] = ""
        d["plant_finish"] = ""
    return d


def _reset_patrol_count_if_needed(d: dict):
    today = _today_str()
    if d.get("patrol_date") != today:
        d["patrol_date"] = today
        d["patrol_count"] = 0
        d["patrol_guard"] = 0
    return d


def _consume_patrol_guard(d: dict):
    guard = _to_int(d.get("patrol_guard"))
    if guard <= 0:
        return False
    d["patrol_guard"] = guard - 1
    return True


def _format_minutes_left(finish: datetime, now: datetime):
    return max(1, int((finish - now).total_seconds() + 59) // 60)


def _format_plant_slot(slot: dict, now: datetime | None = None):
    now = now or _now()
    seed_id = _to_int(slot.get("seed_id"))
    if seed_id not in SEED_CONFIG:
        return f"{slot.get('slot')}号灵田：空闲"

    seed_name = slot.get("seed_name") or SEED_CONFIG[seed_id]["name"]
    fertilizer = _to_int(slot.get("fertilizer"))
    fertilizer_msg = f"，肥力+{fertilizer}" if fertilizer > 0 else ""
    finish = _parse_dt(slot.get("plant_finish", ""))
    if not finish:
        return f"{slot.get('slot')}号灵田：{seed_name}（状态异常{fertilizer_msg}）"
    if now >= finish:
        return f"{slot.get('slot')}号灵田：{seed_name}（可收获{fertilizer_msg}）"
    return f"{slot.get('slot')}号灵田：{seed_name}（剩余{_format_minutes_left(finish, now)}分钟{fertilizer_msg}）"


def _format_plant_slots(d: dict):
    now = _now()
    lines = ["【洞府灵田状态】"]
    lines.extend(_format_plant_slot(slot, now) for slot in _normalize_plant_slots(d))
    return "\n".join(lines)


def _find_seed_id(seed_name: str):
    for sid, conf in SEED_CONFIG.items():
        if conf["name"] == seed_name:
            return sid
    return None


def _parse_plant_args(text: str):
    text = text.strip()
    if not text:
        return None, ""
    parts = text.split(maxsplit=1)
    if parts[0].isdigit():
        return _to_int(parts[0]), parts[1].strip() if len(parts) > 1 else ""
    return None, text


def _parse_slot_no(text: str):
    text = text.strip()
    if not text:
        return None
    return _to_int(text, None)


def _get_slot_by_no(d: dict, slot_no: int):
    plot_count = min(DONGFU_PLOT_MAX, max(DONGFU_PLOT_COUNT, _to_int(d.get("plot_count"), DONGFU_PLOT_COUNT)))
    if slot_no < 1 or slot_no > plot_count:
        return None
    return _normalize_plant_slots(d)[slot_no - 1]


def _consume_item(uid: str, item_id: int, item_name: str, count: int = 1):
    if _to_int(sql_message.goods_num(uid, item_id)) < count:
        return False
    sql_message.update_back_j(uid, item_id, count)
    return True


def _get_dongfu(uid: str):
    d = player_data_manager.get_fields(str(uid), DONGFU_TABLE)
    default = _default_dongfu()
    changed = False

    if not d:
        d = default.copy()
        changed = True
        for k, v in d.items():
            player_data_manager.update_or_write_data(str(uid), DONGFU_TABLE, k, v)
    else:
        for k, v in default.items():
            if k not in d or d.get(k) is None:
                d[k] = v
                changed = True
                player_data_manager.update_or_write_data(str(uid), DONGFU_TABLE, k, v)

    plant_before = d.get("plant_slots")
    legacy_before = (d.get("planting"), d.get("plant_seed_id"), d.get("plant_start"), d.get("plant_finish"))
    node_type_before = d.get("node_type")
    _get_dongfu_node_type(d)
    _sync_plant_fields(d)
    legacy_after = (d.get("planting"), d.get("plant_seed_id"), d.get("plant_start"), d.get("plant_finish"))
    if plant_before != d.get("plant_slots") or legacy_before != legacy_after or node_type_before != d.get("node_type"):
        changed = True

    if changed:
        _save_dongfu(str(uid), d)
    return d


def _save_dongfu(uid: str, d: dict):
    _sync_plant_fields(d)
    for k, v in d.items():
        player_data_manager.update_or_write_data(str(uid), DONGFU_TABLE, k, v)


def _has_dongfu(uid: str):
    d = _get_dongfu(uid)
    return _to_int(d.get("built")) == 1, d


def _pick_pool(pool_name: str):
    if pool_name == "herb_low":
        return HERB_LOW
    if pool_name == "herb_mid":
        return HERB_MID
    if pool_name == "god_low":
        return GOD_LOW
    if pool_name == "god_high":
        return GOD_HIGH
    return HERB_LOW


def _roll_harvest(seed_id: int, array_lv: int):
    conf = SEED_CONFIG.get(seed_id)
    if not conf:
        return []

    pool = _pick_pool(conf["pool"])

    # 降收益：普通 1~2，高阵法最多加1
    base_num = random.randint(1, 2)
    bonus = 1 if (array_lv >= 3 and random.random() < min(0.45, array_lv * 0.04)) else 0
    total_num = base_num + bonus

    result = []
    for _ in range(total_num):
        gid = random.choice(pool)
        item = items.get_data_by_item_id(gid)
        if item:
            result.append((gid, item["name"], item.get("type", "药材"), 1))
    return result


def _get_same_node_users(uid: str):
    me_map = player_data_manager.get_fields(str(uid), MAP_TABLE) or {}
    if not me_map:
        return []
    realm = me_map.get("realm")
    heaven = me_map.get("heaven")
    node_id = me_map.get("node_id")
    if not all([realm, heaven, node_id]):
        return []

    all_user_ids = sql_message.get_all_user_id() or []
    result = []
    for x in all_user_ids:
        x = str(x)
        st = player_data_manager.get_fields(x, MAP_TABLE)
        if not st:
            continue
        if st.get("realm") == realm and st.get("heaven") == heaven and st.get("node_id") == node_id:
            ui = sql_message.get_user_info_with_id(x)
            if ui:
                result.append(ui)
    return result


def _reset_intrude_count_if_needed(d: dict):
    today = _today_str()
    if d.get("intrude_date") != today:
        d["intrude_date"] = today
        d["intrude_count"] = 0
    return d


def _consume_intrude_count(target_uid: str):
    d = _get_dongfu(target_uid)
    d = _reset_intrude_count_if_needed(d)
    d["intrude_count"] = _to_int(d.get("intrude_count")) + 1
    _save_dongfu(target_uid, d)
    return d["intrude_count"]


def _can_intrude(target_uid: str):
    d = _get_dongfu(target_uid)
    d = _reset_intrude_count_if_needed(d)
    _save_dongfu(target_uid, d)
    return _to_int(d.get("intrude_count")) < INFILTRATE_DAILY_LIMIT, d


def _get_random_dongfu_target(my_uid: str):
    all_user_ids = sql_message.get_all_user_id() or []
    candidates = []
    for uid in all_user_ids:
        uid = str(uid)
        if uid == str(my_uid):
            continue
        d = player_data_manager.get_fields(uid, DONGFU_TABLE) or {}
        if _to_int(d.get("built")) != 1:
            continue
        _normalize_plant_slots(d)
        if not _active_plant_slots(d):
            continue
        can_intrude, _ = _can_intrude(uid)
        if not can_intrude:
            continue
        ui = sql_message.get_user_info_with_id(uid)
        if ui:
            candidates.append(ui)
    return random.choice(candidates) if candidates else None


@dongfu_help.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = (
        "【洞府帮助】\n"
        "1. 我的洞府：查看洞府信息\n"
        "2. 洞府种植 [种子名] / 洞府种植 [编号] [种子名]\n"
        "3. 洞府收获 / 洞府收获 [编号]\n"
        "4. 洞府施肥 [编号] / 洞府催熟 [编号]\n"
        "5. 洞府地脉 / 洞府巡山\n"
        "6. 洞府扩建 / 洞府布阵\n"
        "7. 拜访道友 道号\n"
        "8. 潜入洞府 道号 / 随机潜入洞府\n"
        "注：\n"
        f"- 初始拥有{DONGFU_PLOT_COUNT}块洞府灵田，最多可扩建至{DONGFU_PLOT_MAX}块\n"
        "- 地图钓鱼/采集/挖矿/探索可获得洞府材料\n"
        f"- 洞府巡山每日最多{DONGFU_PATROL_DAILY_LIMIT}次，可降低潜入风险\n"
        "- 潜入只能对附近已建设洞府的道友使用\n"
        f"- 每个洞府每日最多被潜入{INFILTRATE_DAILY_LIMIT}次"
    )
    await send_help_message(
        bot, event, msg,
        k1="洞府", v1="我的洞府",
        k2="种植", v2="洞府种植",
        k3="收获", v3="洞府收获"
    )


@my_dongfu.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    d = _reset_intrude_count_if_needed(d)
    d = _reset_patrol_count_if_needed(d)
    intrude_left = INFILTRATE_DAILY_LIMIT - _to_int(d.get("intrude_count"))
    _save_dongfu(uid, d)
    active_count = len(_active_plant_slots(d))
    ready_count = 0
    now = _now()
    for slot in _active_plant_slots(d):
        finish = _parse_dt(slot.get("plant_finish", ""))
        if finish and now >= finish:
            ready_count += 1

    msg = (
        f"【我的洞府】\n"
        f"位置：{d.get('realm')}·{d.get('heaven')}·{d.get('node_name')}\n"
        f"阵法等级：{d.get('array_level', 0)}\n"
        f"{_format_geomancy(d)}\n"
        f"灵田：{active_count}/{_to_int(d.get('plot_count'), DONGFU_PLOT_COUNT)} 已播种，{ready_count}块可收获\n"
        f"巡山护府：{_to_int(d.get('patrol_guard'))}层，今日巡山{_to_int(d.get('patrol_count'))}/{DONGFU_PATROL_DAILY_LIMIT}\n"
        f"今日剩余可被潜入次数：{intrude_left}\n"
        f"{_format_plant_slots(d)}"
    )
    await handle_send(bot, event, msg)


@dongfu_plant.handle(parameterless=[Cooldown(cd_time=0, stamina_cost=2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    now = _now()
    slot_no, seed_name = _parse_plant_args(args.extract_plain_text())
    if not seed_name:
        await handle_send(bot, event, "请指定种子名，例如：洞府种植 青灵草种；也可指定灵田：洞府种植 2 青灵草种")
        return

    seed_id = _find_seed_id(seed_name)
    if seed_id is None:
        await handle_send(bot, event, f"未识别种子【{seed_name}】。")
        return

    slots = _normalize_plant_slots(d)
    if slot_no is None:
        slot = next((s for s in slots if _to_int(s.get("seed_id")) not in SEED_CONFIG), None)
        if slot is None:
            await handle_send(bot, event, f"洞府灵田已全部播种，请先使用洞府收获。\n{_format_plant_slots(d)}")
            return
    else:
        slot = _get_slot_by_no(d, slot_no)
        if slot is None:
            await handle_send(bot, event, f"灵田编号只能是1到{_to_int(d.get('plot_count'), DONGFU_PLOT_COUNT)}。")
            return
        if _to_int(slot.get("seed_id")) in SEED_CONFIG:
            await handle_send(bot, event, f"{slot_no}号灵田已有种植，请先收获后再播种。\n{_format_plant_slots(d)}")
            return

    have = sql_message.goods_num(uid, seed_id)
    if _to_int(have) <= 0:
        await handle_send(bot, event, f"你没有【{seed_name}】，请先去种子商店购买。")
        return

    # 扣种子
    sql_message.update_back_j(uid, seed_id, 1)

    # 阵法加速（平衡后：每级4%）
    array_lv = _to_int(d.get("array_level"))
    base_minutes = int(SEED_CONFIG[seed_id]["minutes"])
    geomancy = _get_geomancy(d)
    speed = 1 + array_lv * 0.04 + float(geomancy.get("grow_speed", 0))
    real_minutes = max(10, int(base_minutes / speed))

    slots[_to_int(slot.get("slot")) - 1] = {
        "slot": _to_int(slot.get("slot")),
        "seed_id": seed_id,
        "seed_name": seed_name,
        "plant_start": _fmt_dt(now),
        "plant_finish": _fmt_dt(now + timedelta(minutes=real_minutes)),
        "fertilizer": 0,
    }
    d["plant_slots"] = slots
    _save_dongfu(uid, d)

    await handle_send(
        bot,
        event,
        f"已在{slot.get('slot')}号灵田播种【{seed_name}】，预计{real_minutes}分钟后可收获。\n{_format_plant_slots(d)}"
    )


@dongfu_harvest.handle(parameterless=[Cooldown(cd_time=0, stamina_cost=2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    text = args.extract_plain_text().strip()
    slot_no = _to_int(text, None) if text else None
    if text and slot_no is None:
        await handle_send(bot, event, f"请使用：洞府收获 或 洞府收获 1-{_to_int(d.get('plot_count'), DONGFU_PLOT_COUNT)}")
        return

    now = _now()
    slots = _normalize_plant_slots(d)
    target_slots = slots
    if slot_no is not None:
        slot = _get_slot_by_no(d, slot_no)
        if slot is None:
            await handle_send(bot, event, f"灵田编号只能是1到{_to_int(d.get('plot_count'), DONGFU_PLOT_COUNT)}。")
            return
        target_slots = [slot]

    harvest_slots = []
    wait_lines = []
    for slot in target_slots:
        seed_id = _to_int(slot.get("seed_id"))
        if seed_id not in SEED_CONFIG:
            if slot_no is not None:
                wait_lines.append(f"{slot_no}号灵田当前空闲。")
            continue
        finish = _parse_dt(slot.get("plant_finish", ""))
        if finish and now >= finish:
            harvest_slots.append(slot)
        elif finish:
            wait_lines.append(f"{slot.get('slot')}号灵田还需{_format_minutes_left(finish, now)}分钟。")
        else:
            wait_lines.append(f"{slot.get('slot')}号灵田状态异常，无法收获。")

    if not harvest_slots:
        detail = "\n".join(wait_lines) if wait_lines else "暂无成熟灵田。"
        await handle_send(bot, event, f"{detail}\n{_format_plant_slots(d)}")
        return

    array_lv = _to_int(d.get("array_level"))
    geomancy = _get_geomancy(d)
    reward_map = {}
    failed_slots = []
    for slot in harvest_slots:
        seed_id = _to_int(slot.get("seed_id"))
        drops = _roll_harvest(seed_id, array_lv)
        fertilizer = _to_int(slot.get("fertilizer"))
        for _ in range(fertilizer):
            extra_drops = _roll_harvest(seed_id, array_lv)
            if extra_drops:
                drops.append(random.choice(extra_drops))
        extra_rate = float(geomancy.get("harvest_bonus", 0))
        if SEED_CONFIG.get(seed_id, {}).get("pool", "").startswith("god"):
            extra_rate += float(geomancy.get("god_bonus", 0))
        if extra_rate > 0 and random.random() < extra_rate:
            extra_drops = _roll_harvest(seed_id, array_lv)
            if extra_drops:
                drops.append(random.choice(extra_drops))
        if not drops:
            failed_slots.append(str(slot.get("slot")))
        for gid, name, tp, num in drops:
            if gid not in reward_map:
                reward_map[gid] = {"name": name, "type": tp, "num": 0}
            reward_map[gid]["num"] += num
        slots[_to_int(slot.get("slot")) - 1] = _empty_plant_slot(_to_int(slot.get("slot")))

    for gid, reward in reward_map.items():
        sql_message.send_back(uid, gid, reward["name"], reward["type"], reward["num"], 1)

    d["plant_slots"] = slots
    _save_dongfu(uid, d)

    lines = [f"洞府收获完成，共收获{len(harvest_slots)}块灵田："]
    if reward_map:
        lines.extend(f"- {reward['name']} x{reward['num']}" for reward in reward_map.values())
    if failed_slots:
        lines.append(f"{'、'.join(failed_slots)}号灵田无产出。")
    lines.append(_format_plant_slots(d))
    await handle_send(bot, event, "\n".join(lines))


@dongfu_geomancy.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    msg = (
        f"【洞府地脉】\n"
        f"位置：{d.get('realm')}·{d.get('heaven')}·{d.get('node_name')}\n"
        f"{_format_geomancy(d)}"
    )
    await handle_send(bot, event, msg)


@dongfu_patrol.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    d = _reset_patrol_count_if_needed(d)
    if _to_int(d.get("patrol_count")) >= DONGFU_PATROL_DAILY_LIMIT:
        await handle_send(bot, event, f"今日洞府巡山次数已达上限（{DONGFU_PATROL_DAILY_LIMIT}次）。")
        return

    stamina = _to_int(user_info.get("user_stamina"))
    if stamina < DONGFU_PATROL_STAMINA:
        await handle_send(bot, event, f"体力不足，洞府巡山需要{DONGFU_PATROL_STAMINA}点体力。")
        return

    sql_message.update_user_stamina(uid, DONGFU_PATROL_STAMINA, 2)
    d["patrol_count"] = _to_int(d.get("patrol_count")) + 1
    d["patrol_guard"] = min(3, _to_int(d.get("patrol_guard")) + 1)

    geomancy = _get_geomancy(d)
    node_type = _get_dongfu_node_type(d)
    rewards = []
    material_plan = {
        "水域": (DONGFU_ITEM_ACCELERATE, "灵息露", 0.35),
        "灵林": (DONGFU_ITEM_FERTILIZER, "五色灵壤", 0.35),
        "仙山": (DONGFU_ITEM_FERTILIZER, "五色灵壤", 0.45),
        "矿脉": (DONGFU_ITEM_ARRAY_STONE, "玄铁阵石", 0.35),
        "险地": (DONGFU_ITEM_DEED, "洞府地契", 0.10),
    }
    if node_type in material_plan:
        item_id, item_name, chance = material_plan[node_type]
        if random.random() < chance:
            sql_message.send_back(uid, item_id, item_name, "特殊物品", 1, 1)
            rewards.append(f"{item_name}x1")

    stone_gain = random.randint(50000, 150000)
    if geomancy.get("name"):
        stone_gain = int(stone_gain * 1.2)
    sql_message.update_ls(uid, stone_gain, 1)
    rewards.append(f"灵石x{number_to(stone_gain)}")

    _save_dongfu(uid, d)
    await handle_send(
        bot,
        event,
        f"洞府巡山完成。\n"
        f"地脉：{geomancy.get('name', '普通地势')}\n"
        f"获得：{'、'.join(rewards)}\n"
        f"巡山护府：{_to_int(d.get('patrol_guard'))}层\n"
        f"今日巡山：{_to_int(d.get('patrol_count'))}/{DONGFU_PATROL_DAILY_LIMIT}"
    )


@dongfu_fertilize.handle(parameterless=[Cooldown(cd_time=0, stamina_cost=1)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    slot_no = _parse_slot_no(args.extract_plain_text())
    if slot_no is None:
        await handle_send(bot, event, "请指定灵田编号，例如：洞府施肥 1")
        return

    slot = _get_slot_by_no(d, slot_no)
    if slot is None:
        await handle_send(bot, event, f"灵田编号只能是1到{_to_int(d.get('plot_count'), DONGFU_PLOT_COUNT)}。")
        return

    seed_id = _to_int(slot.get("seed_id"))
    if seed_id not in SEED_CONFIG:
        await handle_send(bot, event, f"{slot_no}号灵田当前空闲，无法施肥。")
        return

    fertilizer = _to_int(slot.get("fertilizer"))
    if fertilizer >= DONGFU_FERTILIZER_MAX:
        await handle_send(bot, event, f"{slot_no}号灵田肥力已满。")
        return

    if not _consume_item(uid, DONGFU_ITEM_FERTILIZER, "五色灵壤"):
        await handle_send(bot, event, "你没有【五色灵壤】。可通过地图采集/探索获得。")
        return

    slots = _normalize_plant_slots(d)
    slots[slot_no - 1]["fertilizer"] = fertilizer + 1
    d["plant_slots"] = slots
    _save_dongfu(uid, d)
    await handle_send(bot, event, f"已对{slot_no}号灵田施肥，当前肥力+{fertilizer + 1}。\n{_format_plant_slots(d)}")


@dongfu_accelerate.handle(parameterless=[Cooldown(cd_time=0, stamina_cost=1)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    slot_no = _parse_slot_no(args.extract_plain_text())
    if slot_no is None:
        await handle_send(bot, event, "请指定灵田编号，例如：洞府催熟 1")
        return

    slot = _get_slot_by_no(d, slot_no)
    if slot is None:
        await handle_send(bot, event, f"灵田编号只能是1到{_to_int(d.get('plot_count'), DONGFU_PLOT_COUNT)}。")
        return

    seed_id = _to_int(slot.get("seed_id"))
    if seed_id not in SEED_CONFIG:
        await handle_send(bot, event, f"{slot_no}号灵田当前空闲，无法催熟。")
        return

    finish = _parse_dt(slot.get("plant_finish", ""))
    now = _now()
    if not finish:
        await handle_send(bot, event, f"{slot_no}号灵田状态异常，无法催熟。")
        return
    if now >= finish:
        await handle_send(bot, event, f"{slot_no}号灵田已成熟，请使用洞府收获。")
        return

    if not _consume_item(uid, DONGFU_ITEM_ACCELERATE, "灵息露"):
        await handle_send(bot, event, "你没有【灵息露】。可通过地图钓鱼/探索获得。")
        return

    geomancy = _get_geomancy(d)
    accelerate_minutes = DONGFU_ACCELERATE_MINUTES + _to_int(geomancy.get("accelerate_bonus"))
    new_finish = max(now, finish - timedelta(minutes=accelerate_minutes))
    slots = _normalize_plant_slots(d)
    slots[slot_no - 1]["plant_finish"] = _fmt_dt(new_finish)
    d["plant_slots"] = slots
    _save_dongfu(uid, d)
    await handle_send(bot, event, f"已使用【灵息露】催熟{slot_no}号灵田，成熟时间缩短{accelerate_minutes}分钟。\n{_format_plant_slots(d)}")


@dongfu_expand.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    plot_count = _to_int(d.get("plot_count"), DONGFU_PLOT_COUNT)
    if plot_count >= DONGFU_PLOT_MAX:
        await handle_send(bot, event, f"洞府灵田已扩建至上限：{DONGFU_PLOT_MAX}块。")
        return

    next_count = plot_count + 1
    deed_need = next_count - DONGFU_PLOT_COUNT
    stone_cost = 20000000 * deed_need
    if _to_int(sql_message.goods_num(uid, DONGFU_ITEM_DEED)) < deed_need:
        await handle_send(bot, event, f"扩建至{next_count}块灵田需要【洞府地契】x{deed_need}。可通过地图探索获得。")
        return
    if _to_int(user_info.get("stone")) < stone_cost:
        await handle_send(bot, event, f"扩建至{next_count}块灵田需要{number_to(stone_cost)}灵石。")
        return

    sql_message.update_back_j(uid, DONGFU_ITEM_DEED, deed_need)
    sql_message.update_ls(uid, stone_cost, 2)
    d["plot_count"] = next_count
    _normalize_plant_slots(d)
    _save_dongfu(uid, d)
    await handle_send(bot, event, f"洞府扩建成功，灵田数量提升至{next_count}块。\n{_format_plant_slots(d)}")


@visit_friend.handle(parameterless=[Cooldown(cd_time=0, stamina_cost=VISIT_STAMINA)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    tname = args.extract_plain_text().strip()
    if not tname:
        await handle_send(bot, event, "请使用：拜访道友 道号")
        return

    target = sql_message.get_user_info_with_name(tname)
    if not target:
        await handle_send(bot, event, f"未找到道友【{tname}】")
        return

    tid = str(target["user_id"])
    if tid == uid:
        await handle_send(bot, event, "你不能拜访自己。")
        return

    td = _get_dongfu(tid)
    if _to_int(td.get("built")) != 1:
        await handle_send(bot, event, f"{tname}尚未建设洞府。")
        return

    gain = random.randint(10000, 50000)
    sql_message.update_ls(uid, gain, 1)
    await handle_send(bot, event, f"你拜访了【{tname}】的洞府（{td.get('node_name')}），获得灵石{number_to(gain)}。")


@dongfu_array.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    uid = str(user_info["user_id"])
    has, d = _has_dongfu(uid)
    if not has:
        await handle_send(bot, event, "你尚未建设洞府。")
        return

    lv = _to_int(d.get("array_level"))
    if lv >= ARRAY_LEVEL_MAX:
        await handle_send(bot, event, "阵法已达满级。")
        return

    next_lv = lv + 1
    geomancy = _get_geomancy(d)
    cost = int(3000000 * (lv + 1) * (1 - float(geomancy.get("array_discount", 0))))
    array_stone_need = max(0, next_lv - 3 - _to_int(geomancy.get("array_stone_reduce")))
    if _to_int(user_info.get("stone")) < cost:
        await handle_send(bot, event, f"升级阵法需要{number_to(cost)}灵石。")
        return
    if array_stone_need > 0 and _to_int(sql_message.goods_num(uid, DONGFU_ITEM_ARRAY_STONE)) < array_stone_need:
        await handle_send(bot, event, f"升级至{next_lv}级阵法需要【玄铁阵石】x{array_stone_need}。可通过地图挖矿/战斗获得。")
        return

    if array_stone_need > 0:
        sql_message.update_back_j(uid, DONGFU_ITEM_ARRAY_STONE, array_stone_need)
    sql_message.update_ls(uid, cost, 2)
    d["array_level"] = next_lv
    _save_dongfu(uid, d)
    item_msg = f"，玄铁阵石x{array_stone_need}" if array_stone_need > 0 else ""
    await handle_send(bot, event, f"消耗{number_to(cost)}灵石{item_msg}，洞府布阵成功，当前阵法等级：{next_lv}")


@infiltrate_dongfu.handle(parameterless=[Cooldown(cd_time=1.8, stamina_cost=INFILTRATE_STAMINA)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    my_uid = str(user_info["user_id"])
    tname = args.extract_plain_text().strip()
    if not tname:
        target = _get_random_dongfu_target(my_uid)
        if not target:
            await handle_send(bot, event, "暂未找到可随机潜入的洞府。")
            return
        tname = target["user_name"]
    else:
        nearby_users = _get_same_node_users(my_uid)
        target = next((u for u in nearby_users if u["user_name"] == tname), None)
        if not target:
            await handle_send(bot, event, f"附近未找到道友【{tname}】。指定潜入只能针对同一地点附近的洞府。")
            return

    target_uid = str(target["user_id"])
    if target_uid == my_uid:
        await handle_send(bot, event, "你不能潜入自己的洞府。")
        return

    has, td = _has_dongfu(target_uid)
    if not has:
        await handle_send(bot, event, f"{tname}尚未建设洞府。")
        return

    active_slots = _active_plant_slots(td)
    if not active_slots:
        await handle_send(bot, event, f"{tname}的洞府当前没有可图谋的灵田。")
        return
    target_slot = random.choice(active_slots)

    can_intrude, td = _can_intrude(target_uid)
    if not can_intrude:
        await handle_send(bot, event, f"{tname}的洞府今日已被盯上太多次，暂时无法再潜入。")
        return
    target_slot = _get_slot_by_no(td, _to_int(target_slot.get("slot")))
    if target_slot is None:
        await handle_send(bot, event, f"{tname}的灵田状态异常，潜入失败。")
        return

    finish = _parse_dt(target_slot.get("plant_finish", ""))
    if not finish:
        await handle_send(bot, event, f"{tname}的灵田状态异常，潜入失败。")
        return

    seed_id = _to_int(target_slot.get("seed_id"))
    if seed_id not in SEED_CONFIG:
        await handle_send(bot, event, "目标洞府种植数据异常。")
        return

    # 占用目标当日被潜入次数
    current_intrude = _consume_intrude_count(target_uid)

    now = _now()
    array_lv = _to_int(td.get("array_level"))
    geomancy = _get_geomancy(td)
    guarded = _consume_patrol_guard(td)
    if guarded:
        _save_dongfu(target_uid, td)

    detect_rate = min(0.80, 0.20 + array_lv * 0.06)
    matured = now >= finish
    success_rate = max(0.20, 0.72 - array_lv * 0.04) if matured else max(0.12, 0.45 - array_lv * 0.03)
    success_rate += float(geomancy.get("intrude_success_bonus", 0))
    if guarded:
        detect_rate = min(0.90, detect_rate + 0.12)
        success_rate = max(0.05, success_rate - 0.18)
    success_rate = min(0.90, success_rate)

    detected = random.random() < detect_rate
    success = random.random() < success_rate

    if detected and not success:
        loss_stone = random.randint(50000, 200000) * max(1, array_lv)
        sql_message.update_ls(my_uid, loss_stone, 2)
        left = max(0, INFILTRATE_DAILY_LIMIT - current_intrude)
        guard_msg = "\n目标洞府巡山护府尚有余威。" if guarded else ""
        await handle_send(
            bot,
            event,
            f"❌ 你潜入【{tname}】洞府时触发阵法警示，被当场逼退！\n"
            f"对方阵法等级：{array_lv}\n"
            f"损失灵石：{number_to(loss_stone)}\n"
            f"该洞府今日剩余可被潜入次数：{left}"
            f"{guard_msg}"
        )
        return

    stealth_penalty = 0.6 if (detected and success) else 1.0
    rewards = []

    if matured:
        all_drops = _roll_harvest(seed_id, array_lv)
        if all_drops:
            take_n = max(1, min(len(all_drops), int(len(all_drops) * 0.5)))
            random.shuffle(all_drops)
            steal_drops = all_drops[:take_n]
            if stealth_penalty < 1.0 and len(steal_drops) > 1:
                steal_drops = steal_drops[:max(1, len(steal_drops) // 2)]

            for gid, name, tp, num in steal_drops:
                sql_message.send_back(my_uid, gid, name, tp, num, 1)
                rewards.append(f"{name} x{num}")
    else:
        conf = SEED_CONFIG.get(seed_id, {})
        pool_name = conf.get("pool", "herb_low")

        if pool_name == "herb_low":
            pool = HERB_LOW[:8]
        elif pool_name == "herb_mid":
            pool = HERB_MID[:8]
        elif pool_name == "god_low":
            pool = HERB_MID[:8]
        else:
            pool = HERB_LOW[:8]

        steal_count = 1 if random.random() < 0.75 else 2
        if stealth_penalty < 1.0:
            steal_count = 1

        for _ in range(steal_count):
            gid = random.choice(pool)
            item = items.get_data_by_item_id(gid)
            if item:
                sql_message.send_back(my_uid, gid, item["name"], item.get("type", "药材"), 1, 1)
                rewards.append(f"{item['name']} x1")

        ls_gain = int(random.randint(30000, 80000) * stealth_penalty)
        if ls_gain > 0:
            sql_message.update_ls(my_uid, ls_gain, 1)
            rewards.append(f"灵石 x{number_to(ls_gain)}")

    # ===== 新增：偷取成功时，目标增加种植时间 =====
    added_minutes = 0
    if success and rewards:
        # 成熟被偷影响更大，未成熟影响较小；阵法高会造成更强“反制扰动”
        if matured:
            base_delay = random.randint(20, 45)
        else:
            base_delay = random.randint(8, 20)

        added_minutes = base_delay + int(array_lv * 2)

        # 上限保护：单次最多+180分钟
        added_minutes = min(180, added_minutes)

        old_finish = finish
        new_finish = old_finish + timedelta(minutes=added_minutes)
        slots = _normalize_plant_slots(td)
        slots[_to_int(target_slot.get("slot")) - 1]["plant_finish"] = _fmt_dt(new_finish)
        td["plant_slots"] = slots
        _save_dongfu(target_uid, td)

    left = max(0, INFILTRATE_DAILY_LIMIT - current_intrude)

    if not rewards:
        if detected:
            await handle_send(
                bot,
                event,
                f"⚠️ 你虽摸进了【{tname}】的洞府，却在阵法波动中仓促撤离，一无所获。\n"
                f"该洞府今日剩余可被潜入次数：{left}"
            )
        else:
            await handle_send(
                bot,
                event,
                f"你潜入了【{tname}】的洞府，但这次并没有找到可带走的灵材。\n"
                f"该洞府今日剩余可被潜入次数：{left}"
            )
        return

    delay_msg = f"\n对方灵田受扰，生长时间额外增加 {added_minutes} 分钟。" if added_minutes > 0 else ""
    guard_msg = "\n目标洞府巡山护府尚有余威，你行动明显受阻。" if guarded else ""
    slot_msg = f"{target_slot.get('slot')}号灵田"

    if detected:
        msg = (
            f"⚠️ 你潜入【{tname}】洞府的{slot_msg}时触动阵纹，但仍抢先带走部分灵材！\n"
            f"获得：{'、'.join(rewards)}"
            f"{delay_msg}{guard_msg}\n"
            f"该洞府今日剩余可被潜入次数：{left}"
        )
    else:
        msg = (
            f"🕶️ 你悄然潜入【{tname}】洞府的{slot_msg}，顺走了一批灵材。\n"
            f"获得：{'、'.join(rewards)}"
            f"{delay_msg}{guard_msg}\n"
            f"该洞府今日剩余可被潜入次数：{left}"
        )

    await handle_send(bot, event, msg)
