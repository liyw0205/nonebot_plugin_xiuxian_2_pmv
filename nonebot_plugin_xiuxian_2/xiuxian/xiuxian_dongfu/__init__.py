try:
    import ujson as json
except ImportError:
    import json

import random
from datetime import datetime, timedelta
from nonebot import on_command
from nonebot.params import CommandArg

from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, handle_send, number_to
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from ..xiuxian_utils.item_json import Items

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()
items = Items()

MAP_TABLE = "map_status"
DONGFU_TABLE = "dongfu_status"

# 与地图商店统一（补 price；保留 21002）
SEED_CONFIG = {
    21001: {"name": "青灵草种", "price": 500000, "pool": "herb_low", "minutes": 60},
    21002: {"name": "玄木灵种", "price": 3000000, "pool": "herb_mid", "minutes": 180},
    21003: {"name": "星砂神种", "price": 15000000, "pool": "god_low", "minutes": 360},
    21004: {"name": "混元神种", "price": 80000000, "pool": "god_high", "minutes": 720},
}

HERB_LOW = list(range(3001, 3037))
HERB_MID = list(range(3037, 3109))
GOD_LOW = list(range(15000, 15010))
GOD_HIGH = list(range(15010, 15016))

ARRAY_LEVEL_MAX = 10
VISIT_STAMINA = 5

my_dongfu = on_command("我的洞府", priority=8, block=True)
dongfu_plant = on_command("洞府种植", priority=8, block=True)
visit_friend = on_command("拜访道友", priority=8, block=True)
dongfu_array = on_command("洞府布阵", priority=8, block=True)
dongfu_help = on_command("洞府帮助", priority=8, block=True)


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


def _default_dongfu():
    return {
        "built": 0,
        "realm": "",
        "heaven": "",
        "node_id": "",
        "node_name": "",
        "array_level": 0,
        "planting": 0,
        "plant_seed_id": 0,
        "plant_start": "",
        "plant_finish": "",
    }


def _get_dongfu(uid: str):
    d = player_data_manager.get_fields(str(uid), DONGFU_TABLE)
    if not d:
        d = _default_dongfu()
        for k, v in d.items():
            player_data_manager.update_or_write_data(str(uid), DONGFU_TABLE, k, v)
    else:
        default = _default_dongfu()
        for k, v in default.items():
            if k not in d:
                d[k] = v
                player_data_manager.update_or_write_data(str(uid), DONGFU_TABLE, k, v)
    return d


def _save_dongfu(uid: str, d: dict):
    for k, v in d.items():
        player_data_manager.update_or_write_data(str(uid), DONGFU_TABLE, k, v)


def _has_dongfu(uid: str):
    d = _get_dongfu(uid)
    return int(d.get("built", 0)) == 1, d


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
    # 基础掉落数量 1~2
    base_num = random.randint(1, 2)

    # 阵法加成：2级开始有概率 +1
    bonus = 1 if (array_lv >= 2 and random.random() < min(0.8, array_lv * 0.08)) else 0
    total_num = base_num + bonus

    result = []
    for _ in range(total_num):
        gid = random.choice(pool)
        item = items.get_data_by_item_id(gid)
        if item:
            result.append((gid, item["name"], item.get("type", "药材"), 1))
    return result


@dongfu_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = (
        "【洞府帮助】\n"
        "1. 我的洞府：查看洞府信息\n"
        "2. 洞府种植 [种子名]\n"
        "3. 拜访道友 道号\n"
        "4. 洞府布阵\n"
        "注：种子可在地图节点种子商店购买。"
    )
    await handle_send(bot, event, msg)


@my_dongfu.handle(parameterless=[Cooldown(cd_time=1.4)])
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

    planting = int(d.get("planting", 0))
    if planting == 1:
        finish = _parse_dt(d.get("plant_finish", ""))
        if finish:
            remain = max(1, int((finish - _now()).total_seconds() // 60))
            pmsg = f"种植中（预计剩余{remain}分钟）"
        else:
            pmsg = "种植中"
    else:
        pmsg = "空闲"

    msg = (
        f"【我的洞府】\n"
        f"位置：{d.get('realm')}·{d.get('heaven')}·{d.get('node_name')}\n"
        f"阵法等级：{d.get('array_level', 0)}\n"
        f"种植状态：{pmsg}"
    )
    await handle_send(bot, event, msg)


@dongfu_plant.handle(parameterless=[Cooldown(cd_time=1.4, stamina_cost=2)])
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

    # 收获
    if int(d.get("planting", 0)) == 1:
        finish = _parse_dt(d.get("plant_finish", ""))
        if finish and now >= finish:
            seed_id = int(d.get("plant_seed_id", 0))
            array_lv = int(d.get("array_level", 0))
            drops = _roll_harvest(seed_id, array_lv)

            d["planting"] = 0
            d["plant_seed_id"] = 0
            d["plant_start"] = ""
            d["plant_finish"] = ""
            _save_dongfu(uid, d)

            if not drops:
                await handle_send(bot, event, "收获失败，灵田无产出。")
                return

            lines = ["洞府收获完成："]
            for gid, name, tp, num in drops:
                sql_message.send_back(uid, gid, name, tp, num, 1)
                lines.append(f"- {name} x{num}")
            await handle_send(bot, event, "\n".join(lines))
            return
        else:
            remain = max(1, int((finish - now).total_seconds() // 60)) if finish else 1
            await handle_send(bot, event, f"灵田仍在生长中，约剩余{remain}分钟。")
            return

    # 播种
    seed_name = args.extract_plain_text().strip()
    if not seed_name:
        await handle_send(bot, event, "请指定种子名，例如：洞府种植 青灵草种")
        return

    seed_id = None
    for sid, conf in SEED_CONFIG.items():
        if conf["name"] == seed_name:
            seed_id = sid
            break
    if seed_id is None:
        await handle_send(bot, event, f"未识别种子【{seed_name}】。")
        return

    have = sql_message.goods_num(uid, seed_id)
    if int(have) <= 0:
        await handle_send(bot, event, f"你没有【{seed_name}】，请先去种子商店购买。")
        return

    sql_message.update_back_j(uid, seed_id, 1)

    array_lv = int(d.get("array_level", 0))
    base_minutes = int(SEED_CONFIG[seed_id]["minutes"])
    speed = 1 + array_lv * 0.05
    real_minutes = max(10, int(base_minutes / speed))

    d["planting"] = 1
    d["plant_seed_id"] = seed_id
    d["plant_start"] = _fmt_dt(now)
    d["plant_finish"] = _fmt_dt(now + timedelta(minutes=real_minutes))
    _save_dongfu(uid, d)

    await handle_send(bot, event, f"已播种【{seed_name}】，预计{real_minutes}分钟后可收获。")


@visit_friend.handle(parameterless=[Cooldown(cd_time=1.4, stamina_cost=VISIT_STAMINA)])
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
    if int(td.get("built", 0)) != 1:
        await handle_send(bot, event, f"{tname}尚未建设洞府。")
        return

    gain = random.randint(10000, 50000)
    sql_message.update_ls(uid, gain, 1)
    await handle_send(bot, event, f"你拜访了【{tname}】的洞府（{td.get('node_name')}），获得灵石{number_to(gain)}。")


@dongfu_array.handle(parameterless=[Cooldown(cd_time=1.4)])
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

    lv = int(d.get("array_level", 0))
    if lv >= ARRAY_LEVEL_MAX:
        await handle_send(bot, event, "阵法已达满级。")
        return

    cost = 3000000 * (lv + 1)
    if int(user_info.get("stone", 0)) < cost:
        await handle_send(bot, event, f"升级阵法需要{number_to(cost)}灵石。")
        return

    sql_message.update_ls(uid, cost, 2)
    lv += 1
    d["array_level"] = lv
    _save_dongfu(uid, d)
    await handle_send(bot, event, f"洞府布阵成功，当前阵法等级：{lv}")