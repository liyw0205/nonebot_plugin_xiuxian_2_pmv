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

my_dongfu = on_command("我的洞府", priority=8, block=True)
dongfu_plant = on_command("洞府种植", priority=8, block=True)
visit_friend = on_command("拜访道友", priority=8, block=True)
dongfu_array = on_command("洞府布阵", priority=8, block=True)
dongfu_help = on_command("洞府帮助", priority=8, block=True)
infiltrate_dongfu = on_command("潜入洞府", priority=8, block=True)


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
        "intrude_date": "",
        "intrude_count": 0,
    }


def _get_dongfu(uid: str):
    d = player_data_manager.get_fields(str(uid), DONGFU_TABLE)
    default = _default_dongfu()

    if not d:
        d = default.copy()
        for k, v in d.items():
            player_data_manager.update_or_write_data(str(uid), DONGFU_TABLE, k, v)
    else:
        for k, v in default.items():
            if k not in d or d.get(k) is None:
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
    d["intrude_count"] = int(d.get("intrude_count", 0)) + 1
    _save_dongfu(target_uid, d)
    return d["intrude_count"]


def _can_intrude(target_uid: str):
    d = _get_dongfu(target_uid)
    d = _reset_intrude_count_if_needed(d)
    _save_dongfu(target_uid, d)
    return int(d.get("intrude_count", 0)) < INFILTRATE_DAILY_LIMIT, d


@dongfu_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = (
        "【洞府帮助】\n"
        "1. 我的洞府：查看洞府信息\n"
        "2. 洞府种植 [种子名]\n"
        "3. 拜访道友 道号\n"
        "4. 洞府布阵\n"
        "5. 潜入洞府 道号\n"
        "注：\n"
        "- 洞府种植再次输入可收获\n"
        "- 潜入只能对附近已建设洞府的道友使用\n"
        f"- 每个洞府每日最多被潜入{INFILTRATE_DAILY_LIMIT}次"
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

    d = _reset_intrude_count_if_needed(d)
    intrude_left = INFILTRATE_DAILY_LIMIT - int(d.get("intrude_count", 0))
    _save_dongfu(uid, d)

    msg = (
        f"【我的洞府】\n"
        f"位置：{d.get('realm')}·{d.get('heaven')}·{d.get('node_name')}\n"
        f"阵法等级：{d.get('array_level', 0)}\n"
        f"种植状态：{pmsg}\n"
        f"今日剩余可被潜入intr    msgdong_cost, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
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
    speed = 1 + array_lv * 0.04
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


@infiltrate_dongfu.handle(parameterless=[Cooldown(cd_time=1.8, stamina_cost=INFILTRATE_STAMINA)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    潜入洞府 道号
    规则：
    1. 目标必须在附近（同节点）
    2. 目标必须有洞府且正在种植
    3. 每个洞府每天最多被潜入3次
    4. 阵法等级越高越容易发现
    5. 成功只偷部分材料，不偷全部
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    ok, user_info, m = check_user(event)
    if not ok:
        await handle_send(bot, event, m, md_type="我要修仙")
        return

    my_uid = str(user_info["user_id"])
    tname = args.extract_plain_text().strip()
    if not tname:
        await handle_send(bot, event, "请使用：潜入洞府 道号")
        return

    nearby_users = _get_same_node_users(my_uid)
    target = next((u for u in nearby_users if u["user_name"] == tname), None)
    if not target:
        await handle_send(bot, event, f"附近未找到道友【{tname}】。潜入只能针对同一地点附近的洞府。")
        return

    target_uid = str(target["user_id"])
    if target_uid == my_uid:
        await handle_send(bot, event, "你不能潜入自己的洞府。")
        return

    has, td = _has_dongfu(target_uid)
    if not has:
        await handle_send(bot, event, f"{tname}尚未建设洞府。")
        return

    if int(td.get("planting", 0)) != 1:
        await handle_send(bot, event, f"{tname}的洞府当前没有可图谋的灵田。")
        return

    can_intrude, td = _can_intrude(target_uid)
    if not can_intrude:
        await handle_send(bot, event, f"{tname}的洞府今日已被盯上太多次，暂时无法再潜入。")
        return

    finish = _parse_dt(td.get("plant_finish", ""))
    if not finish:
        await handle_send(bot, event, f"{tname}的灵田状态异常，潜入失败。")
        return

    seed_id = int(td.get("plant_seed_id", 0))
    if seed_id not in SEED_CONFIG:
        await handle_send(bot, event, "目标洞府种植数据异常。")
        return

    # 先占用今日被潜入次数
    current_intrude = _consume_intrude_count(target_uid)

    now = _now()
    array_lv = int(td.get("array_level", 0))

    # 阵法发现概率：基础20%，每级+6%，最高80%
    detect_rate = min(0.80, 0.20 + array_lv * 0.06)

    # 若作物已成熟，偷取率更高；未成熟则偷苗/偷灵机，收益更少
    matured = now >= finish

    if matured:
        # 成熟时：可偷部分成品
        success_rate = max(0.20, 0.72 - array_lv * 0.04)
    else:
        # 未成熟时：更难偷到，只能窃取少量“灵机”
        success_rate = max(0.12, 0.45 - array_lv * 0.03)

    detected = random.random() < detect_rate
    success = random.random() < success_rate

    # 被发现后的惩罚
    if detected and not success:
        loss_stone = random.randint(50000, 200000) * max(1, array_lv)
        sql_message.update_ls(my_uid, loss_stone, 2)
        left = max(0, INFILTRATE_DAILY_LIMIT - current_intrude)
        await handle_send(
            bot,
            event,
            f"❌ 你潜入【{tname}】洞府时触发阵法警示，被当场逼退！\n"
            f"对方阵法等级：{array_lv}\n"
            f"损失灵石：{number_to(loss_stone)}\n"
            f"该洞府今日剩余可被潜入次数：{left}"
        )
        return

    if detected and success:
        # 成功偷到但被发现，收益打折
        stealth_penalty = 0.6
    else:
        stealth_penalty = 1.0

    rewards = []

    if matured:
        # 模拟偷取最终收获的一部分，不清空对方作物
        all_drops = _roll_harvest(seed_id, array_lv)
        if not all_drops:
            left = max(0, INFILTRATE_DAILY_LIMIT - current_intrude)
            await handle_send(
                bot,
                event,
                f"你潜入了【{tname}】的洞府，但灵田气息微弱，未能偷到任何东西。\n"
                f"该洞府今日剩余可被潜入次数：{left}"
            )
            return

        # 只偷其中一部分：至少1个，至多一半
        take_n = max(1, min(len(all_drops), int(len(all_drops) * 0.5)))
        random.shuffle(all_drops)
        steal_drops = all_drops[:take_n]

        # 若被发现仍成功，则再砍一半
        if stealth_penalty < 1.0 and len(steal_drops) > 1:
            steal_drops = steal_drops[:max(1, len(steal_drops) // 2)]

        for gid, name, tp, num in steal_drops:
            sql_message.send_back(my_uid, gid, name, tp, num, 1)
            rewards.append(f"{name} x{num}")

    else:
        # 未成熟：偷“灵机” => 少量低档材料/灵石
        temp_rewards = []
        conf = SEED_CONFIG.get(seed_id, {})
        pool_name = conf.get("pool", "herb_low")

        if pool_name == "herb_low":
            pool = HERB_LOW[:8]
        elif pool_name == "herb_mid":
            pool = HERB_MID[:8]
        elif pool_name == "god_low":
            # 未成熟神种不给高神物，只给低档替代资源
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
                temp_rewards.append(f"{item['name']} x1")

        # 附带少量灵石
        ls_gain = int(random.randint(30000, 80000) * stealth_penalty)
        if ls_gain > 0:
            sql_message.update_ls(my_uid, ls_gain, 1)
            temp_rewards.append(f"灵石 x{number_to(ls_gain)}")

        rewards = temp_rewards

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

    if detected:
        msg = (
            f"⚠️ 你潜入【{tname}】洞府时触动阵纹，但仍抢先带走部分灵材！\n"
            f"获得：{'、'.join(rewards)}\n"
            f"该洞府今日剩余可被潜入次数：{left}"
        )
    else:
        msg = (
            f"🕶️ 你悄然潜入【{tname}】的洞府，顺走了一批灵材。\n"
            f"获得：{'、'.join(rewards)}\n"
            f"该洞府今日剩余可被潜入次数：{left}"
        )

    await handle_send(bot, event, msg)