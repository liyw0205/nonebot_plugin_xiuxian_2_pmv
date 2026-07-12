import random
import re
import time
from datetime import datetime
from ..on_compat import on_command
from nonebot.params import CommandArg
from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, number_to, send_help_message
from ..xiuxian_utils.sect_utils import get_user_sect_fairyland_level as _get_user_sect_fairyland_level
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.item_json import Items
from ..xiuxian_world_events import get_spirit_vein_tianti_bonus_msg
from .tianti_data import (
    TiantiDataManager,
    get_tianti_level_data,
    get_next_tianti_level_name,
    get_tianti_level_index,
    get_opened_qiaoxue_count,
    get_qiaoxue_pool,
    get_qiaoxue_map,
)
from .tianti_service import (
    calc_tianti_gain_rate,
    get_active_medicine_bath,
    get_sect_fairyland_bonus,
    get_tianti_cap,
    settle_tianti_gain,
)
from .stone_training_service import StoneTrainingService
from .medicine_bath_service import MedicineBathService
from .breakthrough_service import TiantiBreakthroughService
from ...paths import get_paths

sql_message = XiuxianDateManage()
tianti_manager = TiantiDataManager()
stone_training_service = StoneTrainingService(get_paths().game_db, get_paths().player_db)
medicine_bath_service = MedicineBathService(get_paths().game_db, get_paths().player_db)
tianti_breakthrough_service = TiantiBreakthroughService(get_paths().player_db)
items = Items()

tianti_help = on_command("炼体帮助", priority=10, block=True)
tianti_settle = on_command("炼体结算", priority=10, block=True)
tianti_stone = on_command("灵石炼体", priority=10, block=True)
tianti_break = on_command("炼体突破", priority=10, block=True)
tianti_info = on_command("我的炼体", aliases={"炼体状态"}, priority=10, block=True)
tianti_chongqiao = on_command("冲窍", priority=10, block=True)
tiqiao_info = on_command("我的体窍", priority=10, block=True)
tianti_level_help = on_command("炼体境界", priority=10, block=True)
tianti_medicine_bath = on_command("炼体药浴", aliases={"药浴"}, priority=10, block=True)

MEDICINE_BATH_DURATION_MINUTES = 360
MEDICINE_BATH_UNIT_EFFECT = 1 / 30
MEDICINE_BATH_FULL_EFFECT_UNITS = 15
MEDICINE_BATH_MAX_UNITS = 30
MEDICINE_BATH_MAX_EFFECT = 2.0
MEDICINE_BATH_TIME_CONFIG = [
    {
        "name": "子卯阴息",
        "start": 23,
        "end": 5,
        "range": (1.85, 2.00),
        "herbs": {"夜交藤", "鬼臼草", "腐骨灵花", "渊血冥花", "阴阳黄泉花", "厉魂血珀", "炼魂珠", "绝魂草", "冥胎骨", "鬼面花"},
    },
    {
        "name": "卯午生机",
        "start": 5,
        "end": 11,
        "range": (1.65, 1.85),
        "herbs": {"恒心草", "天青花", "九叶芝", "玉髓芝", "天蝉灵叶", "三叶青芝", "木灵三针花", "檀芒九叶花", "森檀木", "太清玄灵草"},
    },
    {
        "name": "午酉血火",
        "start": 11,
        "end": 17,
        "range": (1.75, 1.95),
        "herbs": {"血莲精", "鸡冠草", "地心火芝", "冰灵焰草", "地心淬灵乳", "离火梧桐芝", "火精枣", "血菩提", "重元换血草", "凤血果"},
    },
    {
        "name": "酉子玄凝",
        "start": 17,
        "end": 23,
        "range": (1.70, 1.90),
        "herbs": {"银精芝", "雪玉骨参", "八角玄冰草", "坎水玄冰果", "浩淼水藤", "玄冰花", "冰灵果", "冰精芝", "月灵花", "太乙碧莹花"},
    },
]


def _get_tianti_cap(data: dict) -> int:
    return get_tianti_cap(data)


def _get_active_medicine_bath(data: dict, now_t: datetime):
    return get_active_medicine_bath(data, now_t)


def _medicine_bath_slot(hour: int):
    for conf in MEDICINE_BATH_TIME_CONFIG:
        start = conf["start"]
        end = conf["end"]
        if start <= end:
            if start <= hour < end:
                return conf
        else:
            if hour >= start or hour < end:
                return conf
    return MEDICINE_BATH_TIME_CONFIG[0]


def _parse_medicine_bath_items(text: str):
    tokens = [token for token in re.split(r"[\s,，、;；]+", text.strip()) if token]
    result = []
    idx = 0
    while idx < len(tokens):
        token = tokens[idx]
        amount = 1
        match = re.match(r"^(.+?)[xX*×](\d+)$", token)
        if match:
            herb_name = match.group(1).strip()
            amount = int(match.group(2))
        else:
            herb_name = token.strip()
            if idx + 1 < len(tokens) and tokens[idx + 1].isdigit():
                amount = int(tokens[idx + 1])
                idx += 1

        if herb_name and amount > 0:
            result.append((herb_name, amount))
        idx += 1
    return result


def _medicine_bath_effect(units: int):
    valid_units = max(0, min(int(units), MEDICINE_BATH_MAX_UNITS))
    return round(min(MEDICINE_BATH_MAX_EFFECT, 1 + valid_units * MEDICINE_BATH_UNIT_EFFECT), 4)


def _format_medicine_bath_plan(plan, limit: int = 6):
    names = [f"{item['name']}x{item['amount']}" for item in plan]
    if len(names) > limit:
        return "、".join(names[:limit]) + f"等{len(names)}种"
    return "、".join(names)


def _format_medicine_bath_percent(effect: float):
    return f"{effect * 100:.2f}".rstrip("0").rstrip(".")


def _settle_tianti_gain(data: dict, now_t: datetime, sect_fairyland_level: int = 0):
    return settle_tianti_gain(data, now_t, sect_fairyland_level)


def _format_sect_fairyland_msg(result: dict) -> str:
    sect_bonus = float(result.get("sect_bonus", 0) or 0)
    if sect_bonus <= 0:
        return ""
    return f"\n宗门炼体堂加成：{sect_bonus * 100:.0f}%"


def _format_spirit_vein_msg(result: dict) -> str:
    if float(result.get("spirit_vein_bonus", 0) or 0) <= 0:
        return ""
    return get_spirit_vein_tianti_bonus_msg()


def _get_qiaoxue_unlock_limit(data: dict) -> int:
    """
    冲窍额度规则：
    每个炼体小境界解锁 1 个可开窍数量
    例如 rank=15，则累计最多可开 15 个窍穴
    """
    return int(min(get_tianti_level_data(data["tianti_level"])["rank"] * 3, 108))


@tianti_help.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = """**炼体系统帮助**
---
**指令**
- 炼体结算：按分钟结算炼体气血
- 灵石炼体 数量：消耗灵石换炼体气血（1灵石=0.1炼体气血）
- 炼体突破：满足修仙境界+炼体气血后突破
- 冲窍：消耗当前10%炼体气血，随机冲击一个未开的窍穴
- 炼体药浴 药材x数量 药材x数量：按当前时段消耗多份药材获得360分钟炼体结算加成
- 使用炼体神物：按神物提供的分钟数获得炼体结算气血
- 我的体窍 [窍穴名/天罡/地煞]：查看体窍总览/分组/单个窍穴
- 我的炼体：查看当前炼体状态
- 炼体境界：查看炼体境界表

**冲窍说明**
> 每提升1个炼体小境界，累计解锁1个可开窍数量。
"""
    await send_help_message(
        bot, event, msg,
        k1="结算", v1="炼体结算",
        k2="突破", v2="炼体突破",
        k3="状态", v3="我的炼体"
    )


@tianti_settle.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    data = tianti_manager.get_user_tianti_info(user_id)
    now_t = datetime.now()

    sect_fairyland_level = _get_user_sect_fairyland_level(user_info)
    result = _settle_tianti_gain(data, now_t, sect_fairyland_level)
    tianti_manager.save_user_tianti_info(user_id, data)
    if result["status"] == "init":
        await handle_send(bot, event, "已初始化炼体计时，请稍后再来结算。")
        return
    if result["status"] == "empty":
        await handle_send(bot, event, "时间太短，暂无可结算炼体收益。")
        return

    bath_msg = ""
    if result.get("bath"):
        bath = result["bath"]
        bath_msg = (
            f"\n药浴加成：{bath['name']}，本次按{_format_medicine_bath_percent(bath['effect'])}%结算"
            f"（有效至{bath['end_time'].strftime('%H:%M')}）"
        )
    elif result.get("bath_expired"):
        bath_msg = "\n药浴已超过360分钟，本次未获得药浴加成。"

    await handle_send(
        bot, event,
        f"炼体结算完成，间隔{result['mins']}分钟。\n"
        f"本次获得炼体气血：{number_to(result['real_gain'])}\n"
        f"当前炼体气血：{number_to(result['new_hp'])}"
        f"{bath_msg}"
        f"{_format_sect_fairyland_msg(result)}"
        f"{_format_spirit_vein_msg(result)}"
    )


@tianti_stone.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    txt = args.extract_plain_text().strip()
    if not txt.isdigit():
        await handle_send(bot, event, "用法：灵石炼体 灵石数量")
        return

    stone_cost = int(txt)
    if stone_cost <= 9:
        await handle_send(bot, event, "请输入大于10的灵石数量。")
        return
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = (
        f"tianti-stone:{event_id}:{user_id}" if event_id
        else f"tianti-stone:{user_id}:{time.time_ns()}"
    )
    result = stone_training_service.train(operation_id, user_id, stone_cost)
    if result.status == "at_cap":
        await handle_send(bot, event, "已达当前炼体境界上限，无法继续灵石炼体。")
        return
    if result.status in {"stone_insufficient", "stone_changed"}:
        await handle_send(bot, event, "你的灵石不足或余额已经变化。")
        return
    if not result.succeeded:
        raise RuntimeError(f"unexpected tianti stone training status: {result.status}")

    await handle_send(
        bot, event,
        f"灵石炼体完成：消耗灵石{number_to(result.stone_cost)}，获得炼体气血{number_to(result.hp_gain)}。"
    )


@tianti_medicine_bath.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    now_t = datetime.now()
    raw_text = args.extract_plain_text().strip()
    slot = _medicine_bath_slot(now_t.hour)

    if not raw_text:
        herbs = "、".join(sorted(slot["herbs"])[:10])
        await handle_send(
            bot,
            event,
            f"【炼体药浴】\n"
            f"当前时段：{slot['name']}\n"
            f"本时段有效药材：{herbs}\n"
            f"药材效果：每1份有效药材+{MEDICINE_BATH_UNIT_EFFECT * 100:.2f}%结算效果，"
            f"{MEDICINE_BATH_FULL_EFFECT_UNITS}份达到150%，{MEDICINE_BATH_MAX_UNITS}份达到200%封顶\n"
            f"其他时段药材：无效果，不消耗\n"
            f"持续时间：{MEDICINE_BATH_DURATION_MINUTES}分钟\n"
            f"用法：炼体药浴 恒心草x20 天青花x20"
        )
        return

    requested_items = _parse_medicine_bath_items(raw_text)
    if not requested_items:
        await handle_send(bot, event, "用法：炼体药浴 恒心草x20 天青花x20")
        return

    invalid_items = []
    no_effect_items = []
    valid_requests = []
    valid_requested_amount = 0
    for herb_name, amount in requested_items:
        item_id, item_info = items.get_data_by_item_name(herb_name)
        if not item_id or not item_info or item_info.get("item_type") != "药材":
            invalid_items.append(f"{herb_name}x{amount}")
            continue
        item_name = item_info["name"]
        if item_name not in slot["herbs"]:
            no_effect_items.append(f"{item_name}x{amount}")
            continue
        valid_requests.append({
            "item_id": item_id,
            "name": item_name,
            "amount": amount,
        })
        valid_requested_amount += amount

    if invalid_items:
        await handle_send(
            bot,
            event,
            f"药浴只能使用药材，以下物品不存在或不是药材：{'、'.join(invalid_items)}"
        )
        return

    if not valid_requests:
        herbs = "、".join(sorted(slot["herbs"])[:10])
        msg = (
            f"当前时段【{slot['name']}】只有这些药材有效：{herbs}\n"
            f"你放入的药材当前时段无效，未消耗。"
        )
        if no_effect_items:
            msg += f"\n无效药材：{'、'.join(no_effect_items)}"
        await handle_send(bot, event, msg)
        return

    consume_plan = []
    plan_by_id = {}
    remaining_units = MEDICINE_BATH_MAX_UNITS
    for request in valid_requests:
        if remaining_units <= 0:
            break
        amount = min(request["amount"], remaining_units)
        if amount <= 0:
            continue
        item_id = request["item_id"]
        if item_id not in plan_by_id:
            plan_by_id[item_id] = {
                "item_id": item_id,
                "name": request["name"],
                "amount": 0,
            }
            consume_plan.append(plan_by_id[item_id])
        plan_by_id[item_id]["amount"] += amount
        remaining_units -= amount

    consume_units = sum(item["amount"] for item in consume_plan)
    if consume_units <= 0:
        await handle_send(bot, event, "有效药材数量不足，无法开启药浴。")
        return

    sect_fairyland_level = _get_user_sect_fairyland_level(user_info)
    effect = _medicine_bath_effect(consume_units)
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = (
        f"tianti-bath:{event_id}:{user_id}" if event_id
        else f"tianti-bath:{user_id}:{time.time_ns()}"
    )
    result = medicine_bath_service.apply(
        operation_id, user_id, consume_plan, effect, slot["name"], now_t,
        MEDICINE_BATH_DURATION_MINUTES, sect_fairyland_level=sect_fairyland_level,
    )
    if result.status == "bath_active":
        await handle_send(bot, event, "当前药浴仍在生效，药浴结束后再使用新的药材。")
        return
    if result.status in {"item_insufficient", "item_changed"}:
        detail = "\n".join(
            f"{item['name']}需要{item['amount']}份，现有{item['have']}份"
            for item in result.insufficient
        )
        await handle_send(bot, event, "药材不足或库存已经变化。" + (f"\n{detail}" if detail else ""))
        return
    if not result.succeeded:
        raise RuntimeError(f"unexpected medicine bath status: {result.status}")

    pre_result = result.settlement

    pre_msg = ""
    if pre_result["status"] == "ok" and pre_result.get("real_gain", 0) > 0:
        pre_msg = f"\n药浴前已自动结算炼体气血：{number_to(pre_result['real_gain'])}"
        pre_msg += _format_sect_fairyland_msg(pre_result)

    ignored_msgs = []
    ignored_amount = max(0, valid_requested_amount - consume_units)
    if ignored_amount:
        ignored_msgs.append(f"超过上限的有效药材未消耗：{ignored_amount}份")
    if no_effect_items:
        ignored_msgs.append(f"当前时段无效未消耗：{'、'.join(no_effect_items)}")
    ignored_msg = "\n" + "\n".join(ignored_msgs) if ignored_msgs else ""

    await handle_send(
        bot,
        event,
        f"药浴开启成功！\n"
        f"时段：{slot['name']}\n"
        f"消耗有效药材：{_format_medicine_bath_plan(consume_plan)}，共{consume_units}份\n"
        f"炼体结算效果：{_format_medicine_bath_percent(effect)}%\n"
        f"持续时间：{MEDICINE_BATH_DURATION_MINUTES}分钟\n"
        f"有效至：{result.end_time}"
        f"{ignored_msg}"
        f"{pre_msg}"
    )


@tianti_break.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    data = tianti_manager.get_user_tianti_info(user_id)
    next_name = get_next_tianti_level_name(data["tianti_level"])
    if not next_name:
        await handle_send(bot, event, "你的炼体已达最高境界。")
        return
    next_cfg = get_tianti_level_data(next_name)
    min_xx = next_cfg["min_xx_level"]
    user_xx_rank = get_tianti_level_index(user_info["level"], is_xiuxian=True)
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = (
        f"tianti-break:{event_id}:{user_id}" if event_id
        else f"tianti-break:{user_id}:{time.time_ns()}"
    )
    result = tianti_breakthrough_service.attempt(
        operation_id, user_id, cultivation_rank=user_xx_rank,
        roll_success=random.random() < 0.5,
    )
    if result.status == "max_level":
        await handle_send(bot, event, "你的炼体已达最高境界。")
        return
    if result.status == "cultivation_insufficient":
        await handle_send(bot, event, f"突破失败：修仙境界不足，需达到【{min_xx}】。")
        return
    if result.status == "hp_insufficient":
        await handle_send(bot, event, f"突破失败：炼体气血不足，需{number_to(int(next_cfg['need_hp']))}。")
        return
    if not result.succeeded:
        raise RuntimeError(f"unexpected tianti breakthrough status: {result.status}")
    if result.success:
        await handle_send(
            bot, event,
            f"炼体突破成功！当前境界：{result.new_level}\n"
            f"本次消耗炼体气血：{number_to(result.hp_cost)}"
        )
    else:
        await handle_send(
            bot, event,
            f"炼体突破失败！\n"
            f"本次消耗炼体气血：{number_to(result.hp_cost)}"
        )


@tianti_info.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    data = tianti_manager.get_user_tianti_info(user_id)
    lvl = data["tianti_level"]
    hp = int(data["tianti_hp"])
    opened = get_opened_qiaoxue_count(data)
    unlock_limit = _get_qiaoxue_unlock_limit(data)

    next_name = get_next_tianti_level_name(lvl)
    if next_name:
        need_hp = int(get_tianti_level_data(next_name)["need_hp"])
        remain = max(0, need_hp - hp)
        cap = _get_tianti_cap(data)
        brk = (
            f"下一境界：{next_name}\n"
            f"突破需求：{number_to(need_hp)}（还需{number_to(remain)}）\n"
            f"当前境界气血上限：{number_to(cap)}"
        )
    else:
        brk = "已达炼体最高境界"

    now_t = datetime.now()
    bath = _get_active_medicine_bath(data, now_t)
    sect_fairyland_level = _get_user_sect_fairyland_level(user_info)
    sect_bonus = get_sect_fairyland_bonus(sect_fairyland_level)
    rate_info = calc_tianti_gain_rate(data, now_t, sect_fairyland_level)
    efficiency_percent = int(rate_info["efficiency"] * 100)
    if bath:
        bath_msg = (
            f"\n药浴：{bath['name']}，结算效果{_format_medicine_bath_percent(bath['effect'])}%"
            f"（有效至{bath['end_time'].strftime('%H:%M')}）"
        )
    else:
        bath_msg = "\n药浴：无"
    sect_msg = f"\n宗门炼体堂：{sect_fairyland_level}级（炼体气血+{sect_bonus * 100:.0f}%）" if sect_bonus > 0 else "\n宗门炼体堂：无加成"
    spirit_vein_msg = _format_spirit_vein_msg(rate_info)

    await handle_send(
        bot, event,
        f"【我的炼体】\n"
        f"境界：{lvl}\n"
        f"炼体气血：{number_to(hp)}\n"
        f"当前效率：{efficiency_percent}%\n"
        f"每分钟气血：{number_to(rate_info['per_min'])}\n"
        f"已开窍：{opened}/108\n"
        f"当前可开上限：{unlock_limit}/108\n"
        f"{brk}"
        f"{bath_msg}"
        f"{sect_msg}"
        f"{spirit_vein_msg}"
    )


@tianti_chongqiao.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    冲窍
    修正逻辑：
    - 每个炼体小境界解锁 1 个可开窍数量
    - 解锁的是“累计可开启总数”
    - 不是“当前境界/当前大境界只能冲一次”
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    data = tianti_manager.get_user_tianti_info(user_id)

    opened = data.get("opened_qiaoxue", [])
    opened_count = len(opened)
    unlock_limit = _get_qiaoxue_unlock_limit(data)
    pool = get_qiaoxue_pool()

    # 按当前炼体境界 rank 判断累计可冲窍数量
    if opened_count >= unlock_limit:
        await handle_send(
            bot,
            event,
            f"你当前境界【{data['tianti_level']}】累计最多可开 {unlock_limit} 个窍穴，"
            f"你已开启 {opened_count} 个。请继续突破炼体境界后再来冲窍。"
        )
        return

    unopen = [q for q in pool if q["name"] not in set(opened)]
    if not unopen:
        await handle_send(bot, event, "你已开满所有窍穴，无可再冲。")
        return

    cur_hp = int(data["tianti_hp"])
    cost = max(1, int(cur_hp * 0.1))
    if cur_hp < cost:
        await handle_send(bot, event, "炼体气血不足，无法冲窍。")
        return

    chosen = random.choice(unopen)
    real_val = float(chosen["effect_value"])

    detail_list = data.get("opened_qiaoxue_detail", [])
    detail_list.append({
        "name": chosen["name"],
        "group": chosen["group"],
        "effect_type": chosen["effect_type"],
        "effect_value": real_val
    })

    data["tianti_hp"] = max(0, cur_hp - cost)
    data["opened_qiaoxue"] = opened + [chosen["name"]]
    data["opened_qiaoxue_detail"] = detail_list

    # 兼容保留旧字段，但不再作为限制依据
    if "qiaoxue_stage_opened" not in data or not isinstance(data["qiaoxue_stage_opened"], dict):
        data["qiaoxue_stage_opened"] = {}

    tianti_manager.save_user_tianti_info(user_id, data)
    effect_cn = _effect_type_cn(chosen["effect_type"])

    await handle_send(
        bot, event,
        f"冲窍成功！\n"
        f"消耗炼体气血：{number_to(cost)}\n"
        f"新开窍穴：{chosen['name']}\n"
        f"效果：{effect_cn} +{real_val * 100:.2f}%\n"
        f"已开窍数：{opened_count + 1}/108\n"
        f"当前境界可开上限：{unlock_limit}/108"
    )


def _effect_type_cn(effect_type: str) -> str:
    """
    将窍穴效果类型转为中文描述
    """
    mapping = {
        "base_per_min_ratio": "每分钟气血",
        "hp_gain_pct": "获得气血",
    }
    return mapping.get(effect_type, effect_type)


@tiqiao_info.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    data = tianti_manager.get_user_tianti_info(user_id)
    opened = set(data.get("opened_qiaoxue", []))
    qpool = get_qiaoxue_pool()
    qmap = get_qiaoxue_map()
    arg = args.extract_plain_text().strip()

    # 计算总加成
    base_ratio = 0.0
    gain_pct = 0.0
    detail_list = data.get("opened_qiaoxue_detail", [])
    for q in detail_list:
        et = q.get("effect_type")
        ev = float(q.get("effect_value", 0))
        if et == "base_per_min_ratio":
            base_ratio += ev
        elif et == "hp_gain_pct":
            gain_pct += ev

    # 无参数：总览
    if not arg:
        opened_tg = len([q for q in qpool if q.get("group") == "天罡" and q.get("name") in opened])
        opened_ds = len([q for q in qpool if q.get("group") == "地煞" and q.get("name") in opened])
        unlock_limit = _get_qiaoxue_unlock_limit(data)

        await handle_send(
            bot, event,
            f"【我的体窍】\n"
            f"已开窍：{len(opened)}/108（当前可开上限：{unlock_limit}/108）\n"
            f"天罡：{opened_tg}/36\n"
            f"地煞：{opened_ds}/72\n"
            f"每分钟气血：{base_ratio * 100:.2f}%\n"
            f"获得气血：{gain_pct * 100:.2f}%"
        )
        return

    # 参数=天罡/地煞：列出该组窍穴
    if arg in ("天罡", "地煞"):
        group_list = [q for q in qpool if q.get("group") == arg]
        group_list.sort(key=lambda q: (q.get("name") not in opened, q.get("name", "")))

        lines = [f"【{arg}窍穴】"]
        opened_count = 0
        for q in group_list:
            name = q.get("name", "未知窍穴")
            is_open = name in opened
            if is_open:
                opened_count += 1
            mark = "✅" if is_open else "⬜"
            et_cn = _effect_type_cn(q.get("effect_type", ""))
            ev = float(q.get("effect_value", 0))
            lines.append(f"{mark} {name} | {et_cn} +{ev * 100:.2f}%")

        lines.append(f"\n进度：{opened_count}/{len(group_list)}")
        await send_msg_handler(bot, event, "我的体窍", bot.self_id, lines, title=f"【{arg}窍穴】")
        return

    # 参数=具体窍穴名：单个详情
    q = qmap.get(arg)
    if q:
        status = "已开启" if arg in opened else "未开启"
        et_cn = _effect_type_cn(q.get("effect_type", ""))
        ev = float(q.get("effect_value", 0))
        await handle_send(
            bot, event,
            f"【{arg}】\n"
            f"类型：{q.get('group', '未知')}\n"
            f"状态：{status}\n"
            f"效果：{et_cn} +{ev * 100:.2f}%"
        )
        return

    await handle_send(bot, event, "没有这个窍穴，请先查看：我的体窍 天罡 / 我的体窍 地煞")


@tianti_level_help.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = """
详情:
            --炼体境界帮助--
淬体境→金刚境→龙象境

神藏境→圣体境→玄黄境

太初境→鸿蒙境→永恒体
""".strip()
    await handle_send(bot, event, msg)
