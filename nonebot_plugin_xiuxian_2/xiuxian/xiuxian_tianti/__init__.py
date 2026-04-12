import random
from datetime import datetime
from nonebot import on_command
from nonebot.params import CommandArg
from ..adapter_compat import Bot, Message, GroupMessageEvent, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, handle_send, send_msg_handler, number_to
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_config import XiuConfig
from .tianti_data import (
    TiantiDataManager,
    get_tianti_level_data,
    get_next_tianti_level_name,
    get_tianti_level_index,
    get_opened_qiaoxue_count,
    get_qiaoxue_pool,
    get_qiaoxue_map,
)

sql_message = XiuxianDateManage()
tianti_manager = TiantiDataManager()

tianti_help = on_command("炼体帮助", priority=10, block=True)
tianti_settle = on_command("炼体结算", priority=10, block=True)
tianti_stone = on_command("灵石炼体", priority=10, block=True)
tianti_break = on_command("炼体突破", priority=10, block=True)
tianti_info = on_command("我的炼体", priority=10, block=True)
tianti_chongqiao = on_command("冲窍", priority=10, block=True)
tiqiao_info = on_command("我的体窍", priority=10, block=True)
tianti_level_help = on_command("炼体境界", priority=10, block=True)


def _get_tianti_cap(data: dict) -> int:
    """
    上限规则：next_need_hp * closing_exp_upper_limit - 1
    """
    next_name = get_next_tianti_level_name(data["tianti_level"])
    if not next_name:
        return 10**30
    need_hp = int(get_tianti_level_data(next_name)["need_hp"])
    return max(0, int(need_hp * XiuConfig().closing_exp_upper_limit) - 1)


def _calc_qiaoxue_bonus(data: dict):
    base_ratio = 0.0
    gain_pct = 0.0
    detail_list = data.get("opened_qiaoxue_detail", [])
    for q in detail_list:
        et = q["effect_type"]
        ev = float(q["effect_value"])
        if et == "base_per_min_ratio":
            base_ratio += ev
        elif et == "hp_gain_pct":
            gain_pct += ev
    return base_ratio, gain_pct


@tianti_help.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    msg = """【炼体系统帮助】
1）炼体结算：按分钟结算炼体气血
2）灵石炼体 数量：消耗灵石换炼体气血（1灵石=0.1炼体气血）
3）炼体突破：满足修仙境界+炼体气血后突破
4）冲窍：消耗当前10%炼体气血，随机开一个未开的窍穴
5）我的体窍 [窍穴名]：查看体窍进度/指定窍穴信息
6）我的炼体：查看当前炼体状态
7）炼体境界：查看炼体境界表
"""
    await handle_send(bot, event, msg)


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

    if not data["last_settle_time"]:
        data["last_settle_time"] = now_t.strftime("%Y-%m-%d %H:%M:%S")
        tianti_manager.save_user_tianti_info(user_id, data)
        await handle_send(bot, event, "已初始化炼体计时，请稍后再来结算。")
        return

    last_t = datetime.strptime(data["last_settle_time"], "%Y-%m-%d %H:%M:%S")
    mins = max(0, int((now_t - last_t).total_seconds() // 60))
    if mins <= 0:
        await handle_send(bot, event, "时间太短，暂无可结算炼体收益。")
        return

    lvl_data = get_tianti_level_data(data["tianti_level"])
    base_per_min = int(lvl_data["hp_gain_per_min"])

    base_ratio, gain_pct = _calc_qiaoxue_bonus(data)

    real_per_min = int(base_per_min * (1 + base_ratio))
    gain = int(mins * real_per_min * (1 + gain_pct))

    cap = _get_tianti_cap(data)
    old_hp = int(data["tianti_hp"])
    new_hp = min(cap, old_hp + gain)
    real_gain = max(0, new_hp - old_hp)

    data["tianti_hp"] = new_hp
    data["last_settle_time"] = now_t.strftime("%Y-%m-%d %H:%M:%S")
    tianti_manager.save_user_tianti_info(user_id, data)

    await handle_send(
        bot, event,
        f"炼体结算完成，间隔{mins}分钟。\n"
        f"本次获得炼体气血：{number_to(real_gain)}\n"
        f"当前炼体气血：{number_to(new_hp)}"
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
    if stone_cost <= 0:
        await handle_send(bot, event, "请输入大于0的灵石数量。")
        return
    if stone_cost > int(user_info["stone"]):
        await handle_send(bot, event, "你的灵石不足。")
        return

    data = tianti_manager.get_user_tianti_info(user_id)
    cap = _get_tianti_cap(data)

    old_hp = int(data["tianti_hp"])
    gain = stone_cost // 10
    new_hp = min(cap, old_hp + gain)
    real_gain = max(0, new_hp - old_hp)
    real_stone_cost = real_gain * 10

    if real_stone_cost <= 0:
        await handle_send(bot, event, "已达当前炼体境界上限，无法继续灵石炼体。")
        return

    sql_message.update_ls(user_id, real_stone_cost, 2)
    data["tianti_hp"] = new_hp
    tianti_manager.save_user_tianti_info(user_id, data)

    await handle_send(
        bot, event,
        f"灵石炼体完成：消耗灵石{number_to(real_stone_cost)}，获得炼体气血{number_to(real_gain)}。"
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
    cur_name = data["tianti_level"]
    next_name = get_next_tianti_level_name(cur_name)

    if not next_name:
        await handle_send(bot, event, "你的炼体已达最高境界。")
        return

    next_cfg = get_tianti_level_data(next_name)
    need_hp = int(next_cfg["need_hp"])
    min_xx = next_cfg["min_xx_level"]

    user_xx_rank = get_tianti_level_index(user_info["level"], is_xiuxian=True)
    need_xx_rank = get_tianti_level_index(min_xx, is_xiuxian=True)
    if user_xx_rank > need_xx_rank:
        await handle_send(bot, event, f"突破失败：修仙境界不足，需达到【{min_xx}】。")
        return

    if int(data["tianti_hp"]) < need_hp:
        await handle_send(bot, event, f"突破失败：炼体气血不足，需{number_to(need_hp)}。")
        return

    data["tianti_level"] = next_name
    tianti_manager.save_user_tianti_info(user_id, data)
    await handle_send(bot, event, f"炼体突破成功！当前境界：{next_name}")


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

    next_name = get_next_tianti_level_name(lvl)
    if next_name:
        need_hp = int(get_tianti_level_data(next_name)["need_hp"])
        remain = max(0, need_hp - hp)
        cap = _get_tianti_cap(data)
        brk = f"下一境界：{next_name}\n突破需求：{number_to(need_hp)}（还需{number_to(remain)}）\n当前境界气血上限：{number_to(cap)}"
    else:
        brk = "已达炼体最高境界"

    await handle_send(
        bot, event,
        f"【我的炼体】\n"
        f"境界：{lvl}\n"
        f"炼体气血：{number_to(hp)}\n"
        f"已开窍：{opened}/108\n"
        f"{brk}"
    )


@tianti_chongqiao.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """
    冲窍：只读取固定 effect_value
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = str(user_info["user_id"])
    data = tianti_manager.get_user_tianti_info(user_id)

    opened = data["opened_qiaoxue"]
    pool = get_qiaoxue_pool()

    cur_level = data["tianti_level"]
    stage_name = cur_level[:-2] if cur_level.endswith(("一重","二重","三重","四重","五重","六重","七重","八重","九重","十重")) else cur_level

    stage_opened_map = data["qiaoxue_stage_opened"]
    opened_in_stage = int(stage_opened_map.get(stage_name, 0))
    if opened_in_stage >= 3:
        await handle_send(bot, event, f"当前【{stage_name}】最多只能开3个窍穴，你已开满。请先突破到下一炼体境界。")
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
    real_val = float(chosen["effect_value"])  # 固定值

    detail_list = data["opened_qiaoxue_detail"]
    detail_list.append({
        "name": chosen["name"],
        "group": chosen["group"],
        "effect_type": chosen["effect_type"],
        "effect_value": real_val
    })

    data["tianti_hp"] = max(0, cur_hp - cost)
    data["opened_qiaoxue"] = opened + [chosen["name"]]
    data["opened_qiaoxue_detail"] = detail_list
    stage_opened_map[stage_name] = opened_in_stage + 1
    data["qiaoxue_stage_opened"] = stage_opened_map

    tianti_manager.save_user_tianti_info(user_id, data)

    await handle_send(
        bot, event,
        f"冲窍成功！\n"
        f"消耗炼体气血：{number_to(cost)}\n"
        f"新开窍穴：{chosen['name']}\n"
        f"效果：{chosen['effect_type']} +{real_val * 100:.2f}%\n"
        f"当前【{stage_name}】已开：{stage_opened_map[stage_name]}/3"
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
        await handle_send(
            bot, event,
            f"【我的体窍】\n"
            f"已开窍：{len(opened)}/108（天罡{opened_tg}/36，地煞{opened_ds}/72）\n"
            f"每分钟气血：{base_ratio * 100:.2f}%\n"
            f"获得气血：{gain_pct * 100:.2f}%"
        )
        return

    # 参数=天罡/地煞：列出该组窍穴
    if arg in ("天罡", "地煞"):
        group_list = [q for q in qpool if q.get("group") == arg]
        # 已开启在前，未开启在后；组内按名称排序
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