import os
import random
from ..on_compat import on_command
from ..adapter_compat import (
    GROUP,
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    Message,
    MessageSegment,
)
from nonebot.params import CommandArg

from .. import NICKNAME
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import (
    number_to,
    append_draw_card_node,
    check_user,
    get_msg_pic,
    handle_send,
    send_msg_handler,
    handle_pic_send,
    update_statistics_value,
    send_help_message,
    log_message
)
from ..xiuxian_utils.xiuxian2_handle import XIUXIAN_IMPART_BUFF
from .impart_data import impart_data_json
from .impart_uitls import (
    get_image_representation,
    get_impart_card_description,
    get_star_rating,
    get_rank,
    img_path,
    impart_check,
    re_impart_data,
    update_user_impart_data,
)
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ...paths import get_paths
from .draw_service import ImpartDrawService
from .card_compose_service import CardComposeService
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()
impart_draw_service = ImpartDrawService(get_paths().game_db, get_paths().impart_db)
card_compose_service = CardComposeService(get_paths().impart_db)


cache_help = {}

time_img = [
    "花园百花",
    "花园温室",
    "画屏春-倒影",
    "画屏春-繁月",
    "画屏春-花临",
    "画屏春-皇女",
    "画屏春-满桂",
    "画屏春-迷花",
    "画屏春-霎那",
    "画屏春-邀舞",
]

impart_draw = on_command("传承祈愿", priority=16, block=True)
impart_draw2 = on_command("传承抽卡", priority=16, block=True)
impart_back = on_command(
    "传承背包", priority=15, block=True
)
impart_info = on_command(
    "传承信息",    
    priority=10,    
    block=True,
)
impart_help = on_command("传承帮助", priority=8, block=True)
impart_pk_help = on_command("虚神界帮助", priority=8, block=True)
impart_compose = on_command("传承合成", priority=15, block=True)
re_impart_load = on_command("加载传承数据", priority=45, block=True)
impart_img = on_command(
    "传承卡图", aliases={"传承卡片"}, priority=50, block=True
)

__impart_help__ = f"""
**虚神界传承系统**
---
**传承祈愿**
- 传承祈愿：花费10颗思恋结晶抽取传承卡片（被动加成）
- 传承抽卡：花费灵石抽取传承卡片

**传承管理**
- 传承信息：查看传承系统说明
- 传承背包：查看已获得的传承卡片
- 加载传承数据：重新加载传承属性（修复显示异常）
- 传承卡图+名字：查看传承卡牌原画
- 传承合成 [重复卡] [目标卡]：消耗5张重复卡合成目标卡
""".strip()

__impart_pk_help__ = f"""
**虚神界帮助**
---
**虚神界功能**
- 投影虚神界：创建可被全服挑战的分身
- 虚神界列表：查看所有虚神界投影
- 虚神界对决 [编号]：挑战指定投影（不填编号挑战{NICKNAME}）
- 虚神界修炼 [时间]：在虚神界中修炼
- 探索虚神界：获取随机虚神界祝福
- 虚神界信息：查看个人虚神界状态

**神魂承载**
> 每次虚神界修炼增加5%承载，今日累计修炼每满150分钟额外增加5%。
> 今日承载达到100%后，虚神界修炼获得修为固定为1。

**思恋结晶**
- 获取方式：虚神界对决（俄罗斯轮盘修仙版）
- 双方共6次机会，其中必有一次暴毙
- 胜利奖励：20结晶（不消耗次数）
- 失败奖励：10结晶（消耗1次次数）
- 每日对决次数：5次
""".strip()

@impart_help.handle(parameterless=[Cooldown(cd_time=0)])
async def impart_help_(
    bot: Bot, event: GroupMessageEvent | PrivateMessageEvent
):
    """传承帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __impart_help__
    await send_help_message(bot, event, msg, k1="祈愿", v1="传承祈愿", k2="信息", v2="传承信息", k3="背包", v3="传承背包")
    await impart_help.finish()

@impart_pk_help.handle(parameterless=[Cooldown(cd_time=0)])
async def impart_pk_help_(
    bot: Bot, event: GroupMessageEvent | PrivateMessageEvent
):
    """虚神界帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __impart_pk_help__
    await send_help_message(bot, event, msg, k1="对决", v1="虚神界对决", k2="信息", v2="虚神界信息", k3="探索", v3="虚神界探索")
    await impart_pk_help.finish()

@impart_draw.handle(parameterless=[Cooldown(cd_time=0)])
async def impart_draw_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承祈愿"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info["user_id"]
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, "发生未知错误！")
        return

    # 解析抽卡次数
    msg_text = args.extract_plain_text().strip()
    times = int(msg_text) if msg_text and 0 < int(msg_text) else 1

    # 检查思恋结晶是否足够
    times = times * 10
    required_crystals = times
    if impart_data_draw["stone_num"] < required_crystals:
        await handle_send(bot, event, f"思恋结晶数量不足，需要{required_crystals}颗!")
        return

    # 初始化变量
    summary = f"道友的传承祈愿"
    img_list = impart_data_json.data_all_keys()
    if not img_list:
        await handle_send(bot, event, "请检查卡图数据完整！")
        return

    current_wish = impart_data_draw["wish"]
    drawn_cards = []  # 记录所有抽到的卡片
    total_seclusion_time = 0
    total_new_cards = 0
    total_duplicates = 0
    guaranteed_pulls = 0  # 记录触发的保底次数

    # 执行抽卡
    for _ in range(times // 10):
        # 每次10连增加10点计数
        current_wish += 10
        
        # 检查是否触发保底
        if current_wish >= 89:
            reap_img = random.choice(img_list)
            drawn_cards.append(reap_img)
            guaranteed_pulls += 1
            total_seclusion_time += 1200  # 保底获得更多闭关时间
            current_wish = 0  # 重置概率计数
            xiuxian_impart.update_impart_wish(current_wish, user_id)
        else:
            if get_rank(user_id):
                # 中奖情况
                reap_img = random.choice(img_list)
                drawn_cards.append(reap_img)
                total_seclusion_time += 1200  # 中奖获得更多闭关时间
                current_wish = 0  # 重置概率计数
                xiuxian_impart.update_impart_wish(current_wish, user_id)
            else:
                # 未中奖情况
                total_seclusion_time += 660

    old_card_counts = impart_data_json.data_person_list(user_id) or {}
    new_cards = list(dict.fromkeys(card for card in drawn_cards if card not in old_card_counts))
    card_counts = {card: old_card_counts.get(card, 0) + drawn_cards.count(card) for card in set(drawn_cards)}
    total_new_cards = len(new_cards)
    total_duplicates = len(drawn_cards) - total_new_cards

    # 计算重复卡片信息（只显示前10个，避免消息过长）
    duplicate_cards_info = []
    duplicate_display_limit = 10
    for card, count in card_counts.items():
        if card in new_cards:
            continue
        if len(duplicate_cards_info) < duplicate_display_limit:
            duplicate_cards_info.append(f"{card}x{drawn_cards.count(card)}")
    
    # 如果有更多重复卡未显示
    more_duplicates_msg = ""
    if total_duplicates > duplicate_display_limit:
        more_duplicates_msg = f"\n(还有{total_duplicates - duplicate_display_limit}张重复卡未显示)"
    total_seclusion_time = total_seclusion_time // 10
    
    # 更新用户数据
    xiuxian_impart.update_stone_num(required_crystals, user_id, 2)
    xiuxian_impart.update_impart_wish(current_wish, user_id)
    await update_user_impart_data(user_id, total_seclusion_time)
    impart_data_draw = await impart_check(user_id)
    update_statistics_value(user_id, "传承祈愿", increment=times)
    update_statistics_value(user_id, "传承祈愿次数", increment=times // 10)
    update_statistics_value(user_id, "思恋结晶消耗", increment=required_crystals)
    update_statistics_value(user_id, "虚神界时间获取", increment=total_seclusion_time)
    update_statistics_value(user_id, "传承新卡", increment=total_new_cards)
    update_statistics_value(user_id, "传承重复卡", increment=total_duplicates)
    update_statistics_value(user_id, "传承保底次数", increment=guaranteed_pulls)
    log_message(
        user_id,
        f"[传承祈愿] 消耗思恋结晶{required_crystals}颗，获得虚神界时间{total_seclusion_time}分钟，新卡{total_new_cards}张，重复{total_duplicates}张"
    )

    # 计算实际抽卡概率
    actual_wish = current_wish % 90  # 显示当前概率计数（0-89）

    summary_msg = (
        f"{summary}\n"
        f"累计获得{total_seclusion_time}分钟闭关时间！\n"
        f"新获得卡片({total_new_cards}张)：{', '.join(new_cards) if new_cards else '无'}\n"
        f"重复卡片({total_duplicates}张)：{', '.join(duplicate_cards_info) if duplicate_cards_info else '无'}{more_duplicates_msg}\n"
        f"触发保底次数：{guaranteed_pulls}次\n"
        f"当前抽卡概率：{actual_wish}/90次\n"
        f"消耗思恋结晶：{times}颗\n"        
        f"剩余思恋结晶：{impart_data_draw['stone_num']}颗"
    )

    await handle_send(bot, event, summary_msg, md_type="传承", k1="祈愿", v1="传承祈愿", k2="背包", v2="传承背包", k3="卡图", v3="传承卡图")
    await impart_draw.finish()

@impart_draw2.handle(parameterless=[Cooldown(cd_time=0)])
async def impart_draw2_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承抽卡"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info["user_id"]
    user_stone_num = user_info['stone']
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, "发生未知错误！")
        return

    if impart_data_draw['impart_num'] >= 100:
        msg = "道友今日抽卡已达上限，请明日再来！"
        await handle_send(bot, event, msg)
        return
    max_impart_num = 100 - impart_data_draw['impart_num']
    
    # 解析抽卡次数
    msg_text = args.extract_plain_text().strip()
    times = int(msg_text) if msg_text and 0 < int(msg_text) else 1

    if times > max_impart_num:
        times = max_impart_num

    # 检查灵石是否足够
    required_crystals = times * 10000000
    if user_stone_num < required_crystals:
        await handle_send(bot, event, f"灵石不足，需要{number_to(required_crystals)}!")
        return
    
    # 初始化变量
    summary = f"道友的传承抽卡"
    img_list = impart_data_json.data_all_keys()
    if not img_list:
        await handle_send(bot, event, "请检查卡图数据完整！")
        return

    current_wish = impart_data_draw["wish"]
    drawn_cards = []  # 记录所有抽到的卡片
    total_new_cards = 0
    total_duplicates = 0
    guaranteed_pulls = 0  # 记录触发的保底次数

    # 执行抽卡
    for _ in range(times):
        # 每次10连增加10点计数
        current_wish += 10
        
        # 检查是否触发保底
        if current_wish >= 89:
            reap_img = random.choice(img_list)
            drawn_cards.append(reap_img)
            guaranteed_pulls += 1
            current_wish = 0  # 重置概率计数
        else:
            if get_rank(user_id):
                # 中奖情况
                reap_img = random.choice(img_list)
                drawn_cards.append(reap_img)
                current_wish = 0  # 重置概率计数

    # 批量添加卡片
    old_card_counts = impart_data_json.data_person_list(user_id) or {}
    new_cards = list(dict.fromkeys(card for card in drawn_cards if card not in old_card_counts))
    card_counts = {card: old_card_counts.get(card, 0) + drawn_cards.count(card) for card in set(drawn_cards)}
    total_new_cards = len(new_cards)
    total_duplicates = len(drawn_cards) - total_new_cards
    # 计算重复卡片信息（只显示前10个，避免消息过长）
    duplicate_cards_info = []
    duplicate_display_limit = 10
    for card, count in card_counts.items():
        if card in new_cards:
            continue
        if len(duplicate_cards_info) < duplicate_display_limit:
            duplicate_cards_info.append(f"{card}x{drawn_cards.count(card)}")
    
    # 如果有更多重复卡未显示
    more_duplicates_msg = ""
    if total_duplicates > duplicate_display_limit:
        more_duplicates_msg = f"\n(还有{total_duplicates - duplicate_display_limit}张重复卡未显示)"

    # 更新用户数据
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or random.getrandbits(64))
    result = impart_draw_service.draw(
        f"impart-draw:{event_id}:{user_id}", user_id, user_stone_num,
        impart_data_draw["wish"], impart_data_draw["impart_num"], required_crystals,
        current_wish, times, drawn_cards,
    )
    if not result.succeeded:
        await handle_send(bot, event, "抽卡状态已变化，请重试。")
        return
    await re_impart_data(user_id)
    impart_data_draw = await impart_check(user_id)
    update_statistics_value(user_id, "传承抽卡", increment=times * 10)
    update_statistics_value(user_id, "传承抽卡次数", increment=times)
    update_statistics_value(user_id, "传承抽卡灵石消耗", increment=required_crystals)
    update_statistics_value(user_id, "传承新卡", increment=total_new_cards)
    update_statistics_value(user_id, "传承重复卡", increment=total_duplicates)
    update_statistics_value(user_id, "传承保底次数", increment=guaranteed_pulls)
    log_message(
        user_id,
        f"[传承抽卡] 消耗灵石{number_to(required_crystals)}，抽卡{times}次，新卡{total_new_cards}张，重复{total_duplicates}张"
    )

    # 计算实际抽卡概率
    actual_wish = current_wish % 90  # 显示当前概率计数（0-89）

    summary_msg = (
        f"{summary}\n"
        f"新获得卡片({total_new_cards}张)：{', '.join(new_cards) if new_cards else '无'}\n"
        f"重复卡片({total_duplicates}张)：{', '.join(duplicate_cards_info) if duplicate_cards_info else '无'}{more_duplicates_msg}\n"
        f"触发保底次数：{guaranteed_pulls}次\n"
        f"当前抽卡概率：{actual_wish}/90次\n"
        f"剩余思恋结晶：{impart_data_draw['stone_num']}颗\n"
        f"消耗灵石：{number_to(required_crystals)}"
    )

    await handle_send(bot, event, summary_msg, md_type="传承", k1="抽卡", v1="传承抽卡", k2="背包", v2="传承背包", k3="卡图", v3="传承卡图")
    await impart_draw2.finish()

async def use_wishing_stone(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用祈愿石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
        
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, "发生未知错误！")
        return
    img_list = impart_data_json.data_all_keys()
    if not img_list:
        await handle_send(bot, event, "请检查卡图数据完整！")
        return

    # 必中奖抽卡 - 直接随机选择卡片
    drawn_cards = [random.choice(img_list) for _ in range(quantity)]

    # 批量添加卡片
    new_cards, card_counts = impart_data_json.data_person_add_batch(user_id, drawn_cards)
    total_new_cards = len(new_cards)
    total_duplicates = len(drawn_cards) - total_new_cards

    # 计算重复卡片信息（只显示前10个，避免消息过长）
    duplicate_cards_info = []
    duplicate_display_limit = 10
    for card, count in card_counts.items():
        if card in new_cards:
            continue
        if len(duplicate_cards_info) < duplicate_display_limit:
            duplicate_cards_info.append(f"{card}x{drawn_cards.count(card)}")
    
    # 如果有更多重复卡未显示
    more_duplicates_msg = ""
    if total_duplicates > duplicate_display_limit:
        more_duplicates_msg = f"\n(还有{total_duplicates - duplicate_display_limit}张重复卡未显示)"

    # 批量消耗祈愿石
    sql_message.update_back_j(user_id, item_id, quantity)

    # 更新用户的抽卡数据（不更新概率计数）
    await re_impart_data(user_id)
    update_statistics_value(user_id, "祈愿石使用", increment=quantity)
    update_statistics_value(user_id, "传承新卡", increment=total_new_cards)
    update_statistics_value(user_id, "传承重复卡", increment=total_duplicates)
    log_message(
        user_id,
        f"[祈愿石] 使用{quantity}颗，获得新卡{total_new_cards}张，重复{total_duplicates}张"
    )
    
    # 构建结果消息
    new_cards_msg = f"新卡片({total_new_cards}张)：{', '.join(new_cards) if new_cards else '无'}"
    duplicate_cards_msg = f"重复卡片({total_duplicates}张)：{', '.join(duplicate_cards_info) if duplicate_cards_info else '无'}{more_duplicates_msg}"
    
    final_msg = f"""结果如下：
{new_cards_msg}
{duplicate_cards_msg}
"""
    await handle_send(bot, event, final_msg, md_type="传承", k1="再次", v1="道具使用 祈愿石", k2="背包", v2="传承背包", k3="卡图", v3="传承卡图")
    return

async def use_love_sand(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用思恋流沙"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
        
    # 获取当前思恋结晶数量
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, "发生未知错误！")
        return
    
    current_stones = impart_data_draw["stone_num"]
    
    # 使用思恋流沙，随机获得思恋结晶
    total_gained = sum(random.choice([10, 20, 30]) for _ in range(quantity))
    
    # 更新思恋结晶数量
    xiuxian_impart.update_stone_num(total_gained, user_id, 1)
    
    # 批量消耗思恋流沙
    sql_message.update_back_j(user_id, item_id, quantity)
    update_statistics_value(user_id, "思恋流沙使用", increment=quantity)
    update_statistics_value(user_id, "思恋结晶获取", increment=total_gained)
    log_message(user_id, f"[思恋流沙] 使用{quantity}个，获得思恋结晶{total_gained}颗")
    
    # 构建结果消息
    final_msg = f"获得思恋结晶 {total_gained} 颗\n当前思恋结晶：{current_stones + total_gained}颗"
    
    await handle_send(bot, event, final_msg)
    return

@impart_back.handle(parameterless=[Cooldown(cd_time=0)])
async def impart_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return

    user_id = user_info["user_id"]
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, "发生未知错误！")
        return

    card_dict = impart_data_json.data_person_list(user_id)
    if not card_dict:
        await handle_send(bot, event, "暂无传承卡片")
        return
    
    # 解析页码参数
    msg_text = args.extract_plain_text().strip()
    try:
        page = int(msg_text) if msg_text else 1
    except ValueError:
        page = 1
    
    # 按数量从多到少排序，数量相同的按卡名排序
    sorted_cards = sorted(card_dict.items(), key=lambda x: (-x[1], x[0]))
    
    # 分页设置
    cards_per_page = 30
    total_pages = (len(sorted_cards) + cards_per_page - 1) // cards_per_page
    page = max(1, min(page, total_pages))
    
    # 获取当前页的卡片
    start_idx = (page - 1) * cards_per_page
    end_idx = start_idx + cards_per_page
    current_page_cards = sorted_cards[start_idx:end_idx]
    
    # 生成卡片列表
    card_lines = []
    for card_name, count in current_page_cards:
        stars = get_star_rating(count)
        card_lines.append(f"{stars} {card_name} (x{count})")
    
    # 构建消息
    title = f"道友的传承卡片：\n"
    msg = "\n".join(card_lines)
    l_msg = []
    
    # 只在第一页显示总数和种类
    if page == 1:
        unique_cards = len(card_dict)
        total_cards = sum(card_dict.values())
        msg += f"\n\n卡片种类：{unique_cards}/106"
        msg += f"\n总卡片数：{total_cards}"
    
    # 添加分页信息
    msg += f"\n\n第{page}/{total_pages}页"
    msg += f"\n输入【传承背包+页码】查看其他页"
    l_msg.append(msg)


@impart_compose.handle(parameterless=[Cooldown(cd_time=0)])
async def impart_compose_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    parts = args.extract_plain_text().strip().split()
    if len(parts) != 2:
        await handle_send(bot, event, "格式：传承合成 重复卡名 目标卡名")
        return
    source_card, target_card = parts
    if source_card not in impart_data_json.data_all_keys() or target_card not in impart_data_json.data_all_keys():
        await handle_send(bot, event, "传承卡名不存在，请检查后重试！")
        return
    user_id = str(user_info["user_id"])
    cards = impart_data_json.data_person_list(user_id) or {}
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or random.getrandbits(64))
    result = card_compose_service.compose(
        f"impart-compose:{event_id}:{user_id}", user_id, source_card, target_card,
        cards.get(source_card, 0), cards.get(target_card, 0), 5,
    )
    messages = {"same_card": "合成材料卡与目标卡不能相同！", "card_missing": "重复卡不足5张，无法合成！", "state_changed": "传承卡牌状态已变化，请重新操作！"}
    if not result.succeeded:
        await handle_send(bot, event, messages.get(result.status, "传承合成失败！"))
        return
    await handle_send(bot, event, f"合成成功：{source_card}剩余{result.source_quantity}张，{target_card}现有{result.target_quantity}张")


