import os
import random
from nonebot import on_command, on_fullmatch
from nonebot.adapters.onebot.v11 import (
    GROUP,
    ActionFailed,
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
    CommandObjectID,
    number_to,
    append_draw_card_node,
    check_user,
    get_msg_pic,
    handle_send,
    send_msg_handler,
    handle_pic_send,
    update_statistics_value
)
from ..xiuxian_utils.xiuxian2_handle import XIUXIAN_IMPART_BUFF
from .impart_data import impart_data_json
from .impart_uitls import (
    get_image_representation,
    get_star_rating,
    get_rank,
    img_path,
    impart_check,
    re_impart_data,
    update_user_impart_data,
)
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()


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
impart_help = on_fullmatch("传承帮助", priority=8, block=True)
impart_pk_help = on_fullmatch("虚神界帮助", priority=8, block=True)
re_impart_load = on_fullmatch("加载传承数据", priority=45, block=True)
impart_img = on_command(
    "传承卡图", aliases={"传承卡片"}, priority=50, block=True
)

__impart_help__ = f"""
【虚神界传承系统】✨

🎴 传承祈愿：
  传承祈愿 - 花费10颗思恋结晶抽取传承卡片（被动加成）
  传承抽卡 - 花费灵石抽取传承卡片

📦 传承管理：
  传承信息 - 查看传承系统说明
  传承背包 - 查看已获得的传承卡片
  加载传承数据 - 重新加载传承属性（修复显示异常）
  传承卡图+名字 - 查看传承卡牌原画
""".strip()

__impart_pk_help__ = f"""
【虚神界帮助】✨

🌌 虚神界功能：
  投影虚神界 - 创建可被全服挑战的分身
  虚神界列表 - 查看所有虚神界投影
  虚神界对决 [编号] - 挑战指定投影（不填编号挑战{NICKNAME}）
  虚神界修炼 [时间] - 在虚神界中修炼
  探索虚神界 - 获取随机虚神界祝福
  虚神界信息 - 查看个人虚神界状态

💎 思恋结晶：
  获取方式：虚神界对决（俄罗斯轮盘修仙版）
  • 双方共6次机会，其中必有一次暴毙
  • 胜利奖励：20结晶（不消耗次数）
  • 失败奖励：10结晶（消耗1次次数）
  • 每日对决次数：5次
""".strip()

@impart_help.handle(parameterless=[Cooldown()])
async def impart_help_(
    bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()
):
    """传承帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help:
        msg = cache_help[session_id]        
        await handle_send(bot, event, msg)
    else:
        msg = __impart_help__
        await handle_send(bot, event, msg)
        await impart_help.finish()

@impart_pk_help.handle(parameterless=[Cooldown()])
async def impart_pk_help_(
    bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()
):
    """虚神界帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help:
        msg = cache_help[session_id]        
        await handle_send(bot, event, msg)
    else:
        msg = __impart_pk_help__
        await handle_send(bot, event, msg)
        await impart_pk_help.finish()

@impart_draw.handle(parameterless=[Cooldown()])
async def impart_draw_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承祈愿"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return

    user_id = user_info["user_id"]
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, "发生未知错误！")
        return

    # 解析抽卡次数
    msg = args.extract_plain_text().strip()
    if msg:
        try:
            times_str = msg.split()[-1]
            times = int(times_str)
            times = (times // 10) * 10
            times = max(10, min(times, 10000))
        except (IndexError, ValueError):
            await handle_send(bot, event, "请输入有效次数（如：传承祈愿 10）")
            return
    else:
        times = 10

    # 检查思恋结晶是否足够
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
    total_seclusion_time = total_seclusion_time // 10
    
    # 更新用户数据
    xiuxian_impart.update_stone_num(required_crystals, user_id, 2)
    xiuxian_impart.update_impart_wish(current_wish, user_id)
    await update_user_impart_data(user_id, total_seclusion_time)
    impart_data_draw = await impart_check(user_id)
    update_statistics_value(user_id, "传承祈愿", increment=times)

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

    try:
        await handle_send(bot, event, summary_msg)
    except ActionFailed:
        await handle_send(bot, event, "祈愿结果发送失败！")
    await impart_draw.finish()

@impart_draw2.handle(parameterless=[Cooldown()])
async def impart_draw2_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承抽卡"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return

    user_id = user_info["user_id"]
    user_stone_num = user_info['stone']
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, "发生未知错误！")
        return

    # 解析抽卡次数
    msg = args.extract_plain_text().strip()
    if msg:
        try:
            times_str = msg.split()[-1]
            times = int(times_str)
            times = (times // 10) * 10
            times = max(10, min(times, 10000))
        except (IndexError, ValueError):
            await handle_send(bot, event, "请输入有效次数（如：传承抽卡 10）")
            return
    else:
        times = 10

    # 检查灵石是否足够
    required_crystals = times * 1000000
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
    for _ in range(times // 10):
        # 每次10连增加10点计数
        current_wish += 10
        
        # 检查是否触发保底
        if current_wish >= 89:
            reap_img = random.choice(img_list)
            drawn_cards.append(reap_img)
            guaranteed_pulls += 1
            current_wish = 0  # 重置概率计数
            xiuxian_impart.update_impart_wish(current_wish, user_id)
        else:
            if get_rank(user_id):
                # 中奖情况
                reap_img = random.choice(img_list)
                drawn_cards.append(reap_img)
                current_wish = 0  # 重置概率计数
                xiuxian_impart.update_impart_wish(current_wish, user_id)

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

    # 更新用户数据
    sql_message.update_ls(user_id, required_crystals, 2)
    xiuxian_impart.update_impart_wish(current_wish, user_id)
    await re_impart_data(user_id)
    impart_data_draw = await impart_check(user_id)
    update_statistics_value(user_id, "传承抽卡", increment=times)

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

    try:
        await handle_send(bot, event, summary_msg)
    except ActionFailed:
        await handle_send(bot, event, "抽卡结果发送失败！")
    await impart_draw2.finish()

async def use_wishing_stone(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用祈愿石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg)
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
    
    # 构建结果消息
    new_cards_msg = f"新卡片({total_new_cards}张)：{', '.join(new_cards) if new_cards else '无'}"
    duplicate_cards_msg = f"重复卡片({total_duplicates}张)：{', '.join(duplicate_cards_info) if duplicate_cards_info else '无'}{more_duplicates_msg}"
    
    final_msg = f"""结果如下：
{new_cards_msg}
{duplicate_cards_msg}
"""
    try:
        await handle_send(bot, event, final_msg)
    except ActionFailed:
        await handle_send(bot, event, "获取祈愿石结果失败！")
    return

async def use_love_sand(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, item_id, quantity):
    """使用思恋流沙"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg)
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
    
    # 构建结果消息
    final_msg = f"获得思恋结晶 {total_gained} 颗\n当前思恋结晶：{current_stones + total_gained}颗"
    
    try:
        await handle_send(bot, event, final_msg)
    except ActionFailed:
        await handle_send(bot, event, "使用思恋流沙结果发送失败！")
    return

@impart_back.handle(parameterless=[Cooldown()])
async def impart_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承背包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
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
    msg = f"\n道友的传承卡片：\n"
    msg += "\n".join(card_lines)
    
    # 只在第一页显示总数和种类
    if page == 1:
        unique_cards = len(card_dict)
        total_cards = sum(card_dict.values())
        msg += f"\n\n卡片种类：{unique_cards}/106"
        msg += f"\n总卡片数：{total_cards}"
    
    # 添加分页信息
    msg += f"\n\n第{page}/{total_pages}页"
    if total_pages > 1:
        msg += f"\n输入【传承背包+页码】查看其他页"
    
    await handle_send(bot, event, msg)

@re_impart_load.handle(parameterless=[Cooldown()])
async def re_impart_load_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """加载传承数据"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return

    user_id = user_info["user_id"]
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(
            bot, event, send_group_id, "发生未知错误！"
        )
        return
    # 更新传承数据
    info = await re_impart_data(user_id)
    if info:
        msg = "传承数据加载完成！"
    else:
        msg = "传承数据加载失败！"
    await handle_send(bot, event, msg)


@impart_info.handle(parameterless=[Cooldown()])
async def impart_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """传承信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    user_id = user_info["user_id"]
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(
            bot, event, send_group_id, "发生未知错误！"
        )
        return

    msg = f"""
道友的传承总属性
攻击提升:{int(impart_data_draw["impart_atk_per"] * 100)}%
气血提升:{int(impart_data_draw["impart_hp_per"] * 100)}%
真元提升:{int(impart_data_draw["impart_mp_per"] * 100)}%
会心提升：{int(impart_data_draw["impart_know_per"] * 100)}%
会心伤害提升：{int(impart_data_draw["impart_burst_per"] * 100)}%
闭关经验提升：{int(impart_data_draw["impart_exp_up"] * 100)}%
炼丹收获数量提升：{impart_data_draw["impart_mix_per"]}颗
灵田收取数量提升：{impart_data_draw["impart_reap_per"]}颗
每日双修次数提升：{impart_data_draw["impart_two_exp"]}次
boss战攻击提升:{int(impart_data_draw["boss_atk"] * 100)}%

思恋结晶：{impart_data_draw["stone_num"]}颗"""
    await handle_send(bot, event, msg)

@impart_img.handle(parameterless=[Cooldown()])
async def impart_img_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承卡图"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    img_list = impart_data_json.data_all_keys()
    img_name = str(args.extract_plain_text().strip())
    if not img_name:
        msg = "请输入正确格式：传承卡图 卡图名"
        await handle_send(bot, event, msg)
        await impart_img.finish()
    if img_name in img_list:
        img = get_image_representation(img_name)
        await handle_pic_send(bot, event, img)
    else:
        msg = "没有找到此卡图！"
        await handle_send(bot, event, msg)
        await impart_img.finish()