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
    send_msg_handler
)
from ..xiuxian_utils.xiuxian2_handle import XIUXIAN_IMPART_BUFF
from .impart_data import impart_data_json
from .impart_uitls import (
    get_image_representation,
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
impart_help = on_command(
    "传承帮助", aliases={"虚神界帮助"}, priority=8, block=True
)
re_impart_load = on_fullmatch("加载传承数据", priority=45, block=True)
impart_img = on_command(
    "传承卡图", aliases={"传承卡片"}, priority=50, block=True
)
use_wishing_stone = on_command("使用祈愿石", priority=5, block=True)

__impart_help__ = f"""
传承帮助信息:
指令:
传承祈愿:花费10颗思恋结晶获取一次传承卡片(抽到的卡片被动加成)
传承信息:获取传承主要信息
传承背包:获取传承全部信息
加载传承数据:重新从卡片中加载所有传承属性(数据显示有误时可用)
传承卡图:加上卡片名字获取传承卡牌原画
投影虚神界:将自己的分身投影到虚神界,将可被所有地域的道友挑战
虚神界列表:查找虚神界里所有的投影
虚神界对决:输入虚神界人物编号即可与对方对决,不输入编号将会与{NICKNAME}进行对决
虚神界修炼:加入对应的修炼时间,即可在虚神界修炼
探索虚神界:获得虚神界祝福
虚神界信息:查看道友的虚神界
千次祈愿:传承祈愿 1000
思恋结晶获取方式:虚神界对决【俄罗斯轮盘修仙版】
双方共6次机会,6次中必有一次暴毙
获胜者将获取20颗思恋结晶并不消耗虚神界对决次数
失败者将获取10颗思恋结晶并且消耗一次虚神界对决次数
每天有五次虚神界对决次数
"""


@impart_help.handle(parameterless=[Cooldown(at_sender=False)])
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


@impart_draw.handle(parameterless=[Cooldown(at_sender=False)])
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

    # 解析抽卡概率
    msg = args.extract_plain_text().strip()
    if msg:
        try:
            times_str = msg.split()[-1]
            times = int(times_str)
            times = (times // 10) * 10
            times = max(10, min(times, 1000))
        except (IndexError, ValueError):
            await handle_send(bot, event, "请输入有效次数（如：传承祈愿 10）")
            return
    else:
        times = 10

    # 检查思恋结晶是否足够
    required_crystals = times  # 每抽一次消耗10颗
    if impart_data_draw["stone_num"] < required_crystals:
        await handle_send(bot, event, f"思恋结晶数量不足，需要{required_crystals}颗!")
        return

    # 初始化变量
    summary = f"道友的传承祈愿"
    img_list = impart_data_json.data_all_keys()
    if not img_list:
        await handle_send(bot, event, "请检查卡图数据完整！")
        return

    total_seclusion_time = 0
    new_cards = []
    duplicate_cards = []
    list_tp = []
    current_wish = impart_data_draw["wish"]  # 初始化抽卡概率

    # 执行抽卡
    for _ in range(times // 10):
        if get_rank(user_id):
            # 中奖情况
            reap_img = random.choice(img_list)
            if impart_data_json.data_person_add(user_id, reap_img):
                # 重复卡片
                duplicate_cards.append(reap_img)
                total_seclusion_time += 1200
            else:
                # 新卡片
                new_cards.append(reap_img)
                total_seclusion_time += 660
            # 中奖（新卡或重复卡）后重置抽卡概率为0
            current_wish = 0
        else:
            # 未中奖情况
            total_seclusion_time += 660
            random.shuffle(time_img)
            # 未中奖时增加10次抽卡计数
            current_wish += 10

        # 每组十连扣除10颗结晶并更新抽卡概率
        xiuxian_impart.update_stone_num(10, user_id, 2)
        xiuxian_impart.update_impart_wish(current_wish, user_id)
    impart_data_draw = await impart_check(user_id)

    summary_msg = (
        f"{summary}\n"
        f"累计获得{total_seclusion_time}分钟闭关时间！\n"
        f"新获得卡片：{', '.join(new_cards) if new_cards else '无'}\n"
        f"重复卡片：{', '.join(duplicate_cards) if duplicate_cards else '无'}\n"
        f"抽卡概率：{current_wish}/90次\n"
        f"消耗思恋结晶：{times}颗"        
        f"剩余思恋结晶：{impart_data_draw['stone_num']}颗"
    )
    await update_user_impart_data(user_id, total_seclusion_time)
    await re_impart_data(user_id)

    try:
        await handle_send(bot, event, summary_msg)
    except ActionFailed:
        await handle_send(bot, event, "祈愿结果发送失败！")
    await impart_draw.finish()


@impart_draw2.handle(parameterless=[Cooldown(at_sender=False)])
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

    # 解析抽卡概率
    msg = args.extract_plain_text().strip()
    if msg:
        try:
            times_str = msg.split()[-1]
            times = int(times_str)
            times = (times // 10) * 10
            times = max(10, min(times, 1000))
        except (IndexError, ValueError):
            await handle_send(bot, event, "请输入有效次数（如：传承抽卡 10）")
            return
    else:
        times = 10

    # 检查灵石是否足够
    required_crystals = times * 1000000 # 每抽一次消耗1000w
    if user_stone_num < required_crystals:
        await handle_send(bot, event, f"灵石不足，需要{number_to(required_crystals)}!")
        return

    # 初始化变量
    summary = f"道友的传承抽卡"
    img_list = impart_data_json.data_all_keys()
    if not img_list:
        await handle_send(bot, event, "请检查卡图数据完整！")
        return

    new_cards = []
    duplicate_cards = []
    current_wish = impart_data_draw["wish"]  # 初始化抽卡概率
    reward_stone = 0

    # 执行抽卡
    for _ in range(times // 10):
        if get_rank(user_id):
            # 中奖情况
            reap_img = random.choice(img_list)
            if impart_data_json.data_person_add(user_id, reap_img):
                # 重复卡片
                duplicate_cards.append(reap_img)
                xiuxian_impart.update_stone_num(10, user_id, 1)
                reward_stone += 10
            else:
                # 新卡片
                new_cards.append(reap_img)
            # 中奖（新卡或重复卡）后重置抽卡概率为0
            current_wish = 0
        else:
            # 未中奖情况
            random.shuffle(time_img)
            # 未中奖时增加10次抽卡计数
            current_wish += 10

        xiuxian_impart.update_impart_wish(current_wish, user_id)
    sql_message.update_ls(user_id, required_crystals, 2)  # 2表示减少
    impart_data_draw = await impart_check(user_id)

    summary_msg = (
        f"{summary}\n"
        f"新获得卡片：{', '.join(new_cards) if new_cards else '无'}\n"
        f"重复卡片：{', '.join(duplicate_cards) if duplicate_cards else '无'}\n"
        f"抽卡概率：{current_wish}/90次\n"
        f"转换思恋结晶：{reward_stone}颗\n"
        f"剩余思恋结晶：{impart_data_draw['stone_num']}颗\n"
        f"消耗灵石：{number_to(required_crystals)}"
    )
    await re_impart_data(user_id)

    try:
        await handle_send(bot, event, summary_msg)
    except ActionFailed:
        await handle_send(bot, event, "抽卡结果发送失败！")
    await impart_draw2.finish()
    
@use_wishing_stone.handle(parameterless=[Cooldown(at_sender=False)])
async def use_wishing_stone_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """使用祈愿石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    user_id = user_info["user_id"]
    if not isUser:
        await handle_send(bot, event, msg)
        await use_wishing_stone.finish()
        
    # 解析祈愿石数量
    msg_text = args.extract_plain_text().strip()
    try:
        stone_num = int(msg_text.split()[0]) if msg_text else 1  # 默认使用1个祈愿石
    except (IndexError, ValueError):
        await handle_send(bot, event, "请输入有效的祈愿石数量（如：使用祈愿石 5）")
        await use_wishing_stone.finish()

    # 检查背包中的祈愿石数量
    back_msg = sql_message.get_back_msg(user_id)
    wishing_stone_id = 20005  
    wishing_stone_total = 0
    for item in back_msg:
        if item['goods_id'] == wishing_stone_id:
            wishing_stone_total = item['goods_num']
            break

    if wishing_stone_total < stone_num:
        msg = f"道友背包中没有足够的祈愿石，无法使用！你当前有 {wishing_stone_total} 个祈愿石，但需要 {stone_num} 个。"
        await handle_send(bot, event, msg)
        await use_wishing_stone.finish()
        
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, "发生未知错误！")
        await use_wishing_stone.finish()
    img_list = impart_data_json.data_all_keys()
    if not img_list:
        await handle_send(bot, event, "请检查卡图数据完整！")
        await use_wishing_stone.finish()

    summary = f"道友使用祈愿石的结果"
    list_tp = []
    img_msg = ""
    sent_images = set()  # 记录已发送的图片

    for _ in range(stone_num):
        reap_img = random.choice(img_list)
        if impart_data_json.data_person_add(user_id, reap_img):
            # 重复卡片
            msg = f"重复卡片：{reap_img}"
        else:
            # 新卡片
            msg = f"新卡片：{reap_img}"
        img_msg += f"\n{msg}"
        # 消耗祈愿石
        sql_message.update_back_j(user_id, wishing_stone_id)

    # 更新用户的抽卡数据
    await re_impart_data(user_id)
    final_msg = f"""道友使用了 {stone_num} 个祈愿石，结果如下：
{img_msg}
    """
    try:
        await handle_send(bot, event, final_msg)
    except ActionFailed:
        await handle_send(bot, event, "获取祈愿石结果失败！")
    await use_wishing_stone.finish()

    
@impart_back.handle(parameterless=[Cooldown(at_sender=False)])
async def impart_back_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """传承背包"""
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

    list_tp = []
    img = None
    img_tp = impart_data_json.data_person_list(user_id)
    card_count = len(img_tp) if img_tp else 0 # 当前卡片数量
    txt_back = f"卡片数量：{card_count}/108"
    txt_tp = f"道友拥有的传承卡片如下:\n"
    if img_tp:
        card_list_str = "\n".join(img_tp)
        txt_tp += card_list_str
    else:
        txt_tp += "暂无传承卡片"

    msg = f"""
{txt_tp}\n\n{txt_back}"""
    try:
        await handle_send(bot, event, msg)
    except ActionFailed:
        await handle_send(bot, event, "获取传承背包数据失败！")


@re_impart_load.handle(parameterless=[Cooldown(at_sender=False)])
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


@impart_info.handle(parameterless=[Cooldown(at_sender=False)])
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

@impart_img.handle(parameterless=[Cooldown(at_sender=False)])
async def impart_img_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承卡图"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    img_list = impart_data_json.data_all_keys()
    img_name = str(args.extract_plain_text().strip())
    if img_name in img_list:
        img = get_image_representation(img_name)
        if isinstance(event, GroupMessageEvent):
           await bot.send_group_msg(group_id=event.group_id, message=img)
        else:
            await bot.send_private_msg(user_id=event.user_id, message=img)
        await impart_img.finish()
    else:
        msg = "没有找到此卡图！"
        await handle_send(bot, event, msg)
        await impart_img.finish()