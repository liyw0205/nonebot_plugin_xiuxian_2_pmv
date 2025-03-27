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

impart_draw = on_command("传承抽卡", aliases={"传承祈愿"}, priority=16, block=True)
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
__impart_help__ = f"""
传承帮助信息:
指令:
1、传承祈愿:花费10颗思恋结晶获取一次传承卡片(抽到的卡片被动加成)
2、传承信息:获取传承主要信息
3、传承背包:获取传承全部信息
4、加载传承数据:重新从卡片中加载所有传承属性(数据显示有误时可用)
5、传承卡图:加上卡片名字获取传承卡牌原画
6、投影虚神界:将自己的分身投影到虚神界,将可被所有地域的道友挑战
7、虚神界列表:查找虚神界里所有的投影
8、虚神界对决:输入虚神界人物编号即可与对方对决,不输入编号将会与{NICKNAME}进行对决
9、虚神界修炼:加入对应的修炼时间,即可在虚神界修炼
千次祈愿:传承祈愿 1000
思恋结晶获取方式:虚神界对决【俄罗斯轮盘修仙版】
双方共6次机会,6次中必有一次暴毙
获胜者将获取30颗思恋结晶并不消耗虚神界对决次数
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
async def impart_draw_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """传承抽卡"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return

    user_id = user_info["user_id"]
    impart_data_draw = await impart_check(user_id)
    if impart_data_draw is None:
        await handle_send(bot, event, send_group_id, "发生未知错误，多次尝试无果请找晓楠！")
        return

    # 解析抽卡次数
    input_arg = event.get_plaintext().split(maxsplit=1)
    times = 10  # 默认单次十连
    if len(input_arg) > 1:
        try:
            times = int(input_arg[1])
            if times % 10 != 0 or times < 10:
                raise ValueError
        except ValueError:
            await handle_send(bot, event, send_group_id, "请输入合法次数（如：传承抽卡 20）")
            return

    # 检查思恋结晶是否足够
    required_crystals = times  # 每抽一次消耗1颗
    if impart_data_draw["stone_num"] < required_crystals:
        await handle_send(bot, event, send_group_id, f"思恋结晶数量不足，需要{required_crystals}颗!")
        return

    # 初始化变量
    summary = f"道友{user_info['user_name']}的传承祈愿"
    img_list = impart_data_json.data_all_keys()
    if not img_list:
        await handle_send(bot, event, send_group_id, "请检查卡图数据完整！")
        return

    total_seclusion_time = 0
    new_cards = []
    duplicate_cards = []
    list_tp = []
    sent_images = set()  # 记录已发送的图片
    current_wish = impart_data_draw["wish"]  # 初始化抽卡次数

    # 执行抽卡
    for _ in range(times // 10):
        if get_rank(user_id):
            # 中奖情况
            reap_img = random.choice(img_list)
            if impart_data_json.data_person_add(user_id, reap_img):
                # 重复卡片
                duplicate_cards.append(reap_img)
                total_seclusion_time += 2100
                if reap_img not in sent_images:
                    img = get_image_representation(reap_img)
                    append_draw_card_node(bot, list_tp, summary, img)
                    sent_images.add(reap_img)
            else:
                # 新卡片
                new_cards.append(reap_img)
                total_seclusion_time += 660
                img = get_image_representation(reap_img)
                append_draw_card_node(bot, list_tp, summary, img)
                sent_images.add(reap_img)
            # 中奖（新卡或重复卡）后重置抽卡次数为0
            current_wish = 0
        else:
            # 未中奖情况
            total_seclusion_time += 660
            random.shuffle(time_img)
            for x in time_img:
                if x not in sent_images:
                    img = get_image_representation(x)
                    append_draw_card_node(bot, list_tp, summary, img)
                    sent_images.add(x)
            # 未中奖时增加10次抽卡计数
            current_wish += 10

        # 每组十连扣除10颗结晶并更新抽卡次数
        xiuxian_impart.update_stone_num(-10, user_id, 1)  # 1表示减少
        xiuxian_impart.update_impart_wish(current_wish, user_id)

    # 生成统计消息并放在图片前
    summary_msg = (
        f"{summary}\n"
        f"累计获得{total_seclusion_time}分钟闭关时间！\n"
        f"新获得卡片：{', '.join(new_cards) if new_cards else '无'}\n"
        f"重复卡片：{', '.join(duplicate_cards) if duplicate_cards else '无'}\n"
        f"抽卡次数：{current_wish}/90次\n"
        f"剩余思恋结晶：{impart_data_draw['stone_num'] - times}颗\n"
    )
    append_draw_card_node(bot, list_tp, summary, summary_msg)

    # 发送结果，只有成功后才更新数据
    try:
        await send_msg_handler(bot, event, list_tp)
        # 发送成功后更新用户数据
        await update_user_impart_data(user_id, total_seclusion_time)
        await re_impart_data(user_id)
    except ActionFailed:
        await handle_send(bot, event, send_group_id, "抽卡结果发送失败！数据未更新，请重试！")
        # 回滚资源更改
        xiuxian_impart.update_stone_num(times, user_id, 2)  # 2表示增加，恢复扣除的结晶
        xiuxian_impart.update_impart_wish(impart_data_draw["wish"], user_id)  # 恢复抽卡次数

    await impart_draw.finish()


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
            bot, event, send_group_id, "发生未知错误，多次尝试无果请找晓楠！"
        )
        return

    list_tp = []
    img = None
    name = user_info["user_name"]
    img_tp = impart_data_json.data_person_list(user_id)
    card_count = len(img_tp) if img_tp else 0 # 当前卡片数量
    txt_back = f"""--道友{name}的传承物资--
思恋结晶：{impart_data_draw["stone_num"]}颗
抽卡次数：{impart_data_draw["wish"]}/90次
卡片数量：{card_count}/108
累计闭关时间：{impart_data_draw["exp_day"]}分钟
"""
    txt_tp = f"""--道友{name}的传承总属性--
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
道友拥有的传承卡片如下:
"""
    summary = f"道友{name}的传承背包"
    if img_tp:
        card_list_str = "\n".join(img_tp)
        txt_tp += card_list_str
    else:
        txt_tp += "暂无传承卡片"

    append_draw_card_node(bot, list_tp, summary, txt_back)
    append_draw_card_node(bot, list_tp, summary, txt_tp)

    try:
        await send_msg_handler(bot, event, list_tp)
    except ActionFailed:
        await handle_send(bot, event, send_group_id, "获取传承背包数据失败！")


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
            bot, event, send_group_id, "发生未知错误，多次尝试无果请找晓楠！"
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
            bot, event, send_group_id, "发生未知错误，多次尝试无果请找晓楠！"
        )
        return

    msg = f"""--道友{user_info["user_name"]}的传承物资--
思恋结晶：{impart_data_draw["stone_num"]}颗
抽卡次数：{impart_data_draw["wish"]}/90次
累计闭关时间：{impart_data_draw["exp_day"]}分钟
    """
    await handle_send(bot, event, msg)
