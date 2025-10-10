import random
import asyncio
import re
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from nonebot.log import logger
from datetime import datetime
from nonebot import on_command, on_fullmatch
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, OtherSet, get_player_info, 
    save_player_info,UserBuffDate, get_main_info_msg, 
    get_user_buff, get_sec_msg, get_sub_info_msg, get_effect_info_msg,
    XIUXIAN_IMPART_BUFF, leave_harm_time
)
from ..xiuxian_config import XiuConfig, convert_rank
from ..xiuxian_utils.data_source import jsondata
from nonebot.params import CommandArg
from ..xiuxian_utils.player_fight import Player_fight
from ..xiuxian_utils.utils import (
    number_to, check_user, send_msg_handler,
    check_user_type, get_msg_pic, CommandObjectID, handle_send, log_message, update_statistics_value
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from .two_exp_cd import two_exp_cd
from nonebot.plugin import on_command, on_fullmatch, on_message
from nonebot.rule import to_me

# 用于存储双修邀请的全局字典
# 结构: {invited_user_id: {"inviter_id": inviter_user_id, "time": timestamp}}
two_exp_invitations = {}

cache_help = {}
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()
BLESSEDSPOTCOST = 3500000 # 洞天福地购买消耗
two_exp_limit = 3 # 默认双修次数上限，修仙之人一天3次也不奇怪（

buffinfo = on_fullmatch("我的功法", priority=25, block=True)
out_closing = on_command("出关", aliases={"灵石出关"}, priority=5, block=True)
in_closing = on_fullmatch("闭关", priority=5, block=True)
up_exp = on_command("修炼", priority=5, block=True)
reset_exp = on_command("重置修炼状态", priority=5, block=True)
stone_exp = on_command("灵石修仙", aliases={"灵石修炼"}, priority=5, block=True)
two_exp = on_command("双修", priority=5, block=True)
mind_state = on_fullmatch("我的状态", priority=7, block=True)
qc = on_command("切磋", priority=6, block=True)
buff_help = on_command("功法帮助", aliases={"灵田帮助", "洞天福地帮助"}, priority=5, block=True)
blessed_spot_creat = on_fullmatch("洞天福地购买", priority=10, block=True)
blessed_spot_info = on_fullmatch("洞天福地查看", priority=11, block=True)
blessed_spot_rename = on_command("洞天福地改名", priority=7, block=True)
ling_tian_up = on_fullmatch("灵田开垦", priority=5, block=True)
del_exp_decimal = on_fullmatch("抑制黑暗动乱", priority=9, block=True)
my_exp_num = on_fullmatch("我的双修次数", priority=9, block=True)

__buff_help__ = f"""
【修仙功法系统】📜

🌿 功法修炼：
  我的功法 - 查看当前修炼的功法详情
  抑制黑暗动乱 - 清除修为浮点数(稳定境界)

🏡 洞天福地：
  洞天福地购买 - 获取专属修炼福地
  洞天福地查看 - 查看福地状态
  洞天福地改名+名字 - 为福地命名

🌱 灵田管理：
  灵田开垦 - 提升灵田等级(增加药材产量)
  当前最高等级：9级

👥 双修系统：
  我的双修次数 - 查看剩余双修机会
  切磋@道友 - 友好比试(不消耗气血)

💡 小贴士：
  1. 洞天福地可加速修炼
  2. 灵田每23小时可收获
""".strip()

async def two_exp_cd_up():
    two_exp_cd.re_data()
    logger.opt(colors=True).info(f"<green>双修次数已更新！</green>")


@buff_help.handle(parameterless=[Cooldown(at_sender=False)])
async def buff_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """功法帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help:
        msg = cache_help[session_id]
        await handle_send(bot, event, msg)
        await buff_help.finish()
    else:
        msg = __buff_help__
        await handle_send(bot, event, msg)
        await buff_help.finish()


@blessed_spot_creat.handle(parameterless=[Cooldown(at_sender=False)])
async def blessed_spot_creat_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """洞天福地购买"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await blessed_spot_creat.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) != 0:
        msg = f"道友已经拥有洞天福地了，请发送洞天福地查看吧~"
        await handle_send(bot, event, msg)
        await blessed_spot_creat.finish()
    if user_info['stone'] < BLESSEDSPOTCOST:
        msg = f"道友的灵石不足{BLESSEDSPOTCOST}枚，无法购买洞天福地"
        await handle_send(bot, event, msg)
        await blessed_spot_creat.finish()
    else:
        sql_message.update_ls(user_id, BLESSEDSPOTCOST, 2)
        sql_message.update_user_blessed_spot_flag(user_id)
        mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
        mix_elixir_info['收取时间'] = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        save_player_info(user_id, mix_elixir_info, 'mix_elixir_info')
        msg = f"恭喜道友拥有了自己的洞天福地，请收集聚灵旗来提升洞天福地的等级吧~\n"
        msg += f"默认名称为：{user_info['user_name']}道友的家"
        sql_message.update_user_blessed_spot_name(user_id, f"{user_info['user_name']}道友的家")
        await handle_send(bot, event, msg)
        await blessed_spot_creat.finish()


@blessed_spot_info.handle(parameterless=[Cooldown(at_sender=False)])
async def blessed_spot_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """洞天福地信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await blessed_spot_info.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买来购买吧~"
        await handle_send(bot, event, msg)
        await blessed_spot_info.finish()
    msg = f"\n道友的洞天福地:\n"
    user_buff_data = UserBuffDate(user_id).BuffInfo
    if user_info['blessed_spot_name'] == 0:
        blessed_spot_name = "尚未命名"
    else:
        blessed_spot_name = user_info['blessed_spot_name']
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    msg += f"名字：{blessed_spot_name}\n"
    msg += f"修炼速度：增加{user_buff_data['blessed_spot'] * 0.5 * 100}%\n"
    msg += f"药材速度：增加{mix_elixir_info['药材速度'] * 100}%\n"
    msg += f"灵田数量：{mix_elixir_info['灵田数量']}"
    await handle_send(bot, event, msg)
    await blessed_spot_info.finish()


@ling_tian_up.handle(parameterless=[Cooldown(at_sender=False)])
async def ling_tian_up_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """洞天福地灵田升级"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await ling_tian_up.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买吧~"
        await handle_send(bot, event, msg)
        await ling_tian_up.finish()
    LINGTIANCONFIG = {
        "1": {
            "level_up_cost": 3500000
        },
        "2": {
            "level_up_cost": 5000000
        },
        "3": {
            "level_up_cost": 7000000
        },
        "4": {
            "level_up_cost": 10000000
        },
        "5": {
            "level_up_cost": 15000000
        },
        "6": {
            "level_up_cost": 23000000
        },
        "7": {
            "level_up_cost": 30000000
        },
        "8": {
            "level_up_cost": 40000000
        },
        "9": {
            "level_up_cost": 50000000
        }
    }
    mix_elixir_info = get_player_info(user_id, "mix_elixir_info")
    now_num = mix_elixir_info['灵田数量']
    if now_num == len(LINGTIANCONFIG) + 1:
        msg = f"道友的灵田已全部开垦完毕，无法继续开垦了！"
    else:
        cost = LINGTIANCONFIG[str(now_num)]['level_up_cost']
        if int(user_info['stone']) < cost:
            msg = f"本次开垦需要灵石：{cost}，道友的灵石不足！"
        else:
            msg = f"道友成功消耗灵石：{cost}，灵田数量+1,目前数量:{now_num + 1}"
            mix_elixir_info['灵田数量'] = now_num + 1
            save_player_info(user_id, mix_elixir_info, 'mix_elixir_info')
            sql_message.update_ls(user_id, cost, 2)
    await handle_send(bot, event, msg)
    await ling_tian_up.finish()


@blessed_spot_rename.handle(parameterless=[Cooldown(at_sender=False)])
async def blessed_spot_rename_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """洞天福地改名"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await blessed_spot_rename.finish()
    user_id = user_info['user_id']
    if int(user_info['blessed_spot_flag']) == 0:
        msg = f"道友还没有洞天福地呢，请发送洞天福地购买吧~"
        await handle_send(bot, event, msg)
        await blessed_spot_rename.finish()
    arg = args.extract_plain_text().strip()
    arg = str(arg)
    if arg == "":
        msg = "请输入洞天福地的名字！"
        await handle_send(bot, event, msg)
        await blessed_spot_rename.finish()
    if len(arg) > 9:
        msg = f"洞天福地的名字不可大于9位,请重新命名"
    else:
        msg = f"道友的洞天福地成功改名为：{arg}"
        sql_message.update_user_blessed_spot_name(user_id, arg)
    await handle_send(bot, event, msg)
    await blessed_spot_rename.finish()


@qc.handle(parameterless=[Cooldown(cd_time=60, stamina_cost=1)])
async def qc_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """切磋，不会掉血"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await qc.finish()
    user_id = user_info['user_id']

    user1 = sql_message.get_user_real_info(user_id)
    give_qq = None  # 艾特的时候存到这里
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if give_qq:
        if give_qq == str(user_id):
            msg = "道友不会左右互搏之术！"
            await handle_send(bot, event, msg)
            await qc.finish()
    else:
        arg = args.extract_plain_text().strip()
        give_info = sql_message.get_user_info_with_name(str(arg))
        give_qq = give_info.get('user_id')
    
    user2 = sql_message.get_user_real_info(give_qq)
    
    if user_info['hp'] is None or user_info['hp'] == 0:
    # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    
    if user_info['hp'] <= user_info['exp'] / 10:
        time = leave_harm_time(user_id)
        msg = f"重伤未愈，动弹不得！距离脱离危险还需要{time}分钟！"
        msg += f"请道友进行闭关，或者使用药品恢复气血，不要干等，没有自动回血！！！"
        sql_message.update_user_stamina(user_id, 20, 1)
        await handle_send(bot, event, msg)
        await qc.finish()
        
    if user1 and user2:
        player1 = sql_message.get_player_data(user1['user_id'])
        player2 = sql_message.get_player_data(user2['user_id'])

        result, victor = Player_fight(player1, player2, 1, bot.self_id)
        await send_msg_handler(bot, event, result)
        msg = f"获胜的是{victor}"
        if victor == "没有人":
            msg = f"{victor}获胜"
        else:
            if victor == player1['道号']:
                update_statistics_value(player1['user_id'], "切磋胜利")
                update_statistics_value(player2['user_id'], "切磋失败")
            else:
                update_statistics_value(player2['user_id'], "切磋胜利")
                update_statistics_value(player1['user_id'], "切磋失败")
        await handle_send(bot, event, msg)
        await qc.finish()
    else:
        msg = "修仙界没有对方的信息，快邀请对方加入修仙界吧！"
        await handle_send(bot, event, msg)
        await qc.finish()

two_exp_invite = on_command("双修", priority=5, block=True)
two_exp_accept = on_command("同意双修", priority=5, block=True)
two_exp_reject = on_command("拒绝双修", priority=5, block=True)

@two_exp_invite.handle(parameterless=[Cooldown(stamina_cost=10, at_sender=False)])
async def handle_two_exp_invite(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """处理双修邀请"""
    global two_exp_invitations
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    isUser, user_1, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    user_1_id = str(user_1['user_id'])
    if user_1_id in two_exp_invitations:
        inviter_id = two_exp_invitations[user_1_id]["inviter_id"]
        inviter_info = sql_message.get_user_real_info(inviter_id)
        if inviter_info:
            msg = f"你正被道友【{inviter_info['user_name']}】邀请双修，请先处理该邀请！(同意双修/拒绝双修)"
        else:
            msg = "你正被他人邀请双修，请先处理该邀请！"
        await handle_send(bot, event, msg)
        await two_exp_invite.finish()

    two_qq = None
    for arg in args:
        if arg.type == "at":
            two_qq = arg.data.get("qq", "")
    if not two_qq:
        await handle_send(bot, event, "请@一位道友发起双修邀请！")
        await two_exp_invite.finish()

    user_2 = sql_message.get_user_real_info(two_qq)
    if not user_2:
        await handle_send(bot, event, "修仙界没有对方的信息，快邀请对方加入修仙界吧！")
        await two_exp_invite.finish()

    if int(user_1['user_id']) == int(two_qq):
        await handle_send(bot, event, "道友无法与自己双修！")
        await two_exp_invite.finish()

    if two_qq in two_exp_invitations:
        await handle_send(bot, event, "对方正在被邀请中，请稍后再试！")
        await two_exp_invite.finish()

    limt_1 = two_exp_cd.find_user(user_1['user_id'])
    limt_2 = two_exp_cd.find_user(user_2['user_id'])

    impart_data_1 = xiuxian_impart.get_user_impart_info_with_id(user_1['user_id'])
    impart_data_2 = xiuxian_impart.get_user_impart_info_with_id(user_2['user_id'])
    impart_two_exp_1 = impart_data_1['impart_two_exp'] if impart_data_1 else 0
    impart_two_exp_2 = impart_data_2['impart_two_exp'] if impart_data_2 else 0

    main_two_data_1 = UserBuffDate(user_1['user_id']).get_user_main_buff_data()
    main_two_data_2 = UserBuffDate(user_2['user_id']).get_user_main_buff_data()
    main_two_1 = main_two_data_1['two_buff'] if main_two_data_1 else 0
    main_two_2 = main_two_data_2['two_buff'] if main_two_data_2 else 0

    total_limit_1 = two_exp_limit + impart_two_exp_1 + main_two_1
    total_limit_2 = two_exp_limit + impart_two_exp_2 + main_two_2

    if limt_1 >= total_limit_1:
        await handle_send(bot, event, "道友今天双修次数已经到达上限！")
        await two_exp_invite.finish()

    if limt_2 >= total_limit_2:
        await handle_send(bot, event, "对方今天双修次数已经到达上限！")
        await two_exp_invite.finish()

    now_time = datetime.now()
    two_exp_invitations[two_qq] = {"inviter_id": user_1_id, "time": now_time}

    await handle_send(bot, event, f"已向道友【{user_2['user_name']}】发送双修邀请，请等待对方回应...")

    msg = MessageSegment.at(
        two_qq) + f" 道友，【{user_1['user_name']}】想与你双修，增进修为。若同意，请在60秒内发送“同意双修”，否则发送“拒绝双修”。"
    await handle_send(bot, event, msg)

    await asyncio.sleep(60)
    if two_qq in two_exp_invitations and two_exp_invitations[two_qq]["inviter_id"] == user_1_id:
        del two_exp_invitations[two_qq]
        await handle_send(bot, event, f"对【{user_2['user_name']}】的双修邀请已超时，自动取消。")


@two_exp_accept.handle()
async def handle_two_exp_accept(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理同意双修"""
    global two_exp_invitations
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    isUser, user_2, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await two_exp_accept.finish()

    user_2_id = str(user_2['user_id'])

    if user_2_id not in two_exp_invitations:
        await handle_send(bot, event, "当前没有道友向你发起双修邀请。")
        await two_exp_accept.finish()

    invitation = two_exp_invitations.pop(user_2_id)
    user_1_id = invitation["inviter_id"]
    user_1 = sql_message.get_user_real_info(user_1_id)

    exp_1 = user_1['exp']
    exp_2 = user_2['exp']
    user1_rank = convert_rank(user_1['level'])[0]

    if exp_2 > exp_1:
        msg = "对方修为远高于你，似乎不屑于此，双修失败！"
        await handle_send(bot, event, msg)
        await two_exp_accept.finish()

    # 基础收益率为2.5%，并确保最低为1点修为
    base_rate = 0.025
    exp = max(1, int((exp_1 + exp_2) * base_rate))

    max_exp_1 = max(1, int(exp_1 * 0.001 * min(0.1 * user1_rank, 1)))
    max_exp_2 = max(1, int(exp_2 * 0.001 * min(0.1 * user1_rank, 1)))

    event_descriptions = [
        f"月明星稀之夜，{user_1['user_name']}与{user_2['user_name']}在灵山之巅相对而坐，双手相抵，周身灵气环绕如雾。",
        f"洞府之中，{user_1['user_name']}与{user_2['user_name']}盘膝对坐，真元交融，形成阴阳鱼图案在两人之间流转。",
    ]
    special_events = [
        f"突然天降异象，七彩祥云笼罩两人，修为大增！",
        f"功法意外产生共鸣，引发天地灵气倒灌！",
    ]

    event_desc = random.choice(event_descriptions)

    two_exp_cd.add_user(user_1['user_id'])
    two_exp_cd.add_user(user_2['user_id'])
    update_statistics_value(user_1['user_id'], "双修次数")
    update_statistics_value(user_2['user_id'], "双修次数")

    # 特殊事件，收益翻倍
    if random.randint(1, 100) <= 15:
        exp = exp * 2  # 收益翻倍
        exp_limit_1 = min(exp, max_exp_1)
        exp_limit_2 = min(exp, max_exp_2)

        sql_message.update_exp(user_1['user_id'], exp_limit_1)
        sql_message.update_exp(user_2['user_id'], exp_limit_2)
        sql_message.update_levelrate(user_1['user_id'], user_1['level_up_rate'] + 2)
        sql_message.update_levelrate(user_2['user_id'], user_2['level_up_rate'] + 2)

        msg = (f"你同意了{user_1['user_name']}的双修邀请。\n{event_desc}\n{random.choice(special_events)}\n"
               f"你增加了修为 {number_to(exp_limit_2)}！\n"
               f"{user_1['user_name']} 增加了修为 {number_to(exp_limit_1)}！\n"
               f"双方各增加突破概率2%。")
    else:
        exp_limit_1 = min(exp, max_exp_1)
        exp_limit_2 = min(exp, max_exp_2)

        sql_message.update_exp(user_1['user_id'], exp_limit_1)
        sql_message.update_exp(user_2['user_id'], exp_limit_2)

        msg = (f"你同意了{user_1['user_name']}的双修邀请。\n{event_desc}\n"
               f"你增加了修为 {number_to(exp_limit_2)}！\n"
               f"{user_1['user_name']} 增加了修为 {number_to(exp_limit_1)}！")

    sql_message.update_power2(user_1['user_id'])
    sql_message.update_power2(user_2['user_id'])

    await handle_send(bot, event, msg)
    log_message(user_1['user_id'], msg)
    log_message(user_2['user_id'], msg)


@two_exp_reject.handle()
async def handle_two_exp_reject(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理拒绝双修"""
    global two_exp_invitations
    bot, send_group_id = await assign_bot(bot=bot, event=event)

    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await two_exp_reject.finish()

    user_id = str(user_info['user_id'])

    if user_id in two_exp_invitations:
        invitation = two_exp_invitations.pop(user_id)
        inviter_id = invitation["inviter_id"]
        inviter_info = sql_message.get_user_real_info(inviter_id)

        rejection_msg = f"你拒绝了道友【{inviter_info['user_name']}】的双修邀请。"
        await handle_send(bot, event, rejection_msg)

        # 给邀请者发送一个被拒绝的通知
        try:
            await bot.send_private_msg(user_id=int(inviter_id),
                                       message=f"道友【{user_info['user_name']}】拒绝了你的双修邀请。")
        except Exception as e:
            logger.warning(f"无法向 {inviter_id} 发送私聊通知: {e}")

    else:
        await handle_send(bot, event, "当前没有道友向你发起双修邀请。")

@reset_exp.handle(parameterless=[Cooldown(at_sender=False, cd_time=60)])
async def reset_exp_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置修炼状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 5  # 状态5为修炼
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await reset_exp.finish()
    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, user_type)
    if not is_type:
        await handle_send(bot, event, msg)
        await up_exp.finish()
    msg = "请等待一分钟生效即可！"
    await handle_send(bot, event, msg)
    await asyncio.sleep(60)
    is_type, msg = check_user_type(user_id, user_type)
    if is_type:
        sql_message.in_closing(user_id, 0)
        msg = "已重置修炼状态！"
        await handle_send(bot, event, msg)
    await reset_exp.finish()
        
    
@up_exp.handle(parameterless=[Cooldown(at_sender=False, cd_time=60)])
async def up_exp_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 5  # 状态5为修炼
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await up_exp.finish()
    user_id = user_info['user_id']
    user_mes = sql_message.get_user_info_with_id(user_id)  # 获取用户信息
    level = user_mes['level']
    use_exp = user_mes['exp']

    max_exp = (
            int(OtherSet().set_closing_type(level)) * XiuConfig().closing_exp_upper_limit
    )  # 获取下个境界需要的修为 * 1.5为闭关上限
    user_get_exp_max = int(max_exp) - use_exp

    if user_get_exp_max < 0:
        # 校验当当前修为超出上限的问题，不可为负数
        user_get_exp_max = 0

    now_time = datetime.now()
    user_cd_message = sql_message.get_user_cd(user_id)
    is_type, msg = check_user_type(user_id, 0)
    if not is_type:
        await handle_send(bot, event, msg)
        await up_exp.finish()
    else:
        level_rate = sql_message.get_root_rate(user_mes['root_type'], user_id)  # 灵根倍率
        realm_rate = jsondata.level_data()[level]["spend"]  # 境界倍率
        user_buff_data = UserBuffDate(user_id)
        user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5
        mainbuffdata = user_buff_data.get_user_main_buff_data()
        mainbuffratebuff = mainbuffdata['ratebuff'] if mainbuffdata != None else 0  # 功法修炼倍率
        mainbuffcloexp = mainbuffdata['clo_exp'] if mainbuffdata != None else 0  # 功法闭关经验
        mainbuffclors = mainbuffdata['clo_rs'] if mainbuffdata != None else 0  # 功法闭关回复
        
        exp = int(
            XiuConfig().closing_exp * ((level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp) * (1 + user_blessed_spot_data)))
            # 洞天福地为加法
        )  # 本次闭关获取的修为
        # 计算传承增益
        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        impart_exp_up = impart_data['impart_exp_up'] if impart_data is not None else 0
        exp = int(exp * (1 + impart_exp_up))
        exp_rate = random.uniform(0.9, 1.3)
        exp = int(exp * exp_rate)
        sql_message.in_closing(user_id, user_type)
        if user_info['root_type'] == '伪灵根':
            msg = f"开始挖矿⛏️！【{user_info['user_name']}开始挖矿】\n挥起玄铁镐砸向发光岩壁\n碎石里蹦出带灵气的矿石\j预计时间：60秒"
            await handle_send(bot, event, msg)
            await asyncio.sleep(60)
            give_stone = random.randint(10000, 300000)
            give_stone_num = int(give_stone * exp_rate)
            sql_message.update_ls(user_info['user_id'], give_stone_num, 1)  # 增加用户灵石
            msg = f"挖矿结束，增加灵石：{give_stone_num}"
            await handle_send(bot, event, msg)
            await up_exp.finish()
        else:
            msg = f"【{user_info['user_name']}开始修炼】\n盘膝而坐，五心朝天，闭目凝神，渐入空明之境...\n周身灵气如涓涓细流汇聚，在经脉中缓缓流转\n丹田内真元涌动，与天地灵气相互呼应\n渐入佳境，物我两忘，进入深度修炼状态\n预计修炼时间：60秒"
        await handle_send(bot, event, msg)
        await asyncio.sleep(60)
        update_statistics_value(user_id, "修炼次数")
        user_type = 0  # 状态0为无事件
        if exp >= user_get_exp_max:
            # 用户获取的修为到达上限
            sql_message.in_closing(user_id, user_type)
            sql_message.update_exp(user_id, user_get_exp_max)
            sql_message.update_power2(user_id)  # 更新战力

            result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10), int(use_exp / 20))
            sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10))
            msg = f"修炼结束，本次修炼到达上限，共增加修为：{number_to(user_get_exp_max)}{result_msg[0]}{result_msg[1]}"
            await handle_send(bot, event, msg)
            await up_exp.finish()
        else:
            # 用户获取的修为没有到达上限
            sql_message.in_closing(user_id, user_type)
            sql_message.update_exp(user_id, exp)
            sql_message.update_power2(user_id)  # 更新战力
            result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10), int(use_exp / 20))
            sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10))
            msg = f"修炼结束，增加修为：{number_to(exp)}{result_msg[0]}{result_msg[1]}"
            await handle_send(bot, event, msg)
            await up_exp.finish()

 
@stone_exp.handle(parameterless=[Cooldown(at_sender=False)])
async def stone_exp_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """灵石修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await stone_exp.finish()
    user_id = user_info['user_id']
    user_mes = sql_message.get_user_info_with_id(user_id)  # 获取用户信息
    level = user_mes['level']
    use_exp = user_mes['exp']
    use_stone = user_mes['stone']
    max_exp = (
            int(OtherSet().set_closing_type(level)) * XiuConfig().closing_exp_upper_limit
    )  # 获取下个境界需要的修为 * 1.5为闭关上限
    user_get_exp_max = int(max_exp) - use_exp

    if user_get_exp_max < 0:
        # 校验当当前修为超出上限的问题，不可为负数
        user_get_exp_max = 0

    msg = args.extract_plain_text().strip()
    stone_num = re.findall(r"\d+", msg)  # 灵石数

    if stone_num:
        pass
    else:
        msg = "请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await stone_exp.finish()
    stone_num = int(stone_num[0])
    if use_stone <= stone_num:
        msg = "你的灵石还不够呢，快去赚点灵石吧！"
        await handle_send(bot, event, msg)
        await stone_exp.finish()

    exp = int(stone_num / 10)
    if exp >= user_get_exp_max:
        # 用户获取的修为到达上限
        sql_message.update_exp(user_id, user_get_exp_max)
        sql_message.update_power2(user_id)  # 更新战力
        msg = f"修炼结束，本次修炼到达上限，共增加修为：{user_get_exp_max},消耗灵石：{user_get_exp_max * 10}"
        sql_message.update_ls(user_id, int(user_get_exp_max * 10), 2)
        update_statistics_value(user_id, "灵石修炼", increment=user_get_exp_max * 10)
        await handle_send(bot, event, msg)
        await stone_exp.finish()
    else:
        sql_message.update_exp(user_id, exp)
        sql_message.update_power2(user_id)  # 更新战力
        msg = f"修炼结束，本次修炼共增加修为：{exp},消耗灵石：{stone_num}"
        sql_message.update_ls(user_id, int(stone_num), 2)
        update_statistics_value(user_id, "灵石修炼", increment=stone_num)
        await handle_send(bot, event, msg)
        await stone_exp.finish()


@in_closing.handle(parameterless=[Cooldown(at_sender=False)])
async def in_closing_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """闭关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 1  # 状态0为无事件
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await in_closing.finish()
    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, 0)
    if user_info['root_type'] == '伪灵根':
        msg = "凡人无法闭关！"
        await handle_send(bot, event, msg)
        await in_closing.finish()
    if is_type:  # 符合
        sql_message.in_closing(user_id, user_type)
        msg = "进入闭关状态，如需出关，发送【出关】！"
        await handle_send(bot, event, msg)
        await in_closing.finish()
    else:
        await handle_send(bot, event, msg)
        await in_closing.finish()


@out_closing.handle(parameterless=[Cooldown(at_sender=False)])
async def out_closing_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """出关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 0  # 状态0为无事件
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await out_closing.finish()
    user_id = user_info['user_id']
    user_mes = sql_message.get_user_info_with_id(user_id)  # 获取用户信息
    level = user_mes['level']
    use_exp = user_mes['exp']

    max_exp = (
            int(OtherSet().set_closing_type(level)) * XiuConfig().closing_exp_upper_limit
    )  # 获取下个境界需要的修为 * 1.5为闭关上限
    user_get_exp_max = int(max_exp) - use_exp

    if user_get_exp_max < 0:
        # 校验当当前修为超出上限的问题，不可为负数
        user_get_exp_max = 0

    now_time = datetime.now()
    user_cd_message = sql_message.get_user_cd(user_id)
    is_type, msg = check_user_type(user_id, 1)
    if not is_type:
        await handle_send(bot, event, msg)
        await out_closing.finish()
    else:
        # 用户状态为1
        in_closing_time = datetime.strptime(
            user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
        )  # 进入闭关的时间
        exp_time = (
                OtherSet().date_diff(now_time, in_closing_time) // 60
        )  # 闭关时长计算(分钟) = second // 60
        level_rate = sql_message.get_root_rate(user_mes['root_type'], user_id)  # 灵根倍率
        realm_rate = jsondata.level_data()[level]["spend"]  # 境界倍率
        user_buff_data = UserBuffDate(user_id)
        user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5
        mainbuffdata = user_buff_data.get_user_main_buff_data()
        mainbuffratebuff = mainbuffdata['ratebuff'] if mainbuffdata != None else 0  # 功法修炼倍率
        mainbuffcloexp = mainbuffdata['clo_exp'] if mainbuffdata != None else 0  # 功法闭关经验
        mainbuffclors = mainbuffdata['clo_rs'] if mainbuffdata != None else 0  # 功法闭关回复
        
        exp = int(
            (exp_time * XiuConfig().closing_exp) * ((level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp) * (1 + user_blessed_spot_data)))
            # 洞天福地为加法
        )  # 本次闭关获取的修为
        # 计算传承增益
        impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
        impart_exp_up = impart_data['impart_exp_up'] if impart_data is not None else 0
        exp = int(exp * (1 + impart_exp_up))
        base_exp_rate = f"{int((level_rate + mainbuffratebuff + mainbuffcloexp + user_blessed_spot_data + impart_exp_up) * 100)}%"
        if exp >= user_get_exp_max:
            # 用户获取的修为到达上限
            sql_message.in_closing(user_id, user_type)
            sql_message.update_exp(user_id, user_get_exp_max)
            sql_message.update_power2(user_id)  # 更新战力

            result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time))
            sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10))
            msg = f"闭关结束，本次闭关到达上限，共增加修为：{number_to(user_get_exp_max)}{result_msg[0]}{result_msg[1]}"
            update_statistics_value(user_id, "闭关时长", increment=exp_time)
            await handle_send(bot, event, msg)
            await out_closing.finish()
        else:
            # 用户获取的修为没有到达上限
            if str(event.message) == "灵石出关":
                user_stone = user_mes['stone']  # 用户灵石数
                if user_stone <= 0:
                    user_stone = 0
                if exp <= user_stone:
                    exp = exp * 2
                    sql_message.in_closing(user_id, user_type)
                    sql_message.update_exp(user_id, exp)
                    sql_message.update_ls(user_id, int(exp / 2), 2)
                    sql_message.update_power2(user_id)  # 更新战力

                    result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time))
                    sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1],
                                                      int(result_hp_mp[2] / 10))
                    msg = f"闭关结束，共闭关{exp_time}分钟，本次闭关增加修为：{number_to(exp)}(修炼效率：{base_exp_rate})，消耗灵石{int(exp / 2)}枚{result_msg[0]}{result_msg[1]}"
                    update_statistics_value(user_id, "闭关时长", increment=exp_time)
                    await handle_send(bot, event, msg)
                    await out_closing.finish()
                else:
                    exp = exp + user_stone
                    sql_message.in_closing(user_id, user_type)
                    sql_message.update_exp(user_id, exp)
                    sql_message.update_ls(user_id, user_stone, 2)
                    sql_message.update_power2(user_id)  # 更新战力
                    result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time))
                    sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1],
                                                      int(result_hp_mp[2] / 10))
                    msg = f"闭关结束，共闭关{exp_time}分钟，本次闭关增加修为：{number_to(exp)}(修炼效率：{base_exp_rate})，消耗灵石{user_stone}枚{result_msg[0]}{result_msg[1]}"
                    update_statistics_value(user_id, "闭关时长", increment=exp_time)
                    await handle_send(bot, event, msg)
                    await out_closing.finish()
            else:
                sql_message.in_closing(user_id, user_type)
                sql_message.update_exp(user_id, exp)
                sql_message.update_power2(user_id)  # 更新战力
                result_msg, result_hp_mp = OtherSet().send_hp_mp(user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time))
                sql_message.update_user_attribute(user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10))
                msg = f"闭关结束，共闭关{exp_time}分钟，本次闭关增加修为：{number_to(exp)}(修炼效率：{base_exp_rate}){result_msg[0]}{result_msg[1]}"
                update_statistics_value(user_id, "闭关时长", increment=exp_time)
                await handle_send(bot, event, msg)
                await out_closing.finish()


@mind_state.handle(parameterless=[Cooldown(at_sender=False)])
async def mind_state_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的状态信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_msg, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await mind_state.finish()
    user_id = user_msg['user_id']
    sql_message.update_last_check_info_time(user_id) # 更新查看修仙信息时间
    if user_msg['hp'] is None or user_msg['hp'] == 0:
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_real_info(user_id)

    level_rate = sql_message.get_root_rate(user_msg['root_type'], user_id)  # 灵根倍率
    realm_rate = jsondata.level_data()[user_msg['level']]["spend"]  # 境界倍率
    user_buff_data = UserBuffDate(user_id)
    user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5
    main_buff_data = user_buff_data.get_user_main_buff_data()
    user_armor_crit_data = user_buff_data.get_user_armor_buff_data() #我的状态防具会心
    user_weapon_data = UserBuffDate(user_id).get_user_weapon_data() #我的状态武器减伤
    user_main_crit_data = UserBuffDate(user_id).get_user_main_buff_data() #我的状态功法会心
    user_main_data = UserBuffDate(user_id).get_user_main_buff_data() #我的状态功法减伤
    
    if user_main_data is not None:
        main_def = user_main_data['def_buff'] * 100 #我的状态功法减伤
    else:
        main_def = 0
    
    if user_armor_crit_data is not None: #我的状态防具会心
        armor_crit_buff = ((user_armor_crit_data['crit_buff']) * 100)
    else:
        armor_crit_buff = 0
        
    if user_weapon_data is not None:
        crit_buff = ((user_weapon_data['crit_buff']) * 100)
    else:
        crit_buff = 0

    user_armor_data = user_buff_data.get_user_armor_buff_data()
    if user_armor_data is not None:
        def_buff = int(user_armor_data['def_buff'] * 100) #我的状态防具减伤
    else:
        def_buff = 0
    
    user_armor_data = user_buff_data.get_user_armor_buff_data()
    
    if user_weapon_data is not None:
        weapon_def = user_weapon_data['def_buff'] * 100 #我的状态武器减伤
    else:
        weapon_def = 0

    if user_main_crit_data is not None: #我的状态功法会心
        main_crit_buff = ((user_main_crit_data['crit_buff']) * 100)
    else:
        main_crit_buff = 0
    
    list_all = len(OtherSet().level) - 1
    now_index = OtherSet().level.index(user_msg['level'])
    if list_all == now_index:
        exp_meg = f"位面至高"
    else:
        is_updata_level = OtherSet().level[now_index + 1]
        need_exp = sql_message.get_level_power(is_updata_level)
        get_exp = need_exp - user_msg['exp']
        if get_exp > 0:
            exp_meg = f"还需{number_to(get_exp)}修为可突破！"
        else:
            exp_meg = f"可突破！"
    
    main_buff_rate_buff = main_buff_data['ratebuff'] if main_buff_data is not None else 0
    main_hp_buff = main_buff_data['hpbuff'] if main_buff_data is not None else 0
    main_mp_buff = main_buff_data['mpbuff'] if main_buff_data is not None else 0
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_hp_per = impart_data['impart_hp_per'] if impart_data is not None else 0
    impart_mp_per = impart_data['impart_mp_per'] if impart_data is not None else 0
    impart_know_per = impart_data['impart_know_per'] if impart_data is not None else 0
    impart_burst_per = impart_data['impart_burst_per'] if impart_data is not None else 0
    boss_atk = impart_data['boss_atk'] if impart_data is not None else 0
    hppractice = user_msg['hppractice'] * 0.05 if user_msg['hppractice'] is not None else 0
    mppractice = user_msg['mppractice'] * 0.05 if user_msg['mppractice'] is not None else 0  
    weapon_critatk_data = UserBuffDate(user_id).get_user_weapon_data() #我的状态武器会心伤害
    weapon_critatk = weapon_critatk_data['critatk'] if weapon_critatk_data is not None else 0 #我的状态武器会心伤害
    user_main_critatk = UserBuffDate(user_id).get_user_main_buff_data() #我的状态功法会心伤害
    main_critatk =  user_main_critatk['critatk'] if  user_main_critatk is not None else 0 #我的状态功法会心伤害
    user_js = def_buff + weapon_def + main_def
    leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    number =  user_main_critatk["number"] if user_main_critatk is not None else 0
    
    msg = f"""
道号：{user_msg['user_name']}
气血:{number_to(user_msg['hp'])}/{number_to(int((user_msg['exp'] / 2) * (1 + main_hp_buff + impart_hp_per + hppractice)))}({((user_msg['hp'] / ((user_msg['exp'] / 2) * (1 + main_hp_buff + impart_hp_per + hppractice)))) * 100:.2f}%)
真元:{number_to(user_msg['mp'])}/{number_to(user_msg['exp'])}({((user_msg['mp'] / user_msg['exp']) * 100):.2f}%)
攻击:{number_to(user_msg['atk'])}
突破状态: {exp_meg}(概率：{jsondata.level_rate_data()[user_msg['level']] + leveluprate + number}%)
攻击修炼:{user_msg['atkpractice']}级(提升攻击力{user_msg['atkpractice'] * 4}%)
元血修炼:{user_msg['hppractice']}级(提升气血{user_msg['hppractice'] * 8}%)
灵海修炼:{user_msg['mppractice']}级(提升真元{user_msg['mppractice'] * 5}%)
修炼效率:{int(((level_rate * realm_rate) * (1 + main_buff_rate_buff) * (1+ user_blessed_spot_data)) * 100)}%
会心:{int(crit_buff + int(impart_know_per * 100) + armor_crit_buff + main_crit_buff)}%
减伤率:{user_js}%
boss战增益:{int(boss_atk * 100)}%
会心伤害增益:{int((1.5 + impart_burst_per + weapon_critatk + main_critatk) * 100)}%
"""
    sql_message.update_last_check_info_time(user_id)
    await handle_send(bot, event, msg)
    await mind_state.finish()


@buffinfo.handle(parameterless=[Cooldown(at_sender=False)])
async def buffinfo_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的功法"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await buffinfo.finish()

    user_id = user_info['user_id']
    mainbuffdata = UserBuffDate(user_id).get_user_main_buff_data()
    if mainbuffdata != None:
        s, mainbuffmsg = get_main_info_msg(str(get_user_buff(user_id)['main_buff']))
    else:
        mainbuffmsg = ''
        
    subbuffdata = UserBuffDate(user_id).get_user_sub_buff_data()#辅修功法13
    if subbuffdata != None:
        sub, subbuffmsg = get_sub_info_msg(str(get_user_buff(user_id)['sub_buff']))
    else:
        subbuffmsg = ''
        
    effect1buffdata = UserBuffDate(user_id).get_user_effect1_buff_data()
    if effect1buffdata != None:
        effect1, effect1buffmsg = get_effect_info_msg(str(get_user_buff(user_id)['effect1_buff']))
    else:
        effect1buffmsg = ''
        
    effect2buffdata = UserBuffDate(user_id).get_user_effect2_buff_data()
    if effect2buffdata != None:
        effect2, effect2buffmsg = get_effect_info_msg(str(get_user_buff(user_id)['effect2_buff']))
    else:
        effect2buffmsg = ''
        
    secbuffdata = UserBuffDate(user_id).get_user_sec_buff_data()
    secbuffmsg = get_sec_msg(secbuffdata) if get_sec_msg(secbuffdata) != '无' else ''
    msg = f"""
主功法：{mainbuffdata["name"] if mainbuffdata != None else '无'}
{mainbuffmsg}

辅修功法：{subbuffdata["name"] if subbuffdata != None else '无'}
{subbuffmsg}

神通：{secbuffdata["name"] if secbuffdata != None else '无'}
{secbuffmsg}

身法：{effect1buffdata["name"] if effect1buffdata != None else '无'}
{effect1buffmsg}

瞳术：{effect2buffdata["name"] if effect2buffdata != None else '无'}
{effect2buffmsg}
"""

    await handle_send(bot, event, msg)
    await buffinfo.finish()


@del_exp_decimal.handle(parameterless=[Cooldown(at_sender=False)])
async def del_exp_decimal_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """清除修为浮点数"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await del_exp_decimal.finish()
    user_id = user_info['user_id']
    exp = user_info['exp']
    sql_message.del_exp_decimal(user_id, exp)
    msg = f"黑暗动乱暂时抑制成功！"
    await handle_send(bot, event, msg)
    await del_exp_decimal.finish()


@my_exp_num.handle(parameterless=[Cooldown(at_sender=False)])
async def my_exp_num_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的双修次数"""
    global two_exp_limit
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await my_exp_num.finish()
    user_id = user_info['user_id']
    limt = two_exp_cd.find_user(user_id)
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_two_exp = impart_data['impart_two_exp'] if impart_data is not None else 0
    
    main_two_data = UserBuffDate(user_id).get_user_main_buff_data()
    main_two = main_two_data['two_buff'] if main_two_data is not None else 0
    
    num = (two_exp_limit + impart_two_exp + main_two) - limt
    if num <= 0:
        num = 0
    msg = f"道友剩余双修次数{num}次！"
    await handle_send(bot, event, msg)
    await my_exp_num.finish()
