from nonebot import on_command, require, on_fullmatch
from nonebot.params import CommandArg
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    ActionFailed
)
import random
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.data_source import jsondata
from nonebot.log import logger
from datetime import datetime
from ..xiuxian_utils.utils import check_user, get_msg_pic, send_msg_handler, handle_send, check_user_type
from .impart_pk_uitls import impart_pk_check
from .xu_world import xu_world
from .impart_pk import impart_pk
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, OtherSet, UserBuffDate, XIUXIAN_IMPART_BUFF
from .. import NICKNAME
from nonebot.log import logger
xiuxian_impart = XIUXIAN_IMPART_BUFF()
sql_message = XiuxianDateManage()  # sql类

impart_re = require("nonebot_plugin_apscheduler").scheduler

impart_pk_project = on_fullmatch("投影虚神界", priority=6, block=True)
impart_pk_go = on_fullmatch("深入虚神界", priority=6, block=True)
impart_pk_now = on_command("虚神界对决", priority=15, block=True)
impart_pk_list = on_fullmatch("虚神界列表", priority=7, block=True)
impart_pk_exp = on_command("虚神界修炼", priority=8, block=True)
impart_pk_out_closing = on_command("虚神界出关", priority=8, block=True)
impart_pk_in_closing = on_command("虚神界闭关", priority=8, block=True)

# 每日0点重置用虚神界次数和等级
@impart_re.scheduled_job("cron", hour=0, minute=0)
async def impart_re_():
    impart_pk.re_data()
    xu_world.re_data()
    xiuxian_impart.update_impart_lv_reset
    logger.opt(colors=True).info(f"<green>已重置虚神界次数和等级</green>")


@impart_pk_project.handle(parameterless=[Cooldown(stamina_cost = 1, at_sender=False)])
async def impart_pk_project_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """投影虚神界"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_project.finish()
    user_id = user_info['user_id']
    impart_data_draw = await impart_pk_check(user_id)
    if impart_data_draw is None:
        msg = f"发生未知错误，多次尝试无果请找晓楠！"
        await handle_send(bot, event, msg)
        await impart_pk_project.finish()
    # 加入虚神界
    if impart_pk.find_user_data(user_id)["pk_num"] <= 0:
        msg = f"道友今日次数已用尽，无法在加入虚神界！"
        await handle_send(bot, event, msg)
        await impart_pk_project.finish()
    msg = xu_world.add_xu_world(user_id)
    await handle_send(bot, event, msg)
    await impart_pk_project.finish()


@impart_pk_list.handle(parameterless=[Cooldown(at_sender=False)])
async def impart_pk_list_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """虚神界列表"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_list.finish()
    user_id = user_info['user_id']
    impart_data_draw = await impart_pk_check(user_id)
    if impart_data_draw is None:
        msg = f"发生未知错误，多次尝试无果请找晓楠！"
        await handle_send(bot, event, msg)
        await impart_pk_list.finish()
    xu_list = xu_world.all_xu_world_user()
    if len(xu_list) == 0:
        msg = f"虚神界里还没有投影呢，快来输入【投影虚神界】加入分身吧！"
        await handle_send(bot, event, msg)
        await impart_pk_list.finish()
    list_msg = []
    win_num = "win_num"
    pk_num = "pk_num"
    for x in range(len(xu_list)):
        user_data = impart_pk.find_user_data(xu_list[x])
        if user_data:
            name = sql_message.get_user_info_with_id(xu_list[x])['user_name']
            msg = ""
            msg += f"编号：{user_data['number']}\n"
            msg += f"道友：{name}\n"
            msg += f"胜场：{user_data[win_num]}\n"
            msg += f"剩余决斗次数：{user_data[pk_num]}"
            list_msg.append(
                {"type": "node", "data": {"name": f"编号 {x}", "uin": bot.self_id,
                                          "content": msg}})
    try:
        await send_msg_handler(bot, event, list_msg)
    except ActionFailed:
        msg = f"未知原因，查看失败!"
        await handle_send(bot, event, msg)
        await impart_pk_list.finish()
    await impart_pk_list.finish()


@impart_pk_now.handle(parameterless=[Cooldown(stamina_cost=3, at_sender=False)])
async def impart_pk_now_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """虚神界对决"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()
    
    user_id = user_info['user_id']
    sql_message.update_last_check_info_time(user_id)  # 更新查看修仙信息时间
    impart_data_draw = await impart_pk_check(user_id)
    if impart_data_draw is None:
        msg = f"发生未知错误，多次尝试无果请找晓楠！"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    num = args.extract_plain_text().strip()
    user_data = impart_pk.find_user_data(user_info['user_id'])

    if user_data["pk_num"] <= 0:
        msg = f"道友今日次数耗尽，明天再来吧！"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    player_1_stones = 0
    player_2_stones = 0
    combined_msg = ""
    list_msg = []

    if not num:
        if user_data["pk_num"] > 0:
            msg, win = await impart_pk_uitls.impart_pk_now_msg_to_bot(user_info['user_name'], NICKNAME)
            if win == 1:
                msg += f"战报：道友{user_info['user_name']}获胜,获得思恋结晶20颗\n"
                impart_pk.update_user_data(user_info['user_id'], True)
                xiuxian_impart.update_stone_num(20, user_id, 1)
                player_1_stones += 20
            elif win == 2:
                msg += f"战报：道友{user_info['user_name']}败了,消耗一次次数,获得思恋结晶10颗\n"
                impart_pk.update_user_data(user_info['user_id'], False)
                xiuxian_impart.update_stone_num(10, user_id, 1)
                player_1_stones += 10
                if impart_pk.find_user_data(user_id)["pk_num"] <= 0 and xu_world.check_xu_world_user_id(user_id) is True:
                    msg += "检测到道友次数已用尽，已帮助道友退出虚神界！"
                    xu_world.del_xu_world(user_id)
            else:
                msg = f"挑战失败"
                combined_msg += f"{msg}\n"

            combined_msg += f"☆--------⚔️对决⚔️--------☆\n{msg}\n"
            user_data = impart_pk.find_user_data(user_info['user_id'])

        combined_msg += f"总计：道友{user_info['user_name']}获得思恋结晶{player_1_stones}颗\n"
        list_msg.append(
                {"type": "node", "data": {"name": f"虚神界对决", "uin": bot.self_id,
                                          "content": combined_msg}})
        await send_msg_handler(bot, event, list_msg)
        await impart_pk_now.finish()

    if not num.isdigit():
        msg = f"编号解析异常，应全为数字!"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    num = int(num) - 1
    xu_world_list = xu_world.all_xu_world_user()

    if num + 1 > len(xu_world_list) or num < 0:
        msg = f"编号解析异常，虚神界没有此编号道友!"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    player_1 = user_info['user_id']
    player_2 = xu_world_list[num]
    if str(player_1) == str(player_2):
        msg = f"道友不能挑战自己的投影!"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    player_1_name = user_info['user_name']
    player_2_name = sql_message.get_user_info_with_id(player_2)['user_name']

    if user_data["pk_num"] > 0:
        msg_list, win = await impart_pk_uitls.impart_pk_now_msg(player_1, player_1_name, player_2, player_2_name)
        if win is None:
            msg = f"挑战失败"
            combined_msg += f"{msg}\n"

        if win == 1:  # 1号玩家胜利 发起者
            impart_pk.update_user_data(player_1, True)
            impart_pk.update_user_data(player_2, False)
            xiuxian_impart.update_stone_num(20, player_1, 1)
            xiuxian_impart.update_stone_num(10, player_2, 1)
            player_1_stones += 20
            player_2_stones += 10
            msg_list.append(
                {"type": "node", "data": {"name": f"虚神界战报", "uin": bot.self_id,
                                          "content": f"道友{player_1_name}获得了胜利,获得了思恋结晶20!\n"
                                                     f"道友{player_2_name}获得败了,消耗一次次数,获得了思恋结晶10颗!"}})
            if impart_pk.find_user_data(player_2)["pk_num"] <= 0:
                msg_list.append(
                    {"type": "node", "data": {"name": f"虚神界变更", "uin": bot.self_id,
                                              "content": f"道友{player_2_name}次数耗尽，离开了虚神界！"}})
                xu_world.del_xu_world(player_2)
                combined_msg += "\n".join([node['data']['content'] for node in msg_list])
        elif win == 2:  # 2号玩家胜利 被挑战者
            impart_pk.update_user_data(player_2, True)
            impart_pk.update_user_data(player_1, False)
            xiuxian_impart.update_stone_num(20, player_2, 1)
            xiuxian_impart.update_stone_num(10, player_1, 1)
            player_2_stones += 20
            player_1_stones += 10
            msg_list.append(
                {"type": "node", "data": {"name": f"虚神界战报", "uin": bot.self_id,
                                          "content": f"道友{player_2_name}获得了胜利,获得了思恋结晶20颗!\n"
                                                     f"道友{player_1_name}获得败了,消耗一次次数,获得了思恋结晶10颗!"}})
            if impart_pk.find_user_data(player_1)["pk_num"] <= 0:
                msg_list.append(
                    {"type": "node", "data": {"name": f"虚神界变更", "uin": bot.self_id,
                                              "content": f"道友{player_1_name}次数耗尽，离开了虚神界！"}})
                xu_world.del_xu_world(player_1)
                combined_msg += "\n".join([node['data']['content'] for node in msg_list])

        combined_msg += f"☆--------⚔️对决⚔️--------☆\n" + "\n".join([node['data']['content'] for node in msg_list]) + "\n"

        try:
            await send_msg_handler(bot, event, msg_list)
        except ActionFailed:
            msg = f"未知原因，对决显示失败!"
            combined_msg += f"{msg}\n"

        user_data = impart_pk.find_user_data(user_info['user_id'])

        combined_msg += f"总计：道友{player_1_name}获得思恋结晶{player_1_stones}颗, 道友{player_2_name}获得思恋结晶{player_2_stones}颗\n"

        list_msg.append(
                {"type": "node", "data": {"name": f"虚神界对决", "uin": bot.self_id,
                                          "content": combined_msg}})
        await send_msg_handler(bot, event, list_msg)
        await impart_pk_now.finish()


@impart_pk_exp.handle(parameterless=[Cooldown(at_sender=False)])
async def impart_pk_exp_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """虚神界修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()
    user_id = user_info['user_id']
    impart_data_draw = await impart_pk_check(user_id)
    if impart_data_draw is None:
        msg = f"发生未知错误，多次尝试无果请找晓楠！"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()

    level = user_info['level']
    hp_speed = 25
    mp_speed = 50

    impaer_exp_time = args.extract_plain_text().strip()
    if not impaer_exp_time.isdigit():
        impaer_exp_time = 1

    closing_type = OtherSet().set_closing_type(user_info['level'])
    max_exp = closing_type * XiuConfig().closing_exp_upper_limit
    current_exp = user_info['exp']

    if int(impaer_exp_time) > int(impart_data_draw['exp_day']):
        msg = f"累计时间不足，修炼失败!"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()

    if user_info['root_type'] == '伪灵根':
        msg = f"器师无法进行修炼!"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()

    # 计算本次修炼经验
    level_rate = sql_message.get_root_rate(user_info['root_type'])  # 灵根倍率
    realm_rate = jsondata.level_data()[level]["spend"]  # 境界倍率
    user_buff_data = UserBuffDate(user_id)
    mainbuffdata = user_buff_data.get_user_main_buff_data()
    mainbuffratebuff = mainbuffdata['ratebuff'] if mainbuffdata is not None else 0  # 功法修炼倍率
    mainbuffcloexp = mainbuffdata['clo_exp'] if mainbuffdata != None else 0  # 功法闭关经验
    mainbuffclors = mainbuffdata['clo_rs'] if mainbuffdata != None else 0  # 功法闭关回复
    exp = int((int(impaer_exp_time) * XiuConfig().closing_exp) * ((level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp))))  # 本次闭关获取的修为
    
    if int(impaer_exp_time) == 1:
        if current_exp + exp > max_exp:
            exp = max((max_exp - current_exp), 1)

    exp = int(round(exp))
    # 校验是否超出上限
    if current_exp + exp > max_exp:
        allowed_time = (max_exp - current_exp) // (XiuConfig().closing_exp * ((level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp))))
        allowed_time = max(int(allowed_time), 1)
        exp2 = max((max_exp - current_exp), 1)
        if current_exp + exp2 > max_exp:
            allowed_time = 0
        msg = f"修炼时长超出上限，最多可修炼{allowed_time}分钟"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()
    else:
        # 更新经验并返回成功
        xiuxian_impart.use_impart_exp_day(impaer_exp_time, user_id)
        sql_message.update_exp(user_id, exp)
        sql_message.update_power2(user_id)  # 更新战力
        sql_message.update_user_attribute(
            user_id, 
            result_hp_mp[0], 
            result_hp_mp[1], 
            int(result_hp_mp[2] / 10)
        )
        msg = f"虚神界修炼结束，共修炼{impaer_exp_time}分钟，本次闭关增加修为：{exp}"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()

@impart_pk_go.handle(parameterless=[Cooldown(at_sender=False)])
async def impart_pk_go_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """深入虚神界"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_go.finish()
    user_id = user_info['user_id']
    user_data = impart_pk.find_user_data(user_info['user_id'])
    if user_data["impart_lv"] <= 0:
        msg = f"道友今日次数耗尽，明天再来吧！"
        await handle_send(bot, event, msg)
        await impart_pk_go.finish()
    impart_data_draw = await impart_pk_check(user_id)
    impart_lv = impart_data_draw['impart_lv'] if impart_data_draw is not None else 0
    impart_level = {0:"边缘",1:"外层",2:"中层",3:"里层",4:"深层",5:"核心",6:"核心10%",7:"核心30%",8:"核心60%",9:"核心99%",10:"核心100%"}
    impart_name = impart_level.get(impart_lv, "未知")
    if impart_lv == 10:
        msg = f"已进入虚神界{impart_name}区域！"
        impart_exp_up = impart_lv * 0.3
        msg += f"\n虚神界祝福：{int(impart_exp_up * 100)}%"
        await handle_send(bot, event, msg)
        await impart_pk_go.finish()
    else:
        if impart_data_draw['exp_day'] < 100:
            msg = f"道友累计时间不足，无法在深入虚神界！"
            impart_exp_up = impart_lv * 0.3
            msg += f"\n虚神界祝福：{int(impart_exp_up * 100)}%"
            await handle_send(bot, event, msg)
            await impart_pk_go.finish()
    impart_suc = random.randint(1, 100)
    impart_time = random.randint(10, 100)
    impart_rate = random.randint(1, 3)
    if impart_suc <= 50:
        msg = f"道友迷失方向，晕头转向😵‍💫，回到了虚神界{impart_name}区域！\n消耗虚神界时间：{impart_time}分钟"
        xiuxian_impart.use_impart_exp_day(impart_time, user_id)
        impart_pk.update_user_impart_lv(user_info['user_id'])
        await handle_send(bot, event, msg)
        await impart_pk_go.finish()
    impart_suc = random.randint(1, 100)
    if 1 <= impart_suc <= 40:
        impart_lv = impart_lv - 1
        impart_lv = max(impart_lv, 0)
        msg = "偶遇时空乱流"
    elif 41 <= impart_suc <= 80:
        impart_lv = impart_lv + 1
        impart_lv = min(impart_lv, 10)
        msg = "机缘巧合"
    elif 81 <= impart_suc <= 90:
        impart_lv = impart_lv - impart_rate
        impart_lv = max(impart_lv, 0)
        msg = "通过随机传送阵"
    else:
        impart_lv = impart_lv + impart_rate
        impart_lv = min(impart_lv, 10)
        msg = "通过随机传送阵"
    xiuxian_impart.use_impart_exp_day(impart_time, user_id)
    xiuxian_impart.update_impart_lv(impart_lv)
    impart_pk.update_user_impart_lv(user_info['user_id'])
    impart_exp_up = impart_lv * 0.3
    impart_name = impart_level.get(impart_lv, "未知")
    msg += f"，道友来到虚神界{impart_name}区域！\n消耗虚神界时间：{impart_time}分钟\n虚神界祝福：{int(impart_exp_up * 100)}%"
    await handle_send(bot, event, msg)
    await impart_pk_go.finish()
        
@impart_pk_in_closing.handle(parameterless=[Cooldown(at_sender=False)])
async def impart_pk_in_closing_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """虚神界闭关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 4  # 状态0为无事件
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_in_closing.finish()
    user_id = user_info['user_id']
    is_type, msg = check_user_type(user_id, 0)
    if user_info['root_type'] == '伪灵根':
        msg = "器师无法闭关！"
        await handle_send(bot, event, msg)
        await impart_pk_in_closing.finish()
    if is_type:  # 符合
        sql_message.in_closing(user_id, user_type)
        msg = f"进入虚神界闭关状态，如需出关，发送【虚神界出关】！"
        await handle_send(bot, event, msg)
        await impart_pk_in_closing.finish()
    else:
        await handle_send(bot, event, msg)
        await impart_pk_in_closing.finish()
        
@impart_pk_out_closing.handle(parameterless=[Cooldown(at_sender=False)])
async def impart_pk_out_closing_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """虚神界出关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 0  # 状态0为无事件
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_out_closing.finish()
    user_id = user_info['user_id']
    user_mes = sql_message.get_user_info_with_id(user_id)  # 获取用户信息
    level = user_mes['level']
    use_exp = user_mes['exp']
    impart_data_draw = await impart_pk_check(user_id)
    if impart_data_draw is None:
        msg = f"发生未知错误，多次尝试无果请找晓楠！"
        await handle_send(bot, event, msg)
        await impart_pk_out_closing.finish()

    max_exp = (
        int(OtherSet().set_closing_type(level)) * XiuConfig().closing_exp_upper_limit
    )  # 获取下个境界需要的修为 * 1.5为虚神界闭关上限
    user_get_exp_max = int(max_exp) - use_exp

    if user_get_exp_max < 0:
        # 校验当当前修为超出上限的问题，不可为负数
        user_get_exp_max = 0

    now_time = datetime.now()
    user_cd_message = sql_message.get_user_cd(user_id)
    is_type, msg = check_user_type(user_id, 4)
    if not is_type:
        await handle_send(bot, event, msg)
        await impart_pk_out_closing.finish()

    # 用户状态为4（虚神界闭关中）
    impart_pk_in_closing_time = datetime.strptime(
        user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
    )  # 进入虚神界闭关的时间
    exp_time = (
        OtherSet().date_diff(now_time, impart_pk_in_closing_time) // 60
    )  # 虚神界闭关时长计算(分钟) = second // 60

    # 获取灵根、境界和功法倍率
    level_rate = sql_message.get_root_rate(user_mes['root_type'])  # 灵根倍率
    realm_rate = jsondata.level_data()[level]["spend"]  # 境界倍率
    user_buff_data = UserBuffDate(user_id)
    user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5 / 1.5
    mainbuffdata = user_buff_data.get_user_main_buff_data()
    mainbuffratebuff = mainbuffdata['ratebuff'] if mainbuffdata is not None else 0  # 功法修炼倍率
    mainbuffcloexp = mainbuffdata['clo_exp'] if mainbuffdata is not None else 0  # 功法闭关经验
    mainbuffclors = mainbuffdata['clo_rs'] if mainbuffdata is not None else 0  # 功法闭关回复

    # 计算传承增益
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_exp_up = impart_data['impart_exp_up'] if impart_data is not None else 0
    impart_lv = impart_data_draw['impart_lv'] if impart_data is not None else 0
    impart_exp_up2 = impart_lv * 0.3
    

    # 计算基础经验倍率
    base_exp_rate = XiuConfig().closing_exp * (
        level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp) * (1 + user_blessed_spot_data) * (1 + impart_exp_up)
    ) 
    base_exp_rate2 = f"{int((level_rate + mainbuffratebuff + mainbuffcloexp + user_blessed_spot_data + impart_exp_up + impart_exp_up2) * 100)}%"

    # 计算可用虚神界修炼时间
    available_exp_day = int(impart_data_draw['exp_day'])  # 可用修炼时间
    max_double_exp_time = available_exp_day // 10
    double_exp_time = min(exp_time, max_double_exp_time) 
    double_exp = int(double_exp_time * base_exp_rate * (1 + impart_exp_up2))

    single_exp_time = exp_time - double_exp_time
    single_exp = int(single_exp_time * base_exp_rate) if single_exp_time > 0 else 0

    # 检查是否超过经验上限并调整时间
    total_exp = double_exp + single_exp
    effective_double_exp_time = double_exp_time
    effective_single_exp_time = single_exp_time
    exp_day_cost = double_exp_time * 10  # 初始exp_day消耗

    if total_exp > user_get_exp_max:
        # 如果超过上限，调整有效时间以不超过上限
        remaining_exp = user_get_exp_max
        if double_exp >= remaining_exp:
            effective_double_exp_time = remaining_exp / (base_exp_rate * (1 + impart_exp_up2))
            double_exp = int(effective_double_exp_time * base_exp_rate * (1 + impart_exp_up2))
            effective_single_exp_time = 0
            single_exp = 0
            exp_day_cost = int(effective_double_exp_time * 10)
        else:
            remaining_exp -= double_exp
            effective_single_exp_time = remaining_exp / base_exp_rate
            single_exp = int(effective_single_exp_time * base_exp_rate)
            # exp_day_cost不变，仅扣除双倍时间对应的exp_day
        total_exp = double_exp + single_exp

    # 更新可用修炼时间
    if exp_day_cost > 0:
        xiuxian_impart.use_impart_exp_day(exp_day_cost, user_id)

    # 更新用户数据
    sql_message.in_closing(user_id, user_type)  # 退出闭关状态
    sql_message.update_exp(user_id, total_exp)  # 更新修为
    sql_message.update_power2(user_id)  # 更新战力

    # 更新HP和MP（基于实际闭关时间）
    result_msg, result_hp_mp = OtherSet().send_hp_mp(
        user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time)
    )
    sql_message.update_user_attribute(
        user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10)
    )

    # 构造返回消息
    if total_exp >= user_get_exp_max:
        msg = (
            f"虚神界闭关结束，本次虚神界闭关到达上限，共增加修为：{total_exp}(修炼效率：{base_exp_rate2}){result_msg[0]}{result_msg[1]}"
        )
    else:
        if effective_single_exp_time == 0:
            msg = (
                f"虚神界闭关结束，共闭关{exp_time}分钟，"
                f"其中{int(effective_double_exp_time)}分钟获得虚神界祝福，"
                f"本次闭关增加修为：{total_exp}(修炼效率：{base_exp_rate2}){result_msg[0]}{result_msg[1]}"
            )
        else:
            msg = (
                f"虚神界闭关结束，共闭关{exp_time}分钟，"
                f"其中{int(effective_double_exp_time)}分钟获得虚神界祝福，"
                f"{int(effective_single_exp_time)}没有获得祝福，"
                f"本次闭关增加修为：{total_exp}(修炼效率：{base_exp_rate2}){result_msg[0]}{result_msg[1]}"
            )
    await handle_send(bot, event, msg)
    await impart_pk_out_closing.finish()