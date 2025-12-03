from nonebot import on_command, on_fullmatch
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
from ..xiuxian_utils.utils import check_user, get_msg_pic, send_msg_handler, handle_send, check_user_type, number_to, update_statistics_value
from .impart_pk_uitls import impart_pk_check
from .xu_world import xu_world
from .impart_pk import impart_pk
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, OtherSet, UserBuffDate, XIUXIAN_IMPART_BUFF
from .. import NICKNAME
from nonebot.log import logger
xiuxian_impart = XIUXIAN_IMPART_BUFF()
sql_message = XiuxianDateManage()  # sql类

impart_pk_project = on_fullmatch("投影虚神界", priority=6, block=True)
impart_pk_go = on_command("探索虚神界", aliases={"虚神界探索"}, priority=6, block=True)
impart_pk_info = on_fullmatch("虚神界信息", priority=6, block=True)
impart_pk_now = on_command("虚神界对决", priority=15, block=True)
impart_pk_list = on_fullmatch("虚神界列表", priority=7, block=True)
impart_pk_exp = on_command("虚神界修炼", priority=8, block=True)
impart_pk_out_closing = on_command("虚神界出关", priority=8, block=True)
impart_pk_in_closing = on_command("虚神界闭关", priority=8, block=True)
impart_top = on_command("虚神界排行榜", priority=8, block=True)

async def impart_re():
    impart_pk.re_data()
    xu_world.re_data()
    logger.opt(colors=True).info(f"<green>已重置虚神界次数</green>")

async def impart_lv():
    """每周调整虚神界等级"""
    logger.opt(colors=True).info(f"<green>开始执行虚神界等级批量调整...</green>")
    
    # 随机决定是增加还是减少等级
    change_type = 2  # 1增加，2减少
    change_amount = random.randint(1, 3)  # 1-3级
    
    xiuxian_impart.update_all_users_impart_lv(change_amount, change_type)
    
    logger.opt(colors=True).info(f"<green>虚神界等级调整完成</green>")

@impart_pk_project.handle(parameterless=[Cooldown(stamina_cost = 1)])
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
        msg = f"发生未知错误！"
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

@impart_top.handle(parameterless=[Cooldown(cd_time=1.4)])
async def impart_top_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    impart_level = {
        0:"凡尘迷雾", 1:"灵气初现", 2:"感气之渊",
        3:"练气云海", 4:"筑基灵台", 5:"金丹道场",
        6:"元神幻境", 7:"化神星域", 8:"炼神火宅",
        9:"返虚古路", 10:"大乘天阶", 11:"虚道玄门",
        12:"斩我剑冢", 13:"遁一星河", 14:"至尊王座",
        15:"微光圣境", 16:"星芒神域", 17:"月华仙宫",
        18:"耀日天穹", 19:"祭道荒原", 20:"自在净土",
        21:"破虚之隙", 22:"无界瀚海", 23:"混元道源",
        24:"造化玉池", 25:"永恒神庭", 26:"至高天阙",
        27:"大道尽头", 28:"法则本源", 29:"混沌核心",
        30:"虚神本源"
    }
    
    v_impart_top = xiuxian_impart.get_impart_rank()
    msg = f"\n✨虚神界等级排行榜TOP50✨\n"
    num = 0
    for i in v_impart_top:
        num += 1
        user_info = sql_message.get_user_info_with_id(i['user_id'])
        user_name = user_info['user_name'] if user_info else "未知修士"
        impart_name = impart_level.get(i['impart_lv'], "未知秘境")
        msg += f"第{num}位  {user_name}\n现位于：{impart_name}（LV {i['impart_lv']}）\n"
        if num == 50:
            break
    await handle_send(bot, event, msg)
    await impart_top.finish()
        
@impart_pk_list.handle(parameterless=[Cooldown(cd_time=1.4)])
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
        msg = f"发生未知错误！"
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

@impart_pk_now.handle(parameterless=[Cooldown(stamina_cost=3)])
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
        msg = f"发生未知错误！"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    args_text = args.extract_plain_text().strip()
    user_data = impart_pk.find_user_data(user_info['user_id'])

    if user_data["pk_num"] <= 0:
        msg = f"道友今日次数耗尽，明天再来吧！"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    # 解析参数
    target_num = None
    max_loss_count = 1  # 默认最多失败1次
    
    if args_text:
        parts = args_text.split()
        for part in parts:
            if part.endswith('次'):
                try:
                    # 提取次数部分
                    count_str = part.replace('次', '')
                    if count_str.isdigit():
                        max_loss_count = int(count_str)
                except:
                    pass
            elif part.isdigit():
                # 纯数字，可能是目标编号
                target_num = part
    
    # 验证失败次数
    if max_loss_count <= 0:
        msg = f"失败次数必须大于0！"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()
    
    if max_loss_count > user_data["pk_num"]:
        msg = f"道友今日剩余次数只有{user_data['pk_num']}次，无法承受{max_loss_count}次失败！"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    player_1_stones = 0
    player_2_stones = 0
    current_loss_count = 0
    total_battles = 0
    total_wins = 0
    total_losses = 0
    combined_msg = ""
    list_msg = []

    # 无目标编号的情况（与机器人对决）
    if not target_num:
        while current_loss_count < max_loss_count and user_data["pk_num"] > 0:
            total_battles += 1
            msg, win = await impart_pk_uitls.impart_pk_now_msg_to_bot(user_info['user_name'], NICKNAME)
            battle_msg = f"☆--------第{total_battles}场对决--------☆\n{msg}"
            
            if win == 1:  # 玩家胜利
                battle_msg += f"战报：道友{user_info['user_name']}获胜，获得思恋结晶20颗\n"
                impart_pk.update_user_data(user_info['user_id'], True)  # 胜利不消耗次数
                xiuxian_impart.update_stone_num(20, user_id, 1)
                player_1_stones += 20
                total_wins += 1
            elif win == 2:  # 玩家失败
                battle_msg += f"战报：道友{user_info['user_name']}败了，消耗1次次数，获得思恋结晶10颗\n"
                impart_pk.update_user_data(user_info['user_id'], False)  # 失败消耗次数
                xiuxian_impart.update_stone_num(10, user_id, 1)
                player_1_stones += 10
                current_loss_count += 1
                total_losses += 1
                
                # 更新用户数据
                user_data = impart_pk.find_user_data(user_info['user_id'])
            else:
                battle_msg += f"对决异常，不计结果\n"

            combined_msg += battle_msg + "\n"
            
            # 检查次数是否用尽
            if user_data["pk_num"] <= 0:
                combined_msg += "道友次数已用尽！\n"
                if xu_world.check_xu_world_user_id(user_id):
                    combined_msg += "已帮助道友退出虚神界！\n"
                    xu_world.del_xu_world(user_id)
                break

        combined_msg += f"☆--------对决结束--------☆\n"
        combined_msg += f"共进行{total_battles}场对决，获胜{total_wins}场，失败{total_losses}场\n"
        combined_msg += f"总计获得思恋结晶{player_1_stones}颗\n"
        
        list_msg.append({
            "type": "node", 
            "data": {
                "name": f"虚神界对决（失败{current_loss_count}/{max_loss_count}次）", 
                "uin": bot.self_id,
                "content": combined_msg
            }
        })
        await send_msg_handler(bot, event, list_msg)
        await impart_pk_now.finish()

    # 有目标编号的情况（与其他玩家对决）
    try:
        num = int(target_num) - 1
    except:
        msg = f"编号解析异常，应全为数字!"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

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

    # 检查对方是否还在虚神界
    if not xu_world.check_xu_world_user_id(player_2):
        msg = f"道友{player_2_name}已离开虚神界！"
        await handle_send(bot, event, msg)
        await impart_pk_now.finish()

    player_1_wins = 0
    player_2_wins = 0
    
    while current_loss_count < max_loss_count and user_data["pk_num"] > 0:
        total_battles += 1
        msg_list, win = await impart_pk_uitls.impart_pk_now_msg(player_1, player_1_name, player_2, player_2_name)
        
        battle_combined_msg = f"☆--------第{total_battles}场对决--------☆\n"
        
        if win is None:
            battle_combined_msg += f"对决异常，不计结果\n"
            combined_msg += battle_combined_msg + "\n"
            continue

        if win == 1:  # 1号玩家胜利
            impart_pk.update_user_data(player_1, True)  # 胜利不消耗次数
            impart_pk.update_user_data(player_2, False)  # 失败消耗次数
            xiuxian_impart.update_stone_num(20, player_1, 1)
            xiuxian_impart.update_stone_num(10, player_2, 1)
            player_1_stones += 20
            player_2_stones += 10
            player_1_wins += 1
            total_wins += 1
            
            battle_combined_msg += "\n".join([node['data']['content'] for node in msg_list]) + "\n"
            battle_combined_msg += f"道友{player_1_name}获得了胜利，获得思恋结晶20颗！\n"
            battle_combined_msg += f"道友{player_2_name}败了，获得思恋结晶10颗！\n"
            
            # 检查对方次数是否用尽
            player_2_data = impart_pk.find_user_data(player_2)
            if player_2_data["pk_num"] <= 0:
                battle_combined_msg += f"道友{player_2_name}次数耗尽，离开了虚神界！\n"
                xu_world.del_xu_world(player_2)
                combined_msg += battle_combined_msg
                break
                
        elif win == 2:  # 2号玩家胜利
            impart_pk.update_user_data(player_2, True)  # 胜利不消耗次数
            impart_pk.update_user_data(player_1, False)  # 失败消耗次数
            xiuxian_impart.update_stone_num(20, player_2, 1)
            xiuxian_impart.update_stone_num(10, player_1, 1)
            player_2_stones += 20
            player_1_stones += 10
            player_2_wins += 1
            current_loss_count += 1
            total_losses += 1
            
            battle_combined_msg += "\n".join([node['data']['content'] for node in msg_list]) + "\n"
            battle_combined_msg += f"道友{player_2_name}获得了胜利，获得思恋结晶20颗！\n"
            battle_combined_msg += f"道友{player_1_name}败了，获得思恋结晶10颗！\n"
            
            # 更新用户数据
            user_data = impart_pk.find_user_data(player_1)
            
            # 检查自己次数是否用尽
            if user_data["pk_num"] <= 0:
                battle_combined_msg += f"道友{player_1_name}次数耗尽！\n"
                if xu_world.check_xu_world_user_id(player_1):
                    battle_combined_msg += "已帮助道友退出虚神界！\n"
                    xu_world.del_xu_world(player_1)
                combined_msg += battle_combined_msg
                break
        
        combined_msg += battle_combined_msg + "\n"

    combined_msg += f"☆--------对决结束--------☆\n"
    combined_msg += f"共进行{total_battles}场对决\n"
    combined_msg += f"{player_1_name}获胜{player_1_wins}场，{player_2_name}获胜{player_2_wins}场\n"
    combined_msg += f"道友失败{current_loss_count}次（设定上限：{max_loss_count}次）\n"
    combined_msg += f"{player_1_name}获得思恋结晶{player_1_stones}颗，{player_2_name}获得思恋结晶{player_2_stones}颗\n"

    list_msg.append({
        "type": "node", 
        "data": {
            "name": f"虚神界对决（失败{current_loss_count}/{max_loss_count}次）", 
            "uin": bot.self_id,
            "content": combined_msg
        }
    })
    
    try:
        await send_msg_handler(bot, event, list_msg)
    except ActionFailed:
        msg = f"未知原因，对决显示失败!"
        await handle_send(bot, event, msg)
        
    await impart_pk_now.finish()

@impart_pk_exp.handle(parameterless=[Cooldown(cd_time=1.4)])
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
        msg = f"发生未知错误！"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()

    level = user_info['level']
    impaer_exp_time = args.extract_plain_text().strip()
    
    # 处理输入时间
    if not impaer_exp_time.isdigit():
        impaer_exp_time = 1
    else:
        impaer_exp_time = int(impaer_exp_time)
    
    # 获取闭关类型和经验上限
    closing_type = OtherSet().set_closing_type(user_info['level'])
    max_exp = closing_type * XiuConfig().closing_exp_upper_limit
    current_exp = user_info['exp']
    
    # 检查可用时间
    if impaer_exp_time > int(impart_data_draw['exp_day']):
        msg = f"累计时间不足，修炼失败!"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()

    if user_info['root_type'] == '伪灵根':
        msg = f"凡人无法进行修炼!"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()

    # 计算每分钟获得的经验值
    level_rate = sql_message.get_root_rate(user_info['root_type'], user_id)  # 灵根倍率
    realm_rate = jsondata.level_data()[level]["spend"]  # 境界倍率
    user_buff_data = UserBuffDate(user_id)
    mainbuffdata = user_buff_data.get_user_main_buff_data()
    mainbuffratebuff = mainbuffdata['ratebuff'] if mainbuffdata is not None else 0  # 功法修炼倍率
    mainbuffcloexp = mainbuffdata['clo_exp'] if mainbuffdata != None else 0  # 功法闭关经验
    impart_data_draw = await impart_pk_check(user_id)
    impart_lv = impart_data_draw['impart_lv'] if impart_data_draw is not None else 0
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_exp_up = impart_data['impart_exp_up'] if impart_data is not None else 0
    impart_exp_up2 = impart_lv * 0.15
    
    # 计算每分钟基础经验
    exp_per_minute = int(XiuConfig().closing_exp * ((level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp) * (1 + impart_exp_up) * (1 + impart_exp_up2))))
    
    # 计算剩余可获取经验
    remaining_exp = max_exp - current_exp
    
    # 计算最大允许修炼时间
    max_allowed_time = remaining_exp // exp_per_minute if exp_per_minute > 0 else 0
    
    # 如果输入时间超过最大允许时间
    if impaer_exp_time > max_allowed_time:
        if max_allowed_time > 0:
            msg = f"修炼时长超出上限，最多可修炼{round(max_allowed_time)}分钟"
        else:
            msg = f"修炼时长超出上限，已不可修炼！"
        await handle_send(bot, event, msg)
        await impart_pk_exp.finish()
    
    # 计算本次修炼实际获得的经验
    exp = exp_per_minute * impaer_exp_time
    
    # 更新经验并返回成功
    xiuxian_impart.use_impart_exp_day(impaer_exp_time, user_id)
    sql_message.update_exp(user_id, exp)
    sql_message.update_power2(user_id)  # 更新战力
    
    # 计算修炼效率百分比
    efficiency_percent = int((level_rate + mainbuffratebuff + mainbuffcloexp + impart_exp_up + impart_exp_up2) * 100)
    msg = f"虚神界修炼结束，共修炼{round(impaer_exp_time)}分钟，本次增加修为：{number_to(exp)}（修炼效率：{efficiency_percent}%）"
    update_statistics_value(user_id, "虚神界修炼", increment=impaer_exp_time)
    await handle_send(bot, event, msg)
    await impart_pk_exp.finish()

@impart_pk_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def impart_pk_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """虚神界信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_info.finish()
    user_id = user_info['user_id']
    user_data = impart_pk.find_user_data(user_info['user_id'])
    pk_num = user_data["pk_num"]
    impart_num = user_data["impart_num"]
    impart_data_draw = await impart_pk_check(user_id)
    impart_lv = impart_data_draw['impart_lv'] if impart_data_draw is not None else 0
    stone_num = impart_data_draw["stone_num"] if impart_data_draw is not None else 0
    user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5 / 1.5
    if user_blessed_spot_data == 0 or user_blessed_spot_data is None:
        user_blessed_spot_msg = ""
    else:
        user_blessed_spot_msg = f"（聚灵旗加成：{int((user_blessed_spot_data) * 100)}%）"
    
    impart_level = {
        0:"凡尘迷雾", 1:"灵气初现", 2:"感气之渊",
        3:"练气云海", 4:"筑基灵台", 5:"金丹道场",
        6:"元神幻境", 7:"化神星域", 8:"炼神火宅",
        9:"返虚古路", 10:"大乘天阶", 11:"虚道玄门",
        12:"斩我剑冢", 13:"遁一星河", 14:"至尊王座",
        15:"微光圣境", 16:"星芒神域", 17:"月华仙宫",
        18:"耀日天穹", 19:"祭道荒原", 20:"自在净土",
        21:"破虚之隙", 22:"无界瀚海", 23:"混元道源",
        24:"造化玉池", 25:"永恒神庭", 26:"至高天阙",
        27:"大道尽头", 28:"法则本源", 29:"混沌核心",
        30:"虚神本源"
    }
    
    impart_time = impart_data_draw['exp_day']
    impart_exp_up = impart_lv * 0.15
    impart_name_new = impart_level.get(impart_lv, "未知秘境")
    msg += f"\n现位于：{impart_name_new}（LV {impart_lv}）"
    msg += f"\n虚神界修炼时间：{impart_time} 分钟"
    msg += f"\n修炼效率：{int((impart_exp_up + user_blessed_spot_data) * 100)}% {user_blessed_spot_msg}"
    msg += f"\n今日可探索次数：{impart_num}"
    msg += f"\n今日可对决次数：{pk_num}"
    msg += f"\n思恋结晶：{stone_num}"
    await handle_send(bot, event, msg)
    await impart_pk_info.finish()

@impart_pk_go.handle(parameterless=[Cooldown(cd_time=1.4)])
async def impart_pk_go_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """探索虚神界"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_go.finish()
    user_id = user_info['user_id']
    user_data = impart_pk.find_user_data(user_info['user_id'])
    if user_data["impart_num"] <= 0:
        msg = f"\n道友今日探索次数耗尽，需打坐调息，明日方可再探虚神界！"
        await handle_send(bot, event, msg)
        await impart_pk_go.finish()
    impart_data_draw = await impart_pk_check(user_id)
    impart_lv = impart_data_draw['impart_lv'] if impart_data_draw is not None else 0
    
    impart_level = {
        0:"凡尘迷雾", 1:"灵气初现", 2:"感气之渊",
        3:"练气云海", 4:"筑基灵台", 5:"金丹道场",
        6:"元神幻境", 7:"化神星域", 8:"炼神火宅",
        9:"返虚古路", 10:"大乘天阶", 11:"虚道玄门",
        12:"斩我剑冢", 13:"遁一星河", 14:"至尊王座",
        15:"微光圣境", 16:"星芒神域", 17:"月华仙宫",
        18:"耀日天穹", 19:"祭道荒原", 20:"自在净土",
        21:"破虚之隙", 22:"无界瀚海", 23:"混元道源",
        24:"造化玉池", 25:"永恒神庭", 26:"至高天阙",
        27:"大道尽头", 28:"法则本源", 29:"混沌核心",
        30:"虚神本源"
    }
    
    impart_name = impart_level.get(impart_lv, "未知秘境")
    if impart_lv == 30:
        msg = f"\n已登临{impart_name}！"
        impart_exp_up = impart_lv * 0.15
        msg += f"\n获得虚神界终极加持：修为增益{int(impart_exp_up * 100)}%"
        await handle_send(bot, event, msg)
        await impart_pk_go.finish()
    else:
        if impart_data_draw['exp_day'] < 100:
            msg = f"\n道友探索虚神界时间不足，难以突破{impart_name}的禁制！"
            impart_exp_up = impart_lv * 0.15
            msg += f"\n当前区域加持：修为增益{int(impart_exp_up * 100)}%"
            await handle_send(bot, event, msg)
            await impart_pk_go.finish()
    
    impart_suc = random.randint(1, 100)
    impart_time = random.randint(1, 100)
    impart_rate = random.randint(1, 3)
    rates = [0.15, 0.15, 0.25, 0.25, 0.10, 0.10]  # stay, fail, down, up, down_rate, up_rate
    all_msgs : Dict[str, List[str]] = {
        "stay": [
            f"道友突然心有所感，决定原地静修，参悟{impart_name}的玄机",
            f"《{random.choice(['太虚','九幽','混元'])}经》自行运转，道友决定暂缓探索",
            f"冥冥中似有警示，道友决定今日不宜继续探索虚神界",
            f"道友在{impart_name}中偶得顿悟，决定就地闭关参悟",
            f"「{random.choice(['青萍剑','昆仑镜','造化玉碟'])}」发出共鸣，道友决定停下脚步"
        ],
        "fail": [
            f"遭遇{impart_name}守护大阵反噬，道友元神受创退回！",
            f"虚空突现《{random.choice(['太虚','九幽','混元'])}禁制》，将道友逼退！",
            f"心魔劫显化{random.choice(['天魔','域外邪神','上古怨灵'])}虚影，道友不得不暂避锋芒！",
            f"{random.choice(['青冥','玄黄','混沌'])}道则显化，阻断道友前进之路！",
            f"道友本命法宝「{random.choice(['青萍剑','昆仑镜','造化玉碟'])}」震颤示警，被迫撤退！"
        ],
        "down": [
            f"道友误触{random.choice(['周天','洪荒','太古'])}禁制，境界暂时跌落",
            f"遭遇{random.choice(['虚空风暴','法则乱流','混沌潮汐'])}，被迫退守",
            f"{random.choice(['诛仙','戮神','陷仙'])}剑气纵横，斩落道友一缕元神",
            f"神秘存在「{random.choice(['荒天帝','叶天帝','楚天尊'])}」虚影显现，威压逼退道友",
            f"《{random.choice(['道藏','佛经','魔典'])}》显化天碑，道友参悟有误反受其害"
        ],
        "up": [
            f"道友顿悟{random.choice(['太初','鸿蒙','混沌'])}真意，境界突破！",
            f"得「{random.choice(['菩提树','悟道石','混沌青莲'])}」相助，勘破一层玄机",
            f"以《{random.choice(['大衍诀','神象镇狱劲','他化自在法'])}》破开禁制",
            f"献祭{random.choice(['千年修为','本命精血','先天灵宝'])}，强行突破桎梏",
            f"引动{random.choice(['周天星辰','地脉龙气','混沌雷劫'])}之力，开辟前路"
        ],
        "down_rate": [
            f"遭逢{random.choice(['量劫','天人五衰','纪元更迭'])}天象，道基受损！",
            f"{random.choice(['天道','大道','混沌'])}反噬，境界连跌！",
            f"被「{random.choice(['时间长河','命运长河','因果长河'])}」冲刷，丢失部分道果",
            f"{random.choice(['上苍之上','界海彼岸','黑暗源头'])}传来诡异低语，道友道心几近崩溃",
            f"《{random.choice(['葬经','度人经','灭世书'])}》显化，强行削去道友修为"
        ],
        "up_rate": [
            f"触发{random.choice(['混沌青莲','世界树','玄黄母气'])}异象，连破数关！",
            f"得「{random.choice(['盘古斧','造化玉碟','东皇钟'])}」道韵洗礼，修为暴涨",
            f"参透《{random.choice(['道经','佛经','魔典'])}》终极奥义，直指大道本源",
            f"{random.choice(['鸿钧','陆压','扬眉'])}老祖显圣点化，醍醐灌顶",
            f"吞噬{random.choice(['先天灵宝','混沌至宝','大道碎片'])}，实力飙升"
        ]
    }
    msg_type = random.choices(list(all_msgs.keys()), weights=rates)[0]
    
    msg = random.choice(all_msgs[msg_type])
    match msg_type:
        case "stay":
            impart_pk.update_user_impart_lv(user_info['user_id'])  # 扣除探索次数
            await handle_send(bot, event, msg)
            await impart_pk_go.finish()
        case "fail":
            msg += f"\n消耗虚神界时间：{impart_time} 分钟"
            xiuxian_impart.use_impart_exp_day(impart_time, user_id)  # 消耗时间
            
            impart_pk.update_user_impart_lv(user_info['user_id'])
            await handle_send(bot, event, msg)
            await impart_pk_go.finish()
        case "down":
            impart_lv = max(impart_lv - 1, 0)
        case "up":
            impart_lv = min(impart_lv + 1, 30)
        case "down_rate":
            impart_lv = max(impart_lv - impart_rate, 0)
        case "up_rate":
            impart_lv = min(impart_lv + impart_rate, 30)

    xiuxian_impart.use_impart_exp_day(impart_time, user_id)
    xiuxian_impart.update_impart_lv(user_id, impart_lv)
    impart_pk.update_user_impart_lv(user_info['user_id'])
    
    impart_exp_up = impart_lv * 0.15
    impart_name_new = impart_level.get(impart_lv, "未知秘境")
    msg += f"\n现位于：{impart_name_new}"
    msg += f"\n消耗虚神界时间：{impart_time} 分钟"
    msg += f"\n获得区域道则加持：修为增益{int(impart_exp_up * 100)}%"
    await handle_send(bot, event, msg)
    await impart_pk_go.finish()

@impart_pk_in_closing.handle(parameterless=[Cooldown(cd_time=1.4)])
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
        msg = "凡人无法闭关！"
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
        
@impart_pk_out_closing.handle(parameterless=[Cooldown(cd_time=1.4)])
async def impart_pk_out_closing_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """虚神界出关"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_type = 0  # 状态0为无事件
    
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await impart_pk_out_closing.finish()
    
    user_id = user_info['user_id']
    
    # 检查用户是否在虚神界闭关状态
    is_type, msg = check_user_type(user_id, 4)
    if not is_type:
        await handle_send(bot, event, msg)
        await impart_pk_out_closing.finish()
    
    # 获取用户信息和传承数据
    user_mes = sql_message.get_user_info_with_id(user_id)
    level = user_mes['level']
    use_exp = user_mes['exp']
    
    impart_data_draw = await impart_pk_check(user_id)
    if impart_data_draw is None:
        msg = f"发生未知错误！"
        await handle_send(bot, event, msg)
        await impart_pk_out_closing.finish()

    # 计算经验上限
    max_exp = int(OtherSet().set_closing_type(level)) * XiuConfig().closing_exp_upper_limit
    user_get_exp_max = max(0, int(max_exp) - use_exp)  # 确保不为负数

    now_time = datetime.now()
    user_cd_message = sql_message.get_user_cd(user_id)
    
    # 计算闭关时长
    impart_pk_in_closing_time = datetime.strptime(
        user_cd_message['create_time'], "%Y-%m-%d %H:%M:%S.%f"
    )
    exp_time = OtherSet().date_diff(now_time, impart_pk_in_closing_time) // 60  # 闭关时长(分钟)

    # 获取各种增益倍率
    level_rate = sql_message.get_root_rate(user_mes['root_type'], user_id)
    realm_rate = jsondata.level_data()[level]["spend"]
    user_buff_data = UserBuffDate(user_id)
    user_blessed_spot_data = UserBuffDate(user_id).BuffInfo['blessed_spot'] * 0.5 / 1.5
    
    mainbuffdata = user_buff_data.get_user_main_buff_data()
    mainbuffratebuff = mainbuffdata['ratebuff'] if mainbuffdata is not None else 0
    mainbuffcloexp = mainbuffdata['clo_exp'] if mainbuffdata is not None else 0
    mainbuffclors = mainbuffdata['clo_rs'] if mainbuffdata is not None else 0

    # 计算传承增益
    impart_data = xiuxian_impart.get_user_impart_info_with_id(user_id)
    impart_exp_up = impart_data['impart_exp_up'] if impart_data is not None else 0
    impart_lv = impart_data_draw['impart_lv'] if impart_data_draw is not None else 0
    impart_exp_up2 = impart_lv * 0.15

    # 计算基础经验倍率
    base_exp_rate = XiuConfig().closing_exp * (
        level_rate * realm_rate * (1 + mainbuffratebuff) * (1 + mainbuffcloexp) * 
        (1 + user_blessed_spot_data) * (1 + impart_exp_up)
    )
    base_exp_rate2 = f"{int((level_rate + mainbuffratebuff + mainbuffcloexp + user_blessed_spot_data + impart_exp_up + impart_exp_up2) * 100)}%"

    # 计算可用虚神界修炼时间
    available_exp_day = int(impart_data_draw['exp_day'])
    max_double_exp_time = available_exp_day
    double_exp_time = min(exp_time, max_double_exp_time)
    double_exp = int(double_exp_time * base_exp_rate * (1 + impart_exp_up2))

    single_exp_time = exp_time - double_exp_time
    single_exp = int(single_exp_time * base_exp_rate) if single_exp_time > 0 else 0

    # 检查是否超过经验上限并调整时间
    total_exp = double_exp + single_exp
    effective_double_exp_time = double_exp_time
    effective_single_exp_time = single_exp_time
    exp_day_cost = double_exp_time

    if total_exp > user_get_exp_max:
        remaining_exp = user_get_exp_max
        if double_exp >= remaining_exp:
            effective_double_exp_time = remaining_exp / (base_exp_rate * (1 + impart_exp_up2))
            double_exp = int(effective_double_exp_time * base_exp_rate * (1 + impart_exp_up2))
            effective_single_exp_time = 0
            single_exp = 0
            exp_day_cost = int(effective_double_exp_time)
        else:
            remaining_exp -= double_exp
            effective_single_exp_time = remaining_exp / base_exp_rate
            single_exp = int(effective_single_exp_time * base_exp_rate)
        
        total_exp = double_exp + single_exp

    # 更新可用修炼时间
    if exp_day_cost > 0:
        xiuxian_impart.use_impart_exp_day(exp_day_cost, user_id)

    # 更新用户数据
    sql_message.in_closing(user_id, user_type)
    sql_message.update_exp(user_id, total_exp)
    sql_message.update_power2(user_id)

    # 更新HP和MP
    result_msg, result_hp_mp = OtherSet().send_hp_mp(
        user_id, int(use_exp / 10 * exp_time), int(use_exp / 20 * exp_time)
    )
    sql_message.update_user_attribute(
        user_id, result_hp_mp[0], result_hp_mp[1], int(result_hp_mp[2] / 10)
    )
    update_statistics_value(user_id, "虚神界闭关时长", increment=exp_time)

    # 构造返回消息
    if total_exp >= user_get_exp_max:
        msg = f"虚神界闭关结束，本次虚神界闭关到达上限，共增加修为：{number_to(total_exp)}(修炼效率：{base_exp_rate2}){result_msg[0]}{result_msg[1]}"
    else:
        if effective_single_exp_time == 0:
            msg = (f"虚神界闭关结束，共闭关{exp_time}分钟，"
                   f"其中{int(effective_double_exp_time)}分钟获得虚神界祝福，"
                   f"本次闭关增加修为：{number_to(total_exp)}(修炼效率：{base_exp_rate2}){result_msg[0]}{result_msg[1]}")
        else:
            msg = (f"虚神界闭关结束，共闭关{exp_time}分钟，"
                   f"其中{int(effective_double_exp_time)}分钟获得虚神界祝福，"
                   f"{int(effective_single_exp_time)}分钟没有获得祝福，"
                   f"本次闭关增加修为：{number_to(total_exp)}(修炼效率：{base_exp_rate2}){result_msg[0]}{result_msg[1]}")
    
    await handle_send(bot, event, msg)
    await impart_pk_out_closing.finish()
