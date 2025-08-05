import re
import random
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, OtherSet, BuffJsonDate,
    get_main_info_msg, UserBuffDate, get_sec_msg
)
from nonebot import on_command, on_fullmatch, require
from nonebot.log import logger
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    ActionFailed
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown, assign_bot_group
from nonebot.params import CommandArg
from ..xiuxian_utils.data_source import jsondata
from datetime import datetime, timedelta
from ..xiuxian_config import XiuConfig, convert_rank, JsonConfig
from .sectconfig import get_config
from ..xiuxian_utils.utils import (
    check_user, number_to,
    get_msg_pic, send_msg_handler, CommandObjectID, handle_send,
    Txt2Img
)
from ..xiuxian_utils.item_json import Items

items = Items()
sql_message = XiuxianDateManage()  # sql类
config = get_config()
LEVLECOST = config["LEVLECOST"]
cache_help = {}
userstask = {}

buffrankkey = {
    "人阶下品": 1,
    "人阶上品": 2,
    "黄阶下品": 3,
    "黄阶上品": 4,
    "玄阶下品": 5,
    "玄阶上品": 6,
    "地阶下品": 7,
    "地阶上品": 8,
    "天阶下品": 9,
    "天阶上品": 10,
    "仙阶下品": 50,
    "仙阶上品": 100,
}

materialsupdate = require("nonebot_plugin_apscheduler").scheduler
resetusertask = require("nonebot_plugin_apscheduler").scheduler
auto_sect_owner_change = require("nonebot_plugin_apscheduler").scheduler
upatkpractice = on_command("升级攻击修炼", priority=5, block=True)
uphppractice = on_command("升级元血修炼", priority=5, block=True)
upmppractice = on_command("升级灵海修炼", priority=5, block=True)
my_sect = on_command("我的宗门", aliases={"宗门信息"}, priority=5, block=True)
create_sect = on_command("创建宗门", priority=5, block=True)
join_sect = on_command("加入宗门", priority=5, block=True)
sect_position_update = on_command("宗门职位变更", priority=5, block=True)
sect_donate = on_command("宗门捐献", priority=5, block=True)
sect_out = on_command("退出宗门", priority=5, block=True)
sect_kick_out = on_command("踢出宗门", priority=5, block=True)
sect_owner_change = on_command("宗主传位", priority=5, block=True)
sect_list = on_fullmatch("宗门列表", priority=5, block=True)
sect_help = on_fullmatch("宗门帮助", priority=5, block=True)
sect_task = on_command("宗门任务接取", aliases={"我的宗门任务"}, priority=7, block=True)
sect_task_complete = on_fullmatch("宗门任务完成", priority=7, block=True)
sect_task_refresh = on_fullmatch("宗门任务刷新", priority=7, block=True)
sect_mainbuff_get = on_command("宗门功法搜寻", aliases={"搜寻宗门功法"}, priority=6, block=True)
sect_mainbuff_learn = on_command("学习宗门功法", priority=5, block=True)
sect_secbuff_get = on_command("宗门神通搜寻", aliases={"搜寻宗门神通"}, priority=6, block=True)
sect_secbuff_learn = on_command("学习宗门神通", priority=5, block=True)
sect_buff_info = on_command("宗门功法查看", aliases={"查看宗门功法"}, priority=9, block=True)
sect_users = on_command("宗门成员查看", aliases={"查看宗门成员"}, priority=8, block=True)
sect_elixir_room_make = on_command("宗门丹房建设", aliases={"建设宗门丹房"}, priority=5, block=True)
sect_elixir_get = on_command("宗门丹药领取", aliases={"领取宗门丹药"}, priority=5, block=True)
sect_rename = on_command("宗门改名", priority=5,  block=True)

__sect_help__ = f"""
【宗门系统】🏯

🏛️ 基础指令：
  • 我的宗门 - 查看当前宗门信息
  • 宗门列表 - 浏览全服宗门
  • 创建宗门 - 消耗{XiuConfig().sect_create_cost}灵石（需境界{XiuConfig().sect_min_level}）
  • 加入宗门 [ID] - 申请加入指定宗门

👑 宗主专属：
  • 宗门职位变更 [道号] [0-4] - 调整成员职位
    0=宗主 | 1=长老 | 2=亲传 | 3=内门 | 4=外门
  • 宗门改名 [新名称] - 修改宗门名称
  • 宗主传位 [道号] - 禅让宗主之位
  • 踢出宗门 [道号] - 移除宗门成员

📈 宗门建设：
  • 宗门捐献 - 提升建设度（每{config["等级建设度"]}建设度提升1级修炼上限）
  • 升级攻击/元血/灵海修炼 - 提升对应属性（每级+4%攻/8%血/5%真元）

📚 功法传承：
  • 宗门功法、神通搜寻 - 宗主可消耗资源搜索功法（100次）
  • 学习宗门功法/神通 [名称] - 成员消耗资材学习
  • 宗门功法查看 - 浏览宗门藏书

💊 丹房系统：
  • 建设宗门丹房 - 开启每日丹药福利
  • 领取宗门丹药 - 获取每日丹药补给

📝 宗门任务：
  • 宗门任务接取 - 获取任务（每日上限：{config["每日宗门任务次上限"]}次）
  • 宗门任务完成 - 提交任务（CD：{config["宗门任务完成cd"]}秒）
  • 宗门任务刷新 - 更换任务（CD：{config["宗门任务刷新cd"]}秒）

⏰ 自动福利：
  • 每日{config["发放宗门资材"]["时间"]}点发放{config["发放宗门资材"]["倍率"]}倍建设度资材
  • 职位修为加成：宗主＞长老＞亲传＞内门＞外门＞散修

💡 小贴士：
  1. 外门弟子无法获得修炼资源
  2. 建设度决定宗门整体实力
  3. 每日任务收益随职位提升
""".strip()


# 定时任务每1小时按照宗门贡献度增加资材
@materialsupdate.scheduled_job("cron", hour=config["发放宗门资材"]["时间"])
async def materialsupdate_():
    all_sects = sql_message.get_all_sects_id_scale()
    for s in all_sects:
        sql_message.update_sect_materials(sect_id=s[0], sect_materials=s[1] * config["发放宗门资材"]["倍率"], key=1)

    logger.opt(colors=True).info(f"<green>已更新所有宗门的资材</green>")


# 每日8点重置用户宗门任务次数、宗门丹药领取次数
@resetusertask.scheduled_job("cron", hour=8, minute=0)
async def resetusertask_():
    sql_message.sect_task_reset()
    sql_message.sect_elixir_get_num_reset()
    all_sects = sql_message.get_all_sects_id_scale()
    for s in all_sects:
        sect_info = sql_message.get_sect_info(s[0])
        if int(sect_info['elixir_room_level']) != 0:
            elixir_room_cost = config['宗门丹房参数']['elixir_room_level'][str(sect_info['elixir_room_level'])]['level_up_cost'][
                '建设度']
            if sect_info['sect_materials'] < elixir_room_cost:
                logger.opt(colors=True).info(f"<red>宗门：{sect_info['sect_name']}的资材无法维持丹房</red>")
                continue
            else:
                sql_message.update_sect_materials(sect_id=sect_info['sect_id'], sect_materials=elixir_room_cost, key=2)
    logger.opt(colors=True).info(f"<green>已重置所有宗门任务次数、宗门丹药领取次数，已扣除丹房维护费</green>")

# 定时任务每1小时自动检测不常玩的宗主
@auto_sect_owner_change.scheduled_job("interval", hours=1)
async def auto_sect_owner_change_():
    logger.opt(colors=True).info(f"<yellow>开始检测不常玩的宗主</yellow>")
    
    all_sect_owners_id = sql_message.get_sect_owners()
    all_active = all(sql_message.get_last_check_info_time(owner_id) is None or
                     datetime.now() - sql_message.get_last_check_info_time(owner_id) < timedelta(days=XiuConfig().auto_change_sect_owner_cd)
                     for owner_id in all_sect_owners_id)
    if all_active:
        logger.opt(colors=True).info(f"<green>各宗宗主在修行之途上勤勉不辍，宗门安危无忧，可喜可贺！</green>")

    for owner_id in all_sect_owners_id:
        last_check_time = sql_message.get_last_check_info_time(owner_id)
        if last_check_time is None or datetime.now() - last_check_time < timedelta(days=XiuConfig().auto_change_sect_owner_cd):
            continue

        user_info = sql_message.get_user_info_with_id(owner_id)
        sect_id = user_info['sect_id']
        logger.opt(colors=True).info(f"<red>{user_info['user_name']}离线时间超过{XiuConfig().auto_change_sect_owner_cd}天，开始自动换宗主</red>")
        new_owner_id = sql_message.get_highest_contrib_user_except_current(sect_id, owner_id)
        new_owner_info = sql_message.get_user_info_with_id(new_owner_id[0])
        
        sql_message.update_usr_sect(owner_id, sect_id, 1)
        sql_message.update_usr_sect(new_owner_id[0], sect_id, 0)
        sql_message.update_sect_owner(new_owner_id[0], sect_id)
        sect_info = sql_message.get_sect_info_by_id(sect_id)
        logger.opt(colors=True).info(f"<green>由{new_owner_info['user_name']}继承{sect_info['sect_name']}宗主之位</green>")

    
@sect_help.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """宗门帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help:
        msg = cache_help[session_id]
        await sect_help.finish()
    else:
        msg = __sect_help__
        title = ""
        font_size = 32
        img = Txt2Img(font_size)
        await handle_send(bot, event, msg)
        await sect_help.finish()


@sect_elixir_room_make.handle(parameterless=[Cooldown(stamina_cost=2, at_sender=False)])
async def sect_elixir_room_make_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门丹房建设"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_elixir_room_make.finish()
    sect_id = user_info['sect_id']
    if sect_id:
        sect_position = user_info['sect_position']
        owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
        owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
        if sect_position == owner_position:
            elixir_room_config = config['宗门丹房参数']
            elixir_room_level_up_config = elixir_room_config['elixir_room_level']
            sect_info = sql_message.get_sect_info(sect_id)
            elixir_room_level = sect_info['elixir_room_level']  # 宗门丹房等级
            if int(elixir_room_level) == len(elixir_room_level_up_config):
                msg = f"宗门丹房等级已经达到最高等级，无法继续建设了！"
                await handle_send(bot, event, msg)
                await sect_elixir_room_make.finish()
            to_up_level = int(elixir_room_level) + 1
            elixir_room_level_up_sect_scale_cost = elixir_room_level_up_config[str(to_up_level)]['level_up_cost']['建设度']
            elixir_room_level_up_use_stone_cost = elixir_room_level_up_config[str(to_up_level)]['level_up_cost'][
                'stone']
            if elixir_room_level_up_use_stone_cost > int(sect_info['sect_used_stone']):
                msg = f"宗门可用灵石不满足升级条件，当前升级需要消耗宗门灵石：{elixir_room_level_up_use_stone_cost}枚！"
                await handle_send(bot, event, msg)
                await sect_elixir_room_make.finish()
            elif elixir_room_level_up_sect_scale_cost > int(sect_info['sect_scale']):
                msg = f"宗门建设度不满足升级条件，当前升级需要消耗宗门建设度：{elixir_room_level_up_sect_scale_cost}点！"
                await handle_send(bot, event, msg)
                await sect_elixir_room_make.finish()
            else:
                msg = f"宗门消耗：{elixir_room_level_up_sect_scale_cost}建设度，{elixir_room_level_up_use_stone_cost}宗门灵石\n"
                msg += f"成功升级宗门丹房，当前丹房为：{elixir_room_level_up_config[str(to_up_level)]['name']}!"
                sql_message.update_sect_scale_and_used_stone(sect_id,
                                                             sect_info['sect_used_stone'] - elixir_room_level_up_use_stone_cost,
                                                             sect_info['sect_scale'] - elixir_room_level_up_sect_scale_cost)
                sql_message.update_sect_elixir_room_level(sect_id, to_up_level)
                await handle_send(bot, event, msg)
                await sect_elixir_room_make.finish()
        else:
            msg = f"道友不是宗主，无法使用该命令！"
            await handle_send(bot, event, msg)
            await sect_elixir_room_make.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_elixir_room_make.finish()


@sect_elixir_get.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_elixir_get_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门丹药领取"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_elixir_get.finish()

    sect_id = user_info['sect_id']
    user_id = user_info['user_id']
    sql_message.update_last_check_info_time(user_id) # 更新查看修仙信息时间
    if sect_id:
        sect_position = user_info['sect_position']
        elixir_room_config = config['宗门丹房参数']
        if sect_position == 4:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]['title']}，不满足领取要求!"""
            await handle_send(bot, event, msg)
            await sect_elixir_get.finish()
        else:
            sect_info = sql_message.get_sect_info(sect_id)
            if int(sect_info['elixir_room_level']) == 0:
                msg = f"道友的宗门目前还未建设丹房！"
                await handle_send(bot, event, msg)
                await sect_elixir_get.finish()
            if int(user_info['sect_contribution']) < elixir_room_config['领取贡献度要求']:
                msg = f"道友的宗门贡献度不满足领取条件，当前宗门贡献度要求：{elixir_room_config['领取贡献度要求']}点！"
                await handle_send(bot, event, msg)
                await sect_elixir_get.finish()
            elixir_room_level_up_config = elixir_room_config['elixir_room_level']
            elixir_room_cost = elixir_room_level_up_config[str(sect_info['elixir_room_level'])]['level_up_cost']['建设度']
            if sect_info['sect_materials'] < elixir_room_cost:
                msg = f"当前宗门资材无法维护丹房，请等待{config['发放宗门资材']['时间']}点发放宗门资材后尝试领取！"
                await handle_send(bot, event, msg)
                await sect_elixir_get.finish()
            if int(user_info['sect_elixir_get']) == 1:
                msg = f"道友已经领取过了，不要贪心哦~"
                await handle_send(bot, event, msg)
                await sect_elixir_get.finish()
            if int(sect_info['elixir_room_level']) == 1:
                msg = f"道友成功领取到丹药:渡厄丹！"
                sql_message.send_back(user_info['user_id'], 1999, "渡厄丹", "丹药", 1, 1)  # 1级丹房送1个渡厄丹
                sql_message.update_user_sect_elixir_get_num(user_info['user_id'])
                await handle_send(bot, event, msg)
                await sect_elixir_get.finish()
            else:
                sect_now_room_config = elixir_room_level_up_config[str(sect_info['elixir_room_level'])]
                give_num = sect_now_room_config['give_level']['give_num'] - 1
                rank_up = sect_now_room_config['give_level']['rank_up']
                give_dict = {}
                give_elixir_id_list = items.get_random_id_list_by_rank_and_item_type(
                    fanil_rank=convert_rank(user_info['level'])[0] - rank_up, item_type=['丹药'])
                if not give_elixir_id_list:  # 没有合适的ID，全部给渡厄丹
                    msg = f"道友成功领取到丹药：渡厄丹 2 枚！"
                    sql_message.send_back(user_info['user_id'], 1999, "渡厄丹", "丹药", 2, 1)  # 送1个渡厄丹
                    sql_message.update_user_sect_elixir_get_num(user_info['user_id'])
                    await handle_send(bot, event, msg)
                    await sect_elixir_get.finish()
                i = 1
                while i <= give_num:
                    id = random.choice(give_elixir_id_list)
                    if int(id) == 1999:  # 不给渡厄丹了
                        continue
                    else:
                        try:
                            give_dict[id] += 1
                            i += 1
                        except:
                            give_dict[id] = 1
                            i += 1
                msg = f"道友成功领取到丹药:渡厄丹 1 枚!\n"
                sql_message.send_back(user_info['user_id'], 1999, "渡厄丹", "丹药", 1, 1)  # 送1个渡厄丹
                for k, v in give_dict.items():
                    goods_info = items.get_data_by_item_id(k)
                    msg += f"道友成功领取到丹药：{goods_info['name']} {v} 枚!\n"
                    sql_message.send_back(user_info['user_id'], k, goods_info['name'], '丹药', v, bind_flag=1)
                sql_message.update_user_sect_elixir_get_num(user_info['user_id'])
                await handle_send(bot, event, msg)
                await sect_elixir_get.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_elixir_get.finish()


@sect_buff_info.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_buff_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门功法查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_buff_info.finish()
    sect_id = user_info['sect_id']
    if sect_id:
        sect_info = sql_message.get_sect_info(sect_id)
        if sect_info['mainbuff'] == 0 and sect_info['secbuff'] == 0:
            msg = f"本宗尚未获得任何功法、神通，请宗主发送宗门功法、神通搜寻来获得！"
            await handle_send(bot, event, msg)
            await sect_buff_info.finish()

        list_tp = []
        msg = ""
        if sect_info['mainbuff'] != 0:
            mainbufflist = get_sect_mainbuff_id_list(sect_id)
            main_msg = f"\n☆------宗门功法------☆\n"
            msg += main_msg
            list_tp.append(
                {"type": "node", "data": {"name": f"道友{user_info['user_name']}的宗门功法信息", "uin": bot.self_id,
                                          "content": main_msg}})
            for main in mainbufflist:
                mainbuff, mainbuffmsg = get_main_info_msg(str(main))
                mainmsg = f"{mainbuff['level']}{mainbuff['name']}:{mainbuffmsg}\n"
                msg += mainmsg
                list_tp.append(
                    {"type": "node", "data": {"name": f"道友{user_info['user_name']}的宗门秘籍信息", "uin": bot.self_id,
                                              "content": mainmsg}})

        if sect_info['secbuff'] != 0:
            secbufflist = get_sect_secbuff_id_list(sect_id)
            sec_msg = f"☆------宗门神通------☆\n"
            msg += sec_msg
            list_tp.append(
                {"type": "node", "data": {"name": f"道友{user_info['user_name']}的宗门神通信息", "uin": bot.self_id,
                                          "content": sec_msg}})
            for sec in secbufflist:
                secbuff = items.get_data_by_item_id(sec)
                secbuffmsg = get_sec_msg(secbuff)
                secmsg = f"{secbuff['level']}:{secbuff['name']} {secbuffmsg}\n"
                msg += secmsg
                list_tp.append(
                    {"type": "node", "data": {"name": f"道友{user_info['user_name']}的宗门神通信息", "uin": bot.self_id,
                                              "content": secmsg}})
        try:
            await send_msg_handler(bot, event, list_tp)
        except ActionFailed:
            await handle_send(bot, event, msg)
            await sect_buff_info.finish()
        await sect_buff_info.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_buff_info.finish()


@sect_mainbuff_learn.handle(parameterless=[Cooldown(stamina_cost = 1, cd_time=10, at_sender=False)])
async def sect_mainbuff_learn_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """学习宗门功法"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_mainbuff_learn.finish()
    msg = args.extract_plain_text().strip()
    sect_id = user_info['sect_id']
    if sect_id:
        sect_position = user_info['sect_position']
        if sect_position == 4:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}，不满足学习要求!"""
            await handle_send(bot, event, msg)
            await sect_mainbuff_learn.finish()
        else:
            sect_info = sql_message.get_sect_info(sect_id)
            if sect_info['mainbuff'] == 0:
                msg = f"本宗尚未获得宗门功法，请宗主发送宗门功法搜寻来获得宗门功法！"
                await handle_send(bot, event, msg)
                await sect_mainbuff_learn.finish()

            sectmainbuffidlist = get_sect_mainbuff_id_list(sect_id)

            if msg not in get_mainname_list(sectmainbuffidlist):
                msg = f"本宗还没有该功法，请发送本宗有的功法进行学习！"
                await handle_send(bot, event, msg)
                await sect_mainbuff_learn.finish()

            userbuffinfo = UserBuffDate(user_info['user_id']).BuffInfo
            mainbuffid = get_mainnameid(msg, sectmainbuffidlist)
            if str(userbuffinfo['main_buff']) == str(mainbuffid):
                msg = f"道友请勿重复学习！"
                await handle_send(bot, event, msg)
                await sect_mainbuff_learn.finish()

            mainbuffconfig = config['宗门主功法参数']
            mainbuff = items.get_data_by_item_id(mainbuffid)
            mainbufftype = mainbuff['level']
            mainbuffgear = buffrankkey[mainbufftype]
            # 获取逻辑
            materialscost = mainbuffgear * mainbuffconfig['学习资材消耗']
            if sect_info['sect_materials'] >= materialscost:
                sql_message.update_sect_materials(sect_id, materialscost, 2)
                sql_message.updata_user_main_buff(user_info['user_id'], mainbuffid)
                mainbuff, mainbuffmsg = get_main_info_msg(str(mainbuffid))
                msg = f"本次学习消耗{materialscost}宗门资材，成功学习到本宗{mainbufftype}功法：{mainbuff['name']}\n{mainbuffmsg}"
                await handle_send(bot, event, msg)
                await sect_mainbuff_learn.finish()
            else:
                msg = f"本次学习需要消耗{materialscost}宗门资材，不满足条件！"
                await handle_send(bot, event, msg)
                await sect_mainbuff_learn.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_mainbuff_learn.finish()


@sect_mainbuff_get.handle(parameterless=[Cooldown(stamina_cost=8, at_sender=False)])
async def sect_mainbuff_get_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """搜寻宗门功法"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_mainbuff_get.finish()
    sect_id = user_info['sect_id']
    if sect_id:
        sect_position = user_info['sect_position']
        owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
        owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
        if sect_position == owner_position:
            mainbuffconfig = config['宗门主功法参数']
            sect_info = sql_message.get_sect_info(sect_id)
            mainbuffgear, mainbufftype = get_sectbufftxt(sect_info['sect_scale'], mainbuffconfig)
            stonecost = mainbuffgear * mainbuffconfig['获取消耗的灵石']
            materialscost = mainbuffgear * mainbuffconfig['获取消耗的资材']
            total_stone_cost = stonecost
            total_materials_cost = materialscost

            if sect_info['sect_used_stone'] >= total_stone_cost and sect_info['sect_materials'] >= total_materials_cost:
                success_count = 0
                fail_count = 0
                repeat_count = 0
                mainbuffidlist = get_sect_mainbuff_id_list(sect_id)
                results = []

                for i in range(100):
                    if random.randint(0, 100) <= mainbuffconfig['获取到功法的概率']:
                        mainbuffid = random.choice(BuffJsonDate().get_gfpeizhi()[mainbufftype]['gf_list'])
                        if mainbuffid in mainbuffidlist:
                            mainbuff, mainbuffmsg = get_main_info_msg(mainbuffid)
                            repeat_count += 1
                            results.append(f"第{i + 1}次获取到重复功法：{mainbuff['name']}")
                        else:
                            mainbuffidlist.append(mainbuffid)
                            mainbuff, mainbuffmsg = get_main_info_msg(mainbuffid)
                            success_count += 1
                            results.append(f"第{i + 1}次获取到{mainbufftype}功法：{mainbuff['name']}")
                    else:
                        fail_count += 1

                sql_message.update_sect_materials(sect_id, total_materials_cost, 2)
                sql_message.update_sect_scale_and_used_stone(sect_id, sect_info['sect_used_stone'] - total_stone_cost, sect_info['sect_scale'])
                sql = set_sect_list(mainbuffidlist)
                sql_message.update_sect_mainbuff(sect_id, sql)

                msg = f"共消耗{total_stone_cost}宗门灵石，{total_materials_cost}宗门资材。\n"
                msg += f"失败{fail_count}次，获取重复功法{repeat_count}次"
                if success_count > 0:
                    msg += f"，搜寻到新功法{success_count}次。\n"
                else:
                    msg += f"，未搜寻到新功法！\n"
                msg += f"\n".join(results)

                await handle_send(bot, event, msg)
                await sect_mainbuff_get.finish()
            else:
                msg = f"需要消耗{total_stone_cost}宗门灵石，{total_materials_cost}宗门资材，不满足条件！"
                await handle_send(bot, event, msg)
                await sect_mainbuff_get.finish()
        else:
            msg = f"道友不是宗主，无法使用该命令！"
            await handle_send(bot, event, msg)
            await sect_mainbuff_get.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_mainbuff_get.finish()


@sect_secbuff_get.handle(parameterless=[Cooldown(stamina_cost=8, at_sender=False)])
async def sect_secbuff_get_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """搜寻宗门神通"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_secbuff_get.finish()
    sect_id = user_info['sect_id']
    if sect_id:
        sect_position = user_info['sect_position']
        owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
        owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
        if sect_position == owner_position:
            secbuffconfig = config['宗门神通参数']
            sect_info = sql_message.get_sect_info(sect_id)
            secbuffgear, secbufftype = get_sectbufftxt(sect_info['sect_scale'], secbuffconfig)
            stonecost = secbuffgear * secbuffconfig['获取消耗的灵石']
            materialscost = secbuffgear * secbuffconfig['获取消耗的资材']
            total_stone_cost = stonecost
            total_materials_cost = materialscost
            
            if sect_info['sect_used_stone'] >= total_stone_cost and sect_info['sect_materials'] >= total_materials_cost:
                success_count = 0
                fail_count = 0
                repeat_count = 0
                secbuffidlist = get_sect_secbuff_id_list(sect_id)
                results = []

                for i in range(100):
                    if random.randint(0, 100) <= secbuffconfig['获取到神通的概率']:
                        secbuffid = random.choice(BuffJsonDate().get_gfpeizhi()[secbufftype]['st_list'])
                        if secbuffid in secbuffidlist:
                            secbuff = items.get_data_by_item_id(secbuffid)
                            repeat_count += 1
                            results.append(f"第{i + 1}次获取到重复神通：{secbuff['name']}")
                        else:
                            secbuffidlist.append(secbuffid)
                            secbuff = items.get_data_by_item_id(secbuffid)
                            success_count += 1
                            results.append(f"第{i + 1}次获取到{secbufftype}神通：{secbuff['name']}\n")
                    else:
                        fail_count += 1

                sql_message.update_sect_materials(sect_id, total_materials_cost, 2)
                sql_message.update_sect_scale_and_used_stone(sect_id, sect_info['sect_used_stone'] - total_stone_cost, sect_info['sect_scale'])
                sql = set_sect_list(secbuffidlist)
                sql_message.update_sect_secbuff(sect_id, sql)

                msg = f"共消耗{total_stone_cost}宗门灵石，{total_materials_cost}宗门资材。\n"
                msg += f"失败{fail_count}次，获取重复神通{repeat_count}次"
                if success_count > 0:
                    msg += f"，搜寻到新神通{success_count}次。\n"
                else:
                    msg += f"，未搜寻到新神通！\n"
                msg += f"\n".join(results)

                await handle_send(bot, event, msg)
                await sect_secbuff_get.finish()
            else:
                msg = f"需要消耗{total_stone_cost}宗门灵石，{total_materials_cost}宗门资材，不满足条件！"
                await handle_send(bot, event, msg)
                await sect_secbuff_get.finish()
        else:
            msg = f"道友不是宗主，无法使用该命令！"
            await handle_send(bot, event, msg)
            await sect_secbuff_get.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_secbuff_get.finish()


@sect_secbuff_learn.handle(parameterless=[Cooldown(stamina_cost=1, cd_time=10, at_sender=False)])
async def sect_secbuff_learn_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """学习宗门神通"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_secbuff_learn.finish()
    msg = args.extract_plain_text().strip()
    sect_id = user_info['sect_id']
    if sect_id:
        sect_position = user_info['sect_position']
        if sect_position == 4:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]['title']}，不满足学习要求!"""
            await handle_send(bot, event, msg)
            await sect_secbuff_learn.finish()
        else:
            sect_info = sql_message.get_sect_info(sect_id)
            if sect_info['secbuff'] == 0:
                msg = f"本宗尚未获得宗门神通，请宗主发送宗门神通搜寻来获得宗门神通！"
                await handle_send(bot, event, msg)
                await sect_secbuff_learn.finish()

            sectsecbuffidlist = get_sect_secbuff_id_list(sect_id)

            if msg not in get_secname_list(sectsecbuffidlist):
                msg = f"本宗还没有该神通，请发送本宗有的神通进行学习！"

                await handle_send(bot, event, msg)
                await sect_secbuff_learn.finish()

            userbuffinfo = UserBuffDate(user_info['user_id']).BuffInfo
            secbuffid = get_secnameid(msg, sectsecbuffidlist)
            if str(userbuffinfo['sec_buff']) == str(secbuffid):
                msg = f"道友请勿重复学习！"
                await handle_send(bot, event, msg)
                await sect_secbuff_learn.finish()

            secbuffconfig = config['宗门神通参数']

            secbuff = items.get_data_by_item_id(secbuffid)
            secbufftype = secbuff['level']
            secbuffgear = buffrankkey[secbufftype]
            # 获取逻辑
            materialscost = secbuffgear * secbuffconfig['学习资材消耗']
            if sect_info['sect_materials'] >= materialscost:
                sql_message.update_sect_materials(sect_id, materialscost, 2)
                sql_message.updata_user_sec_buff(user_info['user_id'], secbuffid)
                secmsg = get_sec_msg(secbuff)
                msg = f"本次学习消耗{materialscost}宗门资材，成功学习到本宗{secbufftype}神通：{secbuff['name']}\n{secbuff['name']}：{secmsg}"
                await handle_send(bot, event, msg)
                await sect_secbuff_learn.finish()
            else:
                msg = f"本次学习需要消耗{materialscost}宗门资材，不满足条件！"
                await handle_send(bot, event, msg)
                await sect_secbuff_learn.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_secbuff_learn.finish()


@upatkpractice.handle(parameterless=[Cooldown(at_sender=False, cd_time=10)])
async def upatkpractice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """升级攻击修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await upatkpractice.finish()
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    level_up_count = 1
    config_max_level = max(int(key) for key in LEVLECOST.keys())
    raw_args = args.extract_plain_text().strip()
    try:
        level_up_count = int(raw_args)
        level_up_count = min(max(1, level_up_count), config_max_level)
    except ValueError:
        level_up_count = 1
    if sect_id:
        sect_materials = int(sql_message.get_sect_info(sect_id)['sect_materials'])  # 当前资材
        useratkpractice = int(user_info['atkpractice'])  # 当前等级
        if useratkpractice == 100:
            msg = f"道友的攻击修炼等级已达到最高等级!"
            await handle_send(bot, event, msg)
            await upatkpractice.finish()

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[
                                                       0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限25级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - useratkpractice)
        if sect_position == 4:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}，不满足使用资材的条件!"""
            await handle_send(bot, event, msg)
            await upatkpractice.finish()

        if useratkpractice >= sect_level:
            msg = f"道友的攻击修炼等级已达到当前宗门修炼等级的最高等级：{sect_level}，请捐献灵石提升贡献度吧！"
            await handle_send(bot, event, msg)
            await upatkpractice.finish()

        total_stone_cost = sum(LEVLECOST[str(useratkpractice + i)] for i in range(level_up_count))
        total_materials_cost = int(total_stone_cost * 10)

        if int(user_info['stone']) < total_stone_cost:
            msg = f"道友的灵石不够，升级到攻击修炼等级 {useratkpractice + level_up_count} 还需 {total_stone_cost - int(user_info['stone'])} 灵石!"
            await handle_send(bot, event, msg)
            await upatkpractice.finish()

        if sect_materials < total_materials_cost:
            msg = f"道友的所处的宗门资材不足，还需 {total_materials_cost - sect_materials} 资材来升级到攻击修炼等级 {useratkpractice + level_up_count}!"
            await handle_send(bot, event, msg)
            await upatkpractice.finish()

        sql_message.update_ls(user_id, total_stone_cost, 2)
        sql_message.update_sect_materials(sect_id, total_materials_cost, 2)
        sql_message.update_user_atkpractice(user_id, useratkpractice + level_up_count)
        msg = f"升级成功，道友当前攻击修炼等级：{useratkpractice + level_up_count}，消耗灵石：{total_stone_cost}枚，消耗宗门资材{total_materials_cost}!"
        await handle_send(bot, event, msg)
        await upatkpractice.finish()
    else:
        msg = f"修炼逆天而行消耗巨大，请加入宗门再进行修炼！"
        await handle_send(bot, event, msg)
        await upatkpractice.finish()

@uphppractice.handle(parameterless=[Cooldown(at_sender=False, cd_time=10)])
async def uphppractice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """升级元血修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await uphppractice.finish()
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    level_up_count = 1
    config_max_level = max(int(key) for key in LEVLECOST.keys())
    raw_args = args.extract_plain_text().strip()
    try:
        level_up_count = int(raw_args)
        level_up_count = min(max(1, level_up_count), config_max_level)
    except ValueError:
        level_up_count = 1
    if sect_id:
        sect_materials = int(sql_message.get_sect_info(sect_id)['sect_materials'])  # 当前资材
        userhppractice = int(user_info['hppractice'])  # 当前等级
        if userhppractice == 100:
            msg = f"道友的元血修炼等级已达到最高等级!"
            await handle_send(bot, event, msg)
            await uphppractice.finish()

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[
                                                       0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限25级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - userhppractice)
        if sect_position == 4:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}，不满足使用资材的条件!"""
            await handle_send(bot, event, msg)
            await uphppractice.finish()

        if userhppractice >= sect_level:
            msg = f"道友的元血修炼等级已达到当前宗门修炼等级的最高等级：{sect_level}，请捐献灵石提升贡献度吧！"
            await handle_send(bot, event, msg)
            await uphppractice.finish()

        total_stone_cost = sum(LEVLECOST[str(userhppractice + i)] for i in range(level_up_count))
        total_materials_cost = int(total_stone_cost * 10)

        if int(user_info['stone']) < total_stone_cost:
            msg = f"道友的灵石不够，升级到元血修炼等级 {userhppractice + level_up_count} 还需 {total_stone_cost - int(user_info['stone'])} 灵石!"
            await handle_send(bot, event, msg)
            await uphppractice.finish()

        if sect_materials < total_materials_cost:
            msg = f"道友的所处的宗门资材不足，还需 {total_materials_cost - sect_materials} 资材来升级到元血修炼等级 {userhppractice + level_up_count}!"
            await handle_send(bot, event, msg)
            await uphppractice.finish()

        sql_message.update_ls(user_id, total_stone_cost, 2)
        sql_message.update_sect_materials(sect_id, total_materials_cost, 2)
        sql_message.update_user_hppractice(user_id, userhppractice + level_up_count)
        msg = f"升级成功，道友当前元血修炼等级：{userhppractice + level_up_count}，消耗灵石：{total_stone_cost}枚，消耗宗门资材{total_materials_cost}!"
        await handle_send(bot, event, msg)
        await uphppractice.finish()
    else:
        msg = f"修炼逆天而行消耗巨大，请加入宗门再进行修炼！"
        await handle_send(bot, event, msg)
        await uphppractice.finish()
        
@upmppractice.handle(parameterless=[Cooldown(at_sender=False, cd_time=10)])
async def upmppractice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """升级灵海修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await upmppractice.finish()
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    level_up_count = 1
    config_max_level = max(int(key) for key in LEVLECOST.keys())
    raw_args = args.extract_plain_text().strip()
    try:
        level_up_count = int(raw_args)
        level_up_count = min(max(1, level_up_count), config_max_level)
    except ValueError:
        level_up_count = 1
    if sect_id:
        sect_materials = int(sql_message.get_sect_info(sect_id)['sect_materials'])  # 当前资材
        usermppractice = int(user_info['mppractice'])  # 当前等级
        if usermppractice == 100:
            msg = f"道友的灵海修炼等级已达到最高等级!"
            await handle_send(bot, event, msg)
            await upmppractice.finish()

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[
                                                       0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限25级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - usermppractice)
        if sect_position == 4:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}，不满足使用资材的条件!"""
            await handle_send(bot, event, msg)
            await upmppractice.finish()

        if usermppractice >= sect_level:
            msg = f"道友的灵海修炼等级已达到当前宗门修炼等级的最高等级：{sect_level}，请捐献灵石提升贡献度吧！"
            await handle_send(bot, event, msg)
            await upmppractice.finish()

        total_stone_cost = sum(LEVLECOST[str(usermppractice + i)] for i in range(level_up_count))
        total_materials_cost = int(total_stone_cost * 10)

        if int(user_info['stone']) < total_stone_cost:
            msg = f"道友的灵石不够，升级到灵海修炼等级 {usermppractice + level_up_count} 还需 {total_stone_cost - int(user_info['stone'])} 灵石!"
            await handle_send(bot, event, msg)
            await upmppractice.finish()

        if sect_materials < total_materials_cost:
            msg = f"道友的所处的宗门资材不足，还需 {total_materials_cost - sect_materials} 资材来升级到灵海修炼等级 {usermppractice + level_up_count}!"
            await handle_send(bot, event, msg)
            await upmppractice.finish()

        sql_message.update_ls(user_id, total_stone_cost, 2)
        sql_message.update_sect_materials(sect_id, total_materials_cost, 2)
        sql_message.update_user_mppractice(user_id, usermppractice + level_up_count)
        msg = f"升级成功，道友当前灵海修炼等级：{usermppractice + level_up_count}，消耗灵石：{total_stone_cost}枚，消耗宗门资材{total_materials_cost}!"
        await handle_send(bot, event, msg)
        await upmppractice.finish()
    else:
        msg = f"修炼逆天而行消耗巨大，请加入宗门再进行修炼！"
        await handle_send(bot, event, msg)
        await upmppractice.finish()
        
        
@sect_task_refresh.handle(parameterless=[Cooldown(cd_time=config['宗门任务刷新cd'], at_sender=False)])
async def sect_task_refresh_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """刷新宗门任务"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_task_refresh.finish()
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    if sect_id:
        if isUserTask(user_id):
            create_user_sect_task(user_id)
            msg = f"已刷新，道友当前接取的任务：{userstask[user_id]['任务名称']}\n{userstask[user_id]['任务内容']['desc']}"
            await handle_send(bot, event, msg)
            await sect_task_refresh.finish()
        else:
            msg = f"道友目前还没有宗门任务，请发送指令宗门任务接取来获取吧"
            await handle_send(bot, event, msg)
            await sect_task_refresh.finish()

    else:
        msg = f"道友尚未加入宗门，请加入宗门后再发送该指令！"
        await handle_send(bot, event, msg)
        await sect_task_refresh.finish()


@sect_list.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_list_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门列表：当前为返回转发内容"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    sect_lists_with_members = sql_message.get_all_sects_with_member_count()

    msg_list = []
    for sect in sect_lists_with_members:
        sect_id, sect_name, sect_scale, user_name, member_count = sect
        msg_list.append(f"编号{sect_id}：{sect_name}\n宗主：{user_name}\n宗门建设度：{number_to(sect_scale)}\n成员数：{member_count}")

    await send_msg_handler(bot, event, '宗门列表', bot.self_id, msg_list)
    await sect_list.finish()


@sect_users.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_users_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):  
    """查看所在宗门成员信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg_list = []
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_users.finish()
    if user_info:
        sect_id = user_info['sect_id']
        if sect_id:
            sect_info = sql_message.get_sect_info(sect_id)
            userlist = sql_message.get_all_users_by_sect_id(sect_id)
            msg = f"☆【{sect_info['sect_name']}】的成员信息☆\n"
            msg_list.append(msg)
            i = 1
            for user in userlist:
                msg = f"""编号{i}:{user['user_name']},{user['level']}\n宗门职位：{jsondata.sect_config_data()[f"{user['sect_position']}"]['title']}\n宗门贡献度：{user['sect_contribution']}\n"""
                msg += f"QQ号{user['user_id']}"
                msg_list.append(msg)
                i += 1
        else:
            msg_list.append(f"一介散修，莫要再问。")
    else:
        msg_list.append(f"未曾踏入修仙世界，输入【我要修仙】加入我们，看破这世间虚妄!")
    await send_msg_handler(bot, event, '宗门成员', bot.self_id, msg_list)
    await sect_users.finish()


@sect_task.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_task_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """获取宗门任务"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_task.finish()
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    if sect_id:
        user_now_num = int(user_info['sect_task'])
        if user_now_num >= config["每日宗门任务次上限"]:
            msg = f"道友已完成{user_now_num}次，今日无法再获取宗门任务了！"
            await handle_send(bot, event, msg)
            await sect_task.finish()

        if isUserTask(user_id):  # 已有任务
            msg = f"道友当前已接取了任务：{userstask[user_id]['任务名称']}\n{userstask[user_id]['任务内容']['desc']}"
            await handle_send(bot, event, msg)
            await sect_task.finish()

        create_user_sect_task(user_id)
        msg = f"{userstask[user_id]['任务内容']['desc']}"
        await handle_send(bot, event, msg)
        await sect_task.finish()
    else:
        msg = f"道友尚未加入宗门，请加入宗门后再获取任务！"
        await handle_send(bot, event, msg)
        await sect_task.finish()


@sect_task_complete.handle(parameterless=[Cooldown(cd_time=config['宗门任务完成cd'], stamina_cost = 3,at_sender=False)])
async def sect_task_complete_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """完成宗门任务"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_task_complete.finish()
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    if sect_id:
        if not isUserTask(user_id):
            msg = f"道友当前没有接取宗门任务，道友浪费了一次出门机会哦！"
            await handle_send(bot, event, msg)
            await sect_task_complete.finish()

        if userstask[user_id]['任务内容']['type'] == 1:  # type=1：需要扣气血，type=2：需要扣灵石
            costhp = int((user_info['exp'] / 2) * userstask[user_id]['任务内容']['cost'])
            if user_info['hp'] < user_info['exp'] / 10 or costhp >= user_info['hp']:
                msg = (
                    f"道友兴高采烈的出门做任务，结果状态欠佳，没过两招就力不从心，坚持不住了，"
                    f"道友只好原路返回，浪费了一次出门机会，看你这么可怜，就不扣你任务次数了！"
                )
                await handle_send(bot, event, msg)
                await sect_task_complete.finish()

            get_exp = int(user_info['exp'] * userstask[user_id]['任务内容']['give'])

            if user_info['sect_position'] is None:
                max_exp_limit = 4
            else:
                max_exp_limit = user_info['sect_position']
            max_exp = jsondata.sect_config_data()[str(max_exp_limit)]["max_exp"]
            if get_exp >= max_exp:
                get_exp = max_exp
            max_exp_next = int((int(OtherSet().set_closing_type(user_info['level'])) * XiuConfig().closing_exp_upper_limit))  # 获取下个境界需要的修为 * 1.5为闭关上限
            if int(get_exp + user_info['exp']) > max_exp_next:
                get_exp = 1
                msg = f"检测到修为将要到达上限！"
            sect_stone = int(userstask[user_id]['任务内容']['sect'])
            sql_message.update_user_hp_mp(user_id, user_info['hp'] - costhp, user_info['mp'])
            sql_message.update_exp(user_id, get_exp)
            sql_message.donate_update(user_info['sect_id'], sect_stone)
            sql_message.update_sect_materials(sect_id, sect_stone * 10, 1)
            sql_message.update_user_sect_task(user_id, 1)
            sql_message.update_user_sect_contribution(user_id, user_info['sect_contribution'] + int(sect_stone))
            msg += f"道友大战一番，气血减少：{costhp}，获得修为：{get_exp}，所在宗门建设度增加：{sect_stone}，资材增加：{sect_stone * 10}, 宗门贡献度增加：{int(sect_stone)}"
            userstask[user_id] = {}
            await handle_send(bot, event, msg)
            await sect_task_complete.finish()

        elif userstask[user_id]['任务内容']['type'] == 2:  # type=1：需要扣气血，type=2：需要扣灵石
            costls = userstask[user_id]['任务内容']['cost']

            if costls > int(user_info['stone']):
                msg = (
                    f"道友兴高采烈的出门做任务，结果发现灵石带少了，当前任务所需灵石：{costls},"
                    f"道友只好原路返回，浪费了一次出门机会，看你这么可怜，就不扣你任务次数了！")
                await handle_send(bot, event, msg)
                await sect_task_complete.finish()

            get_exp = int(user_info['exp'] * userstask[user_id]['任务内容']['give'])

            if user_info['sect_position'] is None:
                max_exp_limit = 4
            else:
                max_exp_limit = user_info['sect_position']
            max_exp = jsondata.sect_config_data()[str(max_exp_limit)]["max_exp"]
            if get_exp >= max_exp:
                get_exp = max_exp
            max_exp_next = int((int(OtherSet().set_closing_type(user_info['level'])) * XiuConfig().closing_exp_upper_limit))  # 获取下个境界需要的修为 * 1.5为闭关上限
            if int(get_exp + user_info['exp']) > max_exp_next:
                get_exp = 1
                msg = f"检测到修为将要到达上限！"
            sect_stone = int(userstask[user_id]['任务内容']['sect'])
            sql_message.update_ls(user_id, costls, 2)
            sql_message.update_exp(user_id, get_exp)
            sql_message.donate_update(user_info['sect_id'], sect_stone)
            sql_message.update_sect_materials(sect_id, sect_stone * 10, 1)
            sql_message.update_user_sect_task(user_id, 1)
            sql_message.update_user_sect_contribution(user_id, user_info['sect_contribution'] + int(sect_stone))
            msg = f"道友为了完成任务购买宝物消耗灵石：{costls}枚，获得修为：{get_exp}，所在宗门建设度增加：{sect_stone}，资材增加：{sect_stone * 10}, 宗门贡献度增加：{int(sect_stone)}"
            userstask[user_id] = {}
            await handle_send(bot, event, msg)
            await sect_task_complete.finish()
    else:
        msg = f"道友尚未加入宗门，请加入宗门后再完成任务，但你申请出门的机会我已经用小本本记下来了！"
        await handle_send(bot, event, msg)
        await sect_task_complete.finish()


@sect_owner_change.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_owner_change_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗主传位"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    enabled_groups = JsonConfig().get_enabled_groups()
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_owner_change.finish()
    user_id = user_info['user_id']
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg)
        await sect_owner_change.finish()
    position_this = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(position_this[0]) if len(position_this) == 1 else 0
    if user_info['sect_position'] != owner_position:
        msg = f"只有宗主才能进行传位。"
        await handle_send(bot, event, msg)
        await sect_owner_change.finish()
    give_qq = None  # 艾特的时候存到这里
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if give_qq:
        if give_qq == user_id:
            msg = f"无法对自己的进行传位操作。"
            await handle_send(bot, event, msg)
            await sect_owner_change.finish()
        else:
            give_user = sql_message.get_user_info_with_id(give_qq)
            if give_user['sect_id'] == user_info['sect_id']:
                sql_message.update_usr_sect(give_user['user_id'], give_user['sect_id'], owner_position)
                sql_message.update_usr_sect(user_info['user_id'], user_info['sect_id'], owner_position + 1)
                sect_info = sql_message.get_sect_info_by_id(give_user['sect_id'])
                sql_message.update_sect_owner(give_user['user_id'], sect_info['sect_id'])
                msg = f"传老宗主{user_info['user_name']}法旨，即日起由{give_user['user_name']}继任{sect_info['sect_name']}宗主"
                for group_id in enabled_groups:
                    bot = await assign_bot_group(group_id=group_id)
                    try:
                        await handle_send(bot, event, msg)
                    except ActionFailed:
                        continue
                await sect_owner_change.finish()
            else:
                msg = f"{give_user['user_name']}不在你管理的宗门内，请检查。"
                await handle_send(bot, event, msg)
                await sect_owner_change.finish()
    else:
        msg = f"请按照规范进行操作,ex:宗主传位@XXX,将XXX道友(需在自己管理下的宗门)升为宗主，自己则变为宗主下一等职位。"
        await handle_send(bot, event, msg)
        await sect_owner_change.finish()


@sect_rename.handle(parameterless=[Cooldown(cd_time=XiuConfig().sect_rename_cd * 86400,at_sender=False)])
async def sect_rename_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗门改名"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_rename.finish()
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg)
        await sect_rename.finish()
    position_this = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(position_this[0]) if len(position_this) == 1 else 0
    if user_info['sect_position'] != owner_position:
        msg = f"只有宗主才能进行改名！"
        await handle_send(bot, event, msg)
        await sect_rename.finish()
    else:
        update_sect_name = args.extract_plain_text().strip()
        sect_id = user_info['sect_id']
        sect_info = sql_message.get_sect_info(sect_id)
        enabled_groups = JsonConfig().get_enabled_groups()
        len_sect_name = len(update_sect_name.encode('gbk'))

        if len_sect_name > 20:
            msg = f"道友输入的宗门名字过长,请重新输入！"
            await handle_send(bot, event, msg)
            await sect_rename.finish()

        elif update_sect_name is None:
            msg = f"道友确定要改名无名之宗门？还请三思。"
            await handle_send(bot, event, msg)
            await sect_rename.finish()

        elif sect_info['sect_used_stone'] < XiuConfig().sect_rename_cost:
            msg = f"道友宗门灵石储备不足，还需{number_to(XiuConfig().sect_rename_cost - sect_info['sect_used_stone'])}灵石!"
            await handle_send(bot, event, msg)
            await sect_rename.finish()

        elif sql_message.update_sect_name(sect_id, update_sect_name) is False:
            msg = f"已存在同名宗门(自己宗门名字一样的就不要改了),请重新输入！"
            await handle_send(bot, event, msg)
            await sect_rename.finish()
        else:
            sql_message.update_sect_name(sect_id, update_sect_name)
            sql_message.update_sect_used_stone(sect_id, XiuConfig().sect_rename_cost, 2)
            msg = f"""
传宗门——{sect_info['sect_name']}
宗主{user_info['user_name']}法旨:
宗门改名为{update_sect_name}！
星斗更迭，法器灵通，神光熠熠。
愿同门共沐神光，共护宗门千世荣光！
青天无云，道韵长存，灵气飘然。
愿同门同心同德，共铸宗门万世辉煌！"""
            for group_id in enabled_groups:
                bot = await assign_bot_group(group_id=group_id)
                try:
                    await handle_send(bot, event, msg)
                except ActionFailed:
                    continue
            await sect_rename.finish()



@create_sect.handle(parameterless=[Cooldown(at_sender=False)])
async def create_sect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """创建宗门，对灵石、修为等级有要求，且需要当前状态无宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        msg = f"区区凡人，也想创立万世仙门，大胆！"
        await handle_send(bot, event, msg)
        await create_sect.finish()
    user_id = user_info['user_id']
    # 首先判断是否满足创建宗门的三大条件
    level = user_info['level']
    list_level_all = list(jsondata.level_data().keys())
    if (list_level_all.index(level) < list_level_all.index(XiuConfig().sect_min_level)):
        msg = f"创建宗门要求:创建者境界最低要求为{XiuConfig().sect_min_level}"

    elif user_info['stone'] < XiuConfig().sect_create_cost:
        msg = f"创建宗门要求:需要创建者拥有灵石{XiuConfig().sect_create_cost}枚"
    elif user_info['sect_id']:
        msg = f"道友已经加入了宗门:{user_info['sect_name']}，无法再创建宗门。"
    else:
        # 获取宗门名称
        sect_name = args.extract_plain_text().strip()
        if sect_name:
            sql_message.create_sect(user_id, sect_name)
            new_sect = sql_message.get_sect_info_by_qq(user_id)
            owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
            owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
            sql_message.update_usr_sect(user_id, new_sect['sect_id'], owner_position)
            sql_message.update_ls(user_id, XiuConfig().sect_create_cost, 2)
            msg = f"恭喜{user_info['user_name']}道友创建宗门——{sect_name}，宗门编号为{new_sect['sect_id']}。为道友贺！为仙道贺！"
        else:
            msg = f"道友确定要创建无名之宗门？还请三思。"
    await handle_send(bot, event, msg)
    await create_sect.finish()


@sect_kick_out.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_kick_out_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """踢出宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()
    user_id = user_info['user_id']
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()
    give_qq = None  # 艾特的时候存到这里
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if bool(give_qq) is False:
        msg = args.extract_plain_text().strip()
        give_qq = re.findall(r"\d+", msg)[0]  # QQ_ID
    if sql_message.get_user_info_with_id(give_qq) is None:
        msg = f"修仙界没有此人,请输入正确QQ_ID或正规at!"
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()
    if give_qq:
        if give_qq == user_id:
            msg = f"无法对自己的进行踢出操作，试试退出宗门？"
            await handle_send(bot, event, msg)
            await sect_kick_out.finish()
        else:
            give_user = sql_message.get_user_info_with_id(give_qq)
            if give_user['sect_id'] == user_info['sect_id']:
                position_zhanglao = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "长老"]
                idx_position = int(position_zhanglao[0]) if len(position_zhanglao) == 1 else 1
                if user_info['sect_position'] <= idx_position:
                    if give_user['sect_position'] <= user_info['sect_position']:
                        msg = f"""{give_user['user_name']}的宗门职务为{jsondata.sect_config_data()[f"{give_user['sect_position']}"]['title']}，不在你之下，无权操作。"""
                        await handle_send(bot, event, msg)
                        await sect_kick_out.finish()
                    else:
                        sect_info = sql_message.get_sect_info_by_id(give_user['sect_id'])
                        sql_message.update_usr_sect(give_user['user_id'], None, None)
                        sql_message.update_user_sect_contribution(give_user['user_id'], 0)
                        msg = f"""传{jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']}{user_info['user_name']}法旨，即日起{give_user['user_name']}被{sect_info['sect_name']}除名"""
                        await handle_send(bot, event, msg)
                        await sect_kick_out.finish()
                else:
                    msg = f"""你的宗门职务为{jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']}，只有长老及以上可执行踢出操作。"""
                    await handle_send(bot, event, msg)
                    await sect_kick_out.finish()
            else:
                msg = f"{give_user['user_name']}不在你管理的宗门内，请检查。"
                await handle_send(bot, event, msg)
                await sect_kick_out.finish()
    else:
        msg = f"请按照规范进行操作,ex:踢出宗门@XXX,将XXX道友(需在自己管理下的宗门）踢出宗门"
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()


@sect_out.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_out_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """退出宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_out.finish()
    user_id = user_info['user_id']
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg)
        await sect_out.finish()
    position_this = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(position_this[0]) if len(position_this) == 1 else 0
    sect_out_id = user_info['sect_id']
    if user_info['sect_position'] != owner_position:
        sql_message.update_usr_sect(user_id, None, None)
        sect_info = sql_message.get_sect_info_by_id(int(sect_out_id))
        sql_message.update_user_sect_contribution(user_id, 0)
        msg = f"道友已退出{sect_info['sect_name']}，今后就是自由散修，是福是祸，犹未可知。"
        await handle_send(bot, event, msg)
        await sect_out.finish()
    else:
        msg = f"宗主无法直接退出宗门，如确有需要，请完成宗主传位后另行尝试。"
        await handle_send(bot, event, msg)
        await sect_out.finish()


@sect_donate.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_donate_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗门捐献"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_donate.finish()
    user_id = user_info['user_id']
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg)
        await sect_donate.finish()
    msg = args.extract_plain_text().strip()
    donate_num = re.findall(r"\d+", msg)  # 捐献灵石数
    if len(donate_num) > 0:
        if int(donate_num[0]) > user_info['stone']:
            msg = f"道友的灵石数量小于欲捐献数量{int(donate_num[0])}，请检查"
            await handle_send(bot, event, msg)
            await sect_donate.finish()
        else:
            sql_message.update_ls(user_id, int(donate_num[0]), 2)
            sql_message.donate_update(user_info['sect_id'], int(donate_num[0]))
            sql_message.update_user_sect_contribution(user_id, user_info['sect_contribution'] + int(donate_num[0]))
            msg = f"道友捐献灵石{int(donate_num[0])}枚，宗门建设度增加：{int(donate_num[0])}，宗门贡献度增加：{int(donate_num[0])}点，蒸蒸日上！"
            await handle_send(bot, event, msg)
            await sect_donate.finish()
    else:
        msg = f"捐献的灵石数量解析异常"
        await handle_send(bot, event, msg)
        await sect_donate.finish()


@sect_position_update.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_position_update_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗门职位变更"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    user_id = user_info['user_id']

    position_zhanglao = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "长老"]
    idx_position = int(position_zhanglao[0]) if len(position_zhanglao) == 1 else 1
    if user_info['sect_position'] > idx_position:
        msg = f"""你的宗门职位为{jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']}，无权进行职位管理！"""
        await handle_send(bot, event, msg)
        await sect_position_update.finish()

    give_qq = None 
    msg = args.extract_plain_text().strip()
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：宗门职位变更 道号 3"
    
    if len(arg_list) < 2:
        position_num = re.findall(r"\d+", msg)
        nick_name = None
    else:
        position_num = arg_list[1]  # 职位
        nick_name = arg_list[0]  # 道号

    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    
    if nick_name:
        nick_name_user_info = sql_message.get_user_info_with_name(nick_name)
        give_qq = nick_name_user_info['user_id']
        
    if give_qq:
        if give_qq == user_id:
            msg = f"无法对自己的职位进行管理。"
            await handle_send(bot, event, msg)
            await sect_position_update.finish()
        else:
            if len(position_num) > 0 and position_num[0] in list(jsondata.sect_config_data().keys()):
                give_user = sql_message.get_user_info_with_id(give_qq)
                if give_user['sect_id'] == user_info['sect_id'] and give_user['sect_position'] > user_info['sect_position']:
                    if int(position_num[0]) > user_info['sect_position']:
                        sql_message.update_usr_sect(give_user['user_id'], give_user['sect_id'], int(position_num[0]))
                        msg = f"""传{jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']}{user_info['user_name']}法旨:即日起{give_user['user_name']}为本宗{jsondata.sect_config_data()[f"{int(position_num[0])}"]['title']}"""
                        await handle_send(bot, event, msg)
                        await sect_position_update.finish()
                    else:
                        msg = f"道友试图变更的职位品阶必须在你品阶之下"
                        await handle_send(bot, event, msg)
                        await sect_position_update.finish()
                else:
                    msg = f"请确保变更目标道友与你在同一宗门，且职位品阶在你之下。"
                    await handle_send(bot, event, msg)
                    await sect_position_update.finish()
            else:
                msg = f"职位品阶数字解析异常，请输入宗门职位变更帮助，查看支持的数字解析配置"
                await handle_send(bot, event, msg)
                await sect_position_update.finish()
    else:
        msg = f"""请按照规范进行操作,ex:宗门职位变更2@XXX,将XXX道友(需在自己管理下的宗门)的变更为{jsondata.sect_config_data().get('2', {'title': '没有找到2品阶'})['title']}"""
        await handle_send(bot, event, msg)
        await sect_position_update.finish()


@join_sect.handle(parameterless=[Cooldown(at_sender=False)])
async def join_sect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """加入宗门,后跟宗门ID,要求加入者当前状态无宗门,入门默认为外门弟子"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        msg = f"守山弟子：凡人，回去吧，仙途难入，莫要自误！"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    user_id = user_info['user_id']
    if not user_info['sect_id']:
        sect_no = args.extract_plain_text().strip()
        sql_sects = sql_message.get_all_sect_id()
        sects_all = [tup[0] for tup in sql_sects]
        if not sect_no.isdigit():
            msg = f"申请加入的宗门编号解析异常，应全为数字!"
        elif int(sect_no) not in sects_all:
            msg = f"申请加入的宗门编号似乎有误，未在宗门名录上发现!"
        else:
            owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "外门弟子"]
            owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 4
            sql_message.update_usr_sect(user_id, int(sect_no), owner_position)
            new_sect = sql_message.get_sect_info_by_id(int(sect_no))
            msg = f"欢迎{user_info['user_name']}师弟入我{new_sect['sect_name']}，共参天道。"
    else:
        msg = f"守山弟子：我观道友气运中已有宗门气运加持，又何必与我为难。"
    await handle_send(bot, event, msg)
    await join_sect.finish()


# editer:zyp981204
@my_sect.handle(parameterless=[Cooldown(at_sender=False)])
async def my_sect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        msg = f"守山弟子：凡人，回去吧，仙途难入，莫要自误！"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    elixir_room_level_up_config = config['宗门丹房参数']['elixir_room_level']
    sect_id = user_info['sect_id']
    sect_position = user_info['sect_position']
    user_name = user_info['user_name']
    sect_info = sql_message.get_sect_info(sect_id)
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    if sect_id:
        sql_res = sql_message.scale_top()
        top_idx_list = [_[0] for _ in sql_res]
        if int(sect_info['elixir_room_level']) == 0:
            elixir_room_name = "暂无"
        else:
            elixir_room_name = elixir_room_level_up_config[str(sect_info['elixir_room_level'])]['name']
        msg = f"""
{user_name}所在宗门
宗门名讳：{sect_info['sect_name']}
宗门编号：{sect_id}
宗   主：{sql_message.get_user_info_with_id(sect_info['sect_owner'])['user_name']}
道友职位：{jsondata.sect_config_data()[f"{sect_position}"]['title']}
宗门建设度：{number_to(sect_info['sect_scale'])}
洞天福地：{sect_info['sect_fairyland'] if sect_info['sect_fairyland'] else "暂无"}
宗门位面排名：{top_idx_list.index(sect_id) + 1}
宗门拥有资材：{number_to(sect_info['sect_materials'])}
宗门贡献度：{number_to(user_info['sect_contribution'])}
宗门丹房：{elixir_room_name}
"""
        if sect_position == owner_position:
            msg += f"\n宗门储备：{number_to(sect_info['sect_used_stone'])}枚灵石"
    else:
        msg = f"一介散修，莫要再问。"

    await handle_send(bot, event, msg)
    await my_sect.finish()


def create_user_sect_task(user_id):
    tasklist = config["宗门任务"]
    key = random.choices(list(tasklist))[0]
    userstask[user_id]['任务名称'] = key
    userstask[user_id]['任务内容'] = tasklist[key]      


def isUserTask(user_id):
    """判断用户是否已有任务 True:有任务"""
    Flag = False
    try:
        userstask[user_id]
    except:
        userstask[user_id] = {}

    if userstask[user_id] != {}:
        Flag = True

    return Flag


def get_sect_mainbuff_id_list(sect_id):
    """获取宗门功法id列表"""
    sect_info = sql_message.get_sect_info(sect_id)
    mainbufflist = str(sect_info['mainbuff'])[1:-1].split(',')
    return mainbufflist


def get_sect_secbuff_id_list(sect_id):
    """获取宗门神通id列表"""
    sect_info = sql_message.get_sect_info(sect_id)
    secbufflist = str(sect_info['secbuff'])[1:-1].split(',')
    return secbufflist


def set_sect_list(bufflist):
    """传入ID列表,返回[ID列表]"""
    sqllist1 = ''
    for buff in bufflist:
        if buff == '':
            continue
        sqllist1 += f'{buff},'
    sqllist = f"[{sqllist1[:-1]}]"
    return sqllist


def get_mainname_list(bufflist):
    """根据传入的功法列表，返回功法名字列表"""
    namelist = []
    for buff in bufflist:
        mainbuff = items.get_data_by_item_id(buff)
        namelist.append(mainbuff['name'])
    return namelist


def get_secname_list(bufflist):
    """根据传入的神通列表，返回神通名字列表"""
    namelist = []
    for buff in bufflist:
        secbuff = items.get_data_by_item_id(buff)
        namelist.append(secbuff['name'])
    return namelist


def get_mainnameid(buffname, bufflist):
    """根据传入的功法名字,获取到功法的id"""
    tempdict = {}
    buffid = 0
    for buff in bufflist:
        mainbuff = items.get_data_by_item_id(buff)
        tempdict[mainbuff['name']] = buff
    for k, v in tempdict.items():
        if buffname == k:
            buffid = v
    return buffid


def get_secnameid(buffname, bufflist):
    tempdict = {}
    buffid = 0
    for buff in bufflist:
        secbuff = items.get_data_by_item_id(buff)
        tempdict[secbuff['name']] = buff
    for k, v in tempdict.items():
        if buffname == k:
            buffid = v
    return buffid


def get_sectbufftxt(sect_scale, config_):
    """
    获取宗门当前获取功法的品阶 档位 + 3
    参数:sect_scale=宗门建设度
    config=宗门主功法参数
    """
    bufftxt = {1: '人阶下品', 2: '人阶上品', 3: '黄阶下品', 4: '黄阶上品', 5: '玄阶下品', 6: '玄阶上品', 7: '地阶下品', 8: '地阶上品', 9: '天阶下品', 10: '天阶上品', 50: '仙阶下品', 100: '仙阶上品'}
    buffgear = divmod(sect_scale, config_['建设度'])[0]
    if buffgear >= 100:
        buffgear = 100
    elif buffgear <= 1:
        buffgear = 1
    else:
        pass
    return buffgear, bufftxt[buffgear]


def get_sect_level(sect_id):
    sect = sql_message.get_sect_info(sect_id)
    return divmod(sect['sect_scale'], config["等级建设度"])
