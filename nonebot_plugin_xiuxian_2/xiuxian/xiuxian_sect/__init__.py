import re
import random
from nonebot.typing import T_State
from typing import List
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
    get_msg_pic, send_msg_handler, CommandObjectID, handle_send, handle_pagination,
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
sect_buff_info2 = on_command("宗门神通查看", aliases={"查看宗门神通"}, priority=9, block=True)
sect_users = on_command("宗门成员查看", aliases={"查看宗门成员"}, priority=8, block=True)
sect_elixir_room_make = on_command("宗门丹房建设", aliases={"建设宗门丹房"}, priority=5, block=True)
sect_elixir_get = on_command("宗门丹药领取", aliases={"领取宗门丹药"}, priority=5, block=True)
sect_rename = on_command("宗门改名", priority=5,  block=True)
sect_close_join = on_command("关闭宗门加入", priority=5, block=True)
sect_open_join = on_command("开放宗门加入", priority=5, block=True)
sect_close_mountain = on_command("封闭山门", priority=5, block=True)
sect_close_mountain2 = on_command("确认封闭山门", priority=5, block=True)
sect_disband = on_command("解散宗门", priority=5, block=True)
sect_disband2 = on_command("确认解散宗门", priority=5, block=True)
sect_inherit = on_command("继承宗主", priority=5, block=True)

__sect_help__ = f"""
【宗门系统】🏯🏯

🏛🏛️ 基础指令：
  • 我的宗门 - 查看当前宗门信息
  • 宗门列表 - 浏览全服宗门
  • 创建宗门 - 消耗{XiuConfig().sect_create_cost}灵石（需境界{XiuConfig().sect_min_level}）
  • 加入宗门 [ID] - 申请加入指定宗门

👑👑 宗主专属：
  • 宗门职位变更 [道号] [0-4] - 调整成员职位
    0=宗主 | 1=长老 | 2=亲传 | 3=内门 | 4=外门
  • 宗门改名 [新名称] - 修改宗门名称
  • 宗主传位 [道号] - 禅让宗主之位
  • 踢出宗门 [道号] - 移除宗门成员
  • 开放宗门加入 - 允许其他修士加入宗门
  • 关闭宗门加入 - 禁止其他修士加入宗门
  • 封闭山门 - 关闭宗门并退位为长老(需确认)
  • 解散宗门 - 解散宗门并踢出所有成员(需确认)

📈📈 宗门建设：
  • 宗门捐献 - 提升建设度（每{config["等级建设度"]}建设度提升1级修炼上限）
  • 升级攻击/元血/灵海修炼 - 提升对应属性（每级+4%攻/8%血/5%真元）

📚📚 功法传承：
  • 宗门功法、神通搜寻 - 宗主可消耗资源搜索功法（100次）
  • 学习宗门功法/神通 [名称] - 成员消耗资材学习
  • 宗门功法查看 - 浏览宗门藏书

💊💊 丹房系统：
  • 建设宗门丹房 - 开启每日丹药福利
  • 领取宗门丹药 - 获取每日丹药补给

📝📝 宗门任务：
  • 宗门任务接取 - 获取任务（每日上限：{config["每日宗门任务次上限"]}次）
  • 宗门任务完成 - 提交任务（CD：{config["宗门任务完成cd"]}秒）
  • 宗门任务刷新 - 更换任务（CD：{config["宗门任务刷新cd"]}秒）

⏰⏰⏰ 自动福利：
  • 每日{config["发放宗门资材"]["时间"]}点发放{config["发放宗门资材"]["倍率"]}倍建设度资材
  • 职位修为加成：宗主＞长老＞亲传＞内门＞外门＞散修

💡💡 小贴士：
  1. 外门弟子无法获得修炼资源
  2. 建设度决定宗门整体实力
  3. 每日任务收益随职位提升
  4. 封闭山门后长老可以使用【继承宗主】来继承宗主之位
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

# 定时任务每6小时自动检测并处理宗门状态
@auto_sect_owner_change.scheduled_job("interval", hours=6)
async def auto_handle_inactive_sect_owners():
    logger.info("⏳ 开始检测并处理宗门状态")
    
    try:
        # 使用新的方法获取宗门列表（包含成员数量）
        all_sects = sql_message.get_all_sects_with_member_count()
        logger.info(f"获取到宗门总数：{len(all_sects)}个")
        
        if not all_sects:
            logger.info("当前没有任何宗门存在，跳过处理")
            return
            
        for sect in all_sects:
            sect_id = sect[0]  # 宗门ID
            sect_name = sect[1]  # 宗门名称
            member_count = sect[4]  # 成员数量
            
            try:
                logger.info(f"处理宗门：{sect_name}(ID:{sect_id})")
                
                # 获取宗门详细信息
                sect_info = sql_message.get_sect_info(sect_id)
                if not sect_info:
                    logger.error(f"获取宗门详细信息失败，跳过处理")
                    continue
                    
                # ===== 第一阶段：优先处理已封闭山门的宗门 =====
                if sect_info['closed']:
                    logger.info("处理封闭山门的宗门（继承流程）")
                    
                    # 获取所有成员
                    members = sql_message.get_all_users_by_sect_id(sect_id)
                    logger.info(f"宗门成员数量：{len(members)}人")
                    
                    if not members:
                        logger.info("宗门没有成员，执行解散操作")
                        sql_message.delete_sect(sect_id)
                        logger.info(f"宗门 {sect_name}(ID:{sect_id}) 已解散")
                        continue
                        
                    # 按职位优先级和贡献度排序：长老(1) > 亲传(2) > 内门(3) > 外门(4)
                    sorted_members = sorted(
                        members,
                        key=lambda x: (x['sect_position'], -x['sect_contribution'])
                    )
                    
                    # 排除当前宗主(如果有)
                    candidates = [m for m in sorted_members if m['sect_position'] != 0]
                    logger.info(f"符合条件的候选人数量：{len(candidates)}")
                    
                    if not candidates:
                        logger.info("没有符合条件的继承人，执行解散操作")
                        sql_message.delete_sect(sect_id)
                        logger.info(f"宗门 {sect_name}(ID:{sect_id}) 已解散")
                        continue
                        
                    # 选择贡献最高的候选人
                    new_owner = candidates[0]
                    logger.info(f"选定继承人：{new_owner['user_name']}")
                    
                    # 执行继承
                    sql_message.update_usr_sect(new_owner['user_id'], sect_id, 0)  # 设为宗主
                    sql_message.update_sect_owner(new_owner['user_id'], sect_id)
                    sql_message.update_sect_closed_status(sect_id, 0)  # 解除封闭
                    sql_message.update_sect_join_status(sect_id, 1)  # 开放加入
                    
                    logger.info(f"宗门【{sect_name}】继承完成：新宗主：{new_owner['user_name']}")
                    continue
                    
                # ===== 第二阶段：处理未封闭的宗门（检测不活跃宗主） =====
                logger.info("检测未封闭的宗门（不活跃宗主检查）")
                
                owner_id = sect_info['sect_owner']
                if not owner_id:
                    logger.info("该宗门没有宗主，跳过检测")
                    continue
                    
                # 获取最后活跃时间
                last_check_time = sql_message.get_last_check_info_time(owner_id)
                if not last_check_time:
                    logger.info(f"宗主 {owner_id} 没有最后活跃时间记录，跳过检测")
                    continue
                    
                # 计算离线天数
                offline_days = (datetime.now() - last_check_time).days
                logger.info(f"宗主 {owner_id} 最后活跃：{last_check_time} | 已离线：{offline_days}天")
                
                if offline_days < 30:
                    logger.info("宗主活跃时间在30天内，跳过处理")
                    continue
                    
                # 获取宗主信息
                user_info = sql_message.get_user_info_with_id(owner_id)
                if not user_info:
                    logger.error(f"获取宗主信息失败：{owner_id}")
                    continue
                    
                logger.info(f"检测到不活跃宗主：{user_info['user_name']} 已离线 {offline_days} 天")
                
                # 执行降位处理
                sql_message.update_sect_join_status(sect_id, 0)  # 关闭宗门加入
                sql_message.update_sect_closed_status(sect_id, 1)  # 设置封闭状态
                sql_message.update_usr_sect(owner_id, sect_id, 1)  # 降为长老
                sql_message.update_sect_owner(None, sect_id)  # 清空宗主
                
                logger.info(f"宗门【{sect_name}】处理完成：原宗主 {user_info['user_name']} 已降为长老")
                
            except Exception as e:
                logger.error(f"处理宗门 {sect_id} 时发生错误：{str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"定时任务执行出错：{str(e)}")
    finally:
        logger.info("✅ 宗门状态检测处理完成")

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
    if not sect_id:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_buff_info.finish()
        
    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info['mainbuff']:
        msg = f"本宗尚未获得任何功法，请宗主发送【宗门功法搜寻】来获取！"
        await handle_send(bot, event, msg)
        await sect_buff_info.finish()

    # 获取功法列表
    mainbuff_list = get_sect_mainbuff_id_list(sect_id)
    if not mainbuff_list:
        msg = f"本宗功法列表为空！"
        await handle_send(bot, event, msg)
        await sect_buff_info.finish()

    # 构建消息
    msg_list = []
    msg_list.append("☆------宗门功法------☆")
    
    for mainbuff_id in mainbuff_list:
        if not mainbuff_id:  # 跳过空ID
            continue
        mainbuff, mainbuffmsg = get_main_info_msg(mainbuff_id)
        msg_list.append(f"{mainbuff['level']}{mainbuff['name']}")

    # 发送消息
    try:
        await send_msg_handler(bot, event, '宗门功法', bot.self_id, msg_list)
    except ActionFailed:
        msg = "\n".join(msg_list)
        await handle_send(bot, event, msg)
    
    await sect_buff_info.finish()

@sect_buff_info2.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_buff_info2_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门神通查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_buff_info2.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_buff_info2.finish()
        
    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info['secbuff']:
        msg = f"本宗尚未获得任何神通，请宗主发送【宗门神通搜寻】来获取！"
        await handle_send(bot, event, msg)
        await sect_buff_info2.finish()

    # 获取神通列表
    secbuff_list = get_sect_secbuff_id_list(sect_id)
    if not secbuff_list:
        msg = f"本宗神通列表为空！"
        await handle_send(bot, event, msg)
        await sect_buff_info2.finish()

    # 构建消息
    msg_list = []
    msg_list.append("☆------宗门神通------☆")
    
    for secbuff_id in secbuff_list:
        if not secbuff_id:  # 跳过空ID
            continue
        secbuff = items.get_data_by_item_id(secbuff_id)
        secbuffmsg = get_sec_msg(secbuff)
        msg_list.append(f"{secbuff['level']}:{secbuff['name']}")

    # 发送消息
    try:
        await send_msg_handler(bot, event, '宗门神通', bot.self_id, msg_list)
    except ActionFailed:
        msg = "\n".join(msg_list)
        await handle_send(bot, event, msg)
    
    await sect_buff_info2.finish()
        
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
        if sect_position == 4 or sect_position == 3:
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
            mainbuffgear = buffrankkey.get(mainbufftype, 100)
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
    """搜寻宗门功法（可获取当前及以下所有品阶功法）"""
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
            
            # 获取当前档位和所有可搜寻品阶
            mainbuffgear, mainbufftypes = get_sectbufftxt(sect_info['sect_scale'], mainbuffconfig)
            
            # 计算消耗（按最高档位计算）
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

                for i in range(100):  # 每次搜寻尝试100次
                    if random.randint(0, 100) <= mainbuffconfig['获取到功法的概率']:
                        # 随机从可获取品阶中选择一个
                        selected_tier = random.choice(mainbufftypes)
                        # 从该品阶的功法列表中随机选择
                        mainbuffid = random.choice(BuffJsonDate().get_gfpeizhi()[selected_tier]['gf_list'])
                        
                        if mainbuffid in mainbuffidlist:
                            mainbuff, mainbuffmsg = get_main_info_msg(mainbuffid)
                            repeat_count += 1
                            results.append(f"第{i+1}次获取到重复功法：{mainbuff['name']}({selected_tier})")
                        else:
                            mainbuffidlist.append(mainbuffid)
                            mainbuff, mainbuffmsg = get_main_info_msg(mainbuffid)
                            success_count += 1
                            results.append(f"第{i+1}次获取到{selected_tier}功法：{mainbuff['name']}")
                    else:
                        fail_count += 1

                # 更新数据库
                sql_message.update_sect_materials(sect_id, total_materials_cost, 2)
                sql_message.update_sect_scale_and_used_stone(
                    sect_id, 
                    sect_info['sect_used_stone'] - total_stone_cost, 
                    sect_info['sect_scale']
                )
                sql = set_sect_list(mainbuffidlist)
                sql_message.update_sect_mainbuff(sect_id, sql)

                # 构建结果消息
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
    """搜寻宗门神通（可获取当前及以下所有品阶神通）"""
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
            
            # 获取当前档位和所有可搜寻品阶
            secbuffgear, secbufftypes = get_sectbufftxt(sect_info['sect_scale'], secbuffconfig)
            
            # 计算消耗（按最高档位计算）
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

                for i in range(100):  # 每次搜寻尝试100次
                    if random.randint(0, 100) <= secbuffconfig['获取到神通的概率']:
                        # 随机从可获取品阶中选择一个
                        selected_tier = random.choice(secbufftypes)
                        # 从该品阶的神通列表中随机选择
                        secbuffid = random.choice(BuffJsonDate().get_gfpeizhi()[selected_tier]['st_list'])
                        
                        if secbuffid in secbuffidlist:
                            secbuff = items.get_data_by_item_id(secbuffid)
                            repeat_count += 1
                            results.append(f"第{i+1}次获取到重复神通：{secbuff['name']}({selected_tier})")
                        else:
                            secbuffidlist.append(secbuffid)
                            secbuff = items.get_data_by_item_id(secbuffid)
                            success_count += 1
                            results.append(f"第{i+1}次获取到{selected_tier}神通：{secbuff['name']}\n")
                    else:
                        fail_count += 1

                # 更新数据库
                sql_message.update_sect_materials(sect_id, total_materials_cost, 2)
                sql_message.update_sect_scale_and_used_stone(
                    sect_id, 
                    sect_info['sect_used_stone'] - total_stone_cost, 
                    sect_info['sect_scale']
                )
                sql = set_sect_list(secbuffidlist)
                sql_message.update_sect_secbuff(sect_id, sql)

                # 构建结果消息
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
        if sect_position == 4 or sect_position == 3:
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

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限100级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - useratkpractice)
        if sect_position == 4 or sect_position == 3:
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

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限100级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - userhppractice)
        if sect_position == 4 or sect_position == 3:
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

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限100级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - usermppractice)
        if sect_position == 4 or sect_position == 3:
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
        if user_name is None:
            user_name = "暂无"
        msg_list.append(f"编号{sect_id}：{sect_name}\n宗主：{user_name}\n宗门建设度：{number_to(sect_scale)}\n成员数：{member_count}")

    await send_msg_handler(bot, event, '宗门列表', bot.self_id, msg_list)
    await sect_list.finish()


@sect_users.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_users_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):  
    """查看所在宗门成员信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_users.finish()
    
    # 获取页码
    try:
        current_page = int(args.extract_plain_text().strip())
    except:
        current_page = 1
    
    if user_info:
        sect_id = user_info['sect_id']
        if sect_id:
            sect_info = sql_message.get_sect_info(sect_id)
            userlist = sql_message.get_all_users_by_sect_id(sect_id)
            
            # 构建成员信息列表
            msg_list = []
            for idx, user in enumerate(userlist, 1):
                msg = f"编号:{idx}\n道号:{user['user_name']}\n境界:{user['level']}\n"
                msg += f"宗门职位:{jsondata.sect_config_data()[str(user['sect_position'])]['title']}\n"
                msg += f"宗门贡献度:{user['sect_contribution']}"
                msg_list.append(msg)
            
            title = f"☆【{sect_info['sect_name']}】的成员信息☆"
            msgs = await handle_pagination(
                msg_list, 
                current_page,
                title=title,
                empty_msg="宗门目前没有成员！"
            )
            
            if isinstance(msgs, str):
                await handle_send(bot, event, msgs)
            else:
                await send_msg_handler(bot, event, '宗门成员', bot.self_id, msgs)
        else:
            msg = "一介散修，莫要再问。"
    else:
        msg = "未曾踏入修仙世界，输入【我要修仙】加入我们，看破这世间虚妄!"
        await handle_send(bot, event, msg)
    
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
            msg += f"道友大战一番，气血减少：{number_to(costhp)}，获得修为：{number_to(get_exp)}，所在宗门建设度增加：{sect_stone}，资材增加：{sect_stone * 10}, 宗门贡献度增加：{int(sect_stone)}"
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
            msg = f"道友为了完成任务购买宝物消耗灵石：{costls}枚，获得修为：{number_to(get_exp)}，所在宗门建设度增加：{sect_stone}，资材增加：{sect_stone * 10}, 宗门贡献度增加：{int(sect_stone)}"
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
async def create_sect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    """创建宗门（提供10个候选名称+取消选项）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    
    if not isUser:
        msg = "区区凡人，也想创立万世仙门，大胆！"
        await handle_send(bot, event, msg)
        await create_sect.finish()
    
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    sect_info = sql_message.get_sect_info(sect_id)
    level = user_info['level']
    list_level_all = list(jsondata.level_data().keys())
    # 检查境界
    if (list_level_all.index(level) < list_level_all.index(XiuConfig().sect_min_level)):
        msg = f"需达到{XiuConfig().sect_min_level}境才可创建宗门！"
        await handle_send(bot, event, msg)
        await create_sect.finish()
    
    # 检查灵石
    if user_info['stone'] < XiuConfig().sect_create_cost:
        msg = f"创建需{XiuConfig().sect_create_cost}灵石！"
        await handle_send(bot, event, msg)
        await create_sect.finish()
    
    # 检查是否已有宗门
    if user_info['sect_id']:
        msg = f"道友已是【{sect_info['sect_name']}】成员，无法另立门户！"
        await handle_send(bot, event, msg)
        await create_sect.finish()
    
    # 生成10个候选名称
    name_options = generate_random_sect_name(10)
    options_msg = "\n".join([f"{i}. {name}" for i, name in enumerate(name_options, 1)])    

    state["options"] = name_options
    state["user_id"] = user_id
    state["stone_cost"] = XiuConfig().sect_create_cost  # 存储创建所需灵石
    state["refresh_count"] = 0  # 刷新次数
    msg = (
        f"\n请选择宗门名称：\n"
        f"{options_msg}\n"
        f"0. 取消创建\n"
        f"00. 刷新名称\n"
        f"回复编号（0-10）进行选择\n"
        f"输入其他内容将随机选择"
    )
    await handle_send(bot, event, msg)

@create_sect.receive()
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    """处理选择结果"""
    user_choice = event.get_plaintext().strip()
    name_options = state["options"]
    user_id = state["user_id"]
    stone_cost = state["stone_cost"]
    refresh_count = state["refresh_count"]
    
    user_info = sql_message.get_user_info_with_id(user_id)
    
    # 0 - 取消创建
    if user_choice == "0":
        await create_sect.finish("道友已取消创建宗门。")
    
    # 00 - 刷新名称
    elif user_choice == "00":
        # 检查灵石是否足够
        if user_info['stone'] < stone_cost:
            # 灵石不足，自动随机选择一个
            sect_name = random.choice(name_options)
            msg = f"灵石不足，已自动选择宗门名称：{sect_name}"
            await handle_send(bot, event, msg)
            # 继续创建流程
        else:
            # 扣除灵石
            sql_message.update_ls(user_id, stone_cost, 2)
            # 生成新名称
            name_options = generate_random_sect_name(10)
            options_msg = "\n".join([f"{i}. {name}" for i, name in enumerate(name_options, 1)])
            
            # 更新状态
            state["options"] = name_options
            state["refresh_count"] = refresh_count + 1
            
            msg = (
                f"\n当前刷新次数：{refresh_count + 1}\n"
                f"请选择宗门名称：\n"
                f"{options_msg}\n"
                f"0. 取消创建\n"
                f"00. 再次刷新（每次刷新消耗{XiuConfig().sect_create_cost}灵石）\n"
                f"回复编号（0-10）进行选择\n"
                f"输入其他内容将随机选择"
            )
            await handle_send(bot, event, msg)
            await create_sect.reject()  # 继续等待用户选择
            return
    
    # 有效选择
    elif user_choice.isdigit() and 1 <= int(user_choice) <= 10:
        sect_name = name_options[int(user_choice)-1]
    else:
        # 非数字或超出范围，随机选择一个名字
        sect_name = random.choice(name_options)
    
    # 创建宗门
    sql_message.create_sect(user_id, sect_name)
    new_sect = sql_message.get_sect_info_by_qq(user_id)
    
    # 设置宗主职位
    owner_position = next(
        (k for k, v in jsondata.sect_config_data().items() if v.get("title") == "宗主"),
        0
    )
    sql_message.update_usr_sect(user_id, new_sect['sect_id'], owner_position)
    sql_message.update_ls(user_id, stone_cost, 2)  # 扣除创建费用
    
    # 获取用户信息
    user_info = sql_message.get_user_info_with_id(user_id)
    
    msg = (
        f"恭喜{user_info['user_name']}道友创建宗门——{sect_name}，"
        f"宗门编号为{new_sect['sect_id']}。\n"
        f"为道友贺！为仙道贺！"
    )
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
    
    # 检查用户是否有宗门
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()
    
    # 解析参数
    arg_list = args.extract_plain_text().strip().split()
    if len(arg_list) < 1:
        msg = f"请按照规范进行操作，例如：踢出宗门 道号"
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()
    
    # 获取目标用户信息
    nick_name = arg_list[0]  # 道号
    give_user = sql_message.get_user_info_with_name(nick_name)
    
    if not give_user:
        msg = f"修仙界没有名为【{nick_name}】的道友，请检查道号是否正确！"
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()
    
    # 检查不能踢自己
    if give_user['user_id'] == user_info['user_id']:
        msg = f"无法对自己进行操作，试试退出宗门？"
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()
    
    # 检查目标是否在同一宗门
    if give_user['sect_id'] != user_info['sect_id']:
        msg = f"{give_user['user_name']}不在你管理的宗门内，请检查。"
        await handle_send(bot, event, msg)
        await sect_kick_out.finish()
    
    # 获取长老职位配置
    position_zhanglao = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "长老"]
    idx_position = int(position_zhanglao[0]) if len(position_zhanglao) == 1 else 1
    
    # 检查操作者权限
    if user_info['sect_position'] <= idx_position:  # 长老及以上职位
        if give_user['sect_position'] <= user_info['sect_position']:
            msg = f"""{give_user['user_name']}的宗门职务为{jsondata.sect_config_data()[f"{give_user['sect_position']}"]['title']}，不在你之下，无权操作。"""
            await handle_send(bot, event, msg)
            await sect_kick_out.finish()
        else:
            # 执行踢出操作
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
    
    # 检查权限
    position_zhanglao = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "长老"]
    idx_position = int(position_zhanglao[0]) if len(position_zhanglao) == 1 else 1
    if user_info['sect_position'] > idx_position:
        msg = f"""你的宗门职位为{jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']}，无权进行职位管理！"""
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 解析参数（支持带空格的格式）
    raw_args = args.extract_plain_text().strip()
    if not raw_args:
        msg = f"请输入正确指令！例如：宗门职位变更 道号 3"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 分割参数（最后一个数字作为职位编号）
    args_list = raw_args.split()
    if len(args_list) < 2:
        msg = f"参数不足！格式应为：宗门职位变更 道号 职位编号"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 获取职位编号（取最后一个参数）
    position_num = args_list[-1]
    if not position_num.isdigit() or position_num not in jsondata.sect_config_data().keys():
        msg = f"职位编号解析异常，请输入宗门职位变更帮助，查看支持的编号"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 获取道号（合并前面的所有参数）
    nick_name = ' '.join(args_list[:-1]).strip()
    if not nick_name:
        msg = f"请输入有效的道号！"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 获取目标用户信息
    give_user = sql_message.get_user_info_with_name(nick_name)
    if not give_user:
        msg = f"修仙界没有名为【{nick_name}】的道友，请检查道号是否正确！"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 检查不能操作自己
    if give_user['user_id'] == user_id:
        msg = f"无法对自己的职位进行管理。"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 检查目标是否在同一宗门
    if give_user['sect_id'] != user_info['sect_id']:
        msg = f"请确保变更目标道友与你在同一宗门。"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 检查目标职位是否低于自己
    if give_user['sect_position'] <= user_info['sect_position']:
        msg = f"""{give_user['user_name']}的宗门职务为{jsondata.sect_config_data()[f"{give_user['sect_position']}"]['title']}，不在你之下，无权操作。"""
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 检查要变更的职位是否低于自己
    if int(position_num) <= user_info['sect_position']:
        msg = f"道友试图变更的职位品阶必须在你品阶之下"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 执行职位变更
    sql_message.update_usr_sect(give_user['user_id'], give_user['sect_id'], int(position_num))
    msg = f"""传{jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']}{user_info['user_name']}法旨:即日起{give_user['user_name']}为本宗{jsondata.sect_config_data()[f"{int(position_num)}"]['title']}"""
    await handle_send(bot, event, msg)
    await sect_position_update.finish()

@join_sect.handle(parameterless=[Cooldown(at_sender=False)])
async def join_sect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """加入宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        msg = f"守山弟子：凡人，回去吧，仙途难入，莫要自误！"
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 检查是否已有宗门
    sect_id = user_info['sect_id']
    sect_info = sql_message.get_sect_info(sect_id)
    if user_info['sect_id']:
        msg = f"道友已经加入了宗门:{sect_info['sect_name']}，无法再加入其他宗门。"
        await handle_send(bot, event, msg)
        await join_sect.finish()
    
    sect_no = args.extract_plain_text().strip()
    sql_sects = sql_message.get_all_sects_id_scale()
    sects_all = [tup[0] for tup in sql_sects]
    
    if not sect_no.isdigit():
        msg = f"申请加入的宗门编号解析异常，应全为数字!"
    elif int(sect_no) not in sects_all:
        msg = f"申请加入的宗门编号似乎有误，未在宗门名录上发现!"
    else:
        sect_info = sql_message.get_sect_info(int(sect_no))
        # 检查宗门是否封闭
        if sect_info['closed']:
            msg = f"该宗门已封闭山门，暂不接收新成员！"
        # 检查宗门是否开放加入
        elif not sect_info['join_open']:
            msg = f"该宗门已关闭加入，暂不接收新成员！"
        else:
            owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "外门弟子"]
            owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 4
            sql_message.update_usr_sect(user_info['user_id'], int(sect_no), owner_position)
            new_sect = sql_message.get_sect_info_by_id(int(sect_no))
            msg = f"欢迎{user_info['user_name']}师弟入我{new_sect['sect_name']}，共参天道。"
    
    await handle_send(bot, event, msg)
    await join_sect.finish()

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
        
        # 获取宗门状态
        join_status = "开放加入" if sect_info['join_open'] else "关闭加入"
        closed_status = "（封闭山门）" if sect_info['closed'] else ""
        
        msg = f"""
{user_name}所在宗门
宗门名讳：{sect_info['sect_name']}
宗门编号：{sect_id}
宗   主：{sql_message.get_user_info_with_id(sect_info['sect_owner'])['user_name'] if sect_info['sect_owner'] else "暂无"}
道友职位：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}
宗门状态：{join_status}{closed_status}
宗门建设度：{number_to(sect_info['sect_scale'])}
洞天福地：{sect_info['sect_fairyland'] if sect_info['sect_fairyland'] else "暂无"}
宗门位面排名：{top_idx_list.index(sect_id) + 1 if sect_id in top_idx_list else "未上榜"}
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

@sect_close_join.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_close_join_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """关闭宗门加入"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_close_join.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_close_join.finish()
    
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        sql_message.update_sect_join_status(sect_id, 0)
        msg = "已关闭宗门加入，其他修士将无法申请加入本宗！"
    else:
        msg = "只有宗主可以关闭宗门加入！"
    
    await handle_send(bot, event, msg)
    await sect_close_join.finish()

@sect_open_join.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_open_join_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """开放宗门加入"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_open_join.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_open_join.finish()
    
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        sql_message.update_sect_join_status(sect_id, 1)
        msg = "已开放宗门加入，其他修士可以申请加入本宗了！"
    else:
        msg = "只有宗主可以开放宗门加入！"
    
    await handle_send(bot, event, msg)
    await sect_open_join.finish()

@sect_close_mountain.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_close_mountain_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """封闭山门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_close_mountain.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_close_mountain.finish()
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        # 再次确认
        msg = "确定要封闭山门吗？封闭后：\n1. 自动关闭宗门加入\n2. 你将退位为长老\n3. 宗门将处于无主状态\n4. 长老们可以继承宗主之位\n\n请确认后再次发送【确认封闭山门】"
        await handle_send(bot, event, msg)
        await sect_close_mountain.finish()
    else:
        msg = "只有宗主可以封闭山门！"
        await handle_send(bot, event, msg)
        await sect_close_mountain.finish()

@sect_close_mountain2.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_close_mountain2_confirm(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """确认封闭山门"""

    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_close_mountain2.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_close_mountain2.finish()
    
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        # 1. 关闭宗门加入
        sql_message.update_sect_join_status(sect_id, 0)
        # 2. 设置封闭状态
        sql_message.update_sect_closed_status(sect_id, 1)
        # 3. 宗主退位为长老
        sql_message.update_usr_sect(user_info['user_id'], sect_id, 1)  # 1是长老职位
        # 4. 清空宗主
        sql_message.update_sect_owner(None, sect_id)
        
        msg = "已封闭山门！你已退位为长老，宗门现在处于无主状态。长老们可以使用【继承宗主】来继承宗主之位。"
    else:
        msg = "只有宗主可以封闭山门！"
    
    await handle_send(bot, event, msg)
    await sect_close_mountain2.finish()

@sect_inherit.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_inherit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """继承宗主"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_inherit.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_inherit.finish()
    
    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info['closed']:
        msg = "宗门未封闭，无需继承！"
        await handle_send(bot, event, msg)
        await sect_inherit.finish()
    
    # 检查职位是否符合继承条件
    if user_info['sect_position'] not in [1, 2, 3]:  # 1=长老, 2=亲传, 3=内门
        msg = "只有长老、亲传弟子或内门弟子可以继承宗主之位！"
        await handle_send(bot, event, msg)
        await sect_inherit.finish()
    
    # 检查是否有更高优先级的继承人
    members = sql_message.get_all_users_by_sect_id(sect_id)
    higher_priority = [
        m for m in members 
        if m['sect_position'] < user_info['sect_position'] 
        and m['sect_position'] != 0  # 排除当前宗主
    ]
    
    if higher_priority:
        msg = "存在更高优先级的继承人，请等待他们继承！"
        await handle_send(bot, event, msg)
        await sect_inherit.finish()
    
    # 执行继承
    # 1. 继承宗主
    sql_message.update_usr_sect(user_info['user_id'], sect_id, 0)  # 0是宗主
    sql_message.update_sect_owner(user_info['user_id'], sect_id)
    # 2. 解除封闭
    sql_message.update_sect_closed_status(sect_id, 0)
    # 3. 开放加入
    sql_message.update_sect_join_status(sect_id, 1)
    
    msg = f"恭喜{user_info['user_name']}继承宗主之位！宗门已解除封闭状态并开放加入。"
    await handle_send(bot, event, msg)
    await sect_inherit.finish()

@sect_disband.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_disband_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """解散宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_disband.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_disband.finish()
    
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        # 再次确认
        msg = "确定要解散宗门吗？解散后：\n1. 所有成员将被踢出\n2. 宗门将被删除\n3. 所有宗门资源将消失\n\n请确认后再次发送【确认解散宗门】"
        await handle_send(bot, event, msg)
        await sect_disband.finish()
    else:
        msg = "只有宗主可以解散宗门！"
        await handle_send(bot, event, msg)
        await sect_disband.finish()

@sect_disband2.handle(parameterless=[Cooldown(at_sender=False)])
async def sect_disband2_confirm(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """确认解散宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sect_disband2.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg)
        await sect_disband2.finish()
    
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        # 1. 获取所有成员
        members = sql_message.get_all_users_by_sect_id(sect_id)
        # 2. 踢出所有成员
        for member in members:
            sql_message.update_usr_sect(member['user_id'], None, None)
            sql_message.update_user_sect_contribution(member['user_id'], 0)
        # 3. 删除宗门
        sql_message.delete_sect(sect_id)
        
        msg = f"宗门已解散！所有成员已被移除。"
    else:
        msg = "只有宗主可以解散宗门！"
    
    await handle_send(bot, event, msg)
    await sect_disband2.finish()

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
    获取宗门当前可搜寻的功法/神通品阶列表（包含当前及以下所有品阶）
    参数:
        sect_scale: 宗门建设度
        config_: 宗门主功法/神通参数
    返回: (当前档位, 可搜寻品阶列表)
    """
    buff_gear_map = {
        1: '人阶下品',
        2: '人阶上品',
        3: '黄阶下品', 
        4: '黄阶上品',
        5: '玄阶下品',
        6: '玄阶上品',
        7: '地阶下品',
        8: '地阶上品', 
        90: '天阶下品',
        100: '天阶上品',
        500: '仙阶下品',
        1000: '仙阶上品'
    }
    
    # 计算当前档位
    current_gear = min(max(1, sect_scale // config_['建设度']), 1000)
    
    # 特殊处理仙阶档位
    if current_gear >= 1000:
        current_gear = 1000
    elif current_gear >= 500:
        current_gear = 500
    elif current_gear >= 100:
        current_gear = 100
    elif current_gear >= 90:
        current_gear = 90
    
    # 获取所有<=当前档位的品阶
    available_gears = [g for g in buff_gear_map.keys() if g <= current_gear]
    
    # 去重并排序
    available_gears = sorted(list(set(available_gears)))
    
    # 转换为品阶名称列表
    available_tiers = [buff_gear_map[g] for g in available_gears]
    
    return current_gear, available_tiers


def get_sect_level(sect_id):
    sect = sql_message.get_sect_info(sect_id)
    return divmod(sect['sect_scale'], config["等级建设度"])

def generate_random_sect_name(count: int = 1) -> List[str]:
    """随机生成多样化的宗门名称（包含正邪佛魔妖鬼等各类宗门）"""
    # 基础前缀词库（按字数分类，已大幅扩充）
    base_prefixes = {
        # 单字（1字） - 权重10%
        1: [
            # 天象类
            "天", "昊", "穹", "霄", "星", "月", "日", "辰", "云", "霞",
            "风", "雷", "电", "雨", "雪", "霜", "露", "雾", "虹", "霓",
            # 地理类
            "山", "海", "川", "河", "江", "湖", "泉", "溪", "渊", "崖",
            "峰", "岭", "谷", "洞", "岛", "洲", "泽", "野", "原", "林",
            # 五行类
            "金", "木", "水", "火", "土", "阴", "阳", "乾", "坤", "艮",
            # 仙道类
            "玄", "虚", "太", "清", "灵", "真", "元", "始", "极", "妙",
            "神", "仙", "圣", "佛", "魔", "妖", "鬼", "邪", "煞", "冥",
            # 数字类
            "一", "三", "五", "七", "九", "十", "百", "千", "万", "亿"
        ],
        # 双字（2字） - 权重30%
        2: [
            # 天象组合
            "九天", "凌霄", "太虚", "玄天", "紫霄", "青冥", "碧落", "黄泉",
            "星河", "月华", "日曜", "云海", "风雷", "霜雪", "虹霓", "霞光",
            # 地理组合
            "昆仑", "蓬莱", "方丈", "瀛洲", "岱舆", "员峤", "峨眉", "青城",
            "天山", "沧海", "长河", "大江", "五湖", "四海", "八荒", "六合",
            # 五行组合
            "太阴", "太阳", "少阴", "少阳", "玄黄", "洪荒", "混沌", "鸿蒙",
            "乾坤", "坎离", "震巽", "艮兑", "两仪", "四象", "八卦", "五行",
            # 仙道组合
            "太上", "玉清", "上清", "太清", "玄都", "紫府", "瑶池", "琼台",
            "菩提", "般若", "金刚", "罗汉", "天魔", "血煞", "幽冥", "黄泉",
            # 数字组合
            "一元", "两仪", "三才", "四象", "五行", "六合", "七星", "八卦",
            "九宫", "十方", "百炼", "千幻", "万法", "亿劫"
        ],
        # 三字（3字） - 权重40%
        3: [
            # 天象三字
            "九霄云", "凌霄殿", "太虚境", "玄天宫", "紫霄阁", "青冥峰", "碧落泉", "黄泉路",
            "星河转", "月华轮", "日曜光", "云海潮", "风雷动", "霜雪寒", "虹霓现", "霞光漫",
            # 地理三字
            "昆仑山", "蓬莱岛", "方丈洲", "瀛洲境", "岱舆峰", "员峤谷", "峨眉顶", "青城山",
            "天山雪", "沧海月", "长河落", "大江流", "五湖烟", "四海平", "八荒寂", "六合清",
            # 五行三字
            "太阴月", "太阳星", "少阴寒", "少阳暖", "玄黄气", "洪荒初", "混沌开", "鸿蒙始",
            "乾坤转", "坎离合", "震巽动", "艮兑静", "两仪生", "四象变", "八卦演", "五行轮",
            # 仙道三字
            "太上道", "玉清宫", "上清观", "太清殿", "玄都府", "紫府天", "瑶池宴", "琼台会",
            "菩提树", "般若智", "金刚身", "罗汉果", "天魔舞", "血煞阵", "幽冥界", "黄泉河",
            # 数字三字
            "一元始", "两仪分", "三才立", "四象成", "五行生", "六合聚", "七星列", "八卦演",
            "九宫变", "十方界", "百炼钢", "千幻影", "万法归", "亿劫渡"
        ],
        # 四字（4字） - 权重20%
        4: [
            "九霄云外", "太虚仙境", "玄天无极", "紫霄神宫", "青冥之上", "碧落黄泉", "星河倒悬", "月华如水",
            "日曜中天", "云海翻腾", "风雷激荡", "霜雪漫天", "虹霓贯日", "霞光万道", "昆仑之巅", "蓬莱仙岛",
            "方丈神山", "瀛洲幻境", "岱舆悬圃", "员峤仙山", "峨眉金顶", "青城洞天", "天山雪莲", "沧海月明",
            "长河落日", "大江东去", "五湖烟雨", "四海升平", "八荒六合", "洪荒宇宙", "混沌初开", "鸿蒙未判",
            "乾坤无极", "坎离既济", "震巽相薄", "艮兑相成", "两仪四象", "五行八卦", "太上忘情", "玉清圣境",
            "上清灵宝", "太清道德", "玄都紫府", "瑶池仙境", "琼台玉宇", "菩提般若", "金刚不坏", "罗汉金身",
            "天魔乱舞", "血煞冲天", "幽冥鬼域", "黄泉路上"
        ]
    }

    # 特色宗门类型（正派）
    righteous_types = {
        "剑修": ["剑", "剑阁", "剑宗", "剑派", "剑宫", "剑山", "剑域", "天剑", "神剑", "仙剑", "御剑", "飞剑", "心剑"],
        "丹修": ["丹", "丹阁", "丹宗", "丹派", "丹鼎", "丹霞", "丹元", "丹心", "灵丹", "仙丹", "神丹", "药王"],
        "器修": ["器", "器阁", "器宗", "器派", "器殿", "器魂", "器灵", "神工", "天工", "炼器", "铸剑", "百炼"],
        "符修": ["符", "符阁", "符宗", "符派", "符殿", "符箓", "符道", "天符", "神符", "灵符", "咒印", "真言"],
        "阵修": ["阵", "阵阁", "阵宗", "阵派", "阵殿", "阵法", "阵玄", "天阵", "神阵", "灵阵", "奇门", "遁甲"],
        "道修": ["道", "道观", "道宫", "道宗", "道院", "道德", "天道", "真武", "玄门", "妙法", "无为", "自然"],
        "佛修": ["佛", "佛寺", "佛院", "佛宗", "禅院", "禅林", "菩提", "金刚", "般若", "罗汉", "明王", "如来"]
    }

    # 邪魔外道类型
    evil_types = {
        "魔修": ["魔", "魔宫", "魔宗", "魔教", "魔殿", "天魔", "血魔", "心魔", "真魔", "幻魔", "阴魔", "煞魔"],
        "妖修": ["妖", "妖宫", "妖宗", "妖盟", "妖殿", "天妖", "万妖", "百妖", "真妖", "幻妖", "灵妖", "大妖"],
        "鬼修": ["鬼", "鬼门", "鬼宗", "鬼教", "鬼殿", "幽冥", "黄泉", "阴司", "夜叉", "罗刹", "无常", "判官"],
        "邪修": ["邪", "邪门", "邪宗", "邪派", "邪殿", "极乐", "合欢", "血煞", "噬魂", "夺魄", "摄心", "炼尸"]
    }

    # 王朝类名称
    dynasty_names = [
        "仙朝", "仙廷", "神朝", "天朝", "圣朝", "皇朝", "帝朝", "仙国",
        "神国", "天国", "圣国", "皇庭", "帝庭", "仙庭", "神庭", "天宫",
        "天庭", "玉京", "紫府", "瑶台", "琼楼", "金阙", "银汉", "碧城"
    ]

    # 通用后缀词库
    common_suffixes = [
        "门", "派", "宗", "宫", "殿", "阁", "轩", "楼", "观", "院",
        "堂", "居", "斋", "舍", "苑", "坊", "亭", "台", "榭", "坞",
        "谷", "山", "峰", "岛", "洞", "府", "林", "海", "渊", "崖",
        "境", "界", "天", "地", "台", "坛", "塔", "庙", "庵", "祠"
    ]

    # 邪派专用后缀
    evil_suffixes = [
        "窟", "洞", "渊", "狱", "殿", "教", "门", "派", "宗", "宫",
        "血池", "魔窟", "鬼域", "妖巢", "邪殿", "煞地", "阴间", "炼狱",
        "魔渊", "妖洞", "鬼窟", "邪巢", "血海", "骨山", "尸林", "魂冢"
    ]

    # 权重分配：基础40%，正派30%，邪派20%，王朝10%
    type_weights = [0.4, 0.3, 0.2, 0.1]
    
    # 获取已有宗门名称避免重复
    used_names = {sect['sect_name'] for sect in sql_message.get_all_sects()}
    options = []
    
    while len(options) < count:
        # 随机选择名称类型
        name_type = random.choices(["base", "righteous", "evil", "dynasty"], weights=type_weights, k=1)[0]
        
        if name_type == "base":  # 基础宗门名称
            prefix_length = random.choices([1, 2, 3, 4], weights=[0.1, 0.3, 0.4, 0.2], k=1)[0]
            prefix = random.choice(base_prefixes[prefix_length])
            suffix = random.choice(common_suffixes)
            while prefix.endswith(suffix):
                suffix = random.choice(common_suffixes)
            name = f"{prefix}{suffix}"
            
        elif name_type == "righteous":  # 正派特色宗门
            spec_type = random.choice(list(righteous_types.keys()))
            spec_suffixes = righteous_types[spec_type]
            
            if random.random() < 0.5:  # 50%单字前缀+特色后缀
                prefix = random.choice(base_prefixes[1])
                suffix = random.choice(spec_suffixes)
            else:  # 50%双字前缀+特色后缀
                prefix = random.choice(base_prefixes[2])
                suffix = random.choice(spec_suffixes[1:])  # 跳过单字特色后缀
                
            name = f"{prefix}{suffix}"
            
        elif name_type == "evil":  # 邪魔外道宗门
            spec_type = random.choice(list(evil_types.keys()))
            spec_suffixes = evil_types[spec_type]
            
            if random.random() < 0.7:  # 70%使用邪派专用后缀
                prefix = random.choice(base_prefixes[1 if random.random() < 0.5 else 2])
                suffix = random.choice(evil_suffixes)
            else:  # 30%使用特色后缀
                prefix = random.choice(base_prefixes[1 if random.random() < 0.5 else 2])
                suffix = random.choice(spec_suffixes)
                
            name = f"{prefix}{suffix}"
            
        else:  # 王朝类名称
            prefix = random.choice(base_prefixes[1 if random.random() < 0.5 else 2])
            suffix = random.choice(dynasty_names)
            name = f"{prefix}{suffix}"
        
        # 检查是否已存在
        if name not in used_names and name not in options:
            options.append(name)
    
    return options if count > 1 else options[0]
