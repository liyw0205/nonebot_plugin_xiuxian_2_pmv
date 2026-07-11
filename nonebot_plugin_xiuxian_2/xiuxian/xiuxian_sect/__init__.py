import re
import random
import time
from nonebot.typing import T_State
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, OtherSet, BuffJsonDate,
    get_main_info_msg, UserBuffDate, get_sec_msg
)
from nonebot import require
from ..on_compat import on_command
from ..messaging.delivery import delivery_service
from nonebot.log import logger
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    get_at_user_id,
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown, assign_bot_group
from nonebot.params import CommandArg
from ..xiuxian_utils.data_source import jsondata
from datetime import datetime, timedelta
from ..xiuxian_config import XiuConfig, convert_rank, JsonConfig, added_ranks
from ..xiuxian_utils.economy_log import safe_log_economy_change
from ..xiuxian_utils.game_events import safe_record_game_event
from .sectconfig import get_config, get_sect_weekly_purchases, update_sect_weekly_purchase
from .sect_tasks import sect_task_state_manager
from .sect_weekly_commands import sect_weekly, sect_weekly_claim, sect_weekly_rank
from .sect_member_utils import (
    _md_cmd_link,
    bind_sect_member_dependencies as _bind_sect_member_dependencies,
    can_join_sect,
    create_user_sect_task,
    generate_random_sect_name,
    get_mainname_list,
    get_mainnameid,
    get_secname_list,
    get_secnameid,
    get_sect_contribution_level,
    get_sect_level,
    get_sect_mainbuff_id_list,
    get_sect_member_limit,
    get_sect_secbuff_id_list,
    get_sectbufftxt,
    isUserTask,
    set_sect_list,
)
from ..xiuxian_utils.utils import (
    check_user, number_to,
    send_msg_handler, handle_send,
    update_statistics_value, send_help_message,
    parse_page_arg, paginate_text_blocks, build_pagination_buttons
)
from ..xiuxian_utils.item_json import Items
from ..xiuxian_tianti.tianti_data import TiantiDataManager
from ..xiuxian_tianti.tianti_service import (
    get_sect_fairyland_bonus,
    grant_tianti_settle_minutes,
)
from .sect_fairyland import (
    SECT_FAIRYLAND_CLAIM_TABLE,
    SECT_FAIRYLAND_CONFIG,
    SECT_FAIRYLAND_MAX_LEVEL,
    _fairyland_claim_key,
    _get_fairyland_last_claim,
    _get_sect_fairyland_config,
    _get_sect_fairyland_level,
    _set_fairyland_last_claim,
    _to_int,
)
from ..adapter_compat import is_channel_event
from ...paths import get_paths
from .membership_service import SectMembershipService

items = Items()
sql_message = XiuxianDateManage()  # sql类
sect_membership_service = SectMembershipService(get_paths().game_db)
tianti_manager = TiantiDataManager()
config = get_config()
SECT_RENAME_CARD_ID = 20026
SECT_RENAME_CARD_NAME = "宗门易名符"
LEVLECOST = config["LEVLECOST"]
added_rank = added_ranks()
cache_help = {}
userstask = {}
_bind_sect_member_dependencies(
    task_store=userstask,
    sql_manager=sql_message,
    item_manager=items,
    sect_config=config,
)

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


def _is_sect_owner(user_info: dict) -> bool:
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    return int(user_info.get("sect_position", 99)) == owner_position


def _sect_operation_id(event, action, target_id):
    event_id = str(
        getattr(event, "message_id", "") or getattr(event, "id", "") or ""
    ).strip()
    if event_id:
        return f"sect:{event_id}:{action}:{target_id}"
    return f"sect:{action}:{target_id}:{time.time_ns()}"


materialsupdate = require("nonebot_plugin_apscheduler").scheduler
upatkpractice = on_command("升级攻击修炼", priority=5, block=True)
uphppractice = on_command("升级元血修炼", priority=5, block=True)
upmppractice = on_command("升级灵海修炼", priority=5, block=True)
my_sect = on_command("我的宗门", aliases={"宗门信息"}, priority=5, block=True)
sect_buildings = on_command("宗门建设", aliases={"宗门建筑"}, priority=5, block=True)
create_sect = on_command("创建宗门", priority=5, block=True)
join_sect = on_command("加入宗门", aliases={"宗门加入"}, priority=5, block=True)
sect_position_update = on_command("宗门职位变更", priority=5, block=True)
sect_position_help = on_command("宗门职位帮助", priority=5, block=True)
sect_manage = on_command("宗门管理", aliases={"宗门管理帮助"}, priority=5, block=True)
sect_donate = on_command("宗门捐献", aliases={"宗门贡献"}, priority=5, block=True)
sect_out = on_command("退出宗门", priority=5, block=True)
sect_kick_out = on_command("踢出宗门", priority=5, block=True)
sect_owner_change = on_command("宗主传位", priority=5, block=True)
sect_list = on_command("宗门列表", priority=5, block=True)
sect_power_top = on_command("宗门战力排行榜", priority=5, block=True)
sect_help = on_command("宗门帮助", priority=5, block=True)
sect_task = on_command("宗门任务接取", aliases={"我的宗门任务", "宗门任务"}, priority=7, block=True)
sect_task_complete = on_command("宗门任务完成", priority=7, block=True)
sect_task_refresh = on_command("宗门任务刷新", priority=7, block=True)
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
sect_shop = on_command("宗门商店", priority=5, block=True)
sect_buy = on_command("宗门兑换", priority=5, block=True)
sect_fairyland_info = on_command("宗门炼体堂", aliases={"炼体堂", "宗门淬体堂"}, priority=5, block=True)
sect_fairyland_upgrade = on_command("宗门炼体堂升级", aliases={"炼体堂升级", "宗门淬体堂升级"}, priority=5, block=True)
sect_fairyland_claim = on_command("宗门淬体修行", aliases={"淬体修行", "宗门炼体堂修行", "炼体堂修行", "宗门炼体堂领取"}, priority=5, block=True)
sect_close_join = on_command("关闭宗门加入", priority=5, block=True)
sect_open_join = on_command("开放宗门加入", priority=5, block=True)
sect_close_mountain = on_command("封闭山门", priority=5, block=True)
sect_close_mountain2 = on_command("确认封闭山门", priority=5, block=True)
sect_disband = on_command("解散宗门", priority=5, block=True)
sect_disband2 = on_command("确认解散宗门", priority=5, block=True)
sect_inherit = on_command("继承宗主", priority=5, block=True)

__sect_help__ = f"""
【宗门系统】🏯

🏛️ 基础指令：
  • 我的宗门 - 查看当前宗门信息
  • 宗门列表 - 浏览全服宗门
  • 创建宗门 - 消耗{XiuConfig().sect_create_cost}灵石（需境界{XiuConfig().sect_min_level}）
  • 加入宗门 [ID/名称] - 申请加入指定宗门
  • 退出宗门 - 离开当前宗门
  • 宗门战力排行榜 - 查看战力前50的宗门
  • 宗门管理 - 查看宗主/长老管理指令

📈 宗门建设：
  • 宗门捐献 - 提升建设度（每{config["等级建设度"]}建设度提升1级修炼上限）
  • 升级攻击/元血/灵海修炼 - 提升对应属性（每级+4%攻/8%血/5%真元）
  • 宗门炼体堂 - 查看宗门炼体堂和每日淬体状态
  • 宗门炼体堂升级 - 宗主消耗宗门储备和资材提升炼体堂
  • 宗门淬体修行 - 每日领取炼体结算时间

📚 功法传承：
  • 宗门功法、神通搜寻 - 宗主可消耗资源搜索功法（100次）
  • 学习宗门功法/神通 [名称] - 成员消耗资材学习
  • 宗门功法查看 - 浏览宗门藏书
  • 宗门神通查看 - 浏览宗门神通

💊 丹房系统：
  • 建设宗门丹房 - 开启每日丹药福利
  • 领取宗门丹药 - 获取每日丹药补给

📝 宗门任务：
  • 宗门任务接取 - 获取任务（每日上限：{config["每日宗门任务次上限"]}次）
  • 宗门任务完成 - 提交任务（CD：{config["宗门任务完成cd"]}秒）
  • 宗门任务刷新 - 更换任务（CD：{config["宗门任务刷新cd"]}秒）
  • 宗门周常 - 查看全宗本周协作目标
  • 领取宗门周常 - 领取已完成的宗门周常奖励
  • 宗门周常排行 - 查看本周宗门周常进度排行
  • 宗门商店 - 查看可兑换物品
  • 宗门兑换 [物品] [数量] - 消耗贡献兑换

⏰ 福利：
  • 每日{config["发放宗门资材"]["时间"]}点发放{config["发放宗门资材"]["倍率"]}倍建设度资材
  • 职位修为加成：宗主＞长老＞亲传＞内门＞外门＞散修

💡 小贴士：
  1. 外门弟子无法获得修炼资源
  2. 建设度决定宗门整体实力
  3. 每日任务收益随职位提升
  4. 管理类操作请发送【宗门管理】查看
""".strip()

__sect_manage_help__ = """
【宗门管理】👑

👥 成员管理：
  • 宗门成员查看 [页码] - 查看成员、职位统计和快捷操作
  • 宗门职位变更 [道号] [职位编号/职位名称] - 调整成员职位
  • 宗门职位帮助 - 查看职位编号、加成和人数限制
  • 踢出宗门 [道号] - 移除宗门成员（长老及以上可用）

🏛️ 山门管理：
  • 开放宗门加入 - 允许其他修士加入宗门
  • 关闭宗门加入 - 禁止其他修士加入宗门
  • 封闭山门 - 关闭宗门并退位为长老（需确认）
  • 确认封闭山门 - 确认封闭山门
  • 继承宗主 - 封闭山门后由高职位成员继承宗主

👑 宗主专属：
  • 宗门改名 [新名称] - 消耗宗门易名符修改宗门名称
  • 宗主传位 [道号/@道友] - 禅让宗主之位
  • 解散宗门 - 解散宗门并踢出所有成员（需确认）
  • 确认解散宗门 - 确认解散宗门

🏗️ 建设管理：
  • 宗门功法搜寻 / 宗门神通搜寻 - 消耗资源搜索宗门传承
  • 宗门丹房建设 - 升级宗门丹房
  • 宗门炼体堂升级 - 升级宗门炼体堂

💡 管理规则：
  1. 长老及以上可职位变更、踢出低于自己的成员
  2. 宗主可开放/关闭加入、改名、传位、封闭或解散宗门
  3. 封闭山门后长老可以使用【继承宗主】继承宗主之位
  4. 长期不活跃的宗主会降职，长期不活跃宗门自动解散
""".strip()

# 定时任务每1小时按照宗门贡献度增加资材
@materialsupdate.scheduled_job("cron", hour=config["发放宗门资材"]["时间"])
async def materialsupdate_():
    grant_key = f"sect-materials:{datetime.now().date().isoformat()}"
    all_sects = sql_message.get_all_sects_id_scale()
    granted = 0
    for s in all_sects:
        result = sect_membership_service.grant_scheduled_materials(
            grant_key,
            s[0],
            config["发放宗门资材"]["倍率"],
        )
        granted += int(result.applied)

    logger.opt(colors=True).info(
        f"<green>已更新宗门资材和战力，本次发放 {granted} 个宗门</green>"
    )

# 重置用户宗门任务次数、宗门丹药领取次数
async def resetusertask():
    sql_message.sect_task_reset()
    sql_message.sect_elixir_get_num_reset()
    maintenance_key = f"sect-elixir-maintenance:{datetime.now().date().isoformat()}"
    maintenance_costs = {
        int(level): room_config["level_up_cost"]["建设度"]
        for level, room_config in config["宗门丹房参数"]["elixir_room_level"].items()
    }
    all_sects = sql_message.get_all_sects_id_scale()
    for s in all_sects:
        result = sect_membership_service.charge_elixir_room_maintenance(
            maintenance_key,
            s[0],
            maintenance_costs,
        )
        if result.status == "insufficient" and not result.duplicate:
            logger.opt(colors=True).info(
                f"<red>宗门：{result.sect_name}的资材无法维持丹房</red>"
            )
    logger.opt(colors=True).info(f"<green>已重置所有宗门任务次数、宗门丹药领取次数，已扣除丹房维护费</green>")

# 定时任务自动检测并处理宗门状态
async def auto_handle_inactive_sect_owners():
    logger.info("⏳ 开始检测并处理宗门状态")
    
    try:
        # 使用新的方法获取宗门列表（包含成员数量）
        all_sects = sql_message.get_all_sects_with_member_count()
        auto_change_sect_owner_cd = XiuConfig().auto_change_sect_owner_cd
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
                        
                    # 按职位优先级和贡献度排序
                    sorted_members = sorted(
                        members,
                        key=lambda x: (x['sect_position'], -x['sect_contribution'])
                    )
                    
                    # 排除当前宗主(如果有)
                    candidates = [m for m in sorted_members if m['sect_position'] != 0]
                    logger.info(f"符合条件的候选人数量：{len(candidates)}")
                    
                    # 检查候选人活跃状态：必须最近30天内有活跃
                    active_candidates = []
                    for candidate in candidates:
                        last_active = sql_message.get_last_check_info_time(candidate['user_id'])
                        if last_active and (datetime.now() - last_active).days <= auto_change_sect_owner_cd:
                            active_candidates.append(candidate)
                    
                    logger.info(f"活跃候选人数量：{len(active_candidates)}")
                    
                    if not active_candidates:
                        logger.info("没有活跃的继承人，执行解散操作")
                        sql_message.delete_sect(sect_id)
                        logger.info(f"宗门 {sect_name}(ID:{sect_id}) 已解散")
                        continue
                        
                    # 选择贡献最高的活跃候选人
                    new_owner = active_candidates[0]
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
                
                if offline_days < auto_change_sect_owner_cd:
                    logger.info("宗主活跃时间在30天内，跳过处理")
                    continue
                
                # 获取所有成员
                members = sql_message.get_all_users_by_sect_id(sect_id)
                logger.info(f"宗门成员总数：{len(members)}人")
                
                # 检查宗门成员数量
                if len(members) == 1:
                    logger.info("宗门只有宗主一人，执行解散操作")
                    sql_message.delete_sect(sect_id)
                    logger.info(f"宗门 {sect_name}(ID:{sect_id}) 已解散")
                    continue
                    
                # 获取宗主信息
                user_info = sql_message.get_user_info_with_id(owner_id)
                if not user_info:
                    logger.error(f"获取宗主信息失败：{owner_id}")
                    continue
                    
                logger.info(f"检测到不活跃宗主：{user_info['user_name']} 已离线 {offline_days} 天")
                
                # 执行降位处理（有多名成员时）
                sql_message.update_sect_join_status(sect_id, 0)  # 关闭宗门加入
                sql_message.update_sect_closed_status(sect_id, 1)  # 设置封闭状态
                sql_message.update_usr_sect(owner_id, sect_id, 2)  # 降为长老
                sql_message.update_sect_owner(None, sect_id)  # 清空宗主
                
                logger.info(f"宗门【{sect_name}】处理完成：原宗主 {user_info['user_name']} 已降为长老")
                
            except Exception as e:
                logger.error(f"处理宗门 {sect_id} 时发生错误：{str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"定时任务执行出错：{str(e)}")
    finally:
        logger.info("✅ 宗门状态检测处理完成")

@sect_help.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗门帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    page = parse_page_arg(args.extract_plain_text())
    msg, page, total_pages = paginate_text_blocks(__sect_help__, page, per_page=3)
    msg = f"{msg}\n\n翻页：宗门帮助 页码；管理：宗门管理。"
    button_kwargs = build_pagination_buttons(
        "宗门帮助",
        page,
        total_pages,
        extras=[
            ("宗门", "我的宗门"),
            ("管理", "宗门管理"),
            ("职位", "宗门职位帮助"),
            ("列表", "宗门列表"),
        ],
    )
    await send_help_message(bot, event, msg, **button_kwargs, button_id=XiuConfig().button_id2)
    await sect_help.finish()

@sect_manage.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_manage_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门管理帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot,
        event,
        __sect_manage_help__,
        k1="成员",
        v1="宗门成员查看",
        k2="职位",
        v2="宗门职位帮助",
        k3="宗门",
        v3="我的宗门",
        k4="帮助",
        v4="宗门帮助",
    )
    await sect_manage.finish()

@sect_position_help.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_position_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门职位帮助信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    msg = "【宗门职位系统】\n"
    msg += "职位编号 | 职位名称 | 职位加成 | 人数限制\n"
    msg += "─────────────\n"
    
    for pos_id, pos_data in sorted(jsondata.sect_config_data().items(), key=lambda x: int(x[0])):
        max_count = pos_data.get("max_count", 0)
        speeds = pos_data.get("speeds", 0)
        count_info = f"限{max_count}人" if max_count > 0 else "不限"
        msg += f"{pos_id:2} | {pos_data['title']} | {speeds} | {count_info}\n"
    
    msg += "\n使用示例：\n"
    msg += "• 宗门职位变更 道号 职位编号\n"
    msg += "• 宗门职位变更 道号 职位名称\n"
    msg += "• 注意：只有长老职位及以上才能变更"
    
    await send_help_message(bot, event, msg, k1="变更", v1="宗门职位变更", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
    await sect_position_help.finish()


@sect_fairyland_info.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_fairyland_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门炼体堂信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_fairyland_info.finish()

    sect_id = user_info["sect_id"]
    if not sect_id:
        await handle_send(bot, event, "道友还未加入一方宗门。", md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_fairyland_info.finish()

    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info:
        await handle_send(bot, event, "宗门信息不存在，请重新加入或创建宗门。", md_type="宗门", k1="列表", v1="宗门列表", k2="创建", v2="创建宗门", k3="帮助", v3="宗门帮助")
        await sect_fairyland_info.finish()

    level = _get_sect_fairyland_level(sect_info)
    cur_conf = _get_sect_fairyland_config(level)
    today = datetime.now().strftime("%Y-%m-%d")
    claimed = _get_fairyland_last_claim(user_info["user_id"], sect_id) == today

    next_msg = "已达最高等级"
    if level < SECT_FAIRYLAND_MAX_LEVEL:
        next_conf = _get_sect_fairyland_config(level + 1)
        next_msg = (
            f"下级：{level + 1}级【{next_conf['name']}】\n"
            f"升级消耗：宗门储备{number_to(next_conf['stone'])}灵石，宗门资材{number_to(next_conf['materials'])}"
        )

    msg = (
        f"【宗门炼体堂】\n"
        f"宗门：{sect_info['sect_name']}\n"
        f"等级：{level}级【{cur_conf['name']}】\n"
        f"炼体结算加成：{get_sect_fairyland_bonus(level) * 100:.0f}%\n"
        f"每日淬体修行：{cur_conf['minutes']}分钟炼体结算时间\n"
        f"今日领取：{'已领取' if claimed else '未领取'}\n"
        f"{next_msg}"
    )
    await handle_send(bot, event, msg, md_type="宗门", k1="修行", v1="宗门淬体修行", k2="升级", v2="宗门炼体堂升级", k3="宗门", v3="我的宗门")
    await sect_fairyland_info.finish()


@sect_fairyland_upgrade.handle(parameterless=[Cooldown(cd_time=10)])
async def sect_fairyland_upgrade_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """升级宗门炼体堂"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_fairyland_upgrade.finish()

    sect_id = user_info["sect_id"]
    if not sect_id:
        await handle_send(bot, event, "道友还未加入一方宗门。", md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_fairyland_upgrade.finish()
    if not _is_sect_owner(user_info):
        await handle_send(bot, event, "只有宗主可以升级宗门炼体堂。", md_type="宗门", k1="炼体堂", v1="宗门炼体堂", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
        await sect_fairyland_upgrade.finish()

    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info:
        await handle_send(bot, event, "宗门信息不存在，请重新加入或创建宗门。", md_type="宗门", k1="列表", v1="宗门列表", k2="创建", v2="创建宗门", k3="帮助", v3="宗门帮助")
        await sect_fairyland_upgrade.finish()

    level = _get_sect_fairyland_level(sect_info)
    if level >= SECT_FAIRYLAND_MAX_LEVEL:
        await handle_send(bot, event, "宗门炼体堂已达最高等级。", md_type="宗门", k1="炼体堂", v1="宗门炼体堂", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_fairyland_upgrade.finish()

    next_level = level + 1
    next_conf = _get_sect_fairyland_config(next_level)
    need_stone = next_conf["stone"]
    need_materials = next_conf["materials"]

    if _to_int(sect_info.get("sect_used_stone", 0)) < need_stone:
        lack = need_stone - _to_int(sect_info.get("sect_used_stone", 0))
        await handle_send(bot, event, f"宗门储备不足，还需{number_to(lack)}灵石。", md_type="宗门", k1="捐献", v1="宗门捐献", k2="炼体堂", v2="宗门炼体堂", k3="宗门", v3="我的宗门")
        await sect_fairyland_upgrade.finish()
    if _to_int(sect_info.get("sect_materials", 0)) < need_materials:
        lack = need_materials - _to_int(sect_info.get("sect_materials", 0))
        await handle_send(bot, event, f"宗门资材不足，还需{number_to(lack)}资材。", md_type="宗门", k1="捐献", v1="宗门捐献", k2="炼体堂", v2="宗门炼体堂", k3="宗门", v3="我的宗门")
        await sect_fairyland_upgrade.finish()

    result = sect_membership_service.upgrade_fairyland(
        _sect_operation_id(event, "fairyland_upgrade", sect_id),
        user_info["user_id"],
        sect_id,
        level,
        next_level,
        need_stone,
        need_materials,
    )
    if not result.applied:
        if result.status == "duplicate":
            await handle_send(bot, event, "本次炼体堂升级已经完成，请刷新宗门信息。")
        else:
            await handle_send(bot, event, "宗门资产或权限状态已经变化，请刷新后重试。")
        await sect_fairyland_upgrade.finish()
    safe_log_economy_change(
        user_id=user_info["user_id"],
        sect_id=sect_id,
        source="sect",
        action="fairyland_upgrade",
        sect_scale_delta=0,
        sect_materials_delta=-need_materials,
        detail={
            "stone_delta_in_sect_storage": -need_stone,
            "from_level": level,
            "to_level": next_level,
        },
    )

    msg = (
        f"宗门炼体堂升级成功！\n"
        f"当前等级：{next_level}级【{next_conf['name']}】\n"
        f"炼体结算加成：{get_sect_fairyland_bonus(next_level) * 100:.0f}%\n"
        f"每日淬体修行：{next_conf['minutes']}分钟炼体结算时间\n"
        f"消耗宗门储备：{number_to(need_stone)}灵石\n"
        f"消耗宗门资材：{number_to(need_materials)}"
    )
    await handle_send(bot, event, msg, md_type="宗门", k1="炼体堂", v1="宗门炼体堂", k2="修行", v2="宗门淬体修行", k3="宗门", v3="我的宗门")
    await sect_fairyland_upgrade.finish()


@sect_fairyland_claim.handle(parameterless=[Cooldown(cd_time=5)])
async def sect_fairyland_claim_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """领取宗门炼体堂修行时间"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_fairyland_claim.finish()

    sect_id = user_info["sect_id"]
    if not sect_id:
        await handle_send(bot, event, "道友还未加入一方宗门。", md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_fairyland_claim.finish()

    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info:
        await handle_send(bot, event, "宗门信息不存在，请重新加入或创建宗门。", md_type="宗门", k1="列表", v1="宗门列表", k2="创建", v2="创建宗门", k3="帮助", v3="宗门帮助")
        await sect_fairyland_claim.finish()

    level = _get_sect_fairyland_level(sect_info)
    conf = _get_sect_fairyland_config(level)
    if level <= 0 or conf["minutes"] <= 0:
        await handle_send(bot, event, "宗门尚未建设炼体堂，无法进行淬体修行。", md_type="宗门", k1="炼体堂", v1="宗门炼体堂", k2="捐献", v2="宗门捐献", k3="宗门", v3="我的宗门")
        await sect_fairyland_claim.finish()

    today = datetime.now().strftime("%Y-%m-%d")
    if _get_fairyland_last_claim(user_info["user_id"], sect_id) == today:
        await handle_send(bot, event, "今日已经完成过宗门淬体修行。", md_type="宗门", k1="炼体堂", v1="宗门炼体堂", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_fairyland_claim.finish()

    data = tianti_manager.get_user_tianti_info(str(user_info["user_id"]))
    result = grant_tianti_settle_minutes(data, conf["minutes"], sect_fairyland_level=level)
    tianti_manager.save_user_tianti_info(str(user_info["user_id"]), data)
    _set_fairyland_last_claim(user_info["user_id"], sect_id, today)

    msg = (
        f"宗门淬体修行完成！\n"
        f"炼体堂：{level}级【{conf['name']}】\n"
        f"获得炼体结算时间：{conf['minutes']}分钟\n"
        f"宗门炼体堂加成：{float(result.get('sect_bonus', 0) or 0) * 100:.0f}%\n"
        f"本次获得炼体气血：{number_to(result['real_gain'])}\n"
        f"当前炼体气血：{number_to(result['new_hp'])}"
    )
    await handle_send(bot, event, msg, md_type="宗门", k1="炼体堂", v1="宗门炼体堂", k2="炼体", v2="我的炼体", k3="宗门", v3="我的宗门")
    await sect_fairyland_claim.finish()


@sect_elixir_room_make.handle(parameterless=[Cooldown(stamina_cost=2)])
async def sect_elixir_room_make_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门丹房建设"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
                await sect_elixir_room_make.finish()
            to_up_level = int(elixir_room_level) + 1
            elixir_room_level_up_sect_scale_cost = elixir_room_level_up_config[str(to_up_level)]['level_up_cost']['建设度']
            elixir_room_level_up_use_stone_cost = elixir_room_level_up_config[str(to_up_level)]['level_up_cost'][
                'stone']
            if elixir_room_level_up_use_stone_cost > int(sect_info['sect_used_stone']):
                msg = f"宗门可用灵石不满足升级条件，当前升级需要消耗宗门灵石：{elixir_room_level_up_use_stone_cost}枚！"
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_elixir_room_make.finish()
            elif elixir_room_level_up_sect_scale_cost > int(sect_info['sect_scale']):
                msg = f"宗门建设度不满足升级条件，当前升级需要消耗宗门建设度：{elixir_room_level_up_sect_scale_cost}点！"
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_elixir_room_make.finish()
            else:
                result = sect_membership_service.upgrade_elixir_room(
                    _sect_operation_id(event, "elixir_room_upgrade", sect_id),
                    user_info["user_id"],
                    sect_id,
                    int(elixir_room_level),
                    to_up_level,
                    elixir_room_level_up_use_stone_cost,
                    elixir_room_level_up_sect_scale_cost,
                    owner_position=owner_position,
                )
                if not result.applied:
                    if result.status == "duplicate":
                        await handle_send(bot, event, "本次宗门丹房升级已经完成，请刷新宗门信息。")
                    else:
                        await handle_send(bot, event, "宗门资产或权限状态已经变化，请刷新后重试。")
                    await sect_elixir_room_make.finish()
                safe_log_economy_change(
                    user_id=user_info["user_id"],
                    sect_id=sect_id,
                    source="sect",
                    action="elixir_room_upgrade",
                    sect_scale_delta=-elixir_room_level_up_sect_scale_cost,
                    detail={
                        "stone_delta_in_sect_storage": -elixir_room_level_up_use_stone_cost,
                        "from_level": int(elixir_room_level),
                        "to_level": to_up_level,
                    },
                )
                msg = f"宗门消耗：{elixir_room_level_up_sect_scale_cost}建设度，{elixir_room_level_up_use_stone_cost}宗门灵石\n"
                msg += f"成功升级宗门丹房，当前丹房为：{elixir_room_level_up_config[str(to_up_level)]['name']}!"
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
                await sect_elixir_room_make.finish()
        else:
            msg = f"道友不是宗主，无法使用该命令！"
            await handle_send(bot, event, msg)
            await sect_elixir_room_make.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_elixir_room_make.finish()


@sect_elixir_get.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_elixir_get_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门丹药领取"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_elixir_get.finish()

    sect_id = user_info['sect_id']
    user_id = user_info['user_id']
    sql_message.update_last_check_info_time(user_id) # 更新查看修仙信息时间
    if sect_id:
        sect_position = user_info['sect_position']
        elixir_room_config = config['宗门丹房参数']
        if sect_position == 15:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]['title']}，不满足领取要求!"""
            await handle_send(bot, event, msg)
            await sect_elixir_get.finish()
        else:
            sect_info = sql_message.get_sect_info(sect_id)
            if int(sect_info['elixir_room_level']) == 0:
                msg = f"道友的宗门目前还未建设丹房！"
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_elixir_get.finish()
            if int(user_info['sect_contribution']) < elixir_room_config['领取贡献度要求']:
                msg = f"道友的宗门贡献度不满足领取条件，当前宗门贡献度要求：{elixir_room_config['领取贡献度要求']}点！"
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_elixir_get.finish()
            elixir_room_level_up_config = elixir_room_config['elixir_room_level']
            elixir_room_cost = elixir_room_level_up_config[str(sect_info['elixir_room_level'])]['level_up_cost']['建设度']
            if sect_info['sect_materials'] < elixir_room_cost:
                msg = f"当前宗门资材无法维护丹房，请等待{config['发放宗门资材']['时间']}点发放宗门资材后尝试领取！"
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_elixir_get.finish()
            if int(user_info['sect_elixir_get']) == 1:
                msg = f"道友已经领取过了，不要贪心哦~"
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_elixir_get.finish()
            if int(sect_info['elixir_room_level']) == 1:
                msg = f"道友成功领取到丹药:渡厄丹！"
                sql_message.send_back(user_info['user_id'], 1999, "渡厄丹", "丹药", 1, 1)  # 1级丹房送1个渡厄丹
                sql_message.update_user_sect_elixir_get_num(user_info['user_id'])
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_elixir_get.finish()
            else:
                sect_now_room_config = elixir_room_level_up_config[str(sect_info['elixir_room_level'])]
                give_num = sect_now_room_config['give_level']['give_num'] - 1
                rank_up = sect_now_room_config['give_level']['rank_up']
                give_dict = {}
                give_elixir_id_list = items.get_random_id_list_by_rank_and_item_type(
                    fanil_rank=max(convert_rank(user_info['level'])[0] - rank_up - added_rank, 16), item_type=['丹药'])
                if not give_elixir_id_list:  # 没有合适的ID，全部给渡厄丹
                    msg = f"道友成功领取到丹药：渡厄丹 2 枚！"
                    sql_message.send_back(user_info['user_id'], 1999, "渡厄丹", "丹药", 2, 1)  # 送1个渡厄丹
                    sql_message.update_user_sect_elixir_get_num(user_info['user_id'])
                    await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
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
                        except Exception:
                            give_dict[id] = 1
                            i += 1
                msg = f"道友成功领取到丹药:渡厄丹 1 枚!\n"
                sql_message.send_back(user_info['user_id'], 1999, "渡厄丹", "丹药", 1, 1)  # 送1个渡厄丹
                for k, v in give_dict.items():
                    goods_info = items.get_data_by_item_id(k)
                    msg += f"道友成功领取到丹药：{goods_info['name']} {v} 枚!\n"
                    sql_message.send_back(user_info['user_id'], k, goods_info['name'], '丹药', v, bind_flag=1)
                sql_message.update_user_sect_elixir_get_num(user_info['user_id'])
                await handle_send(bot, event, msg, md_type="宗门", k1="领取丹药", v1="宗门丹药领取", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_elixir_get.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_elixir_get.finish()


@sect_buff_info.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_buff_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门功法查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_buff_info.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_buff_info.finish()
        
    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info['mainbuff']:
        msg = f"本宗尚未获得任何功法，请宗主发送【宗门功法搜寻】来获取！"
        await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门功法搜寻", k2="查看", v2="宗门功法查看", k3="捐献", v3="宗门捐献")
        await sect_buff_info.finish()

    # 获取功法列表
    mainbuff_list = get_sect_mainbuff_id_list(sect_id)
    if not mainbuff_list:
        msg = f"本宗功法列表为空！"
        await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门功法搜寻", k2="查看", v2="宗门功法查看", k3="捐献", v3="宗门捐献")
        await sect_buff_info.finish()

    # 按品阶排序
    sorted_mainbuff_list = sorted(mainbuff_list, key=lambda x: buffrankkey.get(items.get_data_by_item_id(x)['level'], 999))

    # 构建消息
    msg_list = []
    title = "【宗门功法】"
    
    for mainbuff_id in sorted_mainbuff_list:
        if not mainbuff_id:  # 跳过空ID
            continue
        mainbuff, mainbuffmsg = get_main_info_msg(mainbuff_id)
        msg_list.append(f"{mainbuff['level']}{mainbuff['name']}")

    # 发送消息
    page = ["搜寻", f"宗门功法搜寻", "查看", "宗门功法查看", "学习", "宗门功法学习", "宗门功法"]    
    await send_msg_handler(bot, event, '宗门功法', bot.self_id, msg_list, title=title, page=page)
    
    await sect_buff_info.finish()

@sect_buff_info2.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_buff_info2_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门神通查看"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_buff_info2.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_buff_info2.finish()
        
    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info['secbuff']:
        msg = f"本宗尚未获得任何神通，请宗主发送【宗门神通搜寻】来获取！"
        await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门神通搜寻", k2="查看", v2="宗门神通查看", k3="捐献", v3="宗门捐献")
        await sect_buff_info2.finish()

    # 获取神通列表
    secbuff_list = get_sect_secbuff_id_list(sect_id)
    if not secbuff_list:
        msg = f"本宗神通列表为空！"
        await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门神通搜寻", k2="查看", v2="宗门神通查看", k3="捐献", v3="宗门捐献")
        await sect_buff_info2.finish()

    # 按品阶排序
    sorted_secbuff_list = sorted(secbuff_list, key=lambda x: buffrankkey.get(items.get_data_by_item_id(x)['level'], 999))

    # 构建消息
    msg_list = []
    title = "【宗门神通】"
    
    for secbuff_id in sorted_secbuff_list:
        if not secbuff_id:  # 跳过空ID
            continue
        secbuff = items.get_data_by_item_id(secbuff_id)
        secbuffmsg = get_sec_msg(secbuff)
        msg_list.append(f"{secbuff['level']}:{secbuff['name']}")

    # 发送消息
    page = ["搜寻", f"宗门速通搜寻", "查看", "宗门速通查看", "学习", "宗门速通学习", "宗门速通"]    
    await send_msg_handler(bot, event, '宗门速通', bot.self_id, msg_list, title=title, page=page)
    
    await sect_buff_info2.finish()
        
@sect_mainbuff_learn.handle(parameterless=[Cooldown(stamina_cost = 1, cd_time=10)])
async def sect_mainbuff_learn_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """学习宗门功法"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_mainbuff_learn.finish()
    msg = args.extract_plain_text().strip()
    sect_id = user_info['sect_id']
    user_id = user_info['user_id']
    if sect_id:
        sect_position = user_info['sect_position']
        if sect_position in [12, 14, 15]:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}，不满足学习要求!"""
            await handle_send(bot, event, msg, md_type="宗门", k1="学习", v1="宗门功法学习", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await sect_mainbuff_learn.finish()
        else:
            sect_info = sql_message.get_sect_info(sect_id)
            if sect_info['mainbuff'] == 0:
                msg = f"本宗尚未获得宗门功法，请宗主发送宗门功法搜寻来获得宗门功法！"
                await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门功法搜寻", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_mainbuff_learn.finish()

            sectmainbuffidlist = get_sect_mainbuff_id_list(sect_id)

            if msg not in get_mainname_list(sectmainbuffidlist):
                msg = f"本宗还没有该功法，请发送本宗有的功法进行学习！"
                await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门功法搜寻", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_mainbuff_learn.finish()

            userbuffinfo = UserBuffDate(user_info['user_id']).BuffInfo
            mainbuffid = get_mainnameid(msg, sectmainbuffidlist)
            if str(userbuffinfo['main_buff']) == str(mainbuffid):
                msg = f"道友请勿重复学习！"
                await handle_send(bot, event, msg, md_type="宗门", k1="学习", v1="宗门功法学习", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
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
                msg = f"本次学习消耗{number_to(materialscost)}宗门资材，成功学习到本宗{mainbufftype}功法：{mainbuff['name']}\n{mainbuffmsg}"
                await handle_send(bot, event, msg, md_type="宗门", k1="学习", v1="宗门功法学习", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_mainbuff_learn.finish()
            else:
                msg = f"本次学习需要消耗{number_to(materialscost)}宗门资材，不满足条件！"
                await handle_send(bot, event, msg, md_type="宗门", k1="学习", v1="宗门功法学习", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_mainbuff_learn.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_mainbuff_learn.finish()


@sect_mainbuff_get.handle(parameterless=[Cooldown(stamina_cost=8)])
async def sect_mainbuff_get_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """搜寻宗门功法（可获取当前及以下所有品阶功法）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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

                sql = set_sect_list(mainbuffidlist)
                result = sect_membership_service.apply_buff_search(
                    _sect_operation_id(event, "mainbuff_search", sect_id),
                    user_info["user_id"],
                    sect_id,
                    "main",
                    sect_info["mainbuff"],
                    sql,
                    total_stone_cost,
                    total_materials_cost,
                    owner_position=owner_position,
                )
                if not result.applied:
                    if result.status == "duplicate":
                        await handle_send(bot, event, "本次宗门功法搜寻已经完成，请刷新宗门信息。")
                    else:
                        await handle_send(bot, event, "宗门资产、权限或功法列表已经变化，请刷新后重试。")
                    await sect_mainbuff_get.finish()

                # 构建结果消息
                msg = f"共消耗{total_stone_cost}宗门灵石，{total_materials_cost}宗门资材。\n"
                msg += f"失败{fail_count}次，获取重复功法{repeat_count}次"
                if success_count > 0:
                    msg += f"，搜寻到新功法{success_count}次。\n"
                else:
                    msg += f"，未搜寻到新功法！\n"
                msg += f"\n".join(results)

                await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门功法搜寻", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_mainbuff_get.finish()
            else:
                msg = f"需要消耗{total_stone_cost}宗门灵石，{total_materials_cost}宗门资材，不满足条件！"
                await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门功法搜寻", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_mainbuff_get.finish()
        else:
            msg = f"道友不是宗主，无法使用该命令！"
            await handle_send(bot, event, msg)
            await sect_mainbuff_get.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_mainbuff_get.finish()

@sect_secbuff_get.handle(parameterless=[Cooldown(stamina_cost=8)])
async def sect_secbuff_get_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """搜寻宗门神通（可获取当前及以下所有品阶神通）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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

                sql = set_sect_list(secbuffidlist)
                result = sect_membership_service.apply_buff_search(
                    _sect_operation_id(event, "secbuff_search", sect_id),
                    user_info["user_id"],
                    sect_id,
                    "secondary",
                    sect_info["secbuff"],
                    sql,
                    total_stone_cost,
                    total_materials_cost,
                    owner_position=owner_position,
                )
                if not result.applied:
                    if result.status == "duplicate":
                        await handle_send(bot, event, "本次宗门神通搜寻已经完成，请刷新宗门信息。")
                    else:
                        await handle_send(bot, event, "宗门资产、权限或神通列表已经变化，请刷新后重试。")
                    await sect_secbuff_get.finish()

                # 构建结果消息
                msg = f"共消耗{total_stone_cost}宗门灵石，{total_materials_cost}宗门资材。\n"
                msg += f"失败{fail_count}次，获取重复神通{repeat_count}次"
                if success_count > 0:
                    msg += f"，搜寻到新神通{success_count}次。\n"
                else:
                    msg += f"，未搜寻到新神通！\n"
                msg += f"\n".join(results)

                await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门神通搜寻", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_secbuff_get.finish()
            else:
                msg = f"需要消耗{total_stone_cost}宗门灵石，{total_materials_cost}宗门资材，不满足条件！"
                await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门神通搜寻", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_secbuff_get.finish()
        else:
            msg = f"道友不是宗主，无法使用该命令！"
            await handle_send(bot, event, msg)
            await sect_secbuff_get.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_secbuff_get.finish()
        
@sect_secbuff_learn.handle(parameterless=[Cooldown(stamina_cost=1, cd_time=10)])
async def sect_secbuff_learn_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """学习宗门神通"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_secbuff_learn.finish()
    msg = args.extract_plain_text().strip()
    sect_id = user_info['sect_id']
    user_id = user_info['user_id']
    if sect_id:
        sect_position = user_info['sect_position']
        if sect_position in [12, 14, 15]:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]['title']}，不满足学习要求!"""
            await handle_send(bot, event, msg, md_type="宗门", k1="学习", v1="宗门神通学习", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await sect_secbuff_learn.finish()
        else:
            sect_info = sql_message.get_sect_info(sect_id)
            if sect_info['secbuff'] == 0:
                msg = f"本宗尚未获得宗门神通，请宗主发送宗门神通搜寻来获得宗门神通！"
                await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门神通搜寻", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_secbuff_learn.finish()

            sectsecbuffidlist = get_sect_secbuff_id_list(sect_id)

            if msg not in get_secname_list(sectsecbuffidlist):
                msg = f"本宗还没有该神通，请发送本宗有的神通进行学习！"

                await handle_send(bot, event, msg, md_type="宗门", k1="搜寻", v1="宗门神通搜寻", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_secbuff_learn.finish()

            userbuffinfo = UserBuffDate(user_info['user_id']).BuffInfo
            secbuffid = get_secnameid(msg, sectsecbuffidlist)
            if str(userbuffinfo['sec_buff']) == str(secbuffid):
                msg = f"道友请勿重复学习！"
                await handle_send(bot, event, msg, md_type="宗门", k1="学习", v1="宗门神通学习", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
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
                msg = f"本次学习消耗{number_to(materialscost)}宗门资材，成功学习到本宗{secbufftype}神通：{secbuff['name']}\n{secbuff['name']}：{secmsg}"
                await handle_send(bot, event, msg, md_type="宗门", k1="学习", v1="宗门神通学习", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_secbuff_learn.finish()
            else:
                msg = f"本次学习需要消耗{number_to(materialscost)}宗门资材，不满足条件！"
                await handle_send(bot, event, msg, md_type="宗门", k1="学习", v1="宗门神通学习", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
                await sect_secbuff_learn.finish()
    else:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_secbuff_learn.finish()


@upatkpractice.handle(parameterless=[Cooldown(cd_time=10)])
async def upatkpractice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """升级攻击修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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
            await handle_send(bot, event, msg, md_type="宗门", k1="状态", v1="我的状态", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await upatkpractice.finish()

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限100级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - useratkpractice)
        if sect_position in [12, 14, 15]:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}，不满足使用资材的条件!"""
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级攻击修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upatkpractice.finish()
        elif sect_position == 11 or sect_position == 13:
            sect_contribution_level = get_sect_contribution_level(int(user_info['sect_contribution']))[0]
        else:
            sect_contribution_level = get_sect_contribution_level(int(user_info['sect_contribution'] * 5))[0]

        if useratkpractice >= sect_level:
            msg = f"道友的攻击修炼等级已达到当前宗门修炼等级的最高等级：{sect_level}，请继续捐献灵石提升宗门建设度吧！"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级攻击修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upatkpractice.finish()

        if useratkpractice + level_up_count > sect_contribution_level:
            msg = f"道友的贡献度修炼等级：{sect_contribution_level}，请继续捐献灵石提升贡献度吧！"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级攻击修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upatkpractice.finish()

        total_stone_cost = sum(LEVLECOST[str(useratkpractice + i)] for i in range(level_up_count))
        total_materials_cost = int(total_stone_cost * 10)

        if int(user_info['stone']) < total_stone_cost:
            msg = f"道友的灵石不够，升级到攻击修炼等级 {useratkpractice + level_up_count} 还需 {total_stone_cost - int(user_info['stone'])} 灵石!"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级攻击修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upatkpractice.finish()

        if sect_materials < total_materials_cost:
            msg = f"道友的所处的宗门资材不足，还需 {total_materials_cost - sect_materials} 资材来升级到攻击修炼等级 {useratkpractice + level_up_count}!"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级攻击修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upatkpractice.finish()

        result = sect_membership_service.upgrade_practice(
            _sect_operation_id(event, "attack_practice_upgrade", sect_id),
            user_id,
            sect_id,
            "attack",
            useratkpractice,
            useratkpractice + level_up_count,
            total_stone_cost,
            total_materials_cost,
        )
        if not result.applied:
            if result.status == "duplicate":
                await handle_send(bot, event, "本次攻击修炼升级已经完成，请刷新状态。")
            else:
                await handle_send(bot, event, "个人或宗门资产状态已经变化，请刷新后重试。")
            await upatkpractice.finish()
        msg = f"升级成功！\n道友当前攻击修炼等级：{useratkpractice + level_up_count}\n消耗灵石：{number_to(total_stone_cost)}枚\n消耗宗门资材{number_to(total_materials_cost)}"
        await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级攻击修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
        await upatkpractice.finish()
    else:
        msg = f"修炼逆天而行消耗巨大，请加入宗门再进行修炼！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await upatkpractice.finish()

@uphppractice.handle(parameterless=[Cooldown(cd_time=10)])
async def uphppractice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """升级元血修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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
            await handle_send(bot, event, msg, md_type="宗门", k1="状态", v1="我的状态", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await uphppractice.finish()

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限100级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - userhppractice)
        if sect_position in [12, 14, 15]:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}，不满足使用资材的条件!"""
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级元血修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await uphppractice.finish()
        elif sect_position == 11 or sect_position == 13:
            sect_contribution_level = get_sect_contribution_level(int(user_info['sect_contribution']))[0]
        else:
            sect_contribution_level = get_sect_contribution_level(int(user_info['sect_contribution'] * 5))[0]

        if userhppractice >= sect_level:
            msg = f"道友的元血修炼等级已达到当前宗门修炼等级的最高等级：{sect_level}，请捐献灵石提升贡献度吧！"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级元血修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await uphppractice.finish()

        if userhppractice + level_up_count > sect_contribution_level:
            msg = f"道友的贡献度修炼等级：{sect_contribution_level}，请继续捐献灵石提升贡献度吧！"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级元血修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await uphppractice.finish()

        total_stone_cost = sum(LEVLECOST[str(userhppractice + i)] for i in range(level_up_count))
        total_materials_cost = int(total_stone_cost * 10)

        if int(user_info['stone']) < total_stone_cost:
            msg = f"道友的灵石不够，升级到元血修炼等级 {userhppractice + level_up_count} 还需 {total_stone_cost - int(user_info['stone'])} 灵石!"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级元血修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await uphppractice.finish()

        if sect_materials < total_materials_cost:
            msg = f"道友的所处的宗门资材不足，还需 {total_materials_cost - sect_materials} 资材来升级到元血修炼等级 {userhppractice + level_up_count}!"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级元血修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await uphppractice.finish()

        result = sect_membership_service.upgrade_practice(
            _sect_operation_id(event, "health_practice_upgrade", sect_id),
            user_id,
            sect_id,
            "health",
            userhppractice,
            userhppractice + level_up_count,
            total_stone_cost,
            total_materials_cost,
        )
        if not result.applied:
            if result.status == "duplicate":
                await handle_send(bot, event, "本次元血修炼升级已经完成，请刷新状态。")
            else:
                await handle_send(bot, event, "个人或宗门资产状态已经变化，请刷新后重试。")
            await uphppractice.finish()
        msg = f"升级成功！\n道友当前元血修炼等级：{userhppractice + level_up_count}\n消耗灵石：{number_to(total_stone_cost)}枚\n消耗宗门资材{number_to(total_materials_cost)}"
        await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级元血修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
        await uphppractice.finish()
    else:
        msg = f"修炼逆天而行消耗巨大，请加入宗门再进行修炼！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await uphppractice.finish()
        
@upmppractice.handle(parameterless=[Cooldown(cd_time=10)])
async def upmppractice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """升级灵海修炼"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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
            await handle_send(bot, event, msg, md_type="宗门", k1="状态", v1="我的状态", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await upmppractice.finish()

        sect_level = get_sect_level(sect_id)[0] if get_sect_level(sect_id)[0] <= 100 else 100  # 获取当前宗门修炼等级上限，500w建设度1级,上限100级

        sect_position = user_info['sect_position']
        # 确保用户不会尝试升级超过宗门等级的上限
        level_up_count = min(level_up_count, sect_level - usermppractice)
        if sect_position in [12, 14, 15]:
            msg = f"""道友所在宗门的职位为：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}，不满足使用资材的条件!"""
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级灵海修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upmppractice.finish()
        elif sect_position == 11 or sect_position == 13:
            sect_contribution_level = get_sect_contribution_level(int(user_info['sect_contribution']))[0]
        else:
            sect_contribution_level = get_sect_contribution_level(int(user_info['sect_contribution'] * 5))[0]

        if usermppractice >= sect_level:
            msg = f"道友的灵海修炼等级已达到当前宗门修炼等级的最高等级：{sect_level}，请捐献灵石提升贡献度吧！"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级灵海修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upmppractice.finish()

        if usermppractice + level_up_count > sect_contribution_level:
            msg = f"道友的贡献度修炼等级：{sect_contribution_level}，请继续捐献灵石提升贡献度吧！"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级灵海修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upmppractice.finish()

        total_stone_cost = sum(LEVLECOST[str(usermppractice + i)] for i in range(level_up_count))
        total_materials_cost = int(total_stone_cost * 10)

        if int(user_info['stone']) < total_stone_cost:
            msg = f"道友的灵石不够，升级到灵海修炼等级 {usermppractice + level_up_count} 还需 {total_stone_cost - int(user_info['stone'])} 灵石!"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级灵海修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upmppractice.finish()

        if sect_materials < total_materials_cost:
            msg = f"道友的所处的宗门资材不足，还需 {total_materials_cost - sect_materials} 资材来升级到灵海修炼等级 {usermppractice + level_up_count}!"
            await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级灵海修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
            await upmppractice.finish()

        result = sect_membership_service.upgrade_practice(
            _sect_operation_id(event, "mana_practice_upgrade", sect_id),
            user_id,
            sect_id,
            "mana",
            usermppractice,
            usermppractice + level_up_count,
            total_stone_cost,
            total_materials_cost,
        )
        if not result.applied:
            if result.status == "duplicate":
                await handle_send(bot, event, "本次灵海修炼升级已经完成，请刷新状态。")
            else:
                await handle_send(bot, event, "个人或宗门资产状态已经变化，请刷新后重试。")
            await upmppractice.finish()
        msg = f"升级成功！\n道友当前灵海修炼等级：{usermppractice + level_up_count}\n消耗灵石：{number_to(total_stone_cost)}枚\n消耗宗门资材{number_to(total_materials_cost)}"
        await handle_send(bot, event, msg, md_type="宗门", k1="升级", v1="升级灵海修炼", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
        await upmppractice.finish()
    else:
        msg = f"修炼逆天而行消耗巨大，请加入宗门再进行修炼！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await upmppractice.finish()
        
        
@sect_task_refresh.handle(parameterless=[Cooldown(cd_time=config['宗门任务刷新cd'])])
async def sect_task_refresh_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """刷新宗门任务"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_task_refresh.finish()
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    if sect_id:
        if isUserTask(user_id):
            create_user_sect_task(user_id, sect_id)
            if userstask[user_id]['任务内容']['type'] == 1:
                task_type = "⚔️"
            else:
                task_type = "💰"
            msg = f"已刷新，道友当前接取的任务：{task_type} {userstask[user_id]['任务名称']}\n{userstask[user_id]['任务内容']['desc']}"
            await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
            await sect_task_refresh.finish()
        else:
            msg = f"道友目前还没有宗门任务，请发送指令宗门任务接取来获取吧"
            await handle_send(bot, event, msg, md_type="宗门", k1="接取", v1="宗门任务接取", k2="完成", v2="宗门任务完成", k3="刷新", v3="宗门任务刷新")
            await sect_task_refresh.finish()

    else:
        msg = f"道友尚未加入宗门，请加入宗门后再发送该指令！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_task_refresh.finish()


@sect_list.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_list_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门列表：显示宗门人数信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    sect_lists_with_members = sql_message.get_all_sects_with_member_count()

    msg_list = []
    for sect in sect_lists_with_members:
        sect_id, sect_name, sect_scale, user_name, member_count = sect
        if user_name is None:
            user_name = "暂无"
        
        can_join, reason = can_join_sect(sect_id)
        
        msg_list.append(f"编号{sect_id}：{sect_name}\n宗主：{user_name}\n宗门状态：{reason}\n建设度：{number_to(sect_scale)}\n")

    await send_msg_handler(bot, event, '宗门列表', bot.self_id, msg_list)
    await sect_list.finish()

@sect_users.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_users_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看所在宗门成员信息（第一页显示职位人数统计，支持原生MD快捷键）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_users.finish()

    # 获取页码，默认为1
    try:
        current_page = int(args.extract_plain_text().strip())
    except Exception:
        current_page = 1

    if user_info:
        sect_id = user_info['sect_id']
        if sect_id:
            sect_info = sql_message.get_sect_info(sect_id)
            userlist = sql_message.get_all_users_by_sect_id(sect_id)

            if not userlist:
                msg = "宗门目前没有成员！"
                await handle_send(bot, event, msg)
                await sect_users.finish()

            # 按职位排序（数字越小，权限越高）
            sorted_users = sorted(userlist, key=lambda x: x['sect_position'])

            # 获取长老职位编号
            position_zhanglao = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "长老"]
            elder_position = int(position_zhanglao[0]) if len(position_zhanglao) == 1 else 2

            # 当前操作者职位
            actor_pos = int(user_info['sect_position'])

            # 操作者是否达到长老及以上
            actor_is_elder_or_above = actor_pos <= elder_position

            # 判断是否可对目标显示“职位/踢出”快捷键
            # 规则：操作者需为长老及以上，且目标职位必须低于自己（编号更大）
            def can_manage_target(target_pos: int) -> bool:
                if not actor_is_elder_or_above:
                    return False
                return int(target_pos) > actor_pos

            # 构建标题（文本模式）
            title = [f"☆【{sect_info['sect_name']}】的成员信息☆"]

            # 第一页显示职位统计
            if current_page == 1:
                position_count = {}
                for u in sorted_users:
                    p = u['sect_position']
                    position_count[p] = position_count.get(p, 0) + 1

                title.append("☆------宗门职位统计------☆")
                for pos_id in sorted(position_count.keys()):
                    pos_data = jsondata.sect_config_data().get(str(pos_id), {})
                    pos_title = pos_data.get("title", f"未知职位{pos_id}")
                    max_count = pos_data.get("max_count", 0)
                    count_info = f"{position_count[pos_id]}/{max_count}" if max_count > 0 else f"{position_count[pos_id]}"
                    title.append(f"{pos_title}：{count_info}")

            title = "\n".join(title)

            # 分页
            page_size = 10
            total_members = len(sorted_users)
            total_pages = (total_members + page_size - 1) // page_size
            if total_pages <= 0:
                total_pages = 1

            # 页码修正
            if current_page < 1:
                current_page = 1
            if current_page > total_pages:
                current_page = total_pages

            start_idx = (current_page - 1) * page_size
            end_idx = start_idx + page_size
            current_msgs = sorted_users[start_idx:end_idx]

            # ===== 原生MD模式 =====
            if XiuConfig().markdown_status and not is_channel_event(event):
                md_lines = []
                md_lines.append(f"**【{sect_info['sect_name']}】成员信息**")
                md_lines.append(f"> 第 {current_page}/{total_pages} 页")

                if current_page == 1:
                    md_lines.append("")
                    md_lines.append("**宗门职位统计**")
                    position_count = {}
                    for u in sorted_users:
                        p = u['sect_position']
                        position_count[p] = position_count.get(p, 0) + 1

                    for pos_id in sorted(position_count.keys()):
                        pos_data = jsondata.sect_config_data().get(str(pos_id), {})
                        pos_title = pos_data.get("title", f"未知职位{pos_id}")
                        max_count = pos_data.get("max_count", 0)
                        count_info = f"{position_count[pos_id]}/{max_count}" if max_count > 0 else f"{position_count[pos_id]}"
                        md_lines.append(f"- {pos_title}：{count_info}")

                md_lines.append("")
                md_lines.append("**成员列表**")
                for idx, u in enumerate(current_msgs, start_idx + 1):
                    uname = u['user_name']
                    upos_num = int(u['sect_position'])
                    upos = jsondata.sect_config_data()[str(upos_num)]['title']
                    ulevel = u['level']
                    ucon = number_to(u['sect_contribution'])

                    if can_manage_target(upos_num):
                        pos_link = _md_cmd_link("职位", f"宗门职位变更 {uname} ")
                        kick_link = _md_cmd_link("踢出", f"踢出宗门 {uname}")
                        quick_ops = f"{pos_link} | {kick_link}"
                    else:
                        quick_ops = ""

                    md_lines.append(
                        f"{idx}. **{uname}**  {quick_ops}\n"
                        f"> 职位：{upos}｜境界：{ulevel}｜贡献：{ucon}"
                    )

                if current_page < total_pages:
                    next_link = _md_cmd_link("下一页", f"宗门成员查看 {current_page + 1}")
                    md_lines.append(f"\n{next_link}")
                else:
                    md_lines.append("\n已是最后一页")

                md_msg = MessageSegment.markdown(bot, "\n".join(md_lines), button_id="")
                await delivery_service.reply(bot, event, md_msg)
                await sect_users.finish()

            # ===== 文本模式（回退）=====
            msg_list = []
            for idx, u in enumerate(current_msgs, start_idx + 1):
                upos_num = int(u['sect_position'])
                upos = jsondata.sect_config_data()[str(upos_num)]['title']
                one = f"编号:{idx}\n道号:{u['user_name']}\n境界:{u['level']}\n"
                one += f"宗门职位:{upos}\n"
                one += f"宗门贡献度:{number_to(u['sect_contribution'])}\n"
                if can_manage_target(upos_num):
                    one += f"快捷操作: 宗门职位变更 {u['user_name']} [职位编号] / 踢出宗门 {u['user_name']}\n"
                else:
                    one += f"快捷操作: 不可操作\n"
                msg_list.append(one)

            footer = f"发送'宗门成员查看 页码'查看其他页（共{total_pages}页）"
            msg_list.append(footer)

            # 底部按钮：只有长老及以上才给管理入口
            if actor_is_elder_or_above:
                page = ["翻页", f"宗门成员查看 {current_page + 1}", "变更", "宗门职位变更", "踢出", "踢出宗门", f"{current_page}/{total_pages}"]
            else:
                page = ["翻页", f"宗门成员查看 {current_page + 1}", "宗门", "我的宗门", "帮助", "宗门帮助", f"{current_page}/{total_pages}"]

            await send_msg_handler(bot, event, '宗门成员', bot.self_id, msg_list, title=title, page=page)
        else:
            msg = "一介散修，莫要再问。"
            await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
    else:
        msg = "未曾踏入修仙世界，输入【我要修仙】加入我们，看破这世间虚妄!"
        await handle_send(bot, event, msg, md_type="我要修仙")

    await sect_users.finish()

@sect_task.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_task_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """获取宗门任务"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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
            if userstask[user_id]['任务内容']['type'] == 1:
                task_type = "⚔️"
            else:
                task_type = "💰"
            msg = f"道友当前已接取了任务：{task_type} {userstask[user_id]['任务名称']}\n{userstask[user_id]['任务内容']['desc']}"
            await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
            await sect_task.finish()

        create_user_sect_task(user_id, sect_id)
        if userstask[user_id]['任务内容']['type'] == 1:
            task_type = "⚔️"
        else:
            task_type = "💰"
        msg = f"{task_type} {userstask[user_id]['任务内容']['desc']}"
        await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
        await sect_task.finish()
    else:
        msg = f"道友尚未加入宗门，请加入宗门后再获取任务！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_task.finish()


@sect_task_complete.handle(parameterless=[Cooldown(cd_time=config['宗门任务完成cd'], stamina_cost = 3,)])
async def sect_task_complete_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """完成宗门任务"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_task_complete.finish()
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    if sect_id:
        if not isUserTask(user_id):
            msg = f"道友当前没有接取宗门任务哦！"
            await handle_send(bot, event, msg, md_type="宗门", k1="接取", v1="宗门任务接取", k2="完成", v2="宗门任务完成", k3="刷新", v3="宗门任务刷新")
            await sect_task_complete.finish()
            
        sect_info = sql_message.get_sect_info(sect_id)
        if userstask[user_id]['任务内容']['type'] == 1:  # type=1：需要扣气血，type=2：需要扣灵石
            costhp = int((user_info['exp'] / 2) * userstask[user_id]['任务内容']['cost'])
            if user_info['hp'] < user_info['exp'] / 10 or costhp >= user_info['hp']:
                msg = (
                    f"道友兴高采烈的出门做任务，结果状态欠佳，没过两招就力不从心，坚持不住了，"
                    f"道友只好原路返回，浪费了一次出门机会，看你这么可怜，就不扣你任务次数了！"
                )
                await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
                await sect_task_complete.finish()

            get_exp = int(user_info['exp'] * userstask[user_id]['任务内容']['give'])

            if user_info['sect_position'] is None:
                max_exp_limit = 4
            else:
                max_exp_limit = user_info['sect_position']
            speeds = jsondata.sect_config_data()[str(max_exp_limit)]["speeds"]
            max_exp = int(sect_info['sect_scale'] * 100)
            if max_exp >= 100000000000000:
                max_exp = 100000000000000
            max_exp = max_exp * speeds
            if get_exp >= max_exp:
                get_exp = max_exp
            max_exp_next = int((int(OtherSet().set_closing_type(user_info['level'])) * XiuConfig().closing_exp_upper_limit))  # 获取下个境界需要的修为 * 1.5为闭关上限
            if int(get_exp + user_info['exp']) > max_exp_next:
                get_exp = 1
                msg = "修为已近当前境界上限，本次所得修为收束为1点！\n"
            sect_stone = int(userstask[user_id]['任务内容']['sect'])
            settlement = sect_membership_service.settle_task(
                _sect_operation_id(event, "task_complete", user_id),
                user_id,
                sect_id,
                userstask[user_id]["period"],
                "hp",
                costhp,
                get_exp,
                sect_stone,
            )
            if not settlement.applied:
                msg = "宗门任务状态或角色资产已发生变化，请重新确认后再完成任务。"
                await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
                await sect_task_complete.finish()
            task_type_value = userstask[user_id]['任务内容']['type']
            msg += f"道友大战一番，气血减少：{number_to(costhp)}，获得修为：{number_to(get_exp)}，所在宗门建设度增加：{number_to(sect_stone)}，资材增加：{number_to(sect_stone * 10)}, 宗门贡献度增加：{int(sect_stone)}"
            userstask[user_id] = {}
            if settlement.status == "settled":
                update_statistics_value(user_id, "宗门任务")
                safe_record_game_event(
                    user_id,
                    "sect_task_complete",
                    1,
                    {
                        "source": "sect", "action": "task_complete", "skip_statistics": True,
                        "sect_id": sect_id, "exp_delta": get_exp,
                        "sect_contribution_delta": int(sect_stone), "sect_scale_delta": sect_stone,
                        "sect_materials_delta": sect_stone * 10,
                        "detail": {"task_type": task_type_value, "hp_cost": costhp},
                    },
                )
            await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
            await sect_task_complete.finish()

        elif userstask[user_id]['任务内容']['type'] == 2:  # type=1：需要扣气血，type=2：需要扣灵石
            costls = userstask[user_id]['任务内容']['cost']

            if costls > int(user_info['stone']):
                msg = (
                    f"道友兴高采烈的出门做任务，结果发现灵石带少了，当前任务所需灵石：{number_to(costls)},"
                    f"道友只好原路返回，浪费了一次出门机会，看你这么可怜，就不扣你任务次数了！")
                await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
                await sect_task_complete.finish()

            get_exp = int(user_info['exp'] * userstask[user_id]['任务内容']['give'])

            if user_info['sect_position'] is None:
                max_exp_limit = 4
            else:
                max_exp_limit = user_info['sect_position']
            speeds = jsondata.sect_config_data()[str(max_exp_limit)]["speeds"]
            max_exp = int(sect_info['sect_scale'] * 100)
            if max_exp >= 100000000000000:
                max_exp = 100000000000000
            max_exp = max_exp * speeds
            if get_exp >= max_exp:
                get_exp = max_exp
            max_exp_next = int((int(OtherSet().set_closing_type(user_info['level'])) * XiuConfig().closing_exp_upper_limit))  # 获取下个境界需要的修为 * 1.5为闭关上限
            if int(get_exp + user_info['exp']) > max_exp_next:
                get_exp = 1
                msg = "修为已近当前境界上限，本次所得修为收束为1点！\n"
            sect_stone = int(userstask[user_id]['任务内容']['sect'])
            settlement = sect_membership_service.settle_task(
                _sect_operation_id(event, "task_complete", user_id),
                user_id,
                sect_id,
                userstask[user_id]["period"],
                "stone",
                costls,
                get_exp,
                sect_stone,
            )
            if not settlement.applied:
                msg = "宗门任务状态或角色资产已发生变化，请重新确认后再完成任务。"
                await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
                await sect_task_complete.finish()
            task_type_value = userstask[user_id]['任务内容']['type']
            msg = f"道友为了完成任务购买宝物消耗灵石：{number_to(costls)}枚，获得修为：{number_to(get_exp)}，所在宗门建设度增加：{number_to(sect_stone)}，资材增加：{number_to(sect_stone * 10)}, 宗门贡献度增加：{int(sect_stone)}"
            userstask[user_id] = {}
            if settlement.status == "settled":
                update_statistics_value(user_id, "宗门任务")
                safe_record_game_event(
                    user_id,
                    "sect_task_complete",
                    1,
                    {
                        "source": "sect", "action": "task_complete", "skip_statistics": True,
                        "sect_id": sect_id, "stone_delta": -int(costls), "exp_delta": get_exp,
                        "sect_contribution_delta": int(sect_stone), "sect_scale_delta": sect_stone,
                        "sect_materials_delta": sect_stone * 10,
                        "detail": {"task_type": task_type_value, "stone_cost": int(costls)},
                    },
                )
            await handle_send(bot, event, msg, md_type="宗门", k1="刷新", v1="宗门任务刷新", k2="完成", v2="宗门任务完成", k3="接取", v3="宗门任务接取")
            await sect_task_complete.finish()
    else:
        msg = f"道友尚未加入宗门，请加入宗门后再完成任务，但你申请出门的机会我已经用小本本记下来了！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_task_complete.finish()


@sect_owner_change.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_owner_change_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗主传位"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    enabled_groups = JsonConfig().get_enabled_groups()
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_owner_change.finish()
    user_id = user_info['user_id']
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_owner_change.finish()
    position_this = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(position_this[0]) if len(position_this) == 1 else 0
    if user_info['sect_position'] != owner_position:
        msg = f"只有宗主才能进行传位。"
        await handle_send(bot, event, msg)
        await sect_owner_change.finish()
    give_qq = get_at_user_id(args)  # 艾特的时候存到这里
    if give_qq:
        if give_qq == user_id:
            msg = f"无法对自己的进行传位操作。"
            await handle_send(bot, event, msg)
            await sect_owner_change.finish()
        else:
            result = sect_membership_service.transfer_owner(
                _sect_operation_id(event, "transfer_owner", give_qq),
                user_id,
                give_qq,
                owner_position=owner_position,
            )
            if result.succeeded:
                msg = f"传老宗主{result.actor_name or user_info['user_name']}法旨，即日起由{result.target_name}继任{result.sect_name}宗主"
                await handle_send(bot, event, msg, md_type="宗门", k1="宗门", v1="我的宗门", k2="成员", v2="查看宗门成员", k3="帮助", v3="宗门帮助")
                await sect_owner_change.finish()
            elif result.status == "target_missing":
                msg = "未找到目标道友，请检查后重试。"
                await handle_send(bot, event, msg)
                await sect_owner_change.finish()
            elif result.status == "target_not_member":
                msg = f"目标道友不在你管理的宗门内，请检查。"
                await handle_send(bot, event, msg)
                await sect_owner_change.finish()
            else:
                msg = "宗门状态已经变化，当前无法完成传位，请刷新宗门信息后重试。"
                await handle_send(bot, event, msg)
                await sect_owner_change.finish()
    else:
        msg = f"请按照规范进行操作,ex:宗主传位@XXX,将XXX道友(需在自己管理下的宗门)升为宗主，自己则变为宗主下一等职位。"
        await handle_send(bot, event, msg)
        await sect_owner_change.finish()


@sect_rename.handle(parameterless=[Cooldown(cd_time=XiuConfig().sect_rename_cd * 86400,)])
async def sect_rename_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗门改名"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_rename.finish()
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
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
        len_sect_name = len(update_sect_name.encode('gbk'))

        if len_sect_name > 20:
            msg = f"道友输入的宗门名字过长,请重新输入！"
            await handle_send(bot, event, msg, md_type="宗门", k1="改名", v1="宗门改名", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_rename.finish()

        elif not update_sect_name:
            msg = f"道友确定要改名无名之宗门？还请三思。"
            await handle_send(bot, event, msg, md_type="宗门", k1="改名", v1="宗门改名", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_rename.finish()

        result = sect_membership_service.rename_sect(
            _sect_operation_id(event, "rename", sect_id),
            user_info['user_id'],
            sect_id,
            update_sect_name,
            XiuConfig().sect_rename_cost,
            SECT_RENAME_CARD_ID,
            owner_position=owner_position,
        )
        if result.status == "name_exists":
            msg = f"已存在同名宗门(自己宗门名字一样的就不要改了),请重新输入！"
            await handle_send(bot, event, msg, md_type="宗门", k1="改名", v1="宗门改名", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_rename.finish()
        if result.status == "stone_insufficient":
            msg = f"道友宗门灵石储备不足，还需补足改名所需的{number_to(XiuConfig().sect_rename_cost)}灵石!"
            await handle_send(bot, event, msg, md_type="宗门", k1="改名", v1="宗门改名", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_rename.finish()
        if result.status == "card_insufficient":
            msg = f"宗门改名需要消耗1个{SECT_RENAME_CARD_NAME}！"
            await handle_send(bot, event, msg, md_type="宗门", k1="改名", v1="宗门改名", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_rename.finish()
        if not result.applied:
            await handle_send(bot, event, "宗门状态已经变化，当前无法改名，请刷新宗门信息后重试。")
            await sect_rename.finish()

        if result.status == "renamed":
            msg = f"""
传宗门——{result.previous_name}
宗主{user_info['user_name']}法旨:
宗门改名为{result.new_name}！
星斗更迭，法器灵通，神光熠熠。
愿同门共沐神光，共护宗门千世荣光！
青天无云，道韵长存，灵气飘然。
愿同门同心同德，共铸宗门万世辉煌！"""
            await handle_send(bot, event, msg, md_type="宗门", k1="宗门", v1="我的宗门", k2="成员", v2="查看宗门成员", k3="帮助", v3="宗门帮助")
        else:
            await handle_send(bot, event, f"宗门已改名为{result.new_name}，本次操作未重复扣除资源。")
        await sect_rename.finish()

@create_sect.handle(parameterless=[Cooldown(cd_time=0)])
async def create_sect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    """创建宗门（提供10个候选名称+取消选项）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await create_sect.finish()
    
    user_id = user_info['user_id']
    sect_id = user_info['sect_id']
    sect_info = sql_message.get_sect_info(sect_id)
    level = user_info['level']
    list_level_all = list(jsondata.level_data().keys())

    # 检查境界
    if (list_level_all.index(level) < list_level_all.index(XiuConfig().sect_min_level)):
        msg = f"需达到{XiuConfig().sect_min_level}境才可创建宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await create_sect.finish()
    
    # 检查灵石
    if user_info['stone'] < XiuConfig().sect_create_cost:
        msg = f"创建需{XiuConfig().sect_create_cost}灵石！"
        await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await create_sect.finish()
    
    # 检查是否已有宗门
    if user_info['sect_id']:
        msg = f"道友已是【{sect_info['sect_name']}】成员，无法另立门户！"
        await handle_send(bot, event, msg, md_type="宗门", k1="帮助", v1="宗门帮助", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
        await create_sect.finish()
    
    # 自动宗名模式：直接创建，不进入候选选择流程
    if JsonConfig().is_auto_sect_name_enabled():
        sect_name = generate_random_sect_name(1)[0]
        owner_position = next(
            (k for k, v in jsondata.sect_config_data().items() if v.get("title") == "宗主"),
            0
        )
        creation = sect_membership_service.create_sect(
            _sect_operation_id(event, "create", user_id),
            user_id,
            sect_name,
            XiuConfig().sect_create_cost,
            owner_position,
        )
        if not creation.applied:
            msg = "宗门名称已存在，或道友当前状态无法创建宗门，请重新尝试。"
            await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await create_sect.finish()

        # 获取用户信息
        user_info = sql_message.get_user_info_with_id(user_id)

        msg = (
            f"恭喜{user_info['user_name']}道友创建宗门——{sect_name}，"
            f"宗门编号为{creation.sect_id}。\n"
            f"为道友贺！为仙道贺！"
        )
        await handle_send(
            bot, event, msg, md_type="宗门",
            k1="加入", v1=f"宗门加入 {sect_name}",
            k2="宗门", v2="我的宗门",
            k3="捐献", v3="宗门捐献"
        )
        await create_sect.finish()
    
    # 手动选名模式：生成10个候选名称
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
    await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")


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
            await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            # 继续创建流程（不return，走后续统一创建）
        else:
            refresh = sect_membership_service.charge_name_refresh(
                _sect_operation_id(event, "name_refresh", user_id),
                user_id,
                stone_cost,
            )
            if not refresh.applied:
                msg = "灵石不足或道友状态已变化，无法刷新宗门名称。"
                await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
                await create_sect.finish()
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
            await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await create_sect.reject()  # 继续等待用户选择
            return
    
    # 有效选择
    elif user_choice.isdigit() and 1 <= int(user_choice) <= 10:
        sect_name = name_options[int(user_choice) - 1]
    else:
        # 非数字或超出范围，随机选择一个名字
        sect_name = random.choice(name_options)
    
    owner_position = next(
        (k for k, v in jsondata.sect_config_data().items() if v.get("title") == "宗主"),
        0
    )
    creation = sect_membership_service.create_sect(
        _sect_operation_id(event, "create", user_id),
        user_id,
        sect_name,
        stone_cost,
        owner_position,
    )
    if creation.status == "name_exists":
        msg = "该宗门名称已存在，请重新发起创建并选择其他名称。"
        await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await create_sect.finish()
    if not creation.applied:
        msg = "道友的灵石或宗门状态已发生变化，创建失败。"
        await handle_send(bot, event, msg, md_type="宗门", k1="创建", v1="创建宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await create_sect.finish()
    
    # 获取用户信息
    user_info = sql_message.get_user_info_with_id(user_id)
    
    msg = (
        f"恭喜{user_info['user_name']}道友创建宗门——{sect_name}，"
        f"宗门编号为{creation.sect_id}。\n"
        f"为道友贺！为仙道贺！"
    )
    await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1=f"宗门加入 {sect_name}", k2="宗门", v2="我的宗门", k3="捐献", v3="宗门捐献")
    await create_sect.finish()

@sect_kick_out.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_kick_out_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """踢出宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_kick_out.finish()
    
    # 检查用户是否有宗门
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_kick_out.finish()
    
    # 解析参数
    arg_list = args.extract_plain_text().strip().split()
    if len(arg_list) < 1:
        msg = f"请按照规范进行操作，例如：踢出宗门 道号"
        await handle_send(bot, event, msg, md_type="宗门", k1="提出", v1="宗门踢出", k2="成员", v2="查看宗门成员", k3="帮助", v3="宗门帮助")
        await sect_kick_out.finish()
    
    # 获取目标用户信息
    nick_name = arg_list[0]  # 道号
    give_user = sql_message.get_user_info_with_name(nick_name)
    
    if not give_user:
        msg = f"修仙界没有名为【{nick_name}】的道友，请检查道号是否正确！"
        await handle_send(bot, event, msg, md_type="宗门", k1="踢出", v1="宗门踢出", k2="成员", v2="查看宗门成员", k3="帮助", v3="宗门帮助")
        await sect_kick_out.finish()
    
    # 获取长老职位配置
    position_zhanglao = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "长老"]
    idx_position = int(position_zhanglao[0]) if len(position_zhanglao) == 1 else 2

    if user_info['sect_position'] > idx_position:
        msg = f"""你的宗门职务为{jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']}，只有长老及以上可执行踢出操作。"""
        await handle_send(bot, event, msg, md_type="宗门", k1="踢出", v1="宗门踢出", k2="成员", v2="查看宗门成员", k3="帮助", v3="宗门帮助")
        await sect_kick_out.finish()

    result = sect_membership_service.kick_member(
        _sect_operation_id(event, "kick", give_user['user_id']),
        user_info['user_id'],
        give_user['user_id'],
        manager_max_position=idx_position,
    )
    if result.status == "self_target":
        msg = f"无法对自己进行操作，试试退出宗门？"
    elif result.status == "different_sect":
        msg = f"{give_user['user_name']}不在你管理的宗门内，请检查。"
    elif result.status == "target_not_lower":
        target_position = result.target_position
        target_title = (
            jsondata.sect_config_data()[f"{target_position}"]['title']
            if target_position is not None and f"{target_position}" in jsondata.sect_config_data()
            else "未知职位"
        )
        msg = f"""{give_user['user_name']}的宗门职务为{target_title}，不在你之下，无权操作。"""
    elif result.status in {"kicked", "duplicate"}:
        actor_position = result.actor_position if result.actor_position is not None else user_info['sect_position']
        actor_title = jsondata.sect_config_data()[f"{actor_position}"]['title']
        sect_name = result.sect_name or (sql_message.get_sect_info_by_id(user_info['sect_id']) or {}).get('sect_name', '')
        msg = f"""传{actor_title}{result.actor_name or user_info['user_name']}法旨，即日起{result.target_name or give_user['user_name']}被{sect_name}除名"""
    elif result.status == "target_not_found":
        msg = f"修仙界没有名为【{nick_name}】的道友，请检查道号是否正确！"
    else:
        msg = f"{give_user['user_name']}不在你管理的宗门内，请检查。"

    await handle_send(bot, event, msg, md_type="宗门", k1="踢出", v1="宗门踢出", k2="成员", v2="查看宗门成员", k3="帮助", v3="宗门帮助")
    await sect_kick_out.finish()

@sect_out.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_out_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """退出宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_out.finish()
    user_id = user_info['user_id']
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_out.finish()
    position_this = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(position_this[0]) if len(position_this) == 1 else 0
    result = sect_membership_service.leave_sect(
        _sect_operation_id(event, "leave", user_id),
        user_id,
        owner_position=owner_position,
    )
    if result.status == "owner_cannot_leave":
        msg = f"宗主无法直接退出宗门，如确有需要，请完成宗主传位后另行尝试。"
        await handle_send(bot, event, msg, md_type="宗门", k1="传位", v1="宗主传位", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_out.finish()
    sect_name = result.sect_name or (sql_message.get_sect_info_by_id(int(user_info['sect_id'])) or {}).get('sect_name', '')
    msg = f"道友已退出{sect_name}，今后就是自由散修，是福是祸，犹未可知。"
    await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
    await sect_out.finish()


@sect_donate.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_donate_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗门捐献"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_donate.finish()

    user_id = user_info['user_id']
    if not user_info['sect_id']:
        msg = f"道友还未加入一方宗门。"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_donate.finish()

    msg = args.extract_plain_text().strip()
    donate_num = re.findall(r"\d+", msg)  # 捐献灵石数

    if len(donate_num) > 0:
        donate_stone = int(donate_num[0])

        if donate_stone > user_info['stone']:
            msg = f"道友的灵石数量小于欲捐献数量{donate_stone}，请检查"
            await handle_send(bot, event, msg, md_type="宗门", k1="捐献", v1="宗门捐献", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_donate.finish()

        # 捐献资材倍率
        materials_rate = config.get("宗门捐献资材倍率", 1)
        add_materials = int(donate_stone * materials_rate)

        donation = sect_membership_service.donate(
            _sect_operation_id(event, "donate", user_id),
            user_id,
            user_info["sect_id"],
            donate_stone,
            add_materials,
        )
        if donation.status == "stone_insufficient":
            msg = f"道友的灵石数量小于欲捐献数量{donate_stone}，请检查"
            await handle_send(bot, event, msg, md_type="宗门", k1="捐献", v1="宗门捐献", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_donate.finish()
        if donation.status in {"sect_changed", "sect_missing", "user_changed"}:
            msg = "道友的宗门状态已发生变化，请重新确认后再捐献。"
            await handle_send(bot, event, msg, md_type="宗门", k1="捐献", v1="宗门捐献", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_donate.finish()
        if not donation.applied:
            msg = "宗门捐献失败，请检查输入后重试。"
            await handle_send(bot, event, msg, md_type="宗门", k1="捐献", v1="宗门捐献", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
            await sect_donate.finish()
        if donation.status == "donated":
            safe_log_economy_change(
                user_id=user_id,
                sect_id=user_info["sect_id"],
                source="sect",
                action="donate",
                stone_delta=-donate_stone,
                sect_contribution_delta=donate_stone,
                sect_scale_delta=donate_stone,
                sect_materials_delta=add_materials,
                detail={"donate_stone": donate_stone, "materials_rate": materials_rate},
            )
            safe_record_game_event(
                user_id,
                "sect_donate",
                donate_stone,
                {
                    "source": "sect",
                    "action": "donate",
                    "skip_statistics": True,
                    "sect_id": user_info["sect_id"],
                    "detail": {"donate_stone": donate_stone, "materials_rate": materials_rate},
                },
            )

        msg = (
            f"道友捐献灵石{donate_stone}枚，"
            f"宗门建设度增加：{donate_stone}，"
            f"宗门资材增加：{add_materials}，"
            f"宗门贡献度增加：{donate_stone}点，蒸蒸日上！"
        )
        await handle_send(bot, event, msg, md_type="宗门", k1="捐献", v1="宗门捐献", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_donate.finish()
    else:
        msg = f"捐献的灵石数量解析异常"
        await handle_send(bot, event, msg, md_type="宗门", k1="捐献", v1="宗门捐献", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_donate.finish()

@sect_position_update.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_position_update_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """宗门职位变更（支持职位编号和职位名称）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_position_update.finish()
    
    user_id = user_info['user_id']
    
    # 检查权限（长老及以上可以变更职位）
    position_zhanglao = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "长老"]
    idx_position = int(position_zhanglao[0]) if len(position_zhanglao) == 1 else 2
    
    if user_info['sect_position'] > idx_position:
        msg = f"""你的宗门职位为{jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']}，无权进行职位管理！"""
        await handle_send(bot, event, msg)
        await sect_position_update.finish()
    
    # 解析参数
    raw_args = args.extract_plain_text().strip()
    if not raw_args:
        msg = f"请输入正确指令！例如：宗门职位变更 道号 职位编号/职位名称"
        await handle_send(bot, event, msg, md_type="宗门", k1="变更", v1="宗门职位变更", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_position_update.finish()
    
    # 分割参数
    args_list = raw_args.split()
    if len(args_list) < 2:
        msg = f"参数不足！格式应为：宗门职位变更 道号 职位编号/职位名称"
        await handle_send(bot, event, msg, md_type="宗门", k1="变更", v1="宗门职位变更", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_position_update.finish()
    
    # 获取职位参数（最后一个参数）
    position_arg = args_list[-1]
    
    # 解析职位编号或名称
    position_num = None
    if position_arg.isdigit() and position_arg in jsondata.sect_config_data().keys():
        position_num = position_arg
    else:
        # 通过职位名称查找编号
        for pos_id, pos_data in jsondata.sect_config_data().items():
            if pos_data.get("title", "") == position_arg:
                position_num = pos_id
                break
    
    if position_num is None:
        # 构建职位帮助信息
        position_help = "支持的职位：\n"
        for pos_id, pos_data in jsondata.sect_config_data().items():
            max_count = pos_data.get("max_count", 0)
            count_info = f"（限{max_count}人）" if max_count > 0 else "（不限）"
            position_help += f"{pos_id}. {pos_data['title']}{count_info}\n"
        
        msg = f"职位参数解析异常！请输入有效的职位编号或名称。\n{position_help}"
        await handle_send(bot, event, msg, md_type="宗门", k1="变更", v1="宗门职位变更", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_position_update.finish()
    
    # 获取道号（合并前面的所有参数）
    nick_name = ' '.join(args_list[:-1]).strip()
    if not nick_name:
        msg = f"请输入有效的道号！"
        await handle_send(bot, event, msg, md_type="宗门", k1="变更", v1="宗门职位变更", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_position_update.finish()
    
    # 获取目标用户信息
    give_user = sql_message.get_user_info_with_name(nick_name)
    if not give_user:
        msg = f"修仙界没有名为【{nick_name}】的道友，请检查道号是否正确！"
        await handle_send(bot, event, msg, md_type="宗门", k1="变更", v1="宗门职位变更", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_position_update.finish()
    
    # 检查不能操作自己
    if give_user['user_id'] == user_id:
        msg = f"无法对自己的职位进行管理。"
        await handle_send(bot, event, msg, md_type="宗门", k1="变更", v1="宗门职位变更", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_position_update.finish()
    
    position_limits = {
        int(pos_id): int(pos_data.get("max_count", 0) or 0)
        for pos_id, pos_data in jsondata.sect_config_data().items()
    }
    result = sect_membership_service.change_position(
        _sect_operation_id(event, "position_change", give_user['user_id']),
        user_id,
        give_user['user_id'],
        int(position_num),
        position_limits,
        manager_max_position=idx_position,
    )
    if result.status == "target_not_member":
        msg = f"请确保变更目标道友与你在同一宗门。"
    elif result.status == "target_not_below_actor":
        target_position = result.old_position
        target_title = (
            jsondata.sect_config_data()[f"{target_position}"]['title']
            if target_position is not None and f"{target_position}" in jsondata.sect_config_data()
            else "未知职位"
        )
        msg = f"""{give_user['user_name']}的宗门职务为{target_title}，不在你之下，无权操作。"""
    elif result.status == "position_not_below_actor":
        msg = f"道友试图变更的职位品阶必须在你品阶之下"
    elif result.status == "position_full":
        position_data = jsondata.sect_config_data()[position_num]
        max_count = position_data.get("max_count", 0)
        current_count = max_count
        msg = f"{position_data['title']}职位已有{current_count}人，已达到上限{max_count}人，无法再任命！"
    elif result.status == "unchanged":
        title = jsondata.sect_config_data()[position_num]['title']
        msg = f"{give_user['user_name']}当前已担任本宗{title}，无需重复变更。"
    elif result.status in {"changed", "duplicate"}:
        actor_title = jsondata.sect_config_data()[f"{user_info['sect_position']}"]['title']
        old_title = jsondata.sect_config_data()[f"{result.old_position}"]['title']
        new_title = jsondata.sect_config_data()[f"{result.new_position}"]['title']
        action_text = "晋升为" if result.new_position < result.old_position else "调整为"
        msg = f"""传{actor_title}{result.actor_name or user_info['user_name']}法旨：
即日起{result.target_name or give_user['user_name']}由{old_title}{action_text}本宗{new_title}"""
    else:
        msg = f"请确保变更目标道友与你在同一宗门。"

    await handle_send(bot, event, msg, md_type="宗门", k1="宗门", v1="我的宗门", k2="成员", v2="查看宗门成员", k3="帮助", v3="宗门帮助")
    await sect_position_update.finish()

@join_sect.handle(parameterless=[Cooldown(cd_time=0)])
async def join_sect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """加入宗门（支持宗门ID和宗门名）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await join_sect.finish()
    
    # 检查是否已有宗门
    sect_id = user_info['sect_id']
    if user_info['sect_id']:
        msg = f"道友已经加入了宗门:{sql_message.get_sect_info(sect_id)['sect_name']}，无法再加入其他宗门。"
        await handle_send(bot, event, msg)
        await join_sect.finish()
    
    sect_input = args.extract_plain_text().strip()
    if not sect_input:
        msg = "请输入宗门编号或宗门名称！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await join_sect.finish()
    
    # 判断输入是宗门ID还是宗门名
    target_sect_id = None
    target_sect_name = None
    
    if sect_input.isdigit():
        # 输入的是数字，按宗门ID处理
        target_sect_id = int(sect_input)
        sect_info = sql_message.get_sect_info(target_sect_id)
        if sect_info:
            target_sect_name = sect_info['sect_name']
    else:
        # 输入的是字符串，按宗门名处理
        target_sect_id = sql_message.get_sect_name(sect_input)
        if target_sect_id:
            sect_info = sql_message.get_sect_info(target_sect_id)
            target_sect_name = sect_info['sect_name'] if sect_info else None
    
    # 检查宗门是否存在
    if not target_sect_id or not target_sect_name:
        msg = f"未找到名为【{sect_input}】的宗门，请检查输入是否正确！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await join_sect.finish()
    
    # 检查宗门是否可以加入
    can_join, reason = can_join_sect(target_sect_id)
    if not can_join:
        msg = f"宗门【{target_sect_name}】{reason}，无法加入！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await join_sect.finish()
    
    # 检查人数上限
    max_members = get_sect_member_limit(sql_message.get_sect_info(target_sect_id)['sect_scale'])
    current_members = len(sql_message.get_all_users_by_sect_id(target_sect_id))
    
    if current_members >= max_members:
        msg = f"该宗门人数已满（{current_members}/{max_members}），无法加入！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await join_sect.finish()
    
    # 执行加入宗门
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "外门弟子"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 12
    sql_message.update_usr_sect(user_info['user_id'], target_sect_id, owner_position)
    
    msg = f"欢迎{user_info['user_name']}道友加入【{target_sect_name}】！当前宗门人数：{current_members + 1}/{max_members}"
    await handle_send(bot, event, msg, md_type="宗门", k1="宗门", v1="我的宗门", k2="成员", v2="查看宗门成员", k3="帮助", v3="宗门帮助")
    await join_sect.finish()

@my_sect.handle(parameterless=[Cooldown(cd_time=0)])
async def my_sect_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
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
        sect_power = sect_info.get('combat_power', 0)
        
        # 计算宗门人数上限
        max_members = get_sect_member_limit(sect_info['sect_scale'])
        
        # 获取当前宗门人数
        current_members = len(sql_message.get_all_users_by_sect_id(sect_id))
        
        fairyland_level = _get_sect_fairyland_level(sect_info)
        fairyland_conf = _get_sect_fairyland_config(fairyland_level)
        fairyland_text = (
            "暂无"
            if fairyland_level <= 0
            else (
                f"{fairyland_level}级【{fairyland_conf['name']}】"
                f"（炼体+{get_sect_fairyland_bonus(fairyland_level) * 100:.0f}%，"
                f"每日淬体{fairyland_conf['minutes']}分钟）"
            )
        )

        msg = f"""
{user_name}所在宗门
宗门名讳：{sect_info['sect_name']}
宗门编号：{sect_id}
宗   主：{sql_message.get_user_info_with_id(sect_info['sect_owner'])['user_name'] if sect_info['sect_owner'] else "暂无"}
道友职位：{jsondata.sect_config_data()[f"{sect_position}"]["title"]}
宗门状态：{join_status}{closed_status}
宗门人数：{current_members}/{max_members}
宗门建设度：{number_to(sect_info['sect_scale'])}
炼体堂：{fairyland_text}
宗门排名：{top_idx_list.index(sect_id) + 1 if sect_id in top_idx_list else "未上榜"}
宗门拥有资材：{number_to(sect_info['sect_materials'])}
宗门贡献度：{number_to(user_info['sect_contribution'])}
宗门战力：{number_to(sect_power)}
宗门丹房：{elixir_room_name}
"""
        if sect_position == owner_position:
            msg += f"\n宗门储备：{number_to(sect_info['sect_used_stone'])}枚灵石"
    else:
        msg = f"一介散修，莫要再问。"

    await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
    await my_sect.finish()


@sect_buildings.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_buildings_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门建设聚合入口"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_buildings.finish()

    sect_id = user_info.get("sect_id")
    if not sect_id:
        await handle_send(
            bot,
            event,
            "道友尚未加入宗门，无法查看宗门建设。",
            md_type="宗门",
            k1="加入",
            v1="宗门加入",
            k2="列表",
            v2="宗门列表",
            k3="帮助",
            v3="宗门帮助",
        )
        await sect_buildings.finish()

    sect_info = sql_message.get_sect_info(sect_id)
    elixir_room_level = int(sect_info.get("elixir_room_level", 0) or 0)
    elixir_room_config = config["宗门丹房参数"]["elixir_room_level"]
    elixir_room_name = "未建设" if elixir_room_level <= 0 else elixir_room_config[str(elixir_room_level)]["name"]
    fairyland_level = _get_sect_fairyland_level(sect_info)
    fairyland_conf = _get_sect_fairyland_config(fairyland_level)
    fairyland_name = "未建设" if fairyland_level <= 0 else f"{fairyland_level}级【{fairyland_conf['name']}】"
    task = sect_task_state_manager.get_active_task(user_info["user_id"])
    task_msg = "未接取，发送【宗门任务】获取" if not task else f"{task['任务名称']}：{task['任务内容'].get('desc', '')}"
    members = sql_message.get_all_users_by_sect_id(sect_id)
    max_members = get_sect_member_limit(sect_info["sect_scale"])

    msg = (
        f"【宗门建设】\n"
        f"宗门：{sect_info['sect_name']}（{len(members)}/{max_members}人）\n"
        f"建设度：{number_to(sect_info['sect_scale'])}\n"
        f"宗门储备：{number_to(sect_info['sect_used_stone'])}灵石\n"
        f"宗门资材：{number_to(sect_info['sect_materials'])}\n"
        f"个人贡献：{number_to(user_info['sect_contribution'])}\n"
        f"\n当前建筑：\n"
        f"丹房：{elixir_room_name}\n"
        f"炼体堂：{fairyland_name}\n"
        f"修炼上限：{get_sect_level(sect_id)[0]}级\n"
        f"\n当前任务：\n{task_msg}\n"
        f"\n可执行操作：宗门任务、宗门周常、宗门捐献、宗门商店、宗门丹房建设、宗门炼体堂升级"
    )
    await handle_send(
        bot,
        event,
        msg,
        md_type="宗门",
        k1="任务",
        v1="宗门任务",
        k2="周常",
        v2="宗门周常",
        k3="商店",
        v3="宗门商店",
    )
    await sect_buildings.finish()


@sect_close_join.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_close_join_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """关闭宗门加入"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_close_join.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
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

@sect_open_join.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_open_join_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """开放宗门加入"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_open_join.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
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

@sect_close_mountain.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_close_mountain_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """封闭山门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_close_mountain.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_close_mountain.finish()
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        # 再次确认
        msg = "确定要封闭山门吗？封闭后：\n1. 自动关闭宗门加入\n2. 你将退位为长老\n3. 宗门将处于无主状态\n4. 长老们可以继承宗主之位\n\n请确认后再次发送【确认封闭山门】"
        await handle_send(bot, event, msg, md_type="宗门", k1="确定", v1="确认封闭山门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_close_mountain.finish()
    else:
        msg = "只有宗主可以封闭山门！"
        await handle_send(bot, event, msg)
        await sect_close_mountain.finish()

@sect_close_mountain2.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_close_mountain2_confirm(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """确认封闭山门"""

    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_close_mountain2.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
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
        sql_message.update_usr_sect(user_info['user_id'], sect_id, 2)  # 2是长老职位
        # 4. 清空宗主
        sql_message.update_sect_owner(None, sect_id)
        
        msg = "已封闭山门！你已退位为长老，宗门现在处于无主状态。长老们可以使用【继承宗主】来继承宗主之位。"
    else:
        msg = "只有宗主可以封闭山门！"
    
    await handle_send(bot, event, msg, md_type="宗门", k1="继承", v1="继承宗主", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
    await sect_close_mountain2.finish()

@sect_inherit.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_inherit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """继承宗主"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_inherit.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_inherit.finish()
    
    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info['closed']:
        msg = "宗门未封闭，无需继承！"
        await handle_send(bot, event, msg)
        await sect_inherit.finish()
    
    # 检查职位是否符合继承条件
    if user_info['sect_position'] not in [1, 2, 6, 7]:  # 1=副宗主，2=长老, 6=大师兄，7=大师姐
        msg = "只有副宗主、长老、大师兄、大师姐可以继承宗主之位！"
        await handle_send(bot, event, msg, md_type="宗门", k1="继承", v1="继承宗主", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
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
        await handle_send(bot, event, msg, md_type="宗门", k1="继承", v1="继承宗主", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
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

@sect_disband.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_disband_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """解散宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_disband.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_disband.finish()
    
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        # 再次确认
        msg = "确定要解散宗门吗？解散后：\n1. 所有成员将被踢出\n2. 宗门将被删除\n3. 所有宗门资源将消失\n\n请确认后再次发送【确认解散宗门】"
        await handle_send(bot, event, msg, md_type="宗门", k1="确定", v1="确认解散宗门", k2="宗门", v2="我的宗门", k3="帮助", v3="宗门帮助")
        await sect_disband.finish()
    else:
        msg = "只有宗主可以解散宗门！"
        await handle_send(bot, event, msg)
        await sect_disband.finish()

@sect_disband2.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_disband2_confirm(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """确认解散宗门"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_disband2.finish()
    
    sect_id = user_info['sect_id']
    if not sect_id:
        msg = "道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_disband2.finish()
    
    sect_position = user_info['sect_position']
    owner_idx = [k for k, v in jsondata.sect_config_data().items() if v.get("title", "") == "宗主"]
    owner_position = int(owner_idx[0]) if len(owner_idx) == 1 else 0
    
    if sect_position == owner_position:
        # 删除宗门
        sql_message.delete_sect(sect_id)
        
        msg = f"宗门已解散！所有成员已被移除。"
    else:
        msg = "只有宗主可以解散宗门！"
    
    await handle_send(bot, event, msg)
    await sect_disband2.finish()

@sect_power_top.handle(parameterless=[Cooldown(cd_time=0)])
async def sect_power_top_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """宗门战力排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    top_list = sql_message.combat_power_top()
    
    msg_list = ["【宗门战力排行】"]
    for i, (sect_id, sect_name, power) in enumerate(top_list, 1):
        msg_list.append(f"{i}. {sect_name} - 战力：{number_to(power)}")
    
    await send_msg_handler(bot, event, '宗门战力排行', bot.self_id, msg_list)
    await sect_power_top.finish()

@sect_shop.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看宗门商店"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_shop.finish()
    
    user_id = user_info['user_id']
    sect_id = sql_message.get_user_info_with_id(user_id)['sect_id']
    if not sect_id:
        msg = f"道友尚未加入宗门！"
        await handle_send(bot, event, msg, md_type="宗门", k1="加入", v1="宗门加入", k2="列表", v2="宗门列表", k3="帮助", v3="宗门帮助")
        await sect_shop.finish()
    
    sect_info = sql_message.get_sect_info(sect_id)
    if not sect_info:
        msg = "宗门信息不存在！"
        await handle_send(bot, event, msg)
        await sect_shop.finish()
    
    shop_items = config["商店商品"]
    if not shop_items:
        msg = "宗门商店暂无商品！"
        await handle_send(bot, event, msg)
        await sect_shop.finish()
    
    # 获取页码参数
    page_input = args.extract_plain_text().strip()
    try:
        page = int(page_input) if page_input else 1
    except ValueError:
        page = 1
    
    # 分页设置
    items_per_page = 5
    total_pages = (len(shop_items) + items_per_page - 1) // items_per_page
    page = max(1, min(page, total_pages))
    
    # 获取当前页的商品
    sorted_items = sorted(shop_items.items(), key=lambda x: int(x[0]))
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    current_page_items = sorted_items[start_idx:end_idx]
    
    title = f"\n道友目前拥有的宗门贡献度：{number_to(user_info['sect_contribution'])}点"
    msg_list = []
    msg_list.append(f"════════════\n【宗门商店】第{page}/{total_pages}页")
    
    for item_id, item_data in current_page_items:
        item_info = items.get_data_by_item_id(item_id)
        if not item_info:
            continue
        msg_list.append(
            f"编号：{item_id}\n"
            f"名称：{item_info['name']}\n"
            f"描述：{item_info.get('desc', '暂无描述')}\n"
            f"价格：{number_to(item_data['cost'])}贡献度\n"
            f"每周限购：{item_data['weekly_limit']}个"
        )
    
    msg_list.append(f"提示：发送 宗门商店+页码 查看其他页（共{total_pages}页）")
    page = ["翻页", f"宗门商店 {page + 1}", "宗门", "我的宗门", "兑换", "宗门兑换", f"{page}/{total_pages}"]    
    await send_msg_handler(bot, event, "宗门商店", bot.self_id, msg_list, title=title, page=page)
    await sect_shop.finish()

@sect_buy.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """兑换宗门商店物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await sect_buy.finish()

    user_id = user_info["user_id"]
    msg = args.extract_plain_text().strip()
    shop_info = re.findall(r"(\d+)\s*(\d*)", msg)

    if not shop_info:
        msg = "请输入正确的商品编号！"
        await handle_send(bot, event, msg, md_type="宗门", k1="兑换", v1="宗门兑换", k2="宗门", v2="我的宗门", k3="商店", v3="宗门商店")
        await sect_buy.finish()

    shop_id = shop_info[0][0]
    quantity = int(shop_info[0][1]) if shop_info[0][1] else 1

    sect_info = sql_message.get_sect_info(sql_message.get_user_info_with_id(user_id)['sect_id'])
    if not sect_info:
        msg = "宗门信息不存在！"
        await handle_send(bot, event, msg)
        await sect_buy.finish()

    shop_items = config["商店商品"]
    if shop_id not in shop_items:
        msg = "没有这个商品编号！"
        await handle_send(bot, event, msg, md_type="宗门", k1="兑换", v1="宗门兑换", k2="宗门", v2="我的宗门", k3="商店", v3="宗门商店")
        await sect_buy.finish()

    item_data = shop_items[shop_id]
    sect_contribution = user_info['sect_contribution']

    # 检查贡献度是否足够
    total_cost = item_data["cost"] * quantity
    if sect_contribution < total_cost:
        msg = f"贡献度不足！需要{number_to(total_cost)}点，当前拥有{number_to(sect_contribution)}点"
        await handle_send(bot, event, msg, md_type="宗门", k1="捐献", v1="宗门捐献", k2="宗门", v2="我的宗门", k3="商店", v3="宗门商店")
        await sect_buy.finish()

    # 检查封锁
    if sect_info['closed']:
        msg = "宗门已封闭，无法进行兑换。"
        await handle_send(bot, event, msg)
        await sect_buy.finish()

    # 检查限购
    already_purchased = get_sect_weekly_purchases(user_id, shop_id)
    if already_purchased + quantity > item_data["weekly_limit"]:
        msg = (
            f"该商品每周限购{item_data['weekly_limit']}个\n"
            f"本周已购买{already_purchased}个\n"
            f"无法再购买{quantity}个！"
        )
        await handle_send(bot, event, msg, md_type="宗门", k1="兑换", v1="宗门兑换", k2="宗门", v2="我的宗门", k3="商店", v3="宗门商店")
        await sect_buy.finish()

    # 兑换商品
    sect_contribution -= total_cost
    sql_message.update_sect_materials(user_info['sect_id'], total_cost, 2)
    sql_message.deduct_sect_contribution(user_id, total_cost)

    # 给予物品
    item_info = items.get_data_by_item_id(shop_id)
    sql_message.send_back(
        user_id, 
        shop_id, 
        item_info["name"], 
        item_info["type"], 
        quantity,
        1
    )
    safe_log_economy_change(
        user_id=user_id,
        sect_id=user_info["sect_id"],
        source="sect",
        action="shop_exchange",
        sect_contribution_delta=-total_cost,
        sect_materials_delta=-total_cost,
        item_delta=[
            {
                "id": shop_id,
                "name": item_info["name"],
                "type": item_info["type"],
                "amount": quantity,
            }
        ],
        detail={"shop_id": shop_id, "quantity": quantity},
    )

    msg = f"成功兑换{item_info['name']}×{quantity}，消耗{number_to(total_cost)}宗门贡献度！"
    await handle_send(bot, event, msg, md_type="宗门", k1="兑换", v1="宗门兑换", k2="宗门", v2="我的宗门", k3="商店", v3="宗门商店")

    # 更新限购记录
    update_sect_weekly_purchase(user_id, shop_id, quantity)

    await sect_buy.finish()
