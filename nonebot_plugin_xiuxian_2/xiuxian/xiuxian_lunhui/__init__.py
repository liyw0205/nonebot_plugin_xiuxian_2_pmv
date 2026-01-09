import asyncio
from datetime import datetime, timedelta
from nonebot import on_command, on_fullmatch
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, XIUXIAN_IMPART_BUFF
from ..xiuxian_utils.data_source import jsondata
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment
)
from ..xiuxian_utils.utils import (
    check_user, get_msg_pic,
    CommandObjectID, handle_send
)
from ..xiuxian_impart.impart_uitls import (
    impart_check,
    update_user_impart_data
)

xiuxian_impart = XIUXIAN_IMPART_BUFF()
confirm_lunhui_cache = {}

__warring_help__ = f"""
【轮回重修系统】♾️

⚠️ 警告：此操作不可逆！
散尽毕生修为，轮回重修，凝聚万世道果为极致天赋，开启永恒不灭之路，执掌轮回命运果位

🔥 所有修为、功法、神通、灵石、修炼等级、虚神界修炼时间将被清空！

🔄 进入轮回
   • 千世轮回获得【轮回灵根】
   • 最低境界要求：{XiuConfig().lunhui_min_level}
   
   • 万世轮回获得【真·轮回灵根】 
   • 最低境界要求：{XiuConfig().twolun_min_level}

   • 永恒轮回获得【永恒灵根】
   • 最低境界要求：{XiuConfig().threelun_min_level}
   
♾️ 进入无限轮回 - 获得【命运灵根】
   • 最低境界要求：{XiuConfig().Infinite_reincarnation_min_level}

💀 自废修为 - 仅感气境可用
  • 完全重置修为（慎用！）

📌 注意事项：
• 轮回后将更新灵根资质
• 所有装备、物品不会丢失

""".strip()

cache_help_fk = {}
sql_message = XiuxianDateManage()  # sql类

warring_help = on_command("轮回重修帮助", aliases={"轮回帮助"}, priority=12, block=True)
lunhui = on_command('进入轮回', aliases={"开始轮回"}, priority=15,  block=True)
Infinite_reincarnation = on_command('进入无限轮回', priority=15,  block=True)
resetting = on_command('自废修为', priority=15,  block=True)
confirm_lunhui = on_command('确认轮回', priority=15,  block=True)

@warring_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def warring_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """轮回重修帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __warring_help__
    await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="丹药", v3="丹药背包")
    await warring_help.finish()
        
@resetting.handle(parameterless=[Cooldown(cd_time=1.4)])
async def resetting_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await resetting.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    
                    
    if user_msg['level'] in ['感气境初期', '感气境中期', '感气境圆满']:
        exp = user_msg['exp']
        sql_message.updata_level(user_id, '江湖好手') #重置用户境界
        sql_message.update_levelrate(user_id, 0) #重置突破成功率
        sql_message.update_j_exp(user_id, exp) #重置用户修为
        sql_message.update_exp(user_id, 100)  
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        msg = f"{user_name}现在是一介凡人了！！"
        await handle_send(bot, event, msg)
        await resetting.finish()
    else:
        msg = f"道友境界未达要求，自废修为的最低境界为感气境！"
        await handle_send(bot, event, msg)
        await resetting.finish()
        
@lunhui.handle(parameterless=[Cooldown(cd_time=1.4)])
async def lunhui_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await lunhui.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    
    if str(user_id) in confirm_lunhui_cache:
        msg = "请发送【确认轮回】！"
        await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await lunhui.finish()
    if user_root == '轮回道果':
        root_level = 7
        lunhui_level = XiuConfig().twolun_min_level
        lunhui_level2 = "万世轮回"
        msg = f"万世道果集一身，脱出凡道入仙道，恭喜大能{user_name}万世轮回成功！"
    elif user_root == '真·轮回道果':
        root_level = 8
        lunhui_level = XiuConfig().threelun_min_level
        lunhui_level2 = "永恒轮回"
        msg = f"穿越千劫万难，证得不朽之身，恭喜大能{user_name}步入永恒之道，成就无上永恒！"
    elif user_root == '永恒道果':
        root_level = 9
        lunhui_level = XiuConfig().Infinite_reincarnation_min_level
        lunhui_level2 = "无限轮回"
        msg = f"超越永恒，超脱命运，执掌因果轮回！恭喜大能{user_name}突破命运桎梏，成就无上命运道果！"
    elif user_root == '命运道果':
        await Infinite_reincarnation_(bot, event)
        await lunhui.finish()
    else:
        root_level = 6
        lunhui_level = XiuConfig().lunhui_min_level
        lunhui_level2 = "千世轮回"
        msg = f"千世轮回磨不灭，重回绝颠谁能敌，恭喜大能{user_name}轮回成功！"
        
    if list_level_all.index(level) >= list_level_all.index(lunhui_level):
        await confirm_lunhui_invite(bot, event, user_id, root_level, lunhui_level2, msg)
    else:
        msg = f"道友境界未达要求\n当前进入：{lunhui_level2}\n最低境界为：{lunhui_level}"
        await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
    await lunhui.finish()

@Infinite_reincarnation.handle(parameterless=[Cooldown(cd_time=1.4)])
async def Infinite_reincarnation_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await Infinite_reincarnation.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    
    if str(user_id) in confirm_lunhui_cache:
        msg = "请发送【确认轮回】！"
        await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await Infinite_reincarnation.finish()

    if user_root != '命运道果' :
        msg = "道友还未完成轮回，请先进入轮回！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await Infinite_reincarnation.finish()
    if (list_level_all.index(level) >= list_level_all.index(XiuConfig().Infinite_reincarnation_min_level)) and user_root == '命运道果':
        msg = f"超越永恒，超脱命运，执掌因果轮回！\n恭喜大能{user_name}突破命运桎梏，成就无上命运道果！"
        await confirm_lunhui_invite(bot, event, user_id, 0, "无限轮回", msg)
    else:
        msg = f"道友境界未达要求，无限轮回的最低境界为{XiuConfig().Infinite_reincarnation_min_level}！"
        await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
    await Infinite_reincarnation.finish()

@confirm_lunhui.handle(parameterless=[Cooldown(cd_time=1.4)])
async def confirm_lunhui_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理确认轮回"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await confirm_lunhui.finish()

    user_id = user_info['user_id']
    if str(user_id) not in confirm_lunhui_cache:
        msg = "没有待处理的轮回！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
        await confirm_lunhui.finish()

    confirm_data = confirm_lunhui_cache[str(user_id)]
    root_level = confirm_data['root_level']
    original_msg = confirm_data['msg']
    impart_data_draw = await impart_check(user_id) 
    impaer_exp_time = impart_data_draw["exp_day"] if impart_data_draw is not None else 0 

    # 执行轮回操作
    user_msg = sql_message.get_user_info_with_id(user_id)
    user_name = user_msg['user_name']
    exp = user_msg['exp']
    stone = user_info['stone']
    now_stone = int(stone - 1_0000_0000)
    if now_stone >= 0:
        sql_message.update_ls(user_id, now_stone, 2)
        # 重置用户灵石（保留1亿）
    sql_message.updata_level(user_id, '江湖好手')  
    # 重置用户境界
    sql_message.update_levelrate(user_id, 0)  
    # 重置突破成功率
    sql_message.update_j_exp(user_id, exp)
    sql_message.update_exp(user_id, 100)
    # 重置用户修为
    sql_message.update_user_hp(user_id)  
    # 重置用户HP，mp，atk状态
    sql_message.updata_user_main_buff(user_id, 0)  
    # 重置用户主功法
    sql_message.updata_user_sub_buff(user_id, 0)  
    # 重置用户辅修功法
    sql_message.updata_user_sec_buff(user_id, 0)  
    # 重置用户神通
    sql_message.updata_user_effect1_buff(user_id, 0)  
    # 重置用户身法
    sql_message.updata_user_effect2_buff(user_id, 0)  
    # 重置用户瞳术
    sql_message.reset_user_drug_resistance(user_id)  
    # 重置用户耐药性
    xiuxian_impart.use_impart_exp_day(impaer_exp_time, user_id)
    # 重置用户虚神界修炼时间
    xiuxian_impart.convert_stone_to_wishing_stone(user_id)
    # 转换思恋结晶
    if root_level != 0:
        sql_message.update_user_atkpractice(user_id, 0) #重置用户攻修等级
        sql_message.update_user_hppractice(user_id, 0) #重置用户元血等级
        sql_message.update_user_mppractice(user_id, 0) #重置用户灵海等级
        sql_message.update_root(user_id, root_level)  # 更换灵根
    if root_level == 0 or root_level == 9:
        sql_message.updata_root_level(user_id, 1)  # 更新轮回等级
    msg = f"{original_msg}！"
    await handle_send(bot, event, msg, md_type="轮回", k1="修为", v1="我的修为", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")

    # 删除确认缓存
    del confirm_lunhui_cache[str(user_id)]
    await confirm_lunhui.finish()

async def confirm_lunhui_invite(bot, event, user_id, root_level, lunhui_level2, msg):
    """发送确认轮回"""
    invite_id = f"{user_id}_lunhui_{datetime.now().timestamp()}"
    confirm_lunhui_cache[str(user_id)] = {
        'root_level': root_level,
        'msg': msg,
        'invite_id': invite_id
    }

    # 设置60秒过期
    asyncio.create_task(expire_confirm_lunhui_invite(user_id, invite_id, bot, event))

    msg = f"您即将进入【{lunhui_level2}】，请在60秒内确认！\n发送【确认轮回】以继续，或等待60秒后自动取消。"
    await handle_send(bot, event, msg, md_type="轮回", k1="确认", v1="确认轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")

async def expire_confirm_lunhui_invite(user_id, invite_id, bot, event):
    """确认轮回过期处理"""
    await asyncio.sleep(60)
    if str(user_id) in confirm_lunhui_cache and confirm_lunhui_cache[str(user_id)]['invite_id'] == invite_id:
        del confirm_lunhui_cache[str(user_id)]
        msg = "确认轮回已过期！"
        await handle_send(bot, event, msg, md_type="轮回", k1="轮回", v1="进入轮回", k2="存档", v2="我的修仙信息", k3="帮助", v3="轮回帮助")
