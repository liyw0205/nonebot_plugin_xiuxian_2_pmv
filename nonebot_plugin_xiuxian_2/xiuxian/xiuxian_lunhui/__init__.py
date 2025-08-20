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


__warring_help__ = f"""
【轮回重修系统】♾️

⚠️ 警告：此操作不可逆！
散尽毕生修为，轮回重修，凝聚万世道果为极致天赋，开启永恒不灭之路，执掌轮回命运果位

🔥 所有修为、功法、神通、灵石、修炼等级、虚神界修炼时间将被清空！

🔄 轮回选项：
1. 进入千世轮回 - 获得【轮回灵根】
   • 最低境界要求：{XiuConfig().lunhui_min_level}
   
2. 进入万世轮回 - 获得【真·轮回灵根】 
   • 最低境界要求：{XiuConfig().twolun_min_level}

3. 进入永恒轮回 - 获得【永恒灵根】
   • 最低境界要求：{XiuConfig().threelun_min_level}
   
3. 进入无限轮回 - 获得【命运灵根】
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
lunhui = on_command('进入千世轮回', priority=15,  block=True)
twolun = on_command('进入万世轮回', priority=15,  block=True)
threelun = on_command('进入永恒轮回', priority=15,  block=True)
Infinite_reincarnation = on_command('进入无限轮回', priority=15,  block=True)
resetting = on_command('自废修为', priority=15,  block=True)


@warring_help.handle(parameterless=[Cooldown(at_sender=False)])
async def warring_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """轮回重修帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help_fk:
        msg = cache_help_fk[session_id]
        await handle_send(bot, event, msg)
        await warring_help.finish()
    else:
        msg = __warring_help__
        await handle_send(bot, event, msg)
        await warring_help.finish()

@lunhui.handle(parameterless=[Cooldown(at_sender=False)])
async def lunhui_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await lunhui.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    impart_data_draw = await impart_check(user_id) 
    impaer_exp_time = impart_data_draw["exp_day"] if impart_data_draw is not None else 0
    
    
    if user_root == '轮回道果' :
        msg = "道友已是千世轮回之身！"
        await handle_send(bot, event, msg)
        await lunhui.finish()
    
    if user_root == '真·轮回道果' :
        msg = "道友已是万世轮回之身！"
        await handle_send(bot, event, msg)
        await lunhui.finish()

    if user_root == '永恒道果' :
        msg = "道友已是永恒轮回之身！"
        await handle_send(bot, event, msg)
        await lunhui.finish()

    if user_root == '命运道果' :
        msg = "道友已可无限轮回！"
        await handle_send(bot, event, msg)
        await lunhui.finish()
        
    if list_level_all.index(level) >= list_level_all.index(XiuConfig().lunhui_min_level):
        exp = user_msg['exp']
        now_exp = exp - 100
        sql_message.updata_level(user_id, '江湖好手') #重置用户境界
        sql_message.update_levelrate(user_id, 0) #重置突破成功率
        sql_message.update_j_exp(user_id, now_exp) #重置用户修为
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        sql_message.updata_user_main_buff(user_id, 0) #重置用户主功法
        sql_message.updata_user_sub_buff(user_id, 0) #重置用户辅修功法
        sql_message.updata_user_sec_buff(user_id, 0) #重置用户神通
        sql_message.update_user_atkpractice(user_id, 0) #重置用户攻修等级
        sql_message.update_user_hppractice(user_id, 0) #重置用户元血等级
        sql_message.update_user_mppractice(user_id, 0) #重置用户灵海等级
        xiuxian_impart.use_impart_exp_day(impaer_exp_time, user_id)
        #重置用户虚神界修炼时间
        xiuxian_impart.update_impart_lv(user_id, 0)
        #重置虚神界等级
        sql_message.update_ls(user_id, user_info['stone'], 2)
        #重置用户灵石
        sql_message.update_root(user_id, 6) #更换灵根
        msg = f"千世轮回磨不灭，重回绝颠谁能敌，恭喜大能{user_name}轮回成功！"
        await handle_send(bot, event, msg)
        await lunhui.finish()
    else:
        msg = f"道友境界未达要求，进入千世轮回的最低境界为{XiuConfig().lunhui_min_level}"
        await handle_send(bot, event, msg)
        await lunhui.finish()
        
@twolun.handle(parameterless=[Cooldown(at_sender=False)])
async def twolun_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await twolun.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    impart_data_draw = await impart_check(user_id) 
    impaer_exp_time = impart_data_draw["exp_day"] if impart_data_draw is not None else 0
    
    if user_root == '真·轮回道果':
        msg = "道友已是万世轮回之身！"
        await handle_send(bot, event, msg)
        await twolun.finish() 

    if user_root == '永恒道果' :
        msg = "道友已是永恒轮回之身！"
        await handle_send(bot, event, msg)
        await lunhui.finish()

    if user_root == '命运道果' :
        msg = "道友已可无限轮回！"
        await handle_send(bot, event, msg)
        await lunhui.finish()
        
    if user_root != '轮回道果':
        msg = "道友还未轮回过，请先进入千世轮回！"
        await handle_send(bot, event, msg)
        await twolun.finish() 
    
    if list_level_all.index(level) >= list_level_all.index(XiuConfig().twolun_min_level) and user_root == '轮回道果':
        exp = user_msg['exp']
        now_exp = exp - 100
        sql_message.updata_level(user_id, '江湖好手') #重置用户境界
        sql_message.update_levelrate(user_id, 0) #重置突破成功率
        sql_message.update_j_exp(user_id, now_exp) #重置用户修为
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        sql_message.updata_user_main_buff(user_id, 0) #重置用户主功法
        sql_message.updata_user_sub_buff(user_id, 0) #重置用户辅修功法
        sql_message.updata_user_sec_buff(user_id, 0) #重置用户神通
        sql_message.update_user_atkpractice(user_id, 0) #重置用户攻修等级
        sql_message.update_user_hppractice(user_id, 0) #重置用户元血等级
        sql_message.update_user_mppractice(user_id, 0) #重置用户灵海等级
        xiuxian_impart.use_impart_exp_day(impaer_exp_time, user_id)
        #重置用户虚神界修炼时间
        xiuxian_impart.update_impart_lv(user_id, 0)
        #重置虚神界等级
        sql_message.update_ls(user_id, user_info['stone'], 2)
        #重置用户灵石
        sql_message.update_root(user_id, 7) #更换灵根
        msg = f"万世道果集一身，脱出凡道入仙道，恭喜大能{user_name}万世轮回成功！"
        await handle_send(bot, event, msg)
        await twolun.finish()
    else:
        msg = f"道友境界未达要求，万世轮回的最低境界为{XiuConfig().twolun_min_level}！"
        await handle_send(bot, event, msg)
        await twolun.finish()
        
@resetting.handle(parameterless=[Cooldown(at_sender=False)])
async def resetting_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await resetting.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']


@threelun.handle(parameterless=[Cooldown(at_sender=False)])
async def threelun_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await threelun.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    impart_data_draw = await impart_check(user_id) 
    impaer_exp_time = impart_data_draw["exp_day"] if impart_data_draw is not None else 0
    
    if user_root == '永恒道果':
        msg = "道友已是永恒轮回之身！"
        await handle_send(bot, event, msg)
        await threelun.finish() 

    if user_root == '轮回道果':
        msg = "道友还未万世轮回，请先进入万世轮回！"
        await handle_send(bot, event, msg)
        await threelun.finish() 

    if user_root == '命运道果' :
        msg = "道友已可无限轮回！"
        await handle_send(bot, event, msg)
        await lunhui.finish()

    if user_root != '真·轮回道果':
        msg = "道友还未完成轮回，请先进入轮回！"
        await handle_send(bot, event, msg)
        await threelun.finish()         
        
    
    if list_level_all.index(level) >= list_level_all.index(XiuConfig().threelun_min_level) and user_root == '真·轮回道果':
        exp = user_msg['exp']
        now_exp = exp - 100
        sql_message.updata_level(user_id, '江湖好手') #重置用户境界
        sql_message.update_levelrate(user_id, 0) #重置突破成功率
        sql_message.update_j_exp(user_id, now_exp) #重置用户修为
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        sql_message.updata_user_main_buff(user_id, 0) #重置用户主功法
        sql_message.updata_user_sub_buff(user_id, 0) #重置用户辅修功法
        sql_message.updata_user_sec_buff(user_id, 0) #重置用户神通
        sql_message.update_user_atkpractice(user_id, 0) #重置用户攻修等级
        sql_message.update_user_hppractice(user_id, 0) #重置用户元血等级
        sql_message.update_user_mppractice(user_id, 0) #重置用户灵海等级
        xiuxian_impart.use_impart_exp_day(impaer_exp_time, user_id)
        #重置用户虚神界修炼时间
        xiuxian_impart.update_impart_lv(user_id, 0)
        #重置虚神界等级
        sql_message.update_ls(user_id, user_info['stone'], 2)
        #重置用户灵石
        sql_message.update_root(user_id, 8) #更换灵根
        msg = f"穿越千劫万难，证得不朽之身，恭喜大能{user_name}步入永恒之道，成就无上永恒！"
        await handle_send(bot, event, msg)
        await threelun.finish()
    else:
        msg = f"道友境界未达要求，永恒轮回的最低境界为{XiuConfig().threelun_min_level}！"
        await handle_send(bot, event, msg)
        await threelun.finish()
        
@Infinite_reincarnation.handle(parameterless=[Cooldown(at_sender=False)])
async def Infinite_reincarnation_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await Infinite_reincarnation.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    user_root = user_msg['root_type']
    list_level_all = list(jsondata.level_data().keys())
    level = user_info['level']
    impart_data_draw = await impart_check(user_id) 
    impaer_exp_time = impart_data_draw["exp_day"] if impart_data_draw is not None else 0 

    if user_root == '轮回道果':
        msg = "道友还未万世轮回，请先进入万世轮回！"
        await handle_send(bot, event, msg)
        await Infinite_reincarnation.finish() 

    if user_root == '真·轮回道果':
        msg = "道友还未永恒轮回，请先进入永恒轮回！"
        await handle_send(bot, event, msg)
        await Infinite_reincarnation.finish() 

    if user_root != '永恒道果' and user_root != '命运道果' :
        msg = "道友还未完成轮回，请先进入轮回！"
        await handle_send(bot, event, msg)
        await Infinite_reincarnation.finish()
    
    if list_level_all.index(level) >= list_level_all.index(XiuConfig().Infinite_reincarnation_min_level) and user_root == '永恒道果' or user_root == '命运道果':
        exp = user_msg['exp']
        now_exp = exp - 100
        sql_message.updata_level(user_id, '江湖好手') #重置用户境界
        sql_message.update_levelrate(user_id, 0) #重置突破成功率
        sql_message.update_j_exp(user_id, now_exp) #重置用户修为
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        sql_message.updata_user_main_buff(user_id, 0) #重置用户主功法
        sql_message.updata_user_sub_buff(user_id, 0) #重置用户辅修功法
        sql_message.updata_user_sec_buff(user_id, 0) #重置用户神通
        xiuxian_impart.use_impart_exp_day(impaer_exp_time, user_id)
        #重置用户虚神界修炼时间
        sql_message.update_root(user_id, 9) #更换灵根
        sql_message.updata_root_level(user_id, 1) #更新轮回等级
        msg = f"超越永恒，超脱命运，执掌因果轮回！恭喜大能{user_name}突破命运桎梏，成就无上命运道果！"
        await handle_send(bot, event, msg)
        await Infinite_reincarnation.finish()
    else:
        msg = f"道友境界未达要求，无限轮回的最低境界为{XiuConfig().Infinite_reincarnation_min_level}！"
        await handle_send(bot, event, msg)
        await Infinite_reincarnation.finish()
        
@resetting.handle(parameterless=[Cooldown(at_sender=False)])
async def resetting_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await resetting.finish()
        
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id) 
    user_name = user_msg['user_name']
    
                    
    if user_msg['level'] in ['感气境初期', '感气境中期', '感气境圆满']:
        exp = user_msg['exp']
        now_exp = exp
        sql_message.updata_level(user_id, '江湖好手') #重置用户境界
        sql_message.update_levelrate(user_id, 0) #重置突破成功率
        sql_message.update_j_exp(user_id, now_exp) #重置用户修为
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        msg = f"{user_name}现在是一介凡人了！！"
        await handle_send(bot, event, msg)
        await resetting.finish()
    else:
        msg = f"道友境界未达要求，自废修为的最低境界为感气境！"
        await handle_send(bot, event, msg)
        await resetting.finish()
        
