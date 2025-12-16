try:
    import ujson as json
except ImportError:
    import json
import re
import os
from pathlib import Path
import random
import asyncio
from datetime import datetime
from nonebot.typing import T_State
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from nonebot import require, on_command, on_fullmatch, get_bot
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GROUP_ADMIN,
    GROUP_OWNER,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    ActionFailed
)
from nonebot.permission import SUPERUSER
from nonebot.log import logger
from nonebot.params import CommandArg
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_base import clear_all_xiangyuan
from ..xiuxian_rift import create_rift
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, XiuxianJsonDate, OtherSet, 
    UserBuffDate, XIUXIAN_IMPART_BUFF
)
from ..xiuxian_config import XiuConfig, JsonConfig, convert_rank
from ..xiuxian_utils.utils import (
    check_user, number_to, get_msg_pic, handle_send
)
from ..xiuxian_utils.item_json import Items


items = Items()
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()

gm_command = on_command("神秘力量", permission=SUPERUSER, priority=10, block=True)
adjust_exp_command = on_command("修为调整", permission=SUPERUSER, priority=10, block=True)
gmm_command = on_command("轮回力量", permission=SUPERUSER, priority=10, block=True)
ccll_command = on_command("传承力量", permission=SUPERUSER, priority=10, block=True)
zaohua_xiuxian = on_command('造化力量', permission=SUPERUSER, priority=15, block=True)
cz = on_command('创造力量', permission=SUPERUSER, priority=15, block=True)
hmll = on_command("毁灭力量", priority=5, permission=SUPERUSER, block=True)
restate = on_command("重置状态", permission=SUPERUSER, priority=12, block=True)
set_xiuxian = on_command("启用修仙功能", aliases={'禁用修仙功能'}, permission=GROUP and (SUPERUSER | GROUP_ADMIN | GROUP_OWNER), priority=5, block=True)
set_private_chat = on_command("启用私聊功能", aliases={'禁用私聊功能'}, permission=SUPERUSER, priority=5, block=True)
super_help = on_command("修仙手册", aliases={"修仙管理"}, permission=SUPERUSER, priority=15, block=True)
xiuxian_updata_level = on_fullmatch('修仙适配', permission=SUPERUSER, priority=15, block=True)
clear_xiangyuan = on_command("清空仙缘", permission=SUPERUSER, priority=5, block=True)
xiuxian_novice = on_command('重置新手礼包', permission=SUPERUSER, priority=15,block=True)
create_new_rift = on_fullmatch("生成秘境", priority=5, permission=SUPERUSER, block=True)
do_work_cz = on_command("重置悬赏令", permission=SUPERUSER, priority=6, block=True)
training_reset = on_command("重置历练", permission=SUPERUSER, priority=6, block=True)
boss_reset = on_command("重置世界BOSS", permission=SUPERUSER, priority=6, block=True)
tower_reset = on_command("重置通天塔", permission=SUPERUSER, priority=5, block=True)

# GM加灵石
@gm_command.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gm_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """神秘力量 - 给玩家或全服发放灵石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：神秘力量 灵石数量\n：神秘力量 道号 灵石数量"
        await handle_send(bot, event, msg)
        await gm_command.finish()
        
    if len(arg_list) < 2:
        stone_num = str(arg_list[0])  # 灵石数
        nick_name = None
    else:
        stone_num = arg_list[1]  # 灵石数
        nick_name = arg_list[0]  # 道号

    give_stone_num = stone_num
    # 遍历Message对象，寻找艾特信息
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
        if give_user:
            sql_message.update_ls(give_qq, give_stone_num, 1)  # 增加用户灵石
            msg = f"共赠送{number_to(int(give_stone_num))}枚灵石给{give_user['user_name']}道友！"
            await handle_send(bot, event, msg)
            await gm_command.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await gm_command.finish()
    else:
        sql_message.update_ls_all(give_stone_num)
        msg = f"全服通告：赠送所有用户{number_to(int(give_stone_num))}灵石,请注意查收！"
        await handle_send(bot, event, msg)
        enabled_groups = JsonConfig().get_enabled_groups()
        for group_id in enabled_groups:
            bot = get_bot()
            if int(group_id) == event.group_id:
                continue
            try:
                if XiuConfig().img:
                    pic = await get_msg_pic(msg)
                    await bot.send_group_msg(group_id=int(group_id), message=MessageSegment.image(pic))
                else:
                    await bot.send_group_msg(group_id=int(group_id), message=msg)
            except ActionFailed:  # 发送群消息失败
                continue
    await gm_command.finish()

# GM加思恋结晶
@ccll_command.handle(parameterless=[Cooldown(cd_time=1.4)])
async def ccll_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承力量 - 给玩家或全服发放思恋结晶"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：传承力量 思恋结晶数量\n：传承力量 道号 思恋结晶数量"
        await handle_send(bot, event, msg)
        await ccll_command.finish()
        
    if len(arg_list) < 2:
        stone_num = str(arg_list[0])  # 思恋结晶数
        nick_name = None
    else:
        stone_num = arg_list[1]  # 思恋结晶数
        nick_name = arg_list[0]  # 道号

    give_stone_num = stone_num
    # 遍历Message对象，寻找艾特信息
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
        if give_user:
            xiuxian_impart.update_stone_num(give_stone_num, give_qq, 1)  # 增加用户思恋结晶
            msg = f"共赠送{number_to(int(give_stone_num))}枚思恋结晶给{give_user['user_name']}道友！"
            await handle_send(bot, event, msg)
            await ccll_command.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await ccll_command.finish()
    else:
        xiuxian_impart.update_impart_stone_all(give_stone_num)
        msg = f"全服通告：赠送所有用户{number_to(int(give_stone_num))}思恋结晶,请注意查收！"
        await handle_send(bot, event, msg)
        enabled_groups = JsonConfig().get_enabled_groups()
        for group_id in enabled_groups:
            bot = get_bot()
            if int(group_id) == event.group_id:
                continue
            try:
                if XiuConfig().img:
                    pic = await get_msg_pic(msg)
                    await bot.send_group_msg(group_id=int(group_id), message=MessageSegment.image(pic))
                else:
                    await bot.send_group_msg(group_id=int(group_id), message=msg)
            except ActionFailed:  # 发送群消息失败
                continue
    await ccll_command.finish()

@adjust_exp_command.handle(parameterless=[Cooldown(cd_time=1.4)])
async def adjust_exp_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """修为调整 - 增加或减少玩家修为"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    
    if not args or len(arg_list) < 2:
        msg = f"请输入正确指令！例如：修为调整 道号 修为"
        await handle_send(bot, event, msg)
        await adjust_exp_command.finish()
        
    if len(arg_list) < 2:
        exp_num = str(arg_list[0])  # 修为数量
        nick_name = None
    else:
        exp_num = arg_list[1]  # 修为数量
        nick_name = arg_list[0]  # 道号

    # 解析修为数量（支持正负数）
    try:
        give_exp_num = int(exp_num)
    except ValueError:
        msg = f"请输入有效的修为数量！"
        await handle_send(bot, event, msg)
        await adjust_exp_command.finish()

    # 遍历Message对象，寻找艾特信息
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
        if give_user:
            current_exp = give_user['exp']
            
            # 更新用户修为
            if give_exp_num > 0:
                sql_message.update_exp(give_qq, give_exp_num)
                msg = f"共增加{number_to(give_exp_num)}修为给{give_user['user_name']}道友！"
            else:
                sql_message.update_j_exp(give_qq, abs(give_exp_num))
                msg = f"共减少{number_to(abs(give_exp_num))}修为给{give_user['user_name']}道友！"
            
            await handle_send(bot, event, msg)
            await adjust_exp_command.finish()
        else:
            msg = f"对方未踏入修仙界，不可操作！"
            await handle_send(bot, event, msg)
            await adjust_exp_command.finish()    
    await adjust_exp_command.finish()

@zaohua_xiuxian.handle(parameterless=[Cooldown(cd_time=1.4)])
async def zaohua_xiuxian_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """造化力量 - 修改玩家境界"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：造化力量 道号 境界名"
        await handle_send(bot, event, msg)
        await zaohua_xiuxian.finish()
    if len(arg_list) < 2:
        jj_name = arg_list[0]
    else:
        jj_name = arg_list[1]
        
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
    else:
        give_user = sql_message.get_user_info_with_name(arg_list[0])
        give_qq = give_user['user_id']
    if give_user:
        level = jj_name
        if len(jj_name) == 5:
            level = jj_name
        elif len(jj_name) == 3:
            level = (jj_name + '圆满')
        if convert_rank(level)[0] is None:
            msg = f"境界错误，请输入正确境界名！"
            await handle_send(bot, event, msg)
            await zaohua_xiuxian.finish()
        max_exp = int(jsondata.level_data()[level]["power"])
        exp = give_user['exp']
        now_exp = exp - 100
        sql_message.update_j_exp(give_qq, now_exp) #重置用户修为
        sql_message.update_exp(give_qq, max_exp)  # 更新修为
        sql_message.updata_level(give_qq, level)  # 更新境界
        sql_message.update_user_hp(give_qq)  # 重置用户状态
        sql_message.update_power2(give_qq)  # 更新战力
        msg = f"{give_user['user_name']}道友的境界已变更为{level}！"
        await handle_send(bot, event, msg)
        await zaohua_xiuxian.finish()
    else:
        msg = f"对方未踏入修仙界，不可修改！"
        await handle_send(bot, event, msg)
        await zaohua_xiuxian.finish()

@gmm_command.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gmm_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """轮回力量 - 修改玩家灵根"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：轮回力量 道号 8(1为混沌,2为融合,3为超,4为龙,5为天,6为千世,7为万世,8为永恒,9为命运)"
        await handle_send(bot, event, msg)
        await gmm_command.finish()
    if len(arg_list) < 2:
        root_name_list = arg_list[0]
    else:
        root_name_list = arg_list[1]
        
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
    else:
        give_user = sql_message.get_user_info_with_name(arg_list[0])
        give_qq = give_user['user_id']
    if give_user:
        root_name = sql_message.update_root(give_qq, root_name_list)
        sql_message.update_power2(give_qq)
        msg = f"{give_user['user_name']}道友的灵根已变更为{root_name}！"
        await handle_send(bot, event, msg)
        await gmm_command.finish()
    else:
        msg = f"对方未踏入修仙界，不可修改！"
        await handle_send(bot, event, msg)
        await gmm_command.finish()

@cz.handle(parameterless=[Cooldown(cd_time=1.4)])
async def cz_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """创造力量 - 给玩家或全服发放物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = f"请输入正确指令！例如：创造力量 物品名 数量 [玩家名]\n创造力量 物品名 数量 all (全服发放)"
        await handle_send(bot, event, msg)
        await cz.finish()
    
    goods_name = args[0]
    try:
        quantity = int(args[1])
        if len(args) > 2:
            target = args[2]
        else:
            target = None
    except ValueError:
        msg = "数量必须是整数！"
        await handle_send(bot, event, msg)
        await cz.finish()
    
    # 查找物品ID
    goods_id = None
    for item_id, item_info in items.items.items():
        if goods_name == item_info['name']:
            goods_id = item_id
            break
    
    if not goods_id:
        msg = f"物品 {goods_name} 不存在！"
        await handle_send(bot, event, msg)
        await cz.finish()
    
    # 获取物品类型
    item_info = items.get_data_by_item_id(goods_id)
    goods_type = item_info['type']
    
    # 处理发放目标
    if target and target.lower() == 'all':
        # 全服发放
        all_users = sql_message.get_all_user_id()
        success_count = 0
        
        for user_id in all_users:
            try:
                sql_message.send_back(user_id, goods_id, goods_name, goods_type, quantity, 1)
                success_count += 1
            except Exception as e:
                logger.error(f"给用户 {user_id} 发放物品失败: {e}")
        
        msg = f"全服发放成功！共向 {success_count} 名玩家发放了 {goods_name} x{quantity}"
        
    elif target:
        # 指定玩家发放
        user_info = sql_message.get_user_info_with_name(target)
        if not user_info:
            msg = f"玩家 {target} 不存在！"
            await handle_send(bot, event, msg)
            await cz.finish()
        
        sql_message.send_back(user_info['user_id'], goods_id, goods_name, goods_type, quantity, 1)
        msg = f"成功向 {target} 发放 {goods_name} x{quantity}"
    
    else:
        # 默认给发送者
        is_user, user_info, _ = check_user(event)
        if not is_user:
            msg = "您尚未加入修仙界！"
            await handle_send(bot, event, msg)
            await cz.finish()
        
        sql_message.send_back(user_info['user_id'], goods_id, goods_name, goods_type, quantity, 1)
        msg = f"成功向您发放 {goods_name} x{quantity}"
    
    await handle_send(bot, event, msg)
    await cz.finish()

@hmll.handle(parameterless=[Cooldown(cd_time=1.4)])
async def hmll_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """毁灭力量 - 扣除玩家或全服的物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    args = args.extract_plain_text().split()
    
    if len(args) < 2:
        msg = f"请输入正确指令！例如：毁灭力量 物品名 数量 [玩家名]\n毁灭力量 物品名 数量 all (全服扣除)"
        await handle_send(bot, event, msg)
        await hmll.finish()
    
    goods_name = args[0]
    try:
        quantity = int(args[1])
        if len(args) > 2:
            target = args[2]
        else:
            target = None
    except ValueError:
        msg = "数量必须是整数！"
        await handle_send(bot, event, msg)
        await hmll.finish()
    
    # 查找物品ID
    goods_id = None
    for item_id, item_info in items.items.items():
        if goods_name == item_info['name']:
            goods_id = item_id
            break
    
    if not goods_id:
        msg = f"物品 {goods_name} 不存在！"
        await handle_send(bot, event, msg)
        await hmll.finish()
    
    # 处理扣除目标
    if target and target.lower() == 'all':
        # 全服扣除
        all_users = sql_message.get_all_user_id()
        success_count = 0
        
        for user_id in all_users:
            try:
                # 检查玩家是否有该物品
                back_msg = sql_message.get_back_msg(user_id)
                has_item = False
                for item in back_msg:
                    if item['goods_name'] == goods_name:
                        has_item = True
                        break
                
                if has_item:
                    sql_message.update_back_j(user_id, goods_id, num=quantity)
                    success_count += 1
            except Exception as e:
                logger.error(f"扣除用户 {user_id} 物品失败: {e}")
        
        msg = f"全服扣除成功！共从 {success_count} 名玩家扣除了 {goods_name} x{quantity}"
    
    elif target:
        # 指定玩家扣除
        user_info = sql_message.get_user_info_with_name(target)
        if not user_info:
            msg = f"玩家 {target} 不存在！"
            await handle_send(bot, event, msg)
            await hmll.finish()
        
        # 检查玩家是否有该物品
        back_msg = sql_message.get_back_msg(user_info['user_id'])
        has_item = False
        for item in back_msg:
            if item['goods_name'] == goods_name:
                has_item = True
                break
        
        if not has_item:
            msg = f"玩家 {target} 没有 {goods_name}！"
            await handle_send(bot, event, msg)
            await hmll.finish()
        
        sql_message.update_back_j(user_info['user_id'], goods_id, num=quantity)
        msg = f"成功从 {target} 扣除 {goods_name} x{quantity}"
    
    else:
        # 默认扣除发送者
        is_user, user_info, _ = check_user(event)
        if not isUser:
            msg = "您尚未加入修仙界！"
            await handle_send(bot, event, msg)
            await hmll.finish()
        
        # 检查是否有该物品
        back_msg = sql_message.get_back_msg(user_info['user_id'])
        has_item = False
        for item in back_msg:
            if item['goods_name'] == goods_name:
                has_item = True
                break
        
        if not has_item:
            msg = f"您没有 {goods_name}！"
            await handle_send(bot, event, msg)
            await hmll.finish()
        
        sql_message.update_back_j(user_info['user_id'], goods_id, num=quantity)
        msg = f"成功从您这里扣除 {goods_name} x{quantity}"
    
    await handle_send(bot, event, msg)
    await hmll.finish()

@restate.handle(parameterless=[Cooldown(cd_time=1.4)])
async def restate_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """重置用户状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await restate.finish()
    give_qq = None  # 艾特的时候存到这里
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if not args:
        sql_message.restate()
        sql_message.update_all_users_stamina(XiuConfig().max_stamina, XiuConfig().max_stamina)
        msg = f"所有用户信息重置成功！"
        await handle_send(bot, event, msg)
        await restate.finish()
    else:
        nick_name = args.extract_plain_text().split()[0]
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    if give_qq:
        sql_message.restate(give_qq)
        sql_message.update_user_stamina(give_qq, XiuConfig().max_stamina, 1)  # 增加体力
        msg = f"{give_qq}用户信息重置成功！"
        await handle_send(bot, event, msg)
        await restate.finish()
    else:
        msg = f"对方未踏入修仙界！"
        await handle_send(bot, event, msg)
        await restate.finish()

@set_xiuxian.handle()
async def open_xiuxian_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """群修仙开关配置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_msg = str(event.message)
    group_id = str(event.group_id)
    conf_data = JsonConfig().read_data()

    if "启用" in group_msg:
        if group_id not in conf_data["group"]:
            msg = "当前群聊修仙模组已启用，请勿重复操作！"
            await handle_send(bot, event, msg)
            await set_xiuxian.finish()
        JsonConfig().write_data(2, group_id)
        msg = "当前群聊修仙基础模组已启用，快发送 我要修仙 加入修仙世界吧！"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()

    elif "禁用" in group_msg:
        if group_id in conf_data["group"]:
            msg = "当前群聊修仙模组已禁用，请勿重复操作！"
            await handle_send(bot, event, msg)
            await set_xiuxian.finish()
        JsonConfig().write_data(1, group_id)
        msg = "当前群聊修仙基础模组已禁用！"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()
    else:
        msg = "指令错误，请输入：启用修仙功能/禁用修仙功能"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()

@set_private_chat.handle()
async def set_private_chat_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """私聊功能开关配置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = str(event.message)
    conf_data = JsonConfig().read_data()

    if "启用" in msg:
        if conf_data["private_enabled"]:
            msg = "私聊修仙功能已启用，请勿重复操作！"
        else:
            JsonConfig().write_data(3)
            msg = "私聊修仙功能已启用，所有用户现在可以在私聊中使用修仙命令！"
    elif "禁用" in msg:
        if not conf_data["private_enabled"]:
            msg = "私聊修仙功能已禁用，请勿重复操作！"
        else:
            JsonConfig().write_data(4)
            msg = "私聊修仙功能已禁用，所有用户的私聊修仙功能已关闭！"
    else:
        msg = "指令错误，请输入：启用私聊功能/禁用私聊功能"

    await handle_send(bot, event, msg)
    await set_private_chat.finish()
    
@xiuxian_updata_level.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xiuxian_updata_level_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """将修仙2的境界适配到修仙2魔改"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    level_dict = {
        "搬血境": "感气境",
        "洞天境": "练气境",
        "化灵境": "筑基境",
        "铭纹境": "结丹境",
        "列阵境": "金丹境",
        "尊者境": "元神境",
        "神火境": "化神境",
        "真一境": "炼神境",
        "圣祭境": "返虚境",
        "天神境": "大乘境",
        "真仙境": "微光境",
        "仙王境": "星芒境",
        "准帝境": "月华境",
        "仙帝境": "耀日境"
    }
    
    # 获取所有用户
    all_users = sql_message.get_all_user_id()
    adapted_count = 0
    success_count = 0
    failed_count = 0
    
    for user in all_users:
        user_info = sql_message.get_user_info_with_id(user)
        user_id = user_info['user_id']
        old_level = user_info['level']
        try:
            
            if old_level.endswith(('初期', '中期', '圆满')):
                base_level = old_level[:-2]
                stage = old_level[-2:]
            else:
                base_level = old_level
                stage = ""
            
            # 进行境界适配
            if base_level in level_dict:
                new_level = level_dict[base_level] + stage
                sql_message.updata_level(user_id=user_id, level_name=new_level)
                adapted_count += 1
                
                # 记录适配日志
                logger.info(f"境界适配成功：用户 {user_id} 从【{old_level}】适配为【{new_level}】")
                
            else:
                # 如果不在适配字典中，跳过
                success_count += 1
                logger.info(f"境界无需适配：用户 {user_id} 境界【{old_level}】不在适配范围内")
                
        except Exception as e:
            failed_count += 1
            logger.error(f"境界适配失败：用户 {user_id} 错误：{str(e)}")
    
    # 构建结果消息
    msg = f'境界适配完成！\n成功适配：{adapted_count} 个用户\n适配失败：{failed_count} 个用户\n无需适配：{success_count} 个用户'
    
    if adapted_count >= 0:
        msg += f'\n\n适配规则：\n'
        for old, new in level_dict.items():
            msg += f"{old} → {new}\n"
    
    await handle_send(bot, event, msg)
    await xiuxian_updata_level.finish()

@clear_xiangyuan.handle(parameterless=[Cooldown(cd_time=1.4)])
async def clear_xiangyuan_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = await clear_all_xiangyuan()
    await handle_send(bot, event, msg)
    await clear_xiangyuan.finish()

@xiuxian_novice.handle(parameterless=[Cooldown(cd_time=1.4)])
async def xiuxian_novice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置新手礼包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    sql_message.novice_remake()
    msg = "新手礼包重置成功，所有玩家可以重新领取新手礼包！"
    await handle_send(bot, event, msg)
    await xiuxian_novice.finish()

@create_new_rift.handle(parameterless=[Cooldown(cd_time=1.4)])
async def create_new_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """生成秘境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await create_rift(bot, event)

@do_work_cz.handle(parameterless=[Cooldown(cd_time=1.4)])
async def do_work_cz_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置所有用户的悬赏令"""
    from ..xiuxian_work import count
    sql_message.reset_work_num(count)
    msg = "用户悬赏令刷新次数重置成功"
    await handle_send(bot, event, msg)
    await do_work_cz.finish()

@training_reset.handle(parameterless=[Cooldown(cd_time=1.4)])
async def training_reset_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置所有用户的历练"""
    from ..xiuxian_training import training_reset_limits
    training_reset_limits()
    msg = "用户历练状态重置成功"
    await handle_send(bot, event, msg)
    await training_reset.finish()

@tower_reset.handle(parameterless=[Cooldown(cd_time=1.4)])
async def tower_reset_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置所有用户的通天塔层数"""
    from ..xiuxian_tower import reset_tower_floors
    await reset_tower_floors()  # 重置通天塔层数
    msg = "用户通天塔层数重置成功"
    await handle_send(bot, event, msg)
    await tower_reset.finish()

@boss_reset.handle(parameterless=[Cooldown(cd_time=1.4)])
async def boss_reset_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置所有用户的世界BOSS额度"""
    from ..xiuxian_boss import set_boss_limits_reset
    await set_boss_limits_reset()
    msg = "用户世界BOSS额度重置成功"
    await handle_send(bot, event, msg)
    await boss_reset.finish()

@super_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def super_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """修仙管理帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    help_msg = """
【修仙管理手册】⚡⚡⚡
======================
🌟 管理员专用指令

⚡ 资源管理：
→ 神秘力量 [数量] - 全服发放灵石
→ 神秘力量 [道号] [数量] - 给指定用户发灵石
- 可以负数来扣灵石
→ 传承力量 [数量] - 全服发放思恋结晶
→ 传承力量 [道号] [数量] - 给指定用户发思恋结晶
- 可以负数来扣思恋结晶
→ 创造力量 [物品ID/名称] [数量] - 给自己发物品
→ 创造力量 [物品ID/名称] [数量] all - 全服发物品
→ 创造力量 [物品ID/名称] [数量] [道号] - 给指定用户发物品
→ 毁灭力量 [物品ID/名称] [数量] - 给自己扣物品
→ 毁灭力量 [物品ID/名称] [数量] all - 全服扣物品
→ 毁灭力量 [物品ID/名称] [数量] [道号] - 给指定用户扣物品

⚡ 境界管理：
→ 造化力量 [道号] [境界] - 修改用户境界
→ 轮回力量 [道号] [1-9] - 修改用户灵根
   (1混沌 2融合 3超 4龙 5天 6千世 7万世 8永恒 9命运)
→ 修为调整 [修为数] - 全服发修为
→ 修为调整 [道号] [修为数] - 给指定用户发修为
- 可以负数来扣修为

⚡ 世界BOSS管理：
→ 世界BOSS生成 [数量] - 生成随机境界BOSS
→ 世界BOSS指定生成 [境界] [名称] - 生成指定BOSS
→ 世界BOSS全部生成 - 一键生成所有境界BOSS
→ 天罚世界BOSS [编号] - 删除指定BOSS
→ 天罚全部世界BOSS - 清空所有BOSS

⚡ 补偿系统管理：
→ 新增补偿 [ID] [时间] [物品] [原因] - 创建新补偿
→ 删除补偿 [ID] - 删除指定补偿
→ 补偿列表 - 查看所有补偿
→ 清空补偿 - 清空所有补偿数据

⚡ 礼包系统管理：
→ 新增礼包 [ID] [时间] [物品] [原因] - 创建新礼包
→ 删除礼包 [ID] - 删除指定礼包
→ 礼包列表 - 查看所有礼包
→ 清空礼包 - 清空所有礼包数据

⚡ 兑换码系统管理：
→ 新增兑换码 [兑换码] [时间] [物品] [使用次数] - 创建新兑换码
→ 删除兑换码 [兑换码] - 删除指定兑换码
→ 兑换码列表 - 查看所有兑换码
→ 清空兑换码 - 清空所有兑换码数据

⚡ 邀请系统管理：
→ 邀请奖励设置 [门槛] [物品] - 设置邀请奖励
→ 邀请奖励列表 - 查看所有邀请奖励设置
→ 邀请奖励删除 [门槛] - 删除指定门槛奖励
→ 邀请奖励清空 - 清空所有邀请奖励

⚡ 系统管理：
→ 重置状态 - 重置所有用户状态
→ 重置状态 [道号] - 重置指定用户状态
→ 修仙适配 - 适配修仙2的境界到修仙2魔改版
→ 装备检测 - 检测用户背包异常数据并修复，装备丢失/绑定数量异常
→ 启用修仙功能 - 开启修仙功能（默认全部开启）
→ 禁用修仙功能 - 关闭修仙功能
→ 启用私聊功能 - 开启私聊修仙
→ 禁用私聊功能 - 关闭私聊修仙
→ 启用自动选择灵根 - 开启自动灵根
→ 禁用自动选择灵根 - 关闭自动灵根

⚡ 交易管理：
→ 系统仙肆上架 物品名称 [价格] [数量] - 不带数量为无限
→ 系统仙肆下架 [物品ID/名称] [数量] - 不带数量为1个
→ 清空仙肆 - 清空所有道友的物品并退回
→ 清空鬼市 - 清空所有道友的摆摊和求购
→ 开启拍卖 - 开启拍卖
→ 结束拍卖 - 结束拍卖
→ 封闭拍卖 - 禁止自动开启拍卖
→ 解封拍卖 - 取消禁止

⚡ 功能管理：
→ 清空仙缘 - 清除所有未领取仙缘
→ 重置世界BOSS - 重置所以玩家世界BOSS额度
→ 重置悬赏令 - 重置所以玩家悬赏令
→ 重置通天塔 - 重置玩家通天塔层数
→ 重置历练 - 重置当前历练状态
→ 重置幻境 - 重置当前幻境数据
→ 清空幻境 - 仅清空玩家数据
→ 重置新手礼包

→ 更新日志 - 获取版本日志
→ 版本更新 - 指定版本号更新/latest：更新最新版本
→ 版本查询 - 获取最近发布的版本
→ 检测更新 - 检测是否需要更新
→ bot信息 - 获取机器人和修仙数据
→ 系统信息 - 获取系统信息
→ ping测试 - 测试网络延迟
→ GitHub - liyw0205/nonebot_plugin_xiuxian_2_pmv

======================
注：[]表示必填参数，()表示可选参数
    """

    await handle_send(bot, event, help_msg)
    await super_help.finish()