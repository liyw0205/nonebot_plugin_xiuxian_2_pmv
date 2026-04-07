try:
    import ujson as json
except ImportError:
    import json
import re
import os
import random
import requests
import asyncio
from nonebot.compat import model_dump
from pathlib import Path
from typing import Any
from nonebot.compat import model_dump
from datetime import datetime
from nonebot.typing import T_State
from nonebot.permission import SUPERUSER
from nonebot.log import logger
from nonebot.params import CommandArg, EventPlainText
from nonebot import require, on_command, on_message, get_bot
from nonebot.rule import Rule
from nonebot.matcher import Matcher
from nonebot.adapters import Event as BaseEvent

from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    MessageEvent,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    is_channel_event,
)

from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_base import clear_all_xiangyuan
from ..xiuxian_rift import create_rift
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, XiuxianJsonDate, OtherSet, 
    UserBuffDate, XIUXIAN_IMPART_BUFF, migrate_user_id_to_openid, migrate_single_user_id, swap_two_user_ids
)
from ..xiuxian_config import XiuConfig, JsonConfig, convert_rank
from ..xiuxian_utils.utils import (
    check_user, number_to, get_msg_pic, handle_send, send_msg_handler,
    generate_command, _impersonating_users, handle_pic_msg_send, handle_send_md
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
hmll = on_command("毁灭力量", permission=SUPERUSER, priority=6, block=True)
restate = on_command("重置状态", permission=SUPERUSER, priority=12, block=True)
set_xiuxian = on_command("启用修仙功能", aliases={'禁用修仙功能'}, permission=SUPERUSER, priority=5, block=True)
set_private_chat = on_command("启用私聊功能", aliases={'禁用私聊功能'}, permission=SUPERUSER, priority=5, block=True)
set_auto_root = on_command("开启自动灵根", aliases={'关闭自动灵根'}, permission=SUPERUSER, priority=5, block=True)
set_auto_sect_name = on_command("启用自动宗名", aliases={'禁用自动宗名'}, permission=SUPERUSER, priority=5, block=True)
super_help = on_command("修仙手册", aliases={"修仙管理"}, permission=SUPERUSER, priority=15, block=True)
xiuxian_updata_level = on_command('修仙适配', permission=SUPERUSER, priority=15, block=True)
clear_xiangyuan = on_command("清空仙缘", permission=SUPERUSER, priority=5, block=True)
xiuxian_novice = on_command('重置新手礼包', permission=SUPERUSER, priority=15,block=True)
create_new_rift = on_command("生成秘境", permission=SUPERUSER, priority=6, block=True)
do_work_cz = on_command("重置悬赏令", permission=SUPERUSER, priority=6, block=True)
training_reset = on_command("重置历练", permission=SUPERUSER, priority=6, block=True)
boss_reset = on_command("重置世界BOSS", permission=SUPERUSER, priority=6, block=True)
tower_reset = on_command("重置通天塔", permission=SUPERUSER, priority=5, block=True)
items_refresh = on_command("重载items", permission=SUPERUSER, priority=5, block=True)
blackhouse = on_command("小黑屋", permission=SUPERUSER, priority=10, block=True)
unblackhouse = on_command("解除小黑屋", aliases={"放出小黑屋", "解禁"}, permission=SUPERUSER, priority=10, block=True)
view_blackhouse = on_command("查看小黑屋", aliases={"小黑屋列表"}, permission=SUPERUSER, priority=10, block=True)
impersonate_user_command = on_command("用户伪装", permission=SUPERUSER, priority=5, block=True)
dm_command = on_command("dm", permission=SUPERUSER, priority=5, block=True)
migrate_qqid_cmd = on_command("转换QQID", permission=SUPERUSER, priority=5, block=True)
update_id_cmd = on_command("ID更新", permission=SUPERUSER, priority=5, block=True)
swap_id_cmd = on_command("ID交换", permission=SUPERUSER, priority=5, block=True)
parse_event_cmd = on_command("消息信息", permission=SUPERUSER, priority=100, block=True)

# GM加灵石
@gm_command.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gm_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """神秘力量 [数量] [目标]"""
    bot, _ = await assign_bot(bot=bot, event=event)
    
    plain_args = args.extract_plain_text().strip().split()
    if not plain_args:
        await handle_send(bot, event, "用法：神秘力量 数量 [all/道号]\n示例：神秘力量 10000\n神秘力量 -5000 all")
        return

    # 数量必填，且是第一个参数
    try:
        amount_str = plain_args[0]
        amount = int(amount_str)
    except ValueError:
        await handle_send(bot, event, "数量必须是整数（支持负数）")
        return

    # 目标解析（从第二个参数开始）
    target = None
    if len(plain_args) >= 2:
        target = plain_args[1]

    # 优先找艾特
    at_qq = None
    for seg in args:
        if seg.type == "at":
            at_qq = seg.data.get("qq", "")
            break

    if at_qq:
        user_id = at_qq
        user = sql_message.get_user_info_with_id(user_id)
        if not user:
            await handle_send(bot, event, "该艾特用户尚未踏入修仙界")
            return
        target_name = user['user_name']
    elif target == "all":
        user_id = None      # 代表全服
        target_name = "全服"
    elif target:
        user = sql_message.get_user_info_with_name(target)
        if not user:
            await handle_send(bot, event, f"未找到道号为 {target} 的修士")
            return
        user_id = user['user_id']
        target_name = user['user_name']
    else:
        # 默认给自己
        _, user, _ = check_user(event)
        if not user:
            await handle_send(bot, event, "您尚未踏入修仙界，无法给自己发放")
            return
        user_id = user['user_id']
        target_name = user['user_name']

    # 执行发放/扣除
    if user_id is None:  # 全服
        sql_message.update_ls_all(amount)
        action = "增加" if amount > 0 else "扣除"
        msg = f"全服通告：{action}{number_to(abs(amount))}枚灵石，请注意查收！"
        await handle_send(bot, event, msg)
        # 全服广播（原有逻辑）
        enabled_groups = JsonConfig().get_enabled_groups()
        for gid in enabled_groups:
            if str(gid) == str(event.group_id):
                continue
            try:
                if XiuConfig().img:
                    pic = await get_msg_pic(msg)
                    await bot.send_group_msg(group_id=int(gid), message=MessageSegment.image(pic))
                else:
                    await bot.send_group_msg(group_id=int(gid), message=msg)
            except:
                pass
    else:  # 单人
        key = 1 if amount > 0 else 2
        sql_message.update_ls(user_id, abs(amount), key)
        action = "赠送" if amount > 0 else "扣除"
        msg = f"成功{action}{number_to(abs(amount))}枚灵石给 {target_name} 道友！"
        await handle_send(bot, event, msg)

# GM加思恋结晶
@ccll_command.handle(parameterless=[Cooldown(cd_time=1.4)])
async def ccll_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """传承力量 [数量] [目标]"""
    bot, _ = await assign_bot(bot=bot, event=event)
    
    plain_args = args.extract_plain_text().strip().split()
    if not plain_args:
        await handle_send(bot, event, "用法：传承力量 数量 [all/道号]\n示例：传承力量 888 all")
        return

    try:
        amount = int(plain_args[0])
    except ValueError:
        await handle_send(bot, event, "数量必须是整数（支持负数）")
        return

    target = plain_args[1] if len(plain_args) >= 2 else None

    at_qq = None
    for seg in args:
        if seg.type == "at":
            at_qq = seg.data.get("qq", "")
            break

    if at_qq:
        user_id = at_qq
        user = sql_message.get_user_info_with_id(user_id)
        if not user:
            await handle_send(bot, event, "该用户尚未踏入修仙界")
            return
        target_name = user['user_name']
    elif target == "all":
        user_id = None
        target_name = "全服"
    elif target:
        user = sql_message.get_user_info_with_name(target)
        if not user:
            await handle_send(bot, event, f"未找到道号 {target}")
            return
        user_id = user['user_id']
        target_name = user['user_name']
    else:
        _, user, _ = check_user(event)
        if not user:
            await handle_send(bot, event, "您尚未加入修仙界")
            return
        user_id = user['user_id']
        target_name = user['user_name']

    if user_id is None:  # 全服
        xiuxian_impart.update_impart_stone_all(amount)
        action = "增加" if amount > 0 else "扣除"
        msg = f"全服通告：{action}{number_to(abs(amount))}枚思恋结晶，请查收！"
    else:
        key = 1 if amount > 0 else 2
        xiuxian_impart.update_stone_num(abs(amount), user_id, key)
        action = "赠送" if amount > 0 else "扣除"
        msg = f"成功{action}{number_to(abs(amount))}枚思恋结晶给 {target_name}！"

    await handle_send(bot, event, msg)

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
    """
    造化力量 境界名 [道号]
    造化力量 境界名    ← 默认给自己
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    plain_text = args.extract_plain_text().strip()
    if not plain_text:
        await handle_send(bot, event, "用法：造化力量 境界名 [道号]\n示例：造化力量 化神境圆满\n造化力量 祭道境 @某人")
        return

    parts = plain_text.split()
    level_name = parts[0]

    # 目标解析
    target_user = None
    target_qq = None

    # 优先找艾特
    for seg in args:
        if seg.type == "at":
            target_qq = seg.data.get("qq", "")
            break

    if target_qq:
        target_user = sql_message.get_user_info_with_id(target_qq)
    elif len(parts) >= 2:
        # 最后一个参数视为道号
        dao_name = parts[-1]
        target_user = sql_message.get_user_info_with_name(dao_name)
        if target_user:
            target_qq = target_user['user_id']
    else:
        # 默认给自己
        _, user, _ = check_user(event)
        if user:
            target_user = user
            target_qq = user['user_id']

    if not target_user or not target_qq:
        await handle_send(bot, event, "未找到目标用户（或对方未踏入修仙界）")
        return

    # 境界处理
    level = level_name
    if len(level_name) == 3:
        level = level_name + '圆满'
    # elif len(level_name) == 5:  # 已经是完整境界名
    #     pass

    rank_info = convert_rank(level)
    if rank_info[0] is None:
        await handle_send(bot, event, f"境界「{level_name}」不存在或格式错误")
        return

    max_exp = int(jsondata.level_data()[level]["power"])
    # 重置修为到该境界满经验
    sql_message.update_j_exp(target_qq, target_user['exp'] - 100)   # 清掉多余修为
    sql_message.update_exp(target_qq, max_exp)
    sql_message.updata_level(target_qq, level)
    sql_message.update_user_hp(target_qq)
    sql_message.update_power2(target_qq)

    msg = f"已将 {target_user['user_name']} 的境界变更为 【{level}】！"
    await handle_send(bot, event, msg)

@gmm_command.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gmm_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    轮回力量 灵根编号 [道号]
    轮回力量 8          ← 默认给自己改成永恒
    轮回力量 3 @某人
    灵根编号说明：
    1混沌 2融合 3超 4龙 5天 6千世 7万世 8永恒 9命运
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    plain_text = args.extract_plain_text().strip()
    if not plain_text:
        await handle_send(bot, event,
            "用法：轮回力量 灵根编号 [道号]\n"
            "示例：轮回力量 8\n"
            "轮回力量 3 @玩家\n"
            "编号：1混沌 2融合 3超 4龙 5天 6千世 7万世 8永恒 9命运")
        return

    parts = plain_text.split()
    try:
        root_id = int(parts[0])
        if root_id < 1 or root_id > 9:
            raise ValueError
    except:
        await handle_send(bot, event, "第一个参数必须是1~9的整数（灵根编号）")
        return

    # 目标解析
    target_user = None
    target_qq = None

    # 优先艾特
    for seg in args:
        if seg.type == "at":
            target_qq = seg.data.get("qq", "")
            break

    if target_qq:
        target_user = sql_message.get_user_info_with_id(target_qq)
    elif len(parts) >= 2:
        dao_name = parts[-1]
        target_user = sql_message.get_user_info_with_name(dao_name)
        if target_user:
            target_qq = target_user['user_id']
    else:
        # 默认给自己
        _, user, _ = check_user(event)
        if user:
            target_user = user
            target_qq = user['user_id']

    if not target_user or not target_qq:
        await handle_send(bot, event, "未找到目标用户（或对方未踏入修仙界）")
        return

    # 执行修改
    new_root = sql_message.update_root(target_qq, str(root_id))
    sql_message.update_power2(target_qq)

    msg = f"已将 {target_user['user_name']} 的灵根变更为 【{new_root}】！"
    await handle_send(bot, event, msg)

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
                sql_message.send_back(user_id, goods_id, goods_name, goods_type, quantity)
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
        
        sql_message.send_back(user_info['user_id'], goods_id, goods_name, goods_type, quantity)
        msg = f"成功向 {target} 发放 {goods_name} x{quantity}"
    
    else:
        # 默认给发送者
        is_user, user_info, _ = check_user(event)
        if not is_user:
            msg = "您尚未加入修仙界！"
            await handle_send(bot, event, msg)
            await cz.finish()
        
        sql_message.send_back(user_info['user_id'], goods_id, goods_name, goods_type, quantity)
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
        if not is_user: # Corrected variable name from isUser to is_user
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

@set_xiuxian.handle(parameterless=[Cooldown(cd_time=1.4)])
async def open_xiuxian_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """群修仙开关配置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_msg = str(event.message)
    group_id = str(event.group_id)
    conf_data = JsonConfig().read_data()

    if "启用" in group_msg:
        if group_id not in conf_data["group"]:
            # This logic seems inverted: if group_id not in conf_data["group"], it means it's currently disabled.
            # So, enabling it means removing it from the disabled list (or adding to enabled list).
            # JsonConfig().write_data(2, group_id) removes from 'group' list, which means enabling.
            # The current check `if group_id not in conf_data["group"]` implies it's already enabled in terms of the message text.
            # Let's assume 'group' is a list of *disabled* groups.
            if group_id not in conf_data["group"]: # If it's not in the disabled list, it means it's already enabled.
                msg = "当前群聊修仙模组已启用，请勿重复操作！"
                await handle_send(bot, event, msg)
                await set_xiuxian.finish()
            JsonConfig().write_data(2, group_id) # Removes group_id from the 'group' list (disabling).
            msg = "当前群聊修仙基础模组已启用，快发送 我要修仙 加入修仙世界吧！"
            await handle_send(bot, event, msg)
            await set_xiuxian.finish()

    elif "禁用" in group_msg:
        if group_id in conf_data["group"]: # If it's in the disabled list, it means it's already disabled.
            msg = "当前群聊修仙模组已禁用，请勿重复操作！"
            await handle_send(bot, event, msg)
            await set_xiuxian.finish()
        JsonConfig().write_data(1, group_id) # Adds group_id to the 'group' list (enabling).
        msg = "当前群聊修仙基础模组已禁用！"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()
    else:
        msg = "指令错误，请输入：启用修仙功能/禁用修仙功能"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()

@set_private_chat.handle(parameterless=[Cooldown(cd_time=1.4)])
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

@set_auto_root.handle(parameterless=[Cooldown(cd_time=1.4)])
async def set_auto_root_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """自动选择灵根功能开关配置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg_text = str(event.message)
    conf_data = JsonConfig().read_data()

    if "开启" in msg_text:
        if conf_data.get("auto_root_selection", False):
            msg = "自动选择灵根功能已启用，请勿重复操作！"
        else:
            JsonConfig().write_data(5)
            msg = "自动选择灵根功能已启用！新用户将自动选择最佳灵根。"
    elif "关闭" in msg_text:
        if not conf_data.get("auto_root_selection", False):
            msg = "自动选择灵根功能已关闭，请勿重复操作！"
        else:
            JsonConfig().write_data(6)
            msg = "自动选择灵根功能已关闭！"
    else:
        msg = "指令错误，请输入：开启自动灵根/关闭自动灵根"

    await handle_send(bot, event, msg)
    await set_auto_root.finish()    

@set_auto_sect_name.handle(parameterless=[Cooldown(cd_time=1.4)])
async def set_auto_sect_name_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """自动宗名功能开关配置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg_text = str(event.message)
    conf_data = JsonConfig().read_data()

    if "启用" in msg_text:
        if conf_data.get("auto_sect_name", False):
            msg = "自动宗名功能已启用，请勿重复操作！"
        else:
            JsonConfig().write_data(7)
            msg = "自动宗名功能已启用！创建宗门时将自动随机命名。"
    elif "禁用" in msg_text:
        if not conf_data.get("auto_sect_name", False):
            msg = "自动宗名功能已关闭，请勿重复操作！"
        else:
            JsonConfig().write_data(8)
            msg = "自动宗名功能已关闭！创建宗门将恢复手动选择名称。"
    else:
        msg = "指令错误，请输入：启用自动宗名/禁用自动宗名"

    await handle_send(bot, event, msg)
    await set_auto_sect_name.finish()

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

@items_refresh.handle(parameterless=[Cooldown(cd_time=1.4)])
async def items_refresh_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重载items"""
    items.refresh()
    msg = "重载items完成"
    await handle_send(bot, event, msg)
    await items_refresh.finish()

@blackhouse.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    
    plain_text = args.extract_plain_text().strip()

    target_user_id = None
    target_name = None

    # 1. 优先找艾特
    at_qq = None
    for seg in args:
        if seg.type == "at":
            at_qq = seg.data.get("qq", "")
            break

    if at_qq:
        target_user_id = at_qq
        user = sql_message.get_user_info_with_id(target_user_id)
        if user:
            target_name = user['user_name']
    # 2. 没有艾特就用道号（参数里的最后一个词）
    elif plain_text:
        dao_name = plain_text.split()[-1]          # 防止前面有其他参数
        user = sql_message.get_user_info_with_name(dao_name)
        if user:
            target_user_id = user['user_id']
            target_name = user['user_name']

    if not target_user_id:
        await handle_send(bot, event, "未找到目标用户！请正确艾特或输入道号。")
        return

    target_user = sql_message.get_user_info_with_id(target_user_id)
    if not target_user:
        await handle_send(bot, event, "该用户尚未踏入修仙界！")
        return

    success = sql_message.ban_user(target_user_id)
    if success:
        await handle_send(bot, event, f"道友 {target_user['user_name']} 已被关入小黑屋！")
    else:
        await handle_send(bot, event, f"操作失败：用户 {target_user['user_name']} 可能已被封禁或不存在。")


@unblackhouse.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    
    plain_text = args.extract_plain_text().strip()

    target_user_id = None

    at_qq = None
    for seg in args:
        if seg.type == "at":
            at_qq = seg.data.get("qq", "")
            break

    if at_qq:
        target_user_id = at_qq
    elif plain_text:
        dao_name = plain_text.split()[-1]
        user = sql_message.get_user_info_with_name(dao_name)
        if user:
            target_user_id = user['user_id']

    if not target_user_id:
        await handle_send(bot, event, "未找到目标用户！请正确艾特或输入道号。")
        return

    target_user = sql_message.get_user_info_with_id(target_user_id)
    if not target_user:
        await handle_send(bot, event, "该用户尚未踏入修仙界！")
        return

    success = sql_message.unban_user(target_user_id)
    if success:
        await handle_send(bot, event, f"道友 {target_user['user_name']} 已从小黑屋释放，恢复自由！")
    else:
        await handle_send(bot, event, f"操作失败：用户 {target_user['user_name']} 可能未被封禁或不存在。")

@view_blackhouse.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, _ = await assign_bot(bot=bot, event=event)
    
    cur = sql_message.conn.cursor()
    cur.execute("SELECT user_id, user_name FROM user_xiuxian WHERE is_ban=1")
    banned_users = cur.fetchall()
    
    if not banned_users:
        await handle_send(bot, event, "当前小黑屋空空如也～")
        return
    
    msg = "【小黑屋在押人员】\n"
    for uid, name in banned_users:
        msg += f"· {name} (ID: {uid})\n"
    
    await handle_send(bot, event, msg)

@super_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def super_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """修仙管理帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    help_msg = """
【修仙管理手册】⚡⚡⚡
======================
🌟 管理员专用指令

⚡ 资源管理：
→ 神秘力量 [数量] all - 全服发放灵石
→ 神秘力量 [数量] [道号] - 给指定用户发灵石
- 可以负数来扣灵石
→ 传承力量 [数量] all - 全服发放思恋结晶
→ 传承力量 [数量]  [道号] - 给指定用户发思恋结晶
- 可以负数来扣思恋结晶
→ 创造力量 [物品ID/名称] [数量] - 给自己发物品
→ 创造力量 [物品ID/名称] [数量] all - 全服发物品
→ 创造力量 [物品ID/名称] [数量] [道号] - 给指定用户发物品
→ 毁灭力量 [物品ID/名称] [数量] - 给自己扣物品
→ 毁灭力量 [物品ID/名称] [数量] all - 全服扣物品
→ 毁灭力量 [物品ID/名称] [数量] [道号] - 给指定用户扣物品
→ 赠送称号 [物品ID/名称] all - 全服赠送
→ 赠送称号 [物品ID/名称] [道号] - 给指定用户赠送

⚡ 境界管理：
→ 造化力量 [境界] [道号] - 修改用户境界
→ 轮回力量 [1-9] [道号] - 修改用户灵根
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
→ 开启自动灵根 - 启用自动选择灵根
→ 关闭自动灵根 - 禁用自动选择灵根
→ 启用自动宗名 - 开启自动随机宗门名
→ 禁用自动宗名 - 关闭自动随机宗门名

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

→ 重载items - 重新获取物品数据
→ 用户伪装 [目标ID] - 伪装成指定ID

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

mb_template_test = on_command("md模板", permission=SUPERUSER, priority=5, block=True)
@mb_template_test.handle(parameterless=[Cooldown(cd_time=1.4)])
async def mb_template_test_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    使用自定义Markdown模板发送消息，并支持按钮
    """
    args_str = re.sub(r'mqqapi:/aio', 'mqqapi://aio', args.extract_plain_text())
    args_str = args_str.replace("\\r", "\r").replace('\\"', '"').replace(':/', '://').replace(':///', '://')
    if not args_str:
        await bot.send(event, "请提供模板参数，格式如下：mid=模板ID bid=按钮ID k=a,v=\"xx\" k=b k=c,v=x k=d,v=[\"xx\",\"xx\"] button_id=按钮ID")
        return

    config = XiuConfig()

    id_match = re.search(r'mid=([^\s]+)', args_str)
    template_id_input = id_match.group(1) if id_match else None
    button_id_match = re.search(r'bid=([^\s]+)', args_str)
    button_id_input = button_id_match.group(1) if button_id_match else None

    template_id = None
    if template_id_input:
        if template_id_input == '1':
            template_id = config.markdown_id
        elif template_id_input == '2':
            template_id = config.markdown_id2
        else:
            template_id = template_id_input

    button_id = None
    if button_id_input:
        if button_id_input == '1':
            button_id = config.button_id
        elif button_id_input == '2':
            button_id = config.button_id2
        else:
            button_id = button_id_input


    if id_match:
        args_str = args_str.replace(id_match.group(0), '').strip()
    if button_id_match:
        args_str = args_str.replace(button_id_match.group(0), '').strip()

    if not template_id:
        await bot.send(event, "请提供模板ID (mid=模板ID)")
        return

    arg_parts = re.split(r'\s+(?=\w+=)', args_str.strip())  # 仅在键前分割

    params: List[Dict[str, Any]] = []
    def replace_url_format(input_str):
        if not input_str:
            return " "
        pattern = r'(\w+)\]\(([^)]+)\)'
        def replacer(match):
            param_a = match.group(1)
            param_b = match.group(2)
            if '://' in param_b:
                return f'{param_a}]({param_b})'
            return f'{param_a}](mqqapi://aio/inlinecmd?command={param_b}&enter=false&reply=false)'
        return re.sub(pattern, replacer, input_str)
    
    for arg in arg_parts:
        if '=' not in arg:
            continue
    
        key, raw_value = arg.split('=', 1)
        key = key.strip()

        # 处理值中的特殊字符
        value = raw_value.replace("\\'", "'").replace('\\"', '"').replace("\\=", "=")  # 处理单引号和双引号
        if value.startswith('\r'):
            value = value.strip()
            value = '\r' + value
        else:
            value = value.strip()
        value = value.replace('\n', '\r')

        if value.startswith('[') and value.endswith(']'):
            # 处理列表值
            inner_values = [replace_url_format(v.strip().strip('\'"')) for v in value[1:-1].split(',')]
            params.append({"key": key, "values": inner_values})
        else:
            # 处理普通值
            if not value:
                value = " "
            params.append({"key": key, "values": [value]})

    print(f"传入：\n{args_str}\n\n解析：\n{params}")
    try:
        msg = MessageSegment.markdown_template(bot, template_id, params, button_id)
        await bot.send(event, msg)
    except Exception as e:
        err = str(e)
        logger.error(f"dm发送markdown模板失败: {err}")

        reason = "Markdown模板发送失败，请检查内容格式或平台是否支持。"

        m_msg = re.search(r"message=([^,>]+)", err)
        m_code = re.search(r"code=(\d+)", err)

        if m_msg:
            reason = m_msg.group(1).strip()
            if m_code:
                reason = f"\n{reason}\n错误码：{m_code.group(1)}"
        await handle_send(bot, event, f"Markdown模板发送失败：{reason}")

@dm_command.handle(parameterless=[Cooldown(cd_time=0.5)])
async def dm_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    发送原生Markdown内容
    用法：
    dm # 你好
    dm ## 标题\n- 列表1\n- 列表2
    dm ![img](/root/xiu3/data/xiuxian/卡图/白玫瑰.webp)
    """
    bot, _ = await assign_bot(bot=bot, event=event)

    # 关键：不要用 extract_plain_text()，否则 markdown 结构可能丢失
    text = str(args).strip()
    if not text:
        await handle_send(bot, event, "用法：dm Markdown内容\n示例：dm # 你好")
        return

    # 兼容用户输入的转义字符，转换为QQ markdown常用格式
    text = re.sub(r'mqqapi:/aio', 'mqqapi://aio', text)
    text = (
        text
        .replace("\\r", "\r")
        .replace('\\"', '"')
        .replace(':/', '://')
        .replace(':///', '://')
        .replace("\\n", "\r")
        .replace("\n", "\r")
    )

    # 可选：调试日志，确认本地图片语法是否还在
    logger.info(f"[dm] markdown raw text => {text}")

    try:
        msg = MessageSegment.markdown(bot, text)
        await bot.send(event, msg)
    except Exception as e:
        err = str(e)
        logger.error(f"dm发送markdown失败: {err}")

        reason = "Markdown发送失败，请检查内容格式或平台是否支持。"

        m_msg = re.search(r"message=([^,>]+)", err)
        m_code = re.search(r"code=(\d+)", err)

        if m_msg:
            reason = m_msg.group(1).strip()
            if m_code:
                reason = f"\n{reason}\n错误码：{m_code.group(1)}"
        await handle_send(bot, event, f"Markdown发送失败：{reason}")

@impersonate_user_command.handle(parameterless=[Cooldown(cd_time=0.1)])
async def impersonate_user_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    用户伪装功能：管理员可以伪装成其他用户来执行命令。
    用法：
    用户伪装 [目标ID/@用户/道号] - 伪装成指定用户
    用户伪装 取消    - 取消当前伪装
    用户伪装 off     - 取消当前伪装
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    admin_user_id = str(event.get_user_id())
    arg_text = args.extract_plain_text().strip()

    # 取消伪装
    if arg_text.lower() in {"取消", "off"}:
        if admin_user_id in _impersonating_users:
            del _impersonating_users[admin_user_id]
            await handle_send(bot, event, "已取消用户伪装。您现在是您自己了。")
        else:
            await handle_send(bot, event, "您当前没有伪装任何用户。")
        return

    if not arg_text and not any(seg.type == "at" for seg in args):
        current_target_id = _impersonating_users.get(admin_user_id)
        if current_target_id:
            target_user_info = sql_message.get_user_info_with_id(current_target_id)
            target_name = target_user_info['user_name'] if target_user_info else f"ID: {current_target_id}"
            await handle_send(bot, event, f"您当前正在伪装用户：{target_name}。\n发送「用户伪装 取消」停止伪装。")
        else:
            await handle_send(bot, event, "用法：用户伪装 [目标ID/@用户/道号] 或 用户伪装 取消")
        return

    target_user_id = None
    target_user_info = None

    # 1) 优先 @
    at_qq = None
    for seg in args:
        if seg.type == "at":
            at_qq = seg.data.get("qq", "")
            break

    if at_qq:
        target_user_id = str(at_qq)
        target_user_info = sql_message.get_user_info_with_id(target_user_id)

    # 2) 再按道号查
    if not target_user_id and arg_text:
        info_by_name = sql_message.get_user_info_with_name(arg_text)
        if info_by_name:
            target_user_info = info_by_name
            target_user_id = str(info_by_name["user_id"])

    # 3) 最后把输入当ID（重点：即使数据库没有，也允许伪装）
    if not target_user_id and arg_text:
        target_user_id = str(arg_text)
        target_user_info = sql_message.get_user_info_with_id(target_user_id)

    # 兜底
    if not target_user_id:
        await handle_send(bot, event, "未找到可伪装目标，请输入目标ID/@用户/道号")
        return

    # 直接写入伪装映射（不因为数据库不存在而中断）
    _impersonating_users[admin_user_id] = target_user_id

    if target_user_info:
        await handle_send(
            bot, event,
            f"您已成功伪装成用户：{target_user_info['user_name']} (ID {target_user_id})。\n"
            f"后续所有修仙命令都将以此用户身份执行，直至您取消伪装。"
        )
    else:
        await handle_send(
            bot, event,
            f"您已成功伪装为 ID：{target_user_id}。\n"
            f"⚠ 该ID当前不在数据库，仅提醒，不影响伪装执行。\n"
            f"后续所有修仙命令都将以此身份执行，直至您取消伪装。"
        )

@migrate_qqid_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def migrate_qqid_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """将数据库中的QQ user_id迁移为真实ID"""
    bot, _ = await assign_bot(bot=bot, event=event)
    if XiuConfig().gsk_link:
        await handle_send(bot, event, "开始执行QQID迁移，正在自动备份并更新数据库，请稍候...")
    else:
        await handle_send(bot, event, "当前gsk地址为空，请先修改配置gsk_link")
        await migrate_qqid_cmd.finish()

    ok, msg = migrate_user_id_to_openid()
    await handle_send(bot, event, msg)
    await migrate_qqid_cmd.finish()

@update_id_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def update_id_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    手动ID更新
    用法：ID更新 ID1 ID2
    规则：
    - ID1不存在不更新
    - ID2存在则提示并拒绝
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    arg_list = args.extract_plain_text().strip().split()

    if len(arg_list) != 2:
        await handle_send(bot, event, "用法：ID更新 ID1 ID2\n示例：ID更新 123456 987654")
        return

    old_id, new_id = arg_list[0], arg_list[1]

    await handle_send(bot, event, f"开始执行手动ID更新：{old_id} -> {new_id}\n正在备份并校验，请稍候...")

    ok, msg = migrate_single_user_id(old_id, new_id)
    await handle_send(bot, event, msg)
    await update_id_cmd.finish()

@swap_id_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def swap_id_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    ID交换
    用法：ID交换 ID1 ID2
    规则：ID1和ID2都必须存在
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    arg_list = args.extract_plain_text().strip().split()

    if len(arg_list) != 2:
        await handle_send(bot, event, "用法：ID交换 ID1 ID2\n示例：ID交换 123456 654321")
        return

    id1, id2 = arg_list[0], arg_list[1]
    await handle_send(bot, event, f"开始执行ID交换：{id1} - {id2}\n正在备份并校验，请稍候...")

    ok, msg = swap_two_user_ids(id1, id2)
    await handle_send(bot, event, msg)
    await swap_id_cmd.finish()

def get_random_acg_pic_url(timeout: int = 5) -> str | None:
    """
    获取随机二次元图片地址
    成功返回图片URL，失败返回 None
    """
    api_url = "https://v2.xxapi.cn/api/randomAcgPic"
    params = {
        "type": "pc",
        "return": "json",
    }

    try:
        resp = requests.get(api_url, params=params, timeout=timeout)
        resp.raise_for_status()

        data = resp.json()
        if not isinstance(data, dict):
            logger.warning("默认回复图片接口返回格式异常：不是 JSON 对象")
            return None

        if str(data.get("code")) != "200":
            logger.warning(
                f"默认回复图片接口请求失败: code={data.get('code')} msg={data.get('msg')}"
            )
            return None

        image_url = data.get("data")
        if image_url and isinstance(image_url, str):
            return image_url.strip()

        logger.warning("默认回复图片接口未返回有效图片地址")
        return None

    except Exception as e:
        logger.warning(f"获取默认回复随机图片失败: {e}")
        return None

def _fallback_rule() -> Rule:
    async def _checker(event: BaseEvent, text: str = EventPlainText()) -> bool:
        if not XiuConfig().empty_fallback or not XiuConfig().empty_msg:
            return False
        return isinstance(event, (GroupMessageEvent, PrivateMessageEvent))
    return Rule(_checker)

empty_fallback = on_message(priority=999, block=False, rule=_fallback_rule())


@empty_fallback.handle(parameterless=[Cooldown(cd_time=1.4)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, matcher: Matcher):
    config = XiuConfig()
    text_msg = config.empty_msg

    # 先尝试获取随机图片
    image_url = get_random_acg_pic_url(timeout=5)

    # 1. 开启 Markdown
    if config.markdown_status and image_url:
        # 1.1 有模板ID -> 走模板MD
        if config.markdown_id:
            try:
                msg_param = {
                    "key": "t1",
                    "values": [
                        "](mqqapi://aio/inlinecmd?command=修仙帮助&enter=false&reply=false)\r![",
                        f"img #1280px #720px]({image_url})\r",
                        f"{text_msg}\r\r时间：[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    ]
                }
                await handle_send_md(
                    bot,
                    event,
                    " ",
                    markdown_id=config.markdown_id,
                    msg_param=msg_param,
                    at_msg=None,
                )
            except Exception as e:
                logger.warning(f"默认回复模板Markdown发送失败，准备降级: {e}")
            await matcher.finish()
        # 1.2 无模板ID -> 走原生MD
        elif not is_channel_event(event):
            try:
                md_msg = (
                    f"## 提示\r"
                    f"![img #1280px #720px]({image_url})\r"
                    f"{text_msg}"
                )
                await bot.send(event=event, message=MessageSegment.markdown(bot, md_msg))
                return
            except Exception as e:
                logger.warning(f"默认回复原生Markdown发送失败，准备降级: {e}")
            await matcher.finish()
    # 2. 未开启 Markdown -> 走图文
    if not config.markdown_status and image_url:
        try:
            await handle_pic_msg_send(bot, event, image_url, text_msg)
        except Exception as e:
            logger.warning(f"默认回复图文发送失败，准备降级纯文字: {e}")
        await matcher.finish()
    # 3. 最终兜底：纯文字
    try:
        await handle_send(bot, event, text_msg)
    except Exception as e:
        logger.warning(f"默认回复纯文字发送失败: {e}")

    await matcher.finish()

from typing import Any, Dict, Tuple


from typing import Any, Dict, Tuple


@parse_event_cmd.handle(parameterless=[Cooldown(cd_time=0.5)])
async def parse_event_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent
):
    """
    超管：解析当前 event 并按配置发送
    规则：
    1) markdown_status=True 且 markdown_id有值 -> 模板Markdown（清洗后）
    2) 其他情况 -> 强制纯文本发送（避免原生Markdown URL风控/代码块截断）
    """
    bot, _ = await assign_bot(bot=bot, event=event)

    try:
        basic_text, raw_json = _build_event_info_blocks(event)
        await _send_event_info_by_config(bot, event, basic_text, raw_json)
    except Exception as e:
        logger.error(f"解析event并发送失败: {e}")
        await handle_send(bot, event, f"解析event失败：{e}")


def _safe_str(obj) -> str:
    try:
        return str(obj)
    except Exception:
        try:
            return repr(obj)
        except Exception:
            return "<无法转为字符串>"


def _unescape_slashes(text: str) -> str:
    if not isinstance(text, str):
        text = _safe_str(text)
    return text.replace("\\/", "/")


def _segment_to_simple(seg):
    try:
        seg_type = getattr(seg, "type", None)
        seg_data = getattr(seg, "data", None)
        return {
            "type": seg_type,
            "data": seg_data if seg_data is not None else _safe_str(seg),
        }
    except Exception:
        return _safe_str(seg)


def _message_to_simple(msg):
    if msg is None:
        return None
    if isinstance(msg, str):
        return msg
    try:
        return [_segment_to_simple(seg) for seg in msg]
    except Exception:
        return _safe_str(msg)


def _extract_plain_from_message(msg) -> str:
    if msg is None:
        return ""
    try:
        if hasattr(msg, "extract_plain_text"):
            return msg.extract_plain_text()
    except Exception:
        pass
    try:
        return str(msg)
    except Exception:
        return ""


def _extract_reply_info(event) -> dict | None:
    reply_info = {}

    reply_obj = getattr(event, "reply", None)
    if reply_obj is not None:
        try:
            reply_info["source"] = "event.reply"
            reply_info["message_id"] = getattr(reply_obj, "message_id", None)
            reply_info["real_id"] = getattr(reply_obj, "real_id", None)
            reply_info["time"] = getattr(reply_obj, "time", None)

            sender = getattr(reply_obj, "sender", None)
            if sender is not None:
                reply_info["sender"] = {
                    "user_id": getattr(sender, "user_id", None),
                    "nickname": getattr(sender, "nickname", None),
                    "card": getattr(sender, "card", None),
                    "role": getattr(sender, "role", None),
                }

            message = getattr(reply_obj, "message", None)
            if message is not None:
                reply_info["message"] = _message_to_simple(message)
                reply_info["plain_text"] = _extract_plain_from_message(message)

            return reply_info
        except Exception:
            pass

    try:
        original_message = getattr(event, "original_message", None)
        if original_message:
            for seg in original_message:
                if getattr(seg, "type", None) == "reply":
                    reply_info["source"] = "original_message.reply_segment"
                    reply_info["message_id"] = getattr(seg, "data", {}).get("id")
                    return reply_info
    except Exception:
        pass

    try:
        message_reference = getattr(event, "message_reference", None)
        if message_reference is not None:
            reply_info["source"] = "message_reference"
            reply_info["message_id"] = getattr(message_reference, "message_id", None)
            reply_info["ignore_get_message_error"] = getattr(
                message_reference, "ignore_get_message_error", None
            )
            return reply_info
    except Exception:
        pass

    try:
        message_scene = getattr(event, "message_scene", None)
        if message_scene:
            ext_list = getattr(message_scene, "ext", None)
            if ext_list is None and isinstance(message_scene, dict):
                ext_list = message_scene.get("ext")

            ref_msg_idx = None
            if isinstance(ext_list, list):
                for item in ext_list:
                    if isinstance(item, dict) and item.get("key") == "ref_msg_idx":
                        ref_msg_idx = item.get("value")
                        break

            if ref_msg_idx:
                reply_info["source"] = "message_scene.ext.ref_msg_idx"
                reply_info["ref_msg_idx"] = ref_msg_idx
                return reply_info
    except Exception:
        pass

    try:
        msg_elements = getattr(event, "msg_elements", None)
        if isinstance(msg_elements, list):
            for elem in msg_elements:
                if not isinstance(elem, dict):
                    continue
                for key in ("ref_msg_id", "ref_message_id", "message_id", "msg_id", "reply_id"):
                    if key in elem and elem.get(key):
                        reply_info["source"] = "msg_elements"
                        reply_info["message_id"] = elem.get(key)
                        reply_info["raw_element"] = elem
                        return reply_info
    except Exception:
        pass

    return reply_info or None


def _event_to_dict(event):
    data = None
    try:
        data = model_dump(event)
    except Exception:
        pass

    if data is None:
        try:
            if hasattr(event, "dict"):
                data = event.dict()
        except Exception:
            pass

    if data is None:
        try:
            if hasattr(event, "__dict__"):
                data = {}
                for k, v in event.__dict__.items():
                    if k.startswith("_"):
                        continue
                    data[k] = v
        except Exception:
            pass

    if data is None:
        return {"raw": _safe_str(event)}

    try:
        if hasattr(event, "message"):
            data["message"] = _message_to_simple(getattr(event, "message", None))
    except Exception:
        pass

    try:
        if hasattr(event, "original_message"):
            data["original_message"] = _message_to_simple(getattr(event, "original_message", None))
    except Exception:
        pass

    try:
        reply_info = _extract_reply_info(event)
        if reply_info:
            data["__parsed_reply__"] = reply_info
    except Exception:
        pass

    return data


def _pretty_event_json(data) -> str:
    try:
        text = json.dumps(data, ensure_ascii=False, indent=2, default=str)
        return _unescape_slashes(text)
    except Exception:
        return _unescape_slashes(_safe_str(data))


def _truncate_for_send(text: str, limit: int = 3000) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n\n......\n（内容过长，已截断，原长度：{len(text)}）"


def _sanitize_markdown_unsafe_text(text: str) -> str:
    """
    清理可能触发QQ原生Markdown风控或渲染异常的内容：
    保留换行符结构，防止代码块坍塌
    """
    if not isinstance(text, str):
        text = _safe_str(text)

    # 1. 先把标准的 \n 换行符统一转为 \r (QQ Markdown 识别 \r 换行更稳定)
    text = text.replace("\n", "\r")

    # 2. 清理 Markdown 链接防止被解析
    text = strip_md_links(text)

    # 3. 避免 ``` 干扰代码块闭合
    text = text.replace("```", "'''")
    
    return text

def strip_md_links(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)

    # [文本](任意链接) -> 文本
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'\1', text)
    text = re.sub(
        r'(?i)\b(https?|mqqapi)://',
        lambda m: f"{m.group(1)}:\\/\\/",
        text
    )

    return text

def _build_event_info_blocks(event) -> Tuple[str, str]:
    """
    返回：
    - basic_text: 基本信息文本（纯文本）
    - raw_json:   原始event json文本（已美化）
    """
    lines = []
    lines.append("【消息基本信息】")

    try:
        lines.append(f"事件类型：{event.get_type()}")
    except Exception:
        lines.append(f"事件类型：{getattr(event, 'post_type', getattr(event, '__type__', '未知'))}")

    try:
        lines.append(f"事件名称：{event.get_event_name()}")
    except Exception:
        pass

    try:
        lines.append(f"用户ID：{event.get_user_id()}")
    except Exception:
        uid = getattr(event, "user_id", None)
        if uid is not None:
            lines.append(f"用户ID：{uid}")

    try:
        lines.append(f"会话ID：{event.get_session_id()}")
    except Exception:
        pass

    for attr, label in [
        ("group_id", "群ID"),
        ("group_openid", "群OpenID"),
        ("channel_id", "频道ID"),
        ("guild_id", "Guild ID"),
        ("message_id", "消息ID"),
        ("id", "平台消息ID"),
        ("event_id", "事件ID"),
        ("self_id", "Bot ID"),
    ]:
        value = getattr(event, attr, None)
        if value is not None:
            lines.append(f"{label}：{value}")

    try:
        lines.append(f"to_me：{event.is_tome()}")
    except Exception:
        to_me = getattr(event, "to_me", None)
        if to_me is not None:
            lines.append(f"to_me：{to_me}")

    sender = getattr(event, "sender", None)
    author = getattr(event, "author", None)

    if sender is not None:
        lines.append(f"发送者ID：{getattr(sender, 'user_id', None)}")
        lines.append(f"发送者昵称：{getattr(sender, 'nickname', None)}")
        lines.append(f"发送者群名片：{getattr(sender, 'card', None)}")
        lines.append(f"发送者角色：{getattr(sender, 'role', None)}")
    elif author is not None:
        author_id = (
            getattr(author, "id", None)
            or getattr(author, "user_openid", None)
            or getattr(author, "member_openid", None)
        )
        lines.append(f"发送者ID：{author_id}")
        lines.append(f"发送者昵称：{getattr(author, 'username', None)}")

    try:
        msg_obj = event.get_message()
        plain_text = _extract_plain_from_message(msg_obj)
        lines.append(f"纯文本：{plain_text if plain_text else '[空]'}")
        lines.append(f"消息对象：{_safe_str(msg_obj)}")
    except Exception:
        raw_message = getattr(event, "raw_message", None)
        content = getattr(event, "content", None)
        message = getattr(event, "message", None)
        if raw_message is not None:
            lines.append(f"raw_message：{raw_message}")
        elif content is not None:
            lines.append(f"content：{content}")
        elif message is not None:
            lines.append(f"message：{_safe_str(message)}")
        else:
            lines.append("消息内容：<无>")

    reply_info = _extract_reply_info(event)
    if reply_info:
        lines.append(f"引用信息：{_unescape_slashes(json.dumps(reply_info, ensure_ascii=False, default=str))}")

    basic_text = "\n".join(lines)

    event_dict = _event_to_dict(event)
    raw_json = _pretty_event_json(event_dict)
    raw_json = _truncate_for_send(raw_json)

    return basic_text, raw_json


async def _send_event_info_by_config(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    basic_text: str,
    raw_json: str
):
    cfg = XiuConfig()

    # 预处理：这里的 safe_raw 已经通过 _sanitize_markdown_unsafe_text 转换了 \n 为 \r
    safe_basic = _sanitize_markdown_unsafe_text(basic_text)
    safe_raw = _sanitize_markdown_unsafe_text(raw_json)

    if cfg.markdown_status:
        if cfg.markdown_id:
            # 模板 MD 逻辑保持不变
            try:
                content = [safe_raw]
                await send_msg_handler(bot, event, "event", bot.self_id, content, title=safe_basic)
                return
            except Exception as e:
                logger.warning(f"消息信息模板Markdown发送失败，降级原生: {e}")
        
        # 优化原生 Markdown 发送结构
        try:
            # 使用 \r 确保在手机端渲染时，代码块能够正常换行且保持缩进
            plain = (
                f"### 消息基本信息\r"
                f"```text\r"
                f"{safe_basic}\r"
                f"```\r"
                f"### 原始数据 (Event JSON)\r"
                f"```json\r"
                f"{safe_raw}\r"
                f"```"
            )
            await bot.send(event=event, message=MessageSegment.markdown(bot, plain))
            return
        except Exception as e:
            logger.warning(f"消息信息原生Markdown发送失败，降级纯文本: {e}")
                
    # 纯文本兜底
    plain = f"{basic_text}\n\n【原始信息】\n{raw_json}"
    try:
        await bot.send(event=event, message=plain)
    except Exception:
        await handle_send(bot, event, plain)