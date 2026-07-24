try:
    import ujson as json
except ImportError:
    import json
import re
import os
import random
import asyncio
from pathlib import Path
from typing import Any, Dict, List
from ...paths import get_paths
from nonebot.typing import T_State
from nonebot.permission import SUPERUSER
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot import require, get_bot
from ..on_compat import on_command

from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    get_user_id,
    get_at_user_id,
    get_at_user_ids,
    has_at_user,
)
from ..messaging.delivery import delivery_service

from ..broadcast_manager import (
    start_broadcast,
    format_broadcast_status,
    cancel_broadcast,
    clear_broadcast,
)

from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_base import clear_all_xiangyuan
from ..xiuxian_rift import create_rift
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, XiuxianJsonDate, OtherSet, 
    UserBuffDate, migrate_user_id_to_openid, migrate_single_user_id, swap_two_user_ids
)
from ..xiuxian_config import XiuConfig, JsonConfig, convert_rank
from ..xiuxian_utils.utils import (
    check_user, number_to, get_msg_pic, handle_send, send_msg_handler,
    generate_command, _impersonating_users, send_help_message,
    parse_page_arg, paginate_text_blocks, build_pagination_buttons
)
from ..xiuxian_utils.bg_jobs import spawn_admin_job, run_chunked_until_done
from ..xiuxian_utils.item_json import Items
from ..xiuxian_back import ACCESSORY_BAG_LIMIT, create_accessory_instance
from .admin_helpers import (
    _admin_economy_context,
    _extract_keyboard_command,
    _parse_keyboard_test_rows,
    fix_mqqapi_inlinecmd_links,
    parse_broadcast_duration_and_content,
    parse_clear_broadcast_kind,
)
from .transaction_service import AdminLevelChangeService
from .transaction_service import AdminRootChangeService
from .transaction_service import AdminExpAdjustmentService
from .transaction_service import AdminStoneAdjustmentService
from .transaction_service import AdminItemGrantService
from .transaction_service import AdminItemDestroyService
from .transaction_service import AdminItemBatchGrantService
from .transaction_service import AdminAccessoryAdjustmentService
from .transaction_service import (
    AdminAccessoryBatchAdjustmentService,
)
from .transaction_service import AdminImpartStoneAdjustmentService
from .transaction_service import (
    AdminImpartStoneBatchAdjustmentService,
)
from .transaction_service import AdminPlayerStatusResetService
from .transaction_service import AdminPlayerStatusBatchResetService
from .transaction_service import AdminBlackhouseStatusService
from . import command_controls as _command_controls  # noqa: F401
from . import empty_fallback as _empty_fallback  # noqa: F401
from . import event_debug as _event_debug  # noqa: F401
from . import group_welcome as _group_welcome  # noqa: F401

items = Items()
sql_message = XiuxianDateManage()  # sql类
admin_level_change_service = AdminLevelChangeService(get_paths().game_db)
admin_root_change_service = AdminRootChangeService(get_paths().game_db)
admin_exp_adjustment_service = AdminExpAdjustmentService(get_paths().game_db)
admin_stone_adjustment_service = AdminStoneAdjustmentService(get_paths().game_db)
admin_item_grant_service = AdminItemGrantService(get_paths().game_db)
admin_item_destroy_service = AdminItemDestroyService(get_paths().game_db)
admin_item_batch_grant_service = AdminItemBatchGrantService(get_paths().game_db)
admin_accessory_adjustment_service = AdminAccessoryAdjustmentService(
    get_paths().game_db, get_paths().player_db
)
admin_accessory_batch_adjustment_service = AdminAccessoryBatchAdjustmentService(
    get_paths().game_db,
    get_paths().player_db,
    admin_accessory_adjustment_service,
)
admin_impart_stone_adjustment_service = AdminImpartStoneAdjustmentService(
    get_paths().game_db, get_paths().impart_db
)
admin_impart_stone_batch_adjustment_service = AdminImpartStoneBatchAdjustmentService(
    get_paths().game_db,
    get_paths().impart_db,
    admin_impart_stone_adjustment_service,
)
admin_player_status_reset_service = AdminPlayerStatusResetService(get_paths().game_db)
admin_player_status_batch_reset_service = AdminPlayerStatusBatchResetService(
    get_paths().game_db,
    admin_player_status_reset_service,
)
admin_blackhouse_status_service = AdminBlackhouseStatusService(get_paths().game_db)


def _admin_operation_id(event, action: str, user_id: str) -> str:
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    return f"admin-{action}:{event_id or __import__('time').time_ns()}:{user_id}"


def _grant_admin_accessory(
    event,
    user_id: str,
    item_id: int,
    item_name: str,
    quality: int,
    quantity: int,
    target_name: str,
):
    equipped, bag = admin_accessory_adjustment_service.snapshot(user_id)
    return admin_accessory_adjustment_service.grant(
        _admin_operation_id(event, "accessory-grant", user_id),
        str(get_user_id(event) or "unknown"),
        user_id,
        item_id,
        item_name,
        quality,
        quantity,
        equipped,
        bag,
        ACCESSORY_BAG_LIMIT,
        lambda: create_accessory_instance(item_id, quality),
        target_name=target_name,
    )


def _destroy_admin_accessory(
    event,
    user_id: str,
    item_id: int,
    item_name: str,
    quantity: int,
    target_name: str,
):
    equipped, bag = admin_accessory_adjustment_service.snapshot(user_id)
    return admin_accessory_adjustment_service.destroy(
        _admin_operation_id(event, "accessory-destroy", user_id),
        str(get_user_id(event) or "unknown"),
        user_id,
        item_id,
        item_name,
        quantity,
        equipped,
        bag,
        target_name=target_name,
    )


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
keyboard_test_cmd = on_command("按钮测试", permission=SUPERUSER, priority=5, block=True)
at_test_cmd = on_command("艾特测试", permission=SUPERUSER, priority=5, block=True)
admin_rename_cmd = on_command("易名", permission=SUPERUSER, priority=5, block=True)
migrate_qqid_cmd = on_command("转换QQID", permission=SUPERUSER, priority=5, block=True)
update_id_cmd = on_command("ID更新", permission=SUPERUSER, priority=5, block=True)
swap_id_cmd = on_command("ID交换", permission=SUPERUSER, priority=5, block=True)
group_broadcast_cmd = on_command("群聊广播", permission=SUPERUSER, priority=5, block=True)
private_broadcast_cmd = on_command("私聊广播", permission=SUPERUSER, priority=5, block=True)
global_broadcast_cmd = on_command("全局广播", permission=SUPERUSER, priority=5, block=True)
view_broadcast_cmd = on_command(
    "查看广播",
    aliases={"广播列表"},
    permission=SUPERUSER,
    priority=5,
    block=True
)

cancel_broadcast_cmd = on_command(
    "取消广播",
    permission=SUPERUSER,
    priority=5,
    block=True
)

clear_broadcast_cmd = on_command(
    "清空广播",
    aliases={"清除广播"},
    permission=SUPERUSER,
    priority=5,
    block=True
)

broadcast_help_cmd = on_command(
    "广播帮助",
    aliases={"广播说明", "广播指令"},
    permission=SUPERUSER,
    priority=5,
    block=True
)


@at_test_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def at_test_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """测试通用艾特解析"""
    bot, _ = await assign_bot(bot=bot, event=event)
    at_ids = get_at_user_ids(args)
    if not at_ids:
        await handle_send(bot, event, "无艾特\n艾特ID列表：[]")
        return

    lines = ["有艾特", "艾特ID列表："]
    lines.extend(f"{index}. {user_id}" for index, user_id in enumerate(at_ids, 1))
    if len(at_ids) > 25:
        lines.append(f"按钮仅生成前25个，共识别{len(at_ids)}个")

    button_ids = at_ids[:25]
    rows = [
        [(str(index), user_id) for index, user_id in enumerate(button_ids[start:start + 5], start + 1)]
        for start in range(0, len(button_ids), 5)
    ]

    msg = "\n".join(lines)
    try:
        await delivery_service.reply(bot, event, MessageSegment.markdown_keyboard(bot, msg, rows))
    except Exception as e:
        logger.error(f"艾特测试按钮发送失败: {e}")
        await handle_send(bot, event, msg)


@admin_rename_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def admin_rename_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """管理员修改用户道号：易名 旧道号 新道号 / 易名 @用户 新道号"""
    bot, _ = await assign_bot(bot=bot, event=event)
    arg_list = args.extract_plain_text().strip().split()
    at_user_id = get_at_user_id(args)

    if at_user_id:
        if not arg_list:
            await handle_send(bot, event, "用法：易名 @用户 新道号")
            return
        target_user = sql_message.get_user_info_with_id(at_user_id)
        new_name = arg_list[-1]
    else:
        if len(arg_list) < 2:
            await handle_send(bot, event, "用法：易名 旧道号 新道号\n或：易名 @用户 新道号")
            return
        old_name, new_name = arg_list[0], arg_list[1]
        target_user = sql_message.get_user_info_with_name(old_name)

    new_name = new_name.strip()
    if not target_user:
        await handle_send(bot, event, "未找到目标用户（请确认旧道号或艾特用户已踏入修仙界）")
        return

    old_name = str(target_user.get("user_name", ""))
    target_user_id = str(target_user.get("user_id", ""))

    if not new_name:
        await handle_send(bot, event, "新道号不能为空")
        return

    if len(new_name) > 7:
        await handle_send(bot, event, "道号长度不能超过7个字符！")
        return

    same_name_user = sql_message.get_user_info_with_name(new_name)
    if same_name_user and str(same_name_user.get("user_id", "")) != target_user_id:
        await handle_send(bot, event, "该道号已被使用，请选择其他道号！")
        return

    if old_name == new_name:
        await handle_send(bot, event, f"{old_name} 的道号未变化")
        return

    result = sql_message.update_user_name(target_user_id, new_name)
    await handle_send(bot, event, f"已将 {old_name} 的道号修改为 {new_name}\n{result}")


# GM加灵石
@gm_command.handle(parameterless=[Cooldown(cd_time=0)])
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
    at_qq = get_at_user_id(args)

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
                    await delivery_service.send_to_group(bot, gid, MessageSegment.image(pic))
                else:
                    await delivery_service.send_to_group(bot, gid, msg)
            except Exception as e:
                logger.debug(f"全服灵石广播到群 {gid} 失败：{e}")
    else:  # 单人
        if amount == 0:
            await handle_send(bot, event, "单人灵石调整数量不能为 0")
            return
        result = admin_stone_adjustment_service.adjust(
            _admin_operation_id(event, "stone-adjust", str(user_id)),
            str(get_user_id(event) or "unknown"),
            user_id,
            int(user["stone"] or 0),
            amount,
            target_name=target_name,
        )
        if result.status == "state_changed":
            msg = "玩家灵石状态已变化，请重新执行指令"
        elif result.status == "operation_conflict":
            msg = "本次管理员操作与已记录事件冲突"
        elif result.status == "user_missing":
            msg = "该玩家已不存在"
        else:
            action = "赠送" if result.applied_delta > 0 else "扣除"
            msg = f"成功{action}{number_to(abs(result.applied_delta))}枚灵石给 {target_name} 道友！"
        await handle_send(bot, event, msg)

# GM加思恋结晶
@ccll_command.handle(parameterless=[Cooldown(cd_time=0)])
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

    at_qq = get_at_user_id(args)

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
        if amount == 0:
            await handle_send(bot, event, "全服思恋结晶调整数量不能为 0")
            return
        all_users = sql_message.get_all_user_id()
        if not all_users:
            await handle_send(bot, event, "当前没有可调整的用户")
            return
        operator_id = str(get_user_id(event) or "unknown")
        operation_id = admin_impart_stone_batch_adjustment_service.find_running(
            operator_id, amount
        ) or _admin_operation_id(event, "impart-stone-adjust-all", "all")
        action = "增加" if amount > 0 else "扣除"
        users = list(all_users)

        def _work():
            return run_chunked_until_done(
                lambda: admin_impart_stone_batch_adjustment_service.adjust(
                    operation_id, operator_id, users, amount
                )
            )

        def _done(result):
            if result.status == "operation_conflict":
                return "本次全服思恋结晶调整与已记录计划冲突"
            return (
                f"全服思恋结晶{action}完成！已处理 "
                f"{result.completed}/{result.total} 名玩家，"
                f"实际影响 {result.affected_users} 名，"
                f"累计{action} {number_to(abs(result.applied_delta))} 枚，"
                f"跳过 {result.skipped_users} 名"
            )

        await spawn_admin_job(
            bot,
            event,
            job_key=f"impart-stone-all:{operator_id}:{amount}",
            start_msg=f"🔄 全服思恋结晶{action}已在后台开始（共 {len(users)} 人），完成后另行通知。",
            work=_work,
            done_msg=_done,
            fail_prefix="全服思恋结晶调整失败",
        )
        return
    else:
        if amount == 0:
            await handle_send(bot, event, "单人思恋结晶调整数量不能为 0")
            return
        expected_stone = admin_impart_stone_adjustment_service.snapshot(user_id)
        result = admin_impart_stone_adjustment_service.adjust(
            _admin_operation_id(event, "impart-stone-adjust", str(user_id)),
            str(get_user_id(event) or "unknown"),
            user_id,
            expected_stone,
            amount,
            target_name=target_name,
        )
        if result.status == "state_changed":
            msg = "玩家思恋结晶状态已变化，请重新执行指令"
        elif result.status == "operation_conflict":
            msg = "本次管理员传承操作与已记录事件冲突"
        elif result.status == "user_missing":
            msg = "该玩家已不存在"
        elif result.status == "invalid_state":
            msg = "该玩家的传承数据异常，请先修复数据"
        else:
            action = "赠送" if result.applied_delta > 0 else "扣除"
            msg = (
                f"成功{action}{number_to(abs(result.applied_delta))}枚思恋结晶"
                f"给 {target_name}！当前余额 {number_to(result.final_stone)}"
            )

    await handle_send(bot, event, msg)

@adjust_exp_command.handle(parameterless=[Cooldown(cd_time=0)])
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
    if give_exp_num == 0:
        await handle_send(bot, event, "修为调整数量不能为 0")
        await adjust_exp_command.finish()

    # 遍历Message对象，寻找艾特信息
    give_qq = get_at_user_id(args)
    
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = None
    
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
        if give_user:
            result = admin_exp_adjustment_service.adjust(
                _admin_operation_id(event, "exp-adjust", str(give_qq)),
                str(get_user_id(event) or "unknown"),
                give_qq,
                int(give_user["exp"] or 0),
                give_exp_num,
                target_name=give_user["user_name"],
            )
            if result.status == "state_changed":
                msg = "玩家修为状态已变化，请重新执行指令"
            elif result.status == "operation_conflict":
                msg = "本次管理员操作与已记录事件冲突"
            elif result.status == "user_missing":
                msg = "对方未踏入修仙界，不可操作！"
            elif result.applied_delta > 0:
                msg = f"共增加{number_to(result.applied_delta)}修为给{give_user['user_name']}道友！"
            else:
                msg = f"共减少{number_to(abs(result.applied_delta))}修为给{give_user['user_name']}道友！"
            
            await handle_send(bot, event, msg)
            await adjust_exp_command.finish()
        else:
            msg = f"对方未踏入修仙界，不可操作！"
            await handle_send(bot, event, msg)
            await adjust_exp_command.finish()    
    else:
        msg = f"对方未踏入修仙界，不可操作！"
        await handle_send(bot, event, msg)
        await adjust_exp_command.finish()
    await adjust_exp_command.finish()

@zaohua_xiuxian.handle(parameterless=[Cooldown(cd_time=0)])
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
    target_qq = get_at_user_id(args)

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

    level_config = jsondata.level_data()[level]
    result = admin_level_change_service.change(
        _admin_operation_id(event, "level-change", str(target_qq)),
        str(get_user_id(event) or "unknown"),
        target_qq,
        (
            target_user["level"], target_user["exp"], target_user["hp"],
            target_user["mp"], target_user["atk"], target_user["power"],
            target_user["root_type"], target_user["root_level"],
        ),
        level,
        int(level_config["power"]),
        float(level_config["spend"]),
        float(sql_message.get_root_rate(target_user["root_type"], target_qq)),
    )
    if result.status == "state_changed":
        msg = "玩家综合状态已变化，请重新执行指令"
    elif result.status == "operation_conflict":
        msg = "本次管理员境界操作与已记录事件冲突"
    elif result.status == "user_missing":
        msg = "目标玩家已不存在"
    else:
        msg = f"已将 {target_user['user_name']} 的境界变更为 【{result.level}】！"
    await handle_send(bot, event, msg)

@gmm_command.handle(parameterless=[Cooldown(cd_time=0)])
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
    except (TypeError, ValueError):
        await handle_send(bot, event, "第一个参数必须是1~9的整数（灵根编号）")
        return

    # 目标解析
    target_user = None
    target_qq = None

    # 优先艾特
    target_qq = get_at_user_id(args)

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

    _, new_root_type = admin_root_change_service.root_values(root_id, target_user["user_name"])
    root_config = jsondata.root_data()
    if new_root_type == "命运道果":
        new_root_rate = float(root_config["永恒道果"]["type_speeds"])
        step_rate = float(root_config[new_root_type]["type_speeds"])
        remaining = int(target_user["root_level"] or 0)
        while remaining > 0:
            levels = min(remaining, 5)
            new_root_rate += levels * step_rate
            remaining -= levels
            step_rate = round(max(0.5, step_rate - 0.3), 2)
            if step_rate <= 0.5:
                new_root_rate += remaining * 0.5
                break
    else:
        new_root_rate = float(root_config[new_root_type]["type_speeds"])
    result = admin_root_change_service.change(
        _admin_operation_id(event, "root-change", str(target_qq)),
        str(get_user_id(event) or "unknown"),
        target_qq,
        (
            target_user["root"], target_user["root_type"], target_user["root_level"],
            target_user["level"], target_user["exp"], target_user["power"],
            target_user["user_name"],
        ),
        root_id,
        float(jsondata.level_data()[target_user["level"]]["spend"]),
        new_root_rate,
    )
    if result.status == "state_changed":
        msg = "玩家综合状态已变化，请重新执行指令"
    elif result.status == "operation_conflict":
        msg = "本次管理员灵根操作与已记录事件冲突"
    elif result.status == "user_missing":
        msg = "目标玩家已不存在"
    else:
        msg = f"已将 {target_user['user_name']} 的灵根变更为 【{result.root_type}】！"
    await handle_send(bot, event, msg)

@cz.handle(parameterless=[Cooldown(cd_time=0)])
async def cz_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """创造力量 - 给玩家或全服发放物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    arg_list = args.extract_plain_text().split()

    if len(arg_list) < 2:
        msg = (
            "请输入正确指令！例如：\n"
            "创造力量 物品名 数量\n"
            "创造力量 物品名 数量 玩家名\n"
            "创造力量 物品名 数量 all\n"
            "（饰品可选品阶）创造力量 物品名 数量 [玩家名/all] [1-5]"
        )
        await handle_send(bot, event, msg)
        await cz.finish()

    goods_name = arg_list[0]

    try:
        quantity = int(arg_list[1])
        if quantity <= 0:
            await handle_send(bot, event, "数量必须大于0！")
            await cz.finish()
    except ValueError:
        await handle_send(bot, event, "数量必须是整数！")
        await cz.finish()

    # 查找物品
    goods_id, item_info = items.get_data_by_item_name(goods_name)
    if not goods_id or not item_info:
        await handle_send(bot, event, f"物品 {goods_name} 不存在！")
        await cz.finish()

    goods_id = int(goods_id)
    is_accessory = item_info.get("item_type") == "饰品"

    # 解析目标与饰品品阶
    # 规则：
    # - 第3参数：target（玩家名/all，可省略）
    # - 第4参数：quality（仅饰品有效，可省略）
    target = None
    quality = 1

    if len(arg_list) >= 3:
        target = arg_list[2]

    if is_accessory and len(arg_list) >= 4:
        try:
            quality = int(arg_list[3])
            quality = max(1, min(5, quality))
        except ValueError:
            await handle_send(bot, event, "饰品品阶必须是1~5的整数！")
            await cz.finish()

    # 非饰品保持老逻辑需要的 goods_type
    goods_type = item_info.get("type", "")

    # ===== 全服发放 =====
    if target and str(target).lower() == "all":
        all_users = sql_message.get_all_user_id()
        if not all_users:
            await handle_send(bot, event, "当前没有可发放的用户。")
            await cz.finish()

        users = list(all_users)
        operator_id = str(get_user_id(event) or "unknown")
        if is_accessory:
            operation_id = admin_accessory_batch_adjustment_service.find_running(
                "grant",
                operator_id,
                goods_id,
                item_info["name"],
                quality,
                quantity,
                ACCESSORY_BAG_LIMIT,
            ) or _admin_operation_id(event, "accessory-grant-all", str(goods_id))

            def _work():
                return run_chunked_until_done(
                    lambda: admin_accessory_batch_adjustment_service.grant(
                        operation_id,
                        operator_id,
                        users,
                        goods_id,
                        item_info["name"],
                        quality,
                        quantity,
                        ACCESSORY_BAG_LIMIT,
                        lambda _user_id: create_accessory_instance(goods_id, quality),
                    )
                )

            def _done(result):
                if result.status == "operation_conflict":
                    return "本次全服饰品发放与已记录计划冲突"
                return (
                    f"全服饰品发放完成！已处理 {result.completed}/{result.total} 名玩家，"
                    f"实际向 {result.affected_users} 名玩家发放 "
                    f"{item_info['name']} {result.affected_quantity} 件（{quality}阶），"
                    f"跳过 {result.skipped_users} 名"
                )

            await spawn_admin_job(
                bot,
                event,
                job_key=f"accessory-grant-all:{goods_id}:{quality}:{quantity}",
                start_msg=(
                    f"🔄 全服饰品【{item_info['name']}】发放已在后台开始"
                    f"（共 {len(users)} 人），完成后另行通知。"
                ),
                work=_work,
                done_msg=_done,
                fail_prefix="全服饰品发放失败",
            )
        else:
            operation_id = _admin_operation_id(event, "item-add-all", str(goods_id))

            def _work():
                return run_chunked_until_done(
                    lambda: admin_item_batch_grant_service.grant(
                        operation_id,
                        operator_id,
                        users,
                        goods_id,
                        item_info["name"],
                        goods_type,
                        quantity,
                        int(XiuConfig().max_goods_num),
                    )
                )

            def _done(result):
                if result.status == "operation_conflict":
                    return "本次全服物品发放与已记录事件冲突"
                return (
                    f"全服发放完成！已处理 {result.completed}/{result.total} 名玩家，"
                    f"实际向 {result.granted_users} 名玩家发放 {item_info['name']} x{quantity}，"
                    f"累计入包 {result.added} 件"
                )

            await spawn_admin_job(
                bot,
                event,
                job_key=f"item-grant-all:{goods_id}:{quantity}",
                start_msg=(
                    f"🔄 全服物品【{item_info['name']}】发放已在后台开始"
                    f"（共 {len(users)} 人），完成后另行通知。"
                ),
                work=_work,
                done_msg=_done,
                fail_prefix="全服物品发放失败",
            )

        await cz.finish()

    # ===== 指定玩家发放 =====
    if target:
        user_info = sql_message.get_user_info_with_name(target)
        if not user_info:
            await handle_send(bot, event, f"玩家 {target} 不存在！")
            await cz.finish()

        user_id = str(user_info["user_id"])

        if is_accessory:
            result = _grant_admin_accessory(
                event,
                user_id,
                goods_id,
                item_info["name"],
                quality,
                quantity,
                target,
            )
            if result.status == "inventory_full":
                msg = f"{target} 的饰品背包容量不足！"
            elif result.status == "state_changed":
                msg = "玩家饰品状态已变化，请重新执行指令"
            elif result.status == "operation_conflict":
                msg = "本次管理员饰品操作与已记录事件冲突"
            elif result.status == "user_missing":
                msg = f"玩家 {target} 已不存在！"
            elif result.status == "invalid_plan":
                msg = "饰品生成结果无效，请重新执行指令"
            elif result.status == "invalid_state":
                msg = f"{target} 的饰品数据异常，请先执行背包检测"
            else:
                msg = (
                    f"成功向 {target} 发放【{item_info['name']}】饰品 "
                    f"x{result.affected_quantity}（{quality}阶）"
                )
        else:
            expected_quantity = int(sql_message.goods_num(user_id, goods_id) or 0)
            result = admin_item_grant_service.grant(
                _admin_operation_id(event, "item-grant", user_id),
                str(get_user_id(event) or "unknown"),
                user_id,
                goods_id,
                item_info["name"],
                goods_type,
                quantity,
                expected_quantity,
                int(XiuConfig().max_goods_num),
                target_name=target,
            )
            if result.status == "inventory_full":
                msg = f"{target} 的 {item_info['name']} 已达到背包容量上限！"
            elif result.status == "state_changed":
                msg = "玩家背包状态已变化，请重新执行指令"
            elif result.status == "operation_conflict":
                msg = "本次管理员操作与已记录事件冲突"
            elif result.status == "user_missing":
                msg = f"玩家 {target} 已不存在！"
            else:
                msg = f"成功向 {target} 发放 {item_info['name']} x{result.granted_quantity}"

        await handle_send(bot, event, msg)
        await cz.finish()

    # ===== 默认给自己 =====
    is_user, self_user_info, _ = check_user(event)
    if not is_user:
        await handle_send(bot, event, "您尚未加入修仙界！")
        await cz.finish()

    self_user_id = str(self_user_info["user_id"])

    if is_accessory:
        result = _grant_admin_accessory(
            event,
            self_user_id,
            goods_id,
            item_info["name"],
            quality,
            quantity,
            "self",
        )
        if result.status == "inventory_full":
            msg = "您的饰品背包容量不足！"
        elif result.status == "state_changed":
            msg = "您的饰品状态已变化，请重新执行指令"
        elif result.status == "operation_conflict":
            msg = "本次管理员饰品操作与已记录事件冲突"
        elif result.status == "user_missing":
            msg = "您的修仙数据已不存在！"
        elif result.status == "invalid_plan":
            msg = "饰品生成结果无效，请重新执行指令"
        elif result.status == "invalid_state":
            msg = "您的饰品数据异常，请先执行背包检测"
        else:
            msg = (
                f"成功向您发放【{item_info['name']}】饰品 "
                f"x{result.affected_quantity}（{quality}阶）"
            )
    else:
        expected_quantity = int(sql_message.goods_num(self_user_id, goods_id) or 0)
        result = admin_item_grant_service.grant(
            _admin_operation_id(event, "item-grant", self_user_id),
            str(get_user_id(event) or "unknown"),
            self_user_id,
            goods_id,
            item_info["name"],
            goods_type,
            quantity,
            expected_quantity,
            int(XiuConfig().max_goods_num),
            target_name="self",
        )
        if result.status == "inventory_full":
            msg = f"您的 {item_info['name']} 已达到背包容量上限！"
        elif result.status == "state_changed":
            msg = "您的背包状态已变化，请重新执行指令"
        elif result.status == "operation_conflict":
            msg = "本次管理员操作与已记录事件冲突"
        elif result.status == "user_missing":
            msg = "您的修仙数据已不存在！"
        else:
            msg = f"成功向您发放 {item_info['name']} x{result.granted_quantity}"

    await handle_send(bot, event, msg)
    await cz.finish()


@hmll.handle(parameterless=[Cooldown(cd_time=0)])
async def hmll_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """毁灭力量 - 扣除玩家或全服物品"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    arg_list = args.extract_plain_text().split()

    if len(arg_list) < 2:
        msg = (
            "请输入正确指令！例如：\n"
            "毁灭力量 物品名 数量\n"
            "毁灭力量 物品名 数量 玩家名\n"
            "毁灭力量 物品名 数量 all"
        )
        await handle_send(bot, event, msg)
        await hmll.finish()

    goods_name = arg_list[0]

    try:
        quantity = int(arg_list[1])
        if quantity <= 0:
            await handle_send(bot, event, "数量必须大于0！")
            await hmll.finish()
    except ValueError:
        await handle_send(bot, event, "数量必须是整数！")
        await hmll.finish()

    target = arg_list[2] if len(arg_list) > 2 else None

    # 查找物品
    goods_id, item_info = items.get_data_by_item_name(goods_name)
    if not goods_id or not item_info:
        await handle_send(bot, event, f"物品 {goods_name} 不存在！")
        await hmll.finish()

    goods_id = int(goods_id)
    is_accessory = item_info.get("item_type") == "饰品"

    # ===== 全服扣除 =====
    if target and str(target).lower() == "all":
        all_users = sql_message.get_all_user_id()
        if not all_users:
            await handle_send(bot, event, "当前没有可扣除的用户。")
            await hmll.finish()

        if is_accessory:
            operator_id = str(get_user_id(event) or "unknown")
            operation_id = admin_accessory_batch_adjustment_service.find_running(
                "destroy",
                operator_id,
                goods_id,
                item_info["name"],
                0,
                quantity,
                0,
            ) or _admin_operation_id(event, "accessory-destroy-all", str(goods_id))
            users = list(all_users)

            def _work():
                return run_chunked_until_done(
                    lambda: admin_accessory_batch_adjustment_service.destroy(
                        operation_id,
                        operator_id,
                        users,
                        goods_id,
                        item_info["name"],
                        quantity,
                    )
                )

            def _done(result):
                if result.status == "operation_conflict":
                    return "本次全服饰品扣除与已记录计划冲突"
                return (
                    f"全服饰品扣除完成！已处理 {result.completed}/{result.total} 名玩家，"
                    f"共影响 {result.affected_users} 名玩家，累计扣除"
                    f"【{item_info['name']}】{result.affected_quantity} 件，"
                    f"跳过 {result.skipped_users} 名（仅背包，已装备未扣除）"
                )

            await spawn_admin_job(
                bot,
                event,
                job_key=f"accessory-destroy-all:{goods_id}:{quantity}",
                start_msg=(
                    f"🔄 全服饰品【{item_info['name']}】扣除已在后台开始"
                    f"（共 {len(users)} 人），完成后另行通知。"
                ),
                work=_work,
                done_msg=_done,
                fail_prefix="全服饰品扣除失败",
            )
            await hmll.finish()
        else:
            success_user_count = 0
            total_removed = 0
            log_context = _admin_economy_context(
                event,
                "admin_item_cost_all",
                item_id=goods_id,
                item_name=item_info["name"],
                target="all",
            )
            for uid in all_users:
                uid_str = str(uid)
                try:
                    # 普通物品：先检查数量再扣
                    have = sql_message.goods_num(uid_str, goods_id)
                    if have > 0:
                        deduct = min(quantity, have)
                        sql_message.update_back_j(
                            uid_str,
                            goods_id,
                            num=deduct,
                            log_context=log_context,
                        )
                        success_user_count += 1
                        total_removed += deduct
                except Exception as e:
                    logger.error(f"毁灭力量全服扣除失败 user_id={uid}: {e}")
            msg = f"全服扣除完成！共影响 {success_user_count} 名玩家，累计扣除 {item_info['name']} x{total_removed}"

        await handle_send(bot, event, msg)
        await hmll.finish()

    # ===== 指定玩家扣除 =====
    if target:
        user_info = sql_message.get_user_info_with_name(target)
        if not user_info:
            await handle_send(bot, event, f"玩家 {target} 不存在！")
            await hmll.finish()

        user_id = str(user_info["user_id"])

        if is_accessory:
            result = _destroy_admin_accessory(
                event,
                user_id,
                goods_id,
                item_info["name"],
                quantity,
                target,
            )
            if result.status == "state_changed":
                msg = "玩家饰品状态已变化，请重新执行指令"
            elif result.status == "operation_conflict":
                msg = "本次管理员饰品操作与已记录事件冲突"
            elif result.status == "user_missing":
                msg = f"玩家 {target} 已不存在！"
            elif result.status == "item_missing":
                msg = f"玩家 {target} 的背包中没有 {item_info['name']}！"
            elif result.status == "invalid_state":
                msg = f"{target} 的饰品数据异常，请先执行背包检测"
            else:
                msg = (
                    f"成功从 {target} 扣除【{item_info['name']}】饰品 "
                    f"x{result.affected_quantity}"
                )
                if result.affected_quantity < quantity:
                    msg += "（数量不足，已按背包可扣最大值执行）"
            await handle_send(bot, event, msg)
            await hmll.finish()
        else:
            have = sql_message.goods_num(user_id, goods_id)
            if have <= 0:
                await handle_send(bot, event, f"玩家 {target} 没有 {item_info['name']}！")
                await hmll.finish()

            result = admin_item_destroy_service.destroy(
                _admin_operation_id(event, "item-destroy", user_id),
                str(get_user_id(event) or "unknown"),
                user_id,
                goods_id,
                item_info["name"],
                item_info.get("type", ""),
                quantity,
                int(have),
                target_name=target,
            )
            if result.status == "state_changed":
                msg = "玩家背包状态已变化，请重新执行指令"
            elif result.status == "operation_conflict":
                msg = "本次管理员操作与已记录事件冲突"
            elif result.status == "user_missing":
                msg = f"玩家 {target} 已不存在！"
            elif result.status == "item_missing":
                msg = f"玩家 {target} 没有 {item_info['name']}！"
            else:
                msg = f"成功从 {target} 扣除 {item_info['name']} x{result.removed_quantity}"
            if result.succeeded and result.removed_quantity < quantity:
                msg += "（数量不足，已按实际可扣执行）"
            await handle_send(bot, event, msg)
            await hmll.finish()

    # ===== 默认扣自己 =====
    is_user, self_user_info, _ = check_user(event)
    if not is_user:
        await handle_send(bot, event, "您尚未加入修仙界！")
        await hmll.finish()

    self_user_id = str(self_user_info["user_id"])

    if is_accessory:
        result = _destroy_admin_accessory(
            event,
            self_user_id,
            goods_id,
            item_info["name"],
            quantity,
            "self",
        )
        if result.status == "state_changed":
            msg = "您的饰品状态已变化，请重新执行指令"
        elif result.status == "operation_conflict":
            msg = "本次管理员饰品操作与已记录事件冲突"
        elif result.status == "user_missing":
            msg = "您的修仙数据已不存在！"
        elif result.status == "item_missing":
            msg = f"您的背包中没有 {item_info['name']}！"
        elif result.status == "invalid_state":
            msg = "您的饰品数据异常，请先执行背包检测"
        else:
            msg = (
                f"成功从您背包扣除【{item_info['name']}】饰品 "
                f"x{result.affected_quantity}"
            )
            if result.affected_quantity < quantity:
                msg += "（数量不足，已按实际可扣执行）"
        await handle_send(bot, event, msg)
        await hmll.finish()
    else:
        have = sql_message.goods_num(self_user_id, goods_id)
        if have <= 0:
            await handle_send(bot, event, f"您没有 {item_info['name']}！")
            await hmll.finish()

        result = admin_item_destroy_service.destroy(
            _admin_operation_id(event, "item-destroy", self_user_id),
            str(get_user_id(event) or "unknown"),
            self_user_id,
            goods_id,
            item_info["name"],
            item_info.get("type", ""),
            quantity,
            int(have),
            target_name="self",
        )
        if result.status == "state_changed":
            msg = "您的背包状态已变化，请重新执行指令"
        elif result.status == "operation_conflict":
            msg = "本次管理员操作与已记录事件冲突"
        elif result.status == "user_missing":
            msg = "您的修仙数据已不存在！"
        elif result.status == "item_missing":
            msg = f"您没有 {item_info['name']}！"
        else:
            msg = f"成功从您这里扣除 {item_info['name']} x{result.removed_quantity}"
        if result.succeeded and result.removed_quantity < quantity:
            msg += "（数量不足，已按实际可扣执行）"
        await handle_send(bot, event, msg)
        await hmll.finish()


@restate.handle(parameterless=[Cooldown(cd_time=0)])
async def restate_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """重置用户状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = get_at_user_id(args)
    plain_text = (args.extract_plain_text() if args is not None else "") or ""
    plain_args = plain_text.split()
    # 无纯文本判空：QQ 官方 AT 事件可能带 mention 段，bool(args) 为真却无道号
    if not plain_args and not give_qq:
        all_users = sql_message.get_all_user_id()
        operator_id = str(get_user_id(event) or "unknown")
        max_stamina = XiuConfig().max_stamina
        running_operation = admin_player_status_batch_reset_service.find_running(
            operator_id, max_stamina
        )
        if not all_users and not running_operation:
            await handle_send(bot, event, "当前没有可重置的用户")
            await restate.finish()
        operation_id = running_operation or _admin_operation_id(
            event, "player-status-reset-all", "all"
        )
        users = tuple(all_users or ())

        def _work():
            return run_chunked_until_done(
                lambda: admin_player_status_batch_reset_service.reset(
                    operation_id,
                    operator_id,
                    users,
                    max_stamina,
                )
            )

        def _done(result):
            if result.status == "operation_conflict":
                return "本次全服状态重置与已记录计划冲突"
            return (
                f"所有用户信息重置完成！已处理 {result.completed}/{result.total} 名玩家，"
                f"成功重置 {result.reset_users} 名，跳过 {result.skipped_users} 名"
            )

        await spawn_admin_job(
            bot,
            event,
            job_key=f"player-status-reset-all:{operator_id}",
            start_msg=(
                f"🔄 全服重置状态已在后台开始（共 {len(users)} 人），"
                f"完成后另行通知；期间其他指令可正常使用。"
            ),
            work=_work,
            done_msg=_done,
            fail_prefix="全服重置状态失败",
        )
        await restate.finish()
    nick_name = plain_args[0] if plain_args else ""
    if nick_name and not give_qq:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = None
    if give_qq:
        expected_state = admin_player_status_reset_service.snapshot(give_qq)
        if expected_state is None:
            await handle_send(bot, event, "目标玩家已不存在")
            await restate.finish()
        try:
            result = admin_player_status_reset_service.reset(
                _admin_operation_id(event, "player-status-reset", str(give_qq)),
                str(get_user_id(event) or "unknown"),
                give_qq,
                expected_state,
                XiuConfig().max_stamina,
                target_name=nick_name or str(give_qq),
                force=True,
            )
        except Exception as e:
            logger.opt(exception=e).error(f"重置状态失败 user={give_qq}")
            await handle_send(bot, event, f"重置状态失败：{e}")
            await restate.finish()
        if result.status == "state_changed":
            msg = "玩家状态已变化，请重新执行指令"
        elif result.status == "operation_conflict":
            msg = "本次管理员状态重置与已记录事件冲突"
        elif result.status == "user_missing":
            msg = "目标玩家已不存在"
        elif result.succeeded:
            msg = f"{give_qq}用户信息重置成功！"
        else:
            msg = f"重置状态失败：{result.status}"
        await handle_send(bot, event, msg)
        await restate.finish()
    else:
        msg = f"对方未踏入修仙界！"
        await handle_send(bot, event, msg)
        await restate.finish()

@set_xiuxian.handle(parameterless=[Cooldown(cd_time=0)])
async def open_xiuxian_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """群修仙开关配置（默认开启；禁用列表记录关闭的群）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_msg = str(event.message)
    group_id = str(event.group_id)
    conf = JsonConfig()
    conf_data = conf.read_data()
    disabled = set(conf_data.get("group", []))

    if "启用" in group_msg:
        if group_id not in disabled:
            msg = "当前群聊修仙模组已启用，请勿重复操作！"
        else:
            conf.write_data(2, group_id)
            msg = "当前群聊修仙基础模组已启用，快发送 我要修仙 加入修仙世界吧！"
        await handle_send(bot, event, msg, md_type="修仙", k1="我要修仙", v1="我要修仙", k2="修仙帮助", v2="修仙帮助")
        await set_xiuxian.finish()

    if "禁用" in group_msg:
        if group_id in disabled:
            msg = "当前群聊修仙模组已禁用，请勿重复操作！"
        else:
            conf.write_data(1, group_id)
            msg = "当前群聊修仙基础模组已禁用！\n（娱乐功能不受影响；发送【修仙帮助】可查看开启命令）"
        await handle_send(bot, event, msg, md_type="修仙", k1="开启修仙", v1="启用修仙功能", k2="娱乐帮助", v2="娱乐帮助")
        await set_xiuxian.finish()

    msg = "指令错误，请输入：启用修仙功能/禁用修仙功能"
    await handle_send(bot, event, msg)
    await set_xiuxian.finish()

@set_private_chat.handle(parameterless=[Cooldown(cd_time=0)])
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

@set_auto_root.handle(parameterless=[Cooldown(cd_time=0)])
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

@set_auto_sect_name.handle(parameterless=[Cooldown(cd_time=0)])
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

@xiuxian_updata_level.handle(parameterless=[Cooldown(cd_time=0)])
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

@clear_xiangyuan.handle(parameterless=[Cooldown(cd_time=0)])
async def clear_xiangyuan_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = await clear_all_xiangyuan()
    await handle_send(bot, event, msg)
    await clear_xiangyuan.finish()

@xiuxian_novice.handle(parameterless=[Cooldown(cd_time=0)])
async def xiuxian_novice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置新手礼包"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    sql_message.novice_remake()
    msg = "新手礼包重置成功，所有玩家可以重新领取新手礼包！"
    await handle_send(bot, event, msg)
    await xiuxian_novice.finish()

@create_new_rift.handle(parameterless=[Cooldown(cd_time=0)])
async def create_new_rift_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """生成秘境"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    await create_rift(bot, event)

@do_work_cz.handle(parameterless=[Cooldown(cd_time=0)])
async def do_work_cz_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置所有用户的悬赏令"""
    from ..xiuxian_work import count
    sql_message.reset_work_num(count)
    msg = "用户悬赏令刷新次数重置成功"
    await handle_send(bot, event, msg)
    await do_work_cz.finish()

@training_reset.handle(parameterless=[Cooldown(cd_time=0)])
async def training_reset_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置所有用户的历练"""
    from ..xiuxian_training import training_reset_limits

    operation_id = _admin_operation_id(event, "training-reset", "all")
    operator_id = str(get_user_id(event) or "unknown")

    def _work():
        return run_chunked_until_done(
            lambda: training_reset_limits(operation_id, operator_id)
        )

    def _done(result):
        if result.status == "operation_conflict":
            return "本次历练重置与已记录事件冲突"
        return (
            f"用户历练状态重置完成：已处理 {result.completed}/{result.total} 名玩家，"
            f"重置 {result.changed} 名，跳过 {result.skipped} 名"
        )

    await spawn_admin_job(
        bot,
        event,
        job_key=f"training-reset-all:{operator_id}",
        start_msg="🔄 全服历练重置已在后台开始，完成后另行通知。",
        work=_work,
        done_msg=_done,
        fail_prefix="全服历练重置失败",
    )
    await training_reset.finish()

@tower_reset.handle(parameterless=[Cooldown(cd_time=0)])
async def tower_reset_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置所有用户的通天塔层数"""
    from ..xiuxian_tower import reset_tower_floors
    await reset_tower_floors()  # 重置通天塔层数
    msg = "用户通天塔层数重置成功"
    await handle_send(bot, event, msg)
    await tower_reset.finish()

@boss_reset.handle(parameterless=[Cooldown(cd_time=0)])
async def boss_reset_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重置所有用户的世界BOSS额度"""
    from ..xiuxian_boss import set_boss_limits_reset
    result = await set_boss_limits_reset()
    if result.status == "duplicate":
        msg = f"今日世界BOSS额度已重置，共 {result.total} 名玩家"
    else:
        msg = (
            f"用户世界BOSS额度重置完成：已处理 {result.completed}/{result.total} 名玩家，"
            f"重置 {result.changed} 名，跳过 {result.skipped} 名"
        )
    await handle_send(bot, event, msg)
    await boss_reset.finish()

@items_refresh.handle(parameterless=[Cooldown(cd_time=0)])
async def items_refresh_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """重载items"""
    items.refresh()
    msg = "重载items完成"
    await handle_send(bot, event, msg)
    await items_refresh.finish()

@blackhouse.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    
    plain_text = args.extract_plain_text().strip()

    target_user_id = None
    target_name = None

    # 1. 优先找艾特
    at_qq = get_at_user_id(args)

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

    result = admin_blackhouse_status_service.set_banned(
        _admin_operation_id(event, "blackhouse-ban", str(target_user_id)),
        str(get_user_id(event) or "unknown"),
        target_user_id,
        admin_blackhouse_status_service.snapshot(target_user_id),
        True,
    )
    if result.status == "state_changed":
        await handle_send(bot, event, "玩家封禁状态已变化，请重新执行指令")
    elif result.status == "operation_conflict":
        await handle_send(bot, event, "本次封禁操作与已记录事件冲突")
    elif result.status == "user_missing":
        await handle_send(bot, event, "该用户尚未踏入修仙界！")
    elif result.status == "unchanged":
        await handle_send(bot, event, f"道友 {target_user['user_name']} 已在小黑屋中。")
    else:
        await handle_send(bot, event, f"道友 {target_user['user_name']} 已被关入小黑屋！")


@unblackhouse.handle(parameterless=[Cooldown(cd_time=0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)
    
    plain_text = args.extract_plain_text().strip()

    target_user_id = None

    at_qq = get_at_user_id(args)

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

    result = admin_blackhouse_status_service.set_banned(
        _admin_operation_id(event, "blackhouse-unban", str(target_user_id)),
        str(get_user_id(event) or "unknown"),
        target_user_id,
        admin_blackhouse_status_service.snapshot(target_user_id),
        False,
    )
    if result.status == "state_changed":
        await handle_send(bot, event, "玩家封禁状态已变化，请重新执行指令")
    elif result.status == "operation_conflict":
        await handle_send(bot, event, "本次解封操作与已记录事件冲突")
    elif result.status == "user_missing":
        await handle_send(bot, event, "该用户尚未踏入修仙界！")
    elif result.status == "unchanged":
        await handle_send(bot, event, f"道友 {target_user['user_name']} 当前未被封禁。")
    else:
        await handle_send(bot, event, f"道友 {target_user['user_name']} 已从小黑屋释放，恢复自由！")

@view_blackhouse.handle(parameterless=[Cooldown(cd_time=0)])
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

@super_help.handle(parameterless=[Cooldown(cd_time=0)])
async def super_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """修仙管理帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    help_msg = """
**修仙管理手册**

> 管理员专用指令

**资源管理**
→ 神秘力量 [数量] all
> 全服发放或扣除灵石
→ 神秘力量 [数量] [道号]
> 给指定用户发放或扣除灵石
→ 传承力量 [数量] all
> 全服发放或扣除思恋结晶
→ 传承力量 [数量] [道号]
> 给指定用户发放或扣除思恋结晶
→ 创造力量 [物品] [数量]
> 给自己发物品
→ 创造力量 [物品] [数量] all
> 全服发物品
→ 创造力量 [物品] [数量] [道号]
> 给指定用户发物品
→ 毁灭力量 [物品] [数量]
> 给自己扣物品
→ 毁灭力量 [物品] [数量] all
> 全服扣物品
→ 毁灭力量 [物品] [数量] [道号]
> 给指定用户扣物品
→ 赠送称号 [称号] all
> 全服赠送称号
→ 赠送称号 [称号] [道号]
> 给指定用户赠送称号
注
> 数量可以为负数。

**境界管理**
→ 造化力量 [境界] [道号]
> 修改用户境界
→ 轮回力量 [1-9] [道号] - 修改用户灵根
   (1混沌 2融合 3超 4龙 5天 6千世 7万世 8永恒 9命运)
→ 修为调整 [修为数]
> 全服发修为
→ 修为调整 [道号] [修为数]
> 给指定用户发修为
注
> 修为数可以为负数。

**世界BOSS管理**
→ 世界BOSS生成 [数量]
> 生成随机境界BOSS
→ 世界BOSS指定生成 [境界] [名称]
> 生成指定BOSS
→ 世界BOSS全部生成
> 一键生成所有境界BOSS
→ 世界BOSS天罚 [编号] / 天罚世界BOSS [编号]
> 删除指定BOSS
→ 世界BOSS全部天罚 / 天罚全部世界BOSS
> 清空所有BOSS
→ 重置世界BOSS
> 重置玩家世界BOSS额度

**补偿系统管理**
→ 新增补偿 [ID] [物品] [原因] [有效期] [生效期]
> 创建新补偿
   示例
   > 新增补偿 0 灵石x100000,渡厄丹x5 维护补偿 30天 0
→ 删除补偿 [ID]
> 删除指定补偿
→ 补偿列表
> 查看所有补偿
→ 清空补偿
> 清空所有补偿数据

**礼包系统管理**
→ 新增礼包 [ID] [物品] [原因] [有效期] [生效期]
> 创建新礼包
   示例
   > 新增礼包 0 灵石x100000,渡厄丹x1 新人礼包 无限 0
→ 删除礼包 [ID]
> 删除指定礼包
→ 礼包列表
> 查看所有礼包
→ 清空礼包
> 清空所有礼包数据

**兑换码系统管理**
→ 新增兑换码 [兑换码] [物品] [使用上限] [有效期] [生效期]
> 创建新兑换码
   示例
   > 新增兑换码 XMAS2024 灵石x1000000,渡厄丹x1 100 30天 0
→ 删除兑换码 [兑换码]
> 删除指定兑换码
→ 兑换码列表
> 查看所有兑换码
→ 清空兑换码
> 清空所有兑换码数据

**邀请系统管理**
→ 邀请奖励设置 [门槛] [物品]
> 设置邀请奖励
   示例
   > 邀请奖励设置 5 渡厄丹x5,灵石x10000000
→ 邀请奖励列表
> 查看所有邀请奖励设置

**系统管理**
→ 重置状态
> 重置所有用户状态
→ 重置状态 [道号]
> 重置指定用户状态
→ 修仙适配
> 适配修仙2的境界到修仙2魔改版
→ 背包检测
> 检测背包数量、物品名和已装备物品异常并修复
→ 重载items
> 重新加载物品数据
→ 启用修仙功能 / 禁用修仙功能
> 群修仙功能开关（默认开；关闭后仅修仙帮助提示开启命令，娱乐不受限）
→ 开启进群欢迎 / 关闭进群欢迎
> 本群进群欢迎（全局默认开，欢迎消息可带关闭按钮）
→ 指令禁用 [指令/子模块,...]
> 禁用指令（可批量；xiuxian_admin 不可禁）
→ 指令解禁 [指令/子模块,...]
> 解禁指令
→ 指令列表 [页码]
> 按来源分页查看（每页30条）；指令列表 禁用 [页码] 仅看禁用；可加关键词筛选
→ 启用私聊功能 / 禁用私聊功能
> 私聊修仙功能开关
→ 开启自动灵根 / 关闭自动灵根
> 自动选择灵根开关
→ 启用自动宗名 / 禁用自动宗名
> 自动随机宗门名开关

**用户管控**
→ 小黑屋 [目标]
> 封禁指定修仙用户
→ 解除小黑屋 [目标]
> 解除封禁
→ 查看小黑屋 / 小黑屋列表
> 查看封禁名单
→ 用户伪装 [目标]
> 伪装成指定用户
→ 用户伪装 取消
> 取消当前伪装

**交易管理**
→ 系统仙肆上架 [物品名称] [价格] [数量]
> 不带数量为无限
→ 系统仙肆下架 [物品仙肆ID]
> 下架系统仙肆物品
→ 清空仙肆
> 清空所有仙肆上架并退回
→ 清空鬼市
> 清空所有道友的摆摊和求购
→ 开启拍卖
> 开启拍卖
→ 结束拍卖
> 结束拍卖
→ 封闭拍卖
> 禁止自动开启拍卖
→ 解封拍卖
> 取消禁止

**功能管理**
→ 清空仙缘
> 清除所有未领取仙缘
→ 生成秘境
> 生成新秘境
→ 开启魔修入侵
> 手动开启本期魔修入侵
→ 关闭魔修入侵
> 手动结束当前魔修入侵
→ 开启天降灵脉
> 手动开启天降灵脉
→ 关闭天降灵脉
> 手动关闭当前天降灵脉
→ 重置悬赏令
> 重置所有玩家悬赏令次数
→ 重置通天塔
> 重置玩家通天塔层数
→ 重置历练
> 重置当前历练状态
→ 重置副本 / 手动重置
> 重置副本状态
→ 重置前尘 / 重置前尘 all
> 重置所有前尘状态
→ 重置幻境
> 重置当前幻境数据
→ 清空幻境
> 仅清空幻境玩家数据
→ 重置新手礼包
> 重置新手礼包领取状态

**数据维护**
→ 转换QQID
> 将数据库中的QQ ID迁移为真实ID
→ ID更新 [旧ID] [新ID]
> 手动迁移数据库中的单个用户ID
→ ID交换 [ID1] [ID2]
> 交换数据库中的两个用户ID
→ player数据同步 / player数据同步2 / player数据同步3 / player数据同步4
> 旧版玩家数据迁移
→ 同步灵庄
> 迁移灵庄数据
→ 同步鉴石
> 迁移鉴石数据

**调试工具**
→ 消息信息
> 查看当前/引用消息事件信息
→ 取链接
> 引用一条消息，提取图片/附件链接
→ dm [Markdown内容]
> 直接发送原生Markdown
→ md模板 [模板参数]
> 测试自定义Markdown模板
→ 按钮测试 [按钮]
> 测试原生Markdown + 自定义键盘
→ 点歌配置 查看
> 查看点歌配置
→ 点歌配置 设置 song_limit 10
> 设置单次点歌数量
→ 点歌配置 设置 default_platform netease
> 设置默认点歌平台
→ 点歌配置 设置 page_size 5
> 设置点歌列表分页数量

**广播管理**
→ 群聊广播 [时间] [内容]
> 向群聊广播，每群一次
→ 私聊广播 [时间] [内容]
> 向私聊广播，每人一次
→ 全局广播 [时间] [内容]
> 群聊+私聊全局广播
   示例
   > 群聊广播 1天 服务器将在今晚23:00维护
   时间格式
   > 1天 / 1小时 / 1分钟 / 1天1小时10分钟；不填时默认1天
→ 查看广播 / 广播列表
> 查看当前广播任务
→ 取消广播 [广播ID]
> 取消指定广播
→ 清空广播 [群聊/私聊/全局/全部]
> 清空指定类型广播任务

**状态与更新**
→ 插件帮助
> 查看插件状态帮助
→ bot信息
> 获取机器人和修仙数据
→ 系统信息
> 获取系统信息
→ ping测试
> 测试网络延迟
→ 更新日志
> 获取版本日志
→ 版本查询
> 获取最近发布的版本
→ 版本更新 [版本]
> 指定版本或更新最新版本
→ 检测更新
> 检测是否需要更新

---
> [] 表示必填参数，() 表示可选参数。
> GitHub 项目：liyw0205/nonebot_plugin_xiuxian_2_pmv
    """.strip()

    page = parse_page_arg(args.extract_plain_text())
    help_msg, page, total_pages = paginate_text_blocks(help_msg, page, per_page=4)
    help_msg = f"{help_msg}\n\n发送“修仙手册 页码”可跳转页面。"
    button_kwargs = build_pagination_buttons(
        "修仙手册",
        page,
        total_pages,
        extras=[
            ("广播", "广播帮助"),
            ("补偿", "补偿管理"),
            ("插件", "插件帮助"),
            ("查看", "查看广播"),
        ],
    )
    await send_help_message(
        bot, event, help_msg,
        **button_kwargs
    )
    await super_help.finish()

mb_template_test = on_command("md模板", permission=SUPERUSER, priority=5, block=True)
@mb_template_test.handle(parameterless=[Cooldown(cd_time=0)])
async def mb_template_test_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    使用自定义Markdown模板发送消息，并支持按钮
    """
    args_str = re.sub(r'mqqapi:/aio', 'mqqapi://aio', args.extract_plain_text())
    args_str = args_str.replace("\\r", "\r").replace('\\"', '"').replace(':/', '://').replace(':///', '://')
    if not args_str:
        await delivery_service.reply(bot, event, "请提供模板参数，格式如下：mid=模板ID bid=按钮ID k=a,v=\"xx\" k=b k=c,v=x k=d,v=[\"xx\",\"xx\"] button_id=按钮ID")
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
        await delivery_service.reply(bot, event, "请提供模板ID (mid=模板ID)")
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

    logger.debug(f"dm markdown模板参数：传入={args_str!r}，解析={params!r}")
    try:
        msg = MessageSegment.markdown_template(bot, template_id, params, button_id)
        await delivery_service.reply(bot, event, msg)
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

@keyboard_test_cmd.handle(parameterless=[Cooldown(cd_time=0.5)])
async def keyboard_test_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)

    raw = args.extract_plain_text() if hasattr(args, "extract_plain_text") else str(args)
    rows = _parse_keyboard_test_rows(raw)

    try:
        msg = MessageSegment.markdown_keyboard(bot, " ", rows)
        await delivery_service.reply(bot, event, msg)
    except Exception as e:
        err = str(e)
        logger.error(f"按钮测试发送失败: {err}")

        reason = "按钮测试发送失败，请确认当前为 QQ 官方适配器且平台支持自定义键盘。"
        m_msg = re.search(r"message=([^,>]+)", err)
        m_code = re.search(r"code=(\d+)", err)
        if m_msg:
            reason = m_msg.group(1).strip()
            if m_code:
                reason = f"\n{reason}\n错误码：{m_code.group(1)}"

        await handle_send(bot, event, reason)


@dm_command.handle(parameterless=[Cooldown(cd_time=0.5)])
async def dm_command_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    bot, _ = await assign_bot(bot=bot, event=event)

    text = str(args).strip()
    logger.info(f"收到 {text}")

    if not text:
        await handle_send(bot, event, "用法：dm Markdown内容\n示例：dm # 你好")
        return

    # 只修 mqqapi inlinecmd，别再全局 replace(':/', '://')
    text = fix_mqqapi_inlinecmd_links(text)

    logger.info(f"处理 {text}")

    try:
        msg = MessageSegment.markdown(bot, text)
        logger.info(f"转换 {msg}")
        await delivery_service.reply(bot, event, msg)

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

    if not arg_text and not has_at_user(args):
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
    at_qq = get_at_user_id(args)

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


@migrate_qqid_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def migrate_qqid_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """将数据库中的QQ user_id迁移为真实ID"""
    bot, _ = await assign_bot(bot=bot, event=event)
    if XiuConfig().gsk_link:
        await handle_send(bot, event, "开始执行QQID迁移，正在更新 SQLite 数据库，请稍候...")
    else:
        await handle_send(bot, event, "当前gsk地址为空，请先修改配置gsk_link")
        await migrate_qqid_cmd.finish()

    ok, msg = await asyncio.to_thread(migrate_user_id_to_openid)
    await handle_send(bot, event, msg)
    await migrate_qqid_cmd.finish()


@update_id_cmd.handle(parameterless=[Cooldown(cd_time=0)])
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

    await handle_send(bot, event, f"开始执行手动ID更新：{old_id} -> {new_id}\n正在校验并更新 SQLite，请稍候...")

    ok, msg = await asyncio.to_thread(migrate_single_user_id, old_id, new_id)
    await handle_send(bot, event, msg)
    await update_id_cmd.finish()

@swap_id_cmd.handle(parameterless=[Cooldown(cd_time=0)])
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
    await handle_send(bot, event, f"开始执行ID交换：{id1} - {id2}\n正在校验并更新 SQLite，请稍候...")

    ok, msg = await asyncio.to_thread(swap_two_user_ids, id1, id2)
    await handle_send(bot, event, msg)
    await swap_id_cmd.finish()

@group_broadcast_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def group_broadcast_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)

    raw = str(args).strip()
    duration_minutes, content = parse_broadcast_duration_and_content(raw)

    if not content:
        await handle_send(
            bot,
            event,
            "用法：群聊广播 [时间] 广播内容\n"
            "\n示例：\n"
            "群聊广播 广播测试\n"
            "群聊广播 1天 广播测试\n"
            "群聊广播 1小时 广播测试\n"
            "群聊广播 1分钟 广播测试\n"
            "群聊广播 1天1小时10分钟 广播测试"
        )
        return

    msg = await start_broadcast(
        bot,
        "group",
        fix_mqqapi_inlinecmd_links(content),
        duration_minutes=duration_minutes,
    )
    await handle_send(bot, event, msg)


@private_broadcast_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def private_broadcast_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)

    raw = str(args).strip()
    duration_minutes, content = parse_broadcast_duration_and_content(raw)

    if not content:
        await handle_send(
            bot,
            event,
            "用法：私聊广播 [时间] 广播内容\n"
            "\n示例：\n"
            "私聊广播 广播测试\n"
            "私聊广播 1天 广播测试\n"
            "私聊广播 1小时 广播测试\n"
            "私聊广播 1分钟 广播测试\n"
            "私聊广播 1天1小时10分钟 广播测试"
        )
        return

    msg = await start_broadcast(
        bot,
        "private",
        fix_mqqapi_inlinecmd_links(content),
        duration_minutes=duration_minutes,
    )
    await handle_send(bot, event, msg)


@global_broadcast_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def global_broadcast_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)

    raw = str(args).strip()
    duration_minutes, content = parse_broadcast_duration_and_content(raw)

    if not content:
        await handle_send(
            bot,
            event,
            "用法：全局广播 [时间] 广播内容\n"
            "\n示例：\n"
            "全局广播 广播测试\n"
            "全局广播 1天 广播测试\n"
            "全局广播 1小时 广播测试\n"
            "全局广播 1分钟 广播测试\n"
            "全局广播 1天1小时10分钟 广播测试"
        )
        return

    msg = await start_broadcast(
        bot,
        "global",
        fix_mqqapi_inlinecmd_links(content),
        duration_minutes=duration_minutes,
    )
    await handle_send(bot, event, msg)


@view_broadcast_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def view_broadcast_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
):
    bot, _ = await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, format_broadcast_status())


@cancel_broadcast_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def cancel_broadcast_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)

    bid = args.extract_plain_text().strip()
    await handle_send(bot, event, cancel_broadcast(bid))


@clear_broadcast_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def clear_broadcast_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
    args: Message = CommandArg(),
):
    bot, _ = await assign_bot(bot=bot, event=event)

    raw = args.extract_plain_text().strip()
    kind = parse_clear_broadcast_kind(raw)

    await handle_send(bot, event, clear_broadcast(kind))


@broadcast_help_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def broadcast_help_cmd_(
    bot: Bot,
    event: GroupMessageEvent | PrivateMessageEvent,
):
    bot, _ = await assign_bot(bot=bot, event=event)

    msg = """
**广播系统帮助**

**创建广播**

**群聊广播**
指令：
群聊广播 [时间] 广播内容

说明：
只向群聊/频道群聊发送广播。
每个群只发送一次。
到期后不再继续补发。

示例：
群聊广播 服务器将在今晚 23:00 维护，请提前下线。
群聊广播 1天 服务器将在今晚 23:00 维护，请提前下线。
群聊广播 1小时 临时通知：活动即将开启。
群聊广播 1分钟 测试广播。
群聊广播 1天1小时10分钟 长时间广播测试。


**私聊广播**
指令：
私聊广播 [时间] 广播内容

说明：
只向私聊/频道私信用户发送广播。
每个用户只发送一次。
到期后不再继续补发。

示例：
私聊广播 今日活动已开启，记得上线领取奖励。
私聊广播 1小时 今日活动已开启，记得上线领取奖励。


**全局广播**
指令：
全局广播 [时间] 广播内容

说明：
同时面向群聊和私聊广播。
每个群/每个用户只发送一次。
到期后不再继续补发。

示例：
全局广播 # 系统通知
今日更新已完成，祝各位道友修仙愉快。

全局广播 1天 # 系统通知
今日更新已完成，祝各位道友修仙愉快。


**广播时间格式**

时间格式：
x天/x小时/x分钟

可以顺序组合：
1天
1小时
1分钟
1天10分钟
1天1小时10分钟

没有时间参数时：
默认 1 天，也就是 1440 分钟。

注意：
时间参数只会解析广播内容最开头的连续时间格式。
例如：
群聊广播 1天 广播测试内容
会解析为有效期 1 天，内容为“广播测试内容”。

如果开头不是时间格式：
群聊广播 广播测试 1天
则不会解析时间，默认有效期 1 天，整段都是广播内容。


**查看广播**

指令：
查看广播

或：
广播列表

说明：
查看当前内存中的广播任务，包括：
- 广播ID
- 状态
- 广播类型
- 适配器
- 是否 Markdown
- 有效时长
- 过期时间
- 剩余时间
- 已发群数量
- 已发用户数量
- 错误数量
- 内容预览


**取消广播**

指令：
取消广播 广播ID

示例：
取消广播 BC12AB34CD

说明：
取消指定广播后，不再自动补发。
已发送过的群/用户不会撤回。


**清空广播**

指令：
清空广播 [类型]

支持类型：
- 群聊
- 私聊
- 全局
- 全部
- all

示例：
清空广播 群聊
清空广播 私聊
清空广播 全局
清空广播 全部
清空广播 all
清空广播

说明：
不填写类型时，默认清空全部广播任务。


**发送机制说明**

【QQ适配器】
QQ官方适配器无法完全主动发送广播。
创建广播时：
- 只会向最近1分钟内有消息的群/用户发送。
- 后续如果某个未发送过的群/用户再次发消息，会自动补发。
- 每个群/每个用户只发一次。
- 广播过期后不再补发。

【OneBot V11适配器】
OB11支持主动发送。
创建广播时：
- 会读取历史消息中出现过的群ID/用户ID并发送。
- 后续如果出现新群/新用户，会自动补发。
- 每个群/每个用户只发一次。
- 广播过期后不再补发。


**Markdown说明**

如果配置中 markdown_status = True：
- QQ适配器：使用原生Markdown发送。
- OB11适配器：使用合并转发方式发送Markdown内容。

如果 markdown_status = False：
- 使用普通文本消息发送。


**注意事项**

1. 广播任务只保存在内存中。
   机器人重启后，广播任务会自动丢失。

2. 同一个广播任务中：
   每个群只发一次，每个用户只发一次。

3. QQ适配器补发依赖用户/群的新消息。
   如果某个群或用户一直没有发消息，就不会触发补发。

4. 被屏蔽的群聊或私聊不会补发广播。

5. 如果需要停止后续补发，请使用：
   取消广播 广播ID

6. 如果需要直接清空广播任务，请使用：
   清空广播
""".strip()

    await send_help_message(
        bot, event, msg,
        k1="查看", v1="查看广播",
        k2="取消", v2="取消广播",
        k3="清空", v3="清空广播"
    )
