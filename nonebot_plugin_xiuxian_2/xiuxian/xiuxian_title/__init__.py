"""
称号系统 & 成就系统
- 查看称号列表（分页）
- 装备/卸下称号
- 称号帮助
- 自动检查成就解锁
- 超管赠送称号
"""
from nonebot import on_command
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import (
    Bot,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import (
    check_user, handle_send
)
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from .title_data import (
    get_all_titles, get_title_by_id,
    check_and_unlock_titles, get_user_unlocked_titles,
    get_user_equipped_title, equip_title, unequip_title,
    refresh_title_cache, find_title_id_by_name_or_id, grant_title_to_user
)

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()

# ===== 注册命令 =====
title_list_cmd = on_command("我的称号", aliases={"称号列表", "查看称号"}, priority=5, block=True)
title_equip_cmd = on_command("装备称号", priority=5, block=True)
title_unequip_cmd = on_command("卸下称号", aliases={"取消称号"}, priority=5, block=True)
title_help_cmd = on_command("称号帮助", priority=15, block=True)
title_refresh_cmd = on_command("刷新称号", priority=5, block=True)
title_check_cmd = on_command("检查称号", aliases={"检测称号"}, priority=5, block=True)
title_info_cmd = on_command("称号详情", priority=5, block=True)
title_grant_cmd = on_command("赠送称号", permission=SUPERUSER, priority=5, block=True)


@title_list_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def title_list_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看我的称号（分页）"""
    bot, _ = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await title_list_cmd.finish()

    user_id = user_info['user_id']

    # 页码解析
    arg_text = args.extract_plain_text().strip()
    page = 1
    if arg_text.isdigit():
        page = max(1, int(arg_text))

    # 自动检查新称号
    newly_unlocked = check_and_unlock_titles(user_id)

    # 获取已解锁称号
    unlocked_ids = get_user_unlocked_titles(user_id)
    equipped_id = get_user_equipped_title(user_id)
    all_titles = get_all_titles()

    # 新解锁提示
    new_msg = ""
    if newly_unlocked:
        new_msg = "🎉 恭喜解锁新称号：\n"
        for t in newly_unlocked:
            new_msg += f"  【{t['name']}】 - {t['desc']}\n"
        new_msg += "\n"

    if not unlocked_ids:
        msg_text = f"{new_msg}你还没有解锁任何称号！\n继续修仙探索，解锁更多称号吧！\n发送【称号帮助】查看称号获取方式"
        await handle_send(bot, event, msg_text, md_type="修仙", k1="帮助", v1="称号帮助", k2="存档", v2="我的修仙信息", k3="帮助", v3="修仙帮助")
        await title_list_cmd.finish()

    # 构建有效称号列表
    all_unlocked = []
    for tid in unlocked_ids:
        t = all_titles.get(str(tid))
        if t:
            all_unlocked.append((str(tid), t))

    # 分页
    per_page = 10
    total = len(all_unlocked)
    total_page = max(1, (total + per_page - 1) // per_page)
    if page > total_page:
        page = total_page

    start = (page - 1) * per_page
    end = start + per_page
    show_list = all_unlocked[start:end]

    # 构建消息
    title_lines = []
    title_lines.append(f"{new_msg}🏅【我的称号】共{total}个（第{page}/{total_page}页）\n═════════════")

    for idx, (tid, tdata) in enumerate(show_list, start=start + 1):
        equipped_mark = " ⭐已装备" if str(tid) == str(equipped_id) else ""
        title_lines.append(
            f"{idx}. 【{tdata['name']}】{equipped_mark}\n"
            f"   {tdata['desc']}"
        )

    title_lines.append("═════════════")
    title_lines.append("发送【我的称号 页码】翻页（如：我的称号 2）")
    title_lines.append("发送【装备称号 + 名称】装备称号")
    title_lines.append("发送【称号详情 + 名称】查看详情")

    msg_text = "\n".join(title_lines)
    await handle_send(bot, event, msg_text, md_type="修仙", k1="装备", v1="装备称号", k2="帮助", v2="称号帮助", k3="存档", v3="我的修仙信息")
    await title_list_cmd.finish()


@title_equip_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def title_equip_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """装备称号"""
    bot, _ = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await title_equip_cmd.finish()

    user_id = user_info['user_id']
    title_name_or_id = args.extract_plain_text().strip()

    if not title_name_or_id:
        msg_text = "请输入要装备的称号名称或ID！\n例如：装备称号 持之以恒"
        await handle_send(bot, event, msg_text, md_type="修仙", k1="称号", v1="我的称号", k2="帮助", v2="称号帮助", k3="存档", v3="我的修仙信息")
        await title_equip_cmd.finish()

    success, result_msg = equip_title(user_id, title_name_or_id)
    await handle_send(bot, event, result_msg, md_type="修仙", k1="称号", v1="我的称号", k2="卸下", v2="卸下称号", k3="存档", v3="我的修仙信息")
    await title_equip_cmd.finish()


@title_unequip_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def title_unequip_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """卸下称号"""
    bot, _ = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await title_unequip_cmd.finish()

    user_id = user_info['user_id']
    success, result_msg = unequip_title(user_id)
    await handle_send(bot, event, result_msg, md_type="修仙", k1="称号", v1="我的称号", k2="装备", v2="装备称号", k3="存档", v3="我的修仙信息")
    await title_unequip_cmd.finish()


@title_check_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def title_check_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """手动检查可解锁称号"""
    bot, _ = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await title_check_cmd.finish()

    user_id = user_info['user_id']
    newly_unlocked = check_and_unlock_titles(user_id)

    if newly_unlocked:
        msg_text = "🎉 检查完成，恭喜解锁新称号：\n"
        for t in newly_unlocked:
            msg_text += f"\n🏅【{t['name']}】\n   {t['desc']}"
        msg_text += "\n\n发送【装备称号 + 名称】装备称号"
    else:
        unlocked = get_user_unlocked_titles(user_id)
        msg_text = f"检查完成，当前已解锁{len(unlocked)}个称号，暂无新称号解锁。\n继续努力修仙吧！"

    await handle_send(bot, event, msg_text, md_type="修仙", k1="称号", v1="我的称号", k2="帮助", v2="称号帮助", k3="存档", v3="我的修仙信息")
    await title_check_cmd.finish()


@title_info_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def title_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看称号详情"""
    bot, _ = await assign_bot(bot=bot, event=event)
    title_name_or_id = args.extract_plain_text().strip()

    if not title_name_or_id:
        msg_text = "请输入要查看的称号名称或ID！\n例如：称号详情 持之以恒"
        await handle_send(bot, event, msg_text)
        await title_info_cmd.finish()

    # 通过ID或名称查找
    title_id = find_title_id_by_name_or_id(title_name_or_id)
    if not title_id:
        await handle_send(bot, event, "未找到该称号！")
        await title_info_cmd.finish()

    title_data = get_title_by_id(title_id)
    msg_text = (
        f"🏅 称号详情\n"
        f"═════════════\n"
        f"名称：{title_data['name']}\n"
        f"描述：{title_data['desc']}\n"
        f"获取条件：{title_data.get('condition', '无')}\n"
        f"═════════════"
    )

    await handle_send(bot, event, msg_text, md_type="修仙", k1="称号", v1="我的称号", k2="装备", v2=f"装备称号 {title_data['name']}", k3="帮助", v3="称号帮助")
    await title_info_cmd.finish()


@title_grant_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def title_grant_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    赠送称号
    用法：
    赠送称号 称号名/称号ID [用户/@/all]
    目标为空默认给自己
    """
    bot, _ = await assign_bot(bot=bot, event=event)
    plain_text = args.extract_plain_text().strip()
    if not plain_text:
        await handle_send(bot, event, "用法：赠送称号 称号名/称号ID [用户/@/all]\n示例：赠送称号 持之以恒 all")
        return

    parts = plain_text.split()
    title_input = parts[0]
    target = parts[1] if len(parts) >= 2 else None

    # 解析称号
    title_id = find_title_id_by_name_or_id(title_input)
    if not title_id:
        await handle_send(bot, event, f"称号不存在：{title_input}")
        return
    title_data = get_title_by_id(title_id)
    title_name = title_data["name"]

    # 优先解析@
    at_qq = None
    for seg in args:
        if seg.type == "at":
            at_qq = seg.data.get("qq", "")
            break

    # 全服
    if target and target.lower() == "all":
        all_users = sql_message.get_all_user_id()
        success_count = 0
        repeat_or_fail = 0
        for uid in all_users:
            try:
                ok, _ = grant_title_to_user(str(uid), title_id)
                if ok:
                    success_count += 1
                else:
                    repeat_or_fail += 1
            except Exception:
                repeat_or_fail += 1

        await handle_send(bot, event, f"全服赠送称号【{title_name}】完成：成功{success_count}，重复/失败{repeat_or_fail}")
        return

    # 单人目标解析
    target_user = None
    if at_qq:
        target_user = sql_message.get_user_info_with_id(at_qq)
    elif target:
        target_user = sql_message.get_user_info_with_name(target)
    else:
        # 默认自己
        _, me, _ = check_user(event)
        target_user = me

    if not target_user:
        await handle_send(bot, event, "未找到目标用户（或对方未踏入修仙界）")
        return

    ok, msg = grant_title_to_user(str(target_user["user_id"]), title_id)
    if ok:
        await handle_send(bot, event, f"成功给 {target_user['user_name']} 赠送称号【{title_name}】")
    else:
        await handle_send(bot, event, f"赠送失败：{msg}")


@title_refresh_cmd.handle(parameterless=[Cooldown(cd_time=30)])
async def title_refresh_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """刷新称号数据缓存"""
    bot, _ = await assign_bot(bot=bot, event=event)
    refresh_title_cache()
    all_titles = get_all_titles()
    msg_text = f"称号数据已刷新！当前共有{len(all_titles)}个称号。"
    await handle_send(bot, event, msg_text)
    await title_refresh_cmd.finish()


__title_help__ = """
【称号系统】🏅
════════════
🌟 查看称号
→ 发送"我的称号"查看第一页
→ 发送"我的称号 2"查看第二页（每页10个）
→ 发送"称号详情 + 名称"查看详情

🌟 装备称号
→ 发送"装备称号 + 名称"
→ 装备后将在修仙信息中显示
→ 同时只能装备一个称号

🌟 卸下称号
→ 发送"卸下称号"取消当前装备

🌟 检查称号
→ 发送"检查称号"手动检测新称号

🌟 称号获取
→ 通过统计数据达成条件自动解锁
→ 条件包括：签到、历练、战斗、双修、
  炼丹、秘境、BOSS讨伐、境界等
→ 达成条件后自动解锁，无需手动领取
""".strip()


@title_help_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def title_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """称号帮助"""
    bot, _ = await assign_bot(bot=bot, event=event)
    await handle_send(bot, event, __title_help__)
    await title_help_cmd.finish()