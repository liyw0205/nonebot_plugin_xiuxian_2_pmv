"""
称号系统 & 成就系统
- 查看称号列表（分页）
- 装备/卸下称号
- 称号帮助
- 自动检查成就解锁
- 超管赠送称号
"""
from ..on_compat import on_command
from nonebot.log import logger
from nonebot.params import CommandArg
from nonebot.permission import SUPERUSER

from ..adapter_compat import (
    Bot,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    get_at_user_id,
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import (
    check_user, handle_send, send_help_message
)
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from .title_data import (
    get_all_titles, get_title_by_id,
    check_and_unlock_titles, get_user_unlocked_titles,
    get_user_equipped_title,
    refresh_title_cache, find_title_id_by_name_or_id,
    get_title_achievement_records, find_unlockable_titles
)
from ...paths import get_paths
from .title_transaction_service import TitleTransactionService

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()
title_transaction_service = TitleTransactionService(get_paths().player_db)


def _title_operation_id(event, action: str, user_id: str) -> str:
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    return f"title-{action}:{event_id or __import__('time').time_ns()}:{user_id}"


def _unlock_titles_from_event(event, user_id: str):
    expected = get_user_unlocked_titles(user_id)
    unlockable = find_unlockable_titles(user_id)
    if not unlockable:
        return []
    title_ids = [str(title["id"]) for title in unlockable]
    result = title_transaction_service.unlock_batch(
        _title_operation_id(event, "unlock", str(user_id)), user_id, expected, title_ids
    )
    return unlockable if result.succeeded else []

# ===== 注册命令 =====
title_list_cmd = on_command("我的称号", aliases={"称号列表", "查看称号"}, priority=5, block=True)
title_equip_cmd = on_command("装备称号", priority=5, block=True)
title_unequip_cmd = on_command("卸下称号", aliases={"取消称号"}, priority=5, block=True)
title_help_cmd = on_command("称号帮助", priority=15, block=True)
title_refresh_cmd = on_command("刷新称号", priority=5, block=True)
title_check_cmd = on_command("检查称号", aliases={"检测称号"}, priority=5, block=True)
title_info_cmd = on_command("称号详情", priority=5, block=True)
title_grant_cmd = on_command("赠送称号", permission=SUPERUSER, priority=5, block=True)
achievement_list_cmd = on_command("我的成就", aliases={"成就列表", "查看成就"}, priority=5, block=True)
achievement_check_cmd = on_command("检查成就", aliases={"检测成就"}, priority=5, block=True)


def _parse_page_and_filter(arg_text: str):
    page = 1
    filter_text = ""
    for part in arg_text.split():
        if part.isdigit():
            page = max(1, int(part))
        elif part:
            filter_text = part
    return page, filter_text


def _filter_achievement_records(records: list[dict], filter_text: str) -> list[dict]:
    if not filter_text or filter_text in {"全部", "所有"}:
        return records
    if filter_text in {"已完成", "完成", "已解锁", "解锁"}:
        return [record for record in records if record["unlocked"]]
    if filter_text in {"未完成", "未解锁", "进行中"}:
        return [record for record in records if not record["unlocked"]]
    return [
        record for record in records
        if filter_text in record["category"]
        or filter_text in record["name"]
        or filter_text in record["condition"]
    ]


def _format_achievement_record(record: dict) -> str:
    status = "已完成" if record["unlocked"] else ("可解锁" if record["satisfied"] else "未完成")
    percent = int(record["ratio"] * 100)
    lines = [
        f"【{record['name']}】{status} {percent}%",
        f"  {record['desc']}",
    ]
    for item in record["progress"]:
        lines.append(f"  - {item['key']}：{item['display']}")
    return "\n".join(lines)


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
    newly_unlocked = _unlock_titles_from_event(event, user_id)

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
        msg_text = f"{new_msg}你还没有解锁任何称号！\n继续修仙探索，解锁更多称号吧！\n获取方式：称号帮助"
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


@title_equip_cmd.handle(parameterless=[Cooldown(cd_time=0)])
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

    title_id = find_title_id_by_name_or_id(title_name_or_id)
    if not title_id:
        await handle_send(bot, event, "称号不存在！")
        await title_equip_cmd.finish()
    operation_id = _title_operation_id(event, "equip", str(user_id))
    # 先回放：成功后 equipped 变化会挡住同事件幂等。
    prior = title_transaction_service.get_result(operation_id)
    title_data = get_title_by_id(title_id) or {}
    if prior is not None and prior.succeeded:
        await handle_send(
            bot, event,
            f"成功装备称号【{title_data.get('name', prior.title_id or title_id)}】！\n"
            f"该装备请求已经处理，无需重复提交。",
            md_type="修仙", k1="称号", v1="我的称号", k2="卸下", v2="卸下称号", k3="存档", v3="我的修仙信息",
        )
        await title_equip_cmd.finish()
    unlocked = get_user_unlocked_titles(user_id)
    equipped = get_user_equipped_title(user_id) or ""
    result = title_transaction_service.equip(
        operation_id, user_id, unlocked, equipped, title_id
    )
    messages = {
        "applied": f"成功装备称号【{title_data.get('name', title_id)}】！",
        "duplicate": f"成功装备称号【{title_data.get('name', title_id)}】！\n该装备请求已经处理，无需重复提交。",
        "already_equipped": f"称号【{title_data.get('name', title_id)}】已在装备中。",
        "title_locked": "你还未解锁该称号！",
        "state_changed": "称号状态已变化，请重新查看后再试。",
        "operation_conflict": "本次称号请求与已处理记录冲突，请重新操作。",
    }
    result_msg = messages.get(result.status, "装备称号失败，请重试。")
    await handle_send(bot, event, result_msg, md_type="修仙", k1="称号", v1="我的称号", k2="卸下", v2="卸下称号", k3="存档", v3="我的修仙信息")
    await title_equip_cmd.finish()


@title_unequip_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def title_unequip_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """卸下称号"""
    bot, _ = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await title_unequip_cmd.finish()

    user_id = user_info['user_id']
    operation_id = _title_operation_id(event, "unequip", str(user_id))
    prior = title_transaction_service.get_result(operation_id)
    if prior is not None and prior.succeeded:
        title_data = get_title_by_id(prior.title_id) or {}
        await handle_send(
            bot, event,
            f"成功卸下称号【{title_data.get('name', prior.title_id)}】！\n"
            f"该卸下请求已经处理，无需重复提交。",
            md_type="修仙", k1="称号", v1="我的称号", k2="装备", v2="装备称号", k3="存档", v3="我的修仙信息",
        )
        await title_unequip_cmd.finish()
    equipped = get_user_equipped_title(user_id) or ""
    result = title_transaction_service.unequip(operation_id, user_id, equipped)
    title_data = get_title_by_id(equipped) or {}
    messages = {
        "applied": f"成功卸下称号【{title_data.get('name', equipped)}】！",
        "duplicate": f"成功卸下称号【{title_data.get('name', result.title_id)}】！\n该卸下请求已经处理，无需重复提交。",
        "not_equipped": "你当前没有装备任何称号！",
        "state_changed": "称号状态已变化，请重新查看后再试。",
        "operation_conflict": "本次称号请求与已处理记录冲突，请重新操作。",
    }
    result_msg = messages.get(result.status, "卸下称号失败，请重试。")
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
    newly_unlocked = _unlock_titles_from_event(event, user_id)

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


@achievement_list_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def achievement_list_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看称号成就进度"""
    bot, _ = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await achievement_list_cmd.finish()

    user_id = user_info["user_id"]
    newly_unlocked = _unlock_titles_from_event(event, user_id)
    records = get_title_achievement_records(user_id)
    total_count = len(records)
    unlocked_count = len([record for record in records if record["unlocked"]])

    page, filter_text = _parse_page_and_filter(args.extract_plain_text().strip())
    records = _filter_achievement_records(records, filter_text)
    records.sort(key=lambda item: (item["unlocked"], -item["ratio"], item["category"], item["id"]))

    per_page = 6
    total_page = max(1, (len(records) + per_page - 1) // per_page)
    page = min(page, total_page)
    show_records = records[(page - 1) * per_page: page * per_page]

    title = f"【我的成就】{unlocked_count}/{total_count}"
    if filter_text:
        title += f"｜筛选：{filter_text}"
    lines = [title, f"第{page}/{total_page}页"]
    if newly_unlocked:
        lines.append("本次新解锁：")
        lines.extend(f"【{title_data['name']}】{title_data['desc']}" for title_data in newly_unlocked)

    if show_records:
        lines.extend(_format_achievement_record(record) for record in show_records)
    else:
        lines.append("暂无符合条件的成就。")

    lines.append("═════════════")
    lines.append("发送【我的成就 页码】翻页；可加筛选：已完成、未完成、境界突破、战斗挑战等")
    msg_text = "\n\n".join(lines)
    await handle_send(
        bot, event, msg_text, md_type="修仙",
        k1="检查", v1="检查成就",
        k2="称号", v2="我的称号",
        k3="帮助", v3="称号帮助"
    )
    await achievement_list_cmd.finish()


@achievement_check_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def achievement_check_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """手动检查成就称号"""
    bot, _ = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await achievement_check_cmd.finish()

    user_id = user_info["user_id"]
    newly_unlocked = _unlock_titles_from_event(event, user_id)
    records = get_title_achievement_records(user_id)
    unlocked_count = len([record for record in records if record["unlocked"]])

    if newly_unlocked:
        lines = ["检查完成，解锁新成就："]
        lines.extend(f"【{title_data['name']}】{title_data['desc']}" for title_data in newly_unlocked)
    else:
        lines = [f"检查完成，当前已完成{unlocked_count}/{len(records)}个成就，暂无新成就解锁。"]

    await handle_send(
        bot, event, "\n".join(lines), md_type="修仙",
        k1="成就", v1="我的成就",
        k2="称号", v2="我的称号",
        k3="帮助", v3="称号帮助"
    )
    await achievement_check_cmd.finish()


@title_info_cmd.handle(parameterless=[Cooldown(cd_time=0)])
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
        f"【称号详情】\n"
        f"名称：{title_data['name']}\n"
        f"描述：{title_data['desc']}\n"
        f"获取条件：{title_data.get('condition', '无')}"
    )

    await handle_send(bot, event, msg_text, md_type="修仙", k1="称号", v1="我的称号", k2="装备", v2=f"装备称号 {title_data['name']}", k3="帮助", v3="称号帮助")
    await title_info_cmd.finish()


@title_grant_cmd.handle(parameterless=[Cooldown(cd_time=0)])
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
    at_qq = get_at_user_id(args)

    # 全服
    if target and target.lower() == "all":
        all_users = sql_message.get_all_user_id() or []
        if not all_users:
            await handle_send(bot, event, "当前没有可赠送的用户")
            return
        users = [str(u) for u in all_users]
        title_id_local = title_id
        title_name_local = title_name

        def _work():
            success_count = 0
            repeat_or_fail = 0
            for uid in users:
                try:
                    unlocked = get_user_unlocked_titles(str(uid))
                    result = title_transaction_service.grant(
                        _title_operation_id(event, f"grant-{title_id_local}", str(uid)),
                        uid,
                        unlocked,
                        title_id_local,
                    )
                    if result.status in {"applied", "duplicate"}:
                        success_count += 1
                    else:
                        repeat_or_fail += 1
                except Exception:
                    repeat_or_fail += 1
            return success_count, repeat_or_fail

        def _done(pair):
            success_count, repeat_or_fail = pair
            return (
                f"全服赠送称号【{title_name_local}】完成："
                f"成功{success_count}，重复/失败{repeat_or_fail}"
            )

        from ..xiuxian_utils.bg_jobs import spawn_admin_job

        await spawn_admin_job(
            bot,
            event,
            job_key=f"title-grant-all:{title_id}",
            start_msg=(
                f"🔄 全服赠送称号【{title_name}】已在后台开始"
                f"（共 {len(users)} 人），完成后另行通知。"
            ),
            work=_work,
            done_msg=_done,
            fail_prefix="全服赠送称号失败",
        )
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

    target_id = str(target_user["user_id"])
    result = title_transaction_service.grant(
        _title_operation_id(event, f"grant-{title_id}", target_id),
        target_id,
        get_user_unlocked_titles(target_id),
        title_id,
    )
    if result.status in {"applied", "duplicate"}:
        await handle_send(bot, event, f"成功给 {target_user['user_name']} 赠送称号【{title_name}】")
    else:
        messages = {
            "already_unlocked": f"用户已拥有称号【{title_name}】",
            "state_changed": "用户称号状态已变化，请重试",
            "operation_conflict": "本次赠送请求与已处理记录冲突",
        }
        await handle_send(bot, event, f"赠送失败：{messages.get(result.status, '请重试')}")


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
**称号帮助**
---
**查看**
- 我的称号
> 查看第一页
- 我的称号 2
> 查看第二页（每页10个）
- 称号详情 + 名称
> 查看称号详情
- 我的成就
> 查看成就进度

**装备**
- 装备称号 + 名称
> 装备指定称号（修仙信息中显示）
- 同时只能装备一个
- 卸下称号
> 取消当前装备

**检查**
- 检查称号
> 手动检测新称号
- 检查成就
> 手动检测新成就

> 达成条件后自动解锁，无需手动领取。
> 条件含签到、历练、战斗、双修、师徒、炼丹、秘境、BOSS、境界等。
""".strip()


@title_help_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def title_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """称号帮助"""
    bot, _ = await assign_bot(bot=bot, event=event)
    await send_help_message(
        bot, event, __title_help__,
        k1="我的称号", v1="我的称号",
        k2="检查称号", v2="检查称号",
        k3="关系", v3="关系帮助",
        k4="存档", v4="我的修仙信息"
    )
    await title_help_cmd.finish()
