from nonebot.log import logger

from ...core.errors import ConflictError
from ...features.beg.command_application import BegCommandApplication
from ...features.beg.command_replies import render_beg_reply
from ...infrastructure.ids import UUIDGenerator
from ...paths import get_paths
from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_config import XiuConfig
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, handle_send, send_help_message


beg_command_application = BegCommandApplication(
    get_paths().game_db,
    XiuConfig,
    jsondata.level_data,
    lambda: Items().get_data_by_item_id("18052"),
)
runtime_ids = UUIDGenerator()

beg_stone = on_command("仙途奇缘", priority=7, block=True)
beg_help = on_command("仙途奇缘帮助", priority=7, block=True)
novice = on_command("新手礼包", priority=7, block=True)


@beg_help.handle(parameterless=[Cooldown(cd_time=0)])
async def beg_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    try:
        result = beg_command_application.execute(action="help")
        message = render_beg_reply(result)
    except Exception as exc:
        logger.warning("beg help failed: {}", type(exc).__name__)
        await handle_send(bot, event, "机缘帮助暂不可用，请稍后重试。")
        return
    await send_help_message(
        bot, event, message,
        k1="奇缘", v1="仙途奇缘",
        k2="礼包", v2="新手礼包",
        k3="存档", v3="我的修仙信息",
    )
    await beg_help.finish()


@beg_stone.handle(parameterless=[Cooldown(cd_time=0)])
async def beg_stone_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, message = check_user(event)
    if not is_user:
        await handle_send(bot, event, message, md_type="我要修仙")
        await beg_stone.finish()
        return
    user_id = str(user_info["user_id"])
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"beg-daily:{event_id}:{user_id}" if event_id else f"beg-daily:{runtime_ids.new_id()}:{user_id}"
    try:
        result = beg_command_application.execute(
            operation_id=operation_id, user_id=user_id, action="daily_settle",
        )
        message = render_beg_reply(result)
    except ConflictError:
        await handle_send(bot, event, "领取失败：请求冲突，请勿重复提交。")
        return
    except Exception as exc:
        logger.warning("beg daily command failed: {}", type(exc).__name__)
        await handle_send(bot, event, "机缘服务异常，暂时无法确认本次结果，请稍后核查角色状态。")
        return
    await handle_send(bot, event, message)
    await beg_stone.finish()


@novice.handle(parameterless=[Cooldown(cd_time=0)])
async def novice_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, message = check_user(event)
    if not is_user:
        await handle_send(bot, event, message, md_type="我要修仙")
        await novice.finish()
        return
    user_id = str(user_info["user_id"])
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"novice-gift:{event_id}:{user_id}" if event_id else f"novice-gift:{runtime_ids.new_id()}:{user_id}"
    try:
        result = beg_command_application.execute(
            operation_id=operation_id, user_id=user_id, action="novice_claim",
        )
        message = render_beg_reply(result)
    except ConflictError:
        await handle_send(bot, event, "领取失败：请求冲突，请勿重复提交。", md_type="修仙")
        return
    except Exception as exc:
        logger.warning("beg novice command failed: {}", type(exc).__name__)
        await handle_send(bot, event, "机缘服务异常，暂时无法确认本次结果，请稍后核查角色状态。", md_type="修仙")
        return
    await handle_send(
        bot, event, message,
        md_type="修仙",
        k1="签到", v1="修仙签到",
        k2="日常", v2="日常",
        k3="悬赏", v3="悬赏令查看",
        k4="帮助", v4="修仙帮助",
    )
    await novice.finish()
