from typing import Any, Tuple

from ...paths import get_paths
from ...core.errors import ConflictError
from ...features.bank.command_application import BankCommandApplication
from ...features.bank.command_replies import render_bank_reply
from ...infrastructure.ids import UUIDGenerator
from ..on_compat import on_regex
from nonebot.log import logger
from nonebot.params import RegexGroup
from ..adapter_compat import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from .bankconfig import get_config
from ..xiuxian_utils.utils import check_user, handle_send, send_help_message


config = get_config()
BANKLEVEL = config["BANKLEVEL"]
bank_command_application = BankCommandApplication(get_paths().game_db, BANKLEVEL)
runtime_ids = UUIDGenerator()


bank = on_regex(
    r'^灵庄(存灵石|取灵石|升级会员|信息|结算)?(.*)?',
    priority=9,    
    block=True
)

__bank_help__ = """
**灵庄帮助**
---
**存取**
- 灵庄存灵石 [金额]
> 存入灵石获取利息
- 灵庄取灵石 [金额]
> 取出灵石（自动结算利息）

**会员**
- 灵庄升级会员
> 提升会员等级，增加利息倍率

**查询**
- 灵庄信息
> 查看余额和会员信息
- 灵庄结算
> 手动结算当前利息

> 利息按小时计算；会员等级越高收益越高；存取时会自动结算利息。
""".strip()


@bank.handle(parameterless=[Cooldown(cd_time=0)])
async def bank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Tuple[Any, ...] = RegexGroup()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        await bank.finish()
        return
    mode, argument = args[0], args[1]
    user_id = str(user_info["user_id"])
    action = {"存灵石": "deposit", "取灵石": "withdrawal", "升级会员": "upgrade", "结算": "interest"}.get(mode, "info")
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    operation_id = f"bank-{action}:{event_id}:{user_id}" if event_id else f"bank-{action}:{user_id}:{runtime_ids.new_id()}"
    try:
        result = bank_command_application.execute(
            operation_id=operation_id, user_id=user_id, mode=mode, argument=argument,
        )
        is_help = result.get("action") == "help" and result.get("status") == "help"
        message = None if is_help else render_bank_reply(result, BANKLEVEL)
    except ConflictError:
        await handle_send(bot, event, "请求冲突，本次灵庄操作未结算。", md_type="灵庄")
        return
    except Exception as exc:
        logger.warning("bank command failed: {}", type(exc).__name__)
        await handle_send(bot, event, "灵庄服务异常，暂时无法确认本次结果，请稍后核查账户状态。", md_type="灵庄")
        return

    if is_help:
        await send_help_message(bot, event, __bank_help__, k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
    elif result.get("action") == "upgrade":
        await handle_send(bot, event, message, md_type="灵庄", k1="升级", v1="灵庄升级会员", k2="信息", v2="灵庄信息", k3="帮助", v3="灵庄帮助")
    elif result.get("action") == "info":
        await handle_send(bot, event, message, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="结算", v3="灵庄结算")
    else:
        await handle_send(bot, event, message, md_type="灵庄", k1="存灵石", v1="灵庄存灵石", k2="取灵石", v2="灵庄取灵石", k3="信息", v3="灵庄信息")
    await bank.finish()


def savef(user_id, data):
    from ...compatibility.legacy_bank_account_storage import savef as legacy_savef

    return legacy_savef(user_id, data)
