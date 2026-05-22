import random
from datetime import datetime

from ..on_compat import on_command
from nonebot.params import CommandArg

from ..adapter_compat import Bot, GroupMessageEvent, Message, PrivateMessageEvent, is_group_event
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, get_impersonating_target, handle_send
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, PlayerDataManager


avatar_switch_cmd = on_command("身外化身", priority=5, block=True)
my_id_cmd = on_command("我的ID", aliases={"我的id", "myid", "id"}, priority=5, block=True)

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()


@avatar_switch_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def avatar_switch_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """
    身外化身命令
    用法：
    - 身外化身         -> 本号/化身互相切换
    - 身外化身 本体    -> 强制切回本体
    """
    bot, _ = await assign_bot(bot=bot, event=event)

    main_id = str(event.user_id)
    arg_text = args.extract_plain_text().strip()

    if arg_text in ["本体", "回来", "返回", "切回"]:
        init_avatar_if_needed(main_id)
        player_data_manager.update_or_write_data(main_id, "avatar", "active_id", str(main_id))
        await handle_send(
            bot,
            event,
            "🔁 已切回本体！\n当前为【本号】状态\n（后续修仙指令将作用于本号）"
        )
        await avatar_switch_cmd.finish()

    is_user, user_info, msg = check_user(str(event.user_id))
    if not is_user:
        await handle_send(bot, event, "请先使用【我要修仙】进入修仙世界后再开启身外化身！\n切换回来：身外化身 本体")
        await avatar_switch_cmd.finish()

    role, info = toggle_avatar(main_id)

    if role == "avatar":
        avatar_id = info.get("avatar_id")
        await handle_send(
            bot,
            event,
            f"✨ 身外化身已启用！\n已从【本号】切换至【化身】\n化身ID：{avatar_id}\n（后续修仙指令将作用于化身）"
        )
    else:
        await handle_send(
            bot,
            event,
            "🔁 已收回化身，回归本体！\n当前为【本号】状态\n（后续修仙指令将作用于本号）"
        )

    await avatar_switch_cmd.finish()


@my_id_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def my_id_cmd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查询当前ID信息（含伪装/化身状态）"""
    bot, _ = await assign_bot(bot=bot, event=event)

    real_user_id = str(event.get_user_id())

    if is_group_event(event):
        group_id = str(event.group_id)
    else:
        group_id = "私聊无群号"

    impersonated_id = get_impersonating_target(real_user_id)

    avatar_active_id = player_data_manager.get_field_data(real_user_id, "avatar", "active_id")
    avatar_active_id = str(avatar_active_id) if avatar_active_id else real_user_id

    effective_user_id = impersonated_id if impersonated_id else avatar_active_id

    status_list = []
    if impersonated_id:
        status_list.append(f"伪装中 -> {impersonated_id}")
    if avatar_active_id != real_user_id:
        status_list.append(f"化身中 -> {avatar_active_id}")
    if not status_list:
        status_list.append("正常（本体）")

    msg = (
        f"你的ID信息如下：\n"
        f"用户ID：{real_user_id}\n"
        f"当前ID：{effective_user_id}\n"
        f"群ID：{group_id}\n"
        f"状态：{'；'.join(status_list)}"
    )

    await handle_send(bot, event, msg)
    await my_id_cmd.finish()


def _generate_unique_avatar_id() -> str:
    """生成不与现有修仙用户冲突的化身ID"""
    while True:
        new_id = str(random.randint(10_000_000, 9_999_999_999))
        if not sql_message.get_user_info_with_id(new_id):
            return new_id


def get_active_user_id(user_id: str) -> str:
    """获取当前激活ID（本号或化身）"""
    active_id = player_data_manager.get_field_data(user_id, "avatar", "active_id")
    return str(active_id) if active_id else str(user_id)


def get_avatar_info(user_id: str) -> dict:
    """获取玩家化身信息（以本号ID为键）"""
    info = player_data_manager.get_fields(user_id, "avatar")
    return info if info else {}


def init_avatar_if_needed(main_id: str) -> dict:
    """初始化化身信息（首次使用时创建）"""
    info = get_avatar_info(main_id)
    if info and info.get("avatar_id"):
        return info

    avatar_id = _generate_unique_avatar_id()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    player_data_manager.update_or_write_data(main_id, "avatar", "main_id", str(main_id))
    player_data_manager.update_or_write_data(main_id, "avatar", "avatar_id", str(avatar_id))
    player_data_manager.update_or_write_data(main_id, "avatar", "active_id", str(main_id))
    player_data_manager.update_or_write_data(main_id, "avatar", "create_time", now_str)

    return get_avatar_info(main_id)


def toggle_avatar(main_id: str) -> tuple[str, dict]:
    """切换本号/化身，返回(当前激活身份, info)"""
    info = init_avatar_if_needed(main_id)
    main_id = str(info.get("main_id", main_id))
    avatar_id = str(info.get("avatar_id"))
    active_id = str(info.get("active_id", main_id))

    if active_id == main_id:
        new_active = avatar_id
        role = "avatar"
    else:
        new_active = main_id
        role = "main"

    player_data_manager.update_or_write_data(main_id, "avatar", "active_id", new_active)
    info["active_id"] = new_active
    return role, info


__all__ = [
    "avatar_switch_cmd",
    "avatar_switch_cmd_",
    "get_active_user_id",
    "get_avatar_info",
    "init_avatar_if_needed",
    "my_id_cmd",
    "my_id_cmd_",
    "toggle_avatar",
]
