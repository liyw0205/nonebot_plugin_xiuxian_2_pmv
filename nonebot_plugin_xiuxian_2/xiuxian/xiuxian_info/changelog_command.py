import asyncio
from io import BytesIO
from pathlib import Path

from ..on_compat import on_command
from nonebot.log import logger
from nonebot.params import CommandArg

from ..adapter_compat import Bot, GroupMessageEvent, Message, PrivateMessageEvent
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import handle_pic_send, handle_send
from .draw_changelog import create_changelog_image, get_commits


changelog = on_command("更新日志", priority=5, aliases={"更新记录"}, block=True)


@changelog.handle(parameterless=[Cooldown(cd_time=30)])
async def changelog_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """处理更新日志命令"""
    bot, _ = await assign_bot(bot=bot, event=event)

    page_arg = args.extract_plain_text().strip()
    page = 1

    if page_arg:
        if page_arg.isdigit():
            page = int(page_arg)
        else:
            await handle_send(bot, event, "页码格式错误，请发送：更新日志 1")
            await changelog.finish()

    if page <= 0:
        page = 1

    await handle_send(bot, event, "正在获取更新日志，请稍候...")

    try:
        commits = await asyncio.to_thread(get_commits, page)

        if not commits:
            await handle_send(
                bot,
                event,
                "无法获取更新日志，可能已到达最后一页，或 GitHub 请求失败。"
            )
            await changelog.finish()

        img_obj = await asyncio.to_thread(create_changelog_image, commits, page)

        img_buf = None
        need_delete_path = None

        if isinstance(img_obj, BytesIO):
            img_obj.seek(0)
            img_buf = img_obj

        elif isinstance(img_obj, bytes):
            img_buf = BytesIO(img_obj)
            img_buf.seek(0)

        elif isinstance(img_obj, Path):
            need_delete_path = img_obj
            with open(img_obj, "rb") as f:
                img_buf = BytesIO(f.read())
            img_buf.seek(0)

        elif isinstance(img_obj, str):
            img_path = Path(img_obj)
            need_delete_path = img_path
            with open(img_path, "rb") as f:
                img_buf = BytesIO(f.read())
            img_buf.seek(0)

        else:
            await handle_send(
                bot,
                event,
                f"生成更新日志图片失败：不支持的图片类型 {type(img_obj)}"
            )
            await changelog.finish()

        await handle_pic_send(bot, event, img_buf)

        if need_delete_path:
            try:
                if need_delete_path.exists():
                    need_delete_path.unlink()
            except Exception as e:
                logger.warning(f"删除更新日志缓存图片失败: {e}")

    except Exception as e:
        logger.exception("生成或发送更新日志图片时出错")
        await handle_send(bot, event, f"生成更新日志图片时出错: {e}")

    await changelog.finish()


__all__ = ["changelog", "changelog_"]
