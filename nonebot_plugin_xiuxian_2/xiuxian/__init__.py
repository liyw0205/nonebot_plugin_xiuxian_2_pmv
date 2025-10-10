#!usr/bin/env python3
# -*- coding: utf-8 -*-
from nonebot.exception import FinishedException

from .xiuxian_utils.download_xiuxian_data import download_xiuxian_data
from nonebot.plugin import PluginMetadata
from nonebot.log import logger
from nonebot.message import event_preprocessor, IgnoredException
from nonebot.adapters.onebot.v11 import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent
)
from nonebot import get_driver, on_command
from .xiuxian_config import XiuConfig
from pathlib import Path
from pkgutil import iter_modules
from nonebot.log import logger
from nonebot import require, load_all_plugins, get_plugin_by_module_name
from .xiuxian_utils.config import config as _config
from .xiuxian_info.draw_changelog import get_commits, create_changelog_image
from nonebot.params import CommandArg
from nonebot.adapters.onebot.v11 import Message, MessageSegment, GroupMessageEvent, Bot

DRIVER = get_driver()

try:
    NICKNAME: str = list(DRIVER.config.nickname)[0]
except Exception as e:
    logger.opt(colors=True).info(f"<red>缺少超级用户配置文件，{e}!</red>")
    logger.opt(colors=True).info(f"<red>请去.env.dev文件中设置超级用户QQ号以及nickname!</red>")
    NICKNAME = 'bot'

try:
    download_xiuxian_data()
except Exception as e:
    logger.opt(colors=True).info(f"<red>下载配置文件失败，修仙插件无法加载，{e}!</red>")
    raise ImportError

put_bot = XiuConfig().put_bot
shield_group = XiuConfig().shield_group
response_group = XiuConfig().response_group
shield_private = XiuConfig().shield_private

try:
    put_bot_ = put_bot[0]
except:
    logger.opt(colors=True).info(f"<green>修仙插件没有配置put_bot,如果有多个qq和nb链接,请务必配置put_bot,具体介绍参考【风控帮助】！</green>")

require('nonebot_plugin_apscheduler')

if get_plugin_by_module_name("xiuxian"):
    logger.opt(colors=True).info(f"<green>推荐直接加载 xiuxian 仓库文件夹</green>")
    load_all_plugins(
        [
            f"xiuxian.{module.name}"
            for module in iter_modules([str(Path(__file__).parent)])
            if module.ispkg
            and (
                (name := module.name[11:]) == "meta"
                or name not in _config.disabled_plugins
            )
        ],
        [],
    )

__plugin_meta__ = PluginMetadata(
    name='修仙模拟器',
    description='',
    usage=(
        "必死之境机逢仙缘，修仙之路波澜壮阔！\n"
        " 输入 < 修仙帮助 > 获取仙界信息"
    ),
    extra={
        "show": True,
        "priority": 15
    }
)

changelog = on_command("更新日志", priority=5, aliases={"更新记录"})

@changelog.handle()
async def _(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """处理更新日志命令"""
    page_arg = args.extract_plain_text().strip()
    page = 1
    if page_arg and page_arg.isdigit():
        page = int(page_arg)

    if page <= 0:
        page = 1

    await changelog.send("正在获取更新日志，请稍候...")

    try:
        commits = get_commits(page=page)
        if commits:
            image_path = create_changelog_image(commits, page)
            with open(image_path, "rb") as f:
                image_bytes = f.read()
            await changelog.finish(MessageSegment.image(image_bytes))
        else:
            await changelog.finish("无法获取更新日志，可能已到达最后一页或请求失败。")
    except FinishedException:
        raise
    except Exception as e:
        await changelog.finish(f"生成更新日志图片时出错: {e}")

@event_preprocessor
async def do_something(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    global put_bot
    if not put_bot:
        pass
    else:
        if str(bot.self_id) in put_bot:
            # 私聊处理
            if isinstance(event, PrivateMessageEvent):
                if shield_private:  # 如果屏蔽私聊
                    raise IgnoredException("私聊功能已屏蔽,已忽略")
                return  # 私聊不受群聊设置影响
            
            # 群聊处理
            if response_group:
                if str(event.group_id) in shield_group:
                    pass
                else:
                    raise IgnoredException("不为响应群消息,已忽略")
            else:
                if str(event.group_id) in shield_group:
                    raise IgnoredException("为屏蔽群消息,已忽略")
                else:
                    pass
        else:
            raise IgnoredException("非主bot信息,已忽略")
