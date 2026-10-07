import platform
import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
from nonebot import __version__ as nb_version
from ..on_compat import on_command
from nonebot.permission import SUPERUSER
from nonebot.params import CommandArg
from ..adapter_compat import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
)
from ..xiuxian_utils.utils import handle_send, number_to, send_help_message
from ..xiuxian_utils.lay_out import Cooldown
from types import SimpleNamespace
from ..xiuxian_utils.download_xiuxian_data import UpdateManager, UpdateApplication

psutil_available = False
try:
    import psutil
    psutil_available = True
except ImportError:
    print("psutil模块未安装，部分系统信息和机器人信息功能将受限。")
    class DummyPsutilProcess:
        def create_time(self):
            return 0

    class DummyPsutil:
        def Process(self, pid):
            return DummyPsutilProcess()
        def cpu_count(self, logical=True):
            return "未知"
        def cpu_percent(self):
            return "未知"
        def cpu_freq(self):
            class Freq:
                current = "未知"
            return Freq()
        def virtual_memory(self):
            class Mem:
                total = 0
                used = 0
                percent = "未知"
            return Mem()
        def disk_usage(self, path):
            class Disk:
                total = 0
                used = 0
                percent = "未知"
            return Disk()
        def boot_time(self):
            return 0

    psutil = DummyPsutil()

update_manager = UpdateManager()
from ..xiuxian_utils.periods import format_duration_full
from ...features.status.application import StatusApplication
from ...paths import get_paths
from ...infrastructure.ids import UUIDGenerator

status_application = StatusApplication(
    get_paths().game_db, trade_database=get_paths().trade_db
)
runtime_ids = UUIDGenerator()


def _run_status_action(action: str, operation_id: str, user_id: str, call, **payload):
    outcome = status_application.execute_legacy_call(
        operation_id=str(operation_id),
        user_id=str(user_id),
        action=action,
        payload=payload,
        call=call,
    )
    data = dict(outcome.data or {})
    data.setdefault("status", outcome.status)
    data["succeeded"] = outcome.ok
    return SimpleNamespace(**data)

bot_info_cmd = on_command("bot信息", permission=SUPERUSER, priority=5, block=True)
sys_info_cmd = on_command("系统信息", permission=SUPERUSER, priority=5, block=True)
ping_test_cmd = on_command("ping测试", permission=SUPERUSER, priority=5, block=True)
status_cmd = on_command("插件帮助", permission=SUPERUSER, priority=5, block=True)
version_query_cmd = on_command("版本查询", permission=SUPERUSER, priority=5, block=True)
version_update_cmd = on_command("版本更新", permission=SUPERUSER, priority=5, block=True)
check_update_cmd = on_command("检测更新", permission=SUPERUSER, priority=5, block=True)

def format_time(seconds: float) -> str:
    """将秒数格式化为 'X天X小时X分X秒'"""
    return format_duration_full(seconds, zero="未知")

async def get_ping_test(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """保留旧消息流程，将无状态探测委托给 status application。"""
    await ping_test_cmd.send("正在测试网络延迟，请稍候...")
    return await status_application.ping_test()

async def get_bot_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """获取Bot信息"""
    is_group = isinstance(event, GroupMessageEvent)
    group_id = str(event.group_id) if is_group else "私聊"
    overview = status_application.bot_overview()
    
    # 获取Bot运行时间, 仅在psutil可用时
    if psutil_available:
        try:
            current_time = time.time()
            process_create_time = psutil.Process(os.getpid()).create_time()
            bot_uptime = {
                "Bot 启动时间": f"{datetime.fromtimestamp(process_create_time):%Y-%m-%d %H:%M:%S}",
                "Bot 运行时间": format_time(current_time - process_create_time)
            }
        except Exception:
            bot_uptime = {"Bot启动时间": "获取失败", "Bot运行时间": "获取失败"}
    else:
        bot_uptime = {"Bot启动时间": "psutil未安装", "Bot运行时间": "psutil未安装"}
    
    # 获取当前插件版本号
    current_version = update_manager.get_current_version()

    # 组装Bot信息
    bot_info = {
        "Bot ID": bot.self_id,
        "NoneBot2版本": nb_version,
        "会话类型": "群聊" if is_group else "私聊",
        "会话ID": group_id,
        "修仙插件版本": current_version
    }
    
    msg = "Bot信息\n"
    msg += "\n【Bot信息】\n"
    msg += "\n".join(f"{k}: {v}" for k, v in bot_info.items())
    msg += "\n\n【运行时间】\n"
    msg += "\n".join(f"{k}: {v}" for k, v in bot_uptime.items())
    msg += "\n\n【修仙数据】\n"
    msg += f"全部用户：{overview.total_users if overview.total_users is not None else '不可用'}"
    msg += f"\n活跃用户：{overview.today_active_users if overview.today_active_users is not None else '不可用'}"
    msg += f"\n昨日活跃：{overview.yesterday_active_users if overview.yesterday_active_users is not None else '不可用'}"
    msg += f"\n七日活跃：{overview.last_7days_active_users if overview.last_7days_active_users is not None else '不可用'}"
    items_text = (
        "不可用"
        if overview.total_items_quantity is None
        else f"{overview.total_items_quantity}({number_to(overview.total_items_quantity)})"
    )
    trade_text = (
        "不可用"
        if overview.total_goods_quantity is None
        else f"{overview.total_goods_quantity}({number_to(overview.total_goods_quantity)})"
    )
    msg += f"\n用户物品：{items_text}"
    msg += f"\n交易物品：{trade_text}"
    return msg

async def get_system_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """获取系统信息"""
    return status_application.system_info().render()

@bot_info_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_bot_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """bot信息命令"""
    msg = await get_bot_info(bot, event)
    await send_help_message(
        bot, event, msg,
        k1="查询", v1="版本查询",
        k2="更新", v2="检测更新",
        k3="信息", v3="bot信息"
    )

@sys_info_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_sys_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """系统信息命令"""
    sys_msg = await get_system_info(bot, event)
    await handle_send(bot, event, sys_msg)

@ping_test_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_ping_test(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """ping测试命令"""
    ping_msg = await get_ping_test(bot, event)
    await handle_send(bot, event, ping_msg)

@status_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_status(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = """
插件帮助

版本管理
- 更新日志
> 获取版本日志
- 版本查询
> 获取最近发布的版本
- 检测更新
> 检测是否需要更新
- 版本更新 版本号/latest
> 指定版本或最新版本

状态查询
- bot信息
> 获取机器人和修仙数据
- 系统信息
> 获取系统信息
- ping测试
> 测试网络延迟

GitHub
> liyw0205/nonebot_plugin_xiuxian_2_pmv
""".strip()
    await send_help_message(
        bot,
        event,
        msg,
        k1="版本查询",
        v1="版本查询",
        k2="检测更新",
        v2="检测更新",
        k3="bot信息",
        v3="bot信息",
        k4="系统信息",
        v4="系统信息",
    )

def utc_time(published_at):
    utc_time_str = published_at.replace('Z', '+00:00')
    utc_time = datetime.fromisoformat(utc_time_str)
    beijing_timezone = timezone(timedelta(hours=8))
    beijing_time = utc_time.astimezone(beijing_timezone)
    formatted_beijing_time = beijing_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_beijing_time

@version_query_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_version_query(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """版本查询命令"""
    recent_releases = await asyncio.to_thread(UpdateApplication(update_manager).latest_releases, 5)
    if not recent_releases:
        await handle_send(bot, event, "无法获取版本信息。")
        return

    msg = "版本查询\n"
    msg += "最近发布的版本：\n\n"
    for release in recent_releases:
        msg += f"- 版本号：{release['tag_name']}\n"
        msg += f"  发布时间：{utc_time(release['published_at'])}\n"
    msg += "通过【更新日志】查看详情"
    await handle_send(bot, event, msg)

@check_update_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_check_update(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """检测更新命令"""
    latest_release, message = await asyncio.to_thread(UpdateApplication(update_manager).check_update)
    if latest_release:
        release_tag = latest_release['tag_name']
        await handle_send(
            bot,
            event,
            f"发现新版本：{release_tag}\n"
            f"当前版本：{update_manager.get_current_version()}\n"
            f"建议先查看更新日志，再执行版本更新。",
        )
    else:
        await handle_send(bot, event, f"当前已是最新版本：{update_manager.get_current_version()}")

@version_update_cmd.handle(parameterless=[Cooldown(cd_time=0)])
async def handle_version_update(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """版本更新命令"""
    args = args.extract_plain_text().split()
    if len(args) != 1:
        await handle_send(bot, event, "用法：版本更新 版本号/latest")
        return

    action = str(args[0])

    if action in ["latest", "update", "最新"]:
        # 检查是否有更新
        latest_release, message = await asyncio.to_thread(UpdateApplication(update_manager).check_update)
        if not latest_release:
            await handle_send(bot, event, f"当前已是最新版本：{update_manager.get_current_version()}")
            return
        release_tag = latest_release['tag_name']
    else:
        # 指定版本号
        release_tag = action
        recent_releases = await asyncio.to_thread(UpdateApplication(update_manager).latest_releases, 5)
        if not recent_releases:
            await handle_send(bot, event, "无法获取网络版本信息。")
            return
        release_tags = [release['tag_name'] for release in recent_releases]
        if release_tag not in release_tags:
            await handle_send(
                bot,
                event,
                f"输入的版本号 {release_tag} 不正确\n"
                f"请通过【版本查询】获取最近的发布版本。",
            )
            return

    await handle_send(bot, event, f"更新版本 {release_tag}，开始更新...")
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    user_id = str(event.get_user_id())
    operation_id = f"status:version-update:{event_id or runtime_ids.new_id()}:{user_id}:{release_tag}"
    # 执行更新流程，并把整个真实更新调用纳入操作账本。
    outcome = await asyncio.to_thread(
        _run_status_action,
        "version_update",
        operation_id,
        user_id,
        lambda: UpdateApplication(update_manager).perform_update_with_backup(release_tag),
        release_tag=release_tag,
    )
    result = outcome.result if hasattr(outcome, "result") else None
    if isinstance(result, (tuple, list)) and len(result) == 2:
        success, detail = bool(result[0]), result[1]
    else:
        success, detail = bool(outcome.succeeded), result or getattr(outcome, "message", "")
    if success:
        await handle_send(bot, event, f"版本更新成功！当前版本：{update_manager.get_current_version()}")
    else:
        await handle_send(bot, event, f"版本更新失败：{detail}")
