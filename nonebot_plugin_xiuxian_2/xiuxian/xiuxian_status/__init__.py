import platform
import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
from nonebot import on_command, __version__ as nb_version
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
from ..xiuxian_utils.utils import handle_send, number_to
from ..xiuxian_utils.lay_out import Cooldown
import subprocess
import re
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage, TradeDataManager
from ..xiuxian_utils.download_xiuxian_data import UpdateManager

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
sql_message = XiuxianDateManage()
trade_manager = TradeDataManager()

bot_info_cmd = on_command("bot信息", permission=SUPERUSER, priority=5, block=True)
sys_info_cmd = on_command("系统信息", permission=SUPERUSER, priority=5, block=True)
ping_test_cmd = on_command("ping测试", permission=SUPERUSER, priority=5, block=True)
status_cmd = on_command("插件帮助", permission=SUPERUSER, priority=5, block=True)
version_query_cmd = on_command("版本查询", permission=SUPERUSER, priority=5, block=True)
version_update_cmd = on_command("版本更新", permission=SUPERUSER, priority=5, block=True)
check_update_cmd = on_command("检测更新", permission=SUPERUSER, priority=5, block=True)

def format_time(seconds: float) -> str:
    """将秒数格式化为 'X天X小时X分X秒'"""
    if seconds <= 0:
        return "未知"
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(days)}天{int(hours)}小时{int(minutes)}分{int(seconds)}秒"

def get_ping_emoji(delay: float) -> str:
    """根据延迟返回对应的表情"""
    if delay == 0:
        return "💀"  # 超时/失败
    elif delay < 20:
        return "🚀"  # 极快
    elif delay < 50:
        return "⚡"  # 快速
    elif delay < 100:
        return "🐎"  # 中等
    elif delay < 200:
        return "🐢"  # 慢速
    else:
        return "🐌"  # 极慢

async def ping_host(host: str) -> tuple:
    """
    异步执行单个 ping 测试
    返回 (host, delay_ms, is_timeout, emoji)
    """
    loop = asyncio.get_event_loop()
    try:
        # Windows和Linux/macOS的ping命令参数不同
        param = '-n' if platform.system().lower() == 'windows' else '-c'
        count = '4'  # ping 4次

        # 使用 asyncio 创建子进程执行 ping
        def _ping():
            try:
                result = subprocess.run(
                    ['ping', param, count, host],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=10
                )
                output = result.stdout
                if platform.system().lower() == 'windows':
                    match = re.search(r'平均 = (\d+)ms', output)
                    if match:
                        return (float(match.group(1)), False)
                else:
                    match = re.search(r'min/avg/max/mdev = [\d.]+/([\d.]+)/', output)
                    if match:
                        return (float(match.group(1)), False)
                return (0, True)  # 未找到平均延迟，视为超时
            except subprocess.TimeoutExpired:
                return (0, True)
            except Exception:
                return (0, True)

        delay, is_timeout = await loop.run_in_executor(None, _ping)

        emoji = get_ping_emoji(delay)

        return (host, delay, is_timeout, emoji)

    except Exception:
        return (host, 0, True, "💀")  # 兜底异常也视为超时

async def get_ping_test(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """异步并发执行所有 ping 测试"""
    await ping_test_cmd.send("正在测试网络延迟，请稍候...")

    sites = {
        "百度": "www.baidu.com",
        "腾讯": "www.qq.com",
        "阿里": "www.aliyun.com",
        "必应": "cn.bing.com",
        "GitHub": "github.com",
        "Gitee": "gitee.com",
        "谷歌": "www.google.com",
        "苹果": "www.apple.com"
    }

    # 构造所有要 ping 的任务
    tasks = [ping_host(host) for host in sites.values()]

    # 并发执行所有 ping
    results = await asyncio.gather(*tasks)

    # 组装消息
    msg = "\n☆------网络延迟测试------☆\n"

    # 国内站点（前4个）
    msg += "\n【国内站点】\n"
    for (name, host), (_, delay, is_timeout, emoji) in zip(list(sites.items())[:4], results[:4]):
        if is_timeout:
            msg += f"{emoji} {name}: 超时(0ms)\n"
        else:
            msg += f"{emoji} {name}: {delay:.3f}ms\n"

    # 国外站点（后4个）
    msg += "\n【国外站点】\n"
    for (name, host), (_, delay, is_timeout, emoji) in zip(list(sites.items())[4:], results[4:]):
        if is_timeout:
            msg += f"{emoji} {name}: 超时(0ms)\n"
        else:
            msg += f"{emoji} {name}: {delay:.3f}ms\n"

    return msg

async def get_bot_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """获取Bot信息"""
    is_group = isinstance(event, GroupMessageEvent)
    group_id = str(event.group_id) if is_group else "私聊"
    all_users = sql_message.all_users()
    active_users = sql_message.today_active_users()
    yesterday_active_users = sql_message.yesterday_active_users()
    last_7days_active_users = sql_message.last_7days_active_users()
    total_items_quantity = sql_message.total_items_quantity()
    total_goods_quantity = trade_manager.total_goods_quantity()
    
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
    
    msg = "\n☆------Bot信息------☆\n"
    msg += "\n【🤖 Bot信息】\n"
    msg += "\n".join(f"{k}: {v}" for k, v in bot_info.items())
    msg += "\n\n【⏱ 运行时间】\n"
    msg += "\n".join(f"{k}: {v}" for k, v in bot_uptime.items())
    msg += "\n\n【🧘 修仙数据】\n"
    msg += f"全部用户：{all_users}"
    msg += f"\n活跃用户：{active_users}"
    msg += f"\n昨日活跃：{yesterday_active_users}"
    msg += f"\n七日活跃：{last_7days_active_users}"
    msg += f"\n用户物品：{total_items_quantity}({number_to(total_items_quantity)})"
    msg += f"\n交易物品：{total_goods_quantity}({number_to(total_goods_quantity)})"
    return msg

async def get_system_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """获取系统信息"""
    # 获取系统信息 (platform模块不依赖psutil，所以始终可用)
    system_info = {
        "平台": platform.platform(),
        "系统": platform.system(),
        "版本": platform.version(),
        "机器": platform.machine(),
        "处理器": platform.processor(),
        "Python版本": platform.python_version(),
    }
    
    # 获取CPU信息, 仅在psutil可用时
    if psutil_available:
        try:
            cpu_info = {
                "物理核心数": psutil.cpu_count(logical=False),
                "逻辑核心数": psutil.cpu_count(logical=True),
                "CPU使用率": f"{psutil.cpu_percent()}%",
                "CPU频率": f"{psutil.cpu_freq().current:.2f}MHz" if hasattr(psutil, "cpu_freq") and psutil.cpu_freq().current != '未知' else "未知"
            }
        except Exception:
            cpu_info = {
                "物理核心数": "获取失败", "逻辑核心数": "获取失败",
                "CPU使用率": "获取失败", "CPU频率": "获取失败"
            }
    else:
        cpu_info = {
            "物理核心数": "psutil未安装", "逻辑核心数": "psutil未安装",
            "CPU使用率": "psutil未安装", "CPU频率": "psutil未安装"
        }
    
    # 获取内存信息, 仅在psutil可用时
    if psutil_available:
        try:
            mem = psutil.virtual_memory()
            mem_info = {
                "总内存": f"{mem.total / (1024**3):.2f}GB",
                "已用内存": f"{mem.used / (1024**3):.2f}GB",
                "内存使用率": f"{mem.percent}%"
            }
        except Exception:
            mem_info = {
                "总内存": "获取失败", "已用内存": "获取失败",
                "内存使用率": "获取失败"
            }
    else:
        mem_info = {
            "总内存": "psutil未安装", "已用内存": "psutil未安装",
            "内存使用率": "psutil未安装"
        }
    
    # 获取磁盘信息, 仅在psutil可用时
    if psutil_available:
        try:
            disk = psutil.disk_usage('/')
            disk_info = {
                "总磁盘空间": f"{disk.total / (1024**3):.2f}GB",
                "已用空间": f"{disk.used / (1024**3):.2f}GB",
                "磁盘使用率": f"{disk.percent}%"
            }
        except Exception:
            disk_info = {
                "总磁盘空间": "获取失败", "已用空间": "获取失败",
                "磁盘使用率": "获取失败"
            }
    else:
        disk_info = {
            "总磁盘空间": "psutil未安装", "已用空间": "psutil未安装",
            "磁盘使用率": "psutil未安装"
        }
    
    # 获取系统启动时间, 仅在psutil可用时
    if psutil_available:
        try:
            boot_time = psutil.boot_time()
            current_time = time.time()
            uptime_seconds = current_time - boot_time
            
            system_uptime_info = {
                "系统启动时间": f"{datetime.fromtimestamp(boot_time):%Y-%m-%d %H:%M:%S}",
                "系统运行时间": format_time(uptime_seconds)
            }
        except Exception:
            system_uptime_info = {"系统启动时间": "获取失败", "系统运行时间": "获取失败"}
    else:
        system_uptime_info = {"系统启动时间": "psutil未安装", "系统运行时间": "psutil未安装"}
    
    # 组装系统信息
    msg = "\n☆------系统信息------☆\n"
    info_sections = [
        ("⏱ 运行时间", system_uptime_info),
        ("💻 系统信息", system_info),
        ("⚡ CPU信息", cpu_info),
        ("🧠 内存信息", mem_info),
        ("💾 磁盘信息", disk_info)
    ]
    
    for section, data in info_sections:
        msg += f"\n【{section}】\n"
        msg += "\n".join(f"{k}: {v}" for k, v in data.items())
    
    return msg

@bot_info_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_bot_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """bot信息命令"""
    msg = await get_bot_info(bot, event)
    await handle_send(bot, event, msg)

@sys_info_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_sys_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """系统信息命令"""
    sys_msg = await get_system_info(bot, event)
    await handle_send(bot, event, sys_msg)

@ping_test_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_ping_test(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """ping测试命令"""
    ping_msg = await get_ping_test(bot, event)
    await handle_send(bot, event, ping_msg)

@status_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_status(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    msg = f"""
更新日志 - 获取版本日志

版本更新 - 指定版本号更新/latest：更新最新版本

版本查询 - 获取最近发布的版本

检测更新 - 检测是否需要更新

bot信息 - 获取机器人和修仙数据

系统信息 - 获取系统信息

ping测试 - 测试网络延迟

→ GitHub - liyw0205/nonebot_plugin_xiuxian_2_pmv
"""
    await handle_send(bot, event, msg)

def utc_time(published_at):
    utc_time_str = published_at.replace('Z', '+00:00')
    utc_time = datetime.fromisoformat(utc_time_str)
    beijing_timezone = timezone(timedelta(hours=8))
    beijing_time = utc_time.astimezone(beijing_timezone)
    formatted_beijing_time = beijing_time.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_beijing_time

@version_query_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_version_query(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """版本查询命令"""
    recent_releases = update_manager.get_latest_releases(5)  # 获取最近的5个发布
    if not recent_releases:
        await handle_send(bot, event, "无法获取版本信息。")
        return

    msg = "\n☆------版本查询------☆\n"
    msg += "最近发布的版本：\n\n"
    for release in recent_releases:
        msg += f"版本号: {release['tag_name']}\n"
        msg += f"发布时间: {utc_time(release['published_at'])}\n\n"
    msg += "通过【更新日志】查看详情"
    await handle_send(bot, event, msg)

@check_update_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_check_update(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """检测更新命令"""
    latest_release, message = update_manager.check_update()
    if latest_release:
        release_tag = latest_release['tag_name']
        await handle_send(bot, event, f"发现新版本 {release_tag}\n当前版本 {update_manager.get_current_version()}\n建议【查看日志】后更新")
    else:
        await handle_send(bot, event, f"当前已是最新版本：{update_manager.get_current_version()}")

@version_update_cmd.handle(parameterless=[Cooldown(cd_time=1.4)])
async def handle_version_update(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """版本更新命令"""
    args = args.extract_plain_text().split()
    if len(args) != 1:
        await handle_send(bot, event, "用法：版本更新 <版本号|latest>")
        return

    action = str(args[0])

    if action in ["latest", "update", "最新"]:
        # 检查是否有更新
        latest_release, message = update_manager.check_update()
        if not latest_release:
            await handle_send(bot, event, f"当前已是最新版本：{update_manager.get_current_version()}")
            return
        release_tag = latest_release['tag_name']
    else:
        # 指定版本号
        release_tag = action
        recent_releases = update_manager.get_latest_releases(5)
        if not recent_releases:
            await handle_send(bot, event, "无法获取网络版本信息。")
            return
        release_tags = [release['tag_name'] for release in recent_releases]
        if release_tag not in release_tags:
            await handle_send(bot, event, f"输入的版本号{release_tag}不正确\n请通过【版本查询】\n获取最近的发布版本")
            return

    await handle_send(bot, event, f"更新版本 {release_tag}，开始更新...")
    # 执行更新流程
    success, result = update_manager.perform_update_with_backup(release_tag)
    if success:
        await handle_send(bot, event, f"版本更新成功！当前版本：{update_manager.get_current_version()}")
    else:
        await handle_send(bot, event, f"版本更新失败：{result}")