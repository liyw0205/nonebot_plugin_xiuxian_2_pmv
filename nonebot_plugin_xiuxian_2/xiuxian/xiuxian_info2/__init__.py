import platform
import psutil
import os
import time
from datetime import datetime
from nonebot import on_command, __version__ as nb_version
from nonebot.permission import SUPERUSER
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    GROUP_ADMIN,
    GROUP_OWNER,
    ActionFailed
)
from ..xiuxian_utils.utils import handle_send
import subprocess
import re

# 注册四个独立的命令处理器
bot_info_cmd = on_command("bot信息", permission=SUPERUSER, priority=5, block=True)
sys_info_cmd = on_command("系统信息", permission=SUPERUSER, priority=5, block=True)
ping_test_cmd = on_command("ping测试", permission=SUPERUSER, priority=5, block=True)
status_cmd = on_command("全部信息", permission=SUPERUSER, priority=5, block=True)

def format_time(seconds: float) -> str:
    """将秒数格式化为 'X天X小时X分X秒'"""
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

def ping_test(host: str) -> tuple:
    """执行ping测试并返回(延迟ms, 是否超时)"""
    try:
        # Windows和Linux/macOS的ping命令参数不同
        param = '-n' if platform.system().lower() == 'windows' else '-c'
        count = '4'  # ping 4次
        
        # 执行ping命令
        result = subprocess.run(
            ['ping', param, count, host],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10
        )
        
        # 解析输出获取平均延迟
        output = result.stdout
        if platform.system().lower() == 'windows':
            # Windows ping输出格式
            match = re.search(r'平均 = (\d+)ms', output)
            if match:
                return (float(match.group(1)), False)
        else:
            # Linux/macOS ping输出格式
            match = re.search(r'min/avg/max/mdev = [\d.]+/([\d.]+)/', output)
            if match:
                return (float(match.group(1)), False)
        
        return (0, True)  # 解析失败视为超时
    except subprocess.TimeoutExpired:
        return (0, True)  # 超时
    except Exception:
        return (0, True)  # 其他错误视为超时

async def get_bot_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """获取Bot信息"""
    is_group = isinstance(event, GroupMessageEvent)
    group_id = str(event.group_id) if is_group else "私聊"
    
    # 获取Bot运行时间
    try:
        current_time = time.time()
        bot_uptime = {
            "Bot 启动时间": f"{datetime.fromtimestamp(psutil.Process(os.getpid()).create_time()):%Y-%m-%d %H:%M:%S}",
            "Bot 运行时间": format_time(current_time - psutil.Process(os.getpid()).create_time())
        }
    except Exception:
        bot_uptime = {"Bot运行时间": "获取失败"}
    
    # 组装Bot信息
    bot_info = {
        "Bot ID": bot.self_id,
        "NoneBot2版本": nb_version,
        "会话类型": "群聊" if is_group else "私聊",
        "会话ID": group_id
    }
    
    msg = "====== Bot信息 ======\n"
    msg += "\n【🤖🤖 Bot信息】\n"
    msg += "\n".join(f"{k}: {v}" for k, v in bot_info.items())
    msg += "\n\n【⏱⏱⏱ 运行时间】\n"
    msg += "\n".join(f"{k}: {v}" for k, v in bot_uptime.items())
    
    return msg

async def get_system_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """获取系统信息"""
    # 获取系统信息
    system_info = {
        "平台": platform.platform(),
        "系统": platform.system(),
        "版本": platform.version(),
        "机器": platform.machine(),
        "处理器": platform.processor(),
        "Python版本": platform.python_version(),
    }
    
    # 获取CPU信息
    try:
        cpu_info = {
            "物理核心数": psutil.cpu_count(logical=False),
            "逻辑核心数": psutil.cpu_count(logical=True),
            "CPU使用率": f"{psutil.cpu_percent()}%",
            "CPU频率": f"{psutil.cpu_freq().current:.2f}MHz" if hasattr(psutil, "cpu_freq") else "未知"
        }
    except Exception:
        cpu_info = {"CPU信息": "获取失败"}
    
    # 获取内存信息
    try:
        mem = psutil.virtual_memory()
        mem_info = {
            "总内存": f"{mem.total / (1024**3):.2f}GB",
            "已用内存": f"{mem.used / (1024**3):.2f}GB",
            "内存使用率": f"{mem.percent}%"
        }
    except Exception:
        mem_info = {"内存信息": "获取失败"}
    
    # 获取磁盘信息
    try:
        disk = psutil.disk_usage('/')
        disk_info = {
            "总磁盘空间": f"{disk.total / (1024**3):.2f}GB",
            "已用空间": f"{disk.used / (1024**3):.2f}GB",
            "磁盘使用率": f"{disk.percent}%"
        }
    except Exception:
        disk_info = {"磁盘信息": "获取失败"}
    
    # 获取系统启动时间
    try:
        boot_time = psutil.boot_time()
        current_time = time.time()
        uptime_seconds = current_time - boot_time
        
        system_uptime_info = {
            "系统启动时间": f"{datetime.fromtimestamp(boot_time):%Y-%m-%d %H:%M:%S}",
            "系统运行时间": format_time(uptime_seconds)
        }
    except Exception:
        system_uptime_info = {"系统运行时间": "获取失败"}
    
    # 组装系统信息
    msg = "====== 系统信息 ======\n"
    info_sections = [
        ("⏱⏱⏱ 运行时间", system_uptime_info),
        ("💻💻 系统信息", system_info),
        ("⚡⚡ CPU信息", cpu_info),
        ("🧠🧠 内存信息", mem_info),
        ("💾💾 磁盘信息", disk_info)
    ]
    
    for section, data in info_sections:
        msg += f"\n【{section}】\n"
        msg += "\n".join(f"{k}: {v}" for k, v in data.items())
    
    return msg

async def get_ping_test(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> str:
    """执行ping测试"""
    # 发送测试开始提示
    await ping_test_cmd.send("正在测试网络延迟，请稍候...")
    
    # 测试多个网站的ping
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
    
    # 分组测试：先测国内站点，再测国外站点
    msg = "====== 网络延迟测试 ======\n"
    
    # 国内站点测试
    msg += "\n【国内站点】\n"
    for name, host in list(sites.items())[:4]:  # 前4个是国内站点
        delay, is_timeout = ping_test(host)
        emoji = get_ping_emoji(delay)
        
        if is_timeout:
            msg += f"{emoji} {name}: 超时(0ms)\n"
        else:
            msg += f"{emoji} {name}: {delay:.3f}ms\n"
        
        time.sleep(1)  # 避免连续ping
    
    # 国外站点测试
    msg += "\n【国外站点】\n"
    for name, host in list(sites.items())[4:]:  # 后4个是国外站点
        delay, is_timeout = ping_test(host)
        emoji = get_ping_emoji(delay)
        
        if is_timeout:
            msg += f"{emoji} {name}: 超时(0ms)\n"
        else:
            msg += f"{emoji} {name}: {delay:.3f}ms\n"
        
        time.sleep(1)  # 避免连续ping
    
    return msg

@bot_info_cmd.handle()
async def handle_bot_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理bot信息命令"""
    msg = await get_bot_info(bot, event)
    await handle_send(bot, event, msg)

@sys_info_cmd.handle()
async def handle_sys_info(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理系统信息命令"""
    sys_msg = await get_system_info(bot, event)
    await handle_send(bot, event, msg)

@ping_test_cmd.handle()
async def handle_ping_test(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理ping测试命令"""
    ping_msg = await get_ping_test(bot, event)
    await handle_send(bot, event, msg)

@status_cmd.handle()
async def handle_status(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """处理状态命令 - 调用其他三个功能"""
    # 先发送Bot信息
    bot_msg = await get_bot_info(bot, event)
    await handle_send(bot, event, bot_msg)
    
    # 然后发送系统信息
    sys_msg = await get_system_info(bot, event)
    await handle_send(bot, event, sys_msg)
    
    # 最后执行ping测试
    ping_msg = await get_ping_test(bot, event)
    await handle_send(bot, event, ping_msg)
