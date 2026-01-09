import random
import json
import os
import asyncio
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
from nonebot import on_command
from nonebot.params import CommandArg
from nonebot.adapters.onebot.v11 import (
    Bot, Message, GroupMessageEvent, 
    PrivateMessageEvent, MessageSegment
)
from ..xiuxian_utils.lay_out import assign_bot, Cooldown
from ..xiuxian_utils.utils import check_user, get_msg_pic, handle_send, number_to, log_message
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from datetime import datetime, timedelta
from .games.gomoku import *
from .games.half_ten import *
sql_message = XiuxianDateManage()

# 五子棋
gomoku_help = on_command("五子棋帮助", priority=10, block=True)
gomoku_start = on_command("开始五子棋", priority=10, block=True)
gomoku_join = on_command("加入五子棋", priority=10, block=True)
gomoku_move = on_command("落子", priority=10, block=True)
gomoku_surrender = on_command("认输", priority=10, block=True)
gomoku_info = on_command("棋局信息", priority=10, block=True)
gomoku_quit = on_command("退出五子棋", priority=10, block=True)
# 十点半
half_ten_start = on_command("开始十点半", priority=10, block=True)
half_ten_join = on_command("加入十点半", priority=10, block=True)
half_ten_close = on_command("结算十点半", priority=10, block=True)
half_ten_quit = on_command("退出十点半", priority=10, block=True)
half_ten_info = on_command("十点半信息", priority=10, block=True)
half_ten_help = on_command("十点半帮助", priority=10, block=True)

# 开始五子棋命令
@gomoku_start.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gomoku_start_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """开始五子棋游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 检查用户是否已经在其他房间
    existing_room = room_manager.get_user_room(user_id)
    if existing_room:
        msg = f"您已经在房间 {existing_room} 中，请先退出当前房间再创建新房间！"
        await handle_send(bot, event, msg, md_type="游戏", k1="退出", v1="退出五子棋", k2="落子", v2="落子", k3="帮助", v3="五子棋帮助")
        return
    
    # 如果没有指定房间号，自动生成随机房间号
    if not arg:
        room_id = generate_random_room_id()
        # 确保房间号不重复
        while room_manager.get_room(room_id):
            room_id = generate_random_room_id()
    else:
        room_id = arg
    
    game = room_manager.create_room(room_id, user_id)
    
    if game is None:
        if room_manager.get_user_room(user_id):
            msg = "您已经在其他房间中，无法创建新房间！"
        else:
            msg = f"房间 {room_id} 已存在！请换一个房间号。"
        await handle_send(bot, event, msg, md_type="游戏", k1="创建", v1="开始五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    # 记录用户房间状态
    user_room_status[user_id] = room_id
    
    # 创建初始棋盘图片
    board_image = create_board_image(game)
    
    msg = (
        f"五子棋房间 {room_id} 创建成功！\n"
        f"创建者：{user_info['user_name']}（黑棋）\n"
        f"等待其他玩家加入...\n"
        f"房间将在 {ROOM_TIMEOUT} 秒后自动关闭\n"
        f"其他玩家可以使用命令：加入五子棋 {room_id}"
    )
    
    await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1=f"加入五子棋 {room_id}", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
    await bot.send(event, MessageSegment.image(board_image))
    
    # 启动房间超时任务
    await start_room_timeout(bot, event, room_id)

# 加入五子棋命令
@gomoku_join.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gomoku_join_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """加入五子棋游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 检查用户是否已经在其他房间
    existing_room = room_manager.get_user_room(user_id)
    if existing_room:
        msg = f"您已经在房间 {existing_room} 中，请先退出当前房间再加入新房间！"
        await handle_send(bot, event, msg, md_type="游戏", k1="退出", v1=f"退出五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    if not arg:
        msg = "请指定要加入的房间号！例如：加入五子棋 房间001"
        await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    room_id = arg
    success = room_manager.join_room(room_id, user_id)
    
    if not success:
        if room_manager.get_user_room(user_id):
            msg = "您已经在其他房间中，无法加入新房间！"
        else:
            msg = f"加入房间 {room_id} 失败！房间可能不存在或已满。"
        await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    # 记录用户房间状态
    user_room_status[user_id] = room_id
    
    # 取消房间超时任务
    if room_id in room_timeout_tasks:
        room_timeout_tasks[room_id].cancel()
        del room_timeout_tasks[room_id]
    
    game = room_manager.get_room(room_id)
    
    # 更新棋盘图片
    board_image = create_board_image(game)
    
    black_player_info = sql_message.get_user_info_with_id(game.player_black)
    white_player_info = sql_message.get_user_info_with_id(game.player_white)
    
    msg = (
        f"成功加入五子棋房间 {room_id}！\n"
        f"黑棋：{black_player_info['user_name']}\n"
        f"白棋：{white_player_info['user_name']}\n"
        f"游戏开始！黑棋先行。\n"
        f"落子超时时间：{MOVE_TIMEOUT} 秒\n"
        f"使用命令：落子 A1 来下棋"
    )
    
    await handle_send(bot, event, msg, md_type="游戏", k1="落子", v1="落子", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
    await bot.send(event, MessageSegment.image(board_image))
    
    # 启动落子超时任务
    await start_move_timeout(bot, event, room_id)

# 落子命令
@gomoku_move.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gomoku_move_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """落子操作"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    if not arg:
        msg = "请指定落子位置！例如：落子 A1 或 落子 B15"
        await handle_send(bot, event, msg, md_type="游戏", k1="落子", v1="落子", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    # 查找用户所在的房间
    user_room = room_manager.get_user_room(user_id)
    
    if user_room is None:
        msg = "您当前没有参与任何五子棋游戏！"
        await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    game = room_manager.get_room(user_room)
    
    if game.status != "playing":
        msg = "游戏尚未开始或已经结束！"
        await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    if game.current_player != user_id:
        msg = "现在不是您的回合！请等待对方落子。"
        await handle_send(bot, event, msg, md_type="游戏", k1="落子", v1="落子", k2="信息", v2="棋局信息", k3="认输", v3="认输")
        return
    
    # 解析坐标
    position = coordinate_to_position(arg)
    if position is None:
        msg = f"坐标 {arg} 无效！请使用类似 A1、B15 的格式。"
        await handle_send(bot, event, msg, md_type="游戏", k1="落子", v1="落子", k2="信息", v2="棋局信息", k3="认输", v3="认输")
        return
    
    x, y = position
    
    # 检查位置是否可用
    if game.board[y][x] != 0:
        msg = f"位置 {arg} 已经有棋子了！请选择其他位置。"
        await handle_send(bot, event, msg, md_type="游戏", k1="落子", v1="落子", k2="信息", v2="棋局信息", k3="认输", v3="认输")
        return
    
    # 落子
    player_stone = 1 if user_id == game.player_black else 2
    game.board[y][x] = player_stone
    game.moves.append((x, y))
    game.last_move_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 检查是否获胜
    if check_win(game.board, x, y, player_stone):
        game.status = "finished"
        game.winner = user_id
        game.current_player = None
        
        # 取消落子超时任务
        if user_room in move_timeout_tasks:
            move_timeout_tasks[user_room].cancel()
            del move_timeout_tasks[user_room]
        
        winner_info = sql_message.get_user_info_with_id(user_id)
        msg = f"🎉 恭喜 {winner_info['user_name']} 获胜！五子连珠！"
        
    else:
        # 切换回合
        game.current_player = game.player_white if user_id == game.player_black else game.player_black
        next_player_info = sql_message.get_user_info_with_id(game.current_player)
        msg = f"落子成功！轮到 {next_player_info['user_name']} 的回合。"
        
        # 重启落子超时任务
        await start_move_timeout(bot, event, user_room)
    
    # 保存游戏状态
    room_manager.save_room(user_room)
    
    # 更新棋盘图片
    board_image = create_board_image(game)
    
    await handle_send(bot, event, msg, md_type="游戏", k1="落子", v1="落子", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
    await bot.send(event, MessageSegment.image(board_image))
    
    # 如果游戏结束，清理房间
    if game.status == "finished":
        room_manager.delete_room(user_room)

# 认输命令
@gomoku_surrender.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gomoku_surrender_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """认输操作"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    # 查找用户所在的房间
    user_room = room_manager.get_user_room(user_id)
    
    if user_room is None:
        msg = "您当前没有参与任何五子棋游戏！"
        await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    game = room_manager.get_room(user_room)
    
    if game.status != "playing":
        msg = "游戏尚未开始或已经结束！"
        await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
        return
    
    # 取消超时任务
    if user_room in move_timeout_tasks:
        move_timeout_tasks[user_room].cancel()
        del move_timeout_tasks[user_room]
    
    # 确定获胜者
    winner_id = game.player_white if user_id == game.player_black else game.player_black
    winner_info = sql_message.get_user_info_with_id(winner_id)
    
    game.status = "finished"
    game.winner = winner_id
    game.current_player = None
    
    msg = f"{user_info['user_name']} 认输！恭喜 {winner_info['user_name']} 获胜！"
    
    # 保存最终棋盘
    board_image = create_board_image(game)
    
    await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
    await bot.send(event, MessageSegment.image(board_image))
    
    # 清理房间
    room_manager.delete_room(user_room)

# 棋局信息命令
@gomoku_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gomoku_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看棋局信息"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    arg = args.extract_plain_text().strip()
    
    if arg:
        # 查看指定房间
        room_id = arg
        game = room_manager.get_room(room_id)
        
        if game is None:
            msg = f"房间 {room_id} 不存在！"
            await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
            return
    else:
        # 查看自己参与的房间
        
        user_id = user_info['user_id']
        
        user_room = room_manager.get_user_room(user_id)
        
        if user_room is None:
            msg = "您当前没有参与任何五子棋游戏！"
            await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
            return
        
        game = room_manager.get_room(user_room)
        room_id = user_room
    
    # 获取玩家信息
    black_player_info = sql_message.get_user_info_with_id(game.player_black)
    black_name = black_player_info['user_name'] if black_player_info else "未知玩家"
    
    white_name = "等待加入"
    if game.player_white:
        white_player_info = sql_message.get_user_info_with_id(game.player_white)
        white_name = white_player_info['user_name'] if white_player_info else "未知玩家"
    
    # 构建信息消息
    status_map = {
        "waiting": "等待中",
        "playing": "进行中", 
        "finished": "已结束"
    }
    
    msg = (
        f"五子棋房间：{room_id}\n"
        f"状态：{status_map[game.status]}\n"
        f"黑棋：{black_name}\n"
        f"白棋：{white_name}\n"
        f"总步数：{len(game.moves)}\n"
    )
    
    if game.status == "playing":
        current_player_info = sql_message.get_user_info_with_id(game.current_player)
        # 计算剩余时间
        if game.last_move_time:
            last_time = datetime.strptime(game.last_move_time, "%Y-%m-%d %H:%M:%S")
            elapsed = (datetime.now() - last_time).total_seconds()
            remaining = max(MOVE_TIMEOUT - elapsed, 0)
            msg += f"当前回合：{current_player_info['user_name']}\n"
            msg += f"剩余时间：{int(remaining)} 秒\n"
        msg += "使用命令：落子 A1 来下棋"
    elif game.status == "finished" and game.winner:
        winner_info = sql_message.get_user_info_with_id(game.winner)
        msg += f"获胜者：{winner_info['user_name']}"
    
    # 发送棋盘图片
    board_image = create_board_image(game)
    
    await handle_send(bot, event, msg, md_type="游戏", k1="落子", v1="落子", k2="信息", v2="棋局信息", k3="认输", v3="认输")
    await bot.send(event, MessageSegment.image(board_image))

# 退出五子棋命令
@gomoku_quit.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gomoku_quit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """退出五子棋游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    # 退出房间
    success, result = room_manager.quit_room(user_id)
    
    if not success:
        await handle_send(bot, event, result)
        return
    
    if result.startswith("quit_success"):
        _, room_id = result.split("|")
        
        # 取消可能的超时任务
        if room_id in room_timeout_tasks:
            room_timeout_tasks[room_id].cancel()
            del room_timeout_tasks[room_id]
        if room_id in move_timeout_tasks:
            move_timeout_tasks[room_id].cancel()
            del move_timeout_tasks[room_id]
        
        msg = f"您已成功退出五子棋房间 {room_id}！"
    
    await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")

@gomoku_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def gomoku_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """五子棋帮助信息"""
    help_msg = f"""※※ 五子棋游戏帮助 ※※

【开始五子棋 房间号】- 创建五子棋房间（不指定房间号自动生成）
【加入五子棋 房间号】- 加入已有房间  
【落子 坐标】- 在指定位置落子（如：落子 A1）
【认输】- 主动认输结束游戏
【退出五子棋】- 退出当前房间（仅限等待中状态）
【棋局信息】- 查看当前棋局状态
【棋局信息 房间号】- 查看指定房间信息

◆ 棋盘坐标：A1 到 AD30（30x30棋盘）
◆ 黑棋先手，轮流落子
◆ 先形成五子连珠者获胜
◆ 连珠方向：横、竖、斜均可
◆ 房间超时：{ROOM_TIMEOUT}秒无人加入自动关闭
◆ 落子超时：{MOVE_TIMEOUT}秒未落子自动判负
◆ 同一时间只能参与一个房间

祝您游戏愉快！"""
    
    await handle_send(bot, event, help_msg, md_type="游戏", k1="开始", v1="开始五子棋", k2="加入", v2="加入五子棋", k3="信息", v3="棋局信息")

# 开始十点半命令
@half_ten_start.handle(parameterless=[Cooldown(cd_time=1.4)])
async def half_ten_start_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """开始十点半游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 检查用户是否已经在其他房间
    existing_room = half_manager.get_user_room(user_id)
    if existing_room:
        game = half_manager.get_room(existing_room)
        if game and game.status == "waiting":
            msg = f"您已经在房间 {existing_room} 中，请先退出当前房间再创建新房间！"
            await handle_send(bot, event, msg, md_type="游戏", k1="退出", v1="退出十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
            return
    
    # 如果没有指定房间号，自动生成随机房间号
    if not arg:
        room_id = generate_random_half_id()
        # 确保房间号不重复
        while half_manager.get_room(room_id):
            room_id = generate_random_half_id()
    else:
        room_id = arg
    
    game = half_manager.create_room(room_id, user_id)
    
    if game is None:
        if half_manager.get_user_room(user_id):
            msg = "您已经在其他房间中，无法创建新房间！"
        else:
            msg = f"房间 {room_id} 已存在！请换一个房间号。"
        await handle_send(bot, event, msg, md_type="游戏", k1="创建", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
        return
    
    # 记录用户房间状态
    user_half_status[user_id] = room_id
    
    msg = (
        f"十点半房间 {room_id} 创建成功！\n"
        f"房主：{user_info['user_name']}\n"
        f"当前人数：1/{MAX_PLAYERS}\n"
        f"最少需要：{MIN_PLAYERS}人，最多支持：{MAX_PLAYERS}人\n"
        f"房间将在 {HALF_TIMEOUT} 秒后自动结算\n"
        f"其他玩家可以使用命令：加入十点半 {room_id}\n"
        f"房主可以使用命令：结算十点半 手动开始游戏\n"
        f"使用命令：退出十点半 可以退出当前房间"
    )
    
    await handle_send(bot, event, msg, md_type="游戏", k1="退出", v1="退出十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
    
    # 启动房间超时任务
    await start_half_timeout(bot, event, room_id)

# 加入十点半命令
@half_ten_join.handle(parameterless=[Cooldown(cd_time=1.4)])
async def half_ten_join_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """加入十点半游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 检查用户是否已经在其他房间
    existing_room = half_manager.get_user_room(user_id)
    if existing_room:
        game = half_manager.get_room(existing_room)
        if game and game.status == "waiting":
            msg = f"您已经在房间 {existing_room} 中，请先退出当前房间再加入新房间！"
            await handle_send(bot, event, msg, md_type="游戏", k1="退出", v1="退出十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
            return
    
    if not arg:
        msg = "请指定要加入的房间号！例如：加入十点半 房间001"
        await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
        return
    
    room_id = arg
    success = half_manager.join_room(room_id, user_id)
    
    if not success:
        if half_manager.get_user_room(user_id):
            msg = "您已经在其他房间中，无法加入新房间！"
        else:
            msg = f"加入房间 {room_id} 失败！房间可能不存在或已满。"
        await handle_send(bot, event, msg, md_type="游戏", k1="加入", v1="加入十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
        return
    
    # 记录用户房间状态
    user_half_status[user_id] = room_id
    
    game = half_manager.get_room(room_id)
    
    # 检查是否达到最大人数，自动开始游戏
    if len(game.players) >= MAX_PLAYERS:
        # 取消超时任务
        if room_id in half_timeout_tasks:
            half_timeout_tasks[room_id].cancel()
            del half_timeout_tasks[room_id]
        
        # 开始游戏
        half_manager.start_game(room_id)
        game = half_manager.get_room(room_id)
        
        # 发送游戏结果文本
        result_text = create_game_text(game)
        winner_info = sql_message.get_user_info_with_id(game.winner) if game.winner else None
        winner_name = winner_info['user_name'] if winner_info else "未知玩家"
        
        msg = (
            f"十点半房间 {room_id} 人数已满，游戏开始！\n"
            f"参赛人数：{len(game.players)}人\n"
            f"🎉 恭喜 {winner_name} 获得冠军！\n\n"
            f"{result_text}"
        )
        
        await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
        
        # 清理房间
        half_manager.delete_room(room_id)
    else:
        # 更新房间信息
        creator_info = sql_message.get_user_info_with_id(game.creator_id)
        
        msg = (
            f"成功加入十点半房间 {room_id}！\n"
            f"房主：{creator_info['user_name']}\n"
            f"当前人数：{len(game.players)}/{MAX_PLAYERS}\n"
            f"还需 {max(0, MIN_PLAYERS - len(game.players))} 人即可开始游戏\n"
            f"人数满{MAX_PLAYERS}人将自动开始游戏"
        )
        
        await handle_send(bot, event, msg, md_type="游戏", k1="退出", v1="退出十点半", k2="信息", v2="十点半信息", k3="结算", v3="结算十点半")
        
        # 重启超时任务（因为人数变化）
        await start_half_timeout(bot, event, room_id)

# 结算十点半命令
@half_ten_close.handle(parameterless=[Cooldown(cd_time=1.4)])
async def half_ten_close_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """结算十点半游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    # 查找用户所在的房间
    user_room = half_manager.get_user_room(user_id)
    
    if user_room is None:
        msg = "您当前没有参与任何十点半游戏！"
        await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
        return
    
    # 手动结算房间
    success, result = half_manager.close_room_manually(user_room, user_id)
    
    if not success:
        await handle_send(bot, event, result)
        return
    
    if result == "close":
        # 人数不足，关闭房间
        msg = f"人数不足{MIN_PLAYERS}人，房间 {user_room} 已关闭！"
        half_manager.delete_room(user_room)
        await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
        return
    
    # 开始游戏
    game = half_manager.get_room(user_room)
    
    # 取消超时任务
    if user_room in half_timeout_tasks:
        half_timeout_tasks[user_room].cancel()
        del half_timeout_tasks[user_room]
    
    # 发送游戏结果文本
    result_text = create_game_text(game)
    winner_info = sql_message.get_user_info_with_id(game.winner) if game.winner else None
    winner_name = winner_info['user_name'] if winner_info else "未知玩家"
    
    msg = (
        f"十点半房间 {user_room} 游戏开始！\n"
        f"参赛人数：{len(game.players)}人\n"
        f"🎉 恭喜 {winner_name} 获得冠军！\n\n"
        f"{result_text}"
    )
    
    await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
    
    # 清理房间
    half_manager.delete_room(user_room)

# 退出十点半命令
@half_ten_quit.handle(parameterless=[Cooldown(cd_time=1.4)])
async def half_ten_quit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """退出十点半游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    
    # 退出房间
    success, result = half_manager.quit_room(user_id)
    
    if not success:
        await handle_send(bot, event, result)
        return
    
    if result == "quit_and_close":
        msg = "您已退出房间，由于房间内没有其他玩家，房间已关闭！"
    elif result.startswith("quit_success"):
        _, room_id, new_creator_name = result.split("|")
        msg = (
            f"您已成功退出房间 {room_id}！\n"
            f"新房主变更为：{new_creator_name}"
        )
    else:
        msg = "退出成功！"
    
    await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")

# 十点半信息命令
@half_ten_info.handle(parameterless=[Cooldown(cd_time=1.4)])
async def half_ten_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看十点半游戏信息"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg, md_type="我要修仙")
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 如果没有指定房间号，查看自己所在的房间
    if not arg:
        room_id = half_manager.get_user_room(user_id)
        if not room_id:
            msg = "您当前没有参与任何十点半游戏！请指定房间号或先加入一个房间。"
            await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
            return
    else:
        room_id = arg
    
    game = half_manager.get_room(room_id)
    if not game:
        msg = f"房间 {room_id} 不存在！"
        await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
        return
    
    # 构建房间信息
    creator_info = sql_message.get_user_info_with_id(game.creator_id)
    creator_name = creator_info['user_name'] if creator_info else "未知玩家"
    
    players_info = []
    for player_id in game.players:
        player_info = sql_message.get_user_info_with_id(player_id)
        player_name = player_info['user_name'] if player_info else f"玩家{player_id}"
        players_info.append(player_name)
    
    status_map = {
        "waiting": "等待中",
        "playing": "进行中", 
        "finished": "已结束",
        "closed": "已关闭"
    }
    
    msg = (
        f"十点半房间信息 - {room_id}\n"
        f"状态：{status_map.get(game.status, game.status)}\n"
        f"房主：{creator_name}\n"
        f"玩家人数：{len(game.players)}/{MAX_PLAYERS}\n"
        f"创建时间：{game.create_time}\n"
        f"玩家列表：{', '.join(players_info)}"
    )
    
    if game.status == "finished" and game.winner:
        winner_info = sql_message.get_user_info_with_id(game.winner)
        winner_name = winner_info['user_name'] if winner_info else "未知玩家"
        msg += f"\n🎉 冠军：{winner_name}"
    
    if game.close_reason:
        msg += f"\n关闭原因：{game.close_reason}"
    
    await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")

# 十点半帮助命令
@half_ten_help.handle(parameterless=[Cooldown(cd_time=1.4)])
async def half_ten_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """十点半游戏帮助"""
    help_msg = f"""
🎮 十点半游戏帮助 🎮

【游戏规则】
- 每人发3张牌，计算点数总和
- A=1点，2-9=对应点数，10/J/Q/K=0.5点
- 点数取个位数（10.5除外）
- 10.5为最大牌型，其次按点数大小排名
- 点数相同按加入顺序排名

【游戏命令】
1. 开始十点半 [房间号] - 创建房间（不填房间号自动生成）
2. 加入十点半 <房间号> - 加入指定房间
3. 结算十点半 - 房主手动开始游戏
4. 退出十点半 - 退出当前房间
5. 十点半信息 [房间号] - 查看房间信息
6. 十点半帮助 - 查看本帮助

【游戏设置】
- 最少玩家：2人
- 最多玩家：10人
- 房间超时：{HALF_TIMEOUT}秒自动结算
- 满{MAX_PLAYERS}人自动开始游戏

【胜负判定】
🥇 冠军：点数最高者（10.5为最大）
🥈 亚军：点数第二高者  
🥉 季军：点数第三高者

祝您游戏愉快！🎉
"""
    await handle_send(bot, event, help_msg)

async def start_room_timeout(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, room_id: str):
    """启动房间超时任务"""
    if room_id in room_timeout_tasks:
        room_timeout_tasks[room_id].cancel()
    
    async def room_timeout():
        await asyncio.sleep(ROOM_TIMEOUT)
        game = room_manager.get_room(room_id)
        if game and game.status == "waiting" and game.player_white is None:
            # 房间超时，自动关闭
            creator_info = sql_message.get_user_info_with_id(game.player_black)
            msg = f"五子棋房间 {room_id} 已超时（{ROOM_TIMEOUT}秒无人加入），房间已自动关闭！"
            await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
            room_manager.delete_room(room_id)
    
    task = asyncio.create_task(room_timeout())
    room_timeout_tasks[room_id] = task

async def start_move_timeout(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, room_id: str):
    """启动落子超时任务"""
    if room_id in move_timeout_tasks:
        move_timeout_tasks[room_id].cancel()
    
    async def move_timeout():
        await asyncio.sleep(MOVE_TIMEOUT)
        game = room_manager.get_room(room_id)
        if game and game.status == "playing":
            # 检查最后落子时间
            if game.last_move_time:
                last_time = datetime.strptime(game.last_move_time, "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - last_time).total_seconds() >= MOVE_TIMEOUT:
                    # 超时判负
                    timeout_player = game.current_player
                    winner_id = game.player_white if timeout_player == game.player_black else game.player_black
                    
                    timeout_info = sql_message.get_user_info_with_id(timeout_player)
                    winner_info = sql_message.get_user_info_with_id(winner_id)
                    
                    game.status = "finished"
                    game.winner = winner_id
                    game.current_player = None
                    
                    msg = f"玩家 {timeout_info['user_name']} 超时未落子，自动判负！恭喜 {winner_info['user_name']} 获胜！"
                    
                    # 保存最终棋盘
                    board_image = create_board_image(game)
                    
                    await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始五子棋", k2="信息", v2="棋局信息", k3="帮助", v3="五子棋帮助")
                    await bot.send(event, MessageSegment.image(board_image))
                    
                    # 清理房间
                    room_manager.delete_room(room_id)
    
    task = asyncio.create_task(move_timeout())
    move_timeout_tasks[room_id] = task

async def start_half_timeout(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, room_id: str):
    """启动房间超时任务"""
    if room_id in half_timeout_tasks:
        half_timeout_tasks[room_id].cancel()
    
    async def room_timeout():
        await asyncio.sleep(HALF_TIMEOUT)
        game = half_manager.get_room(room_id)
        if game and game.status == "waiting":
            # 检查是否满足最低人数要求
            if len(game.players) >= MIN_PLAYERS:
                # 自动开始游戏
                half_manager.start_game(room_id)
                game = half_manager.get_room(room_id)
                
                # 发送游戏结果文本
                result_text = create_game_text(game)
                winner_info = sql_message.get_user_info_with_id(game.winner) if game.winner else None
                winner_name = winner_info['user_name'] if winner_info else "未知玩家"
                
                msg = (
                    f"十点半房间 {room_id} 已超时，游戏自动开始！\n"
                    f"参赛人数：{len(game.players)}人\n"
                    f"🎉 恭喜 {winner_name} 获得冠军！\n\n"
                    f"{result_text}"
                )
                
                await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
                
                # 清理房间
                half_manager.delete_room(room_id)
            else:
                # 人数不足，关闭房间
                creator_info = sql_message.get_user_info_with_id(game.creator_id)
                msg = f"十点半房间 {room_id} 已超时（{HALF_TIMEOUT}秒后人数不足{MIN_PLAYERS}人），房间已自动关闭！"
                game.close_room("超时人数不足自动关闭")
                half_manager.save_room(room_id)
                half_manager.delete_room(room_id)
                await handle_send(bot, event, msg, md_type="游戏", k1="开始", v1="开始十点半", k2="信息", v2="十点半信息", k3="帮助", v3="十点半帮助")
    
    task = asyncio.create_task(room_timeout())
    half_timeout_tasks[room_id] = task
