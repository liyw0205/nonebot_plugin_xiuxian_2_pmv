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

sql_message = XiuxianDateManage()

# 五子棋数据路径
GOMOKU_DATA_PATH = Path(__file__).parent / "games" / "gomoku"
GOMOKU_BOARDS_PATH = GOMOKU_DATA_PATH / "boards"
GOMOKU_ROOMS_PATH = GOMOKU_DATA_PATH / "rooms"

# 创建必要的目录
GOMOKU_BOARDS_PATH.mkdir(parents=True, exist_ok=True)
GOMOKU_ROOMS_PATH.mkdir(parents=True, exist_ok=True)

# 帮助命令
gomoku_help = on_command("五子棋帮助", priority=10, block=True)
gomoku_start = on_command("开始五子棋", priority=10, block=True)
gomoku_join = on_command("加入五子棋", priority=10, block=True)
gomoku_move = on_command("落子", priority=10, block=True)
gomoku_surrender = on_command("认输", priority=10, block=True)
gomoku_info = on_command("棋局信息", priority=10, block=True)
gomoku_quit = on_command("退出五子棋", priority=10, block=True)

# 棋盘配置
BOARD_SIZE = 30  # 30x30 棋盘
CELL_SIZE = 30   # 每个格子30像素
MARGIN = 50      # 边距
BOARD_COLOR = (210, 180, 140)  # 棋盘颜色 (米色)
LINE_COLOR = (0, 0, 0)         # 线条颜色 (黑色)
BLACK_STONE = (0, 0, 0)        # 黑棋颜色
WHITE_STONE = (255, 255, 255)  # 白棋颜色
STONE_BORDER = (100, 100, 100) # 棋子边框
COORD_COLOR = (0, 0, 0)        # 坐标颜色

# 超时配置
ROOM_TIMEOUT = 180  # 房间等待超时时间（秒）
MOVE_TIMEOUT = 120  # 落子超时时间（秒）

# 用户状态跟踪
user_room_status = {}  # 记录用户当前所在的房间 {user_id: room_id}
room_timeout_tasks = {}  # 房间超时任务 {room_id: task}
move_timeout_tasks = {}  # 落子超时任务 {room_id: task}

class GomokuGame:
    def __init__(self, room_id: str, creator_id: str):
        self.room_id = room_id
        self.creator_id = creator_id
        self.player_black = creator_id  # 创建者为黑棋
        self.player_white = None        # 等待加入的白棋
        self.current_player = creator_id # 当前回合玩家
        self.board = [[0 for _ in range(BOARD_SIZE)] for _ in range(BOARD_SIZE)]  # 0:空, 1:黑, 2:白
        self.moves = []  # 落子记录
        self.status = "waiting"  # waiting, playing, finished
        self.winner = None
        self.create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.last_move_time = None  # 最后落子时间
        
    def to_dict(self):
        return {
            "room_id": self.room_id,
            "creator_id": self.creator_id,
            "player_black": self.player_black,
            "player_white": self.player_white,
            "current_player": self.current_player,
            "board": self.board,
            "moves": self.moves,
            "status": self.status,
            "winner": self.winner,
            "create_time": self.create_time,
            "last_move_time": self.last_move_time
        }
    
    @classmethod
    def from_dict(cls, data):
        game = cls(data["room_id"], data["creator_id"])
        game.player_black = data["player_black"]
        game.player_white = data["player_white"]
        game.current_player = data["current_player"]
        game.board = data["board"]
        game.moves = data["moves"]
        game.status = data["status"]
        game.winner = data["winner"]
        game.create_time = data["create_time"]
        game.last_move_time = data.get("last_move_time")
        return game

# 房间管理
class GomokuRoomManager:
    def __init__(self):
        self.rooms = {}
        self.load_rooms()
    
    def load_rooms(self):
        """加载所有房间数据"""
        for room_file in GOMOKU_ROOMS_PATH.glob("*.json"):
            try:
                with open(room_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    room_id = room_file.stem
                    self.rooms[room_id] = GomokuGame.from_dict(data)
            except Exception as e:
                print(f"加载房间 {room_file} 失败: {e}")
    
    def save_room(self, room_id: str):
        """保存房间数据"""
        if room_id in self.rooms:
            room_file = GOMOKU_ROOMS_PATH / f"{room_id}.json"
            with open(room_file, 'w', encoding='utf-8') as f:
                json.dump(self.rooms[room_id].to_dict(), f, ensure_ascii=False, indent=2)
    
    def create_room(self, room_id: str, creator_id: str) -> GomokuGame:
        """创建新房间"""
        if room_id in self.rooms:
            return None
        
        # 检查创建者是否已经在其他房间
        for existing_room_id, existing_game in self.rooms.items():
            if (creator_id == existing_game.player_black or 
                creator_id == existing_game.player_white):
                return None
        
        game = GomokuGame(room_id, creator_id)
        self.rooms[room_id] = game
        self.save_room(room_id)
        return game
    
    def join_room(self, room_id: str, player_id: str) -> bool:
        """加入房间"""
        if room_id not in self.rooms:
            return False
        
        game = self.rooms[room_id]
        
        # 检查加入者是否已经在其他房间
        for existing_room_id, existing_game in self.rooms.items():
            if (player_id == existing_game.player_black or 
                player_id == existing_game.player_white):
                return False
        
        if game.status != "waiting" and game.player_white is not None:
            return False
        
        game.player_white = player_id
        game.status = "playing"
        game.current_player = game.player_black  # 黑棋先手
        game.last_move_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_room(room_id)
        return True
    
    def get_room(self, room_id: str) -> GomokuGame:
        """获取房间"""
        return self.rooms.get(room_id)

    def quit_room(self, user_id: str) -> tuple:
        """玩家退出房间"""
        room_id = self.get_user_room(user_id)
        if not room_id:
            return False, "您当前没有参与任何五子棋游戏"
        
        game = self.rooms[room_id]
        
        # 如果游戏正在进行中，需要特殊处理
        if game.status == "playing":
            return False, "游戏正在进行中，请使用【认输】命令或等待游戏结束"
        
        # 移除玩家
        other_player = None
        other_player_name = "对方"
        
        if user_id == game.player_black:
            other_player = game.player_white
        elif user_id == game.player_white:
            other_player = game.player_black
        
        # 获取对方玩家名称
        if other_player:
            other_player_info = sql_message.get_user_info_with_id(other_player)
            other_player_name = other_player_info['user_name'] if other_player_info else "对方"
        
        # 删除房间
        self.delete_room(room_id)
        
        return True, f"quit_success|{room_id}|{other_player_name}"

    def delete_room(self, room_id: str):
        """删除房间"""
        if room_id in self.rooms:
            # 清理用户状态
            game = self.rooms[room_id]
            if game.player_black in user_room_status:
                del user_room_status[game.player_black]
            if game.player_white and game.player_white in user_room_status:
                del user_room_status[game.player_white]
            
            # 删除文件
            room_file = GOMOKU_ROOMS_PATH / f"{room_id}.json"
            if room_file.exists():
                room_file.unlink()
            del self.rooms[room_id]
    
    def get_user_room(self, user_id: str) -> str:
        """获取用户所在的房间ID"""
        for room_id, game in self.rooms.items():
            if user_id in [game.player_black, game.player_white]:
                return room_id
        return None

# 全局房间管理器
room_manager = GomokuRoomManager()

def generate_random_room_id() -> str:
    """生成随机房间号"""
    return f"{random.randint(1000, 9999)}"

def create_board_image(game: GomokuGame) -> BytesIO:
    """创建棋盘图片"""
    img_width = BOARD_SIZE * CELL_SIZE + MARGIN * 2
    img_height = BOARD_SIZE * CELL_SIZE + MARGIN * 2
    
    # 创建图片
    img = Image.new('RGB', (img_width, img_height), BOARD_COLOR)
    draw = ImageDraw.Draw(img)
    
    # 绘制棋盘网格
    for i in range(BOARD_SIZE):
        # 横线
        y = MARGIN + i * CELL_SIZE
        draw.line([(MARGIN, y), (img_width - MARGIN, y)], fill=LINE_COLOR, width=2)
        
        # 竖线
        x = MARGIN + i * CELL_SIZE
        draw.line([(x, MARGIN), (x, img_height - MARGIN)], fill=LINE_COLOR, width=2)
    
    # 绘制天元和星位
    star_points = [3, 9, 15, 21, 27]  # 30x30棋盘的星位
    for x in star_points:
        for y in star_points:
            center_x = MARGIN + x * CELL_SIZE
            center_y = MARGIN + y * CELL_SIZE
            draw.ellipse([(center_x-3, center_y-3), (center_x+3, center_y+3)], fill=LINE_COLOR)
    
    # 绘制棋子
    stone_radius = CELL_SIZE // 2 - 2
    for y in range(BOARD_SIZE):
        for x in range(BOARD_SIZE):
            if game.board[y][x] != 0:
                center_x = MARGIN + x * CELL_SIZE
                center_y = MARGIN + y * CELL_SIZE
                
                if game.board[y][x] == 1:  # 黑棋
                    draw.ellipse([(center_x-stone_radius, center_y-stone_radius),
                                (center_x+stone_radius, center_y+stone_radius)], 
                               fill=BLACK_STONE, outline=STONE_BORDER, width=2)
                else:  # 白棋
                    draw.ellipse([(center_x-stone_radius, center_y-stone_radius),
                                (center_x+stone_radius, center_y+stone_radius)], 
                               fill=WHITE_STONE, outline=STONE_BORDER, width=2)
    
    # 添加最后落子标记（如果有）
    if game.moves:
        last_x, last_y = game.moves[-1]
        center_x = MARGIN + last_x * CELL_SIZE
        center_y = MARGIN + last_y * CELL_SIZE
        marker_radius = 4
        draw.ellipse([(center_x-marker_radius, center_y-marker_radius),
                     (center_x+marker_radius, center_y+marker_radius)], 
                    fill=(255, 0, 0))  # 红色标记
    
    # 绘制坐标
    font_size = 12
    try:
        font = ImageFont.truetype("arial.ttf", font_size)
    except:
        font = ImageFont.load_default()
    
    # 绘制横坐标（字母）
    for i in range(BOARD_SIZE):
        x = MARGIN + i * CELL_SIZE
        coord_text = position_to_coordinate(i, 0).replace('1', '')  # 只显示字母部分
        bbox = draw.textbbox((0, 0), coord_text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        draw.text((x - text_width//2, MARGIN - text_height - 5), 
                 coord_text, fill=COORD_COLOR, font=font)
        draw.text((x - text_width//2, img_height - MARGIN + 5), 
                 coord_text, fill=COORD_COLOR, font=font)
    
    # 绘制纵坐标（数字）
    for i in range(BOARD_SIZE):
        y = MARGIN + i * CELL_SIZE
        coord_text = str(i + 1)
        bbox = draw.textbbox((0, 0), coord_text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        draw.text((MARGIN - text_width - 5, y - text_height//2), 
                 coord_text, fill=COORD_COLOR, font=font)
        draw.text((img_width - MARGIN + 5, y - text_height//2), 
                 coord_text, fill=COORD_COLOR, font=font)
    
    # 保存图片到BytesIO
    img_bytes = BytesIO()
    img.save(img_bytes, format='PNG')
    img_bytes.seek(0)
    return img_bytes

def check_win(board, x, y, player):
    """检查是否获胜"""
    directions = [
        [(1, 0), (-1, 0)],   # 水平
        [(0, 1), (0, -1)],   # 垂直
        [(1, 1), (-1, -1)],  # 主对角线
        [(1, -1), (-1, 1)]   # 副对角线
    ]
    
    for direction_pair in directions:
        count = 1  # 当前位置的棋子
        
        for dx, dy in direction_pair:
            temp_x, temp_y = x, y
            for _ in range(4):  # 检查4个方向各4个棋子
                temp_x += dx
                temp_y += dy
                if (0 <= temp_x < BOARD_SIZE and 0 <= temp_y < BOARD_SIZE and 
                    board[temp_y][temp_x] == player):
                    count += 1
                else:
                    break
        
        if count >= 5:
            return True
    
    return False

def coordinate_to_position(coord: str) -> tuple:
    """将坐标转换为棋盘位置"""
    if len(coord) < 2:
        return None
    
    try:
        # 处理字母坐标（A-Z, AA-AZ等）
        col_str = ''
        row_str = ''
        
        for char in coord:
            if char.isalpha():
                col_str += char.upper()
            elif char.isdigit():
                row_str += char
        
        if not col_str or not row_str:
            return None
        
        # 将字母转换为数字（A=0, B=1, ..., Z=25, AA=26, AB=27, ...）
        col = 0
        for i, char in enumerate(reversed(col_str)):
            col += (ord(char) - ord('A') + 1) * (26 ** i)
        col -= 1  # 调整为0-based
        
        row = int(row_str) - 1  # 调整为0-based
        
        if 0 <= col < BOARD_SIZE and 0 <= row < BOARD_SIZE:
            return (col, row)
        else:
            return None
            
    except:
        return None

def position_to_coordinate(x: int, y: int) -> str:
    """将棋盘位置转换为坐标"""
    # 将数字转换为字母（0=A, 1=B, ..., 25=Z, 26=AA, 27=AB, ...）
    col_str = ""
    n = x + 1  # 调整为1-based
    
    while n > 0:
        n -= 1
        col_str = chr(ord('A') + n % 26) + col_str
        n //= 26
    
    return f"{col_str}{y + 1}"

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
            await handle_send(bot, event, msg)
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
                    
                    await handle_send(bot, event, msg)
                    await bot.send(event, MessageSegment.image(board_image))
                    
                    # 清理房间
                    room_manager.delete_room(room_id)
    
    task = asyncio.create_task(move_timeout())
    move_timeout_tasks[room_id] = task

# 开始五子棋命令
@gomoku_start.handle()
async def gomoku_start_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """开始五子棋游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 检查用户是否已经在其他房间
    existing_room = room_manager.get_user_room(user_id)
    if existing_room:
        msg = f"您已经在房间 {existing_room} 中，请先退出当前房间再创建新房间！"
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)
    await bot.send(event, MessageSegment.image(board_image))
    
    # 启动房间超时任务
    await start_room_timeout(bot, event, room_id)

# 加入五子棋命令
@gomoku_join.handle()
async def gomoku_join_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """加入五子棋游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 检查用户是否已经在其他房间
    existing_room = room_manager.get_user_room(user_id)
    if existing_room:
        msg = f"您已经在房间 {existing_room} 中，请先退出当前房间再加入新房间！"
        await handle_send(bot, event, msg)
        return
    
    if not arg:
        msg = "请指定要加入的房间号！例如：加入五子棋 房间001"
        await handle_send(bot, event, msg)
        return
    
    room_id = arg
    success = room_manager.join_room(room_id, user_id)
    
    if not success:
        if room_manager.get_user_room(user_id):
            msg = "您已经在其他房间中，无法加入新房间！"
        else:
            msg = f"加入房间 {room_id} 失败！房间可能不存在或已满。"
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)
    await bot.send(event, MessageSegment.image(board_image))
    
    # 启动落子超时任务
    await start_move_timeout(bot, event, room_id)

# 落子命令
@gomoku_move.handle()
async def gomoku_move_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """落子操作"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    if not arg:
        msg = "请指定落子位置！例如：落子 A1 或 落子 B15"
        await handle_send(bot, event, msg)
        return
    
    # 查找用户所在的房间
    user_room = room_manager.get_user_room(user_id)
    
    if user_room is None:
        msg = "您当前没有参与任何五子棋游戏！"
        await handle_send(bot, event, msg)
        return
    
    game = room_manager.get_room(user_room)
    
    if game.status != "playing":
        msg = "游戏尚未开始或已经结束！"
        await handle_send(bot, event, msg)
        return
    
    if game.current_player != user_id:
        msg = "现在不是您的回合！请等待对方落子。"
        await handle_send(bot, event, msg)
        return
    
    # 解析坐标
    position = coordinate_to_position(arg)
    if position is None:
        msg = f"坐标 {arg} 无效！请使用类似 A1、B15 的格式。"
        await handle_send(bot, event, msg)
        return
    
    x, y = position
    
    # 检查位置是否可用
    if game.board[y][x] != 0:
        msg = f"位置 {arg} 已经有棋子了！请选择其他位置。"
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)
    await bot.send(event, MessageSegment.image(board_image))
    
    # 如果游戏结束，清理房间
    if game.status == "finished":
        room_manager.delete_room(user_room)

# 认输命令
@gomoku_surrender.handle()
async def gomoku_surrender_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """认输操作"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    
    # 查找用户所在的房间
    user_room = room_manager.get_user_room(user_id)
    
    if user_room is None:
        msg = "您当前没有参与任何五子棋游戏！"
        await handle_send(bot, event, msg)
        return
    
    game = room_manager.get_room(user_room)
    
    if game.status != "playing":
        msg = "游戏尚未开始或已经结束！"
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)
    await bot.send(event, MessageSegment.image(board_image))
    
    # 清理房间
    room_manager.delete_room(user_room)

# 棋局信息命令
@gomoku_info.handle()
async def gomoku_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看棋局信息"""
    arg = args.extract_plain_text().strip()
    
    if arg:
        # 查看指定房间
        room_id = arg
        game = room_manager.get_room(room_id)
        
        if game is None:
            msg = f"房间 {room_id} 不存在！"
            await handle_send(bot, event, msg)
            return
    else:
        # 查看自己参与的房间
        isUser, user_info, msg = check_user(event)
        if not isUser:
            await handle_send(bot, event, msg)
            return
        
        user_id = user_info['user_id']
        
        user_room = room_manager.get_user_room(user_id)
        
        if user_room is None:
            msg = "您当前没有参与任何五子棋游戏！"
            await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)
    await bot.send(event, MessageSegment.image(board_image))

# 退出五子棋命令
@gomoku_quit.handle()
async def gomoku_quit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """退出五子棋游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    
    # 退出房间
    success, result = room_manager.quit_room(user_id)
    
    if not success:
        await handle_send(bot, event, result)
        return
    
    if result.startswith("quit_success"):
        _, room_id, other_player_name = result.split("|")
        
        # 取消可能的超时任务
        if room_id in room_timeout_tasks:
            room_timeout_tasks[room_id].cancel()
            del room_timeout_tasks[room_id]
        if room_id in move_timeout_tasks:
            move_timeout_tasks[room_id].cancel()
            del move_timeout_tasks[room_id]
        
        msg = f"您已成功退出五子棋房间 {room_id}！"
        
        # 如果有对方玩家，通知对方
        if other_player_name != "对方":
            try:
                other_player_id = None
                game_before_quit = None
                # 这里需要从保存的文件中读取房间信息来获取对方ID
                room_file = GOMOKU_ROOMS_PATH / f"{room_id}.json"
                if room_file.exists():
                    with open(room_file, 'r', encoding='utf-8') as f:
                        game_data = json.load(f)
                        if user_id == game_data["player_black"]:
                            other_player_id = game_data["player_white"]
                        else:
                            other_player_id = game_data["player_black"]
                
                if other_player_id:
                    notify_msg = f"您的对手 {user_info['user_name']} 已退出五子棋房间 {room_id}，房间已关闭！"
                    await handle_send(bot, event, notify_msg)
            except Exception as e:
                print(f"通知对手失败: {e}")
    
    await handle_send(bot, event, msg)

@gomoku_help.handle()
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
    
    await handle_send(bot, event, help_msg)

# 十点半数据路径
HALF_TEN_DATA_PATH = Path(__file__).parent / "games" / "half_ten"
HALF_TEN_ROOMS_PATH = HALF_TEN_DATA_PATH / "rooms"

# 创建必要的目录
HALF_TEN_ROOMS_PATH.mkdir(parents=True, exist_ok=True)

# 命令注册
half_ten_start = on_command("开始十点半", priority=10, block=True)
half_ten_join = on_command("加入十点半", priority=10, block=True)
half_ten_close = on_command("结算十点半", priority=10, block=True)
half_ten_quit = on_command("退出十点半", priority=10, block=True)
half_ten_info = on_command("十点半信息", priority=10, block=True)
half_ten_help = on_command("十点半帮助", priority=10, block=True)

# 游戏配置
MIN_PLAYERS = 2      # 最少玩家数
MAX_PLAYERS = 10     # 最多玩家数
CARDS_PER_PLAYER = 3 # 每人发牌数
HALF_TIMEOUT = 180   # 房间等待超时时间（秒）

# 扑克牌配置
CARD_SUITS = ["♠", "♥", "♦", "♣"]
CARD_VALUES = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
CARD_POINTS = {
    "A": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9, "10": 10,
    "J": 0.5, "Q": 0.5, "K": 0.5
}

# 用户状态跟踪
user_half_status = {}  # 记录用户当前所在的房间 {user_id: room_id}
half_timeout_tasks = {}  # 房间超时任务 {room_id: task}

class HalfTenGame:
    def __init__(self, room_id: str, creator_id: str):
        self.room_id = room_id
        self.creator_id = creator_id
        self.players = [creator_id]  # 玩家列表，创建者为第一个
        self.status = "waiting"  # waiting, playing, finished, closed
        self.cards = {}  # 玩家手牌 {user_id: [card1, card2, card3]}
        self.points = {}  # 玩家点数 {user_id: point}
        self.rankings = []  # 排名结果 [user_id1, user_id2, ...]
        self.create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.start_time = None
        self.winner = None
        self.close_reason = None  # 关闭原因
        
    def to_dict(self):
        return {
            "room_id": self.room_id,
            "creator_id": self.creator_id,
            "players": self.players,
            "status": self.status,
            "cards": self.cards,
            "points": self.points,
            "rankings": self.rankings,
            "create_time": self.create_time,
            "start_time": self.start_time,
            "winner": self.winner,
            "close_reason": self.close_reason
        }
    
    @classmethod
    def from_dict(cls, data):
        game = cls(data["room_id"], data["creator_id"])
        game.players = data["players"]
        game.status = data["status"]
        game.cards = data["cards"]
        game.points = data["points"]
        game.rankings = data["rankings"]
        game.create_time = data["create_time"]
        game.start_time = data.get("start_time")
        game.winner = data.get("winner")
        game.close_reason = data.get("close_reason")
        return game

    def add_player(self, user_id: str) -> bool:
        """添加玩家"""
        if user_id in self.players:
            return False
        if len(self.players) >= MAX_PLAYERS:
            return False
        if self.status != "waiting":
            return False
        self.players.append(user_id)
        return True

    def remove_player(self, user_id: str) -> bool:
        """移除玩家"""
        if user_id in self.players:
            self.players.remove(user_id)
            
            # 如果房主退出，需要指定新房主
            if user_id == self.creator_id and self.players:
                self.creator_id = self.players[0]
            
            return True
        return False

    def deal_cards(self):
        """发牌"""
        # 生成一副牌（没有大小王）
        deck = []
        for suit in CARD_SUITS:
            for value in CARD_VALUES:
                deck.append(f"{suit}{value}")
        
        # 洗牌
        random.shuffle(deck)
        
        # 给每个玩家发牌
        card_index = 0
        self.cards = {}
        
        for player in self.players:
            player_cards = []
            for _ in range(CARDS_PER_PLAYER):
                if card_index < len(deck):
                    player_cards.append(deck[card_index])
                    card_index += 1
            self.cards[player] = player_cards
        
        # 计算每个玩家的点数
        self.points = {}
        for player, player_cards in self.cards.items():
            total_points = 0
            for card in player_cards:
                # 提取牌面值（去掉花色）
                value = card[1:]  # 去掉第一个字符（花色）
                total_points += CARD_POINTS[value]
            
            # 取个位数，但如果是10.5则保留
            if total_points == 10.5:
                self.points[player] = 10.5
            else:
                self.points[player] = total_points % 10
        
        # 计算排名（点数大的在前，相同点数按加入顺序）
        def get_sort_key(player):
            point = self.points[player]
            # 10.5排在最前面
            if point == 10.5:
                return (2, 0)  # 第一优先级
            else:
                return (1, point, -self.players.index(player))  # 第二优先级：点数+加入顺序
        
        self.rankings = sorted(self.players, key=get_sort_key, reverse=True)
        self.winner = self.rankings[0] if self.players else None

    def close_room(self, reason: str):
        """关闭房间"""
        self.status = "closed"
        self.close_reason = reason

# 房间管理
class HalfTenRoomManager:
    def __init__(self):
        self.rooms = {}
        self.load_rooms()
    
    def load_rooms(self):
        """加载所有房间数据"""
        for room_file in HALF_TEN_ROOMS_PATH.glob("*.json"):
            try:
                with open(room_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    room_id = room_file.stem
                    self.rooms[room_id] = HalfTenGame.from_dict(data)
            except Exception as e:
                print(f"加载房间 {room_file} 失败: {e}")
    
    def save_room(self, room_id: str):
        """保存房间数据"""
        if room_id in self.rooms:
            room_file = HALF_TEN_ROOMS_PATH / f"{room_id}.json"
            with open(room_file, 'w', encoding='utf-8') as f:
                json.dump(self.rooms[room_id].to_dict(), f, ensure_ascii=False, indent=2)
    
    def create_room(self, room_id: str, creator_id: str) -> HalfTenGame:
        """创建新房间"""
        if room_id in self.rooms:
            return None
        
        # 检查创建者是否已经在其他房间
        for existing_room_id, existing_game in self.rooms.items():
            if creator_id in existing_game.players and existing_game.status == "waiting":
                return None
        
        game = HalfTenGame(room_id, creator_id)
        self.rooms[room_id] = game
        self.save_room(room_id)
        return game
    
    def join_room(self, room_id: str, player_id: str) -> bool:
        """加入房间"""
        if room_id not in self.rooms:
            return False
        
        game = self.rooms[room_id]
        
        # 检查加入者是否已经在其他房间
        for existing_room_id, existing_game in self.rooms.items():
            if player_id in existing_game.players and existing_game.status == "waiting":
                return False
        
        if game.status != "waiting":
            return False
        
        success = game.add_player(player_id)
        if success:
            self.save_room(room_id)
            
            # 检查是否达到最大人数，自动开始游戏
            if len(game.players) >= MAX_PLAYERS:
                self.start_game(room_id)
            
        return success
    
    def start_game(self, room_id: str) -> bool:
        """开始游戏"""
        if room_id not in self.rooms:
            return False
        
        game = self.rooms[room_id]
        
        if game.status != "waiting":
            return False
        
        # 检查人数是否足够
        if len(game.players) < MIN_PLAYERS:
            return False
        
        game.status = "playing"
        game.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        game.deal_cards()
        game.status = "finished"  # 十点半是即时游戏，发完牌就结束
        self.save_room(room_id)
        return True
    
    def close_room_manually(self, room_id: str, user_id: str) -> tuple:
        """手动结算房间"""
        if room_id not in self.rooms:
            return False, "房间不存在"
        
        game = self.rooms[room_id]
        
        # 检查是否是房主
        if game.creator_id != user_id:
            return False, "只有房主可以结算游戏"
        
        if game.status != "waiting":
            return False, "游戏已经结束或正在进行中"
        
        # 检查人数是否足够
        if len(game.players) < MIN_PLAYERS:
            # 人数不足，关闭房间
            game.close_room(f"人数不足{MIN_PLAYERS}人，房间关闭")
            self.save_room(room_id)
            return True, "close"
        
        # 人数足够，开始游戏
        success = self.start_game(room_id)
        if success:
            return True, "start"
        else:
            return False, "游戏开始失败"
    
    def quit_room(self, user_id: str) -> tuple:
        """玩家退出房间"""
        room_id = self.get_user_room(user_id)
        if not room_id:
            return False, "您当前没有参与任何十点半游戏"
        
        game = self.rooms[room_id]
        
        if game.status != "waiting":
            return False, "游戏已开始，无法退出"
        
        # 移除玩家
        game.remove_player(user_id)
        
        # 如果房间没有玩家了，关闭房间
        if not game.players:
            self.delete_room(room_id)
            return True, "quit_and_close"
        
        # 如果房主退出且还有玩家，指定新房主
        new_creator_info = sql_message.get_user_info_with_id(game.creator_id)
        new_creator_name = new_creator_info['user_name'] if new_creator_info else "未知玩家"
        
        self.save_room(room_id)
        return True, f"quit_success|{room_id}|{new_creator_name}"
    
    def get_room(self, room_id: str) -> HalfTenGame:
        """获取房间"""
        return self.rooms.get(room_id)
    
    def delete_room(self, room_id: str):
        """删除房间"""
        if room_id in self.rooms:
            # 清理用户状态
            game = self.rooms[room_id]
            for player in game.players:
                if player in user_half_status:
                    del user_half_status[player]
            
            # 删除文件
            room_file = HALF_TEN_ROOMS_PATH / f"{room_id}.json"
            if room_file.exists():
                room_file.unlink()
            del self.rooms[room_id]
    
    def get_user_room(self, user_id: str) -> str:
        """获取用户所在的房间ID"""
        for room_id, game in self.rooms.items():
            if user_id in game.players:
                return room_id
        return None

# 全局房间管理器
half_manager = HalfTenRoomManager()

def generate_random_half_id() -> str:
    """生成随机房间号"""
    return f"{random.randint(1000, 9999)}"

def create_game_text(game: HalfTenGame) -> str:
    """创建游戏结果文本"""
    result_text = f"🎮 十点半游戏结果 - 房间 {game.room_id} 🎮\n\n"
    
    for rank, player_id in enumerate(game.rankings, 1):
        player_info = sql_message.get_user_info_with_id(player_id)
        player_name = player_info['user_name'] if player_info else f"玩家{player_id}"
        
        # 获取玩家手牌和点数
        player_cards = game.cards.get(player_id, [])
        point = game.points.get(player_id, 0)
        
        # 排名标识
        if rank == 1:
            rank_text = "🥇 冠军"
        elif rank == 2:
            rank_text = "🥈 亚军"
        elif rank == 3:
            rank_text = "🥉 季军"
        else:
            rank_text = f"第{rank}名"
        
        # 点数显示
        point_text = f"{point}点"
        if point == 10.5:
            point_text = "10.5点 ✨"
        
        result_text += f"{rank_text}：{player_name}\n"
        result_text += f"   手牌：{' '.join(player_cards)}\n"
        result_text += f"   点数：{point_text}\n\n"
    
    return result_text

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
                
                await handle_send(bot, event, msg)
                
                # 清理房间
                half_manager.delete_room(room_id)
            else:
                # 人数不足，关闭房间
                creator_info = sql_message.get_user_info_with_id(game.creator_id)
                msg = f"十点半房间 {room_id} 已超时（{HALF_TIMEOUT}秒后人数不足{MIN_PLAYERS}人），房间已自动关闭！"
                game.close_room("超时人数不足自动关闭")
                half_manager.save_room(room_id)
                half_manager.delete_room(room_id)
                await handle_send(bot, event, msg)
    
    task = asyncio.create_task(room_timeout())
    half_timeout_tasks[room_id] = task

# 开始十点半命令
@half_ten_start.handle()
async def half_ten_start_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """开始十点半游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 检查用户是否已经在其他房间
    existing_room = half_manager.get_user_room(user_id)
    if existing_room:
        game = half_manager.get_room(existing_room)
        if game and game.status == "waiting":
            msg = f"您已经在房间 {existing_room} 中，请先退出当前房间再创建新房间！"
            await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)
    
    # 启动房间超时任务
    await start_half_timeout(bot, event, room_id)

# 加入十点半命令
@half_ten_join.handle()
async def half_ten_join_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """加入十点半游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 检查用户是否已经在其他房间
    existing_room = half_manager.get_user_room(user_id)
    if existing_room:
        game = half_manager.get_room(existing_room)
        if game and game.status == "waiting":
            msg = f"您已经在房间 {existing_room} 中，请先退出当前房间再加入新房间！"
            await handle_send(bot, event, msg)
            return
    
    if not arg:
        msg = "请指定要加入的房间号！例如：加入十点半 房间001"
        await handle_send(bot, event, msg)
        return
    
    room_id = arg
    success = half_manager.join_room(room_id, user_id)
    
    if not success:
        if half_manager.get_user_room(user_id):
            msg = "您已经在其他房间中，无法加入新房间！"
        else:
            msg = f"加入房间 {room_id} 失败！房间可能不存在或已满。"
        await handle_send(bot, event, msg)
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
        
        await handle_send(bot, event, msg)
        
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
        
        await handle_send(bot, event, msg)
        
        # 重启超时任务（因为人数变化）
        await start_half_timeout(bot, event, room_id)

# 结算十点半命令
@half_ten_close.handle()
async def half_ten_close_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """结算十点半游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    
    # 查找用户所在的房间
    user_room = half_manager.get_user_room(user_id)
    
    if user_room is None:
        msg = "您当前没有参与任何十点半游戏！"
        await handle_send(bot, event, msg)
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
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)
    
    # 清理房间
    half_manager.delete_room(user_room)

# 退出十点半命令
@half_ten_quit.handle()
async def half_ten_quit_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """退出十点半游戏"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)

# 十点半信息命令
@half_ten_info.handle()
async def half_ten_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看十点半游戏信息"""
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        return
    
    user_id = user_info['user_id']
    arg = args.extract_plain_text().strip()
    
    # 如果没有指定房间号，查看自己所在的房间
    if not arg:
        room_id = half_manager.get_user_room(user_id)
        if not room_id:
            msg = "您当前没有参与任何十点半游戏！请指定房间号或先加入一个房间。"
            await handle_send(bot, event, msg)
            return
    else:
        room_id = arg
    
    game = half_manager.get_room(room_id)
    if not game:
        msg = f"房间 {room_id} 不存在！"
        await handle_send(bot, event, msg)
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
    
    await handle_send(bot, event, msg)

# 十点半帮助命令
@half_ten_help.handle()
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
