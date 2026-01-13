import random
import json
import os
import asyncio
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import math

from ...xiuxian_utils.utils import check_user, get_msg_pic, handle_send
from ...xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from datetime import datetime, timedelta

sql_message = XiuxianDateManage()

# 五子棋数据路径
GOMOKU_DATA_PATH = Path(__file__).parent / "gomoku"
GOMOKU_BOARDS_PATH = GOMOKU_DATA_PATH / "boards"
GOMOKU_ROOMS_PATH = GOMOKU_DATA_PATH / "rooms"

# 创建必要的目录
GOMOKU_BOARDS_PATH.mkdir(parents=True, exist_ok=True)
GOMOKU_ROOMS_PATH.mkdir(parents=True, exist_ok=True)

# 棋盘配置
BOARD_SIZE = 15  # 15x15 棋盘
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
        
        # 删除房间
        self.delete_room(room_id)
        
        return True, f"quit_success|{room_id}"

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

MAX_DEPTH = 5  # 增加搜索深度，根据性能需求调整，可以尝试5或更高

# 定义棋型分数
SCORES = {
    'FIVE': 100000,          # 五连，必胜
    'OPEN_FOUR': 10000,      # 活四，必须堵/抢
    'FOUR': 1000,            # 冲四（被堵的四），也要防守
    'CROSS_POINT': 500,      # 两个活二汇聚点，未来可能双活三，必争
    'OPEN_THREE': 1000,      # 活三，进攻好点
    'THREE': 100,            # 眠三
    'OPEN_TWO': 50,          # 活二
    'TWO': 10,               # 眠二
    'ONE': 1,                # 单子
}

# 四个主要方向：横、竖、主对角线、副对角线
DIRECTIONS = [
    (1, 0),   # 水平 →
    (0, 1),   # 垂直 ↓
    (1, 1),   # 主对角线 ↘
    (1, -1),  # 副对角线 ↙
]

def find_best_move_enhanced(game, ai_player):
    board = game.board
    opponent = 2 if ai_player == 1 else 1

    # 第一步：检查是否能直接获胜（五连）
    for y in range(len(board)):
        for x in range(len(board)):
            if board[y][x] == 0:
                board[y][x] = ai_player
                if check_win(board, x, y, ai_player):
                    board[y][x] = 0
                    return (x, y)
                board[y][x] = 0

    # 第二步：检查是否必须防守（对手下一步五连）
    for y in range(len(board)):
        for x in range(len(board)):
            if board[y][x] == 0:
                board[y][x] = opponent
                if check_win(board, x, y, opponent):
                    board[y][x] = 0
                    return (x, y)
                board[y][x] = 0

    # 第三步：基于棋型评分选择最佳落子点
    best_score = -float('inf')
    best_move = None

    candidates = generate_candidate_moves(board, ai_player)

    for x, y in candidates:
        if board[y][x] != 0:
            continue

        score = evaluate_position(board, x, y, ai_player)
        if score > best_score:
            best_score = score
            best_move = (x, y)

    # 如果没有找到好的点，随便选一个候选
    if best_move is None and candidates:
        best_move = random.choice(candidates)
    # 如果棋盘全空，选中心
    elif best_move is None:
        center = len(board) // 2
        best_move = (center, center)

    return best_move

def minimax_enhanced(game, depth, alpha, beta, is_maximizing, ai_player, opponent):
    if depth == 0:
        return evaluate_board_enhanced(game.board, ai_player, opponent)

    # 检查是否有即时的胜负
    winner = check_board_win_enhanced(game.board)
    if winner == ai_player:
        return SCORES['FIVE'] + depth  # 越早赢越好
    elif winner == opponent:
        return -SCORES['FIVE'] - depth
    elif is_board_full_enhanced(game.board):
        return 0  # 平局

    if is_maximizing:
        max_eval = -math.inf
        candidates = generate_candidate_moves_enhanced(game.board, ai_player)
        for x, y in candidates:
            if game.board[y][x] != 0:
                continue
            game.board[y][x] = ai_player
            eval_score = minimax_enhanced(game, depth - 1, alpha, beta, False, ai_player, opponent)
            game.board[y][x] = 0
            max_eval = max(max_eval, eval_score)
            alpha = max(alpha, eval_score)
            if beta <= alpha:
                break  # Alpha-Beta 剪枝
        return max_eval
    else:
        min_eval = math.inf
        candidates = generate_candidate_moves_enhanced(game.board, opponent)
        for x, y in candidates:
            if game.board[y][x] != 0:
                continue
            game.board[y][x] = opponent
            eval_score = minimax_enhanced(game, depth - 1, alpha, beta, True, ai_player, opponent)
            game.board[y][x] = 0
            min_eval = min(min_eval, eval_score)
            beta = min(beta, eval_score)
            if beta <= alpha:
                break  # Alpha-Beta 剪枝
        return min_eval

def evaluate_board_enhanced(board, ai_player, opponent):
    ai_score = evaluate_all_lines_enhanced(board, ai_player)
    opponent_score = evaluate_all_lines_enhanced(board, opponent)
    return ai_score - opponent_score

def evaluate_all_lines_enhanced(board, player):
    total_score = 0
    directions = [(1, 0), (0, 1), (1, 1), (1, -1)]  # 横、竖、主对角、副对角
    for y in range(BOARD_SIZE):
        for x in range(BOARD_SIZE):
            if board[y][x] == player:
                for dx, dy in directions:
                    score = evaluate_line_enhanced(board, x, y, dx, dy, player)
                    total_score += score
    return total_score

def evaluate_line_enhanced(board, x, y, dx, dy, player):
    score = 0
    count = 1  # 包括当前位置
    blocked_left = False
    blocked_right = False

    # 向左/上延伸
    nx, ny = x - dx, y - dy
    while 0 <= nx < BOARD_SIZE and 0 <= ny < BOARD_SIZE:
        if board[ny][nx] == player:
            count += 1
            nx -= dx
            ny -= dy
        else:
            if board[ny][nx] != 0:
                blocked_left = True
            break

    # 向右/下延伸
    nx, ny = x + dx, y + dy
    while 0 <= nx < BOARD_SIZE and 0 <= ny < BOARD_SIZE:
        if board[ny][nx] == player:
            count += 1
            nx += dx
            ny += dy
        else:
            if board[ny][nx] != 0:
                blocked_right = True
            break

    # 根据连子数和阻挡情况评分
    if count >= 5:
        score += SCORES['FIVE']
    elif count == 4:
        if not blocked_left and not blocked_right:
            score += SCORES['OPEN_FOUR']
        elif not blocked_left or not blocked_right:
            score += SCORES['FOUR']
    elif count == 3:
        if not blocked_left and not blocked_right:
            score += SCORES['OPEN_THREE']
        elif not blocked_left or not blocked_right:
            score += SCORES['THREE']
    elif count == 2:
        if not blocked_left and not blocked_right:
            score += SCORES['OPEN_TWO']
        else:
            score += SCORES['TWO']
    return score

def check_board_win_enhanced(board):
    for y in range(BOARD_SIZE):
        for x in range(BOARD_SIZE):
            if board[y][x] != 0:
                if check_win(board, x, y, board[y][x]):
                    return board[y][x]
    return None

def is_board_full_enhanced(board):
    return all(cell != 0 for row in board for cell in row)

def generate_candidate_moves(board, player):
    n = len(board)
    candidates = []

    # 如果棋盘为空，返回中心点
    if all(board[i][j] == 0 for i in range(n) for j in range(n)):
        center = n // 2
        candidates.append((center, center))
        return candidates

    # 只考虑已有棋子周围 2 格范围内的空位
    occupied = [(i, j) for i in range(n) for j in range(n) if board[i][j] != 0]
    for (x, y) in occupied:
        for dx in range(-2, 3):
            for dy in range(-2, 3):
                nx, ny = x + dx, y + dy
                if 0 <= nx < n and 0 <= ny < n and board[ny][nx] == 0:
                    if (nx, ny) not in candidates:
                        candidates.append((nx, ny))
    return candidates

def evaluate_position(board, x, y, player):
    if board[y][x] != 0:
        return 0  # 已有棋子，不可落子

    total_score = 0
    opponent = 2 if player == 1 else 1

    for dx, dy in DIRECTIONS:
        # 检测当前玩家的棋型
        line_info = detect_line_pattern(board, x, y, dx, dy, player)
        if line_info and line_info['pattern_type'] in SCORES:
            total_score += SCORES.get(line_info['pattern_type'], 0)

        # 检测对手的棋型（用于防守）
        opp_line_info = detect_line_pattern(board, x, y, dx, dy, opponent)
        if opp_line_info and opp_line_info['pattern_type'] in SCORES:
            # 防守权重略高
            total_score += SCORES.get(opp_line_info['pattern_type'], 0) * 1.2

    # 🔍 检测当前空位是否为交叉点（两个活二汇聚，未来可能双活三）
    if detect_cross_point(board, x, y, player):
        total_score += SCORES['CROSS_POINT']  # 交叉点高分

    return total_score

def detect_cross_point(board, x, y, player):
    """
    检测 (x, y) 是否为交叉点：即两个不同方向上存在活二（或活二+眠二），
    且这些方向下一步都能形成活三，但当前未形成活三。
    返回：True/False 表示是否为交叉点
    """
    if board[y][x] != 0:
        return False  # 已有棋子，不是空位

    cross_count = 0
    directions = [(1, 0), (0, 1), (1, 1), (1, -1)]  # 横、竖、主对角、副对角

    for dx, dy in directions:
        line_info = detect_line_pattern(board, x, y, dx, dy, player)
        if line_info and line_info['pattern_type'] == 'OPEN_TWO':
            cross_count += 1
        elif line_info and line_info['pattern_type'] == 'TWO':  # 如果希望更宽松，也可以把眠二算上
            cross_count += 1

        # 只要两个方向有活二或关键潜在连子，就可能是交叉点
        if cross_count >= 2:
            return True

    return False

def detect_line_pattern(board, x, y, dx, dy, player):
    """
    从(x,y)出发，沿(dx,dy)方向，检测连续的player棋子，返回棋型信息。
    返回值是一个字典，包含：
    - count: 连子数
    - blocked_left: 左端是否被阻挡（非空/边界）
    - blocked_right: 右端是否被阻挡
    - pattern_type: 棋型类型，如 'FIVE', 'OPEN_FOUR', 'OPEN_THREE'...
    """
    n = len(board)
    if x < 0 or y < 0 or x >= n or y >= n:
        return None

    # 正反两个方向计数
    left, right = 0, 0
    blocked_left, blocked_right = False, False

    # 向左探索
    i = 1
    while x - i * dx >= 0 and y - i * dy >= 0 and y - i * dy < len(board) and x - i * dx < len(board):
        if board[y - i * dy][x - i * dx] == player:
            left += 1
        elif board[y - i * dy][x - i * dx] == 0:
            break
        else:
            blocked_left = True
            break
        i += 1

    # 向右探索
    i = 1
    while x + i * dx >= 0 and y + i * dy >= 0 and y + i * dy < len(board) and x + i * dx < len(board):
        if board[y + i * dy][x + i * dx] == player:
            right += 1
        elif board[y + i * dy][x + i * dx] == 0:
            break
        else:
            blocked_right = True
            break
        i += 1

    count = 1 + left + right  # 当前子 + 左边连续 + 右边连续

    # 判断棋型
    is_open = not blocked_left and not blocked_right
    is_one_side_open = not (blocked_left and blocked_right)

    if count >= 5:
        return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
                'pattern_type': 'FIVE'}
    elif count == 4:
        if is_open:
            return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
                    'pattern_type': 'OPEN_FOUR'}
        elif is_one_side_open:
            return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
                    'pattern_type': 'FOUR'}  # 冲四
    elif count == 3:
        if is_open:
            return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
                    'pattern_type': 'OPEN_THREE'}
        elif is_one_side_open:
            return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
                    'pattern_type': 'THREE'}  # 眠三
    elif count == 2:
        if is_open:
            return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
                    'pattern_type': 'OPEN_TWO'}
        elif is_one_side_open:
            return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
                    'pattern_type': 'TWO'}  # 眠二
    elif count == 1:
        return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
                'pattern_type': 'ONE'}

    return {'count': count, 'blocked_left': blocked_left, 'blocked_right': blocked_right,
            'pattern_type': 'UNKNOWN'}
