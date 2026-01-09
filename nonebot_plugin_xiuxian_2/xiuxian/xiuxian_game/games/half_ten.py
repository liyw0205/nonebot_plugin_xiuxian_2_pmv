import random
import json
import os
import asyncio
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO

from ...xiuxian_utils.utils import check_user, get_msg_pic, handle_send
from ...xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from datetime import datetime, timedelta

sql_message = XiuxianDateManage()

# 十点半数据路径
HALF_TEN_DATA_PATH = Path(__file__).parent / "half_ten"
HALF_TEN_ROOMS_PATH = HALF_TEN_DATA_PATH / "rooms"

# 创建必要的目录
HALF_TEN_ROOMS_PATH.mkdir(parents=True, exist_ok=True)

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
