import random
import json
import asyncio
from pathlib import Path
from io import BytesIO
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont

from nonebot import on_command
from nonebot.params import CommandArg

from ..command import *

# =========================
# 数据目录（娱乐独立）
# =========================
GOMOKU_ROOMS_PATH = Path(__file__).resolve().parent / "data" / "rooms"
GOMOKU_ROOMS_PATH.mkdir(parents=True, exist_ok=True)

# =========================
# 棋盘配置
# =========================
BOARD_SIZE = 15
CELL_SIZE = 36
MARGIN = 56

BOARD_COLOR = (224, 196, 147)
LINE_COLOR = (35, 35, 35)
BLACK_STONE = (20, 20, 20)
WHITE_STONE = (245, 245, 245)
STONE_BORDER = (120, 120, 120)
COORD_COLOR = (20, 20, 20)
LAST_MOVE_COLOR = (230, 30, 30)

ROOM_TIMEOUT = 180   # 等待对手
MOVE_TIMEOUT = 120   # 每步超时

# 用户状态（一个用户只能在一个房间）
user_room_status: dict[str, str] = {}
room_timeout_tasks: dict[str, asyncio.Task] = {}
move_timeout_tasks: dict[str, asyncio.Task] = {}


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _name_from_event(event, user_id: str):
    sender = getattr(event, "sender", None)
    if sender:
        return sender.card or sender.nickname or str(user_id)
    return str(user_id)


# =========================
# 房间模型
# =========================
class GomokuGame:
    def __init__(self, room_id: str, creator_id: str, creator_name: str):
        self.room_id = room_id
        self.creator_id = creator_id
        self.player_black = creator_id
        self.player_white = None  # 对战时填 user_id，单人时填 "__AI__"

        self.player_names = {creator_id: creator_name, "__AI__": "AI"}
        self.current_player = creator_id

        self.board = [[0 for _ in range(BOARD_SIZE)] for _ in range(BOARD_SIZE)]  # 0空1黑2白
        self.moves = []  # [(x,y),...]

        self.status = "waiting"  # waiting/playing/finished
        self.winner = None

        self.create_time = _now_str()
        self.last_move_time = None

    def to_dict(self):
        return {
            "room_id": self.room_id,
            "creator_id": self.creator_id,
            "player_black": self.player_black,
            "player_white": self.player_white,
            "player_names": self.player_names,
            "current_player": self.current_player,
            "board": self.board,
            "moves": self.moves,
            "status": self.status,
            "winner": self.winner,
            "create_time": self.create_time,
            "last_move_time": self.last_move_time,
        }

    @classmethod
    def from_dict(cls, data):
        g = cls(data["room_id"], data["creator_id"], data.get("player_names", {}).get(data["creator_id"], data["creator_id"]))
        g.player_black = data["player_black"]
        g.player_white = data["player_white"]
        g.player_names = data.get("player_names", {g.player_black: g.player_black, "__AI__": "AI"})
        g.current_player = data["current_player"]
        g.board = data["board"]
        g.moves = data["moves"]
        g.status = data["status"]
        g.winner = data["winner"]
        g.create_time = data["create_time"]
        g.last_move_time = data.get("last_move_time")
        return g


class GomokuRoomManager:
    def __init__(self):
        self.rooms: dict[str, GomokuGame] = {}
        self.load_rooms()

    def load_rooms(self):
        for f in GOMOKU_ROOMS_PATH.glob("*.json"):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                room_id = f.stem
                game = GomokuGame.from_dict(data)
                self.rooms[room_id] = game

                # 重建用户状态
                for uid in [game.player_black, game.player_white]:
                    if uid and uid != "__AI__":
                        user_room_status[uid] = room_id
            except Exception:
                continue

    def save_room(self, room_id: str):
        game = self.rooms.get(room_id)
        if not game:
            return
        fp = GOMOKU_ROOMS_PATH / f"{room_id}.json"
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(game.to_dict(), f, ensure_ascii=False, indent=2)

    def create_room(self, room_id: str, creator_id: str, creator_name: str):
        if room_id in self.rooms:
            return None
        if self.get_user_room(creator_id):
            return None
        game = GomokuGame(room_id, creator_id, creator_name)
        self.rooms[room_id] = game
        self.save_room(room_id)
        return game

    def join_room(self, room_id: str, user_id: str, user_name: str) -> bool:
        game = self.rooms.get(room_id)
        if not game:
            return False
        if self.get_user_room(user_id):
            return False
        if game.status != "waiting":
            return False
        if game.player_white is not None:
            return False

        game.player_white = user_id
        game.player_names[user_id] = user_name
        game.status = "playing"
        game.current_player = game.player_black
        game.last_move_time = _now_str()
        self.save_room(room_id)
        return True

    def get_room(self, room_id: str):
        return self.rooms.get(room_id)

    def get_user_room(self, user_id: str):
        for room_id, g in self.rooms.items():
            if user_id in [g.player_black, g.player_white]:
                return room_id
        return None

    def delete_room(self, room_id: str):
        g = self.rooms.get(room_id)
        if g:
            for uid in [g.player_black, g.player_white]:
                if uid and uid != "__AI__":
                    user_room_status.pop(uid, None)
        self.rooms.pop(room_id, None)

        fp = GOMOKU_ROOMS_PATH / f"{room_id}.json"
        if fp.exists():
            fp.unlink()

        # 清任务
        if room_id in room_timeout_tasks:
            room_timeout_tasks[room_id].cancel()
            room_timeout_tasks.pop(room_id, None)
        if room_id in move_timeout_tasks:
            move_timeout_tasks[room_id].cancel()
            move_timeout_tasks.pop(room_id, None)

    def quit_room(self, user_id: str):
        room_id = self.get_user_room(user_id)
        if not room_id:
            return False, "你当前没有参与五子棋。"

        g = self.rooms[room_id]
        if g.status == "playing":
            return False, "游戏进行中不能直接退出，请使用【认输】。"

        self.delete_room(room_id)
        return True, f"已退出房间 {room_id}。"


room_manager = GomokuRoomManager()


# =========================
# 坐标转换
# =========================
def coordinate_to_position(coord: str):
    coord = coord.strip().upper()
    if len(coord) < 2:
        return None
    col_str = ""
    row_str = ""
    for ch in coord:
        if ch.isalpha():
            col_str += ch
        elif ch.isdigit():
            row_str += ch
    if not col_str or not row_str:
        return None

    col = 0
    for i, c in enumerate(reversed(col_str)):
        col += (ord(c) - ord("A") + 1) * (26 ** i)
    col -= 1
    row = int(row_str) - 1

    if 0 <= col < BOARD_SIZE and 0 <= row < BOARD_SIZE:
        return col, row
    return None


def position_to_coordinate(x: int, y: int):
    n = x + 1
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord("A") + n % 26) + letters
        n //= 26
    return f"{letters}{y+1}"


# =========================
# 渲染
# =========================
def create_board_image(game: GomokuGame) -> BytesIO:
    img_w = BOARD_SIZE * CELL_SIZE + MARGIN * 2
    img_h = BOARD_SIZE * CELL_SIZE + MARGIN * 2
    img = Image.new("RGB", (img_w, img_h), BOARD_COLOR)
    draw = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("arial.ttf", 16)
    except Exception:
        font = ImageFont.load_default()

    # 网格
    for i in range(BOARD_SIZE):
        y = MARGIN + i * CELL_SIZE
        x = MARGIN + i * CELL_SIZE
        draw.line([(MARGIN, y), (img_w - MARGIN, y)], fill=LINE_COLOR, width=2)
        draw.line([(x, MARGIN), (x, img_h - MARGIN)], fill=LINE_COLOR, width=2)

    # 星位（15路）
    star = [3, 7, 11]
    for sx in star:
        for sy in star:
            cx = MARGIN + sx * CELL_SIZE
            cy = MARGIN + sy * CELL_SIZE
            draw.ellipse([cx-4, cy-4, cx+4, cy+4], fill=LINE_COLOR)

    # 棋子
    r = CELL_SIZE // 2 - 3
    for y in range(BOARD_SIZE):
        for x in range(BOARD_SIZE):
            v = game.board[y][x]
            if v == 0:
                continue
            cx = MARGIN + x * CELL_SIZE
            cy = MARGIN + y * CELL_SIZE
            if v == 1:
                draw.ellipse([cx-r, cy-r, cx+r, cy+r], fill=BLACK_STONE, outline=STONE_BORDER, width=2)
            else:
                draw.ellipse([cx-r, cy-r, cx+r, cy+r], fill=WHITE_STONE, outline=STONE_BORDER, width=2)

    # 最后一步标记
    if game.moves:
        lx, ly = game.moves[-1]
        cx = MARGIN + lx * CELL_SIZE
        cy = MARGIN + ly * CELL_SIZE
        draw.ellipse([cx-5, cy-5, cx+5, cy+5], fill=LAST_MOVE_COLOR)

    # 坐标
    for i in range(BOARD_SIZE):
        letter = position_to_coordinate(i, 0).rstrip("1")
        draw.text((MARGIN + i*CELL_SIZE - 6, MARGIN - 26), letter, fill=COORD_COLOR, font=font)
        draw.text((MARGIN + i*CELL_SIZE - 6, img_h - MARGIN + 8), letter, fill=COORD_COLOR, font=font)

        num = str(i + 1)
        draw.text((MARGIN - 30, MARGIN + i*CELL_SIZE - 8), num, fill=COORD_COLOR, font=font)
        draw.text((img_w - MARGIN + 8, MARGIN + i*CELL_SIZE - 8), num, fill=COORD_COLOR, font=font)

    out = BytesIO()
    img.save(out, format="PNG")
    out.seek(0)
    return out


# =========================
# 胜负判断
# =========================
def check_win(board, x, y, stone):
    dirs = [
        [(1, 0), (-1, 0)],
        [(0, 1), (0, -1)],
        [(1, 1), (-1, -1)],
        [(1, -1), (-1, 1)],
    ]
    for pair in dirs:
        cnt = 1
        for dx, dy in pair:
            tx, ty = x, y
            while True:
                tx += dx
                ty += dy
                if 0 <= tx < BOARD_SIZE and 0 <= ty < BOARD_SIZE and board[ty][tx] == stone:
                    cnt += 1
                else:
                    break
        if cnt >= 5:
            return True
    return False


# =========================
# AI（简版）
# =========================
def _neighbors(board, x, y, dis=2):
    for dy in range(-dis, dis+1):
        for dx in range(-dis, dis+1):
            nx, ny = x + dx, y + dy
            if 0 <= nx < BOARD_SIZE and 0 <= ny < BOARD_SIZE:
                yield nx, ny


def _candidate_moves(board):
    occupied = [(x, y) for y in range(BOARD_SIZE) for x in range(BOARD_SIZE) if board[y][x] != 0]
    if not occupied:
        c = BOARD_SIZE // 2
        return [(c, c)]
    s = set()
    for x, y in occupied:
        for nx, ny in _neighbors(board, x, y, 2):
            if board[ny][nx] == 0:
                s.add((nx, ny))
    return list(s)


def _line_score(board, x, y, stone):
    def count_dir(dx, dy):
        cnt = 0
        tx, ty = x, y
        while True:
            tx += dx
            ty += dy
            if 0 <= tx < BOARD_SIZE and 0 <= ty < BOARD_SIZE and board[ty][tx] == stone:
                cnt += 1
            else:
                break
        return cnt

    score = 0
    for dx, dy in [(1,0), (0,1), (1,1), (1,-1)]:
        c = 1 + count_dir(dx,dy) + count_dir(-dx,-dy)
        if c >= 5:
            score += 100000
        elif c == 4:
            score += 5000
        elif c == 3:
            score += 500
        elif c == 2:
            score += 50
        else:
            score += 5
    return score


def ai_best_move(game: GomokuGame):
    board = game.board
    me = 2
    opp = 1
    cand = _candidate_moves(board)

    # 1. 我方一步必胜
    for x, y in cand:
        board[y][x] = me
        if check_win(board, x, y, me):
            board[y][x] = 0
            return x, y
        board[y][x] = 0

    # 2. 堵对手一步必胜
    for x, y in cand:
        board[y][x] = opp
        if check_win(board, x, y, opp):
            board[y][x] = 0
            return x, y
        board[y][x] = 0

    # 3. 综合评分
    best = None
    best_score = -10**18
    for x, y in cand:
        board[y][x] = me
        s1 = _line_score(board, x, y, me)
        board[y][x] = 0

        board[y][x] = opp
        s2 = _line_score(board, x, y, opp)
        board[y][x] = 0

        score = s1 + int(s2 * 1.2)
        if score > best_score:
            best_score = score
            best = (x, y)

    if best is None:
        c = BOARD_SIZE // 2
        return c, c
    return best


# =========================
# 超时任务
# =========================
async def start_room_timeout(bot, event, room_id: str):
    if room_id in room_timeout_tasks:
        room_timeout_tasks[room_id].cancel()

    async def _task():
        await asyncio.sleep(ROOM_TIMEOUT)
        g = room_manager.get_room(room_id)
        if not g or g.status != "waiting":
            return
        room_manager.delete_room(room_id)
        await handle_send(bot, event, f"房间 {room_id} 超时无人加入，已自动关闭。")

    room_timeout_tasks[room_id] = asyncio.create_task(_task())


async def start_move_timeout(bot, event, room_id: str):
    if room_id in move_timeout_tasks:
        move_timeout_tasks[room_id].cancel()

    async def _task():
        await asyncio.sleep(MOVE_TIMEOUT)
        g = room_manager.get_room(room_id)
        if not g or g.status != "playing" or not g.last_move_time:
            return

        last = datetime.strptime(g.last_move_time, "%Y-%m-%d %H:%M:%S")
        if (datetime.now() - last).total_seconds() < MOVE_TIMEOUT:
            return

        loser = g.current_player
        winner = g.player_white if loser == g.player_black else g.player_black
        loser_name = g.player_names.get(loser, loser)
        winner_name = g.player_names.get(winner, winner)

        img = create_board_image(g)
        await handle_pic_msg_send(bot, event, img, f"{loser_name} 超时未落子，判负！\n🎉 {winner_name} 获胜。")
        room_manager.delete_room(room_id)

    move_timeout_tasks[room_id] = asyncio.create_task(_task())


# =========================
# 命令注册
# =========================
gomoku_help = on_command("五子棋帮助", priority=5, block=True)
gomoku_start = on_command("开始五子棋", priority=5, block=True)
gomoku_single = on_command("开始单人五子棋", priority=5, block=True)
gomoku_join = on_command("加入五子棋", priority=5, block=True)
gomoku_move = on_command("落子", priority=5, block=True)
gomoku_surrender = on_command("认输", priority=5, block=True)
gomoku_info = on_command("棋局信息", priority=5, block=True)
gomoku_quit = on_command("退出五子棋", priority=5, block=True)


def _random_room_id():
    return str(random.randint(1000, 9999))


@gomoku_start.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    user_name = _name_from_event(event, user_id)
    arg = args.extract_plain_text().strip()

    if room_manager.get_user_room(user_id):
        await handle_send(bot, event, "你已在其它棋局中，请先退出。")
        return

    room_id = arg if arg else _random_room_id()
    while room_manager.get_room(room_id):
        room_id = _random_room_id()

    g = room_manager.create_room(room_id, user_id, user_name)
    if not g:
        await handle_send(bot, event, "创建失败（可能房间号重复或你已在其它房间）。")
        return

    user_room_status[user_id] = room_id
    await handle_pic_msg_send(
        bot, event, create_board_image(g),
        f"房间 {room_id} 创建成功！\n你是黑棋，等待对手加入。\n命令：加入五子棋 {room_id}"
    )
    await start_room_timeout(bot, event, room_id)


@gomoku_single.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    user_name = _name_from_event(event, user_id)
    arg = args.extract_plain_text().strip()

    if room_manager.get_user_room(user_id):
        await handle_send(bot, event, "你已在其它棋局中，请先退出。")
        return

    room_id = arg if arg else f"S{_random_room_id()}"
    while room_manager.get_room(room_id):
        room_id = f"S{_random_room_id()}"

    g = room_manager.create_room(room_id, user_id, user_name)
    if not g:
        await handle_send(bot, event, "创建失败。")
        return

    g.player_white = "__AI__"
    g.status = "playing"
    g.current_player = g.player_black
    g.last_move_time = _now_str()
    room_manager.save_room(room_id)

    user_room_status[user_id] = room_id
    await handle_pic_msg_send(
        bot, event, create_board_image(g),
        f"单人五子棋已开始（房间 {room_id}）n黑先AI执白。\n命令：落子 A1"
    )


@gomoku_join.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    user_name = _name_from_event(event, user_id)
    room_id = args.extract_plain_text().strip()

    if not room_id:
        await handle_send(bot, event, "请带房间号：加入五子棋 1234")
        return

    if room_manager.get_user_room(user_id):
        await handle_send(bot, event, "你已在其它棋局中，请先退出。")
        return

    ok = room_manager.join_room(room_id, user_id, user_name)
    if not ok:
        await handle_send(bot, event, "加入失败（房间不存在/已开始/已满）。")
        return

    user_room_status[user_id] = room_id
    g = room_manager.get_room(room_id)

    if room_id in room_timeout_tasks:
        room_timeout_tasks[room_id].cancel()
        room_timeout_tasks.pop(room_id, None)

    bname = g.player_names.get(g.player_black, g.player_black)
    wname = g.player_names.get(g.player_white, g.player_white)
    await handle_pic_msg_send(
        bot, event, create_board_image(g),
        f"加入成功！房间 {room_id}\n黑棋：{bname}\n白棋：{wname}\n游戏开始，黑棋先手。"
    )
    await start_move_timeout(bot, event, room_id)


@gomoku_move.handle(parameterless=[Cooldown(cd_time=0.8)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    ptxt = args.extract_plain_text().strip()

    room_id = room_manager.get_user_room(user_id)
    if not room_id:
        await handle_send(bot, event, "你当前不在棋局中。")
        return

    g = room_manager.get_room(room_id)
    if not g or g.status != "playing":
        await handle_send(bot, event, "当前棋局不可落子。")
        return

    pos = coordinate_to_position(ptxt)
    if not pos:
        await handle_send(bot, event, "坐标格式错误，如：落子 A1")
        return
    x, y = pos

    # 当前回合检查
    if g.current_player != user_id:
        await handle_send(bot, event, "还没轮到你落子。")
        return
    if g.board[y][x] != 0:
        await handle_send(bot, event, f"{ptxt.upper()} 位置已有棋子。")
        return

    # 玩家落子
    stone = 1 if user_id == g.player_black else 2
    g.board[y][x] = stone
    g.moves.append((x, y))
    g.last_move_time = _now_str()

    # 胜负
    if check_win(g.board, x, y, stone):
        g.status = "finished"
        g.winner = user_id
        room_manager.save_room(room_id)

        win_name = g.player_names.get(user_id, user_id)
        await handle_pic_msg_send(bot, event, create_board_image(g), f"🎉 {win_name} 五子连珠，获胜！")
        room_manager.delete_room(room_id)
        return

    # 单人模式：AI走
    if g.player_white == "__AI__":
        g.current_player = "__AI__"
        ax, ay = ai_best_move(g)
        if g.board[ay][ax] == 0:
            g.board[ay][ax] = 2
            g.moves.append((ax, ay))
            g.last_move_time = _now_str()

            if check_win(g.board, ax, ay, 2):
                g.status = "finished"
                g.winner = "__AI__"
                room_manager.save_room(room_id)
                await handle_pic_msg_send(bot, event, create_board_image(g), f"AI 落子 {position_to_coordinate(ax, ay)}，AI 获胜。")
                room_manager.delete_room(room_id)
                return

            g.current_player = g.player_black
            room_manager.save_room(room_id)
            await handle_pic_msg_send(
                bot, event, create_board_image(g),
                f"你落子 {position_to_coordinate(x, y)}\nAI 落子 {position_to_coordinate(ax, ay)}\n轮到你。"
            )
        else:
            # 理论不会到这里
            g.current_player = g.player_black
            room_manager.save_room(room_id)
            await handle_pic_msg_send(bot, event, create_board_image(g), "AI无有效落子，轮到你。")
        return

    # 双人模式切换回合
    g.current_player = g.player_white if user_id == g.player_black else g.player_black
    next_name = g.player_names.get(g.current_player, g.current_player)
    room_manager.save_room(room_id)
    await handle_pic_msg_send(bot, event, create_board_image(g), f"落子成功：{position_to_coordinate(x, y)}\n轮到 {next_name}")
    await start_move_timeout(bot, event, room_id)


@gomoku_surrender.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    user_id = str(event.get_user_id())
    room_id = room_manager.get_user_room(user_id)
    if not room_id:
        await handle_send(bot, event, "你当前不在棋局中。")
        return

    g = room_manager.get_room(room_id)
    if not g or g.status != "playing":
        await handle_send(bot, event, "当前棋局不可认输。")
        return

    if g.current_player != user_id:
        await handle_send(bot, event, "仅当前回合玩家可认输。")
        return

    winner = g.player_white if user_id == g.player_black else g.player_black
    loser_name = g.player_names.get(user_id, user_id)
    winner_name = g.player_names.get(winner, winner)
    await handle_pic_msg_send(bot, event, create_board_image(g), f"{loser_name} 认输。\n🎉 {winner_name} 获胜！")
    room_manager.delete_room(room_id)


@gomoku_info.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    arg = args.extract_plain_text().strip()

    if arg:
        room_id = arg
    else:
        room_id = room_manager.get_user_room(user_id)

    if not room_id:
        await handle_send(bot, event, "你当前没有棋局。")
        return

    g = room_manager.get_room(room_id)
    if not g:
        await handle_send(bot, event, f"房间 {room_id} 不存在。")
        return

    bname = g.player_names.get(g.player_black, g.player_black)
    wname = g.player_names.get(g.player_white, "等待加入") if g.player_white else "等待加入"
    s = {"waiting": "等待中", "playing": "进行中", "finished": "已结束"}.get(g.status, g.status)

    msg = (
        f"房间：{g.room_id}\n"
        f"状态：{s}\n"
        f"黑棋：{bname}\n"
        f"白棋：{wname}\n"
        f"步数：{len(g.moves)}\n"
    )
    if g.status == "playing":
        turn_name = g.player_names.get(g.current_player, g.current_player)
        msg += f"当前回合：{turn_name}\n"
    if g.status == "finished":
        win_name = g.player_names.get(g.winner, g.winner)
        msg += f"胜者：{win_name}\n"

    await handle_pic_msg_send(bot, event, create_board_image(g), msg)


@gomoku_quit.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    user_id = str(event.get_user_id())
    ok, msg = room_manager.quit_room(user_id)
    await handle_send(bot, event, msg)


@gomoku_help.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(
        bot, event,
        "五子棋帮助：\n"
        "开始五子棋 [房间号]\n"
        "开始单人五子棋 [房间号]\n"
        "加入五子棋 <房间号>\n"
        "落子 <坐标>（如 A1）\n"
        "认输\n"
        "棋局信息 [房间号]\n"
        "退出五子棋\n"
        f"规则：{BOARD_SIZE}x{BOARD_SIZE}，黑先，五连胜；房间超时{ROOM_TIMEOUT}s，落子超时{MOVE_TIMEOUT}s。"
    )