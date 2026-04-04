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

ms_start = on_command("开始扫雷", priority=5, block=True)
ms_open = on_command("翻开", priority=5, block=True)
ms_flag = on_command("标记", priority=5, block=True)
ms_info = on_command("扫雷信息", priority=5, block=True)
ms_end = on_command("结束扫雷", priority=5, block=True)
ms_help = on_command("扫雷帮助", priority=5, block=True)


def _name(event):
    sender = getattr(event, "sender", None)
    if sender:
        return sender.card or sender.nickname or str(event.get_user_id())
    return str(event.get_user_id())


@ms_start.handle(parameterless=[Cooldown(cd_time=1.2)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    raw = args.extract_plain_text().strip()
    if not raw:
        w, h, m = DIFFICULTY["初级"]
    else:
        parts = raw.split()
        if parts[0] in DIFFICULTY:
            w, h, m = DIFFICULTY[parts[0]]
        elif len(parts) == 4 and parts[0] == "自定义":
            try:
                w, h, m = int(parts[1]), int(parts[2]), int(parts[3])
            except Exception:
                await handle_send(bot, event, "格式错误：开始扫雷 自定义 宽 高 雷数")
                return
        else:
            await handle_send(bot, event, "格式错误：开始扫雷 [初级|中级|高级|自定义 宽 高 雷数]")
            return

    g, err = ms_manager.create(str(event.get_user_id()), _name(event), w, h, m)
    if not g:
        await handle_send(bot, event, err)
        return

    await start_ms_timeout(bot, event, g.game_id, handle_send)
    img = render_game(g)
    await handle_pic_msg_send(bot, event, img, f"扫雷开始：{w}x{h} 雷数{m}\n使用：翻开 A1 / 标记 B2")


@ms_open.handle(parameterless=[Cooldown(cd_time=0.8)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    g = ms_manager.get_user_game(str(event.get_user_id()))
    if not g:
        await handle_send(bot, event, "你当前没有扫雷局，发送【开始扫雷】创建。")
        return

    p = parse_coord(args.extract_plain_text().strip())
    if not p:
        await handle_send(bot, event, "坐标格式错误，例如：翻开 A1")
        return

    x, y = p
    if not (0 <= x < g.width and 0 <= y < g.height):
        await handle_send(bot, event, "坐标超出棋盘范围。")
        return

    if g.flagged[y][x]:
        await handle_send(bot, event, "该位置已标记旗子，请先取消标记。")
        return
    if g.revealed[y][x]:
        await handle_send(bot, event, "该位置已经翻开。")
        return

    if not g.first_click_done:
        plant_mines(g, x, y)
        g.first_click_done = True

    if g.board[y][x] == -1:
        g.revealed[y][x] = True
        g.status = "lose"
        ms_manager.save(g.game_id)
        img = render_game(g, reveal_all=True)
        await handle_pic_msg_send(bot, event, img, "💥 踩雷了，游戏失败！")
        ms_manager.delete(g.game_id)
        return

    flood_reveal(g, x, y)
    g.last_action_time = __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if check_win(g):
        g.status = "win"
        ms_manager.save(g.game_id)
        img = render_game(g, reveal_all=True)
        await handle_pic_msg_send(bot, event, img, "🎉 恭喜通关扫雷！")
        ms_manager.delete(g.game_id)
        return

    ms_manager.save(g.game_id)
    img = render_game(g)
    await handle_pic_send(bot, event, img)


@ms_flag.handle(parameterless=[Cooldown(cd_time=0.8)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    g = ms_manager.get_user_game(str(event.get_user_id()))
    if not g:
        await handle_send(bot, event, "你当前没有扫雷局。")
        return

    p = parse_coord(args.extract_plain_text().strip())
    if not p:
        await handle_send(bot, event, "坐标格式错误，例如：标记 B2")
        return

    x, y = p
    if not (0 <= x < g.width and 0 <= y < g.height):
        await handle_send(bot, event, "坐标超出范围。")
        return
    if g.revealed[y][x]:
        await handle_send(bot, event, "该位置已经翻开，不能标记。")
        return

    g.flagged[y][x] = not g.flagged[y][x]
    ms_manager.save(g.game_id)
    img = render_game(g)
    await handle_pic_send(bot, event, img)


@ms_info.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    g = ms_manager.get_user_game(str(event.get_user_id()))
    if not g:
        await handle_send(bot, event, "你当前没有扫雷局。")
        return

    opened = sum(1 for yy in range(g.height) for xx in range(g.width) if g.revealed[yy][xx])
    flags = sum(1 for yy in range(g.height) for xx in range(g.width) if g.flagged[yy][xx])
    await handle_send(bot, event, f"扫雷信息：{g.width}x{g.height} 雷{g.mines}\n已翻开：{opened}\n旗子：{flags}")
    await handle_pic_send(bot, event, render_game(g))


@ms_end.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    g = ms_manager.get_user_game(str(event.get_user_id()))
    if not g:
        await handle_send(bot, event, "你当前没有扫雷局。")
        return
    if g.game_id in minesweeper_timeout_tasks:
        minesweeper_timeout_tasks[g.game_id].cancel()
        minesweeper_timeout_tasks.pop(g.game_id, None)
    ms_manager.delete(g.game_id)
    await handle_send(bot, event, "已结束当前扫雷局。")


@ms_help.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(
        bot, event,
        "扫雷帮助：\n"
        "开始扫雷 [初级|中级|高级|自定义 宽 高 雷数]\n"
        "翻开 A1\n"
        "标记 B2\n"
        "扫雷信息\n"
        "结束扫雷"
    )

# =========================
# 数据目录
# =========================
MINESWEEPER_DATA_PATH = Path(__file__).resolve().parent / "data" / "rooms"
MINESWEEPER_DATA_PATH.mkdir(parents=True, exist_ok=True)

# 用户状态：每个用户仅一个扫雷局
user_minesweeper_status = {}   # {user_id: game_id}
minesweeper_timeout_tasks = {} # {game_id: task}

# 超时
GAME_TIMEOUT = 600  # 10分钟

# 难度
DIFFICULTY = {
    "初级": (9, 9, 10),
    "中级": (16, 16, 40),
    "高级": (30, 16, 99),
}


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_name(event, user_id: str):
    try:
        sender = getattr(event, "sender", None)
        if sender:
            return sender.card or sender.nickname or str(user_id)
    except Exception:
        pass
    return str(user_id)


class MinesweeperGame:
    def __init__(self, game_id: str, user_id: str, user_name: str, w: int, h: int, mines: int):
        self.game_id = game_id
        self.user_id = str(user_id)
        self.user_name = user_name
        self.width = w
        self.height = h
        self.mines = mines

        self.status = "playing"  # playing/win/lose/closed
        self.create_time = _now_str()
        self.last_action_time = _now_str()

        self.first_click_done = False

        # board: -1 雷，0-8 数字
        self.board = [[0 for _ in range(w)] for _ in range(h)]
        self.revealed = [[False for _ in range(w)] for _ in range(h)]
        self.flagged = [[False for _ in range(w)] for _ in range(h)]

    def to_dict(self):
        return self.__dict__

    @classmethod
    def from_dict(cls, d):
        obj = cls(d["game_id"], d["user_id"], d["user_name"], d["width"], d["height"], d["mines"])
        obj.status = d["status"]
        obj.create_time = d["create_time"]
        obj.last_action_time = d["last_action_time"]
        obj.first_click_done = d["first_click_done"]
        obj.board = d["board"]
        obj.revealed = d["revealed"]
        obj.flagged = d["flagged"]
        return obj


class MinesweeperManager:
    def __init__(self):
        self.games = {}
        self._load()

    def _file(self, game_id: str):
        return MINESWEEPER_DATA_PATH / f"{game_id}.json"

    def _load(self):
        for f in MINESWEEPER_DATA_PATH.glob("*.json"):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                game = MinesweeperGame.from_dict(data)
                self.games[game.game_id] = game
                if game.status == "playing":
                    user_minesweeper_status[game.user_id] = game.game_id
            except Exception:
                continue

    def save(self, game_id: str):
        game = self.games.get(game_id)
        if not game:
            return
        with open(self._file(game_id), "w", encoding="utf-8") as fp:
            json.dump(game.to_dict(), fp, ensure_ascii=False, indent=2)

    def delete(self, game_id: str):
        g = self.games.get(game_id)
        if g:
            user_minesweeper_status.pop(g.user_id, None)
        self.games.pop(game_id, None)
        fp = self._file(game_id)
        if fp.exists():
            fp.unlink()

    def create(self, user_id: str, user_name: str, w: int, h: int, mines: int):
        if str(user_id) in user_minesweeper_status:
            return None, "你已有进行中的扫雷局，请先结束。"
        if mines <= 0 or mines >= w * h:
            return None, "雷数不合法。"
        game_id = f"ms_{random.randint(100000, 999999)}"
        while game_id in self.games:
            game_id = f"ms_{random.randint(100000, 999999)}"
        g = MinesweeperGame(game_id, str(user_id), user_name, w, h, mines)
        self.games[game_id] = g
        user_minesweeper_status[str(user_id)] = game_id
        self.save(game_id)
        return g, ""

    def get_user_game(self, user_id: str):
        gid = user_minesweeper_status.get(str(user_id))
        if not gid:
            return None
        return self.games.get(gid)


ms_manager = MinesweeperManager()


def parse_coord(s: str):
    s = s.strip().upper()
    if len(s) < 2:
        return None
    letters = ""
    nums = ""
    for ch in s:
        if ch.isalpha():
            letters += ch
        elif ch.isdigit():
            nums += ch
    if not letters or not nums:
        return None
    col = 0
    for i, c in enumerate(reversed(letters)):
        col += (ord(c) - ord("A") + 1) * (26 ** i)
    col -= 1
    row = int(nums) - 1
    return col, row


def pos_to_coord(x: int, y: int):
    n = x + 1
    letters = ""
    while n > 0:
        n -= 1
        letters = chr(ord("A") + (n % 26)) + letters
        n //= 26
    return f"{letters}{y+1}"


def in_board(g: MinesweeperGame, x, y):
    return 0 <= x < g.width and 0 <= y < g.height


def plant_mines(g: MinesweeperGame, safe_x: int, safe_y: int):
    # 首击保护：safe点及其8邻域不放雷
    forbidden = set()
    for dy in (-1, 0, 1):
        for dx in (-1, 0, 1):
            nx, ny = safe_x + dx, safe_y + dy
            if in_board(g, nx, ny):
                forbidden.add((nx, ny))

    cells = [(x, y) for y in range(g.height) for x in range(g.width) if (x, y) not in forbidden]
    random.shuffle(cells)
    for i in range(g.mines):
        x, y = cells[i]
        g.board[y][x] = -1

    # 计算数字
    for y in range(g.height):
        for x in range(g.width):
            if g.board[y][x] == -1:
                continue
            c = 0
            for dy in (-1, 0, 1):
                for dx in (-1, 0, 1):
                    nx, ny = x + dx, y + dy
                    if in_board(g, nx, ny) and g.board[ny][nx] == -1:
                        c += 1
            g.board[y][x] = c


def flood_reveal(g: MinesweeperGame, sx: int, sy: int):
    stack = [(sx, sy)]
    while stack:
        x, y = stack.pop()
        if not in_board(g, x, y):
            continue
        if g.revealed[y][x] or g.flagged[y][x]:
            continue
        g.revealed[y][x] = True
        if g.board[y][x] == 0:
            for dy in (-1, 0, 1):
                for dx in (-1, 0, 1):
                    if dx == 0 and dy == 0:
                        continue
                    stack.append((x + dx, y + dy))


def check_win(g: MinesweeperGame):
    # 非雷格全部翻开即胜利
    total_safe = g.width * g.height - g.mines
    opened = 0
    for y in range(g.height):
        for x in range(g.width):
            if g.revealed[y][x] and g.board[y][x] != -1:
                opened += 1
    return opened >= total_safe


def render_game(g: MinesweeperGame, reveal_all=False) -> BytesIO:
    cell = 36
    margin = 52
    w = g.width * cell + margin * 2
    h = g.height * cell + margin * 2

    img = Image.new("RGB", (w, h), (245, 245, 245))
    d = ImageDraw.Draw(img)

    try:
        font = ImageFont.truetype("arial.ttf", 18)
    except Exception:
        font = ImageFont.load_default()

    number_color = {
        1: (25, 118, 210),
        2: (56, 142, 60),
        3: (211, 47, 47),
        4: (123, 31, 162),
        5: (93, 64, 55),
        6: (0, 131, 143),
        7: (66, 66, 66),
        8: (0, 0, 0),
    }

    # 格子
    for y in range(g.height):
        for x in range(g.width):
            x1 = margin + x * cell
            y1 = margin + y * cell
            x2 = x1 + cell
            y2 = y1 + cell

            opened = g.revealed[y][x] or reveal_all
            if opened:
                d.rectangle([x1, y1, x2, y2], fill=(225, 225, 225), outline=(170, 170, 170))
                val = g.board[y][x]
                if val == -1:
                    d.ellipse([x1+8, y1+8, x2-8, y2-8], fill=(40, 40, 40))
                elif val > 0:
                    txt = str(val)
                    d.text((x1+12, y1+8), txt, fill=number_color.get(val, (0, 0, 0)), font=font)
            else:
                d.rectangle([x1, y1, x2, y2], fill=(189, 189, 189), outline=(130, 130, 130))
                if g.flagged[y][x]:
                    d.text((x1+9, y1+7), "⚑", fill=(220, 20, 60), font=font)

    # 坐标
    for x in range(g.width):
        d.text((margin + x*cell + 10, margin - 28), pos_to_coord(x, 0).rstrip("1"), fill=(0, 0, 0), font=font)
    for y in range(g.height):
        d.text((margin - 30, margin + y*cell + 8), str(y+1), fill=(0, 0, 0), font=font)

    out = BytesIO()
    img.save(out, format="PNG")
    out.seek(0)
    return out


async def start_ms_timeout(bot, event, game_id: str, handle_send):
    if game_id in minesweeper_timeout_tasks:
        minesweeper_timeout_tasks[game_id].cancel()

    async def _timeout():
        await asyncio.sleep(GAME_TIMEOUT)
        g = ms_manager.games.get(game_id)
        if not g or g.status != "playing":
            return
        g.status = "closed"
        ms_manager.save(game_id)
        await handle_send(bot, event, f"扫雷超时（{GAME_TIMEOUT}秒无操作），本局已关闭。")
        ms_manager.delete(game_id)

    t = asyncio.create_task(_timeout())
    minesweeper_timeout_tasks[game_id] = t