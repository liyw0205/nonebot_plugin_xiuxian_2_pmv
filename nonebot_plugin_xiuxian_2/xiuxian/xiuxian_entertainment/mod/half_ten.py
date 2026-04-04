import random
import json
import asyncio
from pathlib import Path
from datetime import datetime

from nonebot import on_command
from nonebot.params import CommandArg

from ..command import *

# =========================
# 数据目录（娱乐独立）
# =========================
HALF_TEN_ROOMS_PATH = Path(__file__).resolve().parent / "data" / "rooms"
HALF_TEN_ROOMS_PATH.mkdir(parents=True, exist_ok=True)

# =========================
# 游戏配置
# =========================
MIN_PLAYERS = 2
MAX_PLAYERS = 10
CARDS_PER_PLAYER = 3
ROOM_TIMEOUT = 180  # 秒

CARD_SUITS = ["♠", "♥", "♦", "♣"]
CARD_VALUES = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
CARD_POINTS = {
    "A": 1, "2": 2, "3": 3, "4": 4, "5": 5, "6": 6, "7": 7, "8": 8, "9": 9, "10": 10,
    "J": 0.5, "Q": 0.5, "K": 0.5
}

# 状态
user_half_status: dict[str, str] = {}       # user_id -> room_id
half_timeout_tasks: dict[str, asyncio.Task] = {}  # room_id -> task


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _name_from_event(event, user_id: str):
    sender = getattr(event, "sender", None)
    if sender:
        return sender.card or sender.nickname or str(user_id)
    return str(user_id)


def _random_room_id():
    return str(random.randint(1000, 9999))


# =========================
# 房间模型
# =========================
class HalfTenGame:
    def __init__(self, room_id: str, creator_id: str, creator_name: str):
        self.room_id = room_id
        self.creator_id = creator_id
        self.players = [creator_id]  # 按加入顺序
        self.player_names = {creator_id: creator_name}

        self.status = "waiting"  # waiting/finished/closed
        self.create_time = _now_str()
        self.start_time = None
        self.close_reason = None

        self.cards = {}     # user_id -> ["♠A", ...]
        self.points = {}    # user_id -> float
        self.rankings = []  # [user_id...]
        self.winner = None

    def to_dict(self):
        return {
            "room_id": self.room_id,
            "creator_id": self.creator_id,
            "players": self.players,
            "player_names": self.player_names,
            "status": self.status,
            "create_time": self.create_time,
            "start_time": self.start_time,
            "close_reason": self.close_reason,
            "cards": self.cards,
            "points": self.points,
            "rankings": self.rankings,
            "winner": self.winner,
        }

    @classmethod
    def from_dict(cls, data):
        creator_id = data["creator_id"]
        creator_name = data.get("player_names", {}).get(creator_id, creator_id)
        g = cls(data["room_id"], creator_id, creator_name)

        g.players = data["players"]
        g.player_names = data.get("player_names", {creator_id: creator_name})
        g.status = data["status"]
        g.create_time = data["create_time"]
        g.start_time = data.get("start_time")
        g.close_reason = data.get("close_reason")
        g.cards = data.get("cards", {})
        g.points = data.get("points", {})
        g.rankings = data.get("rankings", [])
        g.winner = data.get("winner")
        return g

    def add_player(self, user_id: str, user_name: str):
        if user_id in self.players:
            return False
        if self.status != "waiting":
            return False
        if len(self.players) >= MAX_PLAYERS:
            return False
        self.players.append(user_id)
        self.player_names[user_id] = user_name
        return True

    def remove_player(self, user_id: str):
        if user_id not in self.players:
            return False
        self.players.remove(user_id)
        self.player_names.pop(user_id, None)

        # 房主退出，转移房主
        if self.creator_id == user_id and self.players:
            self.creator_id = self.players[0]
        return True

    def close(self, reason: str):
        self.status = "closed"
        self.close_reason = reason

    def start_and_settle(self):
        """十点半：发牌后立即结算。"""
        self.status = "finished"
        self.start_time = _now_str()

        # 组牌并洗牌
        deck = [f"{s}{v}" for s in CARD_SUITS for v in CARD_VALUES]
        random.shuffle(deck)

        self.cards = {}
        idx = 0
        for uid in self.players:
            hand = []
            for _ in range(CARDS_PER_PLAYER):
                hand.append(deck[idx])
                idx += 1
            self.cards[uid] = hand

        # 计分
        self.points = {}
        for uid, hand in self.cards.items():
            total = 0.0
            for card in hand:
                val = card[1:]  # 去花色
                total += CARD_POINTS[val]

            # 10.5 特判保留
            if abs(total - 10.5) < 1e-9:
                pt = 10.5
            else:
                # 取个位
                pt = total % 10
            self.points[uid] = pt

        # 排名：10.5最大，其次点数大；同分按加入先后
        def sort_key(uid):
            pt = self.points.get(uid, 0)
            is_ten_half = 1 if abs(pt - 10.5) < 1e-9 else 0
            join_idx = self.players.index(uid)
            return (is_ten_half, pt, -join_idx)

        self.rankings = sorted(self.players, key=sort_key, reverse=True)
        self.winner = self.rankings[0] if self.rankings else None


# =========================
# 管理器
# =========================
class HalfTenRoomManager:
    def __init__(self):
        self.rooms: dict[str, HalfTenGame] = {}
        self.load_rooms()

    def _file(self, room_id: str):
        return HALF_TEN_ROOMS_PATH / f"{room_id}.json"

    def load_rooms(self):
        for f in HALF_TEN_ROOMS_PATH.glob("*.json"):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    data = json.load(fp)
                g = HalfTenGame.from_dict(data)
                self.rooms[g.room_id] = g

                # 重建用户状态
                if g.status == "waiting":
                    for uid in g.players:
                        user_half_status[uid] = g.room_id
            except Exception:
                continue

    def save_room(self, room_id: str):
        g = self.rooms.get(room_id)
        if not g:
            return
        with open(self._file(room_id), "w", encoding="utf-8") as fp:
            json.dump(g.to_dict(), fp, ensure_ascii=False, indent=2)

    def create_room(self, room_id: str, creator_id: str, creator_name: str):
        if room_id in self.rooms:
            return None
        if self.get_user_room(creator_id):
            return None
        g = HalfTenGame(room_id, creator_id, creator_name)
        self.rooms[room_id] = g
        self.save_room(room_id)
        return g

    def join_room(self, room_id: str, user_id: str, user_name: str):
        g = self.rooms.get(room_id)
        if not g:
            return False
        if self.get_user_room(user_id):
            return False
        ok = g.add_player(user_id, user_name)
        if ok:
            self.save_room(room_id)
        return ok

    def get_room(self, room_id: str):
        return self.rooms.get(room_id)

    def get_user_room(self, user_id: str):
        for rid, g in self.rooms.items():
            if g.status == "waiting" and user_id in g.players:
                return rid
        return None

    def delete_room(self, room_id: str):
        g = self.rooms.get(room_id)
        if g:
            for uid in g.players:
                user_half_status.pop(uid, None)

        self.rooms.pop(room_id, None)

        fp = self._file(room_id)
        if fp.exists():
            fp.unlink()

        if room_id in half_timeout_tasks:
            half_timeout_tasks[room_id].cancel()
            half_timeout_tasks.pop(room_id, None)

    def quit_room(self, user_id: str):
        """
        玩家退出房间（仅 waiting 状态可退）
        返回: (success: bool, msg: str)
        """
        rid = self.get_user_room(user_id)
        if not rid:
            return False, "你当前没有参与十点半。"

        g = self.rooms[rid]

        # 游戏已结算/关闭，直接清理并提示
        if g.status != "waiting":
            return False, "当前房间已开始或已结束，无法退出。"

        old_creator = g.creator_id

        ok = g.remove_player(user_id)
        if not ok:
            return False, "退出失败：你不在该房间中。"

        # 同步状态映射
        user_half_status.pop(user_id, None)

        # 房间没人了 -> 删除房间
        if not g.players:
            self.delete_room(rid)
            return True, "你已退出，房间无人，已关闭。"

        # 仍有玩家，保存房间
        self.save_room(rid)

        # 房主变更提示
        if old_creator != g.creator_id:
            new_owner = g.player_names.get(g.creator_id, g.creator_id)
            return True, f"你已退出房间 {rid}，新房主：{new_owner}"

        return True, f"你已退出房间 {rid}。"

    def manual_settle(self, room_id: str, operator_id: str):
        g = self.rooms.get(room_id)
        if not g:
            return False, "房间不存在。"
        if g.status != "waiting":
            return False, "房间已结算或关闭。"
        if g.creator_id != operator_id:
            return False, "只有房主可以结算。"

        if len(g.players) < MIN_PLAYERS:
            g.close(f"人数不足{MIN_PLAYERS}人")
            self.save_room(room_id)
            return True, "close"

        g.start_and_settle()
        self.save_room(room_id)
        return True, "settle"


half_manager = HalfTenRoomManager()


# =========================
# 文本构造
# =========================
def _pt_text(pt):
    if abs(pt - 10.5) < 1e-9:
        return "10.5点 ✨"
    if abs(pt - int(pt)) < 1e-9:
        return f"{int(pt)}点"
    return f"{pt}点"


def build_result_text(g: HalfTenGame):
    lines = [f"🎮 十点半结算（房间 {g.room_id}）", ""]
    for i, uid in enumerate(g.rankings, start=1):
        name = g.player_names.get(uid, uid)
        hand = " ".join(g.cards.get(uid, []))
        pt = _pt_text(g.points.get(uid, 0))

        if i == 1:
            rank = "🥇 冠军"
        elif i == 2:
            rank = "🥈 亚军"
        elif i == 3:
            rank = "🥉 季军"
        else:
            rank = f"第{i}名"

        lines.append(f"{rank}：{name}")
        lines.append(f"  手牌：{hand}")
        lines.append(f"  点数：{pt}")
        lines.append("")
    return "\n".join(lines).strip()


# =========================
# 超时任务
# =========================
async def start_half_timeout(bot, event, room_id: str):
    if room_id in half_timeout_tasks:
        half_timeout_tasks[room_id].cancel()

    async def _task():
        await asyncio.sleep(ROOM_TIMEOUT)
        g = half_manager.get_room(room_id)
        if not g or g.status != "waiting":
            return

        if len(g.players) < MIN_PLAYERS:
            g.close(f"超时且人数不足{MIN_PLAYERS}人")
            half_manager.save_room(room_id)
            half_manager.delete_room(room_id)
            await handle_send(bot, event, f"房间 {room_id} 超时，人数不足，已关闭。")
            return

        g.start_and_settle()
        half_manager.save_room(room_id)

        winner = g.player_names.get(g.winner, g.winner) if g.winner else "未知"
        result = build_result_text(g)
        await handle_send(bot, event, f"房间 {room_id} 超时自动结算。\n🎉 冠军：{winner}\n\n{result}")

        half_manager.delete_room(room_id)

    half_timeout_tasks[room_id] = asyncio.create_task(_task())


# =========================
# 命令注册
# =========================
half_ten_start = on_command("开始十点半", priority=5, block=True)
half_ten_join = on_command("加入十点半", priority=5, block=True)
half_ten_close = on_command("结算十点半", priority=5, block=True)
half_ten_quit = on_command("退出十点半", priority=5, block=True)
half_ten_info = on_command("十点半信息", priority=5, block=True)
half_ten_help = on_command("十点半帮助", priority=5, block=True)


@half_ten_start.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    user_name = _name_from_event(event, user_id)
    arg = args.extract_plain_text().strip()

    if half_manager.get_user_room(user_id):
        await handle_send(bot, event, "你已在其它十点半房间中，请先退出。")
        return

    room_id = arg if arg else _random_room_id()
    while half_manager.get_room(room_id):
        room_id = _random_room_id()

    g = half_manager.create_room(room_id, user_id, user_name)
    if not g:
        await handle_send(bot, event, "创建失败（房间号重复或你已在其它房间）。")
        return

    user_half_status[user_id] = room_id
    await handle_send(
        bot, event,
        f"十点半房间 {room_id} 创建成功！\n"
        f"房主：{user_name}\n"
        f"人数：1/{MAX_PLAYERS}（最少{MIN_PLAYERS}人）\n"
        f"命令：加入十点半 {room_id}\n"
        f"房主可用【结算十点半】提前开局。"
    )
    await start_half_timeout(bot, event, room_id)


@half_ten_join.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    user_name = _name_from_event(event, user_id)
    room_id = args.extract_plain_text().strip()

    if not room_id:
        await handle_send(bot, event, "请带房间号：加入十点半 1234")
        return

    if half_manager.get_user_room(user_id):
        await handle_send(bot, event, "你已在其它十点半房间中，请先退出。")
        return

    ok = half_manager.join_room(room_id, user_id, user_name)
    if not ok:
        await handle_send(bot, event, "加入失败（房间不存在/已结算/已满）。")
        return

    user_half_status[user_id] = room_id
    g = half_manager.get_room(room_id)

    # 满员自动结算
    if len(g.players) >= MAX_PLAYERS:
        if room_id in half_timeout_tasks:
            half_timeout_tasks[room_id].cancel()
            half_timeout_tasks.pop(room_id, None)

        g.start_and_settle()
        half_manager.save_room(room_id)

        winner = g.player_names.get(g.winner, g.winner) if g.winner else "未知"
        result = build_result_text(g)
        await handle_send(bot, event, f"房间 {room_id} 人数已满，自动开局结算。\n🎉 冠军：{winner}\n\n{result}")
        half_manager.delete_room(room_id)
        return

    await handle_send(
        bot, event,
        f"加入成功：房间 {room_id}\n"
        f"当前人数：{len(g.players)}/{MAX_PLAYERS}\n"
        f"还需 {max(0, MIN_PLAYERS - len(g.players))} 人可开局。"
    )

    # 人数变化，重置超时
    await start_half_timeout(bot, event, room_id)


@half_ten_close.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    user_id = str(event.get_user_id())
    room_id = half_manager.get_user_room(user_id)

    if not room_id:
        await handle_send(bot, event, "你当前没有十点半房间。")
        return

    ok, res = half_manager.manual_settle(room_id, user_id)
    if not ok:
        await handle_send(bot, event, res)
        return

    if res == "close":
        half_manager.delete_room(room_id)
        await handle_send(bot, event, f"人数不足{MIN_PLAYERS}人，房间已关闭。")
        return

    g = half_manager.get_room(room_id)
    if room_id in half_timeout_tasks:
        half_timeout_tasks[room_id].cancel()
        half_timeout_tasks.pop(room_id, None)

    winner = g.player_names.get(g.winner, g.winner) if g.winner else "未知"
    result = build_result_text(g)
    await handle_send(bot, event, f"手动结算完成。\n🎉 冠军：{winner}\n\n{result}")
    half_manager.delete_room(room_id)


@half_ten_quit.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    user_id = str(event.get_user_id())
    ok, msg = half_manager.quit_room(user_id)
    await handle_send(bot, event, msg)


@half_ten_info.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    user_id = str(event.get_user_id())
    arg = args.extract_plain_text().strip()

    room_id = arg if arg else half_manager.get_user_room(user_id)
    if not room_id:
        await handle_send(bot, event, "你当前没有十点半房间。")
        return

    g = half_manager.get_room(room_id)
    if not g:
        await handle_send(bot, event, f"房间 {room_id} 不存在。")
        return

    status_map = {"waiting": "等待中", "finished": "已结算", "closed": "已关闭"}
    players = "、".join([g.player_names.get(uid, uid) for uid in g.players])

    msg = (
        f"房间：{g.room_id}\n"
        f"状态：{status_map.get(g.status, g.status)}\n"
        f"房主：{g.player_names.get(g.creator_id, g.creator_id)}\n"
        f"人数：{len(g.players)}/{MAX_PLAYERS}\n"
        f"玩家：{players}\n"
        f"创建时间：{g.create_time}\n"
    )

    if g.status == "finished" and g.winner:
        msg += f"冠军：{g.player_names.get(g.winner, g.winner)}\n"

    if g.close_reason:
        msg += f"关闭原因：{g.close_reason}\n"

    await handle_send(bot, event, msg.strip())


@half_ten_help.handle(parameterless=[Cooldown(cd_time=1.0)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(
        bot, event,
        "十点半帮助：\n"
        "开始十点半 [房间号]\n"
        "加入十点半 <房间号>\n"
        "结算十点半（仅房主）\n"
        "退出十点半\n"
        "十点半信息 [房间号]\n\n"
        "规则：每人3张牌，A=1，2-10按点，J/Q/K=0.5；10.5最大，其次点数大者胜；同分按入场顺序。"
    )