from __future__ import annotations

import asyncio
import hashlib
import json
import random
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator, Literal, Optional, Union
from urllib.parse import urlparse

from nonebot.adapters import Bot as BaseBot
from nonebot.adapters import Event as BaseEvent
from nonebot.log import logger
from nonebot.permission import Permission

# =========================
# 可选导入：onebot v11
# =========================
try:
    from nonebot.adapters.onebot.v11 import (
        Bot as OB11Bot,
        Message as OB11Message,
        MessageSegment as OB11MessageSegment,
    )
    from nonebot.adapters.onebot.v11.event import (
        GroupMessageEvent as OB11GroupMessageEvent,
        PrivateMessageEvent as OB11PrivateMessageEvent,
    )

    HAS_OB11 = True
except Exception:
    HAS_OB11 = False
    OB11Bot = None  # type: ignore
    OB11Message = str  # type: ignore
    OB11MessageSegment = None  # type: ignore
    OB11GroupMessageEvent = tuple()  # type: ignore
    OB11PrivateMessageEvent = tuple()  # type: ignore

# =========================
# 可选导入：qq
# =========================
try:
    from nonebot.adapters.qq import (
        Bot as QQBot,
        Message as QQMessage,
        MessageSegment as QQMessageSegment,
    )
    from nonebot.adapters.qq.event import (
        C2CMessageCreateEvent as QQPrivateMessageEvent,
        GroupAtMessageCreateEvent as QQGroupMessageEvent,
        AtMessageCreateEvent as QQAtChannelMessageEvent,
        DirectMessageCreateEvent as QQChannelPrivateMessageEvent,
    )
    from nonebot.adapters.qq.models import MessageMarkdown, MessageKeyboard

    HAS_QQ = True
except Exception:
    HAS_QQ = False
    QQBot = None  # type: ignore
    QQMessage = str  # type: ignore
    QQMessageSegment = None  # type: ignore
    QQPrivateMessageEvent = tuple()  # type: ignore
    QQGroupMessageEvent = tuple()  # type: ignore
    QQAtChannelMessageEvent = tuple()  # type: ignore
    QQChannelPrivateMessageEvent = tuple()  # type: ignore
    MessageMarkdown = None  # type: ignore
    MessageKeyboard = None  # type: ignore


@dataclass
class CompatSender:
    """兼容 sender 对象，支持属性访问与序列化"""

    user_id: Optional[str] = None
    nickname: Optional[str] = None
    card: Optional[str] = None
    role: Optional[str] = "member"

    def dict(self) -> dict[str, Any]:
        return asdict(self)

    def model_dump(self) -> dict[str, Any]:
        return asdict(self)

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        yield from asdict(self).items()


# =========================
# Web 消息记录数据库
# =========================

MESSAGE_DB = Path() / "message.db"
_last_message_db_cleanup_ts = 0.0
message_db_max_size_mb = 1000
# 消息数据库最大大小 MB。达到或超过该值时，按最早日期清理聊天记录。
# 最低 100，最高 10000。清理优先级高于保留天数。
# 清理顺序：优先清理群聊/频道群聊，再清理私聊/频道私聊。

message_group_keep_days = 0
# 群聊消息最大保留天数，0 表示不启用。
# 例如 3 表示超过 3 天的群聊/频道群聊消息会被清理。

message_private_keep_days = 0
# 私聊消息最大保留天数，0 表示不启用。
# 例如 10 表示超过 10 天的私聊/频道私聊消息会被清理。

def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _init_message_db():
    MESSAGE_DB.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(MESSAGE_DB)
    try:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                adapter TEXT,
                bot_id TEXT,

                direction TEXT NOT NULL,
                scene TEXT NOT NULL,

                message_id TEXT,
                source_message_id TEXT,

                group_id TEXT,
                group_name TEXT,

                user_id TEXT,
                username TEXT,
                nickname TEXT,
                avatar TEXT,

                content TEXT,

                reply_used_count INTEGER DEFAULT 0,

                created_at TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_nicknames (
                user_id TEXT PRIMARY KEY,

                username TEXT NOT NULL,

                adapter TEXT,
                bot_id TEXT,

                source_scene TEXT,
                source_group_id TEXT,

                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
                    )
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_nicknames_username ON user_nicknames(username)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_nicknames_source_group_id ON user_nicknames(source_group_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_direction ON messages(direction)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_scene ON messages(scene)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_message_id ON messages(message_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_source_message_id ON messages(source_message_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_group_id ON messages(group_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at)")

        conn.commit()
    finally:
        conn.close()

def _get_message_cleanup_config() -> tuple[int, int, int]:
    """
    返回:
    - max_size_mb: message.db 最大大小，最低100，最高10000
    - group_keep_days: 群聊保留天数，0关闭
    - private_keep_days: 私聊保留天数，0关闭
    """

    max_size_mb = max(100, min(10000, message_db_max_size_mb))
    group_keep_days = max(0, message_group_keep_days)
    private_keep_days = max(0, message_private_keep_days)

    return max_size_mb, group_keep_days, private_keep_days


def _message_db_size_mb() -> float:
    try:
        if not MESSAGE_DB.exists():
            return 0.0
        return MESSAGE_DB.stat().st_size / 1024 / 1024
    except Exception:
        return 0.0


def _vacuum_message_db(conn: sqlite3.Connection):
    try:
        conn.execute("VACUUM")
    except Exception as e:
        logger.debug(f"[message.db] VACUUM失败: {e}")


def _cleanup_message_db_by_size(conn: sqlite3.Connection, max_size_mb: int) -> int:
    """
    message.db 达到阈值时按最早日期清理。
    优先级高于保留天数。
    清理顺序：
    1. 群聊 / 频道群聊
    2. 私聊 / 频道私聊

    每次按“最早一天”删除。
    如果删除后仍超阈值，会继续删下一天。
    """
    deleted_total = 0

    if max_size_mb <= 0:
        return 0

    cur = conn.cursor()

    scene_groups = [
        ("群聊", ("group", "channel_group")),
        ("私聊", ("private", "channel_private")),
    ]

    # 防止极端情况下死循环
    max_round = 500
    round_count = 0

    while _message_db_size_mb() >= max_size_mb and round_count < max_round:
        round_count += 1
        deleted_this_round = 0

        for label, scenes in scene_groups:
            placeholders = ",".join(["?"] * len(scenes))

            cur.execute(
                f"""
                SELECT date(created_at) AS d
                FROM messages
                WHERE scene IN ({placeholders})
                  AND created_at IS NOT NULL
                  AND created_at != ''
                GROUP BY date(created_at)
                ORDER BY d ASC
                LIMIT 1
                """,
                list(scenes),
            )

            row = cur.fetchone()
            if not row or not row[0]:
                continue

            oldest_date = str(row[0])

            cur.execute(
                f"""
                DELETE FROM messages
                WHERE scene IN ({placeholders})
                  AND date(created_at) = ?
                """,
                list(scenes) + [oldest_date],
            )

            deleted_count = cur.rowcount or 0
            conn.commit()

            deleted_total += deleted_count
            deleted_this_round += deleted_count

            logger.warning(
                f"[message.db] 数据库大小超过 {max_size_mb}MB，"
                f"已按大小清理{label}最早日期 {oldest_date} 的消息 {deleted_count} 条"
            )

            # 每次优先清一个群聊日期；如果群聊清了，就重新判断大小。
            # 只有群聊没得清时才会进入私聊。
            if deleted_count > 0:
                break

        if deleted_this_round <= 0:
            break

    if deleted_total > 0:
        _vacuum_message_db(conn)

    return deleted_total


def _cleanup_message_db_by_keep_days(conn: sqlite3.Connection, group_keep_days: int, private_keep_days: int) -> int:
    """
    按保留天数清理。
    注意：大小清理优先级更高，所以外部应先执行 _cleanup_message_db_by_size。
    """
    deleted_total = 0
    cur = conn.cursor()

    if group_keep_days > 0:
        cutoff = (datetime.now() - timedelta(days=group_keep_days)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """
            DELETE FROM messages
            WHERE scene IN ('group', 'channel_group')
              AND created_at < ?
            """,
            (cutoff,),
        )
        deleted = cur.rowcount or 0
        deleted_total += deleted
        if deleted > 0:
            logger.info(f"[message.db] 已按群聊保留天数 {group_keep_days} 天清理消息 {deleted} 条")

    if private_keep_days > 0:
        cutoff = (datetime.now() - timedelta(days=private_keep_days)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """
            DELETE FROM messages
            WHERE scene IN ('private', 'channel_private')
              AND created_at < ?
            """,
            (cutoff,),
        )
        deleted = cur.rowcount or 0
        deleted_total += deleted
        if deleted > 0:
            logger.info(f"[message.db] 已按私聊保留天数 {private_keep_days} 天清理消息 {deleted} 条")

    if deleted_total > 0:
        conn.commit()
        _vacuum_message_db(conn)

    return deleted_total


def _maybe_cleanup_message_db():
    """
    消息库清理入口。
    为避免每条消息都扫描数据库，做简单节流。
    """
    global _last_message_db_cleanup_ts

    now_ts = datetime.now().timestamp()

    # 每 10 分钟最多检查一次
    if now_ts - _last_message_db_cleanup_ts < 600:
        return

    _last_message_db_cleanup_ts = now_ts

    try:
        _init_message_db()

        max_size_mb, group_keep_days, private_keep_days = _get_message_cleanup_config()

        conn = sqlite3.connect(MESSAGE_DB)
        try:
            # 重要：大小清理优先于保留天数
            _cleanup_message_db_by_size(conn, max_size_mb)
            _cleanup_message_db_by_keep_days(conn, group_keep_days, private_keep_days)
        finally:
            conn.close()

    except Exception as e:
        logger.warning(f"[message.db] 自动清理失败: {e}")

def _insert_message_record(
    *,
    adapter: str = "",
    bot_id: str = "",
    direction: str,
    scene: str,
    message_id: str = "",
    source_message_id: str = "",
    group_id: str = "",
    group_name: str = "",
    user_id: str = "",
    username: str = "",
    nickname: str = "",
    avatar: str = "",
    content: str = "",
):
    try:
        _init_message_db()

        conn = sqlite3.connect(MESSAGE_DB)
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO messages (
                    adapter, bot_id,
                    direction, scene,
                    message_id, source_message_id,
                    group_id, group_name,
                    user_id, username, nickname, avatar,
                    content,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(adapter or ""),
                    str(bot_id or ""),
                    str(direction or ""),
                    str(scene or "unknown"),
                    str(message_id or ""),
                    str(source_message_id or ""),
                    str(group_id or ""),
                    str(group_name or ""),
                    str(user_id or ""),
                    str(username or ""),
                    str(nickname or ""),
                    str(avatar or ""),
                    str(content or ""),
                    _now_str(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
        _maybe_cleanup_message_db()
    except Exception as e:
        logger.warning(f"[message.db] 写入消息记录失败: {e}")


def _increase_recv_reply_used_count(
    *,
    source_message_id: str,
    adapter: str = "",
    bot_id: str = "",
    scene: str = "",
    group_id: str = "",
    user_id: str = "",
):
    """
    source_message_id 是 send 记录里保存的“被回复消息ID”
    也就是 recv.message_id。

    QQ 回复限制应统计 recv 消息被回复次数：
    recv.reply_used_count += 1
    """
    if not source_message_id:
        return

    try:
        _init_message_db()
        conn = sqlite3.connect(MESSAGE_DB)

        try:
            cur = conn.cursor()

            where = [
                "direction = 'recv'",
                "message_id = ?",
            ]
            params: list[Any] = [str(source_message_id)]

            # 尽量加条件，避免不同适配器/不同 bot/message_id 撞号
            if adapter:
                where.append("adapter = ?")
                params.append(str(adapter))

            if bot_id:
                where.append("bot_id = ?")
                params.append(str(bot_id))

            if scene:
                where.append("scene = ?")
                params.append(str(scene))

            if group_id:
                where.append("group_id = ?")
                params.append(str(group_id))

            if user_id:
                where.append("user_id = ?")
                params.append(str(user_id))

            sql = f"""
                UPDATE messages
                SET reply_used_count = COALESCE(reply_used_count, 0) + 1
                WHERE {' AND '.join(where)}
            """

            cur.execute(sql, params)

            # 如果条件太严格没更新到，则退化为只按 adapter/bot/message_id 更新
            if cur.rowcount == 0:
                fallback_where = [
                    "direction = 'recv'",
                    "message_id = ?",
                ]
                fallback_params: list[Any] = [str(source_message_id)]

                if adapter:
                    fallback_where.append("adapter = ?")
                    fallback_params.append(str(adapter))

                if bot_id:
                    fallback_where.append("bot_id = ?")
                    fallback_params.append(str(bot_id))

                fallback_sql = f"""
                    UPDATE messages
                    SET reply_used_count = COALESCE(reply_used_count, 0) + 1
                    WHERE {' AND '.join(fallback_where)}
                """
                cur.execute(fallback_sql, fallback_params)

            conn.commit()

        finally:
            conn.close()

    except Exception as e:
        logger.warning(f"[message.db] 更新 recv.reply_used_count 失败: {e}")


def _extract_text_from_message_obj(message: Any) -> str:
    try:
        if message is None:
            return ""
        if isinstance(message, str):
            return message
        if hasattr(message, "extract_plain_text"):
            text = message.extract_plain_text()
            if text:
                return str(text)
        if hasattr(message, "extract_content"):
            text = message.extract_content()
            if text:
                return str(text)
        return str(message)
    except Exception:
        return ""


def _get_adapter_name(bot: Any) -> str:
    try:
        return str(bot.adapter.get_name())
    except Exception:
        return ""


def _get_bot_id(bot: Any) -> str:
    try:
        return str(bot.self_id)
    except Exception:
        return ""


def _extract_result_message_id(result: Any) -> str:
    try:
        if result is None:
            return ""

        if isinstance(result, dict):
            return str(
                result.get("message_id")
                or result.get("msg_id")
                or result.get("id")
                or ""
            )

        return str(
            getattr(result, "message_id", "")
            or getattr(result, "msg_id", "")
            or getattr(result, "id", "")
            or ""
        )
    except Exception:
        return ""


def _get_author_username_avatar(event: BaseEvent) -> tuple[str, str]:
    username = ""
    avatar = ""

    try:
        author = getattr(event, "author", None)
        if author is not None:
            username = str(getattr(author, "username", "") or "")
            avatar = str(getattr(author, "avatar", "") or "")
    except Exception:
        pass

    return username, avatar

def _record_group_user_nickname(
    *,
    adapter: str = "",
    bot_id: str = "",
    scene: str = "",
    group_id: str = "",
    user_id: str = "",
    username: str = "",
):
    """
    记录群聊里见过的用户昵称。
    规则：
    - 只记录群聊 / 频道群聊
    - username 不为空
    - user_id 不为空
    - 如果该 user_id 已记录，则不覆盖
    """
    try:
        if scene not in ("group", "channel_group"):
            return

        user_id = str(user_id or "").strip()
        username = str(username or "").strip()

        if not user_id or not username:
            return

        _init_message_db()

        conn = sqlite3.connect(MESSAGE_DB)
        try:
            cur = conn.cursor()
            now = _now_str()

            cur.execute(
                """
                INSERT OR IGNORE INTO user_nicknames (
                    user_id,
                    username,
                    adapter,
                    bot_id,
                    source_scene,
                    source_group_id,
                    first_seen_at,
                    last_seen_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    username,
                    str(adapter or ""),
                    str(bot_id or ""),
                    str(scene or ""),
                    str(group_id or ""),
                    now,
                    now,
                ),
            )

            conn.commit()
        finally:
            conn.close()

    except Exception as e:
        logger.warning(f"[message.db] 记录用户昵称失败: {e}")

def _record_recv_message(bot: Any, event: BaseEvent):
    """记录收到的消息"""
    try:
        if getattr(event, "__message_db_recv_recorded__", False):
            return

        if event.get_type() != "message":
            return

        scene = get_chat_scene(event)
        adapter = _get_adapter_name(bot) if bot is not None else ""
        bot_id = _get_bot_id(bot) if bot is not None else ""

        message_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "")
        user_id = str(getattr(event, "user_id", "") or get_user_id(event) or "")
        group_id = str(getattr(event, "group_id", "") or get_group_id(event) or "")

        raw_message = str(
            getattr(event, "raw_message", "")
            or getattr(event, "content", "")
            or ""
        )

        content = raw_message
        try:
            msg = event.get_message()
            content = _extract_text_from_message_obj(msg) or raw_message
        except Exception:
            pass

        sender = getattr(event, "sender", None)
        nickname = ""
        username = ""
        avatar = ""

        if sender is not None:
            nickname = str(getattr(sender, "nickname", "") or "")
            username = str(getattr(sender, "card", "") or nickname or "")

        author_username, author_avatar = _get_author_username_avatar(event)
        username = author_username or username
        avatar = author_avatar or avatar

        group_name = str(getattr(event, "group_name", "") or "")

        _insert_message_record(
            adapter=adapter,
            bot_id=bot_id,
            direction="recv",
            scene=scene,
            message_id=message_id,
            group_id=group_id,
            group_name=group_name,
            user_id=user_id,
            username=username,
            nickname=nickname,
            avatar=avatar,
            content=content,
        )
        
        _record_group_user_nickname(
            adapter=adapter,
            bot_id=bot_id,
            scene=scene,
            group_id=group_id,
            user_id=user_id,
            username=username,
        )

        setattr(event, "__message_db_recv_recorded__", True)
    except Exception as e:
        logger.warning(f"[message.db] 记录接收消息失败: {e}")


def _record_send_message(
    bot: Any,
    *,
    scene: str,
    message: Any,
    message_id: str = "",
    source_message_id: str = "",
    group_id: str = "",
    user_id: str = "",
    raw_result: Any = None,
):
    """
    记录发送消息。

    注意：
    - message_id：本次发送出去的消息ID
    - source_message_id：被回复的 recv.message_id
    - 如果 source_message_id 存在，说明这次发送消耗了一次被回复消息的回复次数，
      需要更新对应 recv 记录的 reply_used_count。
    """
    try:
        adapter = _get_adapter_name(bot)
        bot_id = _get_bot_id(bot)
        content = _extract_text_from_message_obj(message)

        _insert_message_record(
            adapter=adapter,
            bot_id=bot_id,
            direction="send",
            scene=scene,
            message_id=str(message_id or ""),
            source_message_id=str(source_message_id or ""),
            group_id=str(group_id or ""),
            user_id=str(user_id or ""),
            username="Bot",
            nickname="Bot",
            content=content,
        )

        if source_message_id:
            _increase_recv_reply_used_count(
                source_message_id=str(source_message_id),
                adapter=adapter,
                bot_id=bot_id,
                scene=scene,
                group_id=str(group_id or ""),
                user_id=str(user_id or ""),
            )

    except Exception as e:
        logger.warning(f"[message.db] 记录发送消息失败: {e}")


# =========================
# 跨适配器 MessageSegment 工具
# =========================
class CompatMessageSegment:
    """跨适配器消息段工厂"""

    @staticmethod
    def _is_url(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        value = value.strip()
        if not value:
            return False
        try:
            parsed = urlparse(value)
            return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
        except Exception:
            return False

    @staticmethod
    def _to_bytes(data: Any) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, BytesIO):
            return data.getvalue()
        if isinstance(data, Path):
            return data.read_bytes()
        raise TypeError(f"不支持的本地文件类型: {type(data)}")

    @staticmethod
    def _is_qq_bot(bot: Any) -> bool:
        return HAS_QQ and QQBot is not None and isinstance(bot, QQBot)

    @staticmethod
    def _is_ob11_bot(bot: Any) -> bool:
        return HAS_OB11 and OB11Bot is not None and isinstance(bot, OB11Bot)

    @staticmethod
    def markdown_param(key: str, value: str) -> dict[str, list[str]]:
        return {"key": key, "values": [value]}

    @staticmethod
    def markdown_template(
        bot: Any,
        md_id: str,
        msg_body: list[dict[str, Any]],
        button_id: str = "",
    ):
        if CompatMessageSegment._is_qq_bot(bot):
            md_seg = QQMessageSegment.markdown(  # type: ignore[union-attr]
                MessageMarkdown(custom_template_id=md_id, params=msg_body)  # type: ignore[misc]
            )
            if button_id:
                kb_seg = QQMessageSegment.keyboard(MessageKeyboard(id=button_id))  # type: ignore[union-attr,misc]
                return QQMessage(md_seg) + kb_seg  # type: ignore[call-arg]
            return md_seg

        if CompatMessageSegment._is_ob11_bot(bot):
            data: dict[str, Any] = {
                "markdown": {"custom_template_id": md_id, "params": msg_body}
            }
            if button_id:
                data["keyboard"] = {"id": button_id}
            return OB11MessageSegment("markdown", {"data": data})

        raise RuntimeError("无法根据 bot 判断适配器类型，markdown_template 构造失败")

    @staticmethod
    def markdown(bot: Any, msg: str, button_id: str = ""):
        if CompatMessageSegment._is_qq_bot(bot):
            md_seg = QQMessageSegment.markdown(MessageMarkdown(content=msg))  # type: ignore[union-attr,misc]
            if button_id:
                kb_seg = QQMessageSegment.keyboard(MessageKeyboard(id=button_id))  # type: ignore[union-attr,misc]
                return QQMessage(md_seg) + kb_seg  # type: ignore[call-arg]
            return md_seg

        if CompatMessageSegment._is_ob11_bot(bot):
            data: dict[str, Any] = {"markdown": {"content": msg}}
            if button_id:
                data["keyboard"] = {"id": button_id}
            return OB11MessageSegment("markdown", {"data": data})

        raise RuntimeError("无法根据 bot 判断适配器类型，markdown 构造失败")

    @staticmethod
    def text(bot_or_text: Any, text: Optional[str] = None):
        if text is None:
            pure_text = str(bot_or_text)
            if HAS_OB11 and OB11MessageSegment is not None:
                return OB11MessageSegment.text(pure_text)
            if HAS_QQ and QQMessageSegment is not None:
                return QQMessageSegment.text(pure_text)
            return pure_text

        bot = bot_or_text
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.text(text)
        if CompatMessageSegment._is_qq_bot(bot):
            return QQMessageSegment.text(text)
        return text

    @staticmethod
    def image(bot: Any, file: Any):
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.image(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_image(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.image(file)  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.image(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.image(file)
            return QQMessageSegment.file_image(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 image 消息段")

    @staticmethod
    def audio(bot: Any, file: Any):
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.audio(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_audio(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.record(file)  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.record(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.audio(file)
            return QQMessageSegment.file_audio(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 audio 消息段")

    @staticmethod
    def video(bot: Any, file: Any):
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.video(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_video(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.video(file)  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.video(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.video(file)
            return QQMessageSegment.file_video(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 video 消息段")

    @staticmethod
    def file(bot: Any, file: Any):
        if CompatMessageSegment._is_qq_bot(bot):
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.file(file)  # type: ignore[union-attr]
            return QQMessageSegment.file_file(CompatMessageSegment._to_bytes(file))  # type: ignore[union-attr]

        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.image(file)  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.image(file)
        if HAS_QQ and QQMessageSegment is not None:
            if CompatMessageSegment._is_url(file):
                return QQMessageSegment.file(file)
            return QQMessageSegment.file_file(CompatMessageSegment._to_bytes(file))
        raise RuntimeError("当前环境未安装可用适配器，无法构造 file 消息段")

    @staticmethod
    def at(bot: Any, user_id: str):
        if CompatMessageSegment._is_ob11_bot(bot):
            return OB11MessageSegment.at(str(user_id))  # type: ignore[union-attr]

        if CompatMessageSegment._is_qq_bot(bot):
            return QQMessageSegment.text("")  # type: ignore[union-attr]

        if HAS_OB11 and OB11MessageSegment is not None:
            return OB11MessageSegment.at(str(user_id))
        if HAS_QQ and QQMessageSegment is not None:
            return QQMessageSegment.text("")
        return ""

    @staticmethod
    async def upload_image_and_get_url(
        bot: BaseBot,
        channel_id: str,
        image: Union[str, Path, bytes, BytesIO],
        *,
        mode: Literal["md5", "link"] = "md5",
        fallback_url: Optional[str] = None,
        audit_timeout: float = 30.0,
    ) -> Optional[str]:
        """
        mode:
        - "md5": 立即返回 md5 链接
        - "link": 等待审核并回查真实附件链接后返回
        """

        try:
            from nonebot.adapters.qq.exception import AuditException
        except Exception:
            AuditException = Exception  # type: ignore

        if not (HAS_QQ and QQBot is not None and isinstance(bot, QQBot)):
            logger.warning("upload_image_and_get_url: 当前 bot 不是 QQBot")
            return fallback_url

        if not channel_id:
            logger.warning("upload_image_and_get_url: channel_id为空，跳过上传")
            return fallback_url

        try:
            if isinstance(image, str):
                image_bytes = Path(image).read_bytes()
            elif isinstance(image, Path):
                image_bytes = image.read_bytes()
            elif isinstance(image, BytesIO):
                image_bytes = image.getvalue()
            elif isinstance(image, bytes):
                image_bytes = image
            else:
                raise TypeError(f"不支持的 image 类型: {type(image)}")
        except Exception as e:
            logger.error(f"upload_image_and_get_url: 读取图片失败: {e}")
            return fallback_url

        md5_hex = hashlib.md5(image_bytes).hexdigest().upper()
        md5_url = f"https://gchat.qpic.cn/qmeetpic/0/0-0-{md5_hex}/0"

        async def _resolve_real_url_from_message(message_id: str) -> Optional[str]:
            msg_obj = await bot.get_message_of_id(
                channel_id=str(channel_id),
                message_id=str(message_id),
            )
            atts = getattr(msg_obj, "attachments", None) or []
            for att in atts:
                url = getattr(att, "url", None)
                if url:
                    return str(url)
            return None

        async def _audit_followup(audit_exc: Exception):
            try:
                audit_id = getattr(audit_exc, "audit_id", None)
                logger.warning(f"upload_image_and_get_url: 触发审核 audit_id={audit_id}，先返回md5链接")
                audit_res = await audit_exc.get_audit_result(timeout=audit_timeout)  # type: ignore[attr-defined]
                event_name = str(getattr(audit_res, "__type__", ""))
                message_id = getattr(audit_res, "message_id", None)
                if "MESSAGE_AUDIT_PASS" not in event_name or not message_id:
                    logger.warning(
                        f"upload_image_and_get_url: 审核未通过或缺少message_id, "
                        f"event={event_name}, message_id={message_id}"
                    )
                    return
                real_url = await _resolve_real_url_from_message(str(message_id))
                if real_url:
                    logger.info(f"upload_image_and_get_url: 审核回查真实链接={real_url}")
            except Exception as ex:
                logger.warning(f"upload_image_and_get_url: 审核后台任务异常: {ex}")

        try:
            result = await bot.post_messages(
                channel_id=str(channel_id),
                content=" ",
                file_image=image_bytes,
            )

            if mode == "link":
                message_id = getattr(result, "id", None)
                if message_id:
                    real_url = await _resolve_real_url_from_message(str(message_id))
                    if real_url:
                        return real_url
                return fallback_url or md5_url

            return md5_url

        except AuditException as e:
            if mode == "md5":
                asyncio.create_task(_audit_followup(e))
                return md5_url

            try:
                audit_res = await e.get_audit_result(timeout=audit_timeout)  # type: ignore[attr-defined]
                event_name = str(getattr(audit_res, "__type__", ""))
                message_id = getattr(audit_res, "message_id", None)

                if "MESSAGE_AUDIT_PASS" not in event_name or not message_id:
                    logger.warning(
                        f"upload_image_and_get_url(link): 审核未通过或缺少message_id, "
                        f"event={event_name}, message_id={message_id}"
                    )
                    return fallback_url

                real_url = await _resolve_real_url_from_message(str(message_id))
                return real_url or fallback_url
            except Exception as ex:
                logger.warning(f"upload_image_and_get_url(link): 等待审核失败: {ex}")
                return fallback_url

        except Exception as e:
            logger.error(f"upload_image_and_get_url: 上传失败: {e}")
            return fallback_url


# =========================
# 对外导出的兼容类型
# =========================
Bot = BaseBot
Message = Union[OB11Message, QQMessage, str]
MessageSegment = CompatMessageSegment

if HAS_QQ:
    GroupMessageEvent = Union[
        OB11GroupMessageEvent,
        QQGroupMessageEvent,
        QQAtChannelMessageEvent,
    ]
else:
    GroupMessageEvent = Union[OB11GroupMessageEvent]

if HAS_QQ:
    PrivateMessageEvent = Union[
        OB11PrivateMessageEvent,
        QQPrivateMessageEvent,
        QQChannelPrivateMessageEvent,
    ]
else:
    PrivateMessageEvent = Union[OB11PrivateMessageEvent]

MessageEvent = Union[GroupMessageEvent, PrivateMessageEvent]


def is_group_event(event: BaseEvent) -> bool:
    types: list[type] = []
    if HAS_OB11:
        types.append(OB11GroupMessageEvent)  # type: ignore[arg-type]
    if HAS_QQ:
        types.append(QQGroupMessageEvent)  # type: ignore[arg-type]
        types.append(QQAtChannelMessageEvent)  # type: ignore[arg-type]
    return isinstance(event, tuple(types)) if types else False


def is_private_event(event: BaseEvent) -> bool:
    types: list[type] = []
    if HAS_OB11:
        types.append(OB11PrivateMessageEvent)  # type: ignore[arg-type]
    if HAS_QQ:
        types.append(QQPrivateMessageEvent)  # type: ignore[arg-type]
        types.append(QQChannelPrivateMessageEvent)  # type: ignore[arg-type]
    return isinstance(event, tuple(types)) if types else False


def is_channel_event(event: BaseEvent) -> bool:
    if HAS_QQ:
        if get_chat_scene(event) in ("channel_group", "channel_private"):
            return True
    return False


def get_chat_scene(event: BaseEvent) -> str:
    """
    获取会话场景：
    - group
    - private
    - channel_group
    - channel_private
    - unknown
    """
    if HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
        return "channel_group"
    if HAS_QQ and isinstance(event, QQChannelPrivateMessageEvent):
        return "channel_private"
    if is_group_event(event):
        return "group"
    if is_private_event(event):
        return "private"
    return "unknown"


_group_seq_cache: dict[str, int] = {}
_c2c_seq_cache: dict[str, int] = {}


def _next_group_seq(group_openid: str) -> int:
    current = _group_seq_cache.get(group_openid)
    if current is None:
        current = random.randint(1000, 900000)

    current += random.randint(1, 3)

    if current <= 0:
        current = 1
    if current > 1_000_000:
        current = random.randint(1, 10000)

    _group_seq_cache[group_openid] = current
    return current


def _next_c2c_seq(user_openid: str) -> int:
    current = _c2c_seq_cache.get(user_openid)
    if current is None:
        current = random.randint(1000, 900000)

    current += random.randint(1, 3)

    if current <= 0:
        current = 1
    if current > 1_000_000:
        current = random.randint(1, 10000)

    _c2c_seq_cache[user_openid] = current
    return current


def _is_msgseq_conflict_error(exc: Exception) -> bool:
    try:
        code = getattr(exc, "retcode", None)
        if code == 40054005:
            return True
    except Exception:
        pass

    s = str(exc)
    return ("40054005" in s) or ("消息被去重" in s) or ("msgseq" in s.lower())


async def _send_with_retry(
    send_coro_factory,
    *,
    get_new_seq=None,
    max_retry: int = 3,
    base_delay: float = 0.05,
):
    last_exc = None

    for i in range(max_retry + 1):
        try:
            kwargs = {}
            if get_new_seq is not None:
                kwargs["msg_seq"] = int(get_new_seq())
            return await send_coro_factory(**kwargs)
        except Exception as e:
            last_exc = e
            if (not _is_msgseq_conflict_error(e)) or i >= max_retry:
                raise
            delay = base_delay * (i + 1) + random.uniform(0.01, 0.08)
            logger.warning(
                f"[QQ发送重试] 检测到msg_seq冲突，准备重试 {i + 1}/{max_retry}，"
                f"等待 {delay:.3f}s，错误: {e}"
            )
            await asyncio.sleep(delay)

    raise last_exc


async def _group_checker(event: BaseEvent) -> bool:
    return is_group_event(event)


GROUP: Permission = Permission(_group_checker)


def get_user_id(event: BaseEvent) -> Optional[str]:
    if hasattr(event, "user_id"):
        uid = getattr(event, "user_id")
        return str(uid) if uid is not None else None
    try:
        return str(event.get_user_id())
    except Exception:
        return None


def get_group_id(event: BaseEvent) -> Optional[str]:
    if hasattr(event, "group_id"):
        gid = getattr(event, "group_id")
        return str(gid) if gid is not None else None
    if hasattr(event, "group_openid"):
        gid = getattr(event, "group_openid")
        return str(gid) if gid is not None else None
    if hasattr(event, "channel_id"):
        gid = getattr(event, "channel_id")
        return str(gid) if gid is not None else None
    return None


def _resolve_sender_name(
    e: BaseEvent,
    fallback_user_id: Optional[str] = None,
) -> str:
    author = getattr(e, "author", None)
    username = getattr(author, "username", None)
    if username:
        return str(username)

    author_id = getattr(author, "id", None)
    if author_id:
        return str(author_id)

    if fallback_user_id:
        return str(fallback_user_id)

    uid = get_user_id(e)
    return str(uid) if uid is not None else ""


def _build_compat_sender(
    *,
    user_id: Optional[str],
    nickname: Optional[str],
    card: Optional[str],
    role: Optional[str] = "member",
) -> CompatSender:
    return CompatSender(
        user_id=str(user_id) if user_id is not None else None,
        nickname=nickname,
        card=card,
        role=role,
    )


def patch_event_inplace(event: BaseEvent) -> BaseEvent:
    if getattr(event, "__compat_patched__", False):
        return event

    if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
        raw = event.content or ""
        sender_id = str(event.author.user_openid)
        sender_name = _resolve_sender_name(event, fallback_user_id=sender_id)

        setattr(event, "message_type", "private")
        setattr(event, "user_id", sender_id)
        setattr(event, "group_id", None)
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=sender_id,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    elif HAS_QQ and isinstance(event, QQChannelPrivateMessageEvent):
        raw = event.content or ""
        uid = getattr(getattr(event, "author", None), "id", None) or get_user_id(event) or ""
        uid = str(uid)
        sender_name = _resolve_sender_name(event, fallback_user_id=uid)

        setattr(event, "message_type", "private")
        setattr(event, "user_id", uid)
        setattr(event, "group_id", None)
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=uid,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    elif HAS_QQ and isinstance(event, QQGroupMessageEvent):
        raw = event.content or ""
        sender_id = str(event.author.member_openid)
        sender_name = _resolve_sender_name(event, fallback_user_id=sender_id)

        setattr(event, "message_type", "group")
        setattr(event, "user_id", sender_id)
        setattr(event, "group_id", str(event.group_openid))
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=sender_id,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    elif HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
        raw = event.content or ""
        uid = getattr(getattr(event, "author", None), "id", None) or get_user_id(event) or ""
        uid = str(uid)
        sender_name = _resolve_sender_name(event, fallback_user_id=uid)

        setattr(event, "message_type", "group")
        setattr(event, "user_id", uid)
        setattr(event, "group_id", str(event.channel_id))
        setattr(event, "message_id", str(event.id))
        setattr(event, "raw_message", raw)
        setattr(event, "message", raw)
        setattr(event, "plaintext", raw)
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=uid,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    if not hasattr(event, "user_id"):
        uid = get_user_id(event)
        if uid is not None:
            setattr(event, "user_id", uid)

    if not hasattr(event, "group_id"):
        gid = get_group_id(event)
        if gid is not None and is_group_event(event):
            setattr(event, "group_id", gid)

    if not hasattr(event, "sender"):
        sender_id = getattr(event, "user_id", None)
        sender_name = _resolve_sender_name(event, fallback_user_id=str(sender_id or ""))
        setattr(
            event,
            "sender",
            _build_compat_sender(
                user_id=sender_id,
                nickname=sender_name,
                card=sender_name,
                role="member",
            ),
        )

    setattr(event, "__compat_patched__", True)
    return event


def _patch_ob11_send_record(bot: BaseBot):
    """给 OneBot V11 尝试添加发送记录包装"""
    if not (HAS_OB11 and OB11Bot is not None and isinstance(bot, OB11Bot)):
        return

    if getattr(bot, "__message_db_ob11_send_patched__", False):
        return

    # 包装 send_group_msg
    if hasattr(bot, "send_group_msg"):
        try:
            _origin_send_group_msg = bot.send_group_msg

            async def send_group_msg(*args, **kwargs):
                result = await _origin_send_group_msg(*args, **kwargs)

                group_id = kwargs.get("group_id", "")
                message = kwargs.get("message", "")

                _record_send_message(
                    bot,
                    scene="group",
                    message=message,
                    message_id=_extract_result_message_id(result),
                    group_id=str(group_id or ""),
                    raw_result=result,
                )
                return result

            setattr(bot, "send_group_msg", send_group_msg)
        except Exception as e:
            logger.debug(f"[message.db] OB11 send_group_msg 包装失败: {e}")

    # 包装 send_private_msg
    if hasattr(bot, "send_private_msg"):
        try:
            _origin_send_private_msg = bot.send_private_msg

            async def send_private_msg(*args, **kwargs):
                result = await _origin_send_private_msg(*args, **kwargs)

                user_id = kwargs.get("user_id", "")
                message = kwargs.get("message", "")

                _record_send_message(
                    bot,
                    scene="private",
                    message=message,
                    message_id=_extract_result_message_id(result),
                    user_id=str(user_id or ""),
                    raw_result=result,
                )
                return result

            setattr(bot, "send_private_msg", send_private_msg)
        except Exception as e:
            logger.debug(f"[message.db] OB11 send_private_msg 包装失败: {e}")

    # 包装 send(event, message)
    if hasattr(bot, "send"):
        try:
            _origin_send = bot.send

            async def send(event, message, **kwargs):
                result = await _origin_send(event=event, message=message, **kwargs)

                try:
                    patched_event = patch_event_inplace(event)
                    scene = get_chat_scene(patched_event)

                    _record_send_message(
                        bot,
                        scene=scene,
                        message=message,
                        message_id=_extract_result_message_id(result),
                        source_message_id=str(getattr(patched_event, "message_id", "") or ""),
                        group_id=str(getattr(patched_event, "group_id", "") or ""),
                        user_id=str(getattr(patched_event, "user_id", "") or ""),
                        raw_result=result,
                    )
                except Exception:
                    pass

                return result

            setattr(bot, "send", send)
        except Exception as e:
            logger.debug(f"[message.db] OB11 send 包装失败: {e}")

    # 包装 call_api 兜底
    if hasattr(bot, "call_api"):
        try:
            _origin_call_api = bot.call_api

            async def call_api(api: str, **data):
                result = await _origin_call_api(api, **data)
                api_lower = str(api).lower()

                try:
                    if api_lower in ("send_group_msg", "send_group_message"):
                        _record_send_message(
                            bot,
                            scene="group",
                            message=data.get("message", ""),
                            message_id=_extract_result_message_id(result),
                            group_id=str(data.get("group_id", "")),
                            raw_result=result,
                        )

                    elif api_lower in ("send_private_msg", "send_private_message"):
                        _record_send_message(
                            bot,
                            scene="private",
                            message=data.get("message", ""),
                            message_id=_extract_result_message_id(result),
                            user_id=str(data.get("user_id", "")),
                            raw_result=result,
                        )
                except Exception:
                    pass

                return result

            setattr(bot, "call_api", call_api)
        except Exception as e:
            logger.debug(f"[message.db] OB11 call_api 包装失败: {e}")

    setattr(bot, "__message_db_ob11_send_patched__", True)


def patch_bot_inplace(bot: BaseBot) -> BaseBot:
    if getattr(bot, "__compat_patched__", False):
        _patch_ob11_send_record(bot)
        return bot

    # OB11 尝试包装发送记录
    _patch_ob11_send_record(bot)

    if HAS_QQ and QQBot is not None and isinstance(bot, QQBot):
        _origin_send = bot.send

        async def send(event, message, **kwargs):
            # ===== 普通 QQ 群 =====
            if HAS_QQ and isinstance(event, QQGroupMessageEvent):
                group_openid = str(event.group_openid)

                async def _do_send(msg_seq: int):
                    return await bot.send_to_group(
                        group_openid=group_openid,
                        message=message,
                        msg_id=str(event.id),
                        msg_seq=int(msg_seq),
                        event_id=kwargs.pop("event_id", None),
                    )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    result = await _do_send(msg_seq)
                else:
                    result = await _send_with_retry(
                        _do_send,
                        get_new_seq=lambda: _next_group_seq(group_openid),
                        max_retry=3,
                    )

                _record_send_message(
                    bot,
                    scene="group",
                    message=message,
                    message_id=_extract_result_message_id(result),
                    source_message_id=str(event.id),
                    group_id=group_openid,
                    raw_result=result,
                )
                return result

            # ===== QQ 私聊 C2C =====
            if HAS_QQ and isinstance(event, QQPrivateMessageEvent):
                user_openid = str(event.author.user_openid)

                async def _do_send(msg_seq: int):
                    return await bot.send_to_c2c(
                        openid=user_openid,
                        message=message,
                        msg_id=str(event.id),
                        msg_seq=int(msg_seq),
                        event_id=kwargs.pop("event_id", None),
                    )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    result = await _do_send(msg_seq)
                else:
                    result = await _send_with_retry(
                        _do_send,
                        get_new_seq=lambda: _next_c2c_seq(user_openid),
                        max_retry=3,
                    )

                _record_send_message(
                    bot,
                    scene="private",
                    message=message,
                    message_id=_extract_result_message_id(result),
                    source_message_id=str(event.id),
                    user_id=user_openid,
                    raw_result=result,
                )
                return result

            # ===== QQ 频道公域消息 =====
            if HAS_QQ and isinstance(event, QQAtChannelMessageEvent):
                channel_id = str(event.channel_id)

                async def _do_send(msg_seq: int):
                    try:
                        return await bot.send_to_channel(
                            channel_id=channel_id,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=kwargs.pop("event_id", None),
                        )
                    except TypeError:
                        return await bot.send_to_channel(
                            channel_id=channel_id,
                            message=message,
                            msg_id=str(event.id),
                            event_id=kwargs.pop("event_id", None),
                        )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    result = await _do_send(msg_seq)
                else:
                    result = await _send_with_retry(
                        _do_send,
                        get_new_seq=lambda: _next_group_seq(channel_id),
                        max_retry=3,
                    )

                _record_send_message(
                    bot,
                    scene="channel_group",
                    message=message,
                    message_id=_extract_result_message_id(result),
                    source_message_id=str(event.id),
                    group_id=channel_id,
                    raw_result=result,
                )
                return result

            # ===== QQ 频道私信 DMS =====
            if HAS_QQ and isinstance(event, QQChannelPrivateMessageEvent):
                guild_id = str(event.guild_id)
                uid = str(getattr(getattr(event, "author", None), "id", "") or "")
                seq_key = f"{guild_id}:{uid}" if uid else guild_id

                async def _do_send(msg_seq: int):
                    try:
                        return await bot.send_to_dms(
                            guild_id=guild_id,
                            message=message,
                            msg_id=str(event.id),
                            msg_seq=int(msg_seq),
                            event_id=kwargs.pop("event_id", None),
                        )
                    except TypeError:
                        return await bot.send_to_dms(
                            guild_id=guild_id,
                            message=message,
                            msg_id=str(event.id),
                            event_id=kwargs.pop("event_id", None),
                        )

                if "msg_seq" in kwargs:
                    msg_seq = int(kwargs.pop("msg_seq"))
                    result = await _do_send(msg_seq)
                else:
                    result = await _send_with_retry(
                        _do_send,
                        get_new_seq=lambda: _next_c2c_seq(seq_key),
                        max_retry=3,
                    )

                _record_send_message(
                    bot,
                    scene="channel_private",
                    message=message,
                    message_id=_extract_result_message_id(result),
                    source_message_id=str(event.id),
                    user_id=uid,
                    group_id=guild_id,
                    raw_result=result,
                )
                return result

            # ===== 兜底 =====
            result = await _origin_send(event=event, message=message, **kwargs)

            _record_send_message(
                bot,
                scene=get_chat_scene(event),
                message=message,
                message_id=_extract_result_message_id(result),
                source_message_id=str(getattr(event, "id", "") or getattr(event, "message_id", "") or ""),
                group_id=str(get_group_id(event) or ""),
                user_id=str(get_user_id(event) or ""),
                raw_result=result,
            )
            return result

        async def send_private_msg(*, user_id, message, **kwargs):
            openid = str(user_id)

            async def _do_send(msg_seq: int):
                return await bot.send_to_c2c(
                    openid=openid,
                    message=message,
                    msg_seq=int(msg_seq),
                    **kwargs,
                )

            if "msg_seq" in kwargs:
                msg_seq = int(kwargs.pop("msg_seq"))
                result = await _do_send(msg_seq)
            else:
                result = await _send_with_retry(
                    _do_send,
                    get_new_seq=lambda: _next_c2c_seq(openid),
                    max_retry=3,
                )

            _record_send_message(
                bot,
                scene="private",
                message=message,
                message_id=_extract_result_message_id(result),
                user_id=openid,
                raw_result=result,
            )
            return result

        async def send_group_msg(*, group_id, message, **kwargs):
            group_openid = str(group_id)

            async def _do_send(msg_seq: int):
                return await bot.send_to_group(
                    group_openid=group_openid,
                    message=message,
                    msg_id=kwargs.pop("msg_id", None),
                    msg_seq=int(msg_seq),
                    event_id=kwargs.pop("event_id", None),
                )

            if "msg_seq" in kwargs:
                msg_seq = int(kwargs.pop("msg_seq"))
                result = await _do_send(msg_seq)
            else:
                result = await _send_with_retry(
                    _do_send,
                    get_new_seq=lambda: _next_group_seq(group_openid),
                    max_retry=3,
                )

            _record_send_message(
                bot,
                scene="group",
                message=message,
                message_id=_extract_result_message_id(result),
                group_id=group_openid,
                raw_result=result,
            )
            return result

        async def delete_msg(*, message_id, group_id=None, user_id=None):
            if group_id is not None:
                return await bot.delete_group_message(
                    group_openid=str(group_id),
                    message_id=str(message_id),
                )

            if user_id is not None:
                return await bot.delete_c2c_message(
                    openid=str(user_id),
                    message_id=str(message_id),
                )

            raise ValueError("QQ delete_msg 需要 group_id 或 user_id")

        setattr(bot, "send", send)
        setattr(bot, "send_private_msg", send_private_msg)
        setattr(bot, "send_group_msg", send_group_msg)
        setattr(bot, "delete_msg", delete_msg)

    setattr(bot, "__compat_patched__", True)
    return bot


def patch_context(bot: BaseBot, event: BaseEvent) -> tuple[BaseBot, BaseEvent]:
    bot = patch_bot_inplace(bot)
    event = patch_event_inplace(event)

    # 收消息入库
    _record_recv_message(bot, event)

    return bot, event

# =========================
# 对 Web 管理面板开放的公共接口
# =========================

def init_message_db():
    """初始化 message.db，供 Web 面板调用"""
    return _init_message_db()


def get_message_db_path() -> Path:
    """获取消息数据库路径"""
    return MESSAGE_DB


def extract_result_message_id(result: Any) -> str:
    """从不同适配器发送结果中提取 message_id"""
    return _extract_result_message_id(result)


def get_bot_id(bot: Any) -> str:
    """安全获取 bot_id/self_id"""
    return _get_bot_id(bot)


def record_web_send_message(
    bot: Any,
    *,
    scene: str,
    message: Any,
    message_id: str = "",
    source_message_id: str = "",
    group_id: str = "",
    user_id: str = "",
    raw_result: Any = None,
):
    """
    Web 面板主动发送消息后记录发送消息。

    会自动：
    - 写入 messages 表 direction='send'
    - 如果 source_message_id 存在，增加对应 recv.reply_used_count
    """
    return _record_send_message(
        bot,
        scene=scene,
        message=message,
        message_id=message_id,
        source_message_id=source_message_id,
        group_id=group_id,
        user_id=user_id,
        raw_result=raw_result,
    )


def increase_recv_reply_used_count(
    *,
    source_message_id: str,
    adapter: str = "",
    bot_id: str = "",
    scene: str = "",
    group_id: str = "",
    user_id: str = "",
):
    """增加被回复 recv 消息的 reply_used_count"""
    return _increase_recv_reply_used_count(
        source_message_id=source_message_id,
        adapter=adapter,
        bot_id=bot_id,
        scene=scene,
        group_id=group_id,
        user_id=user_id,
    )

__all__ = [
    "Bot",
    "GROUP",
    "Message",
    "MessageSegment",
    "CompatSender",
    "GroupMessageEvent",
    "PrivateMessageEvent",
    "MessageEvent",
    "is_group_event",
    "is_private_event",
    "is_channel_event",
    "get_chat_scene",
    "get_user_id",
    "get_group_id",
    "patch_bot_inplace",
    "patch_event_inplace",
    "patch_context",

    # Web 公共接口
    "init_message_db",
    "get_message_db_path",
    "extract_result_message_id",
    "get_bot_id",
    "record_web_send_message",
    "increase_recv_reply_used_count",
]