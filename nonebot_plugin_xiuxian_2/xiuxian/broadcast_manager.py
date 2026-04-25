# broadcast_manager.py
# -*- coding: utf-8 -*-

import random
import sqlite3
import uuid
from pathlib import Path
from datetime import datetime, timedelta

from nonebot.log import logger

from .xiuxian_config import XiuConfig
from .adapter_compat import (
    Bot,
    MessageSegment,
    get_chat_scene,
    get_group_id,
    get_user_id,
    get_message_db_path,
)

BROADCAST_TASKS: dict[str, dict] = {}


def _new_broadcast_id() -> str:
    return "BC" + uuid.uuid4().hex[:8].upper()


def _get_adapter_name(bot: Bot) -> str:
    try:
        return str(bot.adapter.get_name())
    except Exception:
        return ""


def _get_bot_self_id(bot: Bot) -> str:
    return str(getattr(bot, "self_id", "") or "")


def _is_ob11_adapter(adapter: str) -> bool:
    low = str(adapter or "").lower()
    return "onebot" in low or "ob11" in low or "v11" in low


def _is_qq_adapter(adapter: str) -> bool:
    return str(adapter or "") == "QQ"


def _target_key(scene: str, target_id: str) -> str:
    return f"{scene}:{target_id}"


def _is_group_scene(scene: str) -> bool:
    return scene in ("group", "channel_group")


def _is_private_scene(scene: str) -> bool:
    return scene in ("private", "channel_private")


def _broadcast_accept_scene(kind: str, scene: str) -> bool:
    if kind == "group":
        return _is_group_scene(scene)

    if kind == "private":
        return _is_private_scene(scene)

    if kind == "global":
        return _is_group_scene(scene) or _is_private_scene(scene)

    return False


def _get_event_target(scene: str, event) -> str:
    if _is_group_scene(scene):
        return str(
            get_group_id(event)
            or getattr(event, "group_id", "")
            or getattr(event, "group_openid", "")
            or getattr(event, "channel_id", "")
            or ""
        )

    if _is_private_scene(scene):
        return str(
            get_user_id(event)
            or getattr(event, "user_id", "")
            or ""
        )

    return ""


def _get_event_message_id(event) -> str:
    return str(
        getattr(event, "message_id", "")
        or getattr(event, "id", "")
        or ""
    )


def _mark_broadcast_sent(task: dict, scene: str, target_id: str):
    key = _target_key(scene, target_id)

    if _is_group_scene(scene):
        task["sent_groups"].add(key)

    elif _is_private_scene(scene):
        task["sent_users"].add(key)


def _is_broadcast_sent(task: dict, scene: str, target_id: str) -> bool:
    key = _target_key(scene, target_id)

    if _is_group_scene(scene):
        return key in task["sent_groups"]

    if _is_private_scene(scene):
        return key in task["sent_users"]

    return True


def _remember_broadcast_target(task: dict, scene: str, target_id: str):
    key = _target_key(scene, target_id)

    if _is_group_scene(scene):
        task["known_groups"].add(key)

    elif _is_private_scene(scene):
        task["known_users"].add(key)


def _query_message_db_rows(sql: str, params: tuple = ()) -> list[dict]:
    db_path = get_message_db_path()

    if not Path(db_path).exists():
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]

    finally:
        conn.close()


def _make_broadcast_message(bot: Bot, task: dict):
    content = task["content"]

    # QQ 适配器：开启 Markdown 时走原生 Markdown
    if task.get("markdown") and _is_qq_adapter(task.get("adapter", "")):
        return MessageSegment.markdown(bot, content)

    return content


async def _send_ob11_broadcast(bot: Bot, task: dict, scene: str, target_id: str):
    """
    OB11 主动发送。
    markdown_status=True 时使用单节点合并转发。
    """
    content = task["content"]
    use_markdown = bool(task.get("markdown"))

    def maybe_int(x: str):
        return int(x) if str(x).isdigit() else x

    if use_markdown:
        node_uin = str(getattr(bot, "self_id", "") or "10000")
        messages = [
            {
                "type": "node",
                "data": {
                    "name": "系统广播",
                    "uin": node_uin,
                    "content": content or " ",
                },
            }
        ]

        if _is_group_scene(scene):
            await bot.call_api(
                "send_group_forward_msg",
                group_id=maybe_int(target_id),
                messages=messages,
            )

        elif _is_private_scene(scene):
            await bot.call_api(
                "send_private_forward_msg",
                user_id=maybe_int(target_id),
                messages=messages,
            )

        else:
            raise RuntimeError(f"OB11 不支持 scene={scene}")

    else:
        if _is_group_scene(scene):
            await bot.send_group_msg(
                group_id=maybe_int(target_id),
                message=content,
            )

        elif _is_private_scene(scene):
            await bot.send_private_msg(
                user_id=maybe_int(target_id),
                message=content,
            )

        else:
            raise RuntimeError(f"OB11 不支持 scene={scene}")


async def _send_qq_broadcast_by_reply(
    bot: Bot,
    task: dict,
    scene: str,
    target_id: str,
    source_message_id: str,
):
    """
    QQ 官方适配器不能纯主动发，因此用当前消息 id 作为 msg_id 回复式发送。
    """
    if not source_message_id:
        raise RuntimeError("QQ 广播缺少可回复 message_id")

    message = _make_broadcast_message(bot, task)

    if scene == "group":
        await bot.send_to_group(
            group_openid=str(target_id),
            message=message,
            msg_id=str(source_message_id),
            msg_seq=random.randint(1, 900000),
        )

    elif scene == "private":
        await bot.send_to_c2c(
            openid=str(target_id),
            message=message,
            msg_id=str(source_message_id),
            msg_seq=random.randint(1, 900000),
        )

    elif scene == "channel_group":
        await bot.send_to_channel(
            channel_id=str(target_id),
            message=message,
            msg_id=str(source_message_id),
        )

    elif scene == "channel_private":
        await bot.send_to_dms(
            guild_id=str(target_id),
            message=message,
            msg_id=str(source_message_id),
        )

    else:
        raise RuntimeError(f"QQ 不支持 scene={scene}")


async def _send_broadcast_to_target(
    bot: Bot,
    task: dict,
    scene: str,
    target_id: str,
    source_message_id: str = "",
):
    if not target_id:
        return False, "target_id为空"

    if not _broadcast_accept_scene(task["kind"], scene):
        return False, "scene不匹配广播类型"

    if _is_broadcast_sent(task, scene, target_id):
        return False, "已发送过"

    adapter = task["adapter"]

    try:
        _remember_broadcast_target(task, scene, target_id)

        if _is_qq_adapter(adapter):
            await _send_qq_broadcast_by_reply(
                bot=bot,
                task=task,
                scene=scene,
                target_id=target_id,
                source_message_id=source_message_id,
            )

        elif _is_ob11_adapter(adapter):
            await _send_ob11_broadcast(
                bot=bot,
                task=task,
                scene=scene,
                target_id=target_id,
            )

        else:
            return False, f"暂不支持适配器: {adapter}"

        _mark_broadcast_sent(task, scene, target_id)
        return True, "ok"

    except Exception as e:
        task["errors"].append(
            {
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "scene": scene,
                "target_id": target_id,
                "error": str(e),
            }
        )
        logger.warning(
            f"[广播] 发送失败 id={task['id']} scene={scene} target={target_id}: {e}"
        )
        return False, str(e)


def _get_qq_recent_targets(adapter: str, kind: str) -> list[dict]:
    """
    QQ 初始广播：
    只读取最近 1 分钟内有 recv 消息且 message_id 不为空的目标。
    """
    since = (datetime.now() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")

    scenes = []

    if kind in ("group", "global"):
        scenes.extend(["group", "channel_group"])

    if kind in ("private", "global"):
        scenes.extend(["private", "channel_private"])

    if not scenes:
        return []

    placeholders = ",".join(["?"] * len(scenes))

    rows = _query_message_db_rows(
        f"""
        SELECT *
        FROM messages
        WHERE adapter = ?
          AND direction = 'recv'
          AND scene IN ({placeholders})
          AND created_at >= ?
          AND message_id IS NOT NULL
          AND message_id != ''
        ORDER BY created_at DESC, id DESC
        """,
        tuple([adapter] + scenes + [since]),
    )

    result = []
    seen = set()

    for r in rows:
        scene = str(r.get("scene") or "")

        if _is_group_scene(scene):
            target_id = str(r.get("group_id") or "")
        else:
            target_id = str(r.get("user_id") or "")

        if not target_id:
            continue

        key = _target_key(scene, target_id)

        if key in seen:
            continue

        seen.add(key)

        result.append(
            {
                "scene": scene,
                "target_id": target_id,
                "message_id": str(r.get("message_id") or ""),
            }
        )

    return result


def _get_ob11_history_targets(adapter: str, kind: str) -> list[dict]:
    """
    OB11 初始广播：
    从历史 message.db 中提取群 ID / 用户 ID。
    """
    targets = []

    if kind in ("group", "global"):
        rows = _query_message_db_rows(
            """
            SELECT scene, group_id AS target_id, MAX(id) AS latest_id
            FROM messages
            WHERE adapter = ?
              AND scene IN ('group', 'channel_group')
              AND group_id IS NOT NULL
              AND group_id != ''
            GROUP BY scene, group_id
            ORDER BY latest_id DESC
            """,
            (adapter,),
        )

        targets.extend(
            [
                {
                    "scene": str(r["scene"]),
                    "target_id": str(r["target_id"]),
                    "message_id": "",
                }
                for r in rows
            ]
        )

    if kind in ("private", "global"):
        rows = _query_message_db_rows(
            """
            SELECT scene, user_id AS target_id, MAX(id) AS latest_id
            FROM messages
            WHERE adapter = ?
              AND scene IN ('private', 'channel_private')
              AND user_id IS NOT NULL
              AND user_id != ''
            GROUP BY scene, user_id
            ORDER BY latest_id DESC
            """,
            (adapter,),
        )

        targets.extend(
            [
                {
                    "scene": str(r["scene"]),
                    "target_id": str(r["target_id"]),
                    "message_id": "",
                }
                for r in rows
            ]
        )

    return targets


async def start_broadcast(bot: Bot, kind: str, content: str) -> str:
    """
    创建广播任务并进行首轮发送。
    kind:
    - group
    - private
    - global
    """
    content = str(content or "").strip()

    if not content:
        return "广播内容不能为空。"

    if kind not in ("group", "private", "global"):
        return f"广播类型错误：{kind}"

    adapter = _get_adapter_name(bot)
    bot_id = _get_bot_self_id(bot)
    bid = _new_broadcast_id()

    task = {
        "id": bid,
        "kind": kind,
        "adapter": adapter,
        "bot_id": bot_id,
        "content": content,
        "markdown": bool(XiuConfig().markdown_status),
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "canceled": False,

        "sent_groups": set(),
        "sent_users": set(),

        "known_groups": set(),
        "known_users": set(),

        "errors": [],
    }

    BROADCAST_TASKS[bid] = task

    if _is_qq_adapter(adapter):
        targets = _get_qq_recent_targets(adapter, kind)

    elif _is_ob11_adapter(adapter):
        targets = _get_ob11_history_targets(adapter, kind)

    else:
        targets = []

    success_count = 0

    for t in targets:
        ok, _ = await _send_broadcast_to_target(
            bot=bot,
            task=task,
            scene=t["scene"],
            target_id=t["target_id"],
            source_message_id=t.get("message_id", ""),
        )

        if ok:
            success_count += 1

    if _is_qq_adapter(adapter):
        mode_tip = "QQ 广播已创建：已向最近 1 分钟内活跃目标发送，后续新消息会自动补发。"

    elif _is_ob11_adapter(adapter):
        mode_tip = "OB11 广播已创建：已根据历史消息主动发送，后续新目标会自动补发。"

    else:
        mode_tip = f"广播已创建，但当前适配器暂不支持自动发送：{adapter}"

    return (
        f"{mode_tip}\n"
        f"广播ID：{bid}\n"
        f"类型：{kind}\n"
        f"适配器：{adapter}\n"
        f"Markdown：{'开启' if task['markdown'] else '关闭'}\n"
        f"本轮成功发送：{success_count}\n"
        f"已发群：{len(task['sent_groups'])}\n"
        f"已发用户：{len(task['sent_users'])}"
    )


def format_broadcast_status() -> str:
    if not BROADCAST_TASKS:
        return "当前没有广播。"

    lines = ["【当前广播列表】"]

    for bid, task in BROADCAST_TASKS.items():
        state = "已取消" if task.get("canceled") else "进行中"

        preview = str(task.get("content") or "").replace("\n", " ").replace("\r", " ")

        if len(preview) > 40:
            preview = preview[:40] + "..."

        lines.append(
            f"\nID：{bid}\n"
            f"状态：{state}\n"
            f"类型：{task.get('kind')}\n"
            f"适配器：{task.get('adapter')}\n"
            f"创建时间：{task.get('created_at')}\n"
            f"Markdown：{'开启' if task.get('markdown') else '关闭'}\n"
            f"已发群：{len(task.get('sent_groups', set()))} / 已发现群：{len(task.get('known_groups', set()))}\n"
            f"已发用户：{len(task.get('sent_users', set()))} / 已发现用户：{len(task.get('known_users', set()))}\n"
            f"错误数：{len(task.get('errors', []))}\n"
            f"内容预览：{preview}"
        )

    return "\n".join(lines)


def cancel_broadcast(bid: str) -> str:
    bid = str(bid or "").strip().upper()

    if not bid:
        return "请提供广播ID，例如：取消广播 BC12345678"

    task = BROADCAST_TASKS.get(bid)

    if not task:
        return f"广播ID不存在：{bid}"

    task["canceled"] = True
    return f"已取消广播：{bid}"


async def auto_patch_broadcast_for_event(bot: Bot, event):
    """
    普通消息触发补发。
    QQ：
    - 使用当前消息 message_id/id 作为 msg_id 回复式发送。

    OB11：
    - 直接主动发送。
    """
    if not BROADCAST_TASKS:
        return

    adapter = _get_adapter_name(bot)
    bot_id = _get_bot_self_id(bot)

    scene = get_chat_scene(event)
    target_id = _get_event_target(scene, event)
    source_message_id = _get_event_message_id(event)

    logger.debug(
        f"[广播补发检查] adapter={adapter}, scene={scene}, "
        f"target_id={target_id}, source_message_id={source_message_id}, "
        f"tasks={len(BROADCAST_TASKS)}"
    )

    if not scene or scene == "unknown":
        return

    if not target_id:
        return

    for task in list(BROADCAST_TASKS.values()):
        if task.get("canceled"):
            continue

        if task.get("adapter") != adapter:
            continue

        # 防止多 Bot 重复补发。
        # 如果你希望不同 bot 都能接力补发，可以删除这一段。
        if task.get("bot_id") and bot_id and task.get("bot_id") != bot_id:
            continue

        if not _broadcast_accept_scene(task["kind"], scene):
            continue

        if _is_broadcast_sent(task, scene, target_id):
            continue

        await _send_broadcast_to_target(
            bot=bot,
            task=task,
            scene=scene,
            target_id=target_id,
            source_message_id=source_message_id,
        )