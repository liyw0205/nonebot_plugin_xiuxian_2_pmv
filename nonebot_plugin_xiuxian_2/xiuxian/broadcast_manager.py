"""广播兼容接口及适配器端口；任务状态由 admin feature 独占。"""

import asyncio
from datetime import datetime

from nonebot.log import logger

from ..features.admin.broadcast_application import AdminBroadcastApplication
from ..features.admin.broadcast_history_repository import AdminBroadcastHistoryRepository
from ..features.admin.broadcast_repository import AdminBroadcastRepository, adapter_family
from ..paths import get_paths
from .xiuxian_config import XiuConfig
from .adapter_compat import (
    Bot,
    MessageSegment,
    get_chat_scene,
    get_group_id,
    get_user_id,
)
from .messaging import SendRequest, delivery_service


def _now() -> datetime:
    return datetime.now()


def _parse_dt(text: str) -> datetime | None:
    if not text:
        return None
    try:
        return datetime.strptime(str(text), "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _format_remaining(task: dict) -> str:
    total_seconds = task.get("remaining_seconds")
    if total_seconds is None:
        expire_at = _parse_dt(str(task.get("expire_at") or ""))
        if expire_at is None:
            return "未知"
        total_seconds = (expire_at - _now()).total_seconds()
    total_seconds = int(total_seconds)
    if total_seconds <= 0:
        return "已过期"
    minutes = total_seconds // 60
    days = minutes // 1440
    hours = minutes % 1440 // 60
    mins = minutes % 60
    parts = []
    if days:
        parts.append(f"{days}天")
    if hours:
        parts.append(f"{hours}小时")
    if mins:
        parts.append(f"{mins}分钟")
    return "".join(parts) if parts else "不足1分钟"


def _get_adapter_name(bot: Bot) -> str:
    try:
        return str(bot.adapter.get_name())
    except Exception:
        return ""


def _get_bot_self_id(bot: Bot) -> str:
    return str(getattr(bot, "self_id", "") or "")


def _is_ob11_adapter(adapter: str) -> bool:
    return adapter_family(adapter) == "ob11"


def _is_qq_adapter(adapter: str) -> bool:
    return str(adapter or "") == "QQ"


def _is_group_scene(scene: str) -> bool:
    return scene in ("group", "channel_group")


def _is_private_scene(scene: str) -> bool:
    return scene in ("private", "channel_private")


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
        return str(get_user_id(event) or getattr(event, "user_id", "") or "")
    return ""


def _get_event_message_id(event) -> str:
    return str(getattr(event, "message_id", "") or getattr(event, "id", "") or "")


async def _history_targets(adapter: str, bot_id: str, kind: str, now: datetime) -> list[dict]:
    repository = AdminBroadcastHistoryRepository(get_paths().message_db)
    # The synchronous query closes its read-only connection before any send starts.
    return await asyncio.to_thread(repository.targets, adapter, bot_id, kind, now)


def _make_broadcast_message(bot: Bot, task: dict):
    content = task["content"]
    if task.get("markdown") and _is_qq_adapter(task.get("adapter", "")):
        return MessageSegment.markdown(bot, content)
    return content


async def _send_ob11_broadcast(bot: Bot, task: dict, scene: str, target_id: str):
    if not _is_group_scene(scene) and not _is_private_scene(scene):
        raise ValueError("unsupported broadcast scene")
    if not task.get("markdown"):
        delivery_scene = "group" if _is_group_scene(scene) else "private"
        return await delivery_service.send(
            bot, SendRequest(delivery_scene, str(target_id), task["content"])
        )

    messages = [{
        "type": "node",
        "data": {
            "name": "系统广播",
            "uin": str(getattr(bot, "self_id", "") or "10000"),
            "content": task["content"] or " ",
        },
    }]
    target = int(target_id) if str(target_id).isdigit() else target_id
    if _is_group_scene(scene):
        await bot.call_api("send_group_forward_msg", group_id=target, messages=messages)
    else:
        await bot.call_api("send_private_forward_msg", user_id=target, messages=messages)
    return {"status": "sent"}


async def _send_qq_broadcast_by_reply(
    bot: Bot,
    task: dict,
    scene: str,
    target_id: str,
    source_message_id: str,
):
    if not source_message_id:
        raise ValueError("missing broadcast reply message id")
    if not _is_group_scene(scene) and not _is_private_scene(scene):
        raise ValueError("unsupported broadcast scene")
    return await delivery_service.send(
        bot,
        SendRequest(
            scene,
            str(target_id),
            _make_broadcast_message(bot, task),
            source_message_id=str(source_message_id),
        ),
    )


async def _send_broadcast_to_target(
    bot: Bot,
    task: dict,
    scene: str,
    target_id: str,
    source_message_id: str = "",
):
    try:
        if _is_qq_adapter(task["adapter"]):
            return await _send_qq_broadcast_by_reply(
                bot, task, scene, target_id, source_message_id
            )
        if _is_ob11_adapter(task["adapter"]):
            return await _send_ob11_broadcast(bot, task, scene, target_id)
        raise ValueError("unsupported broadcast adapter")
    except Exception as exc:
        logger.warning("broadcast delivery failed: {}", type(exc).__name__)
        raise


_broadcast_repository = AdminBroadcastRepository()
_broadcast_application = AdminBroadcastApplication(
    _broadcast_repository, history=_history_targets, sender=_send_broadcast_to_target
)


def _application() -> AdminBroadcastApplication:
    return _broadcast_application


async def start_broadcast(
    bot: Bot,
    kind: str,
    content: str,
    duration_minutes: int = 1440,
) -> str:
    adapter = _get_adapter_name(bot)
    bot_id = _get_bot_self_id(bot)
    try:
        result = await _application().start(
            bot,
            adapter=adapter,
            bot_id=bot_id,
            kind=kind,
            content=content,
            duration_minutes=duration_minutes,
            markdown=bool(XiuConfig().markdown_status),
        )
    except Exception as exc:
        logger.warning("broadcast creation failed: {}", type(exc).__name__)
        raise RuntimeError("广播创建失败，请检查服务日志。") from None

    status = result["status"]
    invalid_messages = {
        "invalid_content": "广播内容不能为空。",
        "invalid_kind": "广播类型错误。",
        "invalid_adapter": "当前适配器不支持广播。",
        "invalid_identity": "广播缺少适配器或机器人标识。",
        "invalid_duration": "广播时间无效或超出范围。",
    }
    if status in invalid_messages:
        raise ValueError(invalid_messages[status])
    if status not in {"created", "cancelled", "stopped"}:
        logger.warning("broadcast creation failed: {}", result.get("error_type", "RuntimeError"))
        raise RuntimeError("广播创建失败，请检查消息历史与服务状态。")

    task = result.get("task")
    if status == "cancelled":
        mode_tip = "广播已取消，首轮发送已停止。"
    elif status == "stopped":
        mode_tip = "广播已清空或过期，首轮发送已停止。"
    elif _is_qq_adapter(adapter):
        mode_tip = "QQ 广播已创建：仅处理最近 1 分钟内活跃目标，后续新消息会自动补发。"
    else:
        mode_tip = "OB11 广播已创建：已根据历史消息处理首轮发送，后续新目标会自动补发。"
    lines = [mode_tip, f"广播ID：{result['id']}"]
    if task is not None:
        lines.extend([
            f"类型：{task['kind']}",
            f"适配器：{task['adapter']}",
            f"Markdown：{'开启' if task['markdown'] else '关闭'}",
            f"有效时长：{task['duration_minutes']}分钟",
            f"过期时间：{task['expire_at']}",
        ])
    lines.extend([
        f"本轮成功发送：{result['success_count']}",
        f"本轮待审核：{result['pending_count']}",
        f"本轮发送失败：{result['failed_count']}",
    ])
    if task is not None:
        lines.extend([
            f"已发群：{len(task['sent_groups'])}",
            f"已发用户：{len(task['sent_users'])}",
        ])
    return "\n".join(lines)


def format_broadcast_status() -> str:
    tasks = _application().status()
    if not tasks:
        return "当前没有广播。"
    lines = ["【当前广播列表】"]
    for task in tasks:
        state = "已取消" if task.get("canceled") else "进行中"
        preview = str(task.get("content") or "").replace("\n", " ").replace("\r", " ")
        if len(preview) > 40:
            preview = preview[:40] + "..."
        lines.append(
            f"\nID：{task['id']}\n"
            f"状态：{state}\n"
            f"类型：{task.get('kind')}\n"
            f"适配器：{task.get('adapter')}\n"
            f"创建时间：{task.get('created_at')}\n"
            f"有效时长：{task.get('duration_minutes', '未知')}分钟\n"
            f"过期时间：{task.get('expire_at', '未知')}\n"
            f"剩余时间：{_format_remaining(task)}\n"
            f"Markdown：{'开启' if task.get('markdown') else '关闭'}\n"
            f"已发群：{len(task['sent_groups'])} / 已发现群：{len(task['known_groups'])}\n"
            f"已发用户：{len(task['sent_users'])} / 已发现用户：{len(task['known_users'])}\n"
            f"待审核：{len(task['pending_groups']) + len(task['pending_users'])}\n"
            f"发送中：{len(task['inflight_groups']) + len(task['inflight_users'])}\n"
            f"错误数：{task['error_count']}\n"
            f"内容预览：{preview}"
        )
    return "\n".join(lines)


def _inflight_note(result: dict) -> str:
    count = int(result.get("inflight_count", 0))
    return f"\n有 {count} 个发送请求已发起，无法撤回。" if count else ""


def cancel_broadcast(bid: str) -> str:
    result = _application().cancel(bid)
    if result["status"] == "missing_id":
        return "请提供广播ID，例如：取消广播 BC12345678"
    if result["status"] == "not_found":
        return f"广播ID不存在：{result['id']}"
    if result["status"] != "cancelled":
        raise RuntimeError("取消广播失败，请检查服务状态。")
    return f"已取消广播：{result['id']}" + _inflight_note(result)


def clear_broadcast(kind: str | None = None) -> str:
    result = _application().clear(kind)
    if result["status"] == "invalid_kind":
        return f"广播类型错误：{result['kind']}"
    if result["status"] == "empty":
        return "当前没有广播可清空。"
    if result["status"] != "cleared":
        raise RuntimeError("清空广播失败，请检查服务状态。")
    kind_name = {"group": "群聊", "private": "私聊", "global": "全局"}.get(result["kind"], "全部")
    return f"已清空{kind_name}广播任务，共 {result['count']} 个。" + _inflight_note(result)


async def auto_patch_broadcast_for_event(bot: Bot, event):
    adapter = _get_adapter_name(bot)
    bot_id = _get_bot_self_id(bot)
    scene = get_chat_scene(event)
    if not _is_group_scene(scene) and not _is_private_scene(scene):
        return
    target_id = _get_event_target(scene, event)
    if not target_id:
        return
    try:
        await _application().patch_event(
            bot,
            adapter=adapter,
            bot_id=bot_id,
            scene=scene,
            target_id=target_id,
            source_message_id=_get_event_message_id(event),
        )
    except Exception as exc:
        logger.warning("broadcast event patch failed: {}", type(exc).__name__)
