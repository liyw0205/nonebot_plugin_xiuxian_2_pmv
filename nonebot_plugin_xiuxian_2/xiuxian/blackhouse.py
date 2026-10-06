"""全局小黑屋兼容入口，名单和封禁状态统一由 admin SQL 仓储管理。"""

from __future__ import annotations

from uuid import uuid4

from nonebot.log import logger

from ..features.admin.application import AdminApplication
from ..paths import get_paths


def _application() -> AdminApplication:
    return AdminApplication(get_paths().game_db)


def _normalize_user_id(user_id: str | None) -> str:
    return str(user_id or "").strip()


def is_user_blackhoused(user_id: str | None) -> bool:
    uid = _normalize_user_id(user_id)
    if not uid:
        return False
    try:
        return _application().is_user_blackhoused(uid)
    except Exception as exc:
        logger.warning("blackhouse lookup failed closed: {}", type(exc).__name__)
        return True


def list_blackhoused_users() -> list[dict[str, str]]:
    return _application().list_blackhoused_users()


def ban_user(user_id: str, *, name: str = "", reason: str = "") -> str:
    """返回 banned / unchanged 或仓储失败状态；存储异常向调用方传播。"""
    uid = _normalize_user_id(user_id)
    if not uid:
        return "invalid"
    application = _application()
    expected_banned = application.blackhouse_snapshot(uid)
    result = application.set_blackhouse_status(
        f"blackhouse-compat:{uuid4().hex}",
        "legacy.blackhouse",
        uid,
        expected_banned,
        True,
        name=name,
        reason=reason,
    )
    return "banned" if result.status == "changed" else result.status


def unban_user(user_id: str) -> str:
    """返回 unbanned / unchanged 或仓储失败状态；存储异常向调用方传播。"""
    uid = _normalize_user_id(user_id)
    if not uid:
        return "invalid"
    application = _application()
    expected_banned = application.blackhouse_snapshot(uid)
    result = application.set_blackhouse_status(
        f"blackhouse-compat:{uuid4().hex}",
        "legacy.blackhouse",
        uid,
        expected_banned,
        False,
    )
    return "unbanned" if result.status == "changed" else result.status
