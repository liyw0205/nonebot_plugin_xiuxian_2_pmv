"""全局小黑屋兼容入口，名单和封禁状态统一由 admin SQL 仓储管理。"""

from __future__ import annotations

from collections.abc import Iterable
from contextvars import ContextVar
from uuid import uuid4

from nonebot.log import logger

from ..features.admin.application import AdminApplication
from ..paths import get_paths


_PRIMED_STATUS: ContextVar[dict[str, bool] | None] = ContextVar(
    "xiuxian_primed_blackhouse_status", default=None,
)


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


def is_users_blackhoused(user_ids: Iterable[str | None]) -> dict[str, bool]:
    normalized = tuple(dict.fromkeys(
        _normalize_user_id(user_id) for user_id in user_ids
        if _normalize_user_id(user_id)
    ))
    if not normalized:
        return {}
    cache = _PRIMED_STATUS.get() or {}
    known = {user_id: cache[user_id] for user_id in normalized if user_id in cache}
    if known:
        _PRIMED_STATUS.set({key: value for key, value in cache.items() if key not in known})
    missing = tuple(user_id for user_id in normalized if user_id not in known)
    if not missing:
        return known
    try:
        known.update(_application().blackhouse_statuses(missing))
        return {user_id: bool(known.get(user_id, True)) for user_id in normalized}
    except Exception as exc:
        logger.warning("blackhouse batch lookup failed closed: {}", type(exc).__name__)
        return {user_id: True for user_id in normalized}


def prime_user_blackhouse_status(user_id: str | None) -> bool:
    normalized = _normalize_user_id(user_id)
    if not normalized:
        return False
    status = is_user_blackhoused(normalized)
    cache = dict(_PRIMED_STATUS.get() or {})
    cache[normalized] = status
    _PRIMED_STATUS.set(cache)
    return status


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
