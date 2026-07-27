"""全局小黑屋名单（不依赖是否注册修仙）。

与指令禁用类似：落盘 JSON，on_compat 在路由阶段统一拦截。
若该用户已有修仙档案，会同步 user_xiuxian.is_ban 字段。
"""

from __future__ import annotations

from typing import Any

from nonebot.log import logger

from ..paths import get_paths
from .xiuxian_utils.json_store import load_json_file, save_json_file

BLACKHOUSE_FILE = get_paths().data / "blackhouse.json"

# user_id -> {reason, name, updated_at}
_BANNED: dict[str, dict[str, Any]] = {}


def _normalize_user_id(user_id: str | None) -> str:
    return str(user_id or "").strip()


def load_blackhouse_memory() -> dict[str, dict[str, Any]]:
    global _BANNED
    if not BLACKHOUSE_FILE.exists():
        _BANNED = {}
        return _BANNED
    raw = load_json_file(BLACKHOUSE_FILE, {}, dict)
    users = raw.get("users", raw) if isinstance(raw, dict) else {}
    if not isinstance(users, dict):
        _BANNED = {}
        return _BANNED
    cleaned: dict[str, dict[str, Any]] = {}
    for key, value in users.items():
        uid = _normalize_user_id(key)
        if not uid:
            continue
        if isinstance(value, dict):
            cleaned[uid] = {
                "reason": str(value.get("reason") or ""),
                "name": str(value.get("name") or ""),
                "updated_at": str(value.get("updated_at") or ""),
            }
        else:
            cleaned[uid] = {"reason": "", "name": "", "updated_at": ""}
    _BANNED = cleaned
    return _BANNED


def save_blackhouse_memory() -> None:
    payload = {
        "users": {
            uid: {
                "reason": str(info.get("reason") or ""),
                "name": str(info.get("name") or ""),
                "updated_at": str(info.get("updated_at") or ""),
            }
            for uid, info in sorted(_BANNED.items())
        }
    }
    save_json_file(BLACKHOUSE_FILE, payload, indent=2)


def is_user_blackhoused(user_id: str | None) -> bool:
    uid = _normalize_user_id(user_id)
    if not uid:
        return False
    if not _BANNED and BLACKHOUSE_FILE.exists():
        load_blackhouse_memory()
    return uid in _BANNED


def list_blackhoused_users() -> list[dict[str, str]]:
    if not _BANNED and BLACKHOUSE_FILE.exists():
        load_blackhouse_memory()
    rows = []
    for uid, info in sorted(_BANNED.items()):
        rows.append(
            {
                "user_id": uid,
                "name": str(info.get("name") or uid),
                "reason": str(info.get("reason") or ""),
            }
        )
    return rows


def ban_user(user_id: str, *, name: str = "", reason: str = "") -> str:
    """加入小黑屋。返回：banned / unchanged / invalid"""
    from datetime import datetime

    uid = _normalize_user_id(user_id)
    if not uid:
        return "invalid"
    if not _BANNED and BLACKHOUSE_FILE.exists():
        load_blackhouse_memory()
    if uid in _BANNED:
        # 允许补全名字
        if name and not _BANNED[uid].get("name"):
            _BANNED[uid]["name"] = name
            save_blackhouse_memory()
        return "unchanged"
    _BANNED[uid] = {
        "name": str(name or ""),
        "reason": str(reason or ""),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    save_blackhouse_memory()
    _sync_user_xiuxian_ban(uid, True)
    return "banned"


def unban_user(user_id: str) -> str:
    """移出小黑屋。返回：unbanned / unchanged / invalid"""
    uid = _normalize_user_id(user_id)
    if not uid:
        return "invalid"
    if not _BANNED and BLACKHOUSE_FILE.exists():
        load_blackhouse_memory()
    if uid not in _BANNED:
        _sync_user_xiuxian_ban(uid, False)
        return "unchanged"
    _BANNED.pop(uid, None)
    save_blackhouse_memory()
    _sync_user_xiuxian_ban(uid, False)
    return "unbanned"


def _sync_user_xiuxian_ban(user_id: str, banned: bool) -> None:
    """有修仙档案时同步 is_ban；无档案则忽略。"""
    try:
        from .xiuxian_utils.xiuxian2_handle import sql_message

        if banned:
            sql_message.ban_user(user_id)
        else:
            sql_message.unban_user(user_id)
    except Exception as e:  # noqa: BLE001
        logger.debug(f"blackhouse sync is_ban skipped for {user_id}: {e}")


def bootstrap_from_user_xiuxian() -> int:
    """启动时把库内 is_ban=1 合并进全局名单。"""
    if not _BANNED and BLACKHOUSE_FILE.exists():
        load_blackhouse_memory()
    added = 0
    try:
        from .xiuxian_utils.xiuxian2_handle import sql_message

        cur = sql_message.conn.cursor()
        cur.execute("SELECT user_id, user_name FROM user_xiuxian WHERE COALESCE(is_ban,0)=1")
        rows = cur.fetchall() or []
        for row in rows:
            uid = _normalize_user_id(row[0] if not isinstance(row, dict) else row.get("user_id"))
            name = ""
            if isinstance(row, dict):
                name = str(row.get("user_name") or "")
            elif len(row) > 1:
                name = str(row[1] or "")
            if uid and uid not in _BANNED:
                _BANNED[uid] = {"name": name, "reason": "legacy_is_ban", "updated_at": ""}
                added += 1
        if added:
            save_blackhouse_memory()
    except Exception as e:  # noqa: BLE001
        logger.warning(f"blackhouse bootstrap from user_xiuxian failed: {e}")
    return added
