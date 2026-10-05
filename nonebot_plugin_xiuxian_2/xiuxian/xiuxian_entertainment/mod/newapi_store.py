"""NewAPI 账号与签到历史的兼容适配层。"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from .newapi_client import normalize_base_url
from ....paths import get_paths
from ....features.entertainment.application import EntertainmentApplication
from ....infrastructure.ids import UUIDGenerator

AuthMode = Literal["token", "cookie"]

_MODULE_DATA_DIR = Path(__file__).resolve().parent / "data"
_DATA_DIR = _MODULE_DATA_DIR / "newapi_bindings"

entertainment_application = EntertainmentApplication(get_paths().game_db)
runtime_ids = UUIDGenerator()


def _norm_stored_base(url: str | None) -> str:
    return (url or "").strip().rstrip("/")


def display_base_url(stored: str | None) -> str:
    value = _norm_stored_base(stored)
    return value[:192] if value else "—"


def _path_for_qq(qq_id: str) -> Path:
    safe = "".join(c if c.isalnum() else "_" for c in str(qq_id))
    return _DATA_DIR / f"{safe}.json"


def list_account_summaries(qq_id: str):
    return entertainment_application.list_account_summaries(user_id=str(qq_id))


def resolve_checkin_targets(qq_id: str, selector: str):
    return entertainment_application.resolve_checkin_targets(user_id=str(qq_id), selector=selector)


def resolve_info_targets(qq_id: str, selector: str):
    return entertainment_application.resolve_info_targets(user_id=str(qq_id), selector=selector)


def append_account(
    qq_id: str,
    *,
    mode: AuthMode,
    api_user_id: str,
    secret: str,
    base_url: str,
    label: str = "",
    operation_id: str | None = None,
) -> tuple[bool, str]:
    api_user_id = str(api_user_id).strip()
    secret = (secret or "").strip()
    label = (label or "").strip()
    if not api_user_id.isdigit() or int(api_user_id) <= 0:
        return False, "站点用户 ID 须为正整数"
    if not secret:
        return False, "令牌或 Cookie 不能为空"

    base_stored = normalize_base_url(base_url) if (base_url or "").strip() else ""
    if not base_stored:
        return False, "须填写接口地址（绑定格式：站点用户ID#密钥#接口）"

    op_id = operation_id or (
        f"entertainment:newapi-bind:{qq_id}:{api_user_id}:{base_stored}:{runtime_ids.new_id()}"
    )
    outcome = entertainment_application.bind_newapi_account(
        operation_id=op_id,
        user_id=str(qq_id),
        mode=mode,
        api_user_id=api_user_id,
        secret=secret,
        base_url=base_stored,
        label=label,
    )
    data = dict(outcome.data or {})
    return outcome.ok, str(outcome.message or data.get("message") or "绑定失败")


def delete_accounts(
    qq_id: str,
    indices: list[int] | None,
    *,
    operation_id: str | None = None,
) -> tuple[bool, str]:
    requested = None if indices is None else sorted({int(index) for index in indices})
    op_id = operation_id or f"entertainment:newapi-delete:{qq_id}:{requested or 'all'}:{runtime_ids.new_id()}"
    outcome = entertainment_application.delete_accounts(
        operation_id=op_id,
        user_id=str(qq_id),
        legacy_state_path=str(_path_for_qq(qq_id)),
        indices=requested,
    )
    data = dict(outcome.data or {})
    if outcome.ok:
        if requested is None:
            message = f"已删除全部 {data.get('removed', 0)} 个绑定"
        else:
            message = f"已删除 {data.get('removed', 0)} 个绑定，剩余 {data.get('remaining', 0)} 个"
        return True, str(outcome.message or message)
    return False, str(outcome.message or data.get("message") or "删除失败")


def load_checkin_history(qq_id: str) -> list[dict[str, Any]] | None:
    return entertainment_application.list_checkin_history(user_id=str(qq_id))


def append_checkin_history(
    qq_id: str,
    *,
    account_index: int,
    api_user_id: str,
    base_url: str,
    summary: str,
    source: Literal["manual", "auto"] = "manual",
) -> None:
    entertainment_application.append_checkin_history(
        user_id=str(qq_id),
        account_index=account_index,
        api_user_id=api_user_id,
        base_url=base_url,
        summary=summary,
        source=source,
    )


def toggle_auto_checkin(
    qq_id: str,
    index_text: str,
    *,
    operation_id: str | None = None,
) -> tuple[bool, str]:
    text = (index_text or "").strip()
    if not text:
        return False, "请指定序号，例如：newapi自动签到 1"
    result = resolve_checkin_targets(qq_id, text)
    if result.status != "ok":
        message = result.message or {
            "empty": "尚未绑定账号",
            "missing": "尚未绑定账号",
            "invalid": "绑定数据格式或字段无效，未修改原文件。",
            "unavailable": "暂时无法读取绑定数据，未修改原文件。",
        }.get(result.status, "操作失败")
        return False, message
    if len(result.targets) != 1:
        return False, "请只写一个序号，例如：newapi自动签到 1"

    target = result.targets[0]
    op_id = operation_id or f"entertainment:newapi-auto:{qq_id}:{text}:{runtime_ids.new_id()}"
    outcome = entertainment_application.toggle_auto_checkin(
        operation_id=op_id,
        user_id=str(qq_id),
        account_id=target.account_id,
        index=target.index,
        legacy_state_path=str(_path_for_qq(qq_id)),
    )
    data = dict(outcome.data or {})
    return outcome.ok, str(outcome.message or data.get("message") or "操作失败")


def list_auto_checkin_bindings(
    *, after_account_id: int = 0, limit: int = 32
) -> tuple[tuple[int, str, Any], ...]:
    return entertainment_application.list_auto_checkin_targets(
        after_account_id=after_account_id,
        limit=limit,
    )


__all__ = [
    "append_account",
    "append_checkin_history",
    "delete_accounts",
    "display_base_url",
    "list_account_summaries",
    "list_auto_checkin_bindings",
    "load_checkin_history",
    "resolve_checkin_targets",
    "resolve_info_targets",
    "toggle_auto_checkin",
]
