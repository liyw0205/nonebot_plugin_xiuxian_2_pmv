"""NewAPI 绑定、签到、信息、查看、删除。"""
from __future__ import annotations

import asyncio
import re
import time
from typing import Any, Literal

from ..command import *
from ..io_runtime import run_blocking_io
from ....infrastructure.ids import UUIDGenerator
from ....features.entertainment.schemas import NewApiCheckinTarget
from ...xiuxian_utils.json_store import JsonStoreDataError, JsonStoreLimitError
from .newapi_client import (
    account_base_url,
    detect_auth_mode,
    do_checkin,
    fetch_user_self,
    format_checkin_block,
    format_user_info_block,
    format_user_info_reply,
    normalize_base_url,
    summarize_checkin_for_history,
)
from .newapi_store import (
    account_index,
    append_account,
    append_checkin_history,
    delete_accounts,
    display_base_url,
    iter_all_auto_checkin_bindings,
    list_account_summaries,
    load_accounts,
    load_checkin_history,
    record_checkin_history,
    resolve_checkin_targets,
    resolve_targets,
    toggle_auto_checkin,
)

_NEWAPI_FUN_KW = dict(
    md_type="娱乐",
    k1="查看",
    v1="newapi查看",
    k2="签到",
    v2="newapi签到",
    k3="帮助",
    v3="newapi帮助",
)

runtime_ids = UUIDGenerator()
_MAX_NEWAPI_LIST_RESPONSE_BYTES = 64 * 1024
_MAX_NEWAPI_CHECKIN_REPLY_BYTES = 64 * 1024
_MAX_NEWAPI_INFO_REPLY_BYTES = 64 * 1024
_NEWAPI_INFO_CONCURRENCY = 4
_NEWAPI_INFO_IO_TIMEOUT = 18

_URL_LIKE = re.compile(r"^https?://", re.I)


def _segment_is_base_url(s: str) -> bool:
    t = (s or "").strip()
    if not t:
        return False
    if _URL_LIKE.match(t):
        return True
    if re.match(r"^localhost(?:[:/]|$)", t, re.I):
        return True
    if re.match(r"^[\w.-]+$", t) and re.search(r"\.[a-zA-Z]{2,63}$", t):
        return True
    return False


def _parse_bind_args(text: str) -> tuple[str | None, str | None, str | None, str | None]:
    raw = (text or "").strip()
    if not raw:
        return None, None, None, None

    mode = "token"
    if raw.lower().startswith("cookie"):
        mode = "cookie"
        raw = raw[6:].strip()

    parts = [p.strip() for p in raw.split("#")]
    parts = [p for p in parts if p]

    if len(parts) < 2:
        return None, None, None, None

    api_user_id = parts[0]
    if _segment_is_base_url(api_user_id):
        return None, None, None, None

    url: str | None = None
    secret: str

    if len(parts) == 2:
        if _segment_is_base_url(parts[1]):
            return None, None, None, None
        secret = parts[1]
    elif len(parts) == 3:
        if not _segment_is_base_url(parts[2]):
            secret = "#".join(parts[1:])
        else:
            secret = parts[1]
            url = parts[2]
    else:
        if _segment_is_base_url(parts[-1]):
            url = parts[-1]
            secret = "#".join(parts[1:-1])
        else:
            secret = "#".join(parts[1:])

    if _segment_is_base_url(secret):
        return None, None, None, None

    if mode == "token":
        mode = detect_auth_mode(secret)

    return mode, api_user_id, secret, url


def _qq(event: GroupMessageEvent | PrivateMessageEvent) -> str:
    return str(event.get_user_id())


def _write_operation_id(event, action: str, target: str = "") -> str:
    event_id = str(getattr(event, "message_id", "") or getattr(event, "id", "") or "").strip()
    suffix = f":{target}" if target else ""
    return f"entertainment:{action}:{event_id or runtime_ids.new_id()}:{_qq(event)}{suffix}"


def _parse_delete_indices(text: str) -> list[int] | None:
    t = (text or "").strip().lower()
    if not t:
        return []
    if t in ("全部", "所有", "all", "*"):
        return None
    indices: set[int] = set()
    for part in t.replace("，", ",").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                lo, hi = int(a.strip()), int(b.strip())
                for i in range(min(lo, hi), max(lo, hi) + 1):
                    indices.add(i)
            except ValueError:
                return []
        else:
            try:
                indices.add(int(part))
            except ValueError:
                return []
    return sorted(indices) if indices else []


def _format_list_message(qq_id: str) -> str:
    result = list_account_summaries(qq_id)
    if result.status == "missing" or (result.status == "ok" and not result.accounts):
        return (
            "【NewAPI 绑定列表】\n"
            "暂无绑定\n\n"
            "绑定格式：newapi绑定 站点用户ID#令牌#接口地址\n"
            "Cookie 格式：newapi绑定 cookie 站点用户ID#session或Cookie#接口地址\n"
            "说明：# 用于分隔字段。"
        )
    if result.status != "ok":
        messages = {
            "invalid": "绑定数据格式无效，未修改原文件。",
            "too_large": "绑定数据超过 1 MiB 读取上限，未加载凭据。",
            "too_many": "绑定账号超过 48 条展示上限，未加载凭据。",
            "unavailable": "暂时无法读取绑定数据，未修改原文件。",
        }
        return f"【NewAPI 绑定列表】\n{messages.get(result.status, '绑定数据暂不可用。')}"
    lines = ["【NewAPI 绑定列表】", ""]
    for i, account in enumerate(result.accounts, start=1):
        api_id = escape_markdown_text(account.api_user_id)
        base = escape_markdown_text(account.base_url)
        label = escape_markdown_text(account.label)
        auth = "Cookie" if account.mode == "cookie" else "Token"
        auto = " · 自动签到开" if account.auto_checkin else ""
        extra = f" · {label}" if label else ""
        lines.append(f"{i}. 站点用户 {api_id} · {auth} · {base}{auto}{extra}")
    message = "\n".join(lines)
    if len(message.encode("utf-8")) > _MAX_NEWAPI_LIST_RESPONSE_BYTES:
        return "【NewAPI 绑定列表】\n绑定列表超过安全展示上限，未发送部分内容。"
    return message


def _run_checkin_for_account(
    qq_id: str,
    acc: dict[str, Any],
    *,
    source: Literal["manual", "auto"] = "manual",
) -> tuple[int, dict[str, Any]]:
    all_acc = load_accounts(qq_id)
    idx = account_index(all_acc, acc)
    mode = acc.get("mode") or detect_auth_mode(acc.get("secret") or "")
    base = account_base_url(acc.get("base_url"))
    if not base:
        data = {"_error": "未配置接口地址"}
    else:
        data = do_checkin(
            mode,
            str(acc.get("api_user_id")),
            acc.get("secret") or "",
            base,
        )
    append_checkin_history(
        qq_id,
        account_index=idx,
        api_user_id=str(acc.get("api_user_id")),
        base_url_stored=str(acc.get("base_url") or ""),
        summary=summarize_checkin_for_history(data),
        source=source,
    )
    return idx, data


def _run_manual_checkin_for_target(
    qq_id: str,
    target: NewApiCheckinTarget,
) -> tuple[int, dict[str, Any]]:
    mode = target.mode or detect_auth_mode(target.secret)
    base = account_base_url(target.base_url)
    if not base:
        data = {"_error": "未配置接口地址"}
    else:
        data = do_checkin(mode, target.api_user_id, target.secret, base)
    try:
        record_checkin_history(
            qq_id,
            account_index=target.index,
            api_user_id=target.api_user_id,
            base_url=target.base_url,
            summary=summarize_checkin_for_history(data),
            source="manual",
        )
    except (OSError, JsonStoreDataError, JsonStoreLimitError):
        data["_history_warning"] = "历史记录未更新，签到结果不受影响"
    return target.index, data


async def run_scheduled_auto_checkins() -> int:
    n = 0
    for qq_key, _idx, acc in iter_all_auto_checkin_bindings():
        try:
            await run_blocking_io(
                _run_checkin_for_account,
                qq_key,
                acc,
                source="auto",
                timeout=35,
            )
            n += 1
        except Exception:
            continue
    return n


async def _fetch_info_for_target(target: NewApiCheckinTarget) -> dict[str, Any]:
    base = account_base_url(target.base_url)
    if not base:
        return {"_error": "未配置接口地址"}
    return await run_blocking_io(
        fetch_user_self,
        target.mode or detect_auth_mode(target.secret),
        target.api_user_id,
        target.secret,
        base,
        timeout=_NEWAPI_INFO_IO_TIMEOUT,
    )


newapi_help_cmd = on_command("newapi帮助", aliases={"newapi", "NewAPI帮助"}, priority=5, block=True)
newapi_bind_cmd = on_command("newapi绑定", priority=5, block=True)
newapi_list_cmd = on_command("newapi查看", aliases={"newapi列表", "newapi绑定列表"}, priority=5, block=True)
newapi_checkin_cmd = on_command("newapi签到", priority=5, block=True)
newapi_info_cmd = on_command("newapi信息", priority=5, block=True)
newapi_del_cmd = on_command("newapi删除", aliases={"newapi解绑"}, priority=5, block=True)
newapi_history_cmd = on_command("newapi签到历史", aliases={"newapi签到记录"}, priority=5, block=True)
newapi_auto_cmd = on_command("newapi自动签到", priority=5, block=True)

__NEWAPI_HELP__ = """NewAPI 帮助

【绑定】
1. newapi绑定 站点用户ID#令牌#接口地址
2. newapi绑定 cookie 站点用户ID#session或完整Cookie#接口地址

【查询与管理】
1. newapi查看
> 本 QQ 已绑定的全部账号（带序号）
2. newapi签到 [序号]
> 默认全部；序号可写 1、1,3 或 2-4（会记入签到历史，保留最近 3 次）
3. newapi签到历史
> 查看最近签到记录
4. newapi自动签到 序号
> 切换该账号每日 12:30 自动签到（开/关）
5. newapi信息 [序号]
> 拉取站点用户信息，默认同上
6. newapi删除 序号|全部
> 须写序号（如 1、1,3）或写 全部

说明
> # 用于分隔字段。字段顺序为 站点用户ID#令牌或Cookie#接口地址。密钥中可含 #，接口须为最后一段。"""


@newapi_help_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def newapi_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(bot, event, __NEWAPI_HELP__, **_NEWAPI_FUN_KW)
    await newapi_help_cmd.finish()


@newapi_bind_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def newapi_bind_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    mode, api_user_id, secret, url = _parse_bind_args(args.extract_plain_text())
    if not api_user_id or not secret:
        await handle_send(
            bot,
            event,
            "【NewAPI 绑定】\n"
            "格式：newapi绑定 站点用户ID#令牌#接口地址\n"
            "或：newapi绑定 cookie 站点用户ID#Cookie#接口地址\n"
            "说明：# 用于分隔字段；接口地址放在最后一段。\n"
            "详见：newapi帮助",
            **_NEWAPI_FUN_KW,
        )
        await newapi_bind_cmd.finish()

    base_url = normalize_base_url(url)
    ok, msg = append_account(
        _qq(event),
        mode=mode,  # type: ignore[arg-type]
        api_user_id=api_user_id,
        secret=secret,
        base_url=base_url,
        operation_id=_write_operation_id(event, "newapi-bind", api_user_id),
    )
    await handle_send(bot, event, msg if ok else f"绑定失败：{msg}", **_NEWAPI_FUN_KW)
    await newapi_bind_cmd.finish()


@newapi_list_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def newapi_list_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(bot, event, _format_list_message(_qq(event)), **_NEWAPI_FUN_KW)
    await newapi_list_cmd.finish()


@newapi_checkin_cmd.handle(parameterless=[Cooldown(cd_time=8)])
async def newapi_checkin_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    qq = _qq(event)
    result = resolve_checkin_targets(qq, args.extract_plain_text())
    if result.status != "ok":
        messages = {
            "empty": "尚未绑定账号，请使用：newapi绑定 站点用户ID#令牌#接口地址",
            "missing": "尚未绑定账号，请使用：newapi绑定 站点用户ID#令牌#接口地址",
            "invalid": "绑定数据格式或字段无效，未修改原文件。",
            "too_large": "绑定数据超过 1 MiB 读取上限，未加载凭据。",
            "too_many": "绑定账号超过 48 条处理上限，未加载凭据。",
            "unavailable": "暂时无法读取绑定数据，未修改原文件。",
            "invalid_selector": result.message,
        }
        await handle_send(bot, event, messages.get(result.status, "绑定数据暂不可用。"), **_NEWAPI_FUN_KW)
        await newapi_checkin_cmd.finish()

    blocks: list[str] = ["【NewAPI 签到】", ""]
    reply_bytes = len("【NewAPI 签到】\n\n".encode("utf-8"))
    omitted_results = False
    for target in result.targets:
        idx, data = await run_blocking_io(
            _run_manual_checkin_for_target,
            qq,
            target,
            timeout=35,
        )
        account_view = {"api_user_id": target.api_user_id, "base_url": target.base_url}
        block = format_checkin_block(idx, account_view, data)
        warning = data.get("_history_warning")
        if warning:
            block = f"{block}\n{warning}"
        encoded = (block + "\n\n").encode("utf-8")
        if not omitted_results and reply_bytes + len(encoded) <= _MAX_NEWAPI_CHECKIN_REPLY_BYTES:
            blocks.extend((block, ""))
            reply_bytes += len(encoded)
        else:
            omitted_results = True
    if omitted_results:
        blocks.append("部分签到结果因消息长度限制未展示。")

    await handle_send(bot, event, "\n".join(blocks).strip(), **_NEWAPI_FUN_KW)
    await newapi_checkin_cmd.finish()


@newapi_info_cmd.handle(parameterless=[Cooldown(cd_time=8)])
async def newapi_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    qq = _qq(event)
    result = entertainment_application.resolve_info_targets(
        state_path=_path_for_qq(qq),
        selector=args.extract_plain_text(),
    )
    if result.status != "ok":
        messages = {
            "empty": "尚未绑定账号，请使用：newapi绑定 站点用户ID#令牌#接口地址",
            "missing": "尚未绑定账号，请使用：newapi绑定 站点用户ID#令牌#接口地址",
            "invalid": "绑定数据格式或字段无效，未修改原文件。",
            "too_large": "绑定数据超过 1 MiB 读取上限，未加载凭据。",
            "too_many": result.message or "绑定账号数量超过 48 条读取上限。",
            "unavailable": "暂时无法读取绑定数据，未修改原文件。",
            "invalid_selector": result.message,
        }
        await handle_send(bot, event, messages.get(result.status, "绑定数据暂不可用。"), **_NEWAPI_FUN_KW)
        await newapi_info_cmd.finish()

    semaphore = asyncio.Semaphore(_NEWAPI_INFO_CONCURRENCY)

    async def fetch_one(target: NewApiCheckinTarget):
        async with semaphore:
            return target, await _fetch_info_for_target(target)

    fetched = await asyncio.gather(*(fetch_one(target) for target in result.targets))
    blocks: list[str] = []
    for target, data in fetched:
        account_view = {"api_user_id": target.api_user_id, "base_url": target.base_url}
        blocks.append(format_user_info_block(target.index, account_view, data))

    await handle_send(
        bot,
        event,
        format_user_info_reply(blocks, max_bytes=_MAX_NEWAPI_INFO_REPLY_BYTES),
        **_NEWAPI_FUN_KW,
    )
    await newapi_info_cmd.finish()


@newapi_del_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def newapi_del_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    text = args.extract_plain_text()
    indices = _parse_delete_indices(text)
    if indices == []:
        await handle_send(
            bot,
            event,
            "【NewAPI 删除】\n"
            "用法：newapi删除 序号（如 1、1,3、2-4）\n"
            "或：newapi删除 全部",
            **_NEWAPI_FUN_KW,
        )
        await newapi_del_cmd.finish()

    ok, msg = delete_accounts(
        _qq(event),
        indices,
        operation_id=_write_operation_id(event, "newapi-delete", text or "all"),
    )
    await handle_send(bot, event, msg if ok else f"删除失败：{msg}", **_NEWAPI_FUN_KW)
    await newapi_del_cmd.finish()


def _format_checkin_history(qq_id: str) -> str:
    rows = load_checkin_history(qq_id)
    if not rows:
        return "【NewAPI 签到历史】\n（暂无，执行 newapi签到 后会记录，最多保留 3 条）"
    lines = ["【NewAPI 签到历史】", ""]
    for i, row in enumerate(rows, start=1):
        at = row.get("at") or "—"
        idx = row.get("index", "?")
        api_id = row.get("api_user_id", "?")
        base = display_base_url(row.get("base_url"))
        src = "自动" if row.get("source") == "auto" else "手动"
        summary = row.get("summary") or "—"
        lines.append(f"{i}. {at} · 账号{idx} · 用户{api_id} · {base}")
        lines.append(f"   [{src}] {summary}")
    return "\n".join(lines)


@newapi_history_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def newapi_history_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(bot, event, _format_checkin_history(_qq(event)), **_NEWAPI_FUN_KW)
    await newapi_history_cmd.finish()


@newapi_auto_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def newapi_auto_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    text = args.extract_plain_text()
    ok, msg = toggle_auto_checkin(
        _qq(event),
        text,
        operation_id=_write_operation_id(event, "newapi-auto", text.strip()),
    )
    await handle_send(bot, event, msg if ok else f"操作失败：{msg}", **_NEWAPI_FUN_KW)
    await newapi_auto_cmd.finish()
