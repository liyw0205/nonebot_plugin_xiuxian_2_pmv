import re
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from nonebot.params import CommandArg

from ..command import *
from ..io_runtime import run_blocking_io
from ....features.entertainment.application import EntertainmentApplication
from ....features.entertainment.webdav_repository import (
    WebDavTargetError,
)
from ....paths import get_paths
from ...xiuxian_utils.utils import build_md_command_link


WEBDAV_DATA_DIR = Path(__file__).resolve().parent / "data" / "alist_webdav_bindings"
WEBDAV_DATA_DIR.mkdir(parents=True, exist_ok=True)
WEBDAV_BINDINGS_FILE = WEBDAV_DATA_DIR / "bindings.json"

LIST_LIMIT = 30

entertainment_application = EntertainmentApplication(get_paths().game_db)


def _normalize_dav_url(url: str) -> str:
    value = (url or "").strip().rstrip("/")
    if not value:
        return ""
    if not re.match(r"^https?://", value, re.I):
        value = "https://" + value
    return value.rstrip("/")


def _parse_bind_text(text: str) -> tuple[str, str, str, str] | None:
    parts = [part.strip() for part in (text or "").strip().split("#")]
    parts = [part for part in parts if part]
    if len(parts) < 3:
        return None

    if len(parts) == 3:
        label = "WebDAV"
        dav_url, username, password = parts
    else:
        label = parts[0]
        dav_url = parts[1]
        username = parts[2]
        password = "#".join(parts[3:])

    dav_url = _normalize_dav_url(dav_url)
    if not dav_url or not username or not password:
        return None
    return label[:30], dav_url, username, password


def _format_dav_path(path_text: str) -> str:
    value = (path_text or "").strip()
    if not value:
        return "/"
    value = value.replace("\\", "/")
    if not value.startswith("/"):
        value = "/" + value
    return re.sub(r"/+", "/", value)


def _path_key(path_text: str) -> str:
    path = _format_dav_path(path_text)
    return "/" if path == "/" else path.rstrip("/")


def _join_dav_url(base_url: str, dav_path: str) -> str:
    base = _normalize_dav_url(base_url)
    path = _format_dav_path(dav_path)
    split = urlsplit(base)
    base_path = split.path.rstrip("/")
    encoded_path = "/".join(quote(part, safe="") for part in path.strip("/").split("/") if part)
    full_path = base_path + ("/" + encoded_path if encoded_path else "")
    return urlunsplit((split.scheme, split.netloc, full_path or "/", "", ""))


def _href_to_dav_path(base_url: str, href: str) -> str:
    href_path = unquote(urlsplit(href or "").path or "/")
    base_path = unquote(urlsplit(_normalize_dav_url(base_url)).path or "").rstrip("/")
    if base_path and href_path.startswith(base_path + "/"):
        href_path = href_path[len(base_path) :]
    elif base_path and href_path == base_path:
        href_path = "/"
    return _format_dav_path(href_path)


def _display_url(base_url: str) -> str:
    split = urlsplit(_normalize_dav_url(base_url))
    path = split.path or "/"
    return f"{split.scheme}://{split.netloc}{path}"


def _format_size(size_text: str | None) -> str:
    try:
        size = int(size_text or 0)
    except Exception:
        return "—"
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size)
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.2f} {unit}"


def _entry_display_name(item: dict[str, Any]) -> str:
    name = str(item.get("name") or "").strip()
    if name and name != "/":
        return name.rstrip("/")
    path = _format_dav_path(_href_to_dav_path("", str(item.get("href") or "")))
    return path.rstrip("/").split("/")[-1] or "/"


def _entry_command(binding_idx: int, binding: dict[str, Any], item: dict[str, Any]) -> tuple[str, str]:
    path = _href_to_dav_path(str(binding.get("dav_url") or ""), str(item.get("href") or ""))
    if item.get("is_dir"):
        return _entry_display_name(item), f"webdav列表 {binding_idx} {path}"
    return _entry_display_name(item), f"webdav文件 {binding_idx} {path}"


def _plain_list_line(binding_idx: int, binding: dict[str, Any], item: dict[str, Any]) -> str:
    name, cmd = _entry_command(binding_idx, binding, item)
    kind = "目录" if item.get("is_dir") else "文件"
    size = "" if item.get("is_dir") else f" · {_format_size(item.get('size'))}"
    return f"{kind}：{name}{size}（{cmd}）"


def _md_list_line(binding_idx: int, binding: dict[str, Any], item: dict[str, Any]) -> str:
    name, cmd = _entry_command(binding_idx, binding, item)
    kind = "目录" if item.get("is_dir") else "文件"
    size = "" if item.get("is_dir") else f" · {_format_size(item.get('size'))}"
    return f"{kind}：{build_md_command_link(name, cmd)}{size}"


def _format_bindings(rows) -> str:
    rows = list(rows)
    if not rows:
        return (
            "【WebDAV 绑定】\n"
            "暂无绑定\n\n"
            "管理员绑定格式：webdav绑定 备注#https://站点/dav#用户名#密码\n"
            "说明：# 用于分隔备注、地址、用户名和密码。"
        )
    lines = ["【WebDAV 绑定】", ""]
    for i, row in enumerate(rows, start=1):
        if hasattr(row, "label"):
            label = row.label or "WebDAV"
            username = row.username or "?"
            dav_url = row.dav_url or ""
        else:
            label = row.get("label") or "WebDAV"
            username = row.get("username") or "?"
            dav_url = row.get("dav_url") or ""
        lines.append(f"{i}. {label} · {username} · {_display_url(dav_url)}")
    return "\n".join(lines)


def _binding_mapping(binding) -> dict[str, Any]:
    return {
        "label": getattr(binding, "label", "WebDAV"),
        "dav_url": getattr(binding, "dav_url", ""),
    }


def _entry_mapping(entry) -> dict[str, Any]:
    return {
        "href": getattr(entry, "href", ""),
        "name": getattr(entry, "name", "/"),
        "is_dir": bool(getattr(entry, "is_dir", False)),
        "size": getattr(entry, "size", ""),
        "modified": getattr(entry, "modified", ""),
        "content_type": getattr(entry, "content_type", ""),
    }


def _parent_path(dav_path: str) -> str:
    path = _format_dav_path(dav_path)
    if path == "/":
        return "/"
    parent = path.rstrip("/").rsplit("/", 1)[0]
    return parent or "/"


def _format_list(
    binding: dict[str, Any],
    idx: int,
    dav_path: str,
    entries: list[dict[str, Any]],
    *,
    markdown: bool = False,
) -> str:
    current_path = _format_dav_path(dav_path)
    children = [
        item for item in entries
        if _path_key(_href_to_dav_path(str(binding.get("dav_url") or ""), str(item.get("href") or ""))) != _path_key(current_path)
    ]
    dirs = [item for item in children if item["is_dir"]]
    files = [item for item in children if not item["is_dir"]]
    ordered = dirs + files
    lines = [
        "【WebDAV 列表】",
        f"账号：{idx} · {binding.get('label') or 'WebDAV'}",
        f"路径：{dav_path}",
        "",
    ]
    if current_path != "/":
        parent_cmd = f"webdav列表 {idx} {_parent_path(current_path)}"
        parent = build_md_command_link("上级目录", parent_cmd) if markdown else f"上级目录（{parent_cmd}）"
        lines.append(f"返回：{parent}")
        lines.append("")
    if not ordered:
        lines.append("（空目录或无可显示内容）")
        return "\n".join(lines)
    for item in ordered[:LIST_LIMIT]:
        lines.append(_md_list_line(idx, binding, item) if markdown else _plain_list_line(idx, binding, item))
    if len(ordered) > LIST_LIMIT:
        lines.append(f"还有 {len(ordered) - LIST_LIMIT} 项未显示，请进入子目录或缩小路径范围。")
    return "\n".join(lines)


def _format_info(binding: dict[str, Any], idx: int, dav_path: str, entries: list[dict[str, Any]]) -> str:
    item = entries[0] if entries else {}
    name = item.get("name") or dav_path.rstrip("/").split("/")[-1] or "/"
    kind = "目录" if item.get("is_dir") else "文件"
    lines = [
        f"【WebDAV 信息】账号 {idx} · {binding.get('label') or 'WebDAV'}",
        f"路径：{dav_path}",
        f"名称：{name}",
        f"类型：{kind}",
    ]
    if not item.get("is_dir"):
        lines.append(f"大小：{_format_size(item.get('size'))}")
        if item.get("content_type"):
            lines.append(f"MIME：{item.get('content_type')}")
    if item.get("modified"):
        lines.append(f"修改时间：{item.get('modified')}")
    return "\n".join(lines)


_DAV_KW = dict(
    md_type="娱乐",
    k1="查看",
    v1="webdav查看",
    k2="列表",
    v2="webdav列表",
    k3="帮助",
    v3="webdav帮助",
)

webdav_help_cmd = on_command("webdav帮助", aliases={"网盘帮助"}, priority=5, block=True)
webdav_bind_cmd = on_command("webdav绑定", aliases={"网盘绑定"}, priority=5, block=True)
webdav_list_bind_cmd = on_command("webdav查看", aliases={"网盘查看"}, priority=5, block=True)
webdav_ls_cmd = on_command("webdav列表", aliases={"网盘列表"}, priority=5, block=True)
webdav_info_cmd = on_command("webdav信息", aliases={"网盘信息"}, priority=5, block=True)
webdav_link_cmd = on_command("webdav链接", aliases={"网盘链接"}, priority=5, block=True)
webdav_file_cmd = on_command("webdav文件", aliases={"网盘文件"}, priority=5, block=True)
webdav_del_cmd = on_command("webdav删除", aliases={"webdav解绑", "网盘删除", "网盘解绑"}, priority=5, block=True)


__WEBDAV_HELP__ = """WebDAV 帮助

【管理员】
1. webdav绑定 备注#https
> //站点/dav#用户名#密码
2. webdav绑定 https
> //站点/dav#用户名#密码
3. webdav删除 序号|全部

【查询】
1. webdav查看
2. webdav列表 [序号] [路径]
3. webdav信息 [序号] <路径>
4. webdav链接 [序号] <路径>
5. webdav文件 [序号] <路径>

说明
> # 用于分隔绑定字段。AList 和 OpenList 都支持常用 WebDAV 接口；列表里的目录可点击进入，文件可点击获取链接。文件链接会优先使用站点接口获取下载地址，失败时返回 WebDAV 地址。"""


async def _is_webdav_admin(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> bool:
    return bool(await SUPERUSER(bot, event))


def _format_download_message(result: Any) -> str:
    if result.kind == "direct":
        return (
            f"【WebDAV 文件链接】账号 {result.index}\n"
            f"路径：{result.path}\n"
            f"地址：\n{result.url}\n\n"
            "复制链接到浏览器或下载工具即可使用。"
        )
    return (
        f"【WebDAV 链接】账号 {result.index}\n"
        f"路径：{result.path}\n"
        f"地址：\n{result.url}\n\n"
        "暂未获取到直链，已返回 WebDAV 地址。该地址需要 WebDAV 用户名和密码访问。"
    )


@webdav_help_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def webdav_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(bot, event, __WEBDAV_HELP__, **_DAV_KW)
    await webdav_help_cmd.finish()


@webdav_bind_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def webdav_bind_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    if not await _is_webdav_admin(bot, event):
        await handle_send(bot, event, "WebDAV 绑定仅管理员可用。", **_DAV_KW)
        await webdav_bind_cmd.finish()

    parsed = _parse_bind_text(args.extract_plain_text())
    if not parsed:
        await handle_send(
            bot,
            event,
            "【WebDAV 绑定】\n"
            "格式：webdav绑定 备注#https://站点/dav#用户名#密码\n"
            "或：webdav绑定 https://站点/dav#用户名#密码\n"
            "说明：# 用于分隔字段。",
            **_DAV_KW,
        )
        await webdav_bind_cmd.finish()

    label, dav_url, username, password = parsed
    try:
        result = await run_blocking_io(
            entertainment_application.webdav_bind,
            bindings_path=WEBDAV_BINDINGS_FILE,
            label=label,
            dav_url=dav_url,
            username=username,
            password=password,
            timeout=30,
        )
    except Exception as e:
        result = None
        msg = f"绑定失败：{e}"
    else:
        msg = result.message if result.status == "applied" else f"绑定失败：{result.message}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_bind_cmd.finish()


@webdav_list_bind_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def webdav_list_bind_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    try:
        bindings = await run_blocking_io(
            entertainment_application.webdav_bindings,
            bindings_path=WEBDAV_BINDINGS_FILE,
            timeout=5,
        )
        msg = _format_bindings(bindings)
    except Exception as e:
        msg = f"读取 WebDAV 绑定失败：{e}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_list_bind_cmd.finish()


@webdav_ls_cmd.handle(parameterless=[Cooldown(cd_time=6)])
async def webdav_ls_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    try:
        result = await run_blocking_io(
            entertainment_application.webdav_list,
            bindings_path=WEBDAV_BINDINGS_FILE,
            text=args.extract_plain_text(),
            timeout=25,
        )
    except WebDavTargetError as e:
        await handle_send(bot, event, str(e), **_DAV_KW)
        await webdav_ls_cmd.finish()
    except Exception as e:
        await handle_send(bot, event, f"读取 WebDAV 目录失败：{e}", **_DAV_KW)
        await webdav_ls_cmd.finish()
    binding = _binding_mapping(result.binding)
    entries = [_entry_mapping(entry) for entry in result.entries]
    msg = _format_list(binding, result.binding.index, result.path, entries, markdown=True)
    fallback = _format_list(binding, result.binding.index, result.path, entries, markdown=False)
    await handle_send(
        bot,
        event,
        msg,
        native_markdown=True,
        fallback_msg=fallback,
        keyboard_rows=[
            [
                ("查看绑定", "webdav查看"),
                ("根目录", f"webdav列表 {result.binding.index} /"),
                ("帮助", "webdav帮助"),
            ]
        ],
        at_msg=True,
    )
    await webdav_ls_cmd.finish()


@webdav_info_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def webdav_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    try:
        result = await run_blocking_io(
            entertainment_application.webdav_info,
            bindings_path=WEBDAV_BINDINGS_FILE,
            text=args.extract_plain_text(),
            timeout=25,
        )
    except WebDavTargetError as e:
        await handle_send(bot, event, str(e), **_DAV_KW)
        await webdav_info_cmd.finish()
    except Exception as e:
        msg = f"读取 WebDAV 信息失败：{e}"
    else:
        msg = _format_info(
            _binding_mapping(result.binding),
            result.binding.index,
            result.path,
            [_entry_mapping(entry) for entry in result.entries],
        )
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_info_cmd.finish()


@webdav_link_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def webdav_link_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    try:
        result = await run_blocking_io(
            entertainment_application.webdav_download_link,
            bindings_path=WEBDAV_BINDINGS_FILE,
            text=args.extract_plain_text(),
            timeout=35,
        )
    except WebDavTargetError as e:
        msg = str(e)
    except Exception as e:
        msg = f"获取 WebDAV 链接失败：{e}"
    else:
        msg = _format_download_message(result)
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_link_cmd.finish()


@webdav_file_cmd.handle(parameterless=[Cooldown(cd_time=8)])
async def webdav_file_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    try:
        result = await run_blocking_io(
            entertainment_application.webdav_download_link,
            bindings_path=WEBDAV_BINDINGS_FILE,
            text=args.extract_plain_text(),
            timeout=35,
        )
    except WebDavTargetError as e:
        msg = str(e)
    except Exception as e:
        msg = f"获取 WebDAV 文件失败：{e}"
    else:
        msg = _format_download_message(result)
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_file_cmd.finish()


@webdav_del_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def webdav_del_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    if not await _is_webdav_admin(bot, event):
        await handle_send(bot, event, "WebDAV 删除仅管理员可用。", **_DAV_KW)
        await webdav_del_cmd.finish()

    try:
        result = await run_blocking_io(
            entertainment_application.webdav_delete,
            bindings_path=WEBDAV_BINDINGS_FILE,
            text=args.extract_plain_text(),
            timeout=5,
        )
    except Exception as e:
        msg = f"删除失败：{e}"
    else:
        if result.status == "applied":
            msg = result.message
        else:
            msg = f"删除失败：{result.message}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_del_cmd.finish()
