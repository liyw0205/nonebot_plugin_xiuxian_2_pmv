import asyncio
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from xml.etree import ElementTree as ET

import requests
from nonebot.params import CommandArg

from ..command import *
from ...xiuxian_utils.utils import build_md_command_link


WEBDAV_DATA_DIR = Path(__file__).resolve().parent / "data" / "alist_webdav_bindings"
WEBDAV_DATA_DIR.mkdir(parents=True, exist_ok=True)
WEBDAV_BINDINGS_FILE = WEBDAV_DATA_DIR / "bindings.json"
WEBDAV_TMP_DIR = WEBDAV_DATA_DIR / "tmp"
WEBDAV_TMP_DIR.mkdir(parents=True, exist_ok=True)

DAV_NS = {"d": "DAV:"}
LIST_LIMIT = 30
MAX_SEND_FILE_BYTES = 50 * 1024 * 1024


def _load_bindings() -> list[dict[str, Any]]:
    if not WEBDAV_BINDINGS_FILE.exists():
        return []
    try:
        with open(WEBDAV_BINDINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    except Exception:
        pass
    return []


def _save_bindings(rows: list[dict[str, Any]]) -> None:
    with open(WEBDAV_BINDINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


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


def _parse_target_and_path(text: str, need_path: bool = False) -> tuple[dict[str, Any] | None, int, str, str | None]:
    bindings = _load_bindings()
    if not bindings:
        return None, 0, "", "尚未绑定 WebDAV 账号，请联系管理员使用：webdav绑定 备注#https://站点/dav#用户名#密码"

    raw = (text or "").strip()
    idx = 1
    path_text = raw
    if raw:
        first, _, rest = raw.partition(" ")
        if first.isdigit():
            idx = int(first)
            path_text = rest.strip()

    if idx < 1 or idx > len(bindings):
        return None, idx, "", f"序号 {idx} 超出范围（1～{len(bindings)}）"
    if need_path and not path_text:
        return None, idx, "", "请填写路径，例如：webdav信息 1 /电影/test.mp4"
    return bindings[idx - 1], idx, _format_dav_path(path_text), None


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


def _prop_text(prop: ET.Element, name: str) -> str:
    node = prop.find(f"d:{name}", DAV_NS)
    return (node.text or "").strip() if node is not None and node.text else ""


def _entry_from_response(resp: ET.Element) -> dict[str, Any]:
    href = resp.findtext("d:href", default="", namespaces=DAV_NS)
    prop = None
    for propstat in resp.findall("d:propstat", DAV_NS):
        status = propstat.findtext("d:status", default="", namespaces=DAV_NS)
        if "200" in status:
            prop = propstat.find("d:prop", DAV_NS)
            break
    if prop is None:
        prop = resp.find(".//d:prop", DAV_NS)
    if prop is None:
        prop = ET.Element("prop")

    res_type = prop.find("d:resourcetype", DAV_NS)
    is_dir = res_type is not None and res_type.find("d:collection", DAV_NS) is not None
    display_name = _prop_text(prop, "displayname")
    if not display_name and href:
        display_name = unquote(href.rstrip("/").split("/")[-1])
    return {
        "href": href,
        "name": display_name or "/",
        "is_dir": is_dir,
        "size": _prop_text(prop, "getcontentlength"),
        "modified": _prop_text(prop, "getlastmodified"),
        "content_type": _prop_text(prop, "getcontenttype"),
    }


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
    mark = "[DIR]" if item.get("is_dir") else "[FILE]"
    size = "" if item.get("is_dir") else f" · {_format_size(item.get('size'))}"
    return f"{mark} {name}{size}（{cmd}）"


def _md_list_line(binding_idx: int, binding: dict[str, Any], item: dict[str, Any]) -> str:
    name, cmd = _entry_command(binding_idx, binding, item)
    mark = "[DIR]" if item.get("is_dir") else "[FILE]"
    size = "" if item.get("is_dir") else f" · {_format_size(item.get('size'))}"
    return f"{mark} {build_md_command_link(name, cmd)}{size}"


def _safe_filename(name: str) -> str:
    value = re.sub(r'[\\/:*?"<>|\r\n]+', "_", str(name or "").strip())
    value = value.strip(" .")
    if not value:
        return "webdav_file"
    return value[:120]


def _propfind(binding: dict[str, Any], dav_path: str, depth: str) -> list[dict[str, Any]]:
    url = _join_dav_url(str(binding.get("dav_url") or ""), dav_path)
    headers = {
        "Depth": depth,
        "Content-Type": "application/xml; charset=utf-8",
    }
    body = """<?xml version="1.0" encoding="utf-8" ?>
<propfind xmlns="DAV:">
  <prop>
    <displayname />
    <resourcetype />
    <getcontentlength />
    <getlastmodified />
    <getcontenttype />
  </prop>
</propfind>"""
    resp = requests.request(
        "PROPFIND",
        url,
        data=body.encode("utf-8"),
        headers=headers,
        auth=(str(binding.get("username") or ""), str(binding.get("password") or "")),
        timeout=18,
    )
    if resp.status_code in {401, 403}:
        raise ValueError("认证失败，请检查用户名和密码")
    if resp.status_code == 404:
        raise ValueError("路径不存在")
    if resp.status_code not in {200, 207}:
        raise ValueError(f"WebDAV 返回 {resp.status_code}")

    root = ET.fromstring(resp.content)
    return [_entry_from_response(item) for item in root.findall("d:response", DAV_NS)]


def _test_binding(binding: dict[str, Any]) -> None:
    _propfind(binding, "/", "0")


def _append_binding(
    *,
    label: str,
    dav_url: str,
    username: str,
    password: str,
) -> tuple[bool, str]:
    rows = _load_bindings()
    norm_url = _normalize_dav_url(dav_url)
    for row in rows:
        if _normalize_dav_url(str(row.get("dav_url") or "")) == norm_url and str(row.get("username")) == username:
            return False, "已存在相同 WebDAV 地址和用户名的绑定"

    binding = {
        "label": label or "WebDAV",
        "dav_url": norm_url,
        "username": username,
        "password": password,
    }
    _test_binding(binding)
    rows.append(binding)
    _save_bindings(rows)
    return True, f"已绑定第 {len(rows)} 个 WebDAV：{binding['label']} · {_display_url(norm_url)}"


def _delete_bindings(text: str) -> tuple[bool, str]:
    rows = _load_bindings()
    if not rows:
        return False, "当前没有 WebDAV 绑定"
    value = (text or "").strip().lower()
    if value in {"全部", "所有", "all", "*"}:
        count = len(rows)
        _save_bindings([])
        return True, f"已删除全部 {count} 个 WebDAV 绑定"
    if not value.isdigit():
        return False, "删除用法：webdav删除 序号 或 webdav删除 全部"
    idx = int(value)
    if idx < 1 or idx > len(rows):
        return False, f"序号 {idx} 超出范围（1～{len(rows)}）"
    removed = rows.pop(idx - 1)
    _save_bindings(rows)
    return True, f"已删除绑定 {idx}：{removed.get('label') or _display_url(removed.get('dav_url') or '')}"


def _format_bindings() -> str:
    rows = _load_bindings()
    if not rows:
        return (
            "【WebDAV】\n"
            "（暂无绑定）\n\n"
            "管理员绑定：webdav绑定 备注#https://站点/dav#用户名#密码"
        )
    lines = ["【WebDAV 绑定】", ""]
    for i, row in enumerate(rows, start=1):
        label = row.get("label") or "WebDAV"
        lines.append(f"{i}. {label} · {row.get('username') or '?'} · {_display_url(row.get('dav_url') or '')}")
    return "\n".join(lines)


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
        f"【WebDAV 列表】账号 {idx} · {binding.get('label') or 'WebDAV'}",
        f"路径：{dav_path}",
        "",
    ]
    if current_path != "/":
        parent_cmd = f"webdav列表 {idx} {_parent_path(current_path)}"
        parent = build_md_command_link("上级目录", parent_cmd) if markdown else f"上级目录（{parent_cmd}）"
        lines.append(parent)
        lines.append("")
    if not ordered:
        lines.append("（空目录或无可显示内容）")
        return "\n".join(lines)
    for item in ordered[:LIST_LIMIT]:
        lines.append(_md_list_line(idx, binding, item) if markdown else _plain_list_line(idx, binding, item))
    if len(ordered) > LIST_LIMIT:
        lines.append(f"... 还有 {len(ordered) - LIST_LIMIT} 项未显示")
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


__WEBDAV_HELP__ = """【WebDAV 帮助】

管理员：
- webdav绑定 备注#https://站点/dav#用户名#密码
- webdav绑定 https://站点/dav#用户名#密码
- webdav删除 序号|全部

查询：
- webdav查看
- webdav列表 [序号] [路径]
- webdav信息 [序号] <路径>
- webdav链接 [序号] <路径>
- webdav文件 [序号] <路径>

说明：AList 和 OpenList 都支持常用 WebDAV 接口；列表里的目录可点击进入，文件可点击发送，链接访问仍需要对应用户名和密码。"""


async def _is_webdav_admin(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> bool:
    return bool(await SUPERUSER(bot, event))


def _download_webdav_file(binding: dict[str, Any], dav_path: str) -> tuple[Path, str]:
    entries = _propfind(binding, dav_path, "0")
    item = entries[0] if entries else {}
    if item.get("is_dir"):
        raise ValueError("这是目录，请使用 webdav列表 进入")

    size_text = str(item.get("size") or "0")
    try:
        size = int(size_text)
    except Exception:
        size = 0
    if size > MAX_SEND_FILE_BYTES:
        raise ValueError(f"文件超过发送上限（{_format_size(str(MAX_SEND_FILE_BYTES))}）")

    url = _join_dav_url(str(binding.get("dav_url") or ""), dav_path)
    filename = _safe_filename(Path(_format_dav_path(dav_path)).name or _entry_display_name(item) or "webdav_file")
    fd, tmp_name = tempfile.mkstemp(prefix="webdav_", suffix=f"_{filename}", dir=WEBDAV_TMP_DIR)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with requests.get(
            url,
            stream=True,
            auth=(str(binding.get("username") or ""), str(binding.get("password") or "")),
            timeout=60,
        ) as resp:
            if resp.status_code in {401, 403}:
                raise ValueError("认证失败，请检查用户名和密码")
            if resp.status_code == 404:
                raise ValueError("文件不存在")
            resp.raise_for_status()

            total = 0
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > MAX_SEND_FILE_BYTES:
                        raise ValueError(f"文件超过发送上限（{_format_size(str(MAX_SEND_FILE_BYTES))}）")
                    f.write(chunk)
        return tmp_path, filename
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


async def _send_local_file(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, path: Path, filename: str) -> None:
    call_api = getattr(bot, "call_api", None)
    if callable(call_api):
        group_id = getattr(event, "group_id", None)
        if group_id is not None:
            try:
                await call_api("upload_group_file", group_id=group_id, file=str(path), name=filename)
                return
            except Exception:
                pass
        try:
            await call_api("upload_private_file", user_id=event.get_user_id(), file=str(path), name=filename)
            return
        except Exception:
            pass

    await bot.send(event=event, message=MessageSegment.file(bot, path))


async def _send_webdav_file(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, binding: dict[str, Any], idx: int, dav_path: str) -> None:
    tmp_path: Path | None = None
    try:
        tmp_path, filename = await asyncio.to_thread(_download_webdav_file, binding, dav_path)
        await _send_local_file(bot, event, tmp_path, filename)
    except Exception as e:
        url = _join_dav_url(str(binding.get("dav_url") or ""), dav_path)
        msg = (
            f"发送文件失败：{e}\n\n"
            f"【WebDAV 链接】账号 {idx}\n"
            f"路径：{dav_path}\n"
            f"地址：{url}\n\n"
            "该地址需要 WebDAV 用户名和密码访问。"
        )
        await handle_send(bot, event, msg, **_DAV_KW)
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


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
            "绑定用法：webdav绑定 备注#https://站点/dav#用户名#密码\n"
            "或：webdav绑定 https://站点/dav#用户名#密码",
            **_DAV_KW,
        )
        await webdav_bind_cmd.finish()

    label, dav_url, username, password = parsed
    try:
        ok, msg = _append_binding(
            label=label,
            dav_url=dav_url,
            username=username,
            password=password,
        )
    except Exception as e:
        ok, msg = False, str(e)
    await handle_send(bot, event, msg if ok else f"绑定失败：{msg}", **_DAV_KW)
    await webdav_bind_cmd.finish()


@webdav_list_bind_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def webdav_list_bind_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(bot, event, _format_bindings(), **_DAV_KW)
    await webdav_list_bind_cmd.finish()


@webdav_ls_cmd.handle(parameterless=[Cooldown(cd_time=6)])
async def webdav_ls_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(args.extract_plain_text(), need_path=False)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await webdav_ls_cmd.finish()
    try:
        entries = _propfind(binding, dav_path, "1")
        msg = _format_list(binding, idx, dav_path, entries, markdown=True)
        fallback = _format_list(binding, idx, dav_path, entries, markdown=False)
        await handle_send(
            bot,
            event,
            msg,
            native_markdown=True,
            fallback_msg=fallback,
            keyboard_rows=[
                [("查看绑定", "webdav查看"), ("根目录", f"webdav列表 {idx} /"), ("帮助", "webdav帮助")]
            ],
            at_msg=True,
        )
        await webdav_ls_cmd.finish()
    except Exception as e:
        msg = f"读取 WebDAV 目录失败：{e}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_ls_cmd.finish()


@webdav_info_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def webdav_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(args.extract_plain_text(), need_path=True)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await webdav_info_cmd.finish()
    try:
        entries = _propfind(binding, dav_path, "0")
        msg = _format_info(binding, idx, dav_path, entries)
    except Exception as e:
        msg = f"读取 WebDAV 信息失败：{e}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_info_cmd.finish()


@webdav_link_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def webdav_link_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(args.extract_plain_text(), need_path=True)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await webdav_link_cmd.finish()
    url = _join_dav_url(str(binding.get("dav_url") or ""), dav_path)
    msg = (
        f"【WebDAV 链接】账号 {idx}\n"
        f"路径：{dav_path}\n"
        f"地址：{url}\n\n"
        "该地址需要 WebDAV 用户名和密码访问。"
    )
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_link_cmd.finish()


@webdav_file_cmd.handle(parameterless=[Cooldown(cd_time=8)])
async def webdav_file_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(args.extract_plain_text(), need_path=True)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await webdav_file_cmd.finish()
    await _send_webdav_file(bot, event, binding, idx, dav_path)
    await webdav_file_cmd.finish()


@webdav_del_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def webdav_del_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    if not await _is_webdav_admin(bot, event):
        await handle_send(bot, event, "WebDAV 删除仅管理员可用。", **_DAV_KW)
        await webdav_del_cmd.finish()

    ok, msg = _delete_bindings(args.extract_plain_text())
    await handle_send(bot, event, msg if ok else f"删除失败：{msg}", **_DAV_KW)
    await webdav_del_cmd.finish()
