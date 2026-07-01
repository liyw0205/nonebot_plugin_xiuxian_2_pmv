import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from xml.etree import ElementTree as ET

import requests
from nonebot.params import CommandArg

from ..command import *


WEBDAV_DATA_DIR = Path(__file__).resolve().parent / "data" / "alist_webdav_bindings"
WEBDAV_DATA_DIR.mkdir(parents=True, exist_ok=True)

DAV_NS = {"d": "DAV:"}
LIST_LIMIT = 30


def _qq(event: GroupMessageEvent | PrivateMessageEvent) -> str:
    return str(event.get_user_id())


def _safe_qq_path(qq_id: str) -> Path:
    safe = "".join(c if c.isalnum() else "_" for c in str(qq_id))
    return WEBDAV_DATA_DIR / f"{safe}.json"


def _load_bindings(qq_id: str) -> list[dict[str, Any]]:
    path = _safe_qq_path(qq_id)
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
    except Exception:
        pass
    return []


def _save_bindings(qq_id: str, rows: list[dict[str, Any]]) -> None:
    with open(_safe_qq_path(qq_id), "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def _normalize_dav_url(url: str) -> str:
    value = (url or "").strip().rstrip("/")
    if not value:
        return ""
    if not re.match(r"^https?://", value, re.I):
        value = "https://" + value
    return value.rstrip("/")


def _provider_from_event(event: GroupMessageEvent | PrivateMessageEvent) -> str:
    text = event.get_plaintext().strip().lower()
    return "openlist" if text.startswith("openlist") else "alist"


def _provider_name(provider: str) -> str:
    return "OpenList" if provider == "openlist" else "AList"


def _parse_bind_text(text: str, provider: str) -> tuple[str, str, str, str] | None:
    parts = [part.strip() for part in (text or "").strip().split("#")]
    parts = [part for part in parts if part]
    if len(parts) < 3:
        return None

    if len(parts) == 3:
        label = _provider_name(provider)
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


def _join_dav_url(base_url: str, dav_path: str) -> str:
    base = _normalize_dav_url(base_url)
    path = _format_dav_path(dav_path)
    split = urlsplit(base)
    base_path = split.path.rstrip("/")
    encoded_path = "/".join(quote(part, safe="") for part in path.strip("/").split("/") if part)
    full_path = base_path + ("/" + encoded_path if encoded_path else "")
    return urlunsplit((split.scheme, split.netloc, full_path or "/", "", ""))


def _display_url(base_url: str) -> str:
    split = urlsplit(_normalize_dav_url(base_url))
    path = split.path or "/"
    return f"{split.scheme}://{split.netloc}{path}"


def _parse_target_and_path(qq_id: str, text: str, need_path: bool = False) -> tuple[dict[str, Any] | None, int, str, str | None]:
    bindings = _load_bindings(qq_id)
    if not bindings:
        return None, 0, "", "尚未绑定 WebDAV 账号，请使用：alist绑定 备注#https://站点/dav#用户名#密码"

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
        return None, idx, "", "请填写路径，例如：alist信息 1 /电影/test.mp4"
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
    qq_id: str,
    *,
    provider: str,
    label: str,
    dav_url: str,
    username: str,
    password: str,
) -> tuple[bool, str]:
    rows = _load_bindings(qq_id)
    norm_url = _normalize_dav_url(dav_url)
    for row in rows:
        if _normalize_dav_url(str(row.get("dav_url") or "")) == norm_url and str(row.get("username")) == username:
            return False, "已存在相同 WebDAV 地址和用户名的绑定"

    binding = {
        "provider": provider,
        "label": label or _provider_name(provider),
        "dav_url": norm_url,
        "username": username,
        "password": password,
    }
    _test_binding(binding)
    rows.append(binding)
    _save_bindings(qq_id, rows)
    return True, f"已绑定第 {len(rows)} 个 {_provider_name(provider)} WebDAV：{binding['label']} · {_display_url(norm_url)}"


def _delete_bindings(qq_id: str, text: str) -> tuple[bool, str]:
    rows = _load_bindings(qq_id)
    if not rows:
        return False, "当前没有 WebDAV 绑定"
    value = (text or "").strip().lower()
    if value in {"全部", "所有", "all", "*"}:
        count = len(rows)
        _save_bindings(qq_id, [])
        return True, f"已删除全部 {count} 个 WebDAV 绑定"
    if not value.isdigit():
        return False, "删除用法：alist删除 序号 或 alist删除 全部"
    idx = int(value)
    if idx < 1 or idx > len(rows):
        return False, f"序号 {idx} 超出范围（1～{len(rows)}）"
    removed = rows.pop(idx - 1)
    _save_bindings(qq_id, rows)
    return True, f"已删除绑定 {idx}：{removed.get('label') or _display_url(removed.get('dav_url') or '')}"


def _format_bindings(qq_id: str) -> str:
    rows = _load_bindings(qq_id)
    if not rows:
        return (
            "【AList/OpenList WebDAV】\n"
            "（暂无绑定）\n\n"
            "绑定：alist绑定 备注#https://站点/dav#用户名#密码\n"
            "OpenList 同样可用：openlist绑定 备注#https://站点/dav#用户名#密码"
        )
    lines = ["【AList/OpenList WebDAV 绑定】", ""]
    for i, row in enumerate(rows, start=1):
        provider = _provider_name(str(row.get("provider") or "alist"))
        label = row.get("label") or provider
        lines.append(f"{i}. {label} · {provider} · {row.get('username') or '?'} · {_display_url(row.get('dav_url') or '')}")
    return "\n".join(lines)


def _format_list(binding: dict[str, Any], idx: int, dav_path: str, entries: list[dict[str, Any]]) -> str:
    children = entries[1:] if len(entries) > 1 else []
    dirs = [item for item in children if item["is_dir"]]
    files = [item for item in children if not item["is_dir"]]
    ordered = dirs + files
    lines = [
        f"【WebDAV 列表】账号 {idx} · {binding.get('label') or _provider_name(binding.get('provider') or 'alist')}",
        f"路径：{dav_path}",
        "",
    ]
    if not ordered:
        lines.append("（空目录或无可显示内容）")
        return "\n".join(lines)
    for item in ordered[:LIST_LIMIT]:
        mark = "[DIR]" if item["is_dir"] else "[FILE]"
        size = "" if item["is_dir"] else f" · {_format_size(item.get('size'))}"
        lines.append(f"{mark} {item['name']}{size}")
    if len(ordered) > LIST_LIMIT:
        lines.append(f"... 还有 {len(ordered) - LIST_LIMIT} 项未显示")
    return "\n".join(lines)


def _format_info(binding: dict[str, Any], idx: int, dav_path: str, entries: list[dict[str, Any]]) -> str:
    item = entries[0] if entries else {}
    name = item.get("name") or dav_path.rstrip("/").split("/")[-1] or "/"
    kind = "目录" if item.get("is_dir") else "文件"
    lines = [
        f"【WebDAV 信息】账号 {idx} · {binding.get('label') or _provider_name(binding.get('provider') or 'alist')}",
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
    v1="alist查看",
    k2="列表",
    v2="alist列表",
    k3="帮助",
    v3="alist帮助",
)

alist_help_cmd = on_command("alist帮助", aliases={"openlist帮助", "网盘帮助", "webdav帮助"}, priority=5, block=True)
alist_bind_cmd = on_command("alist绑定", aliases={"openlist绑定", "网盘绑定", "webdav绑定"}, priority=5, block=True)
alist_list_bind_cmd = on_command("alist查看", aliases={"openlist查看", "网盘查看", "webdav查看"}, priority=5, block=True)
alist_ls_cmd = on_command("alist列表", aliases={"openlist列表", "网盘列表", "webdav列表"}, priority=5, block=True)
alist_info_cmd = on_command("alist信息", aliases={"openlist信息", "网盘信息", "webdav信息"}, priority=5, block=True)
alist_link_cmd = on_command("alist链接", aliases={"openlist链接", "网盘链接", "webdav链接"}, priority=5, block=True)
alist_del_cmd = on_command("alist删除", aliases={"openlist删除", "网盘删除", "webdav删除", "alist解绑", "openlist解绑"}, priority=5, block=True)


__ALIST_WEBDAV_HELP__ = """【AList / OpenList WebDAV】

绑定：
- alist绑定 备注#https://站点/dav#用户名#密码
- alist绑定 https://站点/dav#用户名#密码
- openlist绑定 备注#https://站点/dav#用户名#密码

查询：
- alist查看
- alist列表 [序号] [路径]
- alist信息 [序号] <路径>
- alist链接 [序号] <路径>
- alist删除 序号|全部

说明：OpenList 是 AList 分支，WebDAV 常用接口兼容；链接命令只返回 WebDAV 地址，下载时仍需用户名和密码。"""


@alist_help_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def alist_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(bot, event, __ALIST_WEBDAV_HELP__, **_DAV_KW)
    await alist_help_cmd.finish()


@alist_bind_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def alist_bind_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    provider = _provider_from_event(event)
    parsed = _parse_bind_text(args.extract_plain_text(), provider)
    if not parsed:
        await handle_send(
            bot,
            event,
            "绑定用法：alist绑定 备注#https://站点/dav#用户名#密码\n"
            "或：alist绑定 https://站点/dav#用户名#密码\n"
            "OpenList 可用：openlist绑定 备注#https://站点/dav#用户名#密码",
            **_DAV_KW,
        )
        await alist_bind_cmd.finish()

    label, dav_url, username, password = parsed
    try:
        ok, msg = _append_binding(
            _qq(event),
            provider=provider,
            label=label,
            dav_url=dav_url,
            username=username,
            password=password,
        )
    except Exception as e:
        ok, msg = False, str(e)
    await handle_send(bot, event, msg if ok else f"绑定失败：{msg}", **_DAV_KW)
    await alist_bind_cmd.finish()


@alist_list_bind_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def alist_list_bind_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    await handle_send(bot, event, _format_bindings(_qq(event)), **_DAV_KW)
    await alist_list_bind_cmd.finish()


@alist_ls_cmd.handle(parameterless=[Cooldown(cd_time=6)])
async def alist_ls_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(_qq(event), args.extract_plain_text(), need_path=False)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await alist_ls_cmd.finish()
    try:
        entries = _propfind(binding, dav_path, "1")
        msg = _format_list(binding, idx, dav_path, entries)
    except Exception as e:
        msg = f"读取 WebDAV 目录失败：{e}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await alist_ls_cmd.finish()


@alist_info_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def alist_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(_qq(event), args.extract_plain_text(), need_path=True)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await alist_info_cmd.finish()
    try:
        entries = _propfind(binding, dav_path, "0")
        msg = _format_info(binding, idx, dav_path, entries)
    except Exception as e:
        msg = f"读取 WebDAV 信息失败：{e}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await alist_info_cmd.finish()


@alist_link_cmd.handle(parameterless=[Cooldown(cd_time=3)])
async def alist_link_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(_qq(event), args.extract_plain_text(), need_path=True)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await alist_link_cmd.finish()
    url = _join_dav_url(str(binding.get("dav_url") or ""), dav_path)
    msg = (
        f"【WebDAV 链接】账号 {idx}\n"
        f"路径：{dav_path}\n"
        f"地址：{url}\n\n"
        "该地址需要 WebDAV 用户名和密码访问。"
    )
    await handle_send(bot, event, msg, **_DAV_KW)
    await alist_link_cmd.finish()


@alist_del_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def alist_del_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    ok, msg = _delete_bindings(_qq(event), args.extract_plain_text())
    await handle_send(bot, event, msg if ok else f"删除失败：{msg}", **_DAV_KW)
    await alist_del_cmd.finish()
