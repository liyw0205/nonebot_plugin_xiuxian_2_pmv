import json
import posixpath
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from xml.etree import ElementTree as ET

from nonebot.params import CommandArg

from ..command import *
from ..io_runtime import run_blocking_io
from ...xiuxian_utils.utils import build_md_command_link
from ...xiuxian_utils.http_proxy import http_client


WEBDAV_DATA_DIR = Path(__file__).resolve().parent / "data" / "alist_webdav_bindings"
WEBDAV_DATA_DIR.mkdir(parents=True, exist_ok=True)
WEBDAV_BINDINGS_FILE = WEBDAV_DATA_DIR / "bindings.json"

DAV_NS = {"d": "DAV:"}
LIST_LIMIT = 30
LINK_CACHE_TTL = 300
_LIST_CACHE: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
_TOKEN_CACHE: dict[tuple[str, str], str] = {}
_LINK_CACHE: dict[tuple[str, str, str], tuple[float, dict[str, Any]]] = {}


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


def _normalize_openlist_path(path_text: str) -> str:
    path = _format_dav_path(path_text)
    normalized = posixpath.normpath(path)
    if normalized in {"", "."}:
        return "/"
    if not normalized.startswith("/"):
        normalized = "/" + normalized
    return normalized


def _api_base_from_dav_url(dav_url: str) -> str:
    split = urlsplit(_normalize_dav_url(dav_url))
    parts = [part for part in split.path.split("/") if part]
    dav_index = next((i for i, part in enumerate(parts) if part.lower() == "dav"), None)
    api_path = "/" + "/".join(parts[:dav_index]) if dav_index is not None and parts[:dav_index] else ""
    return urlunsplit((split.scheme, split.netloc, api_path.rstrip("/"), "", "")).rstrip("/")


def _openlist_download_path(binding: dict[str, Any], dav_path: str) -> str:
    split = urlsplit(_normalize_dav_url(str(binding.get("dav_url") or "")))
    parts = [unquote(part) for part in split.path.split("/") if part]
    dav_index = next((i for i, part in enumerate(parts) if part.lower() == "dav"), None)
    base_parts = parts[dav_index + 1 :] if dav_index is not None else []
    path = _normalize_openlist_path(dav_path)
    if not base_parts:
        return path

    base_path = _normalize_openlist_path("/" + "/".join(base_parts))
    if path == base_path or path.startswith(base_path.rstrip("/") + "/"):
        return path
    return _normalize_openlist_path(f"{base_path.rstrip('/')}/{path.lstrip('/')}")


def _binding_api_prefix(binding: dict[str, Any]) -> tuple[str, str]:
    return (
        _api_base_from_dav_url(str(binding.get("dav_url") or "")),
        str(binding.get("username") or ""),
    )


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


def _binding_cache_prefix(binding: dict[str, Any]) -> tuple[str, str]:
    return (
        _normalize_dav_url(str(binding.get("dav_url") or "")),
        str(binding.get("username") or ""),
    )


def _list_cache_key(binding: dict[str, Any], dav_path: str, depth: str) -> tuple[str, str, str, str]:
    url, username = _binding_cache_prefix(binding)
    return url, username, _path_key(dav_path), str(depth)


def _clear_binding_cache(binding: dict[str, Any] | None = None) -> None:
    if binding is None:
        _LIST_CACHE.clear()
        _TOKEN_CACHE.clear()
        _LINK_CACHE.clear()
        return
    prefix = _binding_cache_prefix(binding)
    for key in [key for key in _LIST_CACHE if key[:2] == prefix]:
        _LIST_CACHE.pop(key, None)
    api_prefix = _binding_api_prefix(binding)
    _TOKEN_CACHE.pop(api_prefix, None)
    for key in [key for key in _LINK_CACHE if key[:2] == api_prefix]:
        _LINK_CACHE.pop(key, None)


def _parse_target_and_path(text: str, need_path: bool = False) -> tuple[dict[str, Any] | None, int, str, str | None]:
    bindings = _load_bindings()
    if not bindings:
        return None, 0, "", (
            "尚未绑定 WebDAV 账号。\n"
            "管理员绑定格式：webdav绑定 备注#https://站点/dav#用户名#密码"
        )

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
    kind = "目录" if item.get("is_dir") else "文件"
    size = "" if item.get("is_dir") else f" · {_format_size(item.get('size'))}"
    return f"{kind}：{name}{size}（{cmd}）"


def _md_list_line(binding_idx: int, binding: dict[str, Any], item: dict[str, Any]) -> str:
    name, cmd = _entry_command(binding_idx, binding, item)
    kind = "目录" if item.get("is_dir") else "文件"
    size = "" if item.get("is_dir") else f" · {_format_size(item.get('size'))}"
    return f"{kind}：{build_md_command_link(name, cmd)}{size}"


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
    resp = http_client.request(
        "PROPFIND",
        url,
        data=body.encode("utf-8"),
        headers=headers,
        auth=(str(binding.get("username") or ""), str(binding.get("password") or "")),
        timeout=18,
        check_status=False,
    )
    if resp.status_code in {401, 403}:
        raise ValueError("认证失败，请检查用户名和密码")
    if resp.status_code == 404:
        raise ValueError("路径不存在")
    if resp.status_code not in {200, 207}:
        raise ValueError(f"WebDAV 返回 {resp.status_code}")

    root = ET.fromstring(resp.content)
    return [_entry_from_response(item) for item in root.findall("d:response", DAV_NS)]


def _cached_propfind(binding: dict[str, Any], dav_path: str, depth: str) -> list[dict[str, Any]]:
    key = _list_cache_key(binding, dav_path, depth)
    cached = _LIST_CACHE.get(key)
    if cached is not None:
        return cached
    entries = _propfind(binding, dav_path, depth)
    _LIST_CACHE[key] = entries
    return entries


def _openlist_url(api_base: str, api_path: str) -> str:
    return f"{api_base.rstrip('/')}/{api_path.lstrip('/')}"


def _absolute_url(api_base: str, url: str) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    if re.match(r"^https?://", value, re.I):
        return value
    if value.startswith("/"):
        return api_base.rstrip("/") + value
    return value


def _openlist_token(binding: dict[str, Any], *, refresh: bool = False) -> str:
    key = _binding_api_prefix(binding)
    api_base, username = key
    if not api_base or not username:
        return ""
    if not refresh and key in _TOKEN_CACHE:
        return _TOKEN_CACHE[key]

    resp = http_client.request(
        "POST",
        _openlist_url(api_base, "/api/auth/login"),
        json={
            "username": username,
            "password": str(binding.get("password") or ""),
        },
        timeout=15,
        check_status=False,
    )
    if resp.status_code != 200:
        raise ValueError(f"登录接口返回 {resp.status_code}")
    try:
        result = resp.json()
    except Exception as e:
        raise ValueError("登录接口响应不是 JSON") from e
    if result.get("code") != 200:
        raise ValueError(str(result.get("message") or "登录失败"))

    token = str((result.get("data") or {}).get("token") or "")
    if not token:
        raise ValueError("登录接口未返回 token")
    _TOKEN_CACHE[key] = token
    return token


def _openlist_post(
    binding: dict[str, Any],
    api_path: str,
    payload: dict[str, Any],
    *,
    retry_auth: bool = True,
) -> dict[str, Any]:
    api_base = _api_base_from_dav_url(str(binding.get("dav_url") or ""))
    if not api_base:
        raise ValueError("无法识别站点地址")

    headers = {}
    token = ""
    try:
        token = _openlist_token(binding)
    except Exception:
        token = ""
    if token:
        headers["Authorization"] = token

    resp = http_client.request(
        "POST",
        _openlist_url(api_base, api_path),
        json=payload,
        headers=headers,
        timeout=18,
        check_status=False,
    )
    if resp.status_code in {401, 403} and token and retry_auth:
        _TOKEN_CACHE.pop(_binding_api_prefix(binding), None)
        refreshed = _openlist_token(binding, refresh=True)
        headers["Authorization"] = refreshed
        resp = http_client.request(
            "POST",
            _openlist_url(api_base, api_path),
            json=payload,
            headers=headers,
            timeout=18,
            check_status=False,
        )
    if resp.status_code != 200:
        raise ValueError(f"接口返回 {resp.status_code}")

    try:
        result = resp.json()
    except Exception as e:
        raise ValueError("接口响应不是 JSON") from e
    if result.get("code") in {401, 403} and token and retry_auth:
        _TOKEN_CACHE.pop(_binding_api_prefix(binding), None)
        refreshed = _openlist_token(binding, refresh=True)
        return _openlist_post(
            binding,
            api_path,
            payload,
            retry_auth=False,
        ) if refreshed else {}
    if result.get("code") != 200:
        raise ValueError(str(result.get("message") or "接口调用失败"))
    data = result.get("data") or {}
    return data if isinstance(data, dict) else {}


def _openlist_file_info(binding: dict[str, Any], dav_path: str) -> dict[str, Any]:
    return _openlist_post(
        binding,
        "/api/fs/get",
        {"path": _openlist_download_path(binding, dav_path), "password": ""},
    )


def _openlist_signed_download_url(binding: dict[str, Any], dav_path: str) -> str:
    api_base = _api_base_from_dav_url(str(binding.get("dav_url") or ""))
    file_info = _openlist_file_info(binding, dav_path)
    if file_info.get("is_dir", True):
        return ""

    raw_url = _absolute_url(api_base, str(file_info.get("raw_url") or ""))
    if raw_url:
        return raw_url

    download_path = _openlist_download_path(binding, dav_path)
    url = f"{api_base.rstrip('/')}/d{quote(download_path, safe='/')}"
    sign = str(file_info.get("sign") or "")
    if sign:
        url += f"?sign={quote(sign, safe='')}"
    return url


def _openlist_direct_download_link(binding: dict[str, Any], dav_path: str) -> str:
    api_base = _api_base_from_dav_url(str(binding.get("dav_url") or ""))
    download_path = _openlist_download_path(binding, dav_path)
    data = _openlist_post(binding, "/api/fs/link", {"path": download_path})
    return _absolute_url(api_base, str(data.get("url") or ""))


def _download_link_cache_key(binding: dict[str, Any], dav_path: str) -> tuple[str, str, str]:
    api_base, username = _binding_api_prefix(binding)
    return api_base, username, _openlist_download_path(binding, dav_path)


def _get_download_link(binding: dict[str, Any], dav_path: str) -> dict[str, Any]:
    key = _download_link_cache_key(binding, dav_path)
    now = time.time()
    cached = _LINK_CACHE.get(key)
    if cached and cached[0] > now:
        return cached[1]

    try:
        url = _openlist_direct_download_link(binding, dav_path)
        if url:
            result = {"kind": "direct", "url": url}
            _LINK_CACHE[key] = (now + LINK_CACHE_TTL, result)
            return result
    except Exception:
        pass

    try:
        url = _openlist_signed_download_url(binding, dav_path)
        if url:
            result = {"kind": "direct", "url": url}
            _LINK_CACHE[key] = (now + LINK_CACHE_TTL, result)
            return result
    except Exception:
        pass

    result = {"kind": "webdav", "url": _join_dav_url(str(binding.get("dav_url") or ""), dav_path)}
    _LINK_CACHE[key] = (now + LINK_CACHE_TTL, result)
    return result


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
        _clear_binding_cache()
        return True, f"已删除全部 {count} 个 WebDAV 绑定"
    if not value.isdigit():
        return False, "删除用法：webdav删除 序号 或 webdav删除 全部"
    idx = int(value)
    if idx < 1 or idx > len(rows):
        return False, f"序号 {idx} 超出范围（1～{len(rows)}）"
    removed = rows.pop(idx - 1)
    _save_bindings(rows)
    _clear_binding_cache(removed)
    return True, f"已删除绑定 {idx}：{removed.get('label') or _display_url(removed.get('dav_url') or '')}"


def _format_bindings() -> str:
    rows = _load_bindings()
    if not rows:
        return (
            "【WebDAV 绑定】\n"
            "暂无绑定\n\n"
            "管理员绑定格式：webdav绑定 备注#https://站点/dav#用户名#密码\n"
            "说明：# 用于分隔备注、地址、用户名和密码。"
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
1. webdav绑定 备注#https://站点/dav#用户名#密码
2. webdav绑定 https://站点/dav#用户名#密码
3. webdav删除 序号|全部

【查询】
1. webdav查看
2. webdav列表 [序号] [路径]
3. webdav信息 [序号] <路径>
4. webdav链接 [序号] <路径>
5. webdav文件 [序号] <路径>

说明：# 用于分隔绑定字段。AList 和 OpenList 都支持常用 WebDAV 接口；列表里的目录可点击进入，文件可点击获取链接。文件链接会优先使用站点接口获取下载地址，失败时返回 WebDAV 地址。"""


async def _is_webdav_admin(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent) -> bool:
    return bool(await SUPERUSER(bot, event))


def _format_link_message(binding: dict[str, Any], idx: int, dav_path: str) -> str:
    link = _get_download_link(binding, dav_path)
    url = str(link.get("url") or "")
    if link.get("kind") == "direct":
        return (
            f"【WebDAV 文件链接】账号 {idx}\n"
            f"路径：{dav_path}\n"
            f"地址：\n{url}\n\n"
            "复制链接到浏览器或下载工具即可使用。"
        )
    return (
        f"【WebDAV 链接】账号 {idx}\n"
        f"路径：{dav_path}\n"
        f"地址：\n{url}\n\n"
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
        ok, msg = await run_blocking_io(
            _append_binding,
            label=label,
            dav_url=dav_url,
            username=username,
            password=password,
            timeout=30,
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
        entries = await run_blocking_io(
            _cached_propfind, binding, dav_path, "1", timeout=25
        )
    except Exception as e:
        await handle_send(bot, event, f"读取 WebDAV 目录失败：{e}", **_DAV_KW)
        await webdav_ls_cmd.finish()
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


@webdav_info_cmd.handle(parameterless=[Cooldown(cd_time=5)])
async def webdav_info_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(args.extract_plain_text(), need_path=True)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await webdav_info_cmd.finish()
    try:
        entries = await run_blocking_io(
            _propfind, binding, dav_path, "0", timeout=25
        )
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
    try:
        msg = await run_blocking_io(
            _format_link_message, binding, idx, dav_path, timeout=35
        )
    except Exception as e:
        msg = f"获取 WebDAV 链接失败：{e}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_link_cmd.finish()


@webdav_file_cmd.handle(parameterless=[Cooldown(cd_time=8)])
async def webdav_file_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    binding, idx, dav_path, err = _parse_target_and_path(args.extract_plain_text(), need_path=True)
    if err or not binding:
        await handle_send(bot, event, err or "无可用绑定", **_DAV_KW)
        await webdav_file_cmd.finish()
    try:
        msg = await run_blocking_io(
            _format_link_message, binding, idx, dav_path, timeout=35
        )
    except Exception as e:
        msg = f"获取 WebDAV 文件失败：{e}"
    await handle_send(bot, event, msg, **_DAV_KW)
    await webdav_file_cmd.finish()


@webdav_del_cmd.handle(parameterless=[Cooldown(cd_time=2)])
async def webdav_del_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    if not await _is_webdav_admin(bot, event):
        await handle_send(bot, event, "WebDAV 删除仅管理员可用。", **_DAV_KW)
        await webdav_del_cmd.finish()

    ok, msg = _delete_bindings(args.extract_plain_text())
    await handle_send(bot, event, msg if ok else f"删除失败：{msg}", **_DAV_KW)
    await webdav_del_cmd.finish()
