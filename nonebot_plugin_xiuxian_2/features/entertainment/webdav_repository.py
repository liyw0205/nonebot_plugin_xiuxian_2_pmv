from __future__ import annotations

import json
import os
import re
import tempfile
import threading
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit
from xml.etree import ElementTree as ET

from ...xiuxian.xiuxian_utils.http_proxy import http_client as default_http_client
from .schemas import WebDavBinding, WebDavEntry, WebDavMutationResult, WebDavQueryResult

DAV_NS = {"d": "DAV:"}
MAX_WEBDAV_BINDINGS_BYTES = 256 * 1024
MAX_WEBDAV_BINDINGS_ROWS = 32
MAX_WEBDAV_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_WEBDAV_RESPONSE_ENTRIES = 128
MAX_WEBDAV_LABEL_CHARS = 64
MAX_WEBDAV_URL_CHARS = 512
MAX_WEBDAV_USERNAME_CHARS = 128
MAX_WEBDAV_PASSWORD_CHARS = 4096


class WebDavRepositoryError(ValueError):
    """A bounded WebDAV read failed without changing persistent state."""


class WebDavTargetError(WebDavRepositoryError):
    """The command selector or path is invalid before any network request."""


def _normalize_dav_url(value: str) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        return ""
    if not re.match(r"^https?://", raw, re.I):
        raw = "https://" + raw
    parsed = urlsplit(raw)
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.netloc:
        return ""
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return ""
    return urlunsplit((parsed.scheme.casefold(), parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _display_text(value: Any, limit: int) -> str:
    if not isinstance(value, str):
        return ""
    text = " ".join("".join(char for char in value if char.isprintable()).split())
    return text[:limit]


def _format_dav_path(value: str) -> str:
    path = str(value or "").strip().replace("\\", "/")
    if not path:
        return "/"
    if not path.startswith("/"):
        path = "/" + path
    return re.sub(r"/+", "/", path)


def _path_key(value: str) -> str:
    path = _format_dav_path(value)
    return "/" if path == "/" else path.rstrip("/")


def _join_dav_url(base_url: str, dav_path: str) -> str:
    parsed = urlsplit(_normalize_dav_url(base_url))
    path = _format_dav_path(dav_path)
    encoded = "/".join(quote(part, safe="") for part in path.strip("/").split("/") if part)
    base_path = parsed.path.rstrip("/")
    full_path = base_path + ("/" + encoded if encoded else "")
    return urlunsplit((parsed.scheme, parsed.netloc, full_path or "/", "", ""))


def _href_to_dav_path(base_url: str, href: str) -> str:
    href_path = unquote(urlsplit(href or "").path or "/")
    base_path = unquote(urlsplit(_normalize_dav_url(base_url)).path or "").rstrip("/")
    if base_path and href_path.startswith(base_path + "/"):
        href_path = href_path[len(base_path) :]
    elif base_path and href_path == base_path:
        href_path = "/"
    return _format_dav_path(href_path)


def _read_response_bytes(response: Any) -> bytes:
    headers = getattr(response, "headers", {}) or {}
    try:
        content_length = int(headers.get("content-length", 0) or 0)
    except (TypeError, ValueError):
        content_length = 0
    if content_length > MAX_WEBDAV_RESPONSE_BYTES:
        raise WebDavRepositoryError("WebDAV 响应超过大小限制")

    iterator = getattr(response, "iter_content", None)
    if callable(iterator):
        payload = bytearray()
        for chunk in iterator(chunk_size=64 * 1024):
            if not chunk:
                continue
            payload.extend(chunk)
            if len(payload) > MAX_WEBDAV_RESPONSE_BYTES:
                raise WebDavRepositoryError("WebDAV 响应超过大小限制")
        return bytes(payload)

    payload = bytes(getattr(response, "content", b"") or b"")
    if len(payload) > MAX_WEBDAV_RESPONSE_BYTES:
        raise WebDavRepositoryError("WebDAV 响应超过大小限制")
    return payload


def _element_text(parent: ET.Element, name: str) -> str:
    node = parent.find(f"d:{name}", DAV_NS)
    return (node.text or "").strip() if node is not None and node.text else ""


def _entry_from_response(response: ET.Element) -> WebDavEntry:
    href = response.findtext("d:href", default="", namespaces=DAV_NS)
    prop = None
    for propstat in response.findall("d:propstat", DAV_NS):
        status = propstat.findtext("d:status", default="", namespaces=DAV_NS)
        if "200" in status:
            prop = propstat.find("d:prop", DAV_NS)
            break
    if prop is None:
        prop = response.find(".//d:prop", DAV_NS)
    if prop is None:
        prop = ET.Element("prop")
    resource_type = prop.find("d:resourcetype", DAV_NS)
    is_dir = resource_type is not None and resource_type.find("d:collection", DAV_NS) is not None
    name = _element_text(prop, "displayname")
    if not name and href:
        name = unquote(href.rstrip("/").split("/")[-1])
    return WebDavEntry(
        href=href,
        name=name or "/",
        is_dir=is_dir,
        size=_element_text(prop, "getcontentlength"),
        modified=_element_text(prop, "getlastmodified"),
        content_type=_element_text(prop, "getcontenttype"),
    )


class WebDavRepository:
    def __init__(self, *, http_client: Any = default_http_client) -> None:
        self.http_client = http_client
        self._bindings_lock = threading.RLock()

    def load_bindings(self, bindings_path: str | Path) -> tuple[WebDavBinding, ...]:
        path = Path(bindings_path)
        try:
            with path.open("rb") as handle:
                payload = handle.read(MAX_WEBDAV_BINDINGS_BYTES + 1)
        except FileNotFoundError:
            return ()
        except OSError as exc:
            raise WebDavRepositoryError("WebDAV 绑定文件不可读取") from exc
        if len(payload) > MAX_WEBDAV_BINDINGS_BYTES:
            raise WebDavRepositoryError("WebDAV 绑定文件超过大小限制")
        try:
            rows = json.loads(payload)
        except (ValueError, RecursionError) as exc:
            raise WebDavRepositoryError("WebDAV 绑定文件格式无效") from exc
        if not isinstance(rows, list) or len(rows) > MAX_WEBDAV_BINDINGS_ROWS:
            raise WebDavRepositoryError("WebDAV 绑定文件格式无效")

        bindings: list[WebDavBinding] = []
        for index, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                raise WebDavRepositoryError("WebDAV 绑定文件格式无效")
            label = _display_text(row.get("label") or "WebDAV", MAX_WEBDAV_LABEL_CHARS) or "WebDAV"
            dav_url = _normalize_dav_url(_display_text(row.get("dav_url"), MAX_WEBDAV_URL_CHARS))
            username = _display_text(row.get("username"), MAX_WEBDAV_USERNAME_CHARS)
            password = row.get("password")
            if not dav_url or not username or not isinstance(password, str):
                raise WebDavRepositoryError("WebDAV 绑定文件格式无效")
            if len(password) > MAX_WEBDAV_PASSWORD_CHARS:
                raise WebDavRepositoryError("WebDAV 绑定文件格式无效")
            bindings.append(WebDavBinding(index, label, dav_url, username, password))
        return tuple(bindings)

    def resolve_target(
        self,
        bindings_path: str | Path,
        text: str,
        *,
        need_path: bool,
    ) -> tuple[WebDavBinding, str]:
        bindings = self.load_bindings(bindings_path)
        if not bindings:
            raise WebDavTargetError(
                "尚未绑定 WebDAV 账号。\n"
                "管理员绑定格式：webdav绑定 备注#https://站点/dav#用户名#密码"
            )
        raw = str(text or "").strip()
        index = 1
        path_text = raw
        if raw:
            first, _, rest = raw.partition(" ")
            if first.isdigit():
                index = int(first)
                path_text = rest.strip()
        if index < 1 or index > len(bindings):
            raise WebDavTargetError(f"序号 {index} 超出范围（1～{len(bindings)}）")
        if need_path and not path_text:
            raise WebDavTargetError("请填写路径，例如：webdav信息 1 /电影/test.mp4")
        return bindings[index - 1], _format_dav_path(path_text)

    @staticmethod
    def _binding_row(binding: WebDavBinding) -> dict[str, str]:
        return {
            "label": binding.label,
            "dav_url": binding.dav_url,
            "username": binding.username,
            "password": binding.password,
        }

    def _write_bindings(self, path: Path, bindings: tuple[WebDavBinding, ...]) -> None:
        payload = json.dumps(
            [self._binding_row(binding) for binding in bindings],
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")
        if len(payload) > MAX_WEBDAV_BINDINGS_BYTES:
            raise WebDavRepositoryError("WebDAV 绑定文件超过大小限制")

        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
            )
            temp_path = Path(temp_name)
            with os.fdopen(fd, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            temp_path = None
            try:
                directory_fd = os.open(path.parent, os.O_RDONLY)
            except OSError:
                directory_fd = None
            if directory_fd is not None:
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except OSError as exc:
            raise WebDavRepositoryError("WebDAV 绑定文件写入失败") from exc
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink()
                except FileNotFoundError:
                    pass

    def bind(
        self,
        bindings_path: str | Path,
        *,
        label: str,
        dav_url: str,
        username: str,
        password: str,
    ) -> WebDavMutationResult:
        path = Path(bindings_path)
        label_value = _display_text(label or "WebDAV", MAX_WEBDAV_LABEL_CHARS) or "WebDAV"
        url_value = _normalize_dav_url(_display_text(dav_url, MAX_WEBDAV_URL_CHARS))
        username_value = _display_text(username, MAX_WEBDAV_USERNAME_CHARS)
        if not url_value or not username_value or not isinstance(password, str) or not password:
            return WebDavMutationResult("rejected", "WebDAV 绑定参数无效")
        if len(password) > MAX_WEBDAV_PASSWORD_CHARS:
            return WebDavMutationResult("rejected", "WebDAV 绑定参数无效")

        with self._bindings_lock:
            bindings = self.load_bindings(path)
            if len(bindings) >= MAX_WEBDAV_BINDINGS_ROWS:
                return WebDavMutationResult("rejected", "WebDAV 绑定数量超过限制")
            if any(
                binding.dav_url == url_value and binding.username == username_value
                for binding in bindings
            ):
                return WebDavMutationResult("rejected", "已存在相同 WebDAV 地址和用户名的绑定")

            candidate = WebDavBinding(
                len(bindings) + 1,
                label_value,
                url_value,
                username_value,
                password,
            )
            self._propfind_binding(candidate, "/", depth="0")
            self._write_bindings(path, bindings + (candidate,))
            return WebDavMutationResult(
                "applied",
                f"已绑定第 {candidate.index} 个 WebDAV：{candidate.label} · {candidate.dav_url}",
            )

    def delete(self, bindings_path: str | Path, text: str) -> WebDavMutationResult:
        path = Path(bindings_path)
        with self._bindings_lock:
            bindings = self.load_bindings(path)
            if not bindings:
                return WebDavMutationResult("rejected", "当前没有 WebDAV 绑定")
            value = str(text or "").strip().casefold()
            if value in {"全部", "所有", "all", "*"}:
                self._write_bindings(path, ())
                return WebDavMutationResult(
                    "applied",
                    f"已删除全部 {len(bindings)} 个 WebDAV 绑定",
                    removed=bindings,
                )
            if not value.isdigit():
                return WebDavMutationResult("rejected", "删除用法：webdav删除 序号 或 webdav删除 全部")
            index = int(value)
            if index < 1 or index > len(bindings):
                return WebDavMutationResult("rejected", f"序号 {index} 超出范围（1～{len(bindings)}）")
            removed = bindings[index - 1]
            remaining = tuple(
                WebDavBinding(position, item.label, item.dav_url, item.username, item.password)
                for position, item in enumerate(bindings[: index - 1] + bindings[index:], start=1)
            )
            self._write_bindings(path, remaining)
            return WebDavMutationResult(
                "applied",
                f"已删除绑定 {index}：{removed.label or removed.dav_url}",
                removed=(removed,),
            )

    def propfind(
        self,
        bindings_path: str | Path,
        text: str,
        *,
        depth: str,
        need_path: bool,
    ) -> WebDavQueryResult:
        binding, path = self.resolve_target(bindings_path, text, need_path=need_path)
        return self._propfind_binding(binding, path, depth=depth)

    def _propfind_binding(
        self,
        binding: WebDavBinding,
        path: str,
        *,
        depth: str,
    ) -> WebDavQueryResult:
        response = self.http_client.request(
            "PROPFIND",
            _join_dav_url(binding.dav_url, path),
            data=(
                '<?xml version="1.0" encoding="utf-8" ?>\n'
                '<propfind xmlns="DAV:">\n'
                "  <prop>\n"
                "    <displayname />\n"
                "    <resourcetype />\n"
                "    <getcontentlength />\n"
                "    <getlastmodified />\n"
                "    <getcontenttype />\n"
                "  </prop>\n"
                "</propfind>"
            ).encode("utf-8"),
            headers={"Depth": depth, "Content-Type": "application/xml; charset=utf-8"},
            auth=(binding.username, binding.password),
            timeout=18,
            check_status=False,
            stream=True,
        )
        try:
            status = int(getattr(response, "status_code", 0))
            if status in {401, 403}:
                raise WebDavRepositoryError("认证失败，请检查用户名和密码")
            if status == 404:
                raise WebDavRepositoryError("路径不存在")
            if status not in {200, 207}:
                raise WebDavRepositoryError(f"WebDAV 返回 {status}")
            try:
                root = ET.fromstring(_read_response_bytes(response))
            except WebDavRepositoryError:
                raise
            except (ET.ParseError, ValueError) as exc:
                raise WebDavRepositoryError("WebDAV 响应不是合法 XML") from exc
            response_nodes = root.findall("d:response", DAV_NS)
            if len(response_nodes) > MAX_WEBDAV_RESPONSE_ENTRIES:
                raise WebDavRepositoryError("WebDAV 条目数量超过限制")
            entries = tuple(_entry_from_response(item) for item in response_nodes)
            return WebDavQueryResult(binding=binding, path=path, entries=entries)
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()


__all__ = [
    "MAX_WEBDAV_BINDINGS_BYTES",
    "MAX_WEBDAV_BINDINGS_ROWS",
    "MAX_WEBDAV_RESPONSE_BYTES",
    "MAX_WEBDAV_RESPONSE_ENTRIES",
    "WebDavRepository",
    "WebDavRepositoryError",
    "WebDavTargetError",
]
