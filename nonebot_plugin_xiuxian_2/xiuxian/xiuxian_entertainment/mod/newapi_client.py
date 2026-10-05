"""NewAPI 站点 HTTP 客户端。"""
from __future__ import annotations

import json
import re
from typing import Any, Iterable

from ...xiuxian_utils.http_proxy import HttpClient, describe_proxy_request_error, http_client

_UA = (
    "Mozilla/5.0 (Linux; Android; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36"
)
MAX_CHECKIN_RESPONSE_BYTES = 1024 * 1024
MAX_INFO_RESPONSE_BYTES = 512 * 1024
INFO_REQUEST_TIMEOUT = (5, 15)
CHECKIN_REQUEST_TIMEOUT = (8, 45)
checkin_http_client = HttpClient(timeout=CHECKIN_REQUEST_TIMEOUT, retries=0)
info_http_client = HttpClient(timeout=INFO_REQUEST_TIMEOUT, retries=0)


def normalize_base_url(raw: str | None) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = s.rstrip("/")
    if not re.match(r"^https?://", s, re.I):
        s = f"https://{s}"
    return s


def account_base_url(stored: str | None) -> str | None:
    n = normalize_base_url(stored)
    return n if n else None


def normalize_cookie_header(raw: str) -> str:
    s = (raw or "").strip()
    s = re.sub(r"^[Cc][Oo][Oo][Kk][Ii][Ee]:\s*", "", s)
    if not s:
        return ""
    if "=" in s:
        return s
    return f"session={s}"


def detect_auth_mode(secret: str) -> str:
    s = (secret or "").strip()
    if re.match(r"^https?://", s, re.I):
        return "token"
    low = s.lower()
    if "session=" in low or (low.count(";") >= 1 and "=" in low):
        return "cookie"
    if re.match(r"^session=", s, re.I):
        return "cookie"
    return "token"


def bytes_to_human(quota: Any) -> str:
    if quota is None or quota == "null":
        return "💰"
    try:
        n = int(float(quota))
    except (TypeError, ValueError):
        return "💰"
    mb = n / 500_000
    return f"{mb:.2f} 💰"


def _build_headers(mode: str, api_user_id: str, secret: str, base_url: str) -> dict[str, str]:
    h = {
        "new-api-user": str(api_user_id),
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
        "sec-ch-ua-platform": '"Android"',
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Android WebView";v="146"',
        "sec-ch-ua-mobile": "?1",
        "origin": base_url,
        "referer": f"{base_url}/console/personal",
        "x-requested-with": "mark.via",
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "priority": "u=1,i",
    }
    if mode == "cookie":
        cookie = normalize_cookie_header(secret)
        h["Cookie"] = cookie
    else:
        h["Authorization"] = f"Bearer {secret.strip()}"
    return h


def _request(
    method: str,
    base_url: str,
    path: str,
    *,
    mode: str,
    api_user_id: str,
    secret: str,
    json_body: Any = None,
    timeout: tuple[float, float] = (8, 45),
    max_response_bytes: int | None = None,
    client: Any = None,
    response_limit_message: str = "响应超过大小上限",
) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}{path}"
    headers = _build_headers(mode, api_user_id, secret, base_url)
    try:
        request_client = client or http_client
        resp = request_client.request(
            method,
            url,
            timeout=timeout,
            headers=(
                headers
                if method.upper() == "GET"
                else {**headers, "Content-Type": "application/json"}
            ),
            json=None if method.upper() == "GET" else (json_body or {}),
            check_status=False,
            stream=max_response_bytes is not None,
        )
    except Exception as e:
        return {"_error": describe_proxy_request_error(e), "_raw": ""}
    try:
        if max_response_bytes is None:
            text = (resp.text or "").strip()
            if not text:
                return {"_error": f"无响应（HTTP {getattr(resp, 'status_code', '?')}）", "_raw": ""}
            try:
                data = resp.json()
                if isinstance(data, dict):
                    return data
            except (ValueError, RecursionError):
                pass
            return {"_error": "响应非 JSON", "_raw": text[:500]}
        else:
            limit = int(max_response_bytes)
            if limit < 1:
                raise ValueError("max_response_bytes must be positive")
            try:
                content_length = int(resp.headers.get("content-length", 0) or 0)
            except (AttributeError, TypeError, ValueError):
                content_length = 0
            if content_length > limit:
                return {"_error": response_limit_message, "_raw": ""}
            content = bytearray()
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                if len(content) + len(chunk) > limit:
                    return {"_error": response_limit_message, "_raw": ""}
                content.extend(chunk)
            text = bytes(content).decode(getattr(resp, "encoding", None) or "utf-8", errors="replace").strip()
        if not text:
            return {"_error": f"无响应（HTTP {getattr(resp, 'status_code', '?')}）", "_raw": ""}
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                return data
        except (json.JSONDecodeError, RecursionError):
            pass
        return {"_error": "响应非 JSON", "_raw": text[:500]}
    except Exception as e:
        return {"_error": describe_proxy_request_error(e), "_raw": ""}
    finally:
        try:
            resp.close()
        except Exception:
            pass


def fetch_user_self(mode: str, api_user_id: str, secret: str, base_url: str) -> dict[str, Any]:
    data = _request(
        "GET",
        base_url,
        "/api/user/self",
        mode=mode,
        api_user_id=api_user_id,
        secret=secret,
        timeout=INFO_REQUEST_TIMEOUT,
        max_response_bytes=MAX_INFO_RESPONSE_BYTES,
        client=info_http_client,
        response_limit_message="用户信息响应超过 512 KiB 上限",
    )
    if data.get("_error"):
        return data
    if data.get("success") is False and data.get("message"):
        data["_error"] = str(data.get("message"))
    return data


def do_checkin(mode: str, api_user_id: str, secret: str, base_url: str) -> dict[str, Any]:
    data = _request(
        "POST",
        base_url,
        "/api/user/checkin",
        mode=mode,
        api_user_id=api_user_id,
        secret=secret,
        timeout=CHECKIN_REQUEST_TIMEOUT,
        max_response_bytes=MAX_CHECKIN_RESPONSE_BYTES,
        client=checkin_http_client,
        response_limit_message="签到响应超过 1 MiB 上限",
    )
    if data.get("_error"):
        return data
    return data


def format_user_info_block(index: int, acc: dict[str, Any], data: dict[str, Any]) -> str:
    base = account_base_url(acc.get("base_url")) or "—"
    api_id = acc.get("api_user_id", "?")
    lines = [f"【{index}】站点用户 {api_id} · {base}"]

    if data.get("_error"):
        lines.append(f"获取失败：{data['_error']}")
        return "\n".join(lines)

    inner = data.get("data") if isinstance(data.get("data"), dict) else data
    if not isinstance(inner, dict):
        inner = data
    if data.get("success") is False and not inner.get("username"):
        err = data.get("message") or inner.get("message")
        if err:
            lines.append(f"获取失败：{err}")
            return "\n".join(lines)

    username = inner.get("username") or inner.get("display_name") or "—"
    display_name = inner.get("display_name") or ""
    uid = inner.get("id", api_id)
    group = inner.get("group") or "—"
    quota = inner.get("quota")
    used = inner.get("used_quota")
    req_count = inner.get("request_count", "—")

    lines.append(f"昵称：{username}")
    if display_name and display_name != username:
        lines.append(f"显示名：{display_name}")
    lines.append(f"ID：{uid}")
    lines.append(f"群组：{group}")
    lines.append(f"余额：{bytes_to_human(quota)}")
    lines.append(f"消耗：{bytes_to_human(used)}")
    lines.append(f"次数：{req_count}")
    return "\n".join(lines)


def format_user_info_reply(blocks: Iterable[str], *, max_bytes: int = 64 * 1024) -> str:
    title = "【NewAPI 用户信息】"
    notice = "部分用户信息因消息长度限制未展示。"
    accepted: list[str] = []
    reply_bytes = len((title + "\n\n").encode("utf-8"))
    omitted = False

    for block in blocks:
        block_bytes = len((block + "\n\n").encode("utf-8"))
        if not omitted and reply_bytes + block_bytes <= max_bytes:
            accepted.append(block)
            reply_bytes += block_bytes
        else:
            omitted = True

    if omitted:
        notice_bytes = len(notice.encode("utf-8"))
        while accepted and reply_bytes + notice_bytes > max_bytes:
            removed = accepted.pop()
            reply_bytes -= len((removed + "\n\n").encode("utf-8"))
        accepted.append(notice)

    return "\n\n".join([title, *accepted]).strip()


def format_checkin_block(index: int, acc: dict[str, Any], data: dict[str, Any]) -> str:
    base = account_base_url(acc.get("base_url")) or "—"
    api_id = acc.get("api_user_id", "?")
    lines = [f"【{index}】站点用户 {api_id} · {base}"]

    if data.get("_error"):
        lines.append(f"签到失败：{data['_error']}")
        return "\n".join(lines)

    inner_msg = None
    if isinstance(data.get("data"), dict):
        inner_msg = data["data"].get("message")
    message = str(data.get("message") or inner_msg or "—")
    success = data.get("success")
    if success is None and isinstance(data.get("data"), dict):
        success = data["data"].get("success")

    quota_awarded = data.get("quota_awarded")
    checkin_date = data.get("checkin_date")
    if quota_awarded is None and isinstance(data.get("data"), dict):
        quota_awarded = data["data"].get("quota_awarded")
        checkin_date = data["data"].get("checkin_date") or checkin_date

    lines.append(f"结果：{message}")
    if success in (True, "true", 1):
        try:
            q = int(float(quota_awarded or 0))
            if q > 0:
                lines.append(f"获得额度：{bytes_to_human(q)}")
                if checkin_date:
                    lines.append(f"签到日期：{checkin_date}")
        except (TypeError, ValueError):
            pass
    elif "已签到" in message or "已经签到" in message or "already" in message.lower():
        lines.append("（今日已签）")
    return "\n".join(lines)


def summarize_checkin_for_history(data: dict[str, Any]) -> str:
    if data.get("_error"):
        return f"失败：{data['_error']}"
    inner_msg = None
    if isinstance(data.get("data"), dict):
        inner_msg = data["data"].get("message")
    message = str(data.get("message") or inner_msg or "—")
    success = data.get("success")
    if success is None and isinstance(data.get("data"), dict):
        success = data["data"].get("success")
    quota_awarded = data.get("quota_awarded")
    if quota_awarded is None and isinstance(data.get("data"), dict):
        quota_awarded = data["data"].get("quota_awarded")
    if success in (True, "true", 1):
        try:
            q = int(float(quota_awarded or 0))
            if q > 0:
                return f"成功 · {message} · +{bytes_to_human(q)}"
        except (TypeError, ValueError):
            pass
        return f"成功 · {message}"
    if "已签到" in message or "已经签到" in message:
        return f"已签 · {message}"
    return f"失败 · {message}"
