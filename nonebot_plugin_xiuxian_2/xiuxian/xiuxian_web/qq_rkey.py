"""QQ multimedia rkey 日缓存。

消息里的 multimedia URL 带 rkey，会过期；渲染/代理下载时用当天最新 rkey 替换。
策略：
- 成功下载或消息库中出现新 rkey 时写入当天缓存
- 当天只保留一份最新 rkey，不重复抓取
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

_RKEY_RE = re.compile(r"(?:[?&])rkey=([^&]+)", re.I)
_CACHE_NAME = "qq_rkey_day_cache.json"


def _cache_path() -> Path:
    try:
        from ...paths import get_paths

        base = Path(get_paths().data)
    except Exception:
        base = Path("data") / "xiuxian"
    base.mkdir(parents=True, exist_ok=True)
    return base / _CACHE_NAME


def extract_rkey(url: str) -> str:
    m = _RKEY_RE.search(str(url or ""))
    if not m:
        return ""
    return str(m.group(1) or "").strip()


def is_multimedia_url(url: str) -> bool:
    try:
        host = (urlparse(str(url or "")).hostname or "").lower()
    except Exception:
        return False
    return host == "multimedia.nt.qq.com.cn" or host.endswith(".nt.qq.com.cn")


def replace_rkey(url: str, rkey: str) -> str:
    url = str(url or "").strip()
    rkey = str(rkey or "").strip()
    if not url or not rkey:
        return url
    try:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=True)
        if "rkey" not in qs and "rkey" not in (parsed.query or ""):
            # 没有 rkey 参数则追加
            qs["rkey"] = [rkey]
        else:
            qs["rkey"] = [rkey]
        # 保持单值
        flat = [(k, v[-1] if isinstance(v, list) and v else "") for k, v in qs.items()]
        new_query = urlencode(flat, doseq=False)
        return urlunparse(parsed._replace(query=new_query))
    except Exception:
        # 兜底正则替换
        if "rkey=" in url:
            return _RKEY_RE.sub(lambda m: m.group(0).split("=")[0] + "=" + rkey, url, count=1)
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}rkey={rkey}"


def _read_cache() -> dict[str, Any]:
    path = _cache_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_cache(data: dict[str, Any]) -> None:
    path = _cache_path()
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def get_day_rkey() -> str:
    data = _read_cache()
    today = date.today().isoformat()
    if str(data.get("day") or "") != today:
        return ""
    return str(data.get("rkey") or "").strip()


def save_day_rkey(rkey: str, *, source: str = "") -> str:
    rkey = str(rkey or "").strip()
    if not rkey:
        return get_day_rkey()
    today = date.today().isoformat()
    data = _read_cache()
    # 当天已是同一 rkey：不重复写
    if str(data.get("day") or "") == today and str(data.get("rkey") or "") == rkey:
        return rkey
    data = {
        "day": today,
        "rkey": rkey,
        "source": str(source or "")[:200],
        "updated_at": date.today().isoformat(),
    }
    try:
        _write_cache(data)
    except Exception:
        pass
    return rkey


def remember_rkey_from_url(url: str, *, source: str = "url") -> str:
    if not is_multimedia_url(url):
        return get_day_rkey()
    rkey = extract_rkey(url)
    if not rkey:
        return get_day_rkey()
    return save_day_rkey(rkey, source=source)


def apply_day_rkey(url: str) -> str:
    """渲染/代理前：若有当天 rkey，则替换 URL 中的 rkey。"""
    if not is_multimedia_url(url):
        return url
    day_rkey = get_day_rkey()
    if not day_rkey:
        # 尝试从当前 url 学习
        remember_rkey_from_url(url, source="seen")
        return url
    old = extract_rkey(url)
    if old == day_rkey:
        return url
    return replace_rkey(url, day_rkey)


def learn_rkey_from_message_db(limit: int = 80) -> str:
    """从最近消息里找最新 rkey，写入当天缓存。"""
    existing = get_day_rkey()
    try:
        from .core import get_message_db_connection
    except Exception:
        try:
            from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web.core import (  # type: ignore
                get_message_db_connection,
            )
        except Exception:
            return existing

    try:
        conn = get_message_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT content FROM messages ORDER BY id DESC LIMIT ?",
            (int(limit),),
        )
        for (content,) in cur.fetchall():
            text = str(content or "")
            if "rkey=" not in text or "multimedia" not in text:
                continue
            m = re.search(
                r"https?://multimedia\.nt\.qq\.com\.cn/download[^\s\"'<>]+",
                text,
            )
            if not m:
                m = re.search(r"https?://[^\s\"'<>]*rkey=[^\s\"'<>]+", text)
            if not m:
                continue
            rkey = extract_rkey(m.group(0))
            if rkey:
                return save_day_rkey(rkey, source="message.db")
    except Exception:
        pass
    return existing
