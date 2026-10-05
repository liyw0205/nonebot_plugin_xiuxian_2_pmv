from __future__ import annotations

import copy
import math
import threading
import time
from collections import OrderedDict
from typing import Any, Callable
from urllib.parse import urlsplit

from .external_query import EntertainmentExternalQueryProvider

DEFAULT_MUSIC_CONFIG = {
    "default_platform": "netease",
    "song_limit": 30,
    "select_timeout": 120,
    "api_base": "https://music.txqq.pro/",
    "page_size": 10,
}
MUSIC_PLATFORMS = frozenset(
    {
        "qq", "netease", "kugou", "kuwo", "baidu", "1ting", "migu",
        "lizhi", "qingting", "ximalaya", "5singyc", "5singfc", "kg",
    }
)
MUSIC_SEARCH_MAX_LIMIT = 50
MUSIC_SEARCH_MAX_PAGES = 5
MUSIC_SEARCH_PAGE_SIZE = 10
MUSIC_SEARCH_RESPONSE_MAX_BYTES = 512 * 1024
MUSIC_SEARCH_REQUEST_TIMEOUT_SECONDS = 5.0
MUSIC_SEARCH_TOTAL_BUDGET_SECONDS = 20.0
MUSIC_SELECT_MAX_SESSIONS = 128
MUSIC_SELECT_MAX_SESSION_BYTES = 512 * 1024

_PLATFORM_ALIASES = {
    "qq": ("qq点歌", "qq音乐"),
    "netease": ("网易点歌", "网易云点歌", "网易云音乐"),
    "kugou": ("酷狗点歌", "酷狗音乐"),
    "kuwo": ("酷我点歌", "酷我音乐"),
    "baidu": ("百度点歌", "百度音乐"),
    "1ting": ("一听点歌", "一听音乐"),
    "migu": ("咪咕点歌", "咪咕音乐"),
    "lizhi": ("荔枝点歌", "荔枝fm"),
    "qingting": ("蜻蜓点歌", "蜻蜓fm"),
    "ximalaya": ("喜马点歌", "喜马拉雅"),
    "5singyc": ("5sing原创",),
    "5singfc": ("5sing翻唱",),
    "kg": ("全民k歌", "全民点歌"),
}


class EntertainmentMusicApplication:
    """Owns volatile music configuration and bounded, short-lived selections."""

    def __init__(
        self,
        external_queries: EntertainmentExternalQueryProvider,
        *,
        clock: Callable[[], float] = time.monotonic,
        max_sessions: int = MUSIC_SELECT_MAX_SESSIONS,
    ) -> None:
        self.external_queries = external_queries
        self._clock = clock
        self._max_sessions = max(1, int(max_sessions))
        self._lock = threading.RLock()
        self._config = dict(DEFAULT_MUSIC_CONFIG)
        self._sessions: OrderedDict[str, dict[str, Any]] = OrderedDict()

    @staticmethod
    def detect_platform(cmd: str, fallback: str = "netease") -> str:
        cmd_lower = cmd.strip().lower()
        if cmd_lower in {"点歌", "音乐", "music"}:
            return fallback
        for platform, aliases in _PLATFORM_ALIASES.items():
            if cmd_lower in {alias.lower() for alias in aliases}:
                return platform
        return fallback

    def load_config(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._config)

    def set_config(self, key: str, value: str) -> tuple[bool, str]:
        if key not in DEFAULT_MUSIC_CONFIG:
            return False, f"不支持的配置项：{key}"
        try:
            if key in ("song_limit", "select_timeout", "page_size"):
                parsed = int(value)
                bounds = {
                    "song_limit": (1, MUSIC_SEARCH_MAX_LIMIT),
                    "select_timeout": (1, 3600),
                    "page_size": (1, 20),
                }
                low, high = bounds[key]
                if not low <= parsed <= high:
                    return False, f"{key} 必须在 {low}~{high} 之间"
                value = parsed
            elif key == "default_platform":
                value = str(value).strip().lower()
                if value not in MUSIC_PLATFORMS:
                    return False, f"不支持的点歌平台：{value}"
            else:
                value = str(value).strip()
                parsed_url = urlsplit(value)
                if (
                    len(value) > 2048
                    or parsed_url.scheme not in {"http", "https"}
                    or not parsed_url.hostname
                    or parsed_url.username
                    or parsed_url.password
                ):
                    return False, "api_base 必须是有效的 HTTP(S) URL"
                _ = parsed_url.port

            with self._lock:
                self._config[key] = value
            return True, f"配置已更新（本次运行生效）：{key} = {value}"
        except (TypeError, ValueError, OverflowError) as exc:
            return False, f"配置更新失败：{exc}"

    @staticmethod
    def _bounded_text(value: Any, limit: int, fallback: str = "") -> str:
        text = str(value if value not in (None, "") else fallback)
        return text[:limit]

    @classmethod
    def _song_dto(cls, item: dict[str, Any], platform: str) -> dict[str, str]:
        artists = item.get("author") or item.get("artists") or "未知歌手"
        if isinstance(artists, list):
            artists = ", ".join(str(value) for value in artists[:8])
        fields = {
            "id": cls._bounded_text(item.get("songid") or item.get("id"), 256),
            "name": cls._bounded_text(item.get("title") or item.get("name"), 256, "未知歌曲"),
            "artists": cls._bounded_text(artists, 256, "未知歌手"),
            "audio_url": cls._bounded_text(item.get("url") or item.get("link_audio") or item.get("audio") or item.get("audio_url"), 2048),
            "cover_url": cls._bounded_text(item.get("pic") or item.get("cover") or item.get("cover_url"), 2048),
            "page_url": cls._bounded_text(item.get("link") or item.get("page") or item.get("page_url"), 2048),
            "platform": cls._bounded_text(item.get("type") or platform, 64),
            "lyrics": cls._bounded_text(item.get("lrc"), 8192),
        }
        if fields["cover_url"].startswith("http://"):
            fields["cover_url"] = "https://" + fields["cover_url"][7:]
        return fields

    def search(self, keyword: str, platform: str | None = None) -> list[dict[str, str]]:
        keyword = str(keyword).strip()
        if not keyword or len(keyword) > 128:
            raise ValueError("点歌关键词长度必须为 1~128 个字符")
        config = self.load_config()
        platform = str(platform or config["default_platform"])
        if platform not in MUSIC_PLATFORMS:
            platform = str(config["default_platform"])
        limit = min(MUSIC_SEARCH_MAX_LIMIT, int(config["song_limit"]))
        max_pages = min(
            MUSIC_SEARCH_MAX_PAGES,
            max(1, math.ceil(limit / MUSIC_SEARCH_PAGE_SIZE)),
        )
        started_at = self._clock()
        deadline = started_at + MUSIC_SEARCH_TOTAL_BUDGET_SECONDS
        raw_items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) "
                "Gecko/20100101 Firefox/146.0"
            ),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
        }
        parsed_base = urlsplit(str(config["api_base"]))
        origin = f"{parsed_base.scheme}://{parsed_base.netloc}"
        headers["Origin"] = origin
        headers["Referer"] = origin

        for page in range(1, max_pages + 1):
            remaining = deadline - self._clock()
            if remaining <= 0:
                if raw_items:
                    break
                raise TimeoutError("点歌搜索超过总时间预算")
            result = self.external_queries.post_form_json(
                str(config["api_base"]),
                {
                    "input": keyword,
                    "filter": "name",
                    "type": platform,
                    "page": page,
                },
                headers=headers,
                timeout=min(MUSIC_SEARCH_REQUEST_TIMEOUT_SECONDS, remaining),
                total_timeout=remaining,
                max_bytes=MUSIC_SEARCH_RESPONSE_MAX_BYTES,
            )
            if not isinstance(result, dict):
                if page == 1:
                    raise ValueError("接口返回格式错误")
                break
            items = result.get("data") or result.get("songs") or []
            if not isinstance(items, list) or not items:
                if page == 1 and not items:
                    return []
                break

            new_count = 0
            for item in items:
                if not isinstance(item, dict):
                    continue
                song_id = str(item.get("songid") or item.get("id") or "")
                if song_id and song_id in seen_ids:
                    continue
                if song_id:
                    seen_ids.add(song_id)
                raw_items.append(item)
                new_count += 1
                if len(raw_items) >= limit:
                    break
            if len(raw_items) >= limit or new_count == 0 or len(items) < MUSIC_SEARCH_PAGE_SIZE:
                break

        return [self._song_dto(item, platform) for item in raw_items[:limit]]

    def save_selection(
        self,
        user_id: str,
        songs: list[dict[str, Any]],
        platform: str,
    ) -> None:
        user_id = str(user_id)
        if not user_id or len(user_id) > 512 or not songs:
            raise ValueError("invalid music selection session")
        config = self.load_config()
        safe_songs = [self._song_dto(song, platform) for song in songs[:MUSIC_SEARCH_MAX_LIMIT]]
        payload_size = sum(
            len(str(value).encode("utf-8"))
            for song in safe_songs
            for value in song.values()
        )
        if payload_size > MUSIC_SELECT_MAX_SESSION_BYTES:
            raise ValueError("music selection session exceeds storage limit")
        now = self._clock()
        session = {
            "songs": safe_songs,
            "platform": platform,
            "created_at": now,
            "expire_at": now + int(config["select_timeout"]),
            "page": 1,
            "page_size": int(config["page_size"]),
        }
        with self._lock:
            self._sessions[user_id] = session
            self._sessions.move_to_end(user_id)
            while len(self._sessions) > self._max_sessions:
                self._sessions.popitem(last=False)

    def _active_session(self, user_id: str) -> dict[str, Any] | None:
        user_id = str(user_id)
        session = self._sessions.get(user_id)
        if session is None:
            return None
        if self._clock() > session["expire_at"]:
            self._sessions.pop(user_id, None)
            return None
        self._sessions.move_to_end(user_id)
        return session

    def get_selection(self, user_id: str) -> dict[str, Any] | None:
        with self._lock:
            session = self._active_session(user_id)
            return copy.deepcopy(session) if session is not None else None

    def set_selection_page(self, user_id: str, page: int) -> dict[str, Any] | None:
        with self._lock:
            session = self._active_session(user_id)
            if session is None:
                return None
            page_size = max(1, int(session["page_size"]))
            total_pages = max(1, math.ceil(len(session["songs"]) / page_size))
            session["page"] = max(1, min(int(page), total_pages))
            return copy.deepcopy(session)

    def select_song(self, user_id: str, index: int) -> dict[str, Any] | None:
        with self._lock:
            session = self._active_session(user_id)
            if session is None or not 1 <= int(index) <= len(session["songs"]):
                return None
            session["expire_at"] = self._clock() + int(self._config["select_timeout"])
            return copy.deepcopy(session["songs"][int(index) - 1])


__all__ = [
    "DEFAULT_MUSIC_CONFIG",
    "MUSIC_PLATFORMS",
    "MUSIC_SEARCH_MAX_LIMIT",
    "MUSIC_SEARCH_MAX_PAGES",
    "MUSIC_SEARCH_RESPONSE_MAX_BYTES",
    "MUSIC_SEARCH_TOTAL_BUDGET_SECONDS",
    "MUSIC_SELECT_MAX_SESSION_BYTES",
    "MUSIC_SELECT_MAX_SESSIONS",
    "EntertainmentMusicApplication",
]
