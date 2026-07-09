"""娱乐模块 — 流媒体解析（复刻 astrbot_plugin_media_parser 行为开关）。"""
from pathlib import Path
from ....paths import get_paths

# 与上游 PARSER_OUTPUT_KEYS 一致，默认全部发送
_DEFAULT_OUTPUT = "全部发送"
_PLATFORMS = (
    "bilibili",
    "douyin",
    "tiktok",
    "kuaishou",
    "weibo",
    "xiaohongshu",
    "xianyu",
    "toutiao",
    "xiaoheihe",
    "twitter",
)


def default_raw_config() -> dict:
    parsers_out = {k: _DEFAULT_OUTPUT for k in _PLATFORMS}
    cache_dir = str((get_paths().data / "media_parser_cache").resolve())
    return {
        "trigger": {
            "auto_parse": True,
            "keywords": ["视频解析", "解析视频", "链接解析"],
            "reply_trigger": False,
        },
        "parsers": parsers_out,
        "message": {
            "pack_mode": "不打包",
            "parser_outputs": parsers_out,
            "hot_comment_count": 0,
            "hot_comment_bilibili": False,
            "hot_comment_weibo": False,
            "hot_comment_xiaohongshu": False,
            "opening_enabled": False,
            "quote_user_message": False,
        },
        "permissions": {
            "admin_id": "",
            "whitelist_users": [],
            "blacklist_users": [],
            "whitelist_groups": [],
            "blacklist_groups": [],
        },
        "download": {
            "max_video_size_mb": 500,
            "large_video_threshold_mb": 80,
            "cache_dir": cache_dir,
            "max_concurrent_downloads": 3,
        },
        "proxy": {
            "address": "",
            "enable_tiktok_proxy": False,
            "enable_twitter_parse_proxy": False,
            "enable_twitter_image_proxy": True,
            "enable_twitter_video_proxy": True,
            "enable_xiaoheihe_video_proxy": True,
        },
        "bilibili_enhanced": {
            "use_cookie": False,
            "cookie": "",
            "max_quality": "1080P",
            "enable_admin_assist": False,
            "admin_assist_reply_timeout_minutes": 1440,
            "admin_assist_request_cooldown_minutes": 1440,
        },
        "media_relay": {"enable": False, "callback_api_base": "", "file_token_ttl": 300},
        "translation": {"enabled": False},
        "admin": {"clean_cache_keyword": "清理媒体缓存", "debug_mode": False},
    }


class MediaParserFunConfig:
    """运行时配置（可从 xiuxian 配置扩展，当前用默认值）。"""

    def __init__(self):
        self._raw = default_raw_config()
        self.auto_parse: bool = bool(self._raw["trigger"]["auto_parse"])
        self.keywords: list[str] = list(self._raw["trigger"]["keywords"])

    def should_parse_message(self, text: str) -> bool:
        t = (text or "").strip()
        if not t:
            return False
        if self.auto_parse:
            return True
        for kw in self.keywords:
            if kw and kw in t:
                return True
        return False

    def as_upstream_dict(self) -> dict:
        return self._raw


_fun_cfg = MediaParserFunConfig()


def get_fun_media_parser_config() -> MediaParserFunConfig:
    return _fun_cfg
