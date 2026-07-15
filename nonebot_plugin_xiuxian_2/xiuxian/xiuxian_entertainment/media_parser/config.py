"""娱乐模块 — 流媒体解析配置（本插件原生解析）。"""
from ....paths import get_paths

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
        },
        "download": {
            "cache_dir": cache_dir,
            "max_video_size_mb": 500,
        },
        "proxy": {"address": ""},
        "bilibili_enhanced": {"use_cookie": False, "cookie": "", "max_quality": "1080P"},
        "media_relay": {"enable": False},
    }


class MediaParserFunConfig:
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
