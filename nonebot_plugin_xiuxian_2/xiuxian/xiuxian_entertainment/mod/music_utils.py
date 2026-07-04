import json
import time
from typing import Any, Optional

import requests
from nonebot.log import logger

from ...adapter_compat import Bot
from ...xiuxian_config import XiuConfig
from ...xiuxian_utils.utils import build_md_command_link, escape_markdown_text


# =========================
# 配置
# =========================
DEFAULT_CONFIG = {
    "default_platform": "netease",     # 默认平台
    "song_limit": 30,                  # 搜索返回数量
    "select_timeout": 45,              # 选歌超时秒数
    "api_base": "https://music.txqq.pro/",
    "page_size": 10,                    # 每页显示数量（翻页用）
}

_MUSIC_CONFIG = DEFAULT_CONFIG.copy()


def load_music_config() -> dict:
    """读取内置点歌配置，不再自动创建 music_config.json"""
    return _MUSIC_CONFIG.copy()


def save_music_config(cfg: dict):
    """更新本次运行中的点歌配置，不落盘。"""
    global _MUSIC_CONFIG
    merged = DEFAULT_CONFIG.copy()
    merged.update({k: v for k, v in cfg.items() if k in DEFAULT_CONFIG})
    _MUSIC_CONFIG = merged


def set_music_config(key: str, value: str) -> tuple[bool, str]:
    cfg = load_music_config()

    if key not in DEFAULT_CONFIG:
        return False, f"不支持的配置项：{key}"

    try:
        if key in ("song_limit", "select_timeout", "page_size"):
            value = int(value)
            if value <= 0:
                return False, f"{key} 必须大于0"
        else:
            value = str(value).strip()

        cfg[key] = value
        save_music_config(cfg)
        return True, f"配置已更新（本次运行生效）：{key} = {value}"
    except Exception as e:
        return False, f"配置更新失败：{e}"


# =========================
# 内存选歌会话
# =========================
MUSIC_SELECT_CACHE: dict[str, dict[str, Any]] = {}


def clean_expired_music_session():
    now = int(time.time())
    expired_keys = [
        uid for uid, data in MUSIC_SELECT_CACHE.items()
        if now > data.get("expire_at", 0)
    ]
    for uid in expired_keys:
        MUSIC_SELECT_CACHE.pop(uid, None)


def set_music_session(user_id: str, songs: list[dict], platform: str, timeout_sec: int, page_size: int = 5):
    clean_expired_music_session()
    MUSIC_SELECT_CACHE[str(user_id)] = {
        "songs": songs,
        "platform": platform,
        "created_at": int(time.time()),
        "expire_at": int(time.time()) + int(timeout_sec),
        "page": 1,
        "page_size": max(1, int(page_size)),
    }


def get_music_session(user_id: str) -> Optional[dict]:
    clean_expired_music_session()
    data = MUSIC_SELECT_CACHE.get(str(user_id))
    if not data:
        return None
    if int(time.time()) > data.get("expire_at", 0):
        MUSIC_SELECT_CACHE.pop(str(user_id), None)
        return None
    return data


def clear_music_session(user_id: str):
    MUSIC_SELECT_CACHE.pop(str(user_id), None)


# =========================
# 搜索
# =========================
PLATFORM_ALIAS = {
    "qq": ["qq点歌", "点歌", "qq音乐"],
    "netease": ["网易点歌", "网易云点歌", "网易云音乐"],
    "kugou": ["酷狗点歌", "酷狗音乐"],
    "kuwo": ["酷我点歌", "酷我音乐"],
    "baidu": ["百度点歌", "百度音乐"],
    "1ting": ["一听点歌", "一听音乐"],
    "migu": ["咪咕点歌", "咪咕音乐"],
    "lizhi": ["荔枝点歌", "荔枝fm"],
    "qingting": ["蜻蜓点歌", "蜻蜓fm"],
    "ximalaya": ["喜马点歌", "喜马拉雅"],
    "5singyc": ["5sing原创"],
    "5singfc": ["5sing翻唱"],
    "kg": ["全民k歌", "全民点歌"],
}


def detect_platform_from_cmd(cmd: str, fallback: str = "qq") -> str:
    cmd_lower = cmd.strip().lower()
    for platform, words in PLATFORM_ALIAS.items():
        for word in words:
            if word.lower() == cmd_lower:
                return platform
    return fallback


def search_music(keyword: str, platform: Optional[str] = None, limit: Optional[int] = None) -> list[dict]:
    cfg = load_music_config()
    api_base = cfg["api_base"]
    platform = platform or cfg["default_platform"]
    limit = limit or cfg["song_limit"]

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) "
            "Gecko/20100101 Firefox/146.0"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://music.txqq.pro",
        "Referer": "https://music.txqq.pro",
    }

    resp = requests.post(
        api_base,
        data={
            "input": keyword,
            "filter": "name",
            "type": platform,
            "page": 1,
        },
        headers=headers,
        timeout=15
    )
    resp.raise_for_status()

    text = (resp.text or "").strip()
    if not text:
        raise ValueError("接口返回为空")

    try:
        result = resp.json()
    except Exception:
        try:
            result = json.loads(text)
        except Exception:
            raise ValueError(f"接口返回非JSON：{text[:200]}")

    if not isinstance(result, dict):
        raise ValueError("接口返回格式错误")

    items = result.get("data") or result.get("songs") or []
    if not isinstance(items, list):
        raise ValueError("接口未返回歌曲列表")

    songs = []
    for s in items[:limit]:
        songs.append({
            "id": str(s.get("songid") or s.get("id") or ""),
            "name": s.get("title") or s.get("name") or "未知歌曲",
            "artists": s.get("author") or s.get("artists") or "未知歌手",
            "audio_url": s.get("url") or s.get("link"),
            "cover_url": s.get("pic"),
            "lyrics": s.get("lrc", ""),
        })
    return songs


# =========================
# 列表文案
# =========================
def build_song_list_page_text(
    platform_name: str,
    songs: list[dict],
    page: int,
    page_size: int,
    *,
    markdown: bool = False,
) -> tuple[str, int]:
    total = len(songs)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))

    start = (page - 1) * page_size
    end = start + page_size
    page_songs = songs[start:end]

    lines = [f"【搜索结果】{platform_name}（第 {page}/{total_pages} 页）", ""]
    for i, song in enumerate(page_songs, start=1):
        global_index = start + i
        item_text = (
            f"{global_index}. "
            f"{song.get('name', '未知')} - {song.get('artists', '未知')}"
        )
        line_text = (
            build_md_command_link(item_text, f"选歌 {global_index}")
            if markdown
            else item_text
        )
        lines.append(line_text)

    lines.append("")
    if markdown:
        prev_page = build_md_command_link("点歌上一页", "点歌上一页")
        next_page = build_md_command_link("点歌下一页", "点歌下一页")
        lines.append("> 点击蓝色歌曲条目或发送 `选歌 序号` 进行选择。")
        lines.append(f"翻页：{prev_page} / {next_page} / `点歌翻页 第N页`")
    else:
        lines.append("操作：发送【选歌 序号】进行选择。")
        lines.append("翻页：点歌上一页 / 点歌下一页 / 点歌翻页 第N页")
    return "\n".join(lines), total_pages


def _clean_song_field(value: Any, default: str) -> str:
    text = str(value if value not in (None, "") else default).strip()
    text = text.replace("\r", " ").replace("\n", " ")
    return text or default


def build_song_plain_text(song_name: str, artists: str) -> str:
    return f"【点歌】\n歌名：{song_name}\n歌手：{artists}"


def build_song_markdown_text(song_name: str, artists: str) -> str:
    retry_link = build_md_command_link("再搜此歌", f"点歌 {song_name}")
    help_link = build_md_command_link("点歌帮助", "点歌帮助")
    return "\n".join(
        [
            "**点歌**",
            "",
            f"> **歌名**：{escape_markdown_text(song_name)}",
            f"> **歌手**：{escape_markdown_text(artists)}",
            "",
            f"{retry_link} / {help_link}",
        ]
    )


# =========================
# 发送（图文 / 原生MD / 普通文本）
# =========================
async def send_song_rich(bot: Bot, event, song: dict) -> tuple[bool, str]:
    """
    发送顺序：
    1) 有封面时优先普通图文混合，保证封面和文本在同一条消息
    2) 无封面时原生MD文字（若开启，频道会自动降级普通文本）
    3) 普通文本
    每条文本后补发音频（若有）
    """
    from ..command import handle_audio_send
    from ...xiuxian_utils.utils import handle_pic_msg_send, handle_send

    config = XiuConfig()
    song_name = _clean_song_field(song.get("name"), "未知歌曲")
    artists = _clean_song_field(song.get("artists"), "未知歌手")
    cover_url = song.get("cover_url")
    audio_url = song.get("audio_url")

    text_msg = build_song_plain_text(song_name, artists)

    # ===== 1) 封面 + 文本同条发送 =====
    if cover_url:
        try:
            await handle_pic_msg_send(bot, event, cover_url, text_msg)

            if audio_url:
                try:
                    await handle_audio_send(bot, event, audio_url)
                    return True, "发送成功"
                except Exception as e:
                    logger.warning(f"点歌音频发送失败：{e}")
                    return False, f"【{song_name} - {artists}】音频发送失败：{e}"
            return False, f"【{song_name} - {artists}】无可用音频链接"

        except Exception as e:
            logger.warning(f"点歌图文发送失败，准备降级文本：{e}")

    # ===== 2) 无封面或图文失败时，Markdown文字 =====
    if config.markdown_status:
        try:
            await handle_send(
                bot,
                event,
                build_song_markdown_text(song_name, artists),
                native_markdown=True,
                fallback_msg=text_msg,
                keyboard_rows=[
                    [("再搜此歌", f"点歌 {song_name}"), ("点歌帮助", "点歌帮助")]
                ],
                at_msg=False,
            )

            if audio_url:
                try:
                    await handle_audio_send(bot, event, audio_url)
                    return True, "发送成功"
                except Exception as e:
                    logger.warning(f"点歌音频发送失败：{e}")
                    return False, f"【{song_name} - {artists}】音频发送失败：{e}"
            return False, f"【{song_name} - {artists}】无可用音频链接"

        except Exception as e:
            logger.warning(f"点歌 Markdown发送失败：{e}")

    # ===== 3) 普通文本 =====
    try:
        await bot.send(event=event, message=text_msg)

        if audio_url:
            await handle_audio_send(bot, event, audio_url)
            return True, "发送成功"

        return False, f"【{song_name} - {artists}】无可用音频链接"
    except Exception as e:
        logger.warning(f"点歌 普通图文发送失败: {e}")
        return False, f"【{song_name} - {artists}】发送失败：{e}"


def get_platform_display_name(platform: str) -> str:
    mapping = {
        "qq": "QQ音乐",
        "netease": "网易云音乐",
        "kugou": "酷狗音乐",
        "kuwo": "酷我音乐",
        "baidu": "百度音乐",
        "1ting": "一听音乐",
        "migu": "咪咕音乐",
        "lizhi": "荔枝FM",
        "qingting": "蜻蜓FM",
        "ximalaya": "喜马拉雅",
        "5singyc": "5sing原创",
        "5singfc": "5sing翻唱",
        "kg": "全民K歌",
    }
    return mapping.get(platform, platform)
