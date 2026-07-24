import json
import time
from typing import Any, Optional

from nonebot.log import logger

from ...adapter_compat import Bot
from ...messaging.delivery import delivery_service
from ...xiuxian_utils.http_proxy import http_client
from ...xiuxian_config import XiuConfig
from ...xiuxian_utils.utils import build_md_command_link, escape_markdown_text


# =========================
# 配置
# =========================
DEFAULT_CONFIG = {
    "default_platform": "netease",     # 默认平台：点歌 / 网易点歌
    "song_limit": 30,                  # 搜索返回数量
    "select_timeout": 120,             # 选歌列表保留秒数（重搜/超时才清）
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


def touch_music_session(user_id: str, timeout_sec: int | None = None) -> Optional[dict]:
    """选歌后刷新过期时间，不清理列表。"""
    clean_expired_music_session()
    data = MUSIC_SELECT_CACHE.get(str(user_id))
    if not data:
        return None
    cfg = load_music_config()
    ttl = int(timeout_sec if timeout_sec is not None else cfg.get("select_timeout", 120))
    data["expire_at"] = int(time.time()) + max(1, ttl)
    return data


def clear_music_session(user_id: str):
    MUSIC_SELECT_CACHE.pop(str(user_id), None)


# =========================
# 搜索
# =========================
PLATFORM_ALIAS = {
    # 裸「点歌」走配置 default_platform（网易），不在这里绑死 QQ
    "qq": ["qq点歌", "qq音乐"],
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


def detect_platform_from_cmd(cmd: str, fallback: str = "netease") -> str:
    cmd_lower = cmd.strip().lower()
    # 纯「点歌」→ 配置默认（网易）
    if cmd_lower in {"点歌", "音乐", "music"}:
        return fallback
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

    resp = http_client.request(
        "POST",
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
        cover = s.get("pic") or s.get("cover") or s.get("cover_url") or ""
        if isinstance(cover, str) and cover.startswith("http://"):
            cover = "https://" + cover[len("http://"):]
        audio = s.get("url") or s.get("link_audio") or s.get("audio") or ""
        page_link = s.get("link") or s.get("page") or ""
        songs.append({
            "id": str(s.get("songid") or s.get("id") or ""),
            "name": s.get("title") or s.get("name") or "未知歌曲",
            "artists": s.get("author") or s.get("artists") or "未知歌手",
            "audio_url": audio,
            "cover_url": cover,
            "page_url": page_link,
            "platform": s.get("type") or platform,
            "lyrics": s.get("lrc", "") or "",
            # 保留原始字段，便于后续卡片扩展
            "raw": {
                k: s.get(k)
                for k in ("type", "songid", "title", "author", "pic", "url", "link")
                if k in s
            },
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

    lines = [f"**搜索结果 · {escape_markdown_text(platform_name)}**", f"> 第 {page}/{total_pages} 页", ""]
    if not markdown:
        lines = [f"【搜索结果】{platform_name}（第 {page}/{total_pages} 页）", ""]

    for i, song in enumerate(page_songs, start=1):
        global_index = start + i
        song_name = _clean_song_field(song.get("name"), "未知歌曲")
        artists = _clean_song_field(song.get("artists"), "未知歌手")
        if markdown:
            song_link = build_md_command_link(song_name, f"选歌 {global_index}")
            cover = _md_cover_thumb(song.get("cover_url"), size=30)
            # 图集同款：![img #30px #30px](url)|歌名
            if cover:
                line_text = (
                    f"{global_index}. {cover}|{song_link} - {escape_markdown_text(artists)}"
                )
            else:
                line_text = f"{global_index}. {song_link} - {escape_markdown_text(artists)}"
        else:
            line_text = f"{global_index}. {song_name} - {artists}"
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


def _md_cover_thumb(cover_url: Any, size: int = 30) -> str:
    """QQ 原生 MD 小封面：![img #Npx #Npx](https://...)"""
    url = str(cover_url or "").strip()
    if not url.startswith("http"):
        return ""
    # 官方 MD 更稳的是 https
    if url.startswith("http://"):
        url = "https://" + url[len("http://") :]
    # 去掉可能破坏 MD 的空白
    url = url.replace(" ", "%20")
    n = max(16, min(int(size or 30), 128))
    return f"![img #{n}px #{n}px]({url})"


def _clean_song_field(value: Any, default: str) -> str:
    text = str(value if value not in (None, "") else default).strip()
    text = text.replace("\r", " ").replace("\n", " ")
    return text or default


def _lyrics_preview_lines(lyrics: Any, limit: int = 10) -> list[str]:
    """LRC 去时间轴，取前 N 行有效歌词。"""
    import re

    text = str(lyrics or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return []
    # [00:12.34] / [00:12.345] / [mm:ss]
    ts_re = re.compile(r"^\[(?:\d{1,2}:)+\d{1,2}(?:\.\d{1,3})?\]\s*")
    out: list[str] = []
    for raw in text.split("\n"):
        line = ts_re.sub("", raw).strip()
        if not line:
            continue
        # 跳过空标签残留
        if line in {"作词", "作曲", "编曲"}:
            continue
        out.append(line)
        if len(out) >= max(1, int(limit)):
            break
    return out


def build_song_plain_text(
    song_name: str,
    artists: str,
    *,
    platform: str = "",
    song_id: str = "",
    lyrics: str = "",
) -> str:
    lines = ["【点歌】", f"歌名：{song_name}", f"歌手：{artists}"]
    if platform:
        lines.append(f"来源：{platform}")
    if song_id:
        lines.append(f"ID：{song_id}")
    preview = _lyrics_preview_lines(lyrics, 10)
    if preview:
        lines.append("歌词：")
        lines.extend(preview)
    return "\n".join(lines)


def build_song_markdown_text(
    song_name: str,
    artists: str,
    *,
    cover_url: str = "",
    platform: str = "",
    song_id: str = "",
    page_url: str = "",
    lyrics: str = "",
) -> str:
    """选歌结果卡片：大封面 + 信息；有歌词则代码框放前 10 行（去时间轴）。"""
    retry_link = build_md_command_link("再搜此歌", f"点歌 {song_name}")
    help_link = build_md_command_link("点歌帮助", "点歌帮助")
    lines = ["**点歌**", ""]
    cover = _md_cover_thumb(cover_url, size=120) if cover_url else ""
    if cover:
        lines.append(cover)
        lines.append("")
    lines.append(f"> **歌名**：{escape_markdown_text(song_name)}")
    lines.append(f"> **歌手**：{escape_markdown_text(artists)}")
    if platform:
        lines.append(f"> **来源**：{escape_markdown_text(platform)}")
    if song_id:
        lines.append(f"> **ID**：`{escape_markdown_text(song_id)}`")
    if page_url and str(page_url).startswith("http"):
        lines.append(f"> [歌曲页]({page_url})")
    preview = _lyrics_preview_lines(lyrics, 10)
    if preview:
        lines.append("")
        lines.append("```")
        lines.extend(preview)
        lines.append("```")
    lines.append("")
    lines.append(f"{retry_link} / {help_link}")
    return "\n".join(lines)


# =========================
# 发送（图文 / 原生MD / 普通文本）
# =========================
async def send_song_rich(bot: Bot, event, song: dict) -> tuple[bool, str]:
    """
    发送顺序：
    1) 开启 MD：封面卡片（大图+信息）
    2) 有封面：图文混合
    3) 普通文本
    每条文本后补发音频（若有）
    """
    from ..command import handle_audio_send
    from ...xiuxian_utils.utils import handle_pic_msg_send, handle_send

    config = XiuConfig()
    song_name = _clean_song_field(song.get("name"), "未知歌曲")
    artists = _clean_song_field(song.get("artists"), "未知歌手")
    cover_url = song.get("cover_url") or ""
    audio_url = song.get("audio_url") or ""
    page_url = song.get("page_url") or ""
    platform = get_platform_display_name(str(song.get("platform") or ""))
    song_id = _clean_song_field(song.get("id"), "")
    lyrics = str(song.get("lyrics") or "")

    text_msg = build_song_plain_text(
        song_name, artists, platform=platform, song_id=song_id, lyrics=lyrics
    )
    md_msg = build_song_markdown_text(
        song_name,
        artists,
        cover_url=str(cover_url or ""),
        platform=platform,
        song_id=song_id,
        page_url=str(page_url or ""),
        lyrics=lyrics,
    )

    # ===== 1) 原生 MD 卡片（封面图 + 字段）=====
    if config.markdown_status:
        try:
            await handle_send(
                bot,
                event,
                md_msg,
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
            logger.warning(f"点歌 Markdown 卡片发送失败，准备降级：{e}")

    # ===== 2) 封面 + 文本同条发送 =====
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

    # ===== 3) 普通文本 =====
    try:
        await delivery_service.reply(bot, event, text_msg)

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
