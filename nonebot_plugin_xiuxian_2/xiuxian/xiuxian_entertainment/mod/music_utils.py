from typing import Any

from nonebot.log import logger

from ...adapter_compat import Bot
from ...messaging.delivery import delivery_service
from ...xiuxian_config import XiuConfig
from ...xiuxian_utils.utils import build_md_command_link, escape_markdown_text


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


def _lyrics_preview_lines(lyrics: Any, limit: int | None = None) -> list[str]:
    """LRC 去时间轴；默认全文，每行独立。limit 仅兼容旧调用。"""
    import re

    text = str(lyrics or "").replace("\r\n", "\n").replace("\r", "\n")
    if not text.strip():
        return []
    # [00:12.34] / [00:12.345] / [mm:ss]；同一行可能叠多个时间戳
    ts_re = re.compile(r"\[(?:\d{1,2}:)+\d{1,2}(?:\.\d{1,3})?\]\s*")
    out: list[str] = []
    for raw in text.split("\n"):
        line = ts_re.sub("", raw).strip()
        if not line:
            continue
        if line in {"作词", "作曲", "编曲"}:
            continue
        out.append(line)
        if limit is not None and len(out) >= max(1, int(limit)):
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
    preview = _lyrics_preview_lines(lyrics)
    if preview:
        lines.append("歌词：")
        # 每行歌词后强制单独成行（join 用 \n）
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
    """选歌结果卡片：大封面 + 信息；有歌词则代码框放全文（去时间轴，一行一行）。"""
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
    preview = _lyrics_preview_lines(lyrics)
    if preview:
        lines.append("")
        # 代码框内用真实换行，避免挤成一行
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
