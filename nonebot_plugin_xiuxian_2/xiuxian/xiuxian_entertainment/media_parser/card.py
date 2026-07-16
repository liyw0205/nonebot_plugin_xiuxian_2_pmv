"""媒体卡片渲染（参考 Zhalslar/astrbot_plugin_parser 布局）。

布局：
- 顶栏：圆形头像 + @作者 + 发布时间；右侧平台图标
- 标题
- 封面（视频叠播放钮；图集用首图）
- 底栏：简介（不再显示「视频×N / 图N张」）
"""
from __future__ import annotations

import io
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from nonebot.log import logger
from PIL import Image, ImageDraw, ImageFont

from ...xiuxian_utils.http_proxy import http_client

_FONT_CANDIDATES = [
    Path("/root/xiu2/data/xiuxian/font/SarasaMonoSC-Bold.ttf"),
    Path("/root/xiu2/data/xiuxian/font/SourceHanSerifCN-Heavy.otf"),
    Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
    Path("/usr/share/fonts/truetype/wqy/wqy-microhei.ttc"),
    Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
]

_ASSETS = Path(__file__).resolve().parent / "assets"
_LOGO_DIR = _ASSETS / "logos"
_PLATFORM_LOGO = {
    "bilibili": "bilibili.png",
    "douyin": "douyin.png",
    "kuaishou": "kuaishou.png",
    "tiktok": "tiktok.png",
    "twitter": "twitter.png",
    "weibo": "weibo.png",
    "xiaohongshu": "xhs.png",
    "youtube": "youtube.png",
}
_PLATFORM_LABEL = {
    "bilibili": "B站",
    "douyin": "抖音",
    "kuaishou": "快手",
    "xiaohongshu": "小红书",
    "weibo": "微博",
    "twitter": "X",
    "tiktok": "TikTok",
    "xiaoheihe": "小黑盒",
    "instagram": "IG",
    "youtube": "YouTube",
}

CARD_W = 720
PAD = 28
GAP = 14
AVATAR = 64
LOGO = 42
TITLE_SIZE = 30
META_SIZE = 24
TIME_SIZE = 20
FOOT_SIZE = 22
MAX_COVER_H = 720
MIN_COVER_H = 240


def _pick_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for p in _FONT_CANDIDATES:
        if p.is_file():
            try:
                return ImageFont.truetype(str(p), size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def _text_width(font: ImageFont.ImageFont, text: str) -> int:
    try:
        return int(font.getlength(text))
    except Exception:
        box = font.getbbox(text)
        return int(box[2] - box[0])


def _wrap(font: ImageFont.ImageFont, text: str, max_w: int, max_lines: int = 4) -> list[str]:
    text = re.sub(r"\s+", " ", (text or "").strip())
    if not text:
        return []
    lines: list[str] = []
    cur = ""
    for ch in text:
        trial = cur + ch
        if _text_width(font, trial) <= max_w:
            cur = trial
            continue
        if cur:
            lines.append(cur)
        cur = ch
        if len(lines) >= max_lines:
            break
    if cur and len(lines) < max_lines:
        lines.append(cur)
    if len(lines) == max_lines:
        s = lines[-1]
        while s and _text_width(font, s + "…") > max_w:
            s = s[:-1]
        if s != lines[-1]:
            lines[-1] = (s + "…") if s else "…"
    return lines


def _download_image(url: str, *, use_proxy: bool = False, timeout: int = 20) -> Image.Image | None:
    if not url or not str(url).startswith("http"):
        return None
    try:
        resp = http_client.request(
            "GET",
            url,
            timeout=timeout,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) "
                    "AppleWebKit/605.1.15 Mobile/15E148"
                ),
                "Accept": "image/avif,image/webp,image/*,*/*;q=0.8",
                "Referer": f"{urlparse(url).scheme}://{urlparse(url).netloc}/",
            },
            check_status=False,
            use_config_proxy=bool(use_proxy),
            stream=True,
        )
        if int(getattr(resp, "status_code", 0) or 0) >= 400:
            return None
        data = bytearray()
        for chunk in resp.iter_content(64 * 1024):
            data.extend(chunk)
            if len(data) > 8 * 1024 * 1024:
                break
        img = Image.open(io.BytesIO(bytes(data)))
        return img.convert("RGBA")
    except Exception as e:
        logger.debug(f"媒体卡片图片下载失败: {e}")
        return None


def _circle_avatar(img: Image.Image | None, size: int = AVATAR) -> Image.Image:
    if img is None:
        # placeholder
        out = Image.new("RGBA", (size, size), (230, 230, 230, 255))
        d = ImageDraw.Draw(out)
        d.ellipse((0, 0, size - 1, size - 1), fill=(200, 200, 200, 255))
        return out
    av = img.convert("RGBA").resize((size, size), Image.Resampling.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size - 1, size - 1), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(av, (0, 0))
    out.putalpha(mask)
    return out


def _load_platform_logo(platform: str) -> Image.Image | None:
    """加载平台图标；保持宽高比，不强制压成正方形（横屏 logo 不拉伸）。"""
    name = _PLATFORM_LOGO.get(platform)
    if not name:
        return None
    path = _LOGO_DIR / name
    if not path.is_file():
        return None
    try:
        img = Image.open(path).convert("RGBA")
        return _fit_logo(img, LOGO)
    except Exception:
        return None


def _fit_logo(img: Image.Image, max_side: int) -> Image.Image:
    """等比缩放到 max_side 内，避免横向 logo 被正方形拉伸。"""
    w, h = img.size
    if w <= 0 or h <= 0:
        return img
    scale = min(max_side / w, max_side / h, 1.0)
    # 也允许略放大到 max_side 高度，保证可见
    if max(w, h) < max_side:
        scale = max_side / max(w, h)
    nw = max(1, int(round(w * scale)))
    nh = max(1, int(round(h * scale)))
    return img.resize((nw, nh), Image.Resampling.LANCZOS)


def _fit_cover(img: Image.Image, target_w: int) -> Image.Image:
    w, h = img.size
    if w <= 0 or h <= 0:
        return Image.new("RGB", (target_w, MIN_COVER_H), (30, 30, 30))
    scale = target_w / w
    resized = img.resize((target_w, max(1, int(h * scale))), Image.Resampling.LANCZOS)
    if resized.height > MAX_COVER_H:
        top = (resized.height - MAX_COVER_H) // 2
        resized = resized.crop((0, top, target_w, top + MAX_COVER_H))
    elif resized.height < MIN_COVER_H:
        canvas = Image.new("RGB", (target_w, MIN_COVER_H), (24, 24, 28))
        y = (MIN_COVER_H - resized.height) // 2
        canvas.paste(resized.convert("RGB"), (0, y))
        return canvas
    return resized.convert("RGB")


def _draw_play_button(cover: Image.Image) -> Image.Image:
    out = cover.convert("RGBA")
    overlay = Image.new("RGBA", out.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(overlay)
    cx, cy = out.width // 2, out.height // 2
    r = max(28, min(out.width, out.height) // 10)
    d.ellipse((cx - r, cy - r, cx + r, cy + r), fill=(0, 0, 0, 110))
    tri = [
        (cx - r // 3, cy - r // 2),
        (cx - r // 3, cy + r // 2),
        (cx + r // 2, cy),
    ]
    d.polygon(tri, fill=(255, 255, 255, 230))
    return Image.alpha_composite(out, overlay).convert("RGB")


def _format_time(meta: dict[str, Any]) -> str:
    ts = meta.get("timestamp") or meta.get("publish_time") or meta.get("create_time")
    if ts is None or ts == "":
        return ""
    try:
        if isinstance(ts, str) and not ts.isdigit():
            # already formatted?
            return ts[:32]
        val = int(float(ts))
        if val > 10_000_000_000:
            val //= 1000
        if val <= 0:
            return ""
        return datetime.fromtimestamp(val).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def render_media_card(
    meta: dict[str, Any],
    *,
    cover_url: str | None = None,
    has_video: bool = False,
    use_proxy: bool = False,
    out_path: str | Path | None = None,
) -> Path | None:
    """根据解析 meta 渲染卡片，返回本地 png 路径。"""
    platform = str(meta.get("platform") or meta.get("parser_name") or "media")
    title = (meta.get("title") or "").strip() or "已解析内容"
    author = (meta.get("author") or "").strip()
    desc = (meta.get("desc") or "").strip()
    if desc == title:
        # 标题即简介时底栏仍可显示标题摘要
        desc = title
    label = _PLATFORM_LABEL.get(platform, platform)
    time_str = _format_time(meta)

    images = list(meta.get("image_urls") or [])
    videos = list(meta.get("video_urls") or [])
    has_video = bool(has_video or videos)
    if not cover_url and images:
        cover_url = images[0]

    avatar_url = meta.get("avatar_url") or meta.get("author_avatar") or ""
    avatar_img = _download_image(str(avatar_url), use_proxy=use_proxy) if avatar_url else None
    avatar = _circle_avatar(avatar_img, AVATAR)
    logo = _load_platform_logo(platform)
    cover_img = _download_image(str(cover_url or ""), use_proxy=use_proxy) if cover_url else None

    font_title = _pick_font(TITLE_SIZE)
    font_meta = _pick_font(META_SIZE)
    font_time = _pick_font(TIME_SIZE)
    font_foot = _pick_font(FOOT_SIZE)
    font_badge = _pick_font(18)

    content_w = CARD_W - 2 * PAD
    # 作者区文字宽度：扣掉头像、logo
    header_text_w = content_w - AVATAR - 12 - (LOGO + 8 if logo else 0)
    name_lines = _wrap(font_meta, f"@{author}" if author else f"@{label}", header_text_w, max_lines=1)
    title_lines = _wrap(font_title, title, content_w, max_lines=3)
    foot_lines = _wrap(font_foot, desc, content_w, max_lines=3) if desc else []

    # heights
    header_h = max(AVATAR, (META_SIZE + 4) + (TIME_SIZE + 4 if time_str else 0))
    y = PAD
    y += header_h + GAP
    y += (TITLE_SIZE + 8) * max(1, len(title_lines)) + GAP

    cover_rgb = None
    if cover_img is not None:
        cover_rgb = _fit_cover(cover_img, content_w)
        if has_video:
            cover_rgb = _draw_play_button(cover_rgb)
        y += cover_rgb.height + GAP
    if foot_lines:
        y += (FOOT_SIZE + 6) * len(foot_lines)
    y += PAD

    img = Image.new("RGB", (CARD_W, y), (255, 255, 255))
    draw = ImageDraw.Draw(img)
    draw.rectangle((0, 0, CARD_W - 1, y - 1), outline=(230, 230, 230), width=1)

    cur = PAD
    # header: avatar + name/time + logo
    img.paste(avatar, (PAD, cur), avatar)
    text_x = PAD + AVATAR + 12
    name_y = cur + max(0, (AVATAR - ((META_SIZE + 4) + (TIME_SIZE + 4 if time_str else 0))) // 2)
    draw.text((text_x, name_y), name_lines[0] if name_lines else f"@{label}", font=font_meta, fill=(30, 100, 200))
    if time_str:
        draw.text((text_x, name_y + META_SIZE + 4), time_str, font=font_time, fill=(140, 140, 140))
    if logo is not None:
        lx = CARD_W - PAD - logo.width
        ly = cur + max(0, (AVATAR - logo.height) // 2)
        img.paste(logo, (lx, ly), logo)
    else:
        # text badge fallback
        badge = f" {label} "
        bw = _text_width(font_badge, badge) + 12
        bx = CARD_W - PAD - bw
        by = cur + (AVATAR - 28) // 2
        draw.rounded_rectangle((bx, by, bx + bw, by + 28), radius=8, fill=(0, 122, 255))
        draw.text((bx + 6, by + 4), badge, font=font_badge, fill=(255, 255, 255))
    cur += header_h + GAP

    for line in title_lines or ["已解析内容"]:
        draw.text((PAD, cur), line, font=font_title, fill=(30, 30, 30))
        cur += TITLE_SIZE + 8
    cur += GAP - 8

    if cover_rgb is not None:
        img.paste(cover_rgb, (PAD, cur))
        draw.rectangle(
            (PAD, cur, PAD + cover_rgb.width - 1, cur + cover_rgb.height - 1),
            outline=(235, 235, 235),
            width=1,
        )
        cur += cover_rgb.height + GAP

    for line in foot_lines:
        draw.text((PAD, cur), line, font=font_foot, fill=(100, 100, 100))
        cur += FOOT_SIZE + 6

    if out_path is None:
        from ....paths import get_paths

        out_dir = get_paths().data / "media_parser_cache" / "cards"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"card_{abs(hash((platform, title, cover_url, time_str))) % (10**12)}.png"
    else:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

    img.save(out_path, format="PNG", optimize=True)
    return Path(out_path)
