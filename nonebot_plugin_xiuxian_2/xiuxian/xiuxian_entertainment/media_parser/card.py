"""简化版媒体卡片渲染（参考 astrbot_plugin_parser / nonebot-plugin-parser 风格）。

布局：白底卡片
- 顶栏：平台徽章 + 作者
- 标题（多行截断）
- 封面（可选播放按钮遮罩）
- 底栏：简介/提示

不依赖 apilmoji / 上游资源包，用本机中文字体。
"""
from __future__ import annotations

import io
import re
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
BADGE_H = 36
GAP = 14
TITLE_SIZE = 30
META_SIZE = 22
FOOT_SIZE = 20
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
    if len(lines) == max_lines and (cur and lines[-1] != text[-len(lines[-1]) :]):
        # ellipsis
        s = lines[-1]
        while s and _text_width(font, s + "…") > max_w:
            s = s[:-1]
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
        logger.debug(f"媒体卡片封面下载失败: {e}")
        return None


def _round_rect(draw: ImageDraw.ImageDraw, box, radius: int, fill) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill)


def _fit_cover(img: Image.Image, target_w: int) -> Image.Image:
    w, h = img.size
    if w <= 0 or h <= 0:
        return Image.new("RGB", (target_w, MIN_COVER_H), (30, 30, 30))
    scale = target_w / w
    nh = max(MIN_COVER_H, min(MAX_COVER_H, int(h * scale)))
    # scale width first then center-crop height if needed
    resized = img.resize((target_w, max(1, int(h * scale))), Image.Resampling.LANCZOS)
    if resized.height > MAX_COVER_H:
        top = (resized.height - MAX_COVER_H) // 2
        resized = resized.crop((0, top, target_w, top + MAX_COVER_H))
        nh = MAX_COVER_H
    elif resized.height < MIN_COVER_H:
        # pad
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
    # triangle
    tri = [
        (cx - r // 3, cy - r // 2),
        (cx - r // 3, cy + r // 2),
        (cx + r // 2, cy),
    ]
    d.polygon(tri, fill=(255, 255, 255, 230))
    return Image.alpha_composite(out, overlay).convert("RGB")


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
        desc = ""
    label = _PLATFORM_LABEL.get(platform, platform)

    images = meta.get("image_urls") or []
    if not cover_url and images:
        cover_url = images[0]
    cover_img = _download_image(str(cover_url or ""), use_proxy=use_proxy) if cover_url else None

    font_title = _pick_font(TITLE_SIZE)
    font_meta = _pick_font(META_SIZE)
    font_foot = _pick_font(FOOT_SIZE)
    font_badge = _pick_font(20)

    content_w = CARD_W - 2 * PAD
    title_lines = _wrap(font_title, title, content_w, max_lines=3)
    author_line = f"@{author}" if author else ""
    foot_bits = []
    if has_video or (meta.get("video_urls") or []):
        foot_bits.append("含视频")
        n = len(meta.get("video_urls") or [])
        if n > 1:
            foot_bits[-1] = f"含视频×{n}"
    if images:
        foot_bits.append(f"图{len(images)}张")
    if desc:
        foot_bits.append(desc[:40] + ("…" if len(desc) > 40 else ""))
    foot = " · ".join(foot_bits) if foot_bits else "链接解析"

    # heights
    y = PAD
    y += BADGE_H + 8
    if author_line:
        y += META_SIZE + 8
    title_h = (TITLE_SIZE + 8) * max(1, len(title_lines))
    y += title_h + GAP

    cover_h = 0
    cover_rgb = None
    if cover_img is not None:
        cover_rgb = _fit_cover(cover_img, content_w)
        if has_video or (meta.get("video_urls") or []):
            cover_rgb = _draw_play_button(cover_rgb)
        cover_h = cover_rgb.height
        y += cover_h + GAP
    y += FOOT_SIZE + PAD

    card_h = y
    img = Image.new("RGB", (CARD_W, card_h), (255, 255, 255))
    draw = ImageDraw.Draw(img)

    # subtle outer border
    draw.rectangle((0, 0, CARD_W - 1, card_h - 1), outline=(230, 230, 230), width=1)

    cur = PAD
    # badge
    badge_text = f" {label} "
    bw = _text_width(font_badge, badge_text) + 16
    _round_rect(draw, (PAD, cur, PAD + bw, cur + BADGE_H), 10, (0, 122, 255))
    # center text in badge
    try:
        tb = font_badge.getbbox(badge_text)
        th = tb[3] - tb[1]
    except Exception:
        th = 18
    draw.text(
        (PAD + 8, cur + (BADGE_H - th) // 2 - 1),
        badge_text,
        font=font_badge,
        fill=(255, 255, 255),
    )
    cur += BADGE_H + 8

    if author_line:
        draw.text((PAD, cur), author_line, font=font_meta, fill=(100, 100, 100))
        cur += META_SIZE + 8

    for line in title_lines or ["已解析内容"]:
        draw.text((PAD, cur), line, font=font_title, fill=(30, 30, 30))
        cur += TITLE_SIZE + 8
    cur += GAP - 8

    if cover_rgb is not None:
        img.paste(cover_rgb, (PAD, cur))
        # border around cover
        draw.rectangle(
            (PAD, cur, PAD + cover_rgb.width - 1, cur + cover_rgb.height - 1),
            outline=(235, 235, 235),
            width=1,
        )
        cur += cover_rgb.height + GAP

    draw.text((PAD, cur), foot, font=font_foot, fill=(136, 136, 136))

    # save
    if out_path is None:
        from ....paths import get_paths

        out_dir = get_paths().data / "media_parser_cache" / "cards"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"card_{abs(hash((platform, title, cover_url))) % (10**12)}.png"
    else:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

    img.save(out_path, format="PNG", optimize=True)
    return Path(out_path)
