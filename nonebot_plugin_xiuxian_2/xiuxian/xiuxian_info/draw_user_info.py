try:
    import ujson as json
except ImportError:
    import json

import time
from functools import lru_cache
from io import BytesIO
from pathlib import Path

from aiohttp import ClientError, ClientSession, ClientTimeout
from nonebot.log import logger
from PIL import Image, ImageDraw, ImageFont, UnidentifiedImageError

from .download import get_avatar_by_user_id_and_save
from .send_image_tool import convert_img

TEXT_PATH = Path() / "data" / "xiuxian" / "info_img"
CACHE_PATH = Path() / "data" / "xiuxian" / "cache"
RANDOM_BG_CACHE = CACHE_PATH / "user_info_random_background.png"

BASE_SIZE = (1100, 2680)
BACKGROUND_CACHE_SECONDS = 6 * 60 * 60
BACKGROUND_TIMEOUT = ClientTimeout(total=6)

first_color = (242, 250, 242)
second_color = (57, 57, 57)

FONT_ORIGIN_PATH = Path() / "data" / "xiuxian" / "font" / "SourceHanSerifCN-Heavy.otf"
FONT_FALLBACK_PATHS = (
    Path("/system/fonts/NotoSansCJK-Regular.ttc"),
    Path("/system/fonts/NotoSerifCJK-Regular.ttc"),
    Path("/system/fonts/DroidSansFallback.ttf"),
    Path("/system/fonts/FZZWXBTOT_Uni.ttf"),
)

LINE_RIGHT_SIZE = (450, 68)
LINE_MAIN_SIZE = (900, 86)
SECTION_TITLE_SIZE = (900, 86)


@lru_cache(maxsize=16)
def font_origin(size: int) -> ImageFont.FreeTypeFont:
    for font_path in (FONT_ORIGIN_PATH, *FONT_FALLBACK_PATHS):
        if not font_path.exists():
            continue
        try:
            return ImageFont.truetype(str(font_path), size=size)
        except Exception as e:
            logger.warning(f"加载字体失败: {font_path}, {e}")

    logger.warning("修仙信息图片字体文件未找到，将使用PIL默认字体。")
    return ImageFont.load_default()


font_36 = font_origin(36)
font_40 = font_origin(40)
font_24 = font_origin(24)


@lru_cache(maxsize=8)
def _template_image(filename: str, size: tuple[int, int]) -> Image.Image:
    return Image.open(TEXT_PATH / filename).resize(size).convert("RGBA")


def _copy_template(filename: str, size: tuple[int, int]) -> Image.Image:
    return _template_image(filename, size).copy()


async def draw_user_info_img(user_id, DETAIL_MAP):
    img = await _load_random_background()
    return await _draw_user_info_common(img, user_id, DETAIL_MAP)


async def draw_user_info_img_with_default_bg(user_id, DETAIL_MAP):
    img = _load_default_background()
    return await _draw_user_info_common(img, user_id, DETAIL_MAP)


async def _load_random_background() -> Image.Image:
    cached = _load_cached_background(allow_stale=False)
    if cached is not None:
        return cached

    try:
        bg_url = await get_anime_pic()
        if not bg_url:
            raise ValueError("随机背景API未返回图片地址")

        raw = await async_request(bg_url, timeout=BACKGROUND_TIMEOUT)
        bg = Image.open(BytesIO(raw)).convert("RGBA")
        bg = _prepare_background(bg)
        CACHE_PATH.mkdir(parents=True, exist_ok=True)
        bg.save(RANDOM_BG_CACHE, format="PNG", optimize=True)
        return bg
    except (ClientError, TimeoutError, UnidentifiedImageError, OSError, ValueError, KeyError) as e:
        logger.opt(colors=True).info(f"<red>下载随机背景图失败，使用缓存或默认背景图: {e}</red>")
    except Exception as e:
        logger.opt(colors=True).warning(f"<red>随机背景处理异常，使用缓存或默认背景图: {e}</red>")

    cached = _load_cached_background(allow_stale=True)
    if cached is not None:
        return cached
    return _load_default_background()


def _load_cached_background(allow_stale: bool) -> Image.Image | None:
    if not RANDOM_BG_CACHE.exists():
        return None

    if not allow_stale:
        age = time.time() - RANDOM_BG_CACHE.stat().st_mtime
        if age > BACKGROUND_CACHE_SECONDS:
            return None

    try:
        return Image.open(RANDOM_BG_CACHE).resize(BASE_SIZE).convert("RGBA")
    except Exception as e:
        logger.warning(f"读取修仙信息背景缓存失败: {e}")
        return None


def _load_default_background() -> Image.Image:
    return _cover_resize(Image.open(TEXT_PATH / "back.png").convert("RGBA"), BASE_SIZE)


def _prepare_background(img: Image.Image) -> Image.Image:
    img = _cover_resize(img, BASE_SIZE)
    overlay = Image.new("RGBA", BASE_SIZE, (0, 0, 0, 168))
    return Image.alpha_composite(img, overlay)


def _cover_resize(img: Image.Image, size: tuple[int, int]) -> Image.Image:
    target_w, target_h = size
    img_w, img_h = img.size
    scale = max(target_w / img_w, target_h / img_h)
    new_w = max(target_w, int(img_w * scale))
    new_h = max(target_h, int(img_h * scale))
    img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
    left = (new_w - target_w) // 2
    top = (new_h - target_h) // 2
    return img.crop((left, top, left + target_w, top + target_h))


async def _draw_user_info_common(img: Image.Image, user_id, DETAIL_MAP):
    user_status = _copy_template("user_state.png", (450, 450))
    temp = await get_avatar_by_user_id_and_save(user_id)
    user_avatar = img_author(temp, user_status)
    img.paste(user_avatar, (100, 100), mask=user_status.split()[-1])

    _draw_center_line(
        img,
        _copy_template("line3.png", (400, 60)),
        (130, 520),
        f"ID:{user_id}",
        max_width=340,
        start_size=36,
        min_size=24,
    )

    right_items = [
        ("道号", DETAIL_MAP.get("道号", "无")),
        ("称号", DETAIL_MAP.get("称号", "无")),
        ("境界", DETAIL_MAP.get("境界", "无")),
        ("修为", DETAIL_MAP.get("修为", "0")),
        ("灵石", DETAIL_MAP.get("灵石", "0")),
        ("战力", DETAIL_MAP.get("战力", "0")),
    ]

    for index, (key, value) in enumerate(right_items):
        _draw_info_line(
            img,
            "line3.png",
            LINE_RIGHT_SIZE,
            (550, 100 + index * 83),
            key,
            value,
            text_x=70,
            max_width=350,
            min_size=22,
        )

    base_items = [
        ("灵根", DETAIL_MAP.get("灵根", "无")),
        ("突破状态", DETAIL_MAP.get("突破状态", "无")),
        ("主修功法", DETAIL_MAP.get("主修功法", "无")),
        ("辅修功法", DETAIL_MAP.get("辅修功法", "无")),
        ("副修神通", DETAIL_MAP.get("副修神通", "无")),
        ("身法", DETAIL_MAP.get("身法", "无")),
        ("瞳术", DETAIL_MAP.get("瞳术", "无")),
        ("修炼等级", DETAIL_MAP.get("修炼等级", "无")),
        ("攻击力", DETAIL_MAP.get("攻击力", "0")),
        ("法器", DETAIL_MAP.get("法器", "无")),
        ("防具", DETAIL_MAP.get("防具", "无")),
        ("道侣", DETAIL_MAP.get("道侣", "无")),
        ("本命法宝", DETAIL_MAP.get("本命法宝", "无")),
    ]

    line_step_y = 86
    base_title_y = 605
    base_line_start_y = base_title_y + 89
    _draw_section_title(img, "【基本信息】", (100, base_title_y))

    for index, (key, value) in enumerate(base_items):
        _draw_info_line(
            img,
            "line4.png",
            LINE_MAIN_SIZE,
            (100, base_line_start_y + index * line_step_y),
            key,
            value,
            text_x=100,
            max_width=760,
            min_size=23,
        )

    sect_items = [
        ("所在宗门", DETAIL_MAP.get("所在宗门", "无宗门")),
        ("宗门职位", DETAIL_MAP.get("宗门职位", "无")),
    ]
    sect_title_y = base_line_start_y + len(base_items) * line_step_y + 18
    sect_line_start_y = sect_title_y + 89
    _draw_section_title(img, "【宗门信息】", (100, sect_title_y))

    for index, (key, value) in enumerate(sect_items):
        _draw_info_line(
            img,
            "line4.png",
            LINE_MAIN_SIZE,
            (100, sect_line_start_y + index * line_step_y),
            key,
            value,
            text_x=100,
            max_width=760,
            min_size=23,
        )

    rank_items = [
        ("注册位数", DETAIL_MAP.get("注册位数", "无")),
        ("修为排行", DETAIL_MAP.get("修为排行", "无")),
        ("灵石排行", DETAIL_MAP.get("灵石排行", "无")),
    ]
    rank_title_y = sect_line_start_y + len(sect_items) * line_step_y + 18
    rank_line_start_y = rank_title_y + 89
    _draw_section_title(img, "【排行信息】", (100, rank_title_y))

    for index, (key, value) in enumerate(rank_items):
        _draw_info_line(
            img,
            "line4.png",
            LINE_MAIN_SIZE,
            (100, rank_line_start_y + index * line_step_y),
            key,
            value,
            text_x=100,
            max_width=760,
            separator="：",
            min_size=23,
        )

    output_dir = CACHE_PATH
    output_dir.mkdir(parents=True, exist_ok=True)
    image_path = output_dir / f"user_xiuxian_info_{user_id}.png"
    final_img = img.convert("RGB")
    final_img.save(image_path, format="PNG", optimize=True)
    return await convert_img(final_img)


def _draw_section_title(img: Image.Image, title: str, xy: tuple[int, int]):
    line = _copy_template("line2.png", SECTION_TITLE_SIZE)
    draw = ImageDraw.Draw(line)
    font, text = _fit_text(title, 820, start_size=40, min_size=30)
    text_width = _text_width(draw, text, font)
    draw.text(((line.width - text_width) / 2, line.height / 2), text, first_color, font, "lm")
    img.paste(line, xy, line)


def _draw_center_line(
    img: Image.Image,
    line: Image.Image,
    xy: tuple[int, int],
    text: str,
    max_width: int,
    start_size: int,
    min_size: int,
):
    draw = ImageDraw.Draw(line)
    font, text = _fit_text(text, max_width, start_size=start_size, min_size=min_size)
    text_width = _text_width(draw, text, font)
    draw.text(((line.width - text_width) / 2, line.height / 2), text, first_color, font, "lm")
    img.paste(line, xy, line)


def _draw_info_line(
    img: Image.Image,
    template_name: str,
    size: tuple[int, int],
    xy: tuple[int, int],
    key: str,
    value,
    text_x: int,
    max_width: int,
    separator: str = ":",
    min_size: int = 24,
):
    line = _copy_template(template_name, size)
    draw = ImageDraw.Draw(line)
    word = f"{key}{separator}{_normalize_image_text(value)}"
    font, word = _fit_text(word, max_width, start_size=36, min_size=min_size)
    draw.text((text_x, line.height / 2), word, first_color, font, "lm")
    img.paste(line, xy, line)


def _normalize_image_text(value) -> str:
    text = str(value) if value is not None else "无"
    replacements = {
        "\r": " ",
        "\n": " ",
        "💖 ": "",
        "💕 ": "",
        "💗 ": "",
        "💓 ": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return " ".join(text.split())


def _fit_text(text: str, max_width: int, start_size: int = 36, min_size: int = 24):
    text = _normalize_image_text(text)
    for size in range(start_size, min_size - 1, -2):
        font = font_origin(size)
        if _font_text_width(text, font) <= max_width:
            return font, text

    font = font_origin(min_size)
    return font, _truncate_text(text, font, max_width)


def _truncate_text(text: str, font, max_width: int) -> str:
    ellipsis = "..."
    if _font_text_width(text, font) <= max_width:
        return text

    max_text_width = max_width - _font_text_width(ellipsis, font)
    if max_text_width <= 0:
        return ellipsis

    result = ""
    for ch in text:
        if _font_text_width(result + ch, font) > max_text_width:
            break
        result += ch
    return result + ellipsis


def _font_text_width(text: str, font) -> float:
    try:
        return font.getlength(text)
    except Exception:
        left, _, right, _ = font.getbbox(text)
        return right - left


def _text_width(draw: ImageDraw.ImageDraw, text: str, font) -> float:
    try:
        return draw.textlength(text, font=font)
    except Exception:
        return _font_text_width(text, font)


def img_author(img, bg):
    w, h = img.size
    alpha_layer = Image.new("L", (w, h), 0)
    draw = ImageDraw.Draw(alpha_layer)
    draw.ellipse((0, 0, w, h), fill=255)
    bg.paste(img, (88, 80), alpha_layer)
    return bg


async def linewh(line, word):
    lw, lh = line.size
    left, _, right, _ = font_36.getbbox(word)
    w = right - left
    return (lw - w) / 2, lh / 2


async def async_request(url, *args, is_text=False, timeout=BACKGROUND_TIMEOUT, **kwargs):
    async with ClientSession(timeout=timeout) as c:
        async with c.get(url, *args, **kwargs) as r:
            r.raise_for_status()
            return (await r.text()) if is_text else (await r.read())


async def get_anime_pic():
    r: str = await async_request(
        "https://imgapi.cn/api.php?zd=mobile&fl=dongman&gs=json",
        is_text=True,
    )
    response_json = json.loads(r)
    if response_json["code"] == "200":
        return response_json["imgurl"]

    logger.opt(colors=True).info("<red>API 返回错误码：</red>" + response_json["code"])
    return None
