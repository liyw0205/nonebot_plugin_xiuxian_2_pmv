import re
import requests
from io import BytesIO
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from nonebot.log import logger
from nonebot_plugin_xiuxian_2.paths import get_paths
from datetime import datetime, timedelta

API_URL = "https://api.github.com/repos/liyw0205/nonebot_plugin_xiuxian_2_pmv/commits"

# 建议别太多，commit message 一长图片会非常高
ITEMS_PER_PAGE = 15

FONT_PATH = get_paths().data / "font" / "SourceHanSerifCN-Heavy.otf"


def get_commits(page: int, per_page: int = ITEMS_PER_PAGE):
    """从 GitHub 获取指定页数的 Commits"""
    params = {"page": page, "per_page": per_page}
    headers = {
        "User-Agent": "nonebot-plugin-xiuxian",
        "Accept": "application/vnd.github+json",
    }

    try:
        response = requests.get(API_URL, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"从 GitHub 获取 Commits 失败: {e}")
        return None


def utc_to_utc8(utc_time_str: str) -> str:
    """将 UTC 时间转换为 UTC+8"""
    try:
        utc_time = datetime.strptime(
            utc_time_str.replace("Z", ""),
            "%Y-%m-%dT%H:%M:%S"
        )
        utc8_time = utc_time + timedelta(hours=8)
        return utc8_time.strftime("%Y-%m-%d %H:%M")
    except Exception as e:
        logger.error(f"时间转换失败: {e}")
        return utc_time_str.split("T")[0]


def load_font(size: int):
    try:
        return ImageFont.truetype(str(FONT_PATH), size)
    except Exception:
        logger.warning("字体文件未找到，将使用默认字体。")
        return ImageFont.load_default()


TITLE_FONT = load_font(36)
META_FONT = load_font(18)
BODY_FONT = load_font(20)
BODY_BOLD_FONT = load_font(21)
SMALL_FONT = load_font(17)
CODE_FONT = load_font(18)
H1_FONT = load_font(28)
H2_FONT = load_font(24)
H3_FONT = load_font(22)


def clean_inline_markdown(text: str) -> str:
    """
    简单清理行内 Markdown。
    由于 PIL 不方便做同一行局部加粗，这里采用保留文本内容的方式。
    """
    # 链接：[文本](url) -> 文本(url)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1(\2)", text)

    # 图片：![alt](url) -> [图片] alt url
    text = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"[图片] \1 \2", text)

    # 加粗、斜体、行内代码，保留内容
    text = text.replace("**", "")
    text = text.replace("__", "")
    text = text.replace("*", "")
    text = text.replace("`", "")

    return text


def wrap_text_by_pixel(draw: ImageDraw.ImageDraw, text: str, font, max_width: int):
    """
    按像素宽度自动换行，比 textwrap.wrap 更适合中文。
    """
    if not text:
        return [""]

    lines = []
    current = ""

    for ch in text:
        test = current + ch
        try:
            width = draw.textlength(test, font=font)
        except Exception:
            width = font.getlength(test)

        if width <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = ch

    if current:
        lines.append(current)

    return lines


def parse_markdown_blocks(markdown_text: str):
    """
    将 Markdown 文本解析成简单块。
    返回类似：
    [
        {"type": "h1", "text": "..."},
        {"type": "p", "text": "..."},
        {"type": "ul", "text": "..."},
        {"type": "code", "text": "..."},
    ]
    """
    blocks = []
    lines = markdown_text.splitlines()

    in_code = False
    code_lines = []

    for raw_line in lines:
        line = raw_line.rstrip()

        # 代码块开始/结束
        if line.strip().startswith("```"):
            if not in_code:
                in_code = True
                code_lines = []
            else:
                in_code = False
                blocks.append({
                    "type": "code",
                    "text": "\n".join(code_lines)
                })
            continue

        if in_code:
            code_lines.append(raw_line)
            continue

        stripped = line.strip()

        if not stripped:
            blocks.append({"type": "blank", "text": ""})
            continue

        # 标题
        if stripped.startswith("### "):
            blocks.append({"type": "h3", "text": clean_inline_markdown(stripped[4:])})
        elif stripped.startswith("## "):
            blocks.append({"type": "h2", "text": clean_inline_markdown(stripped[3:])})
        elif stripped.startswith("# "):
            blocks.append({"type": "h1", "text": clean_inline_markdown(stripped[2:])})

        # 无序列表
        elif stripped.startswith("- ") or stripped.startswith("* "):
            blocks.append({"type": "ul", "text": clean_inline_markdown(stripped[2:])})

        # 有序列表
        elif re.match(r"^\d+\.\s+", stripped):
            text = re.sub(r"^\d+\.\s+", "", stripped)
            prefix = re.match(r"^(\d+\.)\s+", stripped).group(1)
            blocks.append({
                "type": "ol",
                "prefix": prefix,
                "text": clean_inline_markdown(text)
            })

        # 引用
        elif stripped.startswith("> "):
            blocks.append({"type": "quote", "text": clean_inline_markdown(stripped[2:])})

        # 普通段落
        else:
            blocks.append({"type": "p", "text": clean_inline_markdown(stripped)})

    # 防止代码块没有闭合
    if in_code and code_lines:
        blocks.append({
            "type": "code",
            "text": "\n".join(code_lines)
        })

    return blocks


def measure_markdown_blocks(draw, blocks, max_width):
    """
    预计算 Markdown 块渲染高度，同时生成可绘制结构。
    """
    render_items = []
    total_height = 0

    for block in blocks:
        block_type = block["type"]

        if block_type == "blank":
            render_items.append({
                "type": "blank",
                "height": 10
            })
            total_height += 10
            continue

        if block_type == "h1":
            font = H1_FONT
            line_height = 34
            top_gap = 8
            bottom_gap = 6
            lines = wrap_text_by_pixel(draw, block["text"], font, max_width)
            height = top_gap + len(lines) * line_height + bottom_gap
            render_items.append({
                "type": "text",
                "font": font,
                "fill": (35, 35, 35),
                "lines": lines,
                "line_height": line_height,
                "top_gap": top_gap,
                "bottom_gap": bottom_gap,
                "indent": 0,
                "height": height,
            })
            total_height += height

        elif block_type == "h2":
            font = H2_FONT
            line_height = 30
            top_gap = 6
            bottom_gap = 5
            lines = wrap_text_by_pixel(draw, block["text"], font, max_width)
            height = top_gap + len(lines) * line_height + bottom_gap
            render_items.append({
                "type": "text",
                "font": font,
                "fill": (45, 45, 45),
                "lines": lines,
                "line_height": line_height,
                "top_gap": top_gap,
                "bottom_gap": bottom_gap,
                "indent": 0,
                "height": height,
            })
            total_height += height

        elif block_type == "h3":
            font = H3_FONT
            line_height = 28
            top_gap = 5
            bottom_gap = 4
            lines = wrap_text_by_pixel(draw, block["text"], font, max_width)
            height = top_gap + len(lines) * line_height + bottom_gap
            render_items.append({
                "type": "text",
                "font": font,
                "fill": (55, 55, 55),
                "lines": lines,
                "line_height": line_height,
                "top_gap": top_gap,
                "bottom_gap": bottom_gap,
                "indent": 0,
                "height": height,
            })
            total_height += height

        elif block_type == "ul":
            font = BODY_FONT
            line_height = 26
            bullet = "• "
            indent = 24
            text_width = max_width - indent
            lines = wrap_text_by_pixel(draw, block["text"], font, text_width)
            height = len(lines) * line_height + 4
            render_items.append({
                "type": "list",
                "font": font,
                "fill": (30, 30, 30),
                "bullet": bullet,
                "lines": lines,
                "line_height": line_height,
                "indent": indent,
                "height": height,
            })
            total_height += height

        elif block_type == "ol":
            font = BODY_FONT
            line_height = 26
            prefix = block.get("prefix", "1.")
            indent = 34
            text_width = max_width - indent
            lines = wrap_text_by_pixel(draw, block["text"], font, text_width)
            height = len(lines) * line_height + 4
            render_items.append({
                "type": "list",
                "font": font,
                "fill": (30, 30, 30),
                "bullet": prefix + " ",
                "lines": lines,
                "line_height": line_height,
                "indent": indent,
                "height": height,
            })
            total_height += height

        elif block_type == "quote":
            font = BODY_FONT
            line_height = 26
            indent = 28
            text_width = max_width - indent
            lines = wrap_text_by_pixel(draw, block["text"], font, text_width)
            height = len(lines) * line_height + 8
            render_items.append({
                "type": "quote",
                "font": font,
                "fill": (90, 90, 90),
                "lines": lines,
                "line_height": line_height,
                "indent": indent,
                "height": height,
            })
            total_height += height

        elif block_type == "code":
            font = CODE_FONT
            line_height = 24
            code_text = block["text"]
            code_lines = []

            for code_raw_line in code_text.splitlines() or [""]:
                # 代码块也按像素宽度折行
                wrapped = wrap_text_by_pixel(draw, code_raw_line, font, max_width - 32)
                code_lines.extend(wrapped)

            height = len(code_lines) * line_height + 22
            render_items.append({
                "type": "code",
                "font": font,
                "fill": (45, 45, 45),
                "lines": code_lines,
                "line_height": line_height,
                "height": height,
            })
            total_height += height

        else:
            font = BODY_FONT
            line_height = 26
            lines = wrap_text_by_pixel(draw, block["text"], font, max_width)
            height = len(lines) * line_height + 4
            render_items.append({
                "type": "text",
                "font": font,
                "fill": (20, 20, 20),
                "lines": lines,
                "line_height": line_height,
                "top_gap": 0,
                "bottom_gap": 4,
                "indent": 0,
                "height": height,
            })
            total_height += height

    return render_items, total_height


def draw_markdown_items(draw, items, x, y, max_width):
    """
    绘制预计算好的 Markdown 块。
    """
    for item in items:
        item_type = item["type"]

        if item_type == "blank":
            y += item["height"]
            continue

        if item_type == "text":
            y += item.get("top_gap", 0)
            indent = item.get("indent", 0)

            for line in item["lines"]:
                draw.text(
                    (x + indent, y),
                    line,
                    fill=item["fill"],
                    font=item["font"]
                )
                y += item["line_height"]

            y += item.get("bottom_gap", 0)

        elif item_type == "list":
            bullet = item["bullet"]
            font = item["font"]
            line_height = item["line_height"]
            indent = item["indent"]

            for idx, line in enumerate(item["lines"]):
                if idx == 0:
                    draw.text((x, y), bullet, fill=item["fill"], font=font)
                draw.text((x + indent, y), line, fill=item["fill"], font=font)
                y += line_height

            y += 4

        elif item_type == "quote":
            line_height = item["line_height"]
            indent = item["indent"]
            height = item["height"]

            # 左侧引用竖线
            draw.rounded_rectangle(
                (x, y + 2, x + 5, y + height - 6),
                radius=2,
                fill=(190, 190, 190)
            )

            for line in item["lines"]:
                draw.text(
                    (x + indent, y),
                    line,
                    fill=item["fill"],
                    font=item["font"]
                )
                y += line_height

            y += 8

        elif item_type == "code":
            height = item["height"]

            # 代码块背景
            draw.rounded_rectangle(
                (x, y, x + max_width, y + height),
                radius=10,
                fill=(242, 242, 242),
                outline=(220, 220, 220)
            )

            code_y = y + 11
            for line in item["lines"]:
                draw.text(
                    (x + 16, code_y),
                    line,
                    fill=item["fill"],
                    font=item["font"]
                )
                code_y += item["line_height"]

            y += height + 6

    return y


def create_changelog_image(commits: list, page: int) -> BytesIO:
    """
    根据 Commits 列表创建更新日志图片。
    支持简易 Markdown 渲染。
    返回 BytesIO 更稳定。
    """
    image_width = 960
    margin_x = 55
    max_text_width = image_width - margin_x * 2

    top_padding = 35
    bottom_padding = 70
    title_height = 58
    commit_gap = 28

    temp_img = Image.new("RGB", (image_width, 100), color=(255, 255, 255))
    temp_draw = ImageDraw.Draw(temp_img)

    commit_render_data = []
    total_content_height = 0

    if commits:
        for commit_data in commits:
            commit = commit_data["commit"]
            message = commit["message"].strip()
            author = commit["author"]["name"]
            date_str = utc_to_utc8(commit["author"]["date"])

            meta_text = f"[{date_str}] {author}"

            blocks = parse_markdown_blocks(message)
            items, md_height = measure_markdown_blocks(
                temp_draw,
                blocks,
                max_text_width - 30
            )

            item_height = 30 + md_height + commit_gap

            commit_render_data.append({
                "meta": meta_text,
                "items": items,
                "height": item_height,
            })

            total_content_height += item_height
    else:
        blocks = parse_markdown_blocks("无法获取更新日志或已是最后一页")
        items, md_height = measure_markdown_blocks(
            temp_draw,
            blocks,
            max_text_width
        )
        commit_render_data.append({
            "meta": "",
            "items": items,
            "height": md_height + commit_gap,
        })
        total_content_height += md_height + commit_gap

    image_height = (
        top_padding
        + title_height
        + total_content_height
        + bottom_padding
    )

    # 防止图片极端过高，可按需限制
    image_height = max(image_height, 500)

    img = Image.new("RGB", (image_width, image_height), color=(255, 255, 255))
    draw = ImageDraw.Draw(img)

    # 背景
    draw.rectangle((0, 0, image_width, image_height), fill=(255, 255, 255))

    # 标题
    title_text = "更新日志"
    title_bbox = draw.textbbox((0, 0), title_text, font=TITLE_FONT)
    title_width = title_bbox[2] - title_bbox[0]
    draw.text(
        ((image_width - title_width) / 2, top_padding),
        title_text,
        fill=(0, 0, 0),
        font=TITLE_FONT
    )

    y = top_padding + title_height

    for idx, item in enumerate(commit_render_data):
        meta = item["meta"]

        # 每个 commit 的轻微分隔线
        if idx > 0:
            draw.line(
                (margin_x, y - 12, image_width - margin_x, y - 12),
                fill=(230, 230, 230),
                width=1
            )

        if meta:
            draw.text(
                (margin_x, y),
                meta,
                fill=(120, 120, 120),
                font=META_FONT
            )
            y += 30

        y = draw_markdown_items(
            draw,
            item["items"],
            margin_x + 12,
            y,
            max_text_width - 24
        )

        y += commit_gap

    # 页脚
    footer_text = f"第 {page} 页 | 使用【更新日志 {page + 1}】查看更多"
    footer_bbox = draw.textbbox((0, 0), footer_text, font=SMALL_FONT)
    footer_width = footer_bbox[2] - footer_bbox[0]

    draw.text(
        ((image_width - footer_width) / 2, image_height - 45),
        footer_text,
        fill=(130, 130, 130),
        font=SMALL_FONT
    )

    # 返回 BytesIO，避免路径问题
    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf
