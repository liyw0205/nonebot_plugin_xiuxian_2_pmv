import requests
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from nonebot.log import logger
from datetime import datetime, timedelta
import textwrap

API_URL = "https://api.github.com/repos/liyw0205/nonebot_plugin_xiuxian_2_pmv/commits"
ITEMS_PER_PAGE = 10  # 每页显示的条目数

FONT_PATH = Path() / "data" / "xiuxian" / "font" / "SourceHanSerifCN-Heavy.otf"


def get_commits(page: int, per_page: int = ITEMS_PER_PAGE):
    """从GitHub获取指定页数的Commits"""
    params = {"page": page, "per_page": per_page}
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"从GitHub获取Commits失败: {e}")
        return None


def utc_to_utc8(utc_time_str: str) -> str:
    """将UTC时间转换为UTC+8时间"""
    try:
        # 解析UTC时间（格式：2024-01-01T12:00:00Z）
        utc_time = datetime.strptime(utc_time_str.replace('Z', ''), '%Y-%m-%dT%H:%M:%S')
        # 转换为UTC+8时间
        utc8_time = utc_time + timedelta(hours=8)
        # 格式化为字符串：年-月-日 时:分
        return utc8_time.strftime('%Y-%m-%d %H:%M')
    except Exception as e:
        logger.error(f"时间转换失败: {e}")
        return utc_time_str.split('T')[0]  # 失败时返回日期部分


def create_changelog_image(commits: list, page: int) -> Path:
    """根据Commits列表创建更新日志图片"""
    # 创建一张空白图片（增加高度以适应换行）
    image_width = 800
    image_height = 800  # 增加高度以适应换行内容
    bg_color = (255, 255, 255)  # 白色背景
    img = Image.new('RGB', (image_width, image_height), color=bg_color)
    draw = ImageDraw.Draw(img)

    try:
        title_font = ImageFont.truetype(str(FONT_PATH), 32)
        commit_font = ImageFont.truetype(str(FONT_PATH), 18)
        footer_font = ImageFont.truetype(str(FONT_PATH), 16)
    except IOError:
        logger.warning("字体文件未找到，将使用默认字体。")
        title_font = ImageFont.load_default()
        commit_font = ImageFont.load_default()
        footer_font = ImageFont.load_default()

    # 绘制标题
    title_text = "更新日志"
    title_bbox = draw.textbbox((0, 0), title_text, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    draw.text(((image_width - title_width) / 2, 30), title_text, fill=(0, 0, 0), font=title_font)  # 黑色文字

    # 绘制提交信息
    if commits:
        y_position = 100
        max_line_width = image_width - 100  # 左右各留50像素边距
        
        for commit_data in commits:
            commit = commit_data['commit']
            message = commit['message'].split('\n')[0]  # 只取第一行
            author = commit['author']['name']
            date_str = utc_to_utc8(commit['author']['date'])  # 转换为UTC+8时间
            
            # 基础信息（时间、作者）
            base_info = f"[{date_str}] {author}"
            base_color = (128, 128, 128)  # 灰色
            
            # 绘制基础信息
            draw.text((50, y_position), base_info, fill=base_color, font=commit_font)
            y_position += 30
            
            # 处理提交信息换行
            lines = textwrap.wrap(message, width=30)  # 每行约30个字符
            if not lines:  # 如果消息为空
                lines = [""]
            
            for line in lines:
                # 检查是否会超出图片底部
                if y_position > image_height - 100:
                    break
                
                draw.text((70, y_position), line, fill=(0, 0, 0), font=commit_font)  # 黑色文字
                y_position += 25
            
            y_position += 15  # 条目间距
            
            # 检查是否超出图片范围
            if y_position > image_height - 100:
                break
                
    else:
        no_commits_text = "无法获取更新日志或已是最后一页"
        no_commits_bbox = draw.textbbox((0, 0), no_commits_text, font=commit_font)
        no_commits_width = no_commits_bbox[2] - no_commits_bbox[0]
        draw.text(((image_width - no_commits_width) / 2, image_height / 2), 
                 no_commits_text, fill=(0, 0, 0), font=commit_font)

    # 绘制页脚
    footer_text = f"第 {page} 页 | 使用 '更新日志 <页数>' 查看更多"
    footer_bbox = draw.textbbox((0, 0), footer_text, font=footer_font)
    footer_width = footer_bbox[2] - footer_bbox[0]
    draw.text(((image_width - footer_width) / 2, image_height - 50), 
             footer_text, fill=(128, 128, 128), font=footer_font)

    # 保存图片
    output_dir = Path(__file__).parent / "cache"
    output_dir.mkdir(parents=True, exist_ok=True)
    image_path = output_dir / f"changelog_page_{page}.png"
    img.save(image_path)

    return image_path.resolve()
