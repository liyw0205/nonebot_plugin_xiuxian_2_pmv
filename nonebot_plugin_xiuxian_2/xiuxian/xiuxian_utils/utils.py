import asyncio
import datetime
import io
import json
import math
import os
import random
import unicodedata
from nonebot.log import logger
from base64 import b64encode
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Union, Dict, Any
from nonebot.adapters import MessageSegment
from nonebot.adapters.onebot.v11 import (
    Bot,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
)
from nonebot.params import Depends
from PIL import Image, ImageDraw, ImageFont
from wcwidth import wcwidth

from ..xiuxian_config import XiuConfig
from .data_source import jsondata
from .xiuxian2_handle import XiuxianDateManage, PlayerDataManager
from nonebot.internal.adapter import Message
from typing import Union
from .markdown_segment import MessageSegmentPlus, markdown_param

sql_message = XiuxianDateManage()
player_data_manager = PlayerDataManager()
boss_img_path = Path() / "data" / "xiuxian" / "boss_img"
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"

# 全局字典，存储管理员正在伪装的用户信息
# 键为管理员的实际 user_id (str)，值为被伪装的 user_id (str)
_impersonating_users: Dict[str, str] = {}

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, bytes):
            return str(obj, encoding="utf-8")
        if isinstance(obj, int):
            return int(obj)
        elif isinstance(obj, float):
            return float(obj)
        else:
            return super(MyEncoder, self).default(obj)


def check_user_type(user_id, need_type):
    """
    :说明: `check_user_type`
    > 匹配用户状态，返回是否状态一致
    :返回参数:
      * `isType: 是否一致
      * `msg: 消息体
    """
    isType = False
    msg = ""

    # 先转字符串，避免int/str混用
    actual_user_id = str(user_id)

    # 先读取身外化身当前激活ID（如果没配置则返回本号）
    active_id = player_data_manager.get_field_data(actual_user_id, "avatar", "active_id")
    user_id_to_check = str(active_id) if active_id else actual_user_id

    # 兼容管理员伪装逻辑（优先级高于化身）
    if actual_user_id in _impersonating_users:
        user_id_to_check = _impersonating_users[actual_user_id]
        logger.warning(f"用户 {actual_user_id} 正在伪装 {user_id_to_check}")

    user_cd_message = sql_message.get_user_cd(user_id_to_check)
    if user_cd_message is None:
        user_type = 0
    else:
        user_type = user_cd_message["type"]

    if user_type == need_type:  # 状态一致
        isType = True
    else:
        if user_type == 1:
            msg_list = [
                "道友现在在闭关呢，小心走火入魔！",
                "闭关中，请勿打扰！",
                "道友正在潜心修炼，稍后再来吧~",
                "洞府紧闭，道友正在闭关突破！",
                "修炼要紧，道友请稍候片刻！"
            ]
            msg = random.choice(msg_list)

        elif user_type == 2:
            msg_list = [
                "道友现在在做悬赏令呢，小心走火入魔！",
                "悬赏任务进行中，道友请耐心等待！",
                "道友正在执行悬赏，归来后再叙~",
                "悬赏令完成前，道友不便分心！"
            ]
            msg = random.choice(msg_list)

        elif user_type == 3:
            msg_list = [
                "道友现在在秘境中，分身乏术！",
                "秘境探索中，生死未卜，请等待！",
                "道友深陷秘境，暂时无法回应！",
                "秘境险恶，道友正在艰难前行！",
                "探索秘境的关键时刻，请勿打扰！"
            ]
            msg = random.choice(msg_list)

        elif user_type == 4:
            msg_list = [
                "道友现在在虚神界闭关呢，小心走火入魔！",
                "虚神界修炼中，神识离体，请等待！",
                "道友神魂游虚神界，暂时无法回应！",
                "虚神界参悟大道，道友请勿打扰！"
            ]
            msg = random.choice(msg_list)

        elif user_type == 5:
            msg_list = [
                "道友现在在修炼呢，小心走火入魔！",
                "修炼关键时刻，请勿打扰！",
                "道友正在运功调息，稍后再来！",
                "真气运行中，道友请耐心等待！"
            ]
            msg = random.choice(msg_list)

        elif user_type == 0:
            msg_list = [
                "道友现在什么都没干呢~",
                "闲来无事，道友有何指教？",
                "当前状态：游手好闲中~",
                "道友此刻正悠闲自在呢！",
                "无所事事，等待道友吩咐~"
            ]
            msg = random.choice(msg_list)

    return isType, msg

def check_user(event_or_user_id: Union[GroupMessageEvent, PrivateMessageEvent, str]):
    """
    判断用户信息是否存在 + 是否被关小黑屋
    参数:
        event_or_user_id: 可以是 MessageEvent 对象或用户QQ号字符串
    返回: (isUser: bool, user_info: dict, msg: str)
    """
    isUser = False
    original_user_id = None
    user_id_to_check = None

    if isinstance(event_or_user_id, (GroupMessageEvent, PrivateMessageEvent)):
        original_user_id = str(event_or_user_id.get_user_id())

        # 先走身外化身 active_id（没有则本号）
        active_id = player_data_manager.get_field_data(original_user_id, "avatar", "active_id")
        user_id_to_check = str(active_id) if active_id else original_user_id

        # 兼容管理员伪装（优先级高于化身）
        if original_user_id in _impersonating_users:
            user_id_to_check = _impersonating_users[original_user_id]
            logger.warning(f"管理员 {original_user_id} 正在伪装用户 {user_id_to_check} 执行命令")

    elif isinstance(event_or_user_id, str):
        # 传入字符串时，也支持化身映射
        original_user_id = str(event_or_user_id)
        active_id = player_data_manager.get_field_data(original_user_id, "avatar", "active_id")
        user_id_to_check = str(active_id) if active_id else original_user_id

        # 字符串场景也兼容伪装
        if original_user_id in _impersonating_users:
            user_id_to_check = _impersonating_users[original_user_id]
            logger.warning(f"用户 {original_user_id} 正在伪装用户 {user_id_to_check} 执行命令")
    else:
        return False, None, "传入参数类型错误！请提供event对象或用户QQ号字符串。"

    user_info = sql_message.get_user_info_with_id(user_id_to_check)

    if user_info is None:
        msg = "修仙界没有道友的信息，请输入【我要修仙】加入！"
        return isUser, user_info, msg

    isUser = True

    # 检查是否被关小黑屋
    if user_info.get('is_ban', 0) == 1:
        msg = "道友已被关入小黑屋，期间无法使用任何修仙指令！\n请等待管理员处理或联系管理员申诉。"
        return False, user_info, msg

    # 确保后续逻辑使用当前生效ID（本号/化身/伪装）
    user_info['user_id'] = user_id_to_check

    return isUser, user_info, ""

class Txt2Img:
    """文字转图片"""

    def __init__(self, size=32):
        self.font = str(jsondata.FONT_FILE)
        self.font_size = int(size)
        self.use_font = ImageFont.truetype(font=self.font, size=self.font_size)
        self.upper_size = 30
        self.below_size = 30
        self.left_size = 40
        self.right_size = 55
        self.padding = 12
        self.img_width = 780
        self.black_clor = (255, 255, 255)
        self.line_num = 0

        self.user_font_size = int(size * 1.5)
        self.lrc_font_size = int(size)
        self.font_family = str(jsondata.FONT_FILE)
        self.share_img_width = 1080
        self.line_space = int(size)
        self.lrc_line_space = int(size / 2)

    # 预处理
    def prepare(self, text, scale):
        text = unicodedata.normalize("NFKC", text)
        if scale:
            max_text_len = self.img_width - self.left_size - self.right_size
        else:
            max_text_len = 1080 - self.left_size - self.right_size
        use_font = self.use_font
        line_num = self.line_num
        text_len = 0
        text_new = ""
        for x in text:
            text_new += x
            text_len += use_font.getlength(x)
            if x == "\n":
                text_len = 0
            if text_len >= max_text_len:
                text_len = 0
                text_new += "\n"
        text_new = text_new.replace("\n\n", "\n")
        text_new = text_new.rstrip()
        line_num = line_num + text_new.count("\n")
        return text_new, line_num

    def sync_draw_to(self, text, boss_name="", scale=True):
        font_size = self.font_size
        black_clor = self.black_clor
        upper_size = self.upper_size
        below_size = self.below_size
        left_size = self.left_size
        padding = self.padding
        img_width = self.img_width
        use_font = self.use_font
        text, line_num = self.prepare(text=text, scale=scale)
        if scale:
            if line_num < 5:
                blank_space = int(5 - line_num)
                line_num = 5
                text += "\n"
                for k in range(blank_space):
                    text += "(^ ᵕ ^)\n"
            else:
                line_num = line_num
        else:
            img_width = 1080
            line_num = line_num
        img_hight = int(
            upper_size + below_size + font_size * (line_num + 1) + padding * line_num
        )
        out_img = Image.new(mode="RGB", size=(img_width, img_hight), color=black_clor)
        draw = ImageDraw.Draw(out_img, "RGBA")

        # 设置
        banner_size = 12
        border_color = (220, 211, 196)
        out_padding = 15
        mi_img = Image.open(jsondata.BACKGROUND_FILE)
        mi_banner = Image.open(jsondata.BANNER_FILE).resize(
            (banner_size, banner_size), resample=3
        )

        # 添加背景
        for x in range(int(math.ceil(img_hight / 100))):
            out_img.paste(mi_img, (0, x * 100))

        # 添加边框
        def draw_rectangle(draw, rect, width):
            for i in range(width):
                draw.rectangle(
                    (rect[0] + i, rect[1] + i, rect[2] - i, rect[3] - i),
                    outline=border_color,
                )

        draw_rectangle(
            draw,
            (
                out_padding,
                out_padding,
                img_width - out_padding,
                img_hight - out_padding,
            ),
            2,
        )

        # 添加banner
        out_img.paste(mi_banner, (out_padding, out_padding))
        out_img.paste(
            mi_banner.transpose(Image.FLIP_TOP_BOTTOM),
            (out_padding, img_hight - out_padding - banner_size + 1),
        )
        out_img.paste(
            mi_banner.transpose(Image.FLIP_LEFT_RIGHT),
            (img_width - out_padding - banner_size + 1, out_padding),
        )
        out_img.paste(
            mi_banner.transpose(Image.FLIP_LEFT_RIGHT).transpose(Image.FLIP_TOP_BOTTOM),
            (
                img_width - out_padding - banner_size + 1,
                img_hight - out_padding - banner_size + 1,
            ),
        )

        # 绘制文字
        draw.text(
            (left_size, upper_size),
            text,
            font=use_font,
            fill=(125, 101, 89),
            spacing=padding,
        )
        # 贴boss图
        if boss_name:
            boss_img_path = jsondata.BOSS_IMG / f"{boss_name}.png"
            if os.path.exists(boss_img_path):
                boss_img = Image.open(boss_img_path)
                base_cc = boss_img.height / img_hight
                boss_img_w = int(boss_img.width / base_cc)
                boss_img_h = int(boss_img.height / base_cc)
                boss_img = boss_img.resize(
                    (int(boss_img_w), int(boss_img_h)), Image.Resampling.LANCZOS
                )
                out_img.paste(
                    boss_img,
                    (int(img_width - boss_img_w), int(img_hight - boss_img_h)),
                    boss_img,
                )
        if XiuConfig().img_send_type == "io":
            return out_img
        elif XiuConfig().img_send_type == "base64":
            return self.img2b64(out_img)

    def img2b64(self, out_img) -> str:
        """将图片转换为base64"""
        buf = BytesIO()
        out_img.save(buf, format="PNG")
        base64_str = "base64://" + b64encode(buf.getvalue()).decode()
        return base64_str

    async def io_draw_to(self, text, boss_name="", scale=True):
        loop = asyncio.get_running_loop()
        out_img = await loop.run_in_executor(
            None, self.sync_draw_to, text, boss_name, scale
        )
        return await loop.run_in_executor(
            None, self.save_image_with_compression, out_img
        )

    async def save(self, title, lrc):
        """保存图片,涉及title时使用"""
        border_color = (220, 211, 196)
        text_color = (125, 101, 89)

        out_padding = 30
        padding = 45
        banner_size = 20

        user_font = ImageFont.truetype(self.font_family, self.user_font_size)
        lyric_font = ImageFont.truetype(self.font_family, self.lrc_font_size)

        if title == " ":
            title = ""

        lrc = self.wrap(lrc)

        if lrc.find("\n") > -1:
            lrc_rows = len(lrc.split("\n"))
        else:
            lrc_rows = 1

        w = self.share_img_width

        if title:
            inner_h = (
                padding * 2
                + self.user_font_size
                + self.line_space
                + self.lrc_font_size * lrc_rows
                + (lrc_rows - 1) * (self.lrc_line_space)
            )
        else:
            inner_h = (
                padding * 2
                + self.lrc_font_size * lrc_rows
                + (lrc_rows - 1) * (self.lrc_line_space)
            )

        h = out_padding * 2 + inner_h

        out_img = Image.new(mode="RGB", size=(w, h), color=(255, 255, 255))
        draw = ImageDraw.Draw(out_img)

        mi_img = Image.open(jsondata.BACKGROUND_FILE)
        mi_banner = Image.open(jsondata.BANNER_FILE).resize(
            (banner_size, banner_size), resample=3
        )

        # add background
        for x in range(int(math.ceil(h / 100))):
            out_img.paste(mi_img, (0, x * 100))

        # add border
        def draw_rectangle(draw, rect, width):
            for i in range(width):
                draw.rectangle(
                    (rect[0] + i, rect[1] + i, rect[2] - i, rect[3] - i),
                    outline=border_color,
                )

        draw_rectangle(
            draw, (out_padding, out_padding, w - out_padding, h - out_padding), 2
        )

        # add banner
        out_img.paste(mi_banner, (out_padding, out_padding))
        out_img.paste(
            mi_banner.transpose(Image.FLIP_TOP_BOTTOM),
            (out_padding, h - out_padding - banner_size + 1),
        )
        out_img.paste(
            mi_banner.transpose(Image.FLIP_LEFT_RIGHT),
            (w - out_padding - banner_size + 1, out_padding),
        )
        out_img.paste(
            mi_banner.transpose(Image.FLIP_LEFT_RIGHT).transpose(Image.FLIP_TOP_BOTTOM),
            (w - out_padding - banner_size + 1, h - out_padding - banner_size + 1),
        )

        if title:
            tmp_img = Image.new("RGB", (1, 1))
            tmp_draw = ImageDraw.Draw(tmp_img)
            user_bbox = tmp_draw.textbbox(
                (0, 0), title, font=user_font, spacing=self.line_space
            )
            # 四元组(left, top, right, bottom)
            user_w = user_bbox[2] - user_bbox[0]
            user_h = user_bbox[3] - user_bbox[1]
            draw.text(
                ((w - user_w) // 2, out_padding + padding),
                title,
                font=user_font,
                fill=text_color,
                spacing=self.line_space,
            )
            draw.text(
                (
                    out_padding + padding,
                    out_padding + padding + self.user_font_size + self.line_space,
                ),
                lrc,
                font=lyric_font,
                fill=text_color,
                spacing=self.lrc_line_space,
            )
        else:
            draw.text(
                (out_padding + padding, out_padding + padding),
                lrc,
                font=lyric_font,
                fill=text_color,
                spacing=self.lrc_line_space,
            )
        if XiuConfig().img_send_type == "io":
            buf = BytesIO()
            if XiuConfig().img_type == "webp":
                out_img.save(buf, format="WebP")
            elif XiuConfig().img_type == "jpeg":
                out_img.save(buf, format="JPEG")
            buf.seek(0)
            return buf
        elif XiuConfig().img_send_type == "base64":
            return self.img2b64(out_img)

    def save_image_with_compression(self, out_img):
        """对传入图片进行压缩"""
        img_byte_arr = io.BytesIO()
        compression_quality = max(
            1, min(100, 100 - XiuConfig().img_compression_limit)
        )

        if not (0 <= XiuConfig().img_compression_limit <= 100):
            compression_quality = 50

        # 转换为 RGB
        if out_img.mode in ("RGBA", "P"):
            out_img = out_img.convert("RGB")

        try:
            if XiuConfig().img_type == "webp":
                out_img.save(img_byte_arr, format="WebP", quality=compression_quality)
            elif XiuConfig().img_type == "jpeg":
                out_img.save(img_byte_arr, format="JPEG", quality=compression_quality)
            else:
                out_img.save(img_byte_arr, format="WebP", quality=compression_quality)
        except:
            # 尝试降级为 JPEG
            out_img.save(img_byte_arr, format="JPEG", quality=compression_quality)

        img_byte_arr.seek(0)
        return img_byte_arr

    def wrap(self, string):
        max_width = int(1850 / self.lrc_font_size)
        temp_len = 0
        result = ""
        for ch in string:
            result += ch
            temp_len += wcwidth(ch)
            if ch == "\n":
                temp_len = 0
            if temp_len >= max_width:
                temp_len = 0
                result += "\n"
        result = result.rstrip()
        return result


async def get_msg_pic(msg, boss_name="", scale=True):
    img = Txt2Img()
    if XiuConfig().img_send_type == "io":
        pic = await img.io_draw_to(msg, boss_name, scale)
    elif XiuConfig().img_send_type == "base64":
        pic = img.sync_draw_to(msg, boss_name, scale)
    return pic

def CommandObjectID() -> int:
    """
    根据消息事件的类型获取对象id
    私聊->用户id
    群聊->群id
    频道->子频道id
    :return: 对象id
    """

    def _event_id(event):
        if event.message_type == "private":
            return event.user_id
        elif event.message_type == "group":
            return event.group_id
        elif event.message_type == "guild":
            return event.channel_id

    return Depends(_event_id)


def format_number(num):
    """核心格式化逻辑：处理小数和单位"""
    if num < 10000:
        # 无单位时直接返回整数（丢弃小数）
        return str(int(num))
    else:
        # 有单位时保留1位小数，并去除末尾的".0"
        formatted = f"{num:.1f}"
        if formatted.endswith(".0"):
            formatted = formatted[:-2]
        return formatted

def number_to(num):
    """带中文单位的格式化（精确到小数点后2位）"""
    units = [
        "",
        "万",
        "亿",
        "兆",
        "京",
        "垓",
        "秭",
        "穰",
        "沟",
        "涧",
        "正",
        "载",
        "极",
        "恒河沙",
        "阿僧祗",
        "那由他",
        "不思议",
        "无量大",
        "万无量大",
        "亿无量大",
        "兆无量大",
        "京无量大",
        "垓无量大",
        "秭无量大",
        "穰无量大",
        "沟无量大",
        "涧无量大",
        "正无量大",
        "载无量大",
        "极无量大",
    ]
    
    try:
        num = float(num)
    except (TypeError, ValueError):
        raise ValueError("输入必须是数字")

    # 处理负数
    is_negative = False
    if num < 0:
        is_negative = True
        num = abs(num)

    def convert(n, level):
        if level >= len(units) - 1 or n < 10000:
            return n, level
        return convert(n / 10000, level + 1)

    num, level = convert(num, 0)
    
    # 特殊处理：当小数部分为0时显示整数，否则保留1-2位小数
    if num == int(num):
        formatted_num = str(int(num))
    else:
        # 先尝试保留1位小数
        temp = round(num, 1)
        if temp == int(temp):
            formatted_num = str(int(temp))
        else:
            # 保留2位小数并去除末尾的0
            formatted_num = "{0:.2f}".format(num).rstrip('0').rstrip('.')
    
    result = f"{formatted_num}{units[level]}"
    
    # 如果是负数，添加负号
    if is_negative:
        result = f"-{result}"
    
    return result

async def pic_msg_format(msg, event):
    user_name = event.sender.card if event.sender.card else event.sender.nickname
    result = "@" + user_name + "\n" + msg
    return result
            
def append_draw_card_node(bot: Bot, list_tp: list, summary: str, content):
    """添加节点进转发消息

    Args:
        list_tp (list): 要制作的转发消息列表
        summary (str): 转发消息的标题
        content (_type_): 转发消息的内容
    """
    list_tp.append(
        {
            "type": "node",
            "data": {
                "name": summary,
                "uin": bot.self_id,
                "content": content,
            },
        }
    )

async def handle_pagination(
    item_list: List[str],
    current_page: int = 1,
    per_page: int = 31,
    title: str = None,
    empty_msg: str = "空空如也"
) -> Union[List[str], str]:
    """
    通用分页处理函数（返回消息内容版）
    
    参数:
        item_list: 要分页的物品列表
        current_page: 当前页码，默认为1
        per_page: 每页显示的物品数量，默认为31
        title: 可选标题，如"修仙界物品列表"、"我的背包"等
        empty_msg: 列表为空时返回的消息，默认为"空空如也"
        
    返回:
        分页后的消息列表或空提示消息
    """
    if not item_list:
        return empty_msg

    total_items = len(item_list)
    total_pages = (total_items + per_page - 1) // per_page
    
    if current_page < 1 or current_page > total_pages:
        return f"页码错误，有效范围为1~{total_pages}页！"
    
    start_index = (current_page - 1) * per_page
    end_index = start_index + per_page
    paged_items = item_list[start_index:end_index]
    
    final_msg = []
    if title:
        final_msg.append(f"{title}（第{current_page}/{total_pages}页）")
    
    final_msg.extend(paged_items)
    
    final_msg.append(f"提示：发送 命令+页码 查看其他页（共{total_pages}页）")
    
    return final_msg

def optimize_message(msg: Union[Message, str], is_group: bool) -> str:
    """
    优化消息格式，确保将传入的 Message 或 str 处理为干净的字符串。
    
    :param msg: 原始消息，可以是 Message 对象或 str。
    :param is_group: 是否为群聊消息。
    :return: 优化后的纯文本消息 (str)。
    """
    msg_text = str(msg)

    if not msg_text:
        return ""
    
    if is_group:
        if not msg_text.startswith('\n'):
            msg_text = '\n' + msg_text
    else:
        if msg_text.startswith('\n'):
            msg_text = msg_text[1:]
    
    if msg_text.endswith('\n'):
        msg_text = msg_text[:-1]
    
    return msg_text

def optimize_md(msg: Union[Message, str]) -> str:
    """
    优化消息格式，确保将传入的 Message 或 str 处理为干净的字符串。
    
    :param msg: 原始消息，可以是 Message 对象或 str。
    :return: 优化后的纯文本消息 (str)。
    """
    msg_text = str(msg)

    if not msg_text:
        return ""

    if msg_text.startswith('\n'):
        msg_text = msg_text.lstrip('\n')
        if msg_text.startswith('\n'):
            msg_text = msg_text[1:]

    if msg_text.endswith('\n'):
        msg_text = msg_text[:-1]

    msg_text = msg_text.replace('\n', '\r')
    msg_text = msg_text.replace('[', '')
    msg_text = msg_text.replace(']', '')
    return msg_text

def generate_command(msg, status=None, command=None, msg2=None):
    """
    根据状态生成不同的命令字符串。

    :param status: 字符串，表示状态，可以是 'start', 'end' 或 None
    :param msg: 字符串，表示消息内容
    :param command: 字符串，表示命令
    :param msg2: 字符串，表示附加消息内容
    :return: 生成的命令字符串
    """
    if status == 'start':
        return f"{msg}](mqqapi://aio/inlinecmd?command={command}&enter=false&reply=false){msg2}["
    elif status == 'end':
        return f"{msg}](mqqapi://aio/inlinecmd?command={command}&enter=false&reply=false){msg2}"
    else:
        return f"[{msg}"

async def send_msg_handler(bot, event, *args, title=None, page=None, page_param=None, button_id=None):
    """
    统一消息发送处理器
    :param bot: 机器人实例
    :param event: 事件对象
    :param name: 用户名称
    :param uin: 用户ID
    :param msgs: 消息内容列表
    :param messages: 合并转发的消息列表（字典格式）
    :param msg_type: 关键字参数，可用于传递特定命名参数
    """
    is_group = isinstance(event, GroupMessageEvent)
    MAX_LEN = 3800

    def split_to_groups(messages):
        """将消息列表分组，每组总字符数不超过限制"""
        groups, current, current_len = [], [], 0
        for msg in messages:
            content = msg.get("data", {}).get("content", "")
            msg_len = len(content)
            if current_len + msg_len > MAX_LEN and current:
                groups.append(current)
                current = [msg]
                current_len = msg_len
            else:
                current.append(msg)
                current_len += msg_len
        if current:
            groups.append(current)
        return groups
    merge_forward_send = XiuConfig().merge_forward_send
    if XiuConfig().markdown_status:
        merge_forward_send = 1
        
    if merge_forward_send == 1:
        if len(args) == 3:
            name, uin, msgs = args
            msg = "\n".join(msgs)
            # 在合并后应用信息优化
            if XiuConfig().markdown_status and XiuConfig().markdown_id:
                await handle_send_md(bot, event, msg, markdown_id=XiuConfig().markdown_id, title=title, page=page, page_param=page_param, shell=True, button_id=button_id)
                return
            if title:
                msg = title + msg
            if XiuConfig().message_optimization:
                msg = optimize_message(msg, is_group) 
            await handle_send(bot, event, msg)
        elif len(args) == 1 and isinstance(args[0], list):
            merged_contents = [msg["data"]["content"] for msg in args[0]]
            merged_content = "\n\n".join(merged_contents)
            # 在合并后应用信息优化
            if XiuConfig().markdown_status and XiuConfig().markdown_id:
                await handle_send_md(bot, event, merged_content, markdown_id=XiuConfig().markdown_id, title=title, page=page, page_param=page_param, shell=True, button_id=button_id)
                return
            if title:
                msg = title + msg
            if XiuConfig().message_optimization:
                merged_content = optimize_message(merged_content, is_group)
            await handle_send(bot, event, merged_content)
        else:
            raise ValueError("参数数量或类型不匹配")
            
    elif merge_forward_send == 2:
        if len(args) == 3:
            name, uin, msgs = args
            messages = [
                {"type": "node", "data": {"name": name, "uin": uin, "content": msg}}
                for msg in msgs
            ]
            if isinstance(event, GroupMessageEvent):
                await bot.call_api(
                    "send_group_forward_msg", group_id=event.group_id, messages=messages
                )
            else:
                await bot.call_api(
                    "send_private_forward_msg", user_id=event.user_id, messages=messages
                )
        elif len(args) == 1 and isinstance(args[0], list):
            for messages in split_to_groups(args[0]):
                if isinstance(event, GroupMessageEvent):
                    await bot.call_api(
                        "send_group_forward_msg", group_id=event.group_id, messages=messages
                    )
                else:
                    await bot.call_api(
                        "send_private_forward_msg", user_id=event.user_id, messages=messages
                    )
        else:
            raise ValueError("参数数量或类型不匹配")
    elif merge_forward_send == 3:
        if len(args) == 3:
            name, uin, msgs = args
            img = Txt2Img()
            messages = "\n".join(msgs)
            if XiuConfig().img_send_type == "io":
                img_data = await img.io_draw_to(messages)
            elif XiuConfig().img_send_type == "base64":
                img_data = img.sync_draw_to(messages)
            if isinstance(event, GroupMessageEvent):
                await bot.send_group_msg(
                    group_id=event.group_id, message=MessageSegment.image(img_data)
                )
            else:
                await bot.send_private_msg(
                    user_id=event.user_id, message=MessageSegment.image(img_data)
                )
        elif len(args) == 1 and isinstance(args[0], list):
            messages = args[0]
            img = Txt2Img()
            messages = "\n".join([str(msg["data"]["content"]) for msg in messages])
            if XiuConfig().img_send_type == "io":
                img_data = await img.io_draw_to(messages)
            elif XiuConfig().img_send_type == "base64":
                img_data = img.sync_draw_to(messages)
            if isinstance(event, GroupMessageEvent):
                await bot.send_group_msg(
                    group_id=event.group_id, message=MessageSegment.image(img_data)
                )
            else:
                await bot.send_private_msg(
                    user_id=event.user_id, message=MessageSegment.image(img_data)
                )
        else:
            raise ValueError("参数数量或类型不匹配")
    elif merge_forward_send == 4:
        if len(args) == 3:
            name, uin, msgs = args
            msg = "\n".join(msgs)
            if XiuConfig().message_optimization:
                msg = optimize_message(msg, is_group)
            if msg.startswith('\n'):
                msg = msg[1:]
            messages = [
                {"type": "node", "data": {"name": name, "uin": uin, "content": msg}}
            ]
            if is_group:
                await bot.call_api(
                    "send_group_forward_msg", group_id=event.group_id, messages=messages
                )
            else:
                await bot.call_api(
                    "send_private_forward_msg", user_id=event.user_id, messages=messages
                )
        elif len(args) == 1 and isinstance(args[0], list):
            merged_contents = [msg["data"]["content"] for msg in args[0]]
            merged_content = "\n\n".join(merged_contents)
            if XiuConfig().message_optimization:
                merged_content = optimize_message(merged_content, is_group)
            if merged_content.startswith('\n'):
                merged_content = merged_content[1:]
            first_msg = args[0][0] if args[0] else None
            name = first_msg["data"]["name"] if first_msg else "系统"
            uin = first_msg["data"]["uin"] if first_msg else int(bot.self_id)
            messages = [
                {"type": "node", "data": {"name": name, "uin": uin, "content": merged_content}}
            ]
            if is_group:
                await bot.call_api(
                    "send_group_forward_msg", group_id=event.group_id, messages=messages
                )
            else:
                await bot.call_api(
                    "send_private_forward_msg", user_id=event.user_id, messages=messages
                )
        else:
            raise ValueError("参数数量或类型不匹配")

async def handle_send(bot, event, msg: str, title=None, md_type=None, k1=None, v1=None, k2=None, v2=None, k3=None, v3=None, k4=None, v4=None, button_id=None):
    """处理文本，根据配置发送文本或者图片消息"""
    if XiuConfig().markdown_status and XiuConfig().markdown_id and XiuConfig().markdown_id2:
        if md_type:
            await handle_send_md_type(bot, event, msg, md_type, k1, v1, k2, v2, k3, v3, k4, v4, button_id=button_id)
            return
        await handle_send_md(bot, event, msg, markdown_id=XiuConfig().markdown_id, title=title, button_id=button_id)
        return
    if msg == " ":
        return
    is_group = isinstance(event, GroupMessageEvent)
    
    if XiuConfig().message_optimization:
        msg = optimize_message(msg, is_group)
    
    if XiuConfig().img:
        # 处理昵称为空的情况
        # 使用 event.sender.card 或 event.sender.nickname 来获取昵称
        sender_name = event.sender.card if event.sender.card else event.sender.nickname
        pic_msg = f"@{sender_name}\n{msg}" if sender_name else msg
        pic = await get_msg_pic(pic_msg)
        if is_group:
            await bot.send_group_msg(
                group_id=event.group_id, message=MessageSegment.image(pic)
            )
        else:
            await bot.send_private_msg(
                user_id=event.user_id, message=MessageSegment.image(pic)
            )
    else:
        if is_group:
            if XiuConfig().at_sender:
                await bot.send_group_msg(
                    group_id=event.group_id, message=MessageSegment.at(event.get_user_id()) + msg
                )
            else:
                await bot.send_group_msg(
                    group_id=event.group_id, message=msg
                )
        else:
            await bot.send_private_msg(
                user_id=event.user_id, message=msg
            )

def generate_page_param(page_list: list) -> dict:
    values = []
    buttons = []

    # 先收集所有按钮对
    i = 0
    while i + 1 < len(page_list):
        buttons.append((page_list[i], page_list[i+1]))
        i += 2

    # 最多取 9 组（18个参数）
    MAX_BUTTON_PAIRS = 9
    display_buttons = buttons[:MAX_BUTTON_PAIRS]

    # 每3对一组
    for group_idx in range(0, len(display_buttons), 3):
        group = display_buttons[group_idx:group_idx+3]

        for j, (text, cmd) in enumerate(group):
            if j == len(group) - 1 and group_idx + 3 >= len(display_buttons):
                status = "end"
                msg2 = "\r"
            else:
                status = "start"
                msg2 = " " if j < 2 else "\r"

            cmd_str = generate_command(text, command=cmd, status=status, msg2=msg2)
            values.append(cmd_str)

    # 永远把页码文字放在最后（即使被截断了也要显示）
    if len(page_list) % 2 == 1:
        values.append(generate_command(page_list[-1]))

    result = {"key": "t2", "values": values}
    
    return result

async def handle_send_md(bot, event, msg: str, markdown_id=None, shell=None, title=None, page=None, page_param=None, title_param=None, msg_param=None, button_id=None, at_msg=True):
    """发送md模板消息"""
    if not markdown_id:
        await handle_send(bot, event, msg)

    if msg:
        msg = optimize_md(msg)
    if title:
        title = optimize_md(title)
    if page_param:
        page_param = optimize_md(page_param)

    is_group = isinstance(event, GroupMessageEvent)
    open_id = get_real_id(event.user_id)
    shell_param = markdown_param("s1", " ")
    if not title:
        title = " "
        if not shell:
            title = msg
            msg = " "
    if not page:
        if page_param:
            page = markdown_param("t2", page_param)
        else:
            page = markdown_param("t2", " ")
    else:
        page = generate_page_param(page)
    if shell:
        shell_param = markdown_param("s1", "python\r" + msg)
        msg_param = page
    else:
        original_user_id = event.get_user_id()
        if open_id and is_group and XiuConfig().at_sender and at_msg:
            if original_user_id in _impersonating_users:
                # 如果当前用户正在伪装，则@真实用户，但标题显示伪装者的信息
                target_user_id = _impersonating_users[original_user_id]
                target_user_info = sql_message.get_user_info_with_id(target_user_id)
                target_user_name = target_user_info['user_name'] if target_user_info else f"QQ:{target_user_id}"
                title = f"<@{open_id}>\n(伪装[{target_user_name}])\n{title}"
            else:
                title = f"<@{open_id}>\n{title}"
    if not title_param:
        title = optimize_md(title)
        title_param = markdown_param("t1", title)
    if not msg_param:
        msg_param = markdown_param("t2", msg)
    param = [        
        title_param,
        msg_param,
        shell_param,
    ]
    msg = MessageSegmentPlus.markdown_template(markdown_id, param, button_id)
    await bot.send(event=event, message=msg)
    
def check_user_md_type(md_type, event):
    original_user_id = event.user_id
    user_id_to_check = original_user_id
    if original_user_id in _impersonating_users:
        user_id_to_check = _impersonating_users[original_user_id]
        logger.warning(f"用户 {original_user_id} 正在伪装 {user_id_to_check}")

    md_type = int(md_type)
    user_cd_message = sql_message.get_user_cd(user_id_to_check)
    if user_cd_message is None:
        user_type = 0
    else:
        user_type = int(user_cd_message["type"])
    
    k1, v1 = " ", " "
    if user_type == 0 or md_type == user_type:
        k1 = "信息"
        v1 = "我的修仙信息"
    elif user_type == 1:
        k1 = "出关"
        v1 = "出关"
    elif user_type == 2:
        k1 = "悬赏令结算"
        v1 = "悬赏令结算"
    elif user_type == 3:
        k1 = "秘境结算"
        v1 = "秘境结算"
    elif user_type == 4:
        k1 = "虚神界出关"
        v1 = "虚神界出关"
    elif user_type == 5:
        k1 = "重置修炼状态"
        v1 = "重置修炼状态"
    
    return k1, v1

async def handle_send_md_type(bot, event, msg: str, md_type, k1, v1, k2, v2, k3, v3, k4, v4, button_id=None):
    """
    发送md模板消息
    为k2,v2,k3,v3,k4,v4提供默认值，避免在某些md_type下没有传入而导致报错
    """
    k2 = k2 if k2 is not None else "帮助"
    v2 = v2 if v2 is not None else "修仙帮助"
    k3 = k3 if k3 is not None else "存档"
    v3 = v3 if v3 is not None else "我的修仙信息"

    # 根据 md_type 调用 check_user_md_type 或设置特定按钮
    if md_type in ["0", "1", "2", "3", "4", "5"]:
        _k1, _v1 = check_user_md_type(md_type, event)
        # 如果k1,v1未被外部传入，则使用check_user_md_type返回的值
        k1 = k1 if k1 is not None and k1 != " " else _k1
        v1 = v1 if v1 is not None and v1 != " " else _v1
    elif md_type == "我要修仙":
        k1 = "我要修仙"
        v1 = "我要修仙"
        k2 = "帮助"
        v2 = "修仙帮助"
        k3 = "官群"
        v3 = f"{XiuConfig().qqq}"

    msg = optimize_md(msg)
    is_group = isinstance(event, GroupMessageEvent)
    open_id = get_real_id(event.user_id)
    original_user_id = event.get_user_id()
    if open_id and is_group and XiuConfig().at_sender:
        if original_user_id in _impersonating_users:
            # 如果当前用户正在伪装，则@真实用户，但标题显示伪装者的信息
            target_user_id = _impersonating_users[original_user_id]
            target_user_info = sql_message.get_user_info_with_id(target_user_id)
            target_user_name = target_user_info['user_name'] if target_user_info else f"QQ:{target_user_id}"
            msg = f"<@{open_id}>\r(伪装[{target_user_name}])\r{msg}"
        else:
            msg = f"<@{open_id}>\r{msg}"
    
    # 构造 param 列表
    param = [
        markdown_param("t1", msg + '\r\r---\r\r'),
        markdown_param("button_text_1", v1),
        markdown_param("button_show_1", k1),
        markdown_param("button_text_2", v2),
        markdown_param("button_show_2", k2),
        markdown_param("button_text_3", v3),
        markdown_param("button_show_3", k3),
    ]

    # 如果 k4 存在，则修改第三个按钮的构造方式，以支持第四个按钮
    if k4 and v4:
        param = [
            markdown_param("t1", msg + '\r\r---\r\r'),
            markdown_param("button_text_1", v1),
            markdown_param("button_show_1", k1),
            markdown_param("button_text_2", v2),
            markdown_param("button_show_2", k2),
            markdown_param("button_text_3", v3),
            {"key": "button_show_3",
            "values": [
            f"{k3}\" reference=\"false\" />\r<qqbot-cmd-input text=\"{v4}\" show=\"{k4}"
            ]}
        ]
    msg = MessageSegmentPlus.markdown_template(XiuConfig().markdown_id2, param, button_id)
    await bot.send(event=event, message=msg)
    
async def handle_pic_send(bot, event, imgpath: Union[str, Path, BytesIO] = None):
    """
    图片发送函数
    
    参数:
        bot: 机器人实例
        event: 事件对象
        imgpath: 图片路径或BytesIO对象，可以是字符串、Path对象或BytesIO
    """
    try:
        # 处理不同类型的图片输入
        if isinstance(imgpath, (str, Path)):
            # 检查图片文件是否存在
            if not os.path.exists(imgpath):
                logger.error(f"图片文件不存在: {imgpath}")
                await bot.send(event, "图片文件不存在！")
                return
                
            # 读取图片文件
            with open(imgpath, 'rb') as f:
                img_data = BytesIO(f.read())
        elif isinstance(imgpath, BytesIO):
            img_data = imgpath
        else:
            logger.error(f"不支持的图片类型: {type(imgpath)}")
            await bot.send(event, "不支持的图片类型！")
            return
            
        # 根据配置发送图片
        if XiuConfig().img_send_type == "io":
            pic = img_data
        elif XiuConfig().img_send_type == "base64":
            # 转换为base64格式
            img_data.seek(0)
            base64_str = "base64://" + b64encode(img_data.getvalue()).decode()
            pic = base64_str
        else:
            pic = img_data
            
        # 发送图片消息
        if isinstance(event, GroupMessageEvent):
            await bot.send_group_msg(
                group_id=event.group_id, 
                message=MessageSegment.image(pic)
            )
        else:
            await bot.send_private_msg(
                user_id=event.user_id, 
                message=MessageSegment.image(pic)
            )
            
    except Exception as e:
        logger.error(f"发送图片失败: {e}")
        await bot.send(event, f"发送图片失败: {e}")

async def handle_pic_msg_send(bot, event, imgpath: Union[str, Path, BytesIO, Image.Image] = None, text: str = None):
    """
    增强版图片发送处理器，支持图文混合发送
    
    参数:
        bot: 机器人实例
        event: 事件对象
        imgpath: 图片路径/对象，支持以下类型:
            - str: 图片文件路径
            - Path: 图片Path对象
            - BytesIO: 内存中的图片数据
            - Image.Image: PIL图片对象
            - None: 仅发送文字
        text: 要发送的文字内容(可选)
    """
    try:
        message = []
        
        # 添加文字内容
        if text:
            original_user_id = event.get_user_id()
            if original_user_id in _impersonating_users:
                target_user_id = _impersonating_users[original_user_id]
                target_user_info = sql_message.get_user_info_with_id(target_user_id)
                target_user_name = target_user_info['user_name'] if target_user_info else f"QQ:{target_user_id}"
                text = f"(伪装[{target_user_name}])\n{text}"
            message.append(MessageSegment.text("\n" + text))
        
        # 处理图片内容
        if imgpath is not None:
            # 处理不同类型的图片输入
            if isinstance(imgpath, (str, Path)):
                if not os.path.exists(imgpath):
                    logger.error(f"图片文件不存在: {imgpath}")
                    await bot.send(event, "图片文件不存在！")
                    return
                
                with open(imgpath, 'rb') as f:
                    img_data = BytesIO(f.read())
                    
            elif isinstance(imgpath, BytesIO):
                img_data = imgpath
                
            elif isinstance(imgpath, Image.Image):
                img_data = BytesIO()
                imgpath.save(img_data, format='PNG')
                img_data.seek(0)
                
            else:
                logger.error(f"不支持的图片类型: {type(imgpath)}")
                await bot.send(event, "不支持的图片类型！")
                return
            
            # 根据配置转换图片格式
            if XiuConfig().img_send_type == "io":
                pic = img_data
            elif XiuConfig().img_send_type == "base64":
                img_data.seek(0)
                base64_str = "base64://" + b64encode(img_data.getvalue()).decode()
                pic = base64_str
            else:
                pic = img_data
                
            message.append(MessageSegment.image(pic))
        
        # 发送组合消息
        if isinstance(event, GroupMessageEvent):
            await bot.send_group_msg(
                group_id=event.group_id,
                message=message
            )
        else:
            await bot.send_private_msg(
                user_id=event.user_id,
                message=message
            )
            
    except Exception as e:
        logger.error(f"发送图文消息失败: {e}")
        await bot.send(event, f"发送消息失败: {e}")

def log_message(user_id: str, msg: str):
    """
    记录用户日志
    
    参数:
        user_id: 用户ID
        msg: 要记录的日志消息
    """
    clean_old_logs()
    try:
        original_user_id = user_id
        if original_user_id in _impersonating_users:
            user_id = _impersonating_users[original_user_id]
            logger.warning(f"用户 {original_user_id} 正在伪装 {user_id}")
        # 确保用户文件夹存在
        user_dir = PLAYERSDATA / str(user_id)
        if not user_dir.exists():
            os.makedirs(user_dir)
        
        # 创建logs文件夹
        logs_dir = user_dir / "logs"
        if not logs_dir.exists():
            os.makedirs(logs_dir)
        
        # 获取当前日期(格式: 年月日，如250820)
        today = datetime.now().strftime("%y%m%d")
        log_file = logs_dir / f"{today}.log"
        
        # 准备日志数据
        log_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "message": msg
        }
        
        # 如果文件不存在，创建新文件并写入第一条日志
        if not log_file.exists():
            logs = [log_data]
            with open(log_file, "w", encoding="utf-8") as f:
                json.dump(logs, f, ensure_ascii=False, indent=4)
        else:
            # 读取现有日志，将新日志添加到开头
            with open(log_file, "r", encoding="utf-8") as f:
                existing_logs = json.load(f)
            
            existing_logs.insert(0, log_data)
            
            # 写入更新后的日志
            with open(log_file, "w", encoding="utf-8") as f:
                json.dump(existing_logs, f, ensure_ascii=False, indent=4)
                
    except Exception as e:
        logger.error(f"记录日志失败: {e}")

def get_logs(user_id: str, date_str: str = None, page: int = 1, per_page: int = 10) -> dict:
    """
    获取用户日志（增强版）- 支持自动查找最近日志和智能分页
    
    参数:
        user_id: 用户ID
        date_str: 日期字符串(格式: 年月日，如250820)，不传则自动查找最近有日志的日期
        page: 当前页码，默认为1，超出范围自动调整为最后一页
        per_page: 每页显示的日志数量，默认为10
        
    返回:
        增强后的日志信息字典
    """
    def find_recent_log_date(user_id_for_log):
        """查找用户最近有日志记录的日期"""
        try:
            logs_dir = PLAYERSDATA / str(user_id_for_log) / "logs"
            if not logs_dir.exists():
                return None
                
            # 获取所有日志文件并按日期排序（最新的在前）
            log_files = sorted(logs_dir.glob("*.log"), key=lambda x: x.stem, reverse=True)
            return log_files[0].stem if log_files else None
        except:
            return None
    
    try:
        original_user_id = user_id
        user_id_for_log_query = original_user_id
        if original_user_id in _impersonating_users:
            user_id_for_log_query = _impersonating_users[original_user_id]
            logger.warning(f"用户 {original_user_id} 正在伪装 {user_id_for_log_query}")

        # 确定日期：优先使用指定日期，否则查找最近有日志的日期
        target_date = date_str
        if target_date is None:
            target_date = find_recent_log_date(user_id_for_log_query)
            if target_date is None:
                return {
                    "logs": [],
                    "total": 0,
                    "total_pages": 0,
                    "current_page": 1,
                    "message": "暂无任何日志记录",
                    "available_dates": []
                }
        
        # 验证日期格式
        if target_date:
            try:
                datetime.strptime(target_date, "%y%m%d")
            except ValueError:
                return {
                    "logs": [],
                    "total": 0,
                    "total_pages": 0,
                    "current_page": page,
                    "error": "日期格式错误，请使用6位数字格式(yymmdd)，例如250822表示2025年8月22日"
                }
        
        # 构建日志文件路径
        log_file = PLAYERSDATA / str(user_id_for_log_query) / "logs" / f"{target_date}.log"
        
        # 如果指定日期的文件不存在，尝试查找可用的日志日期
        if not log_file.exists():
            available_dates = []
            logs_dir = PLAYERSDATA / str(user_id_for_log_query) / "logs"
            if logs_dir.exists():
                available_dates = sorted([f.stem for f in logs_dir.glob("*.log")], reverse=True)
            
            if available_dates:
                # 自动使用最近的可用日期
                recent_date = available_dates[0]
                log_file = PLAYERSDATA / str(user_id_for_log_query) / "logs" / f"{recent_date}.log"
                if log_file.exists():
                    target_date = recent_date
                    date_str = None
                else:
                    return {
                        "logs": [],
                        "total": 0,
                        "total_pages": 0,
                        "current_page": 1,
                        "message": f"指定日期{target_date}无日志，发现以下可用日期：{', '.join(available_dates[:5])}",
                        "available_dates": available_dates
                    }
            else:
                return {
                    "logs": [],
                    "total": 0,
                    "total_pages": 0,
                    "current_page": 1,
                    "message": "暂无任何日志记录",
                    "available_dates": []
                }
        
        # 读取日志文件
        with open(log_file, "r", encoding="utf-8") as f:
            all_logs = json.load(f)
        
        total_logs = len(all_logs)
        total_pages = max(1, (total_logs + per_page - 1) // per_page)
        
        # 智能页码调整：超出范围自动跳到最后一页
        adjusted_page = page
        if page < 1:
            adjusted_page = 1
        elif page > total_pages:
            adjusted_page = total_pages
        
        # 计算分页范围
        start_index = (adjusted_page - 1) * per_page
        end_index = start_index + per_page
        paged_logs = all_logs[start_index:end_index]
        
        # 获取所有可用日期（用于前端显示）
        available_dates = []
        logs_dir = PLAYERSDATA / str(user_id_for_log_query) / "logs"
        if logs_dir.exists():
            available_dates = sorted([f.stem for f in logs_dir.glob("*.log")], reverse=True)
        
        result = {
            "logs": paged_logs,
            "total": total_logs,
            "total_pages": total_pages,
            "current_page": adjusted_page,
            "date": target_date,
            "available_dates": available_dates
        }
        
        # 添加提示信息
        if adjusted_page != page:
            result["page_adjusted"] = f"页码已从{page}自动调整为{adjusted_page}"
        
        if date_str is None and target_date != datetime.now().strftime("%y%m%d"):
            result["date_auto_selected"] = f"已自动选择最近日志日期：{target_date}"
        
        return result
        
    except Exception as e:
        logger.error(f"获取日志失败: {e}")
        return {
            "logs": [],
            "total": 0,
            "total_pages": 0,
            "current_page": page,
            "error": f"获取日志失败: {str(e)}"
        }

def clean_old_logs(keep_days=10):
    """
    清理旧日志文件，保留指定天数内的日志
    """
    try:
        current_time = datetime.now()
        log_dirs = PLAYERSDATA.rglob("*/logs")

        for log_dir in log_dirs:
            log_files = list(log_dir.glob("*.log"))
            for log_file in log_files:
                try:
                    # 从文件名中提取日期
                    date_str = log_file.stem
                    if len(date_str) != 6:
                        logger.warning(f"日志文件名格式不正确，跳过: {log_file.name}")
                        continue
                    file_date = datetime.strptime(date_str, "%y%m%d")
                    
                    # 计算文件的年龄
                    age_days = (current_time - file_date).days
                    if age_days > keep_days:
                        log_file.unlink()
                        logger.info(f"清理旧日志: {log_file.name} (已保存 {age_days} 天)")
                except ValueError:
                    # 如果文件名格式不匹配，跳过这个文件
                    logger.warning(f"日志文件名格式不正确，跳过: {log_file.name}")
                except Exception as e:
                    logger.error(f"删除日志文件 {log_file.name} 时出错: {e}")
    except Exception as e:
        logger.error(f"清理旧日志时出错: {e}")

def get_statistics_data(user_id: str, key: str = None):
    """
    获取用户的统计数据
    
    参数:
        user_id: 用户ID
        key: 可选参数，指定要获取的统计项键名
        
    返回:
        如果指定key，返回该键对应的值（不存在则返回None）
        如果不指定key，返回整个统计数据字典
    """
    try:
        original_user_id = user_id
        user_id_for_stats = original_user_id
        if original_user_id in _impersonating_users:
            user_id_for_stats = _impersonating_users[original_user_id]
            logger.warning(f"用户 {original_user_id} 正在伪装 {user_id_for_stats}")
        if key:
            return player_data_manager.get_field_data(str(user_id_for_stats), "statistics", key)
        
        stats_data = player_data_manager.get_fields(str(user_id_for_stats), "statistics")
        del stats_data['user_id']
        return stats_data
    except Exception as e:
        return {}

def update_statistics_value(user_id: str, key: str, value: int = None, increment: int = 1) -> dict:
    """
    更新统计数据的参数值
    
    参数:
        user_id: 用户ID
        key: 要更新的参数键名
        value: 要设置的具体值（如果提供，则直接设置为此值）
        increment: 增量值（默认为1，当value为None时使用）
        
    返回:
        更新后的统计数据字典
    """
    try:
        original_user_id = user_id
        user_id_for_stats = original_user_id
        if original_user_id in _impersonating_users:
            user_id_for_stats = _impersonating_users[original_user_id]
            logger.warning(f"用户 {original_user_id} 正在伪装 {user_id_for_stats}")
        stats_data = player_data_manager.get_fields(str(user_id_for_stats), "statistics")
        if not stats_data:
            stats_data = {}
        # 更新指定键的值
        if value is not None:
            # 直接设置具体值
            player_data_manager.update_or_write_data(str(user_id_for_stats), "statistics", key, value)
        else:
            # 增量更新（如果键不存在则初始化为0）
            current_value = stats_data.get(key, 0)
            player_data_manager.update_or_write_data(str(user_id_for_stats), "statistics", key, current_value + increment)
            
    except Exception as e:
        logger.error(f"更新统计数据失败: {e}")

import requests

def get_real_id(id_str):
    """
    调用API接口获取真实ID

    :param id_str: 要查询的ID字符串
    :return: 真实ID (str) 或 None
    """

    url = f"{XiuConfig().gsk_link}/getid?type=2&id={id_str}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get('id')
    except Exception:
        return None