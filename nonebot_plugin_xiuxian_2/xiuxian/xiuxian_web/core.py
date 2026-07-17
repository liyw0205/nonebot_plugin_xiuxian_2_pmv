import os
import asyncio
import json
import re
import ast
import platform
import secrets
import time
import signal
import subprocess
import select
import struct
import threading
import random
import uuid
from werkzeug.utils import secure_filename
from io import BytesIO
from PIL import Image

IS_WINDOWS = platform.system() == "Windows"

if not IS_WINDOWS:
    import pty
    import fcntl
    import termios
else:
    pty = None
    fcntl = None
    termios = None
from pathlib import Path
from datetime import datetime, timedelta

from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file, abort, Response

from nonebot.log import logger
from nonebot import get_driver, get_bots, __version__ as nb_version
from ...paths import get_paths
# --- 消息统计核心导入 ---
from nonebot.message import event_preprocessor
from nonebot.adapters import Bot as BaseBot, Event
from ..adapter_compat import MessageSegment
from ..qq_compat import bot_selector
from ..adapter_message_actions import delete_message_compat
from ..adapter_message_records import (
    extract_result_message_id,
    extract_result_reference_id,
    get_bot_id,
    record_web_send_message,
)
from ..xiuxian_utils.message_db import connect_message_db
from typing import Any
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils import db_backend
from ..xiuxian_config import XiuConfig, Xiu_Plugin, convert_rank
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.download_xiuxian_data import UpdateManager
from ..xiuxian_utils.xiuxian2_handle import config_impart, trade_manager
from ..xiuxian_utils.periods import format_duration_full
from ..infrastructure import settings
from .access import (
    WebPermission,
    resolve_endpoint_permission,
    undeclared_endpoints,
)

# --- 辅助函数 ---
def format_time(seconds: float) -> str:
    return format_duration_full(seconds, zero="未知")

def sql_ident(name):
    return db_backend.quote_ident(str(name))


def sql_like_text(field):
    return f"CAST({sql_ident(field)} AS TEXT) LIKE %s"

# --- Psutil 处理 ---
psutil_available = False
try:
    import psutil
    psutil_available = True
except ImportError:
    class Dummy: pass
    psutil = Dummy()

items = Items()
update_manager = UpdateManager()
WEB_CONFIG = XiuConfig()
app = Flask(__name__)


def _config_value(name: str, default=None):
    return settings.get(name, default)


def _config_bool(name: str, default: bool = False) -> bool:
    value = _config_value(name, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _config_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        return max(minimum, int(float(_config_value(name, default))))
    except Exception:
        return default


def _load_or_create_web_secret_key() -> str:
    configured = (
        os.getenv("XIUXIAN_WEB_SECRET_KEY")
        or str(_config_value("xiuxian_web_secret_key", "") or "")
        or str(_config_value("web_secret_key", "") or "")
    ).strip()
    if configured:
        return configured

    secret_file = get_paths().data / "web_secret_key"
    try:
        if secret_file.exists():
            secret = secret_file.read_text(encoding="utf-8").strip()
            if secret:
                return secret
        secret_file.parent.mkdir(parents=True, exist_ok=True)
        secret = secrets.token_urlsafe(48)
        secret_file.write_text(secret + "\n", encoding="utf-8")
        try:
            os.chmod(secret_file, 0o600)
        except Exception:
            pass
        return secret
    except Exception as e:
        logger.warning(f"Web 面板密钥持久化失败，将使用本次进程临时密钥：{e}")
        return secrets.token_urlsafe(48)


def initialize_web_storage() -> None:
    """Prepare persistent Web state during the NoneBot startup phase."""
    app.secret_key = _load_or_create_web_secret_key()
    WEB_UPLOAD_CACHE.mkdir(parents=True, exist_ok=True)


app.secret_key = secrets.token_urlsafe(48)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=_config_bool("web_session_cookie_secure", False),
    PERMANENT_SESSION_LIFETIME=timedelta(
        minutes=_config_int("web_session_lifetime_minutes", 720, 1)
    ),
)


def api_success(**payload):
    data = {"success": True}
    data.update(payload)
    return jsonify(data)


def api_error(error, status=None, **payload):
    data = {"success": False, "error": str(error) if error else "请求失败"}
    data.update(payload)
    response = jsonify(data)
    if status is not None:
        return response, status
    return response


def _json_request_expected() -> bool:
    accept = request.headers.get("Accept", "")
    requested_with = request.headers.get("X-Requested-With", "")
    return (
        request.path.startswith("/api/")
        or "application/json" in accept
        or requested_with.lower() == "fetch"
        or request.is_json
    )


def web_error(message: str, status: int = 403):
    if _json_request_expected():
        return api_error(message, status=status)
    return message, status


def is_admin_logged_in() -> bool:
    if not ADMIN_IDS:
        return True
    admin_id = session.get("admin_id")
    return bool(admin_id and admin_id in ADMIN_IDS)


def web_auth_is_enabled() -> bool:
    return bool(ADMIN_IDS)


def _is_local_request() -> bool:
    remote_addr = request.remote_addr or ""
    return remote_addr in {"127.0.0.1", "::1", "localhost"}


def is_local_web_request() -> bool:
    return _is_local_request()


def get_endpoint_permission(endpoint: str | None = None, method: str | None = None):
    return resolve_endpoint_permission(
        request.endpoint if endpoint is None else endpoint,
        method or request.method,
    )


def undeclared_web_endpoints() -> set[str]:
    return undeclared_endpoints(app)


def terminal_authorization_is_valid() -> bool:
    if not web_auth_is_enabled():
        return True
    try:
        return float(session.get("terminal_authorized_until", 0)) > time.time()
    except (TypeError, ValueError):
        return False


def safe_path_under(base_dir, *parts) -> Path:
    base = Path(base_dir).resolve()
    candidate = base.joinpath(*[str(part) for part in parts]).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise ValueError("路径不在允许目录内") from exc
    return candidate


def _authorization_error():
    # 未匹配到路由时 endpoint 为 None（favicon/扫描探针/拼写错误等），
    # 不能当成“权限未声明”刷 ERROR；静默 404 即可。
    endpoint = request.endpoint
    if endpoint is None:
        path = (request.path or "").strip() or "/"
        logger.debug(f"Web 未匹配路由：{request.method} {path}")
        return web_error("Not Found", 404)

    permission = get_endpoint_permission(endpoint)
    if permission is None:
        # 真实注册了路由却漏写权限表：这才是需要修代码的问题
        logger.error(
            f"拒绝访问未声明 Web 权限的端点：{endpoint} "
            f"method={request.method} path={request.path}"
        )
        return web_error("Web 端点未声明访问权限", 403)
    if permission == WebPermission.PUBLIC:
        return None
    if permission == WebPermission.LOCAL_UPLOAD:
        if _is_local_request():
            return None
    if not is_admin_logged_in():
        if endpoint in {
            "home", "logout", "update", "backups", "database", "commands", "logs",
            "messages_page", "activity_management", "reward_center", "command_registry",
            "config_management", "economy_logs", "terminal", "terminal_confirm",
            "scheduler_management",
        }:
            return redirect(url_for("login"))
        return api_error("未登录", status=401)
    if permission == WebPermission.TERMINAL and not terminal_authorization_is_valid():
        if endpoint == "terminal":
            return redirect(url_for("terminal_confirm"))
        return api_error("Web 终端需要重新确认密码", status=403)
    return None


def get_csrf_token() -> str:
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def _csrf_exempt_for_request() -> bool:
    if request.endpoint == "static":
        return True
    if request.endpoint == "upload_api_image" and _is_local_request():
        return True
    return False


def _validate_csrf_token():
    if not _config_bool("web_require_csrf", True):
        return None
    if request.method.upper() not in {"POST", "PUT", "PATCH", "DELETE"}:
        return None
    if _csrf_exempt_for_request():
        return None

    expected = session.get("_csrf_token")
    provided = (
        request.headers.get("X-CSRF-Token")
        or request.form.get("_csrf_token")
        or request.args.get("_csrf_token")
    )
    if expected and provided and secrets.compare_digest(str(expected), str(provided)):
        return None
    return web_error("CSRF 校验失败，请刷新页面后重试", 403)


def _validate_host_header():
    allowed_hosts = _config_value("web_allowed_hosts", [])
    if isinstance(allowed_hosts, str):
        allowed = {item.strip().lower() for item in allowed_hosts.split(",") if item.strip()}
    else:
        try:
            allowed = {str(item).strip().lower() for item in allowed_hosts if str(item).strip()}
        except Exception:
            allowed = set()
    if not allowed:
        return None

    host = (request.host or "").split(":", 1)[0].lower()
    if host in allowed:
        return None
    return web_error("Host 不在 Web 面板允许列表中", 403)


@app.before_request
def enforce_web_panel_security():
    host_error = _validate_host_header()
    if host_error:
        return host_error

    if not web_auth_is_enabled():
        session.setdefault("admin_id", "local")
        session.setdefault("_csrf_token", secrets.token_urlsafe(32))

    authorization_error = _authorization_error()
    if authorization_error:
        return authorization_error

    csrf_error = _validate_csrf_token()
    if csrf_error:
        return csrf_error

    if is_admin_logged_in():
        session.permanent = True


@app.after_request
def audit_sensitive_web_operation(response):
    if request.endpoint is None:
        return response
    permission = get_endpoint_permission()
    if permission in {
        WebPermission.DATABASE_WRITE,
        WebPermission.MESSAGE,
        WebPermission.BACKUP,
        WebPermission.UPDATE,
        WebPermission.SCHEDULER,
        WebPermission.TERMINAL,
    }:
        target = ",".join(
            f"{key}={value}" for key, value in sorted((request.view_args or {}).items())
        ) or "-"
        logger.info(
            f"Web 敏感操作：admin={session.get('admin_id', 'anonymous')} "
            f"endpoint={request.endpoint} method={request.method} "
            f"target={target} status={response.status_code}"
        )
    return response


# 配置
XIUXIANDATA = get_paths().data_root
DATABASE = get_paths().game_db
IMPART_DB = get_paths().impart_db
PLAYER_DB = get_paths().player_db
TRADE_DB = get_paths().trade_db
ACTIVITY_DB = get_paths().data / "activity" / "activity.db"
ADMIN_IDS = get_driver().config.superusers
PORT = WEB_CONFIG.web_port
HOST = WEB_CONFIG.web_host

WEB_UPLOAD_CACHE = get_paths().cache / "web_uploads"

ALLOWED_MEDIA_TYPES = {"image", "video", "audio", "file"}


def build_web_message_segment(bot, *, content: str, send_mode: str = "plain",
                              media_type: str = "", media_input=None,
                              quote_message_id: str = ""):
    """
    构造 Web 发送消息：
    - send_mode=plain: 普通文本
    - send_mode=markdown: 原生 Markdown
    - media_type=image/video/audio/file
    - media_input: URL / Path / BytesIO / bytes
    """
    send_mode = send_mode or "plain"
    media_type = media_type or ""

    # 原生 Markdown：只发文本 Markdown，不混媒体
    if send_mode == "markdown":
        return MessageSegment.markdown(bot, content or " ")

    msg = None

    if content:
        msg = MessageSegment.text(bot, content)

    if media_type and media_input is not None:
        if media_type == "image":
            seg = MessageSegment.image(bot, media_input)
        elif media_type == "video":
            seg = MessageSegment.video(bot, media_input)
        elif media_type == "audio":
            seg = MessageSegment.audio(bot, media_input)
        else:
            seg = MessageSegment.file(bot, media_input)

        msg = seg if msg is None else msg + seg

    if msg is None:
        msg = MessageSegment.text(bot, " ")

    if quote_message_id and is_qq_bot_for_web_message(bot):
        ref = MessageSegment.reference(bot, quote_message_id)
        if ref:
            msg = ref + msg

    return msg


def is_qq_bot_for_web_message(bot) -> bool:
    try:
        return str(bot.adapter.get_name()) == "QQ"
    except Exception:
        return False


def save_uploaded_media(file_storage):
    """
    保存上传文件到临时目录，返回 Path。
    """
    filename = secure_filename(file_storage.filename or "upload.bin")
    suffix = Path(filename).suffix
    save_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}{suffix}"
    save_path = WEB_UPLOAD_CACHE / save_name
    file_storage.save(save_path)
    return save_path


def run_async(coro):
    """在同步环境执行协程，兼容已有事件循环/无事件循环两种情况"""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result = {}

    def _runner():
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            result["value"] = new_loop.run_until_complete(coro)
        finally:
            new_loop.close()

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    return result.get("value")

# 境界和灵根预设
LEVELS = convert_rank('江湖好手')[1]

ROOTS = {
    "1": "混沌灵根",
    "2": "融合灵根",
    "3": "超灵根",
    "4": "龙灵根",
    "5": "天灵根",
    "6": "轮回道果",
    "7": "真·轮回道果",
    "8": "永恒道果",
    "9": "命运道果"
}

# 指令中心
ADMIN_COMMANDS = {
    "gm_command": {
        "name": "神秘力量",
        "description": "修改灵石数量",
        "params": [
            {"name": "目标", "type": "select", "options": ["指定用户", "全服"], "key": "target"},
            {"name": "道号", "type": "text", "required": False, "key": "username", "show_if": {"target": "指定用户"}},
            {"name": "数量", "type": "number", "required": True, "key": "amount"}
        ]
    },
    "adjust_exp_command": {
        "name": "修为调整",
        "description": "修改修为数量",
        "params": [
            {"name": "目标", "type": "select", "options": ["指定用户", "全服"], "key": "target"},
            {"name": "道号", "type": "text", "required": False, "key": "username", "show_if": {"target": "指定用户"}},
            {"name": "数量", "type": "number", "required": True, "key": "amount"}
        ]
    },
    "gmm_command": {
        "name": "轮回力量",
        "description": "修改灵根",
        "params": [
            {"name": "道号", "type": "text", "required": True, "key": "username"},
            {"name": "灵根类型", "type": "select", "options": ROOTS, "key": "root_type"}
        ]
    },
    "zaohua_xiuxian": {
        "name": "造化力量",
        "description": "修改境界",
        "params": [
            {"name": "道号", "type": "text", "required": True, "key": "username"},
            {"name": "境界", "type": "select", "options": LEVELS, "key": "level"}
        ]
    },
    "cz": {
        "name": "创造力量",
        "description": "发放物品",
        "params": [
            {"name": "目标", "type": "select", "options": ["指定用户", "全服"], "key": "target"},
            {"name": "道号", "type": "text", "required": False, "key": "username", "show_if": {"target": "指定用户"}},
            {"name": "物品", "type": "text", "required": True, "key": "item", "placeholder": "物品名称或ID"},
            {"name": "数量", "type": "number", "required": True, "key": "amount"}
        ]
    },
    "hmll": {
        "name": "毁灭力量",
        "description": "扣除物品",
        "params": [
            {"name": "目标", "type": "select", "options": ["指定用户", "全服"], "key": "target"},
            {"name": "道号", "type": "text", "required": False, "key": "username", "show_if": {"target": "指定用户"}},
            {"name": "物品", "type": "text", "required": True, "key": "item", "placeholder": "物品名称或ID"},
            {"name": "数量", "type": "number", "required": True, "key": "amount"}
        ]
    },
    "ccll_command": {
        "name": "传承力量",
        "description": "修改思恋结晶数量",
        "params": [
            {"name": "目标", "type": "select", "options": ["指定用户", "全服"], "key": "target"},
            {"name": "道号", "type": "text", "required": False, "key": "username", "show_if": {"target": "指定用户"}},
            {"name": "数量", "type": "number", "required": True, "key": "amount"}
        ]
    }
}

# 从配置类获取表结构信息
def get_config_tables():
    """获取所有数据库的表结构，按数据库分组"""
    tables = {
        "主数据库": {
            "path": DATABASE,
            "tables": get_config_table_structure(XiuConfig())
        },
        "虚神界数据库": {
            "path": IMPART_DB,
            "tables": get_impart_table_structure(config_impart)
        },
        "游戏数据库": {
            "path": PLAYER_DB, # 使用新增的常量
            "tables": get_dynamic_player_tables()
        },
        "交易数据库": { # 新增：交易数据库
            "path": TRADE_DB, # 使用新增的常量
            "tables": get_dynamic_trade_tables()
        },
        "活动数据库": {
            "path": ACTIVITY_DB,
            "tables": get_dynamic_activity_tables()
        }
    }
    return tables

def get_dynamic_player_tables():
    """动态获取 player.db 中所有存在的表及其字段信息"""
    # 路径使用常量
    player_db_path = PLAYER_DB
    if not db_backend.database_exists(player_db_path):
        return {}

    try:
        with db_backend.connection(player_db_path) as conn:
            table_names = conn.list_tables()

            result = {}
            for table_name in table_names:
                # 获取字段列表
                fields_info = conn.table_info(table_name)
                fields = [row[1] for row in fields_info]

                pk_columns = conn.get_primary_key_columns(table_name)
                if len(pk_columns) == 1:
                    primary_key = pk_columns[0]
                elif pk_columns:
                    primary_key = pk_columns
                else:
                    primary_key = "user_id" if "user_id" in fields else None

                result[table_name] = {
                    "name": table_name,
                    "fields": fields,
                    "primary_key": primary_key,
                    "is_dynamic": True
                }

        return result

    except Exception as e:
        logger.error(f"获取 player.db 表结构失败: {e}")
        return {}

def get_dynamic_trade_tables():
    """动态获取 trade.db 中所有存在的表及其字段信息"""
    # 路径使用常量
    trade_db_path = TRADE_DB
    if not db_backend.database_exists(trade_db_path):
        return {}

    try:
        with db_backend.connection(trade_db_path) as conn:
            table_names = conn.list_tables()

            result = {}
            for table_name in table_names:
                # 获取字段列表
                fields_info = conn.table_info(table_name)
                fields = [row[1] for row in fields_info]

                pk_columns = conn.get_primary_key_columns(table_name)
                primary_key = pk_columns[0] if len(pk_columns) == 1 else pk_columns or None
                # 特殊处理，如果表有id字段且没有其他明确的主键，且id是Text类型，作为主键
                if not primary_key and 'id' in fields:
                    for row in fields_info:
                        if row[1] == 'id' and row[2].upper() == 'TEXT':
                            primary_key = 'id'
                            break

                result[table_name] = {
                    "name": table_name,
                    "fields": fields,
                    "primary_key": primary_key,
                    "is_dynamic": True
                }

        return result

    except Exception as e:
        logger.error(f"获取 trade.db 表结构失败: {e}")
        return {}


def get_dynamic_activity_tables():
    """动态获取 activity.db 中所有活动运营表。"""
    if not db_backend.database_exists(ACTIVITY_DB):
        return {}

    try:
        with db_backend.connection(ACTIVITY_DB) as conn:
            table_names = conn.list_tables()

            result = {}
            for table_name in table_names:
                fields_info = conn.table_info(table_name)
                fields = [row[1] for row in fields_info]
                pk_columns = conn.get_primary_key_columns(table_name)
                primary_key = pk_columns[0] if len(pk_columns) == 1 else pk_columns or None
                if not primary_key and "id" in fields:
                    primary_key = "id"

                result[table_name] = {
                    "name": table_name,
                    "fields": fields,
                    "primary_key": primary_key,
                    "is_dynamic": True
                }

        return result

    except Exception as e:
        logger.error(f"获取 activity.db 表结构失败: {e}")
        return {}

def get_config_table_structure(config):
    """从XiuConfig获取表结构"""
    tables = {}
    
    # 主用户表
    tables["user_xiuxian"] = {
        "name": "用户修仙信息",
        "fields": config.sql_user_xiuxian,
        "primary_key": "id"
    }
    
    # CD表
    tables["user_cd"] = {
        "name": "用户CD信息",
        "fields": config.sql_user_cd,
        "primary_key": "user_id"
    }
    
    # 宗门表
    tables["sects"] = {
        "name": "宗门信息",
        "fields": config.sql_sects,
        "primary_key": "sect_id"
    }
    
    # 背包表 - 特殊处理复合主键
    tables["back"] = {
        "name": "用户背包",
        "fields": config.sql_back,
        "primary_key": ["user_id", "goods_id"],  # 改为复合主键
        "composite_key": True  # 添加标识
    }
    
    # Buff信息表
    tables["buffinfo"] = {
        "name": "Buff信息",
        "fields": config.sql_buff,
        "primary_key": "id"
    }
    
    return tables

def get_impart_table_structure(config):
    """从IMPART_BUFF_CONFIG获取表结构"""
    tables = {}
    
    # 虚神界表
    tables["xiuxian_impart"] = {
        "name": "虚神界信息",
        "fields": config.sql_table_impart_buff,
        "primary_key": "id"
    }

    # 传承信息表
    tables["impart_cards"] = {
        "name": "传承信息",
        "fields": ["user_id", "card_name", "quantity"],
        "primary_key": ["user_id", "card_name"],  # 复合主键
        "composite_key": True  # 添加复合主键标识
    }
    
    return tables

def get_tables():
    """获取所有数据库的表结构，按数据库分组（使用预设配置）"""
    return get_config_tables()

def get_database_tables(db_path):
    """动态获取数据库中的所有表及其字段信息，包括主键（备用函数）"""
    tables = {}
    if not db_backend.database_exists(db_path): # 添加文件存在性检查
        return {}
    with db_backend.connection(db_path) as conn:
        table_names = conn.list_tables()

        for table_name in table_names:
            fields_info = conn.table_info(table_name)
            fields = [row[1] for row in fields_info]

            pk_columns = [
                (int(row[5]), row[1])
                for row in fields_info
                if row[5]
            ]
            pk_columns.sort(key=lambda item: item[0])
            primary_key_values = [name for _, name in pk_columns]
            primary_key = primary_key_values[0] if len(primary_key_values) == 1 else primary_key_values or None

            tables[table_name] = {
                "name": table_name,
                "fields": fields,
                "primary_key": primary_key
            }

    return tables

def get_db_connection(db_path):
    """获取数据库连接"""
    conn = db_backend.connect(db_path)
    conn.row_factory = db_backend.Row
    return conn

def execute_sql(db_path, sql, params=None):
    """执行SQL语句"""
    return db_backend.execute_sql_safely(db_path, sql, params)

def get_message_db_connection():
    """获取 message.db 连接"""
    return connect_message_db(row_factory=True)

def get_cached_username_by_user_id(user_id: str) -> str:
    """
    从 user_nicknames 表读取缓存昵称。
    """
    if not user_id:
        return ""

    conn = get_message_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT username
            FROM user_nicknames
            WHERE user_id = %s
            LIMIT 1
            """,
            (str(user_id),),
        )
        row = cur.fetchone()
        if row and row["username"]:
            return str(row["username"])
        return ""
    finally:
        conn.close()

def get_message_stats_from_db():
    """首页统计：从 message.db 读取收发消息数量"""
    try:
        conn = get_message_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) AS c FROM messages WHERE direction = 'recv'")
        recv = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(*) AS c FROM messages WHERE direction = 'send'")
        sent = cur.fetchone()["c"]

        return recv, sent

    except Exception:
        return 0, 0

    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_bot_by_adapter(adapter_name: str):
    """
    根据适配器名获取在线 Bot。
    adapter_name 示例：
    - QQ
    - OneBot V11
    - OB11
    """
    return bot_selector.select(adapter=str(adapter_name or "").strip())


def is_ob11_adapter_name(adapter: str) -> bool:
    adapter_lower = str(adapter or "").lower()
    return adapter_lower in ("ob11", "onebot", "onebot v11") or "onebot" in adapter_lower

def get_latest_reply_candidates_for_qq(scene: str, target_id: str, limit: int = 3):
    """
    QQ Web 发送时，自动找可回复的 recv 消息。

    自动规则：
    - 群聊：4 分钟内
    - 私聊：1 小时内
    - 回复次数 < 5
    """
    conn = get_message_db_connection()
    try:
        cur = conn.cursor()

        seconds = get_qq_auto_reply_seconds(scene)
        if seconds <= 0:
            return []

        since = (datetime.now() - timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")

        if scene in ("group", "channel_group"):
            cur.execute("""
                SELECT *
                FROM messages
                WHERE adapter = 'QQ'
                  AND direction = 'recv'
                  AND scene = %s
                  AND group_id = %s
                  AND message_id IS NOT NULL
                  AND message_id != ''
                  AND created_at >= %s
                  AND COALESCE(reply_used_count, 0) < 5
                ORDER BY created_at DESC, id DESC
                LIMIT %s
            """, (scene, str(target_id), since, limit))

        elif scene in ("private", "channel_private"):
            cur.execute("""
                SELECT *
                FROM messages
                WHERE adapter = 'QQ'
                  AND direction = 'recv'
                  AND scene = %s
                  AND user_id = %s
                  AND message_id IS NOT NULL
                  AND message_id != ''
                  AND created_at >= %s
                  AND COALESCE(reply_used_count, 0) < 5
                ORDER BY created_at DESC, id DESC
                LIMIT %s
            """, (scene, str(target_id), since, limit))

        else:
            return []

        return [dict(r) for r in cur.fetchall()]

    finally:
        conn.close()

def get_specific_reply_candidate_for_qq(
    *,
    scene: str,
    target_id: str,
    message_id: str,
):
    """
    Web 点击某条消息后，指定使用这条消息作为 QQ 回复目标。

    注意：
    - 这不是引用回复，只是使用该消息 ID 作为 msg_id。
    - 群聊有效期 5 分钟。
    - 私聊有效期 1 小时。
    - reply_used_count < 5。
    """
    if not message_id:
        return None

    conn = get_message_db_connection()
    try:
        cur = conn.cursor()

        if scene in ("group", "channel_group"):
            cur.execute("""
                SELECT *
                FROM messages
                WHERE adapter = 'QQ'
                  AND direction = 'recv'
                  AND scene = %s
                  AND group_id = %s
                  AND message_id = %s
                  AND COALESCE(reply_used_count, 0) < 5
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (scene, str(target_id), str(message_id)))

        elif scene in ("private", "channel_private"):
            cur.execute("""
                SELECT *
                FROM messages
                WHERE adapter = 'QQ'
                  AND direction = 'recv'
                  AND scene = %s
                  AND user_id = %s
                  AND message_id = %s
                  AND COALESCE(reply_used_count, 0) < 5
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (scene, str(target_id), str(message_id)))

        else:
            return None

        row = cur.fetchone()
        if not row:
            return None

        row = dict(row)

        seconds = get_qq_reply_valid_seconds(scene)
        if not is_message_within_seconds(row.get("created_at", ""), seconds):
            return None

        return row

    finally:
        conn.close()

def get_specific_reference_candidate_for_qq(
    *,
    scene: str,
    target_id: str,
    message_id: str = "",
    reference_id: str = "",
):
    """
    查找 QQ 引用回复目标。

    这里不校验 msg_id 的 5 分钟回复窗口，只用于确认 reference_id
    是否属于当前会话，并从 message_id 反查 reference_id。
    """
    message_id = str(message_id or "").strip()
    reference_id = str(reference_id or "").strip()

    if not message_id and not reference_id:
        return None

    conn = get_message_db_connection()
    try:
        cur = conn.cursor()

        match_sql = []
        match_params = []
        if message_id:
            match_sql.append("message_id = %s")
            match_params.append(message_id)
        if reference_id:
            match_sql.append("reference_id = %s")
            match_params.append(reference_id)

        if scene in ("group", "channel_group"):
            cur.execute(f"""
                SELECT *
                FROM messages
                WHERE adapter = 'QQ'
                  AND direction = 'recv'
                  AND scene = %s
                  AND group_id = %s
                  AND ({' OR '.join(match_sql)})
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, [scene, str(target_id)] + match_params)

        elif scene in ("private", "channel_private"):
            cur.execute(f"""
                SELECT *
                FROM messages
                WHERE adapter = 'QQ'
                  AND direction = 'recv'
                  AND scene = %s
                  AND user_id = %s
                  AND ({' OR '.join(match_sql)})
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, [scene, str(target_id)] + match_params)

        else:
            return None

        row = cur.fetchone()
        return dict(row) if row else None

    finally:
        conn.close()

def get_table_data(db_path, table_name, page=1, per_page=10, search_field=None, search_value=None, search_condition='='):
    """获取表数据（分页和搜索）"""
    try:
        page = max(1, int(page))
    except Exception:
        page = 1
    try:
        per_page = min(200, max(1, int(per_page)))
    except Exception:
        per_page = 10
    offset = (page - 1) * per_page

    # 获取表信息以确定主键和字段
    tables = get_database_tables(db_path)
    table_info = tables.get(table_name, {})
    if not table_info:
        return {"error": "表不存在", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}

    primary_key = table_info.get('primary_key', 'id')
    primary_keys = set(primary_key if isinstance(primary_key, list) else [primary_key])
    fields = table_info.get('fields', [])
    if not fields:
        return {"error": "表中没有字段", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
    if search_field and search_field not in fields:
        return {"error": "搜索字段不存在", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}

    # 构建基础 SELECT 语句，包含所有字段和 COUNT(*) OVER() 作为总数
    table_sql = sql_ident(table_name)
    sql = f"SELECT *, COUNT(*) OVER() AS total_count FROM {table_sql}"

    params = []

    # 构建 WHERE 条件
    where_clauses = []
    if search_field and search_value:
        if search_condition == '=':
            # 处理多关键词搜索
            values = search_value.split()
            if len(values) > 1:
                placeholders = " OR ".join([sql_like_text(search_field) for _ in values])
                where_clauses.append(f"({placeholders})")
                params.extend([f"%{value}%" for value in values])
            else:
                where_clauses.append(sql_like_text(search_field))
                params.append(f"%{search_value}%")
        elif search_condition in ('>', '<'):
            # 数值大于或小于搜索
            values = search_value.split()
            if len(values) > 2:
                return {"error": "搜索值过多", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
            if len(values) == 1:
                # 单个值，保持原样的匹配
                if not search_value.replace('.', '', 1).isdigit():
                    return {"error": "搜索值必须是数值", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
                where_clauses.append(f"{sql_ident(search_field)} {search_condition} %s")
                params.append(float(values[0]))
            else:
                # 两个值，第一个用于比较，第二个用于全字段搜索
                if not values[0].replace('.', '', 1).isdigit():
                    return {"error": "第一个搜索值必须是数值", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
                if not values[1]:
                    return {"error": "第二个搜索值不能为空", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
                where_clauses.append(f"{sql_ident(search_field)} {search_condition} %s")
                searchable_fields = [field for field in fields if field not in primary_keys]
                where_clauses.append(f"({' OR '.join([sql_like_text(field) for field in searchable_fields])})")
                params.extend([float(values[0])] + [f"%{values[1]}%" for field in searchable_fields])
        else:
            return {"error": "无效的搜索条件", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
    elif search_value and not search_field:
        # 全字段搜索逻辑
        # 排除主键字段
        searchable_fields = [field for field in fields if field not in primary_keys]
        if searchable_fields:
            conditions = []
            for field in searchable_fields:
                conditions.append(sql_like_text(field))
                params.append(f"%{search_value}%")
            if conditions:
                where_clauses.append(f"({' OR '.join(conditions)})")
        else:
            # 如果没有可搜索的字段，返回空结果
            where_clauses.append("1=0")  # 确保不返回任何结果

    # 组合 WHERE 条件
    if where_clauses:
        sql += " WHERE " + " AND ".join(where_clauses)

    # 添加分页
    sql += f" LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    # 执行查询
    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return {
                "data": [],
                "total": 0,
                "page": page,
                "per_page": per_page,
                "total_pages": 0
            }

        # 提取总数（来自第一行的 total_count）
        total = rows[0]['total_count']

        # 计算总页数
        total_pages = (total + per_page - 1) // per_page

        # 提取实际数据（排除 total_count 列）
        data = [dict(row) for row in rows]

        return {
            "data": data,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages
        }

    except Exception as e:
        return {
            "error": str(e),
            "data": [],
            "total": 0,
            "page": page,
            "per_page": per_page,
            "total_pages": 0
        }

def get_user_by_name(username):
    """根据道号获取用户信息（使用execute_sql）"""
    sql = "SELECT * FROM user_xiuxian WHERE user_name = %s"
    result = execute_sql(DATABASE, sql, (username,))
    if result and len(result) > 0:
        return result[0]
    return None

def get_user_by_id(user_id):
    """根据ID获取用户信息（使用execute_sql）"""
    sql = "SELECT * FROM user_xiuxian WHERE user_id = %s"
    result = execute_sql(DATABASE, sql, (user_id,))
    if result and len(result) > 0:
        return result[0]
    return None


def get_qq_reply_valid_seconds(scene: str) -> int:
    """
    QQ 消息可回复有效期：
    - 群聊 / 频道群：5分钟
    - 私聊 / 频道私信：1小时
    """
    if scene in ("group", "channel_group"):
        return 5 * 60
    if scene in ("private", "channel_private"):
        return 60 * 60
    return 0


def get_qq_auto_reply_seconds(scene: str) -> int:
    """
    自动挑选回复目标时使用更保守窗口：
    - 群聊自动取 4 分钟内
    - 私聊自动取 1 小时内
    """
    if scene in ("group", "channel_group"):
        return 4 * 60
    if scene in ("private", "channel_private"):
        return 60 * 60
    return 0


def is_message_within_seconds(created_at: str, seconds: int) -> bool:
    if not created_at or seconds <= 0:
        return False

    try:
        msg_time = datetime.strptime(created_at[:19], "%Y-%m-%d %H:%M:%S")
        return datetime.now() - msg_time <= timedelta(seconds=seconds)
    except Exception:
        return False
