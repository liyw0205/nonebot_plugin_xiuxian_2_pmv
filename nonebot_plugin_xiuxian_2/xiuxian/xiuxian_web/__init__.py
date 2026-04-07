import sqlite3
import os
import asyncio
import json
import re
import ast
import platform
import time
import pty
import signal
import subprocess
import select
import fcntl
import termios
import struct
import threading
from pathlib import Path
from datetime import datetime, timedelta

from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file, abort, Response

from nonebot.log import logger
from nonebot import get_driver, get_bots, __version__ as nb_version
# --- 消息统计核心导入 ---
from nonebot.message import event_preprocessor
from nonebot.adapters import Bot as BaseBot, Event
from ..adapter_compat import MessageSegment
from typing import Any
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, Xiu_Plugin, convert_rank
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.download_xiuxian_data import UpdateManager
from ..xiuxian_utils.xiuxian2_handle import config_impart, trade_manager

# --- 消息统计变量 ---
msg_stats = {
    "received": 0,
    "sent": 0
}

# 钩子：统计收到的消息
@event_preprocessor
async def _count_received(bot: BaseBot, event: Event): # 这里改为 Event
    try:
        # 仅统计消息类型的事件
        if event.get_type() == "message":
            msg_stats["received"] += 1
    except: 
        pass

# 钩子：统计发出的消息
@BaseBot.on_calling_api
async def _count_sent(bot: BaseBot, api: str, data: dict[str, Any]):
    # 只要 API 名字里包含 "send" 或 "post" 且包含 "message" 或 "msg"，基本就是发送消息
    api_lower = api.lower()
    if ("send" in api_lower or "post" in api_lower) and ("msg" in api_lower or "message" in api_lower):
        msg_stats["sent"] += 1

# --- 辅助函数 ---
def format_time(seconds: float) -> str:
    if seconds <= 0: return "未知"
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(days)}天{int(hours)}小时{int(minutes)}分{int(seconds)}秒"

def execute_sql(db_path, sql, params=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        if params: cursor.execute(sql, params)
        else: cursor.execute(sql)
        if sql.strip().lower().startswith('select'):
            return [dict(row) for row in cursor.fetchall()]
        conn.commit()
        return {"affected_rows": cursor.rowcount}
    except Exception as e: return {"error": str(e)}
    finally: conn.close()

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
app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # 用于会话加密

# 配置
XIUXIANDATA = Path() / "data"
DATABASE =  XIUXIANDATA / "xiuxian" / "xiuxian.db"
IMPART_DB = XIUXIANDATA / "xiuxian" / "xiuxian_impart.db"
PLAYER_DB = XIUXIANDATA / "xiuxian" / "player.db" # 新增：player.db 路径
TRADE_DB = XIUXIANDATA / "xiuxian" / "trade.db" # 新增：trade.db 路径
ADMIN_IDS = get_driver().config.superusers
PORT = XiuConfig().web_port
HOST = XiuConfig().web_host

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

# 管理员指令
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
        }
    }
    return tables

def get_dynamic_player_tables():
    """动态获取 player.db 中所有存在的表及其字段信息"""
    # 路径使用常量
    player_db_path = PLAYER_DB
    if not player_db_path.exists():
        return {}

    conn = None
    try:
        conn = sqlite3.connect(player_db_path)
        cursor = conn.cursor()

        # 获取所有表名
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        table_names = [row[0] for row in cursor.fetchall()]

        result = {}
        for table_name in table_names:
            # 获取字段列表
            cursor.execute(f"PRAGMA table_info({table_name})")
            fields_info = cursor.fetchall()
            fields = [row[1] for row in fields_info]

            # 尝试找出主键
            primary_key = "user_id" if "user_id" in fields else None
            if not primary_key:
                # 如果没有 user_id，找第一个 INTEGER PRIMARY KEY
                for row in fields_info:
                    if row[5] == 1:  # pk=1 表示主键
                        primary_key = row[1]
                        break

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
    finally:
        if conn:
            conn.close()

def get_dynamic_trade_tables():
    """动态获取 trade.db 中所有存在的表及其字段信息"""
    # 路径使用常量
    trade_db_path = TRADE_DB
    if not trade_db_path.exists():
        return {}

    conn = None
    try:
        conn = sqlite3.connect(trade_db_path)
        cursor = conn.cursor()

        # 获取所有表名
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        table_names = [row[0] for row in cursor.fetchall()]

        result = {}
        for table_name in table_names:
            # 获取字段列表
            cursor.execute(f"PRAGMA table_info({table_name})")
            fields_info = cursor.fetchall()
            fields = [row[1] for row in fields_info]

            # 尝试找出主键
            primary_key = None
            for row in fields_info:
                if row[5] == 1: # pk=1 表示主键
                    primary_key = row[1]
                    break
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
    finally:
        if conn:
            conn.close()

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
    tables["BuffInfo"] = {
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
    if not Path(db_path).exists(): # 添加文件存在性检查
        return {}
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row # 使用行工厂，使结果可按列名访问
    cursor = conn.cursor()
    
    # 获取所有用户表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    table_names = [row[0] for row in cursor.fetchall()]
    
    for table_name in table_names:
        # 获取表的字段信息
        cursor.execute(f"PRAGMA table_info({table_name})")
        fields_info = cursor.fetchall()
        fields = [row[1] for row in fields_info]
        
        # 查找主键字段
        primary_key = None
        for row in fields_info:
            if row[5] == 1:
                primary_key = row[1]
                break
        
        tables[table_name] = {
            "name": table_name,
            "fields": fields,
            "primary_key": primary_key
        }
    
    conn.close()
    return tables

def get_db_connection(db_path):
    """获取数据库连接"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def execute_sql(db_path, sql, params=None):
    """执行SQL语句"""
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        
        # 判断是否是查询语句
        if sql.strip().lower().startswith('select'):
            result = cursor.fetchall()
            return [dict(row) for row in result]
        else:
            conn.commit()
            return {"affected_rows": cursor.rowcount}
    except Exception as e:
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()

def get_table_data(db_path, table_name, page=1, per_page=10, search_field=None, search_value=None, search_condition='='):
    """获取表数据（分页和搜索）"""
    offset = (page - 1) * per_page

    # 获取表信息以确定主键和字段
    tables = get_database_tables(db_path)
    table_info = tables.get(table_name, {})
    if not table_info:
        return {"error": "表不存在", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}

    primary_key = table_info.get('primary_key', 'id')
    fields = table_info.get('fields', [])
    if not fields:
        return {"error": "表中没有字段", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}

    # 构建基础 SELECT 语句，包含所有字段和 COUNT(*) OVER() 作为总数
    select_fields = ', '.join(fields)
    sql = f"SELECT *, COUNT(*) OVER() AS total_count FROM {table_name}"

    params = []

    # 构建 WHERE 条件
    where_clauses = []
    if search_field and search_value:
        if search_condition == '=':
            # 处理多关键词搜索
            values = search_value.split()
            if len(values) > 1:
                placeholders = " OR ".join([f"{search_field} LIKE ?" for _ in values])
                where_clauses.append(f"({placeholders})")
                params.extend([f"%{value}%" for value in values])
            else:
                where_clauses.append(f"{search_field} LIKE ?")
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
                where_clauses.append(f"{search_field} {search_condition} ?")
                params.append(float(values[0]))
            else:
                # 两个值，第一个用于比较，第二个用于全字段搜索
                if not values[0].replace('.', '', 1).isdigit():
                    return {"error": "第一个搜索值必须是数值", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
                if not values[1]:
                    return {"error": "第二个搜索值不能为空", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
                where_clauses.append(f"{search_field} {search_condition} ?")
                where_clauses.append(f"({' OR '.join([f'{field} LIKE ?' for field in fields if field != primary_key])})")
                params.extend([float(values[0])] + [f"%{values[1]}%" for field in fields if field != primary_key])
        else:
            return {"error": "无效的搜索条件", "data": [], "total": 0, "page": page, "per_page": per_page, "total_pages": 0}
    elif search_value and not search_field:
        # 全字段搜索逻辑
        # 排除主键字段
        searchable_fields = [field for field in fields if field != primary_key]
        if searchable_fields:
            conditions = []
            for field in searchable_fields:
                conditions.append(f"{field} LIKE ?")
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
    sql += f" LIMIT ? OFFSET ?"
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
    sql = "SELECT * FROM user_xiuxian WHERE user_name = ?"
    result = execute_sql(DATABASE, sql, (username,))
    if result and len(result) > 0:
        return result[0]
    return None

def get_user_by_id(user_id):
    """根据ID获取用户信息（使用execute_sql）"""
    sql = "SELECT * FROM user_xiuxian WHERE user_id = ?"
    result = execute_sql(DATABASE, sql, (user_id,))
    if result and len(result) > 0:
        return result[0]
    return None

@app.route('/')
def home():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('home.html', admin_id=session['admin_id'])

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        admin_id = request.form.get('admin_id')
        if admin_id in ADMIN_IDS:
            session['admin_id'] = admin_id
            return redirect(url_for('home'))
        else:
            return render_template('login.html', error="无效的管理员ID")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('admin_id', None)
    return redirect(url_for('login'))

@app.route('/update')
def update():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('update.html')

@app.route('/check_update')
def check_update():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        latest_release, message = update_manager.check_update()
        
        if latest_release:
            return jsonify({
                "success": True,
                "update_available": True,
                "current_version": update_manager.current_version,
                "latest_version": latest_release['tag_name'],
                "release_name": latest_release['name'],
                "published_at": latest_release['published_at'],
                "changelog": latest_release['body'],
                "message": message
            })
        else:
            return jsonify({
                "success": True,
                "update_available": False,
                "current_version": update_manager.current_version,
                "message": message
            })
            
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/get_releases')
def get_releases():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        releases = update_manager.get_latest_releases(10)
        
        return jsonify({
            "success": True,
            "releases": releases,
            "current_version": update_manager.current_version
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/perform_update', methods=['POST'])
def perform_update():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        release_tag = data.get('release_tag')
        
        if not release_tag:
            return jsonify({"success": False, "error": "未指定release标签"})
        
        success, message = update_manager.perform_update_with_backup(release_tag)
        
        return jsonify({
            "success": success,
            "message": message
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/get_backups')
def get_backups():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        backups = update_manager.get_backups()
        return jsonify({
            "success": True,
            "backups": backups
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/get_cloud_backups')
def get_cloud_backups():
    """获取云端备份列表"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    success, result = update_manager.list_webdav_backups()
    if success:
        return jsonify({"success": True, "backups": result})
    else:
        return jsonify({"success": False, "error": result})

@app.route('/sync_cloud_backup', methods=['POST'])
def sync_cloud_backup():
    """将云端备份同步到本地，包含覆盖检测"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    data = request.get_json()
    filename = data.get('filename')
    overwrite = data.get('overwrite', False) # 是否允许覆盖
    
    if not filename:
        return jsonify({"success": False, "error": "文件名不能为空"})
    
    local_path = Path() / "data" / "xiuxian" / "backups" / filename
    
    # 检测本地是否存在
    if local_path.exists() and not overwrite:
        return jsonify({
            "success": False, 
            "error": "FILE_EXISTS", 
            "message": f"本地已存在同名备份文件 {filename}，是否覆盖下载？"
        })

    success, result = update_manager.download_from_webdav(filename)
    if success:
        return jsonify({"success": True, "message": f"已成功从云端同步: {filename}"})
    else:
        return jsonify({"success": False, "error": str(result)})

@app.route('/cloud_restore_backup', methods=['POST'])
def cloud_restore_backup():
    """云端智能恢复：本地有则直接恢复，本地无则下载后恢复"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    data = request.get_json()
    filename = data.get('filename')
    if not filename:
        return jsonify({"success": False, "error": "无效文件名"})
    
    local_path = Path() / "data" / "xiuxian" / "backups" / filename
    
    # 步骤1：检查本地，没有就同步
    if not local_path.exists():
        logger.info(f"本地无备份 {filename}，正在从云端拉取并准备恢复...")
        success, err = update_manager.download_from_webdav(filename)
        if not success:
            return jsonify({"success": False, "error": f"下载失败: {err}"})
    else:
        logger.info(f"本地已存在备份 {filename}，直接进行本地恢复流程")

    # 步骤2：执行恢复
    success, message = update_manager.restore_backup(filename)
    if success:
        return jsonify({"success": True, "message": message})
    else:
        return jsonify({"success": False, "error": message})

@app.route('/cloud_backup_config', methods=['POST'])
def cloud_backup_config():
    """本地配置备份 + 上传云端"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        # 1) 先本地备份
        backup_success, backup_result = update_manager.backup_all_configs()
        if not backup_success:
            return jsonify({"success": False, "error": f"本地备份失败: {backup_result}"})

        backup_path = backup_result

        # 2) 上传云端
        upload_success, upload_msg = update_manager.upload_config_backup_to_webdav(backup_path)
        if not upload_success:
            return jsonify({"success": False, "error": upload_msg})

        return jsonify({
            "success": True,
            "message": f"配置云备份成功：{Path(backup_path).name}"
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"云备份失败: {e}"})


@app.route('/get_cloud_config_backups')
def get_cloud_config_backups():
    """获取云端配置备份列表"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        success, result = update_manager.list_webdav_config_backups()
        if success:
            return jsonify({"success": True, "backups": result})
        return jsonify({"success": False, "error": result})
    except Exception as e:
        return jsonify({"success": False, "error": f"获取云端配置备份失败: {e}"})


@app.route('/sync_cloud_config_backup', methods=['POST'])
def sync_cloud_config_backup():
    """同步云端配置备份到本地（支持覆盖检测）"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        data = request.get_json()
        filename = data.get('filename')
        overwrite = data.get('overwrite', False)

        if not filename:
            return jsonify({"success": False, "error": "文件名不能为空"})

        local_path = Path() / "data" / "xiuxian" / "backups" / "config_backups" / filename
        if local_path.exists() and not overwrite:
            return jsonify({
                "success": False,
                "error": "FILE_EXISTS",
                "message": f"本地已存在同名配置备份 {filename}，是否覆盖下载？"
            })

        success, result = update_manager.download_config_backup_from_webdav(filename, overwrite=overwrite)
        if success:
            return jsonify({"success": True, "message": f"同步成功: {filename}"})
        else:
            if result == "FILE_EXISTS":
                return jsonify({
                    "success": False,
                    "error": "FILE_EXISTS",
                    "message": f"本地已存在同名配置备份 {filename}，是否覆盖下载？"
                })
            return jsonify({"success": False, "error": str(result)})
    except Exception as e:
        return jsonify({"success": False, "error": f"同步失败: {e}"})


@app.route('/cloud_restore_config_backup', methods=['POST'])
def cloud_restore_config_backup():
    """云端配置恢复（返回配置数据给前端，前端点击保存再落地）"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        data = request.get_json()
        filename = data.get('filename')
        if not filename:
            return jsonify({"success": False, "error": "未指定备份文件"})

        success, result = update_manager.cloud_restore_config_backup(filename)
        if not success:
            return jsonify({"success": False, "error": result})

        return jsonify({
            "success": True,
            "data": result["data"],
            "metadata": result.get("metadata", {}),
            "message": "云端配置已加载，请点击保存所有配置应用。"
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"云恢复失败: {e}"})

@app.route('/restore_backup', methods=['POST'])
def restore_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        backup_filename = data.get('backup_filename')
        
        if not backup_filename:
            return jsonify({"success": False, "error": "未指定备份文件"})
        
        # 执行恢复操作
        success, message = update_manager.restore_backup(backup_filename)
        
        return jsonify({
            "success": success,
            "message": message
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/backups')
def backups():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('backups.html')

@app.route('/manual_db_backup', methods=['POST'])
def manual_db_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    ok, msg = update_manager.backup_db_files()
    return jsonify({"success": ok, "message": msg if ok else "", "error": "" if ok else msg})


@app.route('/get_db_backups')
def get_db_backups():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    try:
        backups = update_manager.get_db_backups()
        return jsonify({"success": True, "backups": backups})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/restore_db_backup', methods=['POST'])
def restore_db_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    data = request.get_json() or {}
    backup_filename = data.get("backup_filename")
    selected_dbs = data.get("selected_dbs", [])
    if not backup_filename:
        return jsonify({"success": False, "error": "未指定备份文件"})
    if not selected_dbs:
        return jsonify({"success": False, "error": "至少选择一个数据库"})
    ok, msg = update_manager.restore_db_files(backup_filename, selected_dbs)
    return jsonify({"success": ok, "message": msg if ok else "", "error": "" if ok else msg})


@app.route('/get_cloud_db_backups')
def get_cloud_db_backups():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    ok, result = update_manager.list_webdav_db_backups()
    if ok:
        return jsonify({"success": True, "backups": result})
    return jsonify({"success": False, "error": result})


@app.route('/sync_cloud_db_backup', methods=['POST'])
def sync_cloud_db_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    data = request.get_json() or {}
    filename = data.get("filename")
    overwrite = data.get("overwrite", False)
    if not filename:
        return jsonify({"success": False, "error": "文件名不能为空"})

    ok, result = update_manager.download_db_backup_from_webdav(filename, overwrite=overwrite)
    if ok:
        return jsonify({"success": True, "message": f"同步成功: {filename}"})
    if result == "FILE_EXISTS":
        return jsonify({"success": False, "error": "FILE_EXISTS", "message": f"本地已存在 {filename}，是否覆盖？"})
    return jsonify({"success": False, "error": str(result)})


@app.route('/cloud_restore_db_backup', methods=['POST'])
def cloud_restore_db_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    data = request.get_json() or {}
    filename = data.get("filename")
    selected_dbs = data.get("selected_dbs", [])
    if not filename:
        return jsonify({"success": False, "error": "未指定云端备份文件"})
    if not selected_dbs:
        return jsonify({"success": False, "error": "至少选择一个数据库"})

    ok, msg = update_manager.cloud_restore_db_files(filename, selected_dbs)
    return jsonify({"success": ok, "message": msg if ok else "", "error": "" if ok else msg})

@app.route('/batch_delete_backups', methods=['POST'])
def batch_delete_backups():
    """批量删除本地插件备份（data/xiuxian/backups/*.zip）"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    try:
        data = request.get_json() or {}
        filenames = data.get('filenames', [])
        if not filenames or not isinstance(filenames, list):
            return jsonify({"success": False, "error": "请提供待删除文件列表"})

        backup_dir = Path() / "data" / "xiuxian" / "backups"
        deleted, failed = [], []

        for name in filenames:
            try:
                # 防止路径穿越
                safe_name = Path(name).name
                f = backup_dir / safe_name
                if f.exists() and f.is_file():
                    f.unlink()
                    deleted.append(safe_name)
                else:
                    failed.append({"filename": safe_name, "reason": "文件不存在"})
            except Exception as e:
                failed.append({"filename": str(name), "reason": str(e)})

        return jsonify({
            "success": True,
            "message": f"批量删除完成，成功 {len(deleted)} 个，失败 {len(failed)} 个",
            "deleted": deleted,
            "failed": failed
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"批量删除失败: {str(e)}"})


@app.route('/batch_sync_cloud_backups', methods=['POST'])
def batch_sync_cloud_backups():
    """批量同步云端插件备份到本地"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        data = request.get_json() or {}
        filenames = data.get('filenames', [])
        overwrite = data.get('overwrite', False)

        if not filenames or not isinstance(filenames, list):
            return jsonify({"success": False, "error": "请提供待同步文件列表"})

        success_list, failed_list, exists_list = [], [], []
        for filename in filenames:
            safe_name = Path(filename).name
            local_path = Path() / "data" / "xiuxian" / "backups" / safe_name

            # 覆盖检测
            if local_path.exists() and not overwrite:
                exists_list.append(safe_name)
                continue

            ok, result = update_manager.download_from_webdav(safe_name)
            if ok:
                success_list.append(safe_name)
            else:
                failed_list.append({
                    "filename": safe_name,
                    "reason": str(result)
                })

        return jsonify({
            "success": True,
            "message": f"批量同步完成：成功 {len(success_list)}，已存在 {len(exists_list)}，失败 {len(failed_list)}",
            "synced": success_list,
            "exists": exists_list,
            "failed": failed_list
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"批量同步失败: {str(e)}"})

@app.route('/batch_delete_db_backups', methods=['POST'])
def batch_delete_db_backups():
    """批量删除本地数据库备份"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    try:
        data = request.get_json() or {}
        filenames = data.get('filenames', [])
        if not filenames or not isinstance(filenames, list):
            return jsonify({"success": False, "error": "请提供待删除文件列表"})

        backup_dir = Path() / "data" / "xiuxian" / "backups" / "db_backup"
        deleted, failed = [], []

        for name in filenames:
            safe_name = Path(name).name
            f = backup_dir / safe_name
            try:
                if f.exists() and f.is_file():
                    f.unlink()
                    deleted.append(safe_name)
                else:
                    failed.append({"filename": safe_name, "reason": "文件不存在"})
            except Exception as e:
                failed.append({"filename": safe_name, "reason": str(e)})

        return jsonify({
            "success": True,
            "message": f"数据库备份删除完成：成功 {len(deleted)}，失败 {len(failed)}",
            "deleted": deleted,
            "failed": failed
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"批量删除失败: {e}"})


@app.route('/batch_sync_cloud_db_backups', methods=['POST'])
def batch_sync_cloud_db_backups():
    """批量同步云端数据库备份到本地"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    try:
        data = request.get_json() or {}
        filenames = data.get('filenames', [])
        overwrite = data.get('overwrite', False)

        if not filenames or not isinstance(filenames, list):
            return jsonify({"success": False, "error": "请提供待同步文件列表"})

        synced, exists, failed = [], [], []

        for filename in filenames:
            safe_name = Path(filename).name
            local_path = Path() / "data" / "xiuxian" / "backups" / "db_backup" / safe_name

            if local_path.exists() and not overwrite:
                exists.append(safe_name)
                continue

            ok, result = update_manager.download_db_backup_from_webdav(safe_name, overwrite=overwrite)
            if ok:
                synced.append(safe_name)
            else:
                if str(result) == "FILE_EXISTS":
                    exists.append(safe_name)
                else:
                    failed.append({"filename": safe_name, "reason": str(result)})

        return jsonify({
            "success": True,
            "message": f"数据库云同步完成：成功 {len(synced)}，已存在 {len(exists)}，失败 {len(failed)}",
            "synced": synced,
            "exists": exists,
            "failed": failed
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"批量同步失败: {e}"})

@app.route('/batch_delete_cloud_backups', methods=['POST'])
def batch_delete_cloud_backups():
    """批量删除云端插件备份"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    try:
        data = request.get_json() or {}
        filenames = data.get('filenames', [])
        if not filenames or not isinstance(filenames, list):
            return jsonify({"success": False, "error": "请提供待删除文件列表"})

        deleted, failed = [], []
        for name in filenames:
            safe_name = Path(name).name
            ok, msg = update_manager.delete_webdav_backup(safe_name)
            if ok:
                deleted.append(safe_name)
            else:
                failed.append({"filename": safe_name, "reason": msg})

        return jsonify({
            "success": True,
            "message": f"云端批量删除完成：成功 {len(deleted)}，失败 {len(failed)}",
            "deleted": deleted,
            "failed": failed
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"批量删除失败: {e}"})


@app.route('/batch_delete_cloud_db_backups', methods=['POST'])
def batch_delete_cloud_db_backups():
    """批量删除云端数据库备份"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    try:
        data = request.get_json() or {}
        filenames = data.get('filenames', [])
        if not filenames or not isinstance(filenames, list):
            return jsonify({"success": False, "error": "请提供待删除文件列表"})

        deleted, failed = [], []
        for name in filenames:
            safe_name = Path(name).name
            ok, msg = update_manager.delete_webdav_db_backup(safe_name)
            if ok:
                deleted.append(safe_name)
            else:
                failed.append({"filename": safe_name, "reason": msg})

        return jsonify({
            "success": True,
            "message": f"云端数据库批量删除完成：成功 {len(deleted)}，失败 {len(failed)}",
            "deleted": deleted,
            "failed": failed
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"批量删除失败: {e}"})

# 配置导入导出路由
@app.route('/export_config', methods=['POST'])
def export_config():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        selected_fields = data.get('selected_fields', [])
        export_all = data.get('export_all', False)
        
        config_values = get_config_values()
        
        # 如果选择全部导出或者没有选择任何字段，则导出所有配置
        if export_all or not selected_fields:
            export_data = config_values
        else:
            # 只导出选中的字段
            export_data = {field: config_values[field] for field in selected_fields if field in config_values}
        
        # 添加元数据
        export_data['_metadata'] = {
            'backup_time': datetime.now().isoformat(),
            'backup_fields': list(export_data.keys()) if export_all else selected_fields,
            'version': update_manager.current_version
        }
        
        return jsonify({
            "success": True,
            "data": export_data,
            "filename": f"xiuxian_config_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"导出配置失败: {str(e)}"})

@app.route('/import_config', methods=['POST'])
def import_config():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        if 'config_file' not in request.files:
            return jsonify({"success": False, "error": "没有上传文件"})
        
        file = request.files['config_file']
        if file.filename == '':
            return jsonify({"success": False, "error": "没有选择文件"})
        
        if not file.filename.endswith('.json'):
            return jsonify({"success": False, "error": "只支持JSON格式文件"})
        
        # 读取并解析JSON文件
        file_content = file.read().decode('utf-8')
        config_data = json.loads(file_content)
        
        # 移除元数据字段
        if '_metadata' in config_data:
            del config_data['_metadata']
        
        return jsonify({
            "success": True,
            "data": config_data,
            "message": "配置导入成功，请点击保存按钮应用配置"
        })
        
    except json.JSONDecodeError:
        return jsonify({"success": False, "error": "文件格式错误，不是有效的JSON"})
    except Exception as e:
        return jsonify({"success": False, "error": f"导入配置失败: {str(e)}"})

@app.route('/backup_config', methods=['POST'])
def backup_config():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        selected_fields = data.get('selected_fields', [])
        backup_all = data.get('backup_all', False)
        
        config_values = get_config_values()
        
        # 如果选择全部备份或者没有选择任何字段，则备份所有配置
        if backup_all or not selected_fields:
            backup_data = config_values
        else:
            # 只备份选中的字段
            backup_data = {field: config_values[field] for field in selected_fields if field in config_values}
        
        # 创建备份目录
        backup_dir = Path() / "data" / "xiuxian" / "backups" / "config_backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"config_backup_{timestamp}.json"
        backup_path = backup_dir / backup_filename
        
        # 添加元数据
        backup_data['_metadata'] = {
            'backup_time': datetime.now().isoformat(),
            'backup_fields': list(backup_data.keys()) if backup_all else selected_fields,
            'version': update_manager.current_version
        }
        
        # 保存备份文件
        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
        
        return jsonify({
            "success": True,
            "message": f"配置备份成功: {backup_filename}",
            "backup_path": str(backup_path)
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"备份配置失败: {str(e)}"})

@app.route('/get_config_backups')
def get_config_backups():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        backup_dir = Path() / "data" / "xiuxian" / "backups" / "config_backups"
        backups = []
        
        if backup_dir.exists():
            for file in backup_dir.glob("config_backup_*.json"):
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        metadata = json.load(f).get('_metadata', {})
                    
                    backups.append({
                        'filename': file.name,
                        'path': str(file),
                        'backup_time': metadata.get('backup_time', ''),
                        'version': metadata.get('version', 'unknown'),
                        'size': file.stat().st_size,
                        'created_at': datetime.fromtimestamp(file.stat().st_ctime).isoformat()
                    })
                except:
                    continue
        
        # 按创建时间倒序排列
        backups.sort(key=lambda x: x['created_at'], reverse=True)
        return jsonify({
            "success": True,
            "backups": backups
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"获取备份列表失败: {str(e)}"})

@app.route('/restore_config_backup', methods=['POST'])
def restore_config_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        backup_filename = data.get('backup_filename')
        
        if not backup_filename:
            return jsonify({"success": False, "error": "未指定备份文件"})
        
        backup_path = Path() / "data" / "xiuxian" / "backups" / "config_backups" / backup_filename
        
        if not backup_path.exists():
            return jsonify({"success": False, "error": f"备份文件不存在: {backup_filename}"})
        
        # 读取备份文件
        with open(backup_path, 'r', encoding='utf-8') as f:
            backup_data = json.load(f)
        
        # 保存元数据
        metadata = backup_data.get('_metadata', {})
        
        # 移除元数据字段
        if '_metadata' in backup_data:
            del backup_data['_metadata']
        
        return jsonify({
            "success": True,
            "data": backup_data,
            "metadata": metadata,
            "message": "配置恢复成功，请点击保存按钮应用配置"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"恢复配置失败: {str(e)}"})

@app.route('/manual_backup', methods=['POST'])
def manual_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        # 执行插件备份
        plugin_success, plugin_result = update_manager.enhanced_backup_current_version()
        
        # 执行配置备份
        config_success, config_result = update_manager.backup_all_configs()
        
        if plugin_success and config_success:
            return jsonify({
                "success": True,
                "message": "手动备份成功完成",
                "plugin_backup": str(plugin_result) if isinstance(plugin_result, Path) else plugin_result,
                "config_backup": str(config_result) if isinstance(config_result, Path) else config_result
            })
        else:
            error_msg = []
            if not plugin_success:
                error_msg.append(f"插件备份失败: {plugin_result}")
            if not config_success:
                error_msg.append(f"配置备份失败: {config_result}")
            
            return jsonify({
                "success": False,
                "error": "; ".join(error_msg)
            })
            
    except Exception as e:
        return jsonify({"success": False, "error": f"备份过程中出现错误: {str(e)}"})

@app.route('/download_backup/<filename>')
def download_backup(filename):
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    
    backup_path = Path() / "data" / "xiuxian" / "backups" / filename
    
    if not backup_path.exists():
        return "备份文件不存在", 404
    
    return send_file(
        str(backup_path.absolute()),
        as_attachment=True,
        download_name=filename,
        mimetype='application/zip'
    )

@app.route('/delete_backup', methods=['POST'])
def delete_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        backup_filename = data.get('backup_filename')
        
        if not backup_filename:
            return jsonify({"success": False, "error": "未指定备份文件"})
        
        backup_path = Path() / "data" / "xiuxian" / "backups" / backup_filename
        
        if not backup_path.exists():
            return jsonify({"success": False, "error": f"备份文件不存在: {backup_filename}"})
        
        # 删除备份文件
        backup_path.unlink()
        
        return jsonify({
            "success": True,
            "message": f"备份文件 {backup_filename} 删除成功"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"删除备份失败: {str(e)}"})

@app.route('/delete_config_backup', methods=['POST'])
def delete_config_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        backup_filename = data.get('backup_filename')
        
        if not backup_filename:
            return jsonify({"success": False, "error": "未指定备份文件"})

        backup_path = Path() / "data" / "xiuxian" / "backups" / "config_backups" / backup_filename
        
        if not backup_path.exists():
            return jsonify({"success": False, "error": f"备份文件不存在: {backup_filename}"})
        
        # 删除文件
        backup_path.unlink()
        
        logger.info(f"配置备份文件已删除: {backup_filename}")
        return jsonify({"success": True, "message": f"配置备份文件删除成功: {backup_filename}"})
        
    except Exception as e:
        log_to_file(f"删除配置备份失败: {str(e)}")
        return jsonify({"success": False, "error": f"删除配置备份失败: {str(e)}"})

@app.route('/database')
def database():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    all_tables = get_tables()
    return render_template('database.html', tables=all_tables)

@app.route('/table/<table_name>', methods=['GET'])
def table_view(table_name):
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    
    # 获取所有表结构（按数据库分组）
    all_tables_grouped = get_tables()
    
    # 确定表属于哪个数据库
    db_path = None
    table_info = None
    
    for db_name, db_info in all_tables_grouped.items():
        if table_name in db_info["tables"]:
            db_path = db_info["path"]
            table_info = db_info["tables"][table_name]
            break
    
    if not db_path:
        # 如果在预设配置中没找到，尝试动态获取 player.db 或 trade.db 中的表
        
        # 检查 player.db
        dynamic_player_tables = get_dynamic_player_tables()
        if table_name in dynamic_player_tables:
            db_path = PLAYER_DB
            table_info = dynamic_player_tables[table_name]
        else:
            # 检查 trade.db
            dynamic_trade_tables = get_dynamic_trade_tables()
            if table_name in dynamic_trade_tables:
                db_path = TRADE_DB
                table_info = dynamic_trade_tables[table_name]
    
    if not db_path:
        return "表不存在", 404
    
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    search_field = request.args.get('search_field')
    search_value = request.args.get('search_value')
    search_condition = request.args.get('search_condition', '=')  # 默认搜索条件是=
    
    table_data = get_table_data(
        db_path, table_name, 
        page=page, per_page=per_page,
        search_field=search_field, search_value=search_value,
        search_condition=search_condition  # 传递搜索条件
    )
    
    return render_template(
        'table_view.html',
        table_name=table_name,
        table_info=table_info,
        data=table_data,
        search_field=search_field,
        search_value=search_value,
        search_condition=search_condition,  # 传递搜索条件到模板
        primary_key=table_info.get('primary_key', 'id')
    )

@app.route('/table/<table_name>/<row_id>', methods=['GET', 'POST'])
def row_edit(table_name, row_id):
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    
    # 获取所有表结构（按数据库分组）
    all_tables_grouped = get_tables()
    
    # 确定表属于哪个数据库
    db_path = None
    table_info = None
    is_dynamic_table = False
    primary_key_field = None  # 主键字段名
    
    # 优先从预设配置中查找
    for db_name, db_info in all_tables_grouped.items():
        if table_name in db_info["tables"]:
            db_path = db_info["path"]
            table_info = db_info["tables"][table_name]
            is_dynamic_table = db_info["tables"][table_name].get('is_dynamic', False)
            break
    
    # 如果在预设配置中没找到，尝试动态获取 player.db 或 trade.db 中的表
    if not db_path:
        # 检查 player.db
        dynamic_player_tables = get_dynamic_player_tables()
        if table_name in dynamic_player_tables:
            db_path = PLAYER_DB
            table_info = dynamic_player_tables[table_name]
            is_dynamic_table = True
        else:
            # 检查 trade.db
            dynamic_trade_tables = get_dynamic_trade_tables()
            if table_name in dynamic_trade_tables:
                db_path = TRADE_DB
                table_info = dynamic_trade_tables[table_name]
                is_dynamic_table = True
    
    if not db_path:
        return "表不存在", 404
    
    # 确定主键字段
    pk = table_info.get('primary_key', 'user_id' if is_dynamic_table else 'id')
    if isinstance(pk, list):
        # 复合主键
        primary_key_field = pk
        is_composite_key = True
    else:
        # 单主键
        primary_key_field = pk
        is_composite_key = False
    
    # 特殊处理复合主键表
    if table_name == "impart_cards":
        key_parts = row_id.split('_')
        if len(key_parts) < 2:
            return "无效的主键格式", 400
        primary_conditions = {
            "user_id": key_parts[0],
            "card_name": "_".join(key_parts[1:])
        }
    elif is_composite_key:
        key_parts = row_id.split('_')
        if len(key_parts) != len(primary_key_field):
            return "无效的主键格式", 400
        primary_conditions = {}
        for i, key in enumerate(primary_key_field):
            primary_conditions[key] = key_parts[i]
    else:
        primary_conditions = {primary_key_field: row_id}
    
    # 确定数据库路径（这里冗余了，但确保了 db_path 的最终正确性）
    # 实际上，db_path 已经在前面通过 all_tables_grouped、dynamic_player_tables 或 dynamic_trade_tables 确定
    # if is_dynamic_table and table_name in get_dynamic_player_tables():
    #     db_path = PLAYER_DB
    # elif is_dynamic_table and table_name in get_dynamic_trade_tables():
    #     db_path = TRADE_DB
    # elif table_name in get_database_tables(IMPART_DB):
    #     db_path = IMPART_DB
    # else:
    #     db_path = DATABASE
    
    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'update':
            update_data = {}
            for field in table_info['fields']:
                if field in request.form and field not in primary_conditions.keys():
                    value = request.form[field]
                    if value == '':
                        update_data[field] = None
                    else:
                        update_data[field] = value
            
            if update_data:
                set_clause = ", ".join([f"{field} = ?" for field in update_data.keys()])
                where_conditions = " AND ".join([f"{key} = ?" for key in primary_conditions.keys()])
                sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_conditions}"
                params = list(update_data.values()) + list(primary_conditions.values())
                result = execute_sql(db_path, sql, params)
                
                if 'error' in result:
                    return jsonify({"success": False, "error": result['error']})
            
            return jsonify({"success": True, "message": "更新成功"})
        
        elif action == 'delete':
            where_conditions = " AND ".join([f"{key} = ?" for key in primary_conditions.keys()])
            sql = f"DELETE FROM {table_name} WHERE {where_conditions}"
            result = execute_sql(db_path, sql, list(primary_conditions.values()))
            
            if 'error' in result:
                return jsonify({"success": False, "error": result['error']})
            
            return jsonify({"success": True, "message": "删除成功"})
    
    # GET 请求，获取行数据
    where_conditions = " AND ".join([f"{key} = ?" for key in primary_conditions.keys()])
    sql = f"SELECT * FROM {table_name} WHERE {where_conditions}"
    row_data = execute_sql(db_path, sql, list(primary_conditions.values()))
    
    if not row_data or (isinstance(row_data, list) and len(row_data) == 0) or (isinstance(row_data, dict) and not row_data):
        return "记录不存在", 404
    
    if isinstance(row_data, dict):
        row_data = [row_data]
    
    if not isinstance(row_data, list) or len(row_data) == 0:
        return "记录不存在", 404
    
    display_data = {}
    for key, value in row_data[0].items():
        if value is None:
            display_data[key] = ''
        else:
            display_data[key] = value
    
    # 传递主键字段名给模板（用于导出功能）
    if is_composite_key:
        primary_key_fields = primary_key_field  # 列表
    else:
        primary_key_fields = [primary_key_field]  # 转为列表
    
    return render_template(
        'row_edit.html',
        table_name=table_name,
        table_info=table_info,
        row_data=display_data,
        primary_key=primary_conditions,
        primary_key_fields=primary_key_fields,
        is_dynamic_table=is_dynamic_table,
        is_composite_key=is_composite_key
    )

@app.route('/batch_edit/<table_name>', methods=['POST'])
def batch_edit(table_name):
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    # 获取所有表结构（按数据库分组）
    all_tables_grouped = get_tables()
    
    # 确定表属于哪个数据库
    db_path = None
    table_info = None
    is_dynamic_table = False  # 标记是否为动态表
    
    # 优先从预设配置中查找
    for db_name, db_info in all_tables_grouped.items():
        if table_name in db_info["tables"]:
            db_path = db_info["path"]
            table_info = db_info["tables"][table_name]
            is_dynamic_table = db_info["tables"][table_name].get('is_dynamic', False)
            break
    
    # 如果在预设配置中没找到，尝试动态获取 player.db 或 trade.db 中的表
    if not db_path:
        # 检查 player.db
        dynamic_player_tables = get_dynamic_player_tables()
        if table_name in dynamic_player_tables:
            db_path = PLAYER_DB
            table_info = dynamic_player_tables[table_name]
            is_dynamic_table = True
        else:
            # 检查 trade.db
            dynamic_trade_tables = get_dynamic_trade_tables()
            if table_name in dynamic_trade_tables:
                db_path = TRADE_DB
                table_info = dynamic_trade_tables[table_name]
                is_dynamic_table = True
    
    if not db_path:
        return jsonify({"success": False, "error": f"表不存在：{table_name}"})
    
    # 获取表单数据
    search_field = request.form.get('search_field')
    search_value = request.form.get('search_value')
    search_condition = request.form.get('search_condition', '=')
    batch_field = request.form.get('batch_field')
    operation = request.form.get('operation')
    value = request.form.get('value')
    apply_to_all = request.form.get('apply_to_all') == 'on'
    
    # 验证参数
    if not all([batch_field, operation, value]):
        return jsonify({"success": False, "error": "参数不完整"})
    
    # 如果是全字段搜索但未选择批量修改字段
    if (not search_field or search_field == '') and not batch_field:
        return jsonify({"success": False, "error": "全字段搜索时请选择要修改的字段"})
    
    # 验证表是否存在
    try:
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if not cursor.fetchone():
            conn.close()
            return jsonify({"success": False, "error": f"表不存在：{table_name}"})
        conn.close()
    except Exception as e:
        return jsonify({"success": False, "error": f"检查表失败：{str(e)}"})
    
    try:
        # 构建更新语句
        if operation == "set":
            sql = f"UPDATE {table_name} SET {batch_field} = ?"
            params = [value]
        elif operation == "add":
            sql = f"UPDATE {table_name} SET {batch_field} = {batch_field} + ?"
            params = [value]
        elif operation == "subtract":
            sql = f"UPDATE {table_name} SET {batch_field} = {batch_field} - ?"
            params = [value]
        else:
            return jsonify({"success": False, "error": "无效的操作类型"})
        
        # 添加 WHERE 条件
        if not apply_to_all:
            if search_field and search_value:
                if search_condition == '=':
                    values = search_value.split()
                    if len(values) > 1:
                        condition = " OR ".join([f"{search_field} LIKE ?" for _ in values])
                        sql += f" WHERE ({condition})"
                        params.extend([f"%{v}%" for v in values])
                    else:
                        sql += f" WHERE {search_field} LIKE ?"
                        params.append(f"%{search_value}%")
                elif search_condition in ('>', '<'):
                    values = search_value.split()
                    if len(values) == 1:
                        if not search_value.replace('.', '', 1).isdigit():
                            return jsonify({"success": False, "error": "搜索值必须是数值"})
                        sql += f" WHERE {search_field} {search_condition} ?"
                        params.append(float(values[0]))
                    else:
                        if not values[0].replace('.', '', 1).isdigit():
                            return jsonify({"success": False, "error": "第一个搜索值必须是数值"})
                        if not values[1]:
                            return jsonify({"success": False, "error": "第二个搜索值不能为空"})
                        fields = table_info.get('fields', [])
                        primary_key = table_info.get('primary_key', 'user_id')
                        searchable_fields = [f for f in fields if f != primary_key]
                        if searchable_fields:
                            sql += f" WHERE {search_field} {search_condition} ? AND ({' OR '.join([f'{field} LIKE ?' for field in searchable_fields])})"
                            params.extend([float(values[0])] + [f"%{values[1]}%" for field in searchable_fields])
                        else:
                            sql += f" WHERE {search_field} {search_condition} ?"
                            params.append(float(values[0]))
                else:
                    return jsonify({"success": False, "error": "无效的搜索条件"})
            elif search_value:
                # 全字段搜索
                fields = table_info.get('fields', [])
                primary_key = table_info.get('primary_key', 'user_id')
                searchable_fields = [f for f in fields if f != primary_key]
                
                if searchable_fields:
                    conditions = []
                    for field in searchable_fields:
                        conditions.append(f"{field} LIKE ?")
                        params.append(f"%{search_value}%")
                    sql += f" WHERE ({' OR '.join(conditions)})"
                else:
                    return jsonify({"success": False, "error": "没有可搜索的字段"})
        
        # 执行更新
        result = execute_sql(db_path, sql, params)
        
        if 'error' in result:
            return jsonify({"success": False, "error": result['error']})
        
        affected_rows = result.get('affected_rows', 0)
        
        return jsonify({
            "success": True, 
            "message": f"成功更新 {affected_rows} 条记录"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"执行错误：{str(e)}"})

@app.route('/commands')
def commands():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('commands.html', commands=ADMIN_COMMANDS)

@app.route('/execute_command', methods=['POST'])
def execute_command():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    data = request.get_json()
    command_name = data.get('command_name')
    
    if not command_name:
        return jsonify({"success": False, "error": "未指定命令"})
    
    try:
        if command_name == "gm_command":
            # 神秘力量 - 修改灵石
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            
            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                
                # 使用execute_sql更新灵石
                sql = "UPDATE user_xiuxian SET stone = stone + ? WHERE user_id = ?"
                execute_sql(DATABASE, sql, (amount, user_info['user_id']))
                
                return jsonify({
                    "success": True, 
                    "message": f"成功向 {username} {'增加' if amount >= 0 else '减少'} {abs(amount)} 灵石"
                })
            else:
                # 全服发放
                sql = "UPDATE user_xiuxian SET stone = stone + ?"
                execute_sql(DATABASE, sql, (amount,))
                return jsonify({
                    "success": True, 
                    "message": f"全服{'发放' if amount >= 0 else '扣除'} {abs(amount)} 灵石成功"
                })
        
        elif command_name == "adjust_exp_command":
            # 修为调整
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            
            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                
                if amount > 0:
                    sql = "UPDATE user_xiuxian SET exp = exp + ? WHERE user_id = ?"
                    execute_sql(DATABASE, sql, (amount, user_info['user_id']))
                    return jsonify({
                        "success": True, 
                        "message": f"成功向 {username} 增加 {amount} 修为"
                    })
                else:
                    sql = "UPDATE user_xiuxian SET exp = exp - ? WHERE user_id = ?"
                    execute_sql(DATABASE, sql, (abs(amount), user_info['user_id']))
                    return jsonify({
                        "success": True, 
                        "message": f"成功从 {username} 减少 {abs(amount)} 修为"
                    })
            else:
                # 全服调整
                if amount > 0:
                    sql = "UPDATE user_xiuxian SET exp = exp + ?"
                else:
                    sql = "UPDATE user_xiuxian SET exp = exp - ?"
                execute_sql(DATABASE, sql, (abs(amount),))
                return jsonify({
                    "success": True, 
                    "message": f"全服{'增加' if amount >= 0 else '减少'} {abs(amount)} 修为成功"
                })
        
        elif command_name == "gmm_command":
            # 轮回力量 - 修改灵根
            username = data.get('username')
            root_type = data.get('root_type')
            
            if not username:
                return jsonify({"success": False, "error": "请指定用户名"})
            
            user_info = get_user_by_name(username)
            if not user_info:
                return jsonify({"success": False, "error": f"用户 {username} 不存在"})
            
            # 根据root_type设置灵根名称
            root_names = {
                "1": "全属性灵根",
                "2": "融合万物灵根", 
                "3": "月灵根",
                "4": "言灵灵根",
                "5": "金灵根",
                "6": "轮回千次不灭，只为臻至巅峰",
                "7": "轮回万次不灭，只为超越巅峰", 
                "8": "轮回无尽不灭，只为触及永恒之境",
                "9": f"轮回命主·{username}"
            }
            
            root_name = root_names.get(root_type, "未知灵根")
            root_type_name = ROOTS.get(root_type, "混沌灵根")
            
            # 更新灵根
            sql = "UPDATE user_xiuxian SET root = ?, root_type = ? WHERE user_id = ?"
            execute_sql(DATABASE, sql, (root_name, root_type_name, user_info['user_id']))
            
            # 更新战力
            sql_power = "UPDATE user_xiuxian SET power = round(exp * ? * (SELECT spend FROM level_data WHERE level = user_xiuxian.level), 0) WHERE user_id = ?"
            root_rate = get_root_rate(root_type, user_info['user_id'])
            execute_sql(DATABASE, sql_power, (root_rate, user_info['user_id']))
            
            return jsonify({
                "success": True, 
                "message": f"成功将 {username} 的灵根修改为 {root_name}"
            })
        
        elif command_name == "zaohua_xiuxian":
            # 造化力量 - 修改境界
            username = data.get('username')
            level = data.get('level')
            
            if not username:
                return jsonify({"success": False, "error": "请指定用户名"})
            
            user_info = get_user_by_name(username)
            if not user_info:
                return jsonify({"success": False, "error": f"用户 {username} 不存在"})
            
            # 检查境界是否有效
            levels = convert_rank('江湖好手')[1]
            if level not in levels:
                return jsonify({"success": False, "error": f"无效的境界: {level}"})
            
            # 获取境界所需的最大修为
            sql_level = "SELECT power FROM level_data WHERE level = ?"
            level_data = jsondata.level_data()
            if not level_data:
                return jsonify({"success": False, "error": f"无法获取境界 {level} 的数据"})
            
            max_exp = int(level_data[level]['power'])
            
            # 重置用户修为到刚好满足境界要求
            sql = "UPDATE user_xiuxian SET exp = ?, level = ? WHERE user_id = ?"
            execute_sql(DATABASE, sql, (max_exp, level, user_info['user_id']))
            
            # 更新用户状态和战力
            sql_hp = "UPDATE user_xiuxian SET hp = exp / 2, mp = exp, atk = exp / 10 WHERE user_id = ?"
            execute_sql(DATABASE, sql_hp, (user_info['user_id'],))
            
            sql_power = "UPDATE user_xiuxian SET power = round(exp * ? * (SELECT spend FROM level_data WHERE level = ?), 0) WHERE user_id = ?"
            root_rate = get_root_rate(user_info['root_type'], user_info['user_id'])
            execute_sql(DATABASE, sql_power, (root_rate, level, user_info['user_id']))
            
            return jsonify({
                "success": True, 
                "message": f"成功将 {username} 的境界修改为 {level}"
            })
        
        elif command_name == "cz":
            # 创造力量 - 发放物品
            target = data.get('target')
            username = data.get('username')
            item_input = data.get('item')
            amount = int(data.get('amount', 1))
            
            if not item_input:
                return jsonify({"success": False, "error": "请指定物品"})
            
            # 查找物品ID
            goods_id = None
            if item_input.isdigit():
                goods_id = int(item_input)
                # 检查物品是否存在
                # 这里假设 back 表中的 goods_id 是物品的唯一标识，我们需要一个方法来获取物品名称
                # 可以查询 back 表中是否存在该 goods_id
                sql_item = "SELECT goods_name FROM back WHERE goods_id = ? LIMIT 1"
                item_check = execute_sql(DATABASE, sql_item, (goods_id,))
                if not item_check: # 如果back表中不存在，尝试从items中获取
                    item_data = items.get_data_by_item_id(goods_id)
                    if not item_data:
                         return jsonify({"success": False, "error": f"物品ID {goods_id} 不存在于任何物品配置中"})
                    else:
                        goods_name = item_data['name']
                        goods_type = item_data['type']
                else:
                    goods_name = item_check[0]['goods_name']
                    # 从items中获取type，或者从back表中获取
                    item_data = items.get_data_by_item_id(goods_id)
                    goods_type = item_data['type'] if item_data else item_check[0].get('goods_type', '未知类型')
            else:
                # 按名称查找物品
                item_data = items.get_data_by_item_name(item_input)
                if not item_data:
                    return jsonify({"success": False, "error": f"物品 {item_input} 不存在"})
                goods_id = item_data['id']
                goods_name = item_data['name']
                goods_type = item_data['type']
            
            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                
                # 发放物品
                now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                sql_check = "SELECT * FROM back WHERE user_id = ? AND goods_id = ?"
                existing_item = execute_sql(DATABASE, sql_check, (user_info['user_id'], goods_id))
                
                if existing_item:
                    sql_update = "UPDATE back SET goods_num = goods_num + ?, update_time = ? WHERE user_id = ? AND goods_id = ?"
                    execute_sql(DATABASE, sql_update, (amount, now_time, user_info['user_id'], goods_id))
                else:
                    sql_insert = """
                        INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    """
                    execute_sql(DATABASE, sql_insert, (user_info['user_id'], goods_id, goods_name, goods_type, amount, now_time, now_time))
                
                return jsonify({
                    "success": True, 
                    "message": f"成功向 {username} 发放 {goods_name} x{amount}"
                })
            else:
                # 全服发放 - 获取所有用户
                sql_users = "SELECT user_id FROM user_xiuxian"
                all_users = execute_sql(DATABASE, sql_users, ())
                success_count = 0
                
                now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                for user in all_users:
                    try:
                        user_id = user['user_id']
                        sql_check = "SELECT * FROM back WHERE user_id = ? AND goods_id = ?"
                        existing_item = execute_sql(DATABASE, sql_check, (user_id, goods_id))
                        
                        if existing_item:
                            sql_update = "UPDATE back SET goods_num = goods_num + ?, update_time = ? WHERE user_id = ? AND goods_id = ?"
                            execute_sql(DATABASE, sql_update, (amount, now_time, user_id, goods_id))
                        else:
                            sql_insert = """
                                INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num)
                                VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                            """
                            execute_sql(DATABASE, sql_insert, (user_id, goods_id, goods_name, goods_type, amount, now_time, now_time))
                        
                        success_count += 1
                    except Exception as e:
                        continue
                
                return jsonify({
                    "success": True, 
                    "message": f"全服发放 {goods_name} x{amount} 成功，影响 {success_count} 名用户"
                })
        
        elif command_name == "hmll":
            # 毁灭力量 - 扣除物品
            target = data.get('target')
            username = data.get('username')
            item_input = data.get('item')
            amount = int(data.get('amount', 1))
            
            if not item_input:
                return jsonify({"success": False, "error": "请指定物品"})
            
            # 查找物品ID
            goods_id = None
            if item_input.isdigit():
                goods_id = int(item_input)
                # 检查物品是否存在
                sql_item = "SELECT goods_name FROM back WHERE goods_id = ? LIMIT 1"
                item_check = execute_sql(DATABASE, sql_item, (goods_id,))
                if not item_check:
                    return jsonify({"success": False, "error": f"物品ID {goods_id} 不存在"})
            else:
                # 按名称查找物品
                item_data = items.get_data_by_item_name(item_input)
                if not item_data:
                    return jsonify({"success": False, "error": f"物品 {item_input} 不存在"})
                goods_id = item_data['id']

            # 获取物品信息
            sql_item_info = "SELECT goods_name FROM back WHERE goods_id = ? LIMIT 1"
            item_info = execute_sql(DATABASE, sql_item_info, (goods_id,))
            if not item_info:
                return jsonify({"success": False, "error": f"物品ID {goods_id} 不存在于任何用户背包中"})
            goods_name = item_info[0]['goods_name']
            
            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                
                # 检查用户是否有该物品
                sql_check = "SELECT goods_num FROM back WHERE user_id = ? AND goods_id = ?"
                user_item = execute_sql(DATABASE, sql_check, (user_info['user_id'], goods_id))
                
                if not user_item or user_item[0]['goods_num'] < amount:
                    return jsonify({"success": False, "error": f"用户 {username} 没有足够的 {goods_name}"})
                
                # 扣除物品
                sql_update = "UPDATE back SET goods_num = goods_num - ? WHERE user_id = ? AND goods_id = ?"
                execute_sql(DATABASE, sql_update, (amount, user_info['user_id'], goods_id))
                
                # 如果数量为0则删除记录
                sql_clean = "DELETE FROM back WHERE user_id = ? AND goods_id = ? AND goods_num <= 0"
                execute_sql(DATABASE, sql_clean, (user_info['user_id'], goods_id))
                
                return jsonify({
                    "success": True, 
                    "message": f"成功从 {username} 扣除 {goods_name} x{amount}"
                })
            else:
                # 全服扣除
                sql_users = "SELECT user_id FROM user_xiuxian"
                all_users = execute_sql(DATABASE, sql_users, ())
                success_count = 0
                
                for user in all_users:
                    try:
                        user_id = user['user_id']
                        sql_check = "SELECT goods_num FROM back WHERE user_id = ? AND goods_id = ?"
                        user_item = execute_sql(DATABASE, sql_check, (user_id, goods_id))
                        
                        if user_item and user_item[0]['goods_num'] >= amount:
                            sql_update = "UPDATE back SET goods_num = goods_num - ? WHERE user_id = ? AND goods_id = ?"
                            execute_sql(DATABASE, sql_update, (amount, user_id, goods_id))
                            
                            # 清理空记录
                            sql_clean = "DELETE FROM back WHERE user_id = ? AND goods_id = ? AND goods_num <= 0"
                            execute_sql(DATABASE, sql_clean, (user_id, goods_id))
                            
                            success_count += 1
                    except Exception as e:
                        continue
                
                return jsonify({
                    "success": True, 
                    "message": f"全服扣除 {goods_name} x{amount} 成功，影响 {success_count} 名用户"
                })
        
        elif command_name == "ccll_command":
            # 传承力量 - 修改思恋结晶数量
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            
            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                
                # 更新思恋结晶
                sql_check = "SELECT * FROM xiuxian_impart WHERE user_id = ?"
                impart_data = execute_sql(IMPART_DB, sql_check, (user_info['user_id'],))
                
                if impart_data:
                    sql_update = "UPDATE xiuxian_impart SET stone_num = stone_num + ? WHERE user_id = ?"
                    execute_sql(IMPART_DB, sql_update, (amount, user_info['user_id']))
                else:
                    sql_insert = "INSERT INTO xiuxian_impart (user_id, stone_num) VALUES (?, ?)"
                    execute_sql(IMPART_DB, sql_insert, (user_info['user_id'], amount))
                
                return jsonify({
                    "success": True, 
                    "message": f"成功向 {username} {'增加' if amount >= 0 else '减少'} {abs(amount)} 思恋结晶"
                })
            else:
                # 全服调整
                sql_users = "SELECT user_id FROM user_xiuxian"
                all_users = execute_sql(DATABASE, sql_users, ())
                success_count = 0
                
                for user in all_users:
                    try:
                        user_id = user['user_id']
                        sql_check = "SELECT * FROM xiuxian_impart WHERE user_id = ?"
                        impart_data = execute_sql(IMPART_DB, sql_check, (user_id,))
                        
                        if impart_data:
                            sql_update = "UPDATE xiuxian_impart SET stone_num = stone_num + ? WHERE user_id = ?"
                            execute_sql(IMPART_DB, sql_update, (amount, user_id))
                        else:
                            sql_insert = "INSERT INTO xiuxian_impart (user_id, stone_num) VALUES (?, ?)"
                            execute_sql(IMPART_DB, sql_insert, (user_id, amount))
                        
                        success_count += 1
                    except Exception as e:
                        continue
                
                return jsonify({
                    "success": True, 
                    "message": f"全服{'发放' if amount >= 0 else '扣除'} {abs(amount)} 思恋结晶成功，影响 {success_count} 名用户"
                })
        
        else:
            return jsonify({"success": False, "error": f"未知命令: {command_name}"})
    
    except ValueError as e:
        return jsonify({"success": False, "error": f"参数格式错误: {str(e)}"})
    except Exception as e:
        return jsonify({"success": False, "error": f"执行错误: {str(e)}"})

CONFIG_EDITABLE_FIELDS = {
    "put_bot": {
        "name": "接收消息QQ",
        "description": "负责接收消息的QQ号列表，设置这个屏蔽群聊/私聊才能生效",
        "type": "list[str]",
        "category": "基础设置"
    },
    "main_bo": {
        "name": "主QQ",
        "description": "负责发送消息的QQ号列表",
        "type": "list[str]",
        "category": "基础设置"
    },
    "shield_group": {
        "name": "屏蔽群聊",
        "description": "屏蔽的群聊ID列表",
        "type": "list[str]",
        "category": "基础设置"
    },
    "response_group": {
        "name": "反转屏蔽",
        "description": "是否反转屏蔽的群聊（仅响应这些群的消息）",
        "type": "bool",
        "category": "基础设置"
    },
    "shield_private": {
        "name": "屏蔽私聊",
        "description": "是否屏蔽私聊消息",
        "type": "bool",
        "category": "基础设置"
    },
    "admin_debug": {
        "name": "管理员调试模式",
        "description": "开启后只响应超管指令",
        "type": "bool",
        "category": "调试设置"
    },
    "at_response": {
        "name": "艾特响应命令",
        "description": "是否只接收艾特命令（官机请勿打开）",
        "type": "bool",
        "category": "消息设置"
    },
    "at_sender": {
        "name": "消息是否艾特",
        "description": "发送消息是否艾特",
        "type": "bool",
        "category": "消息设置"
    },
    "empty_fallback": {
        "name": "空指令是否回复",
        "description": "空指令回复",
        "type": "bool",
        "category": "消息设置"
    },
    "empty_msg": {
        "name": "空指令回复",
        "description": "回复内容",
        "type": "str",
        "category": "消息设置"
    },
    "img": {
        "name": "图片发送",
        "description": "是否使用图片发送消息",
        "type": "bool",
        "category": "消息设置"
    },
    "user_info_image": {
        "name": "个人信息图片",
        "description": "是否使用图片发送个人信息",
        "type": "bool",
        "category": "消息设置"
    },
    "xiuxian_info_img": {
        "name": "网络背景图",
        "description": "开启则使用网络背景图",
        "type": "bool",
        "category": "消息设置"
    },
    "use_network_avatar": {
        "name": "网络头像",
        "description": "开启则使用网络头像",
        "type": "bool",
        "category": "消息设置"
    },
    "impart_image": {
        "name": "传承卡图",
        "description": "开启则使用发送图片",
        "type": "bool",
        "category": "消息设置"
    },
    "web_port": {
        "name": "管理面板端口",
        "description": "修仙管理面板端口号",
        "type": "int",
        "category": "Web设置"
    },
    "web_host": {
        "name": "管理面板IP",
        "description": "修仙管理面板IP地址",
        "type": "str",
        "category": "Web设置"
    },
    "level_up_cd": {
        "name": "突破CD",
        "description": "突破CD（分钟）",
        "type": "int",
        "category": "修炼设置"
    },
    "closing_exp": {
        "name": "闭关修为",
        "description": "闭关每分钟获取的修为",
        "type": "int",
        "category": "修炼设置"
    },
    "tribulation_min_level": {
        "name": "最低渡劫境界",
        "description": "最低渡劫境界",
        "type": "select",
        "options": LEVELS,
        "category": "渡劫设置"
    },
    "tribulation_base_rate": {
        "name": "基础渡劫概率",
        "description": "基础渡劫概率（百分比）",
        "type": "int",
        "category": "渡劫设置"
    },
    "tribulation_max_rate": {
        "name": "最大渡劫概率",
        "description": "最大渡劫概率（百分比）",
        "type": "int",
        "category": "渡劫设置"
    },
    "tribulation_cd": {
        "name": "渡劫CD",
        "description": "渡劫冷却时间（分钟）",
        "type": "int",
        "category": "渡劫设置"
    },
    "sect_min_level": {
        "name": "创建宗门境界",
        "description": "创建宗门最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "宗门设置"
    },
    "sect_create_cost": {
        "name": "创建宗门消耗",
        "description": "创建宗门消耗灵石",
        "type": "int",
        "category": "宗门设置"
    },
    "sect_rename_cost": {
        "name": "宗门改名消耗",
        "description": "宗门改名消耗灵石",
        "type": "int",
        "category": "宗门设置"
    },
    "sect_rename_cd": {
        "name": "宗门改名CD",
        "description": "宗门改名冷却时间（天）",
        "type": "int",
        "category": "宗门设置"
    },
    "auto_change_sect_owner_cd": {
        "name": "自动换宗主CD",
        "description": "自动换长时间不玩宗主CD（天）",
        "type": "int",
        "category": "宗门设置"
    },
    "closing_exp_upper_limit": {
        "name": "闭关修为上限",
        "description": "闭关获取修为上限倍数",
        "type": "float",
        "category": "修炼设置"
    },
    "level_punishment_floor": {
        "name": "突破失败惩罚下限",
        "description": "突破失败扣除修为惩罚下限（百分比）",
        "type": "int",
        "category": "修炼设置"
    },
    "level_punishment_limit": {
        "name": "突破失败惩罚上限",
        "description": "突破失败扣除修为惩罚上限（百分比）",
        "type": "int",
        "category": "修炼设置"
    },
    "level_up_probability": {
        "name": "失败增加概率",
        "description": "突破失败增加当前境界突破概率的比例",
        "type": "float",
        "category": "修炼设置"
    },
    "max_goods_num": {
        "name": "物品上限",
        "description": "背包单样物品最高上限",
        "type": "int",
        "category": "资源设置"
    },
    "sign_in_lingshi_lower_limit": {
        "name": "签到灵石下限",
        "description": "每日签到灵石下限",
        "type": "int",
        "category": "资源设置"
    },
    "sign_in_lingshi_upper_limit": {
        "name": "签到灵石上限",
        "description": "每日签到灵石上限",
        "type": "int",
        "category": "资源设置"
    },
    "beg_max_level": {
        "name": "奇缘最高境界",
        "description": "仙途奇缘能领灵石最高境界",
        "type": "select",
        "options": LEVELS,
        "category": "资源设置"
    },
    "beg_max_days": {
        "name": "奇缘最多天数",
        "description": "仙途奇缘能领灵石最多天数",
        "type": "int",
        "category": "资源设置"
    },
    "beg_lingshi_lower_limit": {
        "name": "奇缘灵石下限",
        "description": "仙途奇缘灵石下限",
        "type": "int",
        "category": "资源设置"
    },
    "beg_lingshi_upper_limit": {
        "name": "奇缘灵石上限",
        "description": "仙途奇缘灵石上限",
        "type": "int",
        "category": "资源设置"
    },
    "tou": {
        "name": "偷灵石惩罚",
        "description": "偷灵石惩罚金额",
        "type": "int",
        "category": "资源设置"
    },
    "tou_lower_limit": {
        "name": "偷灵石下限",
        "description": "偷灵石下限（百分比）",
        "type": "float",
        "category": "资源设置"
    },
    "tou_upper_limit": {
        "name": "偷灵石上限",
        "description": "偷灵石上限（百分比）",
        "type": "float",
        "category": "资源设置"
    },
    "remake": {
        "name": "重入仙途消费",
        "description": "重入仙途的消费灵石",
        "type": "int",
        "category": "资源设置"
    },
    "remaname": {
        "name": "修仙改名消费",
        "description": "修仙改名的消费灵石",
        "type": "int",
        "category": "资源设置"
    },
    "max_stamina": {
        "name": "体力上限",
        "description": "体力上限值",
        "type": "int",
        "category": "体力设置"
    },
    "stamina_recovery_points": {
        "name": "体力恢复",
        "description": "体力恢复点数/分钟",
        "type": "int",
        "category": "体力设置"
    },
    "lunhui_min_level": {
        "name": "千世轮回境界",
        "description": "千世轮回最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "轮回设置"
    },
    "twolun_min_level": {
        "name": "万世轮回境界",
        "description": "万世轮回最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "轮回设置"
    },
    "threelun_min_level": {
        "name": "永恒轮回境界",
        "description": "永恒轮回最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "轮回设置"
    },
    "Infinite_reincarnation_min_level": {
        "name": "无限轮回境界",
        "description": "无限轮回最低境界",
        "type": "select",
        "options": LEVELS,
        "category": "轮回设置"
    },
    "markdown_status": {
        "name": "markdown模板",
        "description": "是否发送模板信息（野机请勿打开）",
        "type": "bool",
        "category": "MD设置"
    },
    "markdown_id": {
        "name": "模板ID1",
        "description": "用于发送markdown文本",
        "type": "str",
        "category": "MD设置"
    },
    "markdown_id2": {
        "name": "模板ID2",
        "description": "用于发送markdown蓝字",
        "type": "str",
        "category": "MD设置"
    },
    "button_id": {
        "name": "按钮ID1",
        "description": "用于发送修炼按钮",
        "type": "str",
        "category": "MD设置"
    },
    "button_id2": {
        "name": "按钮ID2",
        "description": "用于发送修仙帮助按钮",
        "type": "str",
        "category": "MD设置"
    },
    "gsk_link": {
        "name": "gsk地址",
        "description": "用于发送md模板艾特",
        "type": "str",
        "category": "MD设置"
    },
    "web_link": {
        "name": "修仙管理面板地址",
        "description": "用于发送md图片",
        "type": "str",
        "category": "MD设置"
    },
    "update_image_web": {
        "name": "频道图床上传接口",
        "description": "用于上传图片",
        "type": "str",
        "category": "MD设置"
    },
    "channel_id": {
        "name": "频道图床ID",
        "description": "用于上传图片的频道",
        "type": "str",
        "category": "MD设置"
    },
    "merge_forward_send": {
        "name": "消息发送方式",
        "description": "1=长文本,2=合并转发,3=合并转长图,4=长文本合并转发",
        "type": "int",
        "category": "消息设置"
    },
    "message_optimization": {
        "name": "消息优化",
        "description": "是否开启信息优化",
        "type": "bool",
        "category": "消息设置"
    },
    "img_compression_limit": {
        "name": "图片压缩率",
        "description": "图片压缩率（0-100）",
        "type": "int",
        "category": "消息设置"
    },
    "img_type": {
        "name": "图片类型",
        "description": "webp或者jpeg",
        "type": "str",
        "category": "消息设置"
    },
    "img_send_type": {
        "name": "图片发送类型",
        "description": "io或base64",
        "type": "str",
        "category": "消息设置"
    },
    "cloud_backup_enabled": {
        "name": "开启自动云备份",
        "description": "手动备份或更新插件时，是否自动上传到云端",
        "type": "bool",
        "category": "云备份设置"
    },
    "webdav_url": {
        "name": "WebDAV 服务器地址",
        "description": "例如：http://192.168.1.10:5244/dav",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_user": {
        "name": "WebDAV 账号",
        "description": "云存储的登录用户名",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_pass": {
        "name": "WebDAV 密码",
        "description": "云存储的登录密码或授权码",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_target_subdir": {
        "name": "云端存储根目录",
        "description": "WebDAV 路径下的存放目录，如：backup/xiuxian",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_backup_folder": {
        "name": "备份二级目录",
        "description": "根目录下再套一层的目录名，默认：backups",
        "type": "str",
        "category": "云备份设置"
    },
    "webdav_delete_days": {
        "name": "云端自动清理天数",
        "description": "删除云端多少天之前的旧备份。0 表示永不删除",
        "type": "int",
        "category": "云备份设置"
    }
}

# 排除数据库相关的配置字段
EXCLUDED_CONFIG_FIELDS = [
    'sql_table', 'sql_user_xiuxian', 'sql_user_cd', 'sql_sects', 
    'sql_buff', 'sql_back', 'level', 'version'
]

def get_config_values():
    """获取当前配置值"""
    config = XiuConfig()
    values = {}
    
    for field_name, field_info in CONFIG_EDITABLE_FIELDS.items():
        if hasattr(config, field_name):
            value = getattr(config, field_name)
            values[field_name] = value
    
    return values

def save_config_values(new_values):
    """
    保存配置到文件。
    支持自动格式化布尔值、数字、列表以及包含特殊字符的 WebDAV 字符串。
    """
    config_file_path = Xiu_Plugin / "xiuxian" / "xiuxian_config.py"
    
    if not config_file_path.exists():
        return False, "配置文件不存在"
    
    try:
        # 读取原文件内容
        with open(config_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        for field_name, new_value in new_values.items():
            # 只有在可编辑字段列表中的项才允许处理
            if field_name in CONFIG_EDITABLE_FIELDS:
                field_type = CONFIG_EDITABLE_FIELDS[field_name]["type"]
                
                # --- 1. 布尔类型转换 ---
                if field_type == "bool":
                    # 处理来自 Web 的 'true'/'false' 字符串或 checkbox 的 'on'
                    if str(new_value).lower() in ('true', '1', 'yes', 'on'):
                        formatted_value = "True"
                    else:
                        formatted_value = "False"
                
                # --- 2. 整数列表转换 [1, 2, 3] ---
                elif field_type == "list[int]":
                    if isinstance(new_value, str):
                        # 移除所有非数字和非逗号字符
                        cleaned = re.sub(r'[^0-9,]', '', new_value)
                        items = [i.strip() for i in cleaned.split(',') if i.strip()]
                        formatted_value = f"[{', '.join(items)}]"
                    else:
                        formatted_value = str(new_value)
                
                # --- 3. 字符串列表转换 ["a", "b"] ---
                elif field_type == "list[str]":
                    if isinstance(new_value, str):
                        # 移除外层方括号，按逗号分割，并去除每个元素两端的引号和空格
                        cleaned = new_value.strip().replace('[', '').replace(']', '')
                        items = [i.strip().strip("'").strip('"') for i in cleaned.split(',') if i.strip()]
                        # 统一使用双引号包裹每个元素
                        formatted_value = "[" + ", ".join([f'"{i}"' for i in items]) + "]"
                    else:
                        formatted_value = str(new_value)
                
                # --- 4. 数字类型转换 ---
                elif field_type == "int":
                    try:
                        formatted_value = str(int(new_value))
                    except (ValueError, TypeError):
                        formatted_value = "0"
                
                elif field_type == "float":
                    try:
                        formatted_value = str(float(new_value))
                    except (ValueError, TypeError):
                        formatted_value = "0.0"
                
                # --- 5. 字符串/选择类型 (最关键：处理 URL、路径和密码) ---
                else:
                    # 确保是字符串并去除首尾空格
                    val_str = str(new_value).strip()
                    # 避免重复包裹：如果用户输入的字符串本身带了引号，先去掉
                    if (val_str.startswith('"') and val_str.endswith('"')) or \
                       (val_str.startswith("'") and val_str.endswith("'")):
                        val_str = val_str[1:-1]
                    
                    # 统一使用双引号包裹，这样即使字符串里有单引号（如密码）也不会崩
                    formatted_value = f'"{val_str}"'
                
                # --- 6. 执行正则替换 ---
                # 匹配模式：捕获 self.变量名 = 这一部分，然后替换掉后面直到行尾的内容
                # 能够处理 self.xxx=yyy, self.xxx = yyy, self.xxx   =   yyy 等各种写法
                pattern = rf"(self\.{re.escape(field_name)}\s*=\s*).+"
                # 检查文件中是否存在该配置项
                if re.search(pattern, content):
                    # \1 代表保留第一个捕获组 (即 self.变量名 = )
                        content = re.sub(
                            pattern,
                            lambda m: f"{m.group(1)}{formatted_value}",
                            content
                        )
                else:
                    # 如果配置项在文件中不存在，可能是手动删除了，这里记录日志但不中断
                    from nonebot.log import logger
                    logger.warning(f"[Web管理] 配置项 {field_name} 在 xiuxian_config.py 中未找到匹配行，跳过修改。")
        
        # 写入更新后的内容
        with open(config_file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True, "配置保存成功，重启机器人后生效。"
    
    except Exception as e:
        return False, f"保存配置时出错: {str(e)}"

# 配置管理路由
@app.route('/config')
def config_management():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    
    current_config = get_config_values()
    
    # 预处理列表值用于显示
    for field_name, value in current_config.items():
        if field_name in CONFIG_EDITABLE_FIELDS:
            field_type = CONFIG_EDITABLE_FIELDS[field_name]["type"]
            if field_type in ['list[int]', 'list[str]']:
                # 格式化列表值用于显示
                current_config[field_name] = format_list_value_for_display(value, field_type)
    
    # 按分类分组配置项
    config_by_category = {}
    for field_name, field_info in CONFIG_EDITABLE_FIELDS.items():
        category = field_info["category"]
        if category not in config_by_category:
            config_by_category[category] = []
        
        config_item = {
            "field_name": field_name,
            "name": field_info["name"],
            "description": field_info["description"],
            "type": field_info["type"],
            "value": current_config.get(field_name, "")
        }
        
        if field_info["type"] == "select" and "options" in field_info:
            config_item["options"] = field_info["options"]
        
        config_by_category[category].append(config_item)
    
    return render_template('config.html', config_by_category=config_by_category)

def format_list_value_for_display(value, field_type):
    """格式化列表值用于显示"""
    if not value:
        return ''
    
    try:
        if isinstance(value, str):
            import ast
            value = ast.literal_eval(value)
        
        if isinstance(value, (list, tuple)):
            if field_type == 'list[int]':
                return ', '.join(str(x) for x in value)
            else:
                return ', '.join(str(x).strip('"\'') for x in value)
        else:
            return str(value)
    except (ValueError, SyntaxError):
        # 如果解析失败，返回清理后的值
        cleaned = str(value).replace('[', '').replace(']', '').replace('"', '').replace("'", '')
        return cleaned

@app.route('/save_config', methods=['POST'])
def save_config():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        config_data = request.get_json()
        if not config_data:
            return jsonify({"success": False, "error": "无效的配置数据"})
        
        success, message = save_config_values(config_data)
        return jsonify({"success": success, "message": message})
    
    except Exception as e:
        return jsonify({"success": False, "error": f"保存配置时出错: {str(e)}"})

@app.context_processor
def inject_navigation():
    """注入导航栏状态和辅助函数到所有模板"""
    def is_active(endpoint):
        """检查当前路由是否匹配给定的端点"""
        if isinstance(endpoint, (list, tuple)):
            return request.endpoint in endpoint
        return request.endpoint == endpoint
    
    return dict(
        get_command_icon=get_command_icon,
        get_config_category_icon=get_config_category_icon,
        is_active=is_active
    )

def get_root_rate(root_type, user_id):
    """获取灵根倍率（完整版本，参考原版实现）"""
    # 获取灵根数据
    root_data = jsondata.root_data()
    
    # 特殊处理命运道果
    if root_type == '命运道果':
        # 获取用户信息
        user_info = get_user_by_id(user_id)
        if not user_info:
            return 1.0
            
        root_level = user_info.get('root_level', 0)
        
        # 获取永恒道果和命运道果的倍率
        eternal_rate = root_data['永恒道果']['type_speeds']
        fate_rate = root_data['命运道果']['type_speeds']
        
        # 计算最终倍率：永恒道果倍率 + (轮回等级 × 命运道果倍率)
        return eternal_rate + (root_level * fate_rate)
    else:
        # 普通灵根，直接从数据中获取倍率
        if root_type in root_data:
            return root_data[root_type]['type_speeds']
        else:
            # 如果找不到对应的灵根类型，返回默认值
            return 1.0

def get_command_icon(command_name):
    """获取命令对应的图标"""
    icon_map = {
        "gm_command": "fas fa-gem",
        "adjust_exp_command": "fas fa-fire",
        "gmm_command": "fas fa-recycle",
        "zaohua_xiuxian": "fas fa-mountain",
        "cz": "fas fa-gift",
        "hmll": "fas fa-trash",
        "ccll_command": "fas fa-history"
    }
    return icon_map.get(command_name, "fas fa-cog")

def get_config_category_icon(category):
    """获取配置分类对应的图标"""
    icon_map = {
        "基础设置": "fas fa-cube",
        "MD设置": "fas fa-palette",
        "调试设置": "fas fa-bug",
        "消息设置": "fas fa-comment",
        "Web设置": "fas fa-globe",
        "修炼设置": "fas fa-medal",
        "渡劫设置": "fas fa-bolt",
        "宗门设置": "fas fa-landmark",
        "资源设置": "fas fa-coins",
        "灵根设置": "fas fa-seedling",
        "体力设置": "fas fa-heart",
        "轮回设置": "fas fa-infinity",
        "云备份设置": "fas fa-cloud-upload-alt"
    }
    return icon_map.get(category, "fas fa-cog")


@app.route('/get_stats')
def get_stats():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        # 1. 数据库统计信息
        total_users_result = execute_sql(DATABASE, "SELECT COUNT(*) FROM user_xiuxian")
        total_users = total_users_result[0]['COUNT(*)'] if total_users_result else 0
        
        total_sects_result = execute_sql(DATABASE, "SELECT COUNT(*) FROM sects WHERE sect_owner IS NOT NULL")
        total_sects = total_sects_result[0]['COUNT(*)'] if total_sects_result else 0
        
        today = datetime.now().strftime('%Y-%m-%d')
        active_users_result = execute_sql(DATABASE, 
            "SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE date(create_time) = ?", (today,))
        active_users = active_users_result[0]['COUNT(DISTINCT user_id)'] if active_users_result else 0
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        yesterday_users_result = execute_sql(DATABASE, 
            "SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE date(create_time) = ?", (yesterday,))
        yesterday_users = yesterday_users_result[0]['COUNT(DISTINCT user_id)'] if yesterday_users_result else 0
        
        seven_days_ago = (datetime.now() - timedelta(days=6)).strftime('%Y-%m-%d')
        seven_days_avg_result = execute_sql(DATABASE, 
            "SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE date(create_time) >= ?", (seven_days_ago,))
        seven_days_avg = seven_days_avg_result[0]['COUNT(DISTINCT user_id)'] if seven_days_avg_result else 0

        # 2. 实时机器人 (Bot) 状态获取
        # 通过 NoneBot2 的 get_bots() 跨线程获取实例
        connected_bots = get_bots()
        bot_info_list = []
        
        # 2. 机器人实时状态
        bots = get_bots()
        bot_info_list = []
        for bid, b in bots.items():
            adapter = "未知"
            try: adapter = b.adapter.get_name()
            except: pass
            bot_info_list.append({"bot_id": bid, "adapter": adapter})

        # 3. 获取运行时间 (基于当前进程)
        bot_uptime = "未知"
        if psutil_available:
            try:
                process_create_time = psutil.Process(os.getpid()).create_time()
                bot_uptime = format_time(time.time() - process_create_time)
            except:
                pass

        return jsonify({
            "success": True,
            "total_users": total_users,
            "total_sects": total_sects,
            "active_users": active_users,
            "yesterday_users": yesterday_users,
            "seven_days_avg": seven_days_avg,
            # 消息统计
            "msg_received": msg_stats["received"],
            "msg_sent": msg_stats["sent"],
            # Bot 信息
            "bot_count": len(bot_info_list),
            "bots": bot_info_list,
            "bot_uptime": bot_uptime,
            "nb_version": nb_version
        })
        
    except Exception as e:
        logger.error(f"统计信息获取失败: {e}")
        return jsonify({"success": False, "error": str(e)})

@app.route('/get_system_info_extended')
def get_system_info_extended():
    """获取详细系统信息，对psutil是否可用进行适配"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    # 系统基本信息 (platform模块不依赖psutil，所以始终可用)
    system_info = {
        "平台": platform.platform(),
        "系统": platform.system(),
        "版本": platform.version(),
        "机器": platform.machine(),
        "处理器": platform.processor(),
        "Python版本": platform.python_version(),
    }
    
    # 获取CPU信息
    if psutil_available:
        try:
            cpu_info = {
                "物理核心数": psutil.cpu_count(logical=False),
                "逻辑核心数": psutil.cpu_count(logical=True),
                "CPU使用率": f"{psutil.cpu_percent()}%",
                "CPU频率": f"{psutil.cpu_freq().current:.2f}MHz" if hasattr(psutil, "cpu_freq") and psutil.cpu_freq().current != '未知' else "未知"
            }
        except Exception:
            cpu_info = {"物理核心数": "获取失败", "逻辑核心数": "获取失败",
                        "CPU使用率": "获取失败", "CPU频率": "获取失败"}
    else:
        cpu_info = {"物理核心数": "psutil未安装", "逻辑核心数": "psutil未安装",
                    "CPU使用率": "psutil未安装", "CPU频率": "psutil未安装"}
    
    # 获取内存信息
    if psutil_available:
        try:
            mem = psutil.virtual_memory()
            mem_info = {
                "总内存": f"{mem.total / (1024**3):.2f}GB",
                "已用内存": f"{mem.used / (1024**3):.2f}GB",
                "内存使用率": f"{mem.percent}%"
            }
        except Exception:
            mem_info = {"总内存": "获取失败", "已用内存": "获取失败",
                        "内存使用率": "获取失败"}
    else:
        mem_info = {"总内存": "psutil未安装", "已用内存": "psutil未安装",
                    "内存使用率": "psutil未安装"}
    
    # 获取磁盘信息
    if psutil_available:
        try:
            disk = psutil.disk_usage('/')
            disk_info = {
                "总磁盘空间": f"{disk.total / (1024**3):.2f}GB",
                "已用空间": f"{disk.used / (1024**3):.2f}GB",
                "磁盘使用率": f"{disk.percent}%"
            }
        except Exception:
            disk_info = {"磁盘信息": "获取失败"}
    else:
        disk_info = {"总磁盘空间": "psutil未安装", "已用空间": "psutil未安装",
                     "磁盘使用率": "psutil未安装"}
    
    # 获取系统启动时间
    if psutil_available:
        try:
            boot_time = psutil.boot_time()
            current_time = time.time()
            uptime_seconds = current_time - boot_time
            
            system_uptime_info = {
                "系统启动时间": f"{datetime.fromtimestamp(boot_time):%Y-%m-%d %H:%M:%S}",
                "系统运行时间": format_time(uptime_seconds)
            }
        except Exception:
            system_uptime_info = {"系统启动时间": "获取失败", "系统运行时间": "获取失败"}
    else:
        system_uptime_info = {"系统启动时间": "psutil未安装", "系统运行时间": "psutil未安装"}

    return jsonify({
        "success": True,
        "system_info": system_info,
        "cpu_info": cpu_info,
        "mem_info": mem_info,
        "disk_info": disk_info,
        "system_uptime": system_uptime_info
    })
        
@app.route('/get_process_info')
def get_process_info():
    """获取进程信息，对psutil是否可用进行适配"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    if not psutil_available:
        return jsonify({
            "success": False, 
            "error": "psutil未安装，无法获取进程信息",
            "processes": []
        })

    try:
        processes = []
        for proc in psutil.process_iter(['pid', 'name', 'memory_percent', 'create_time']):
            try:
                memory_mb = proc.memory_info().rss / 1024 / 1024
                create_time = datetime.fromtimestamp(proc.create_time())
                run_time = datetime.now() - create_time
                
                processes.append({
                    "name": proc.name(),
                    "memory": f"{memory_mb:.1f}MB",
                    "time": str(run_time).split('.')[0]  # 去除毫秒部分
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        # 按内存使用排序并取前5
        processes.sort(key=lambda x: float(x['memory'].replace('MB', '')), reverse=True)
        top_processes = processes[:5]
        
        return jsonify({
            "success": True,
            "processes": top_processes
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"获取进程信息失败: {str(e)}"})

def format_time(seconds: float) -> str:
    """将秒数格式化为 'X天X小时X分X秒'"""
    if seconds <= 0: # 适配psutil占位符可能导致的0秒
        return "未知"
    days, remainder = divmod(seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(days)}天{int(hours)}小时{int(minutes)}分{int(seconds)}秒"

@app.route('/search_users')
def search_users():
    if 'admin_id' not in session:
        return jsonify([])
    
    query = request.args.get('query', '')
    sql = "SELECT user_id, user_name FROM user_xiuxian WHERE user_name LIKE ? LIMIT 10"
    results = execute_sql(DATABASE, sql, (f"%{query}%",))
    
    return jsonify([{"id": r['user_id'], "name": r['user_name']} for r in results])

@app.route('/download/<path:filepath>')
def download_file(filepath):
    # 构建文件的完整路径
    full_path = Path() / "data" / "xiuxian" / "cache" / filepath
    full_path = full_path.absolute()
    # 检查文件是否存在
    if not full_path.exists():
        abort(404)  # 文件不存在，返回404错误
    
    # 检查文件是否在允许的目录下，防止目录遍历攻击
    if not full_path.is_relative_to(Path().absolute()):
        abort(403)  # 文件不在允许的目录下，返回403错误
    
    # 发送文件
    return send_file(str(full_path))

# 全局存储终端会话：admin_id -> {'fd': master_fd, 'pid': child_pid}
terminal_sessions = {}

def get_terminal_session(admin_id):
    """获取或创建一个持久的 bash 会话"""
    if admin_id in terminal_sessions:
        # 检查进程是否还在运行
        pid = terminal_sessions[admin_id]['pid']
        try:
            os.kill(pid, 0)
            return terminal_sessions[admin_id]
        except OSError:
            # 进程已死，清理
            try: os.close(terminal_sessions[admin_id]['fd'])
            except: pass
            del terminal_sessions[admin_id]

    # 创建新的伪终端对
    master_fd, slave_fd = pty.openpty()
    
    # 启动 bash 子进程
    pid = os.fork()

    if pid == 0:  # 子进程
        os.setsid()
        os.dup2(slave_fd, 0)
        os.dup2(slave_fd, 1)
        os.dup2(slave_fd, 2)
        os.close(master_fd)
        
        env = os.environ.copy()
        env["TERM"] = "xterm-256color"
        env["LANG"] = "zh_CN.UTF-8"
        
        # 这里的 PS1 控制【上面终端显示区】的样式
        # \[\033[01;32m\] 为绿色 (用户)
        # \[\033[01;34m\] 为蓝色 (路径)
        # 这里的设置会通过 ansi_up 插件在网页上渲染出颜色
        env["PS1"] = "\[\033[01;32m\]\\u\[\033[00m\]:\[\033[01;34m\]\\w\[\033[00m\]\\$ "
        
        os.execve("/bin/bash", ["/bin/bash", "--login", "-i"], env)
    
    # 父进程
    os.close(slave_fd)
    
    # 将 master_fd 设置为非阻塞模式，防止读取时卡死整个 Flask
    fl = fcntl.fcntl(master_fd, fcntl.F_GETFL)
    fcntl.fcntl(master_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    
    session_data = {'fd': master_fd, 'pid': pid}
    terminal_sessions[admin_id] = session_data
    return session_data

@app.route('/terminal')
def terminal():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('terminal.html', admin_id=session['admin_id'])

@app.route('/terminal/output')
def terminal_output():
    """流式读取终端输出的 Generator"""
    if 'admin_id' not in session:
        return "Unauthorized", 401
    
    admin_id = session['admin_id']
    term = get_terminal_session(admin_id)

    def generate():
        fd = term['fd']
        while True:
            # 使用 select 监听文件描述符是否有数据可读
            r, _, _ = select.select([fd], [], [], 0.5)
            if r:
                try:
                    data = os.read(fd, 1024 * 16)
                    if not data: break
                    yield data.decode('utf-8', errors='replace')
                except (OSError, Exception):
                    break
            # 检查进程是否还存活
            try:
                os.kill(term['pid'], 0)
            except OSError:
                yield "\n[Session Terminated]\n"
                break
    
    return Response(generate(), mimetype='text/plain')

@app.route('/terminal/write', methods=['POST'])
def terminal_write():
    """向终端写入数据（支持多字符组合键）"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "Not logged in"})
    
    admin_id = session['admin_id']
    data = request.get_json()
    input_str = data.get('input', '')
    
    term = get_terminal_session(admin_id)
    try:
        os.write(term['fd'], input_str.encode('utf-8'))
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/terminal/pwd')
def terminal_pwd():
    if 'admin_id' not in session: return jsonify({"cwd": "/"})
    admin_id = session['admin_id']
    if admin_id in terminal_sessions:
        pid = terminal_sessions[admin_id]['pid']
        try:
            cwd = os.readlink(f"/proc/{pid}/cwd")
            return jsonify({"cwd": cwd})
        except: pass
    return jsonify({"cwd": "~"})

def run_async(coro):
    """在同步环境执行协程，兼容已有事件循环/无事件循环两种情况"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # 当前线程无事件循环
        return asyncio.run(coro)
    else:
        # 当前线程已有事件循环：新建线程跑，避免 'This event loop is already running'
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

@app.route('/upload_image', methods=['POST'])
def upload_api_image():
    """
    供外部/其他插件调用的图片上传接口
    """
    # 安全检查：仅允许本地调用或已登录管理员
    if 'admin_id' not in session and request.remote_addr != '127.0.0.1':
        return jsonify({"success": False, "error": "Unauthorized"}), 403

    channel_id = request.form.get('channel_id')
    file = request.files.get('image')
    
    if not file or not channel_id:
        return jsonify({"success": False, "error": "缺少参数 image 或 channel_id"}), 400

    image_bytes = file.read()

    # 获取在线的 QQBot 实例
    bots = get_bots()
    target_bot = None
    for b in bots.values():
        if b.adapter.get_name() == "QQ":
            target_bot = b
            break
    
    if not target_bot:
        return jsonify({"success": False, "error": "未找到在线的 QQBot 实例"}), 500

    try:
        url = run_async(
            MessageSegment.upload_image_and_get_url(
                bot=target_bot,
                channel_id=str(channel_id),
                image=image_bytes,
                mode="md5"
            )
        )
        
        if url:
            return jsonify({"success": True, "url": url})
        else:
            return jsonify({"success": False, "error": "上传失败，无法生成URL"})
            
    except Exception as e:
        logger.error(f"接口上传图片异常: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/logs')
def logs():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('logs.html')


def _get_log_candidates():
    """
    日志候选：
    1) 根目录下 *.log
    2) logs/ 目录下所有文件
    """
    root = Path()
    files = []

    # 根目录 *.log
    files.extend([p for p in root.glob("*.log") if p.is_file()])

    # logs 目录
    logs_dir = root / "logs"
    if logs_dir.exists() and logs_dir.is_dir():
        files.extend([p for p in logs_dir.glob("*") if p.is_file()])

    # 去重 + 按修改时间倒序
    uniq = {str(p.resolve()): p for p in files}
    result = list(uniq.values())
    result.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return result


@app.route('/api/logs/files')
def api_logs_files():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        files = _get_log_candidates()
        data = []
        for p in files:
            st = p.stat()
            data.append({
                "name": p.name,
                "path": str(p.resolve()),
                "size": st.st_size,
                "mtime": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            })
        return jsonify({"success": True, "files": data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

def _parse_dt_flexible(s: str):
    """
    宽松时间解析：
    支持
    - 2026-03-03 06:35:00
    - 2026-03-03 06:35
    - 2026-03-03
    - 2026-03-03T06:35
    - 2026-03-03T06:35:00
    """
    if not s:
        return None
    s = str(s).strip().replace("T", " ")
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"time data '{s}' does not match supported formats")


def _strip_ansi_for_parse(line: str):
    """
    去掉 ANSI 控制符，便于做时间/级别解析。
    同时兼容“丢了ESC，只剩[31m”这种情况。
    """
    if not line:
        return ""
    # 标准 ANSI: \x1b[31m
    line = re.sub(r'\x1b\[[0-9;]*m', '', line)
    # 残缺 ANSI: [31m / [1;31m
    line = re.sub(r'\[[0-9;]*m', '', line)
    return line


def _parse_level(line: str):
    levels = ["TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL"]
    up = (line or "").upper()
    for lv in levels:
        if lv in up:
            return lv
    return "UNKNOWN"


def _parse_line_time(line: str):
    """
    从日志行提取时间，兼容：
    1) YYYY-mm-dd HH:MM:SS
    2) mm-dd HH:MM:SS (自动补当前年)
    """
    clean = _strip_ansi_for_parse(line)

    m1 = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', clean)
    if m1:
        try:
            return datetime.strptime(m1.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    m2 = re.search(r'(\d{2}-\d{2} \d{2}:\d{2}:\d{2})', clean)
    if m2:
        try:
            now_year = datetime.now().year
            return datetime.strptime(f"{now_year}-{m2.group(1)}", "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    return None


@app.route('/api/logs/read')
def api_logs_read():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    file_name = request.args.get("file", "").strip()
    keyword = request.args.get("keyword", "").strip()
    level = request.args.get("level", "ALL").strip().upper()

    start_dt = request.args.get("start", "").strip()
    end_dt = request.args.get("end", "").strip()

    try:
        page = int(request.args.get("page", 1))
    except Exception:
        page = 1
    try:
        page_size = int(request.args.get("page_size", 200))
    except Exception:
        page_size = 200

    page = max(page, 1)
    page_size = min(max(page_size, 50), 1000)

    try:
        # 仅允许候选日志
        candidates = _get_log_candidates()
        file_map = {p.name: p for p in candidates}
        if file_name not in file_map:
            return jsonify({"success": False, "error": "日志文件不存在或不允许访问"})

        target = file_map[file_name]

        # 宽松解析起止时间（修复你报错的核心）
        start_obj = _parse_dt_flexible(start_dt) if start_dt else None
        end_obj = _parse_dt_flexible(end_dt) if end_dt else None

        # 若只填日期（00:00:00），通常希望结束时间覆盖整天，这里可选扩展：
        # 如果 end_dt 只有日期，自动放到 23:59:59
        if end_dt and re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_dt):
            end_obj = end_obj.replace(hour=23, minute=59, second=59)

        matched = []
        with open(target, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                raw = line.rstrip("\n")
                clean_for_match = _strip_ansi_for_parse(raw)

                # 关键字（对清洗后的文本匹配，更稳定）
                if keyword and keyword not in clean_for_match:
                    continue

                lv = _parse_level(clean_for_match)
                if level and level != "ALL" and lv != level:
                    continue

                t = _parse_line_time(raw)
                if start_obj and t and t < start_obj:
                    continue
                if end_obj and t and t > end_obj:
                    continue

                matched.append({
                    "time": t.strftime("%Y-%m-%d %H:%M:%S") if t else "",
                    "level": lv,
                    "text": raw  # 保留原始行，前端可用 ansi_up 上色
                })

        total = len(matched)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        rows = matched[start_idx:end_idx] if start_idx < total else []

        return jsonify({
            "success": True,
            "file": file_name,
            "total": total,
            "page": page,
            "page_size": page_size,
            "rows": rows
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"读取失败：{str(e)}"})

@app.route('/api/logs/tail')
def api_logs_tail():
    """
    增量读取日志（按字节 offset）：
    参数：
      file               日志文件名
      offset             上次读取位置（字节）
      keyword            包含关键字（可空）
      level              日志级别过滤，ALL 表示不过滤
      start/end          时间过滤（可空）
      ignore_unknown     1/0，是否忽略 UNKNOWN
      ignore_keywords    使用 | 分隔的忽略关键字（可空）
    返回：
      {
        success, file, offset, next_offset,
        lines: [{time, level, text}, ...]
      }
    """
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    file_name = request.args.get("file", "").strip()

    try:
        offset = int(request.args.get("offset", 0))
    except Exception:
        offset = 0

    keyword = request.args.get("keyword", "").strip()
    level = request.args.get("level", "ALL").strip().upper()
    start_dt = request.args.get("start", "").strip()
    end_dt = request.args.get("end", "").strip()

    # 新增：忽略项
    ignore_unknown = request.args.get("ignore_unknown", "0") == "1"
    ignore_keywords_raw = request.args.get("ignore_keywords", "").strip()
    ignore_keywords = [x.strip() for x in ignore_keywords_raw.split("|") if x.strip()]

    try:
        candidates = _get_log_candidates()
        file_map = {p.name: p for p in candidates}
        if file_name not in file_map:
            return jsonify({"success": False, "error": "日志文件不存在或不允许访问"})

        target = file_map[file_name]
        file_size = target.stat().st_size

        # 日志轮转/截断处理：offset 越界则回到 0
        if offset < 0 or offset > file_size:
            offset = 0

        # 时间解析
        start_obj = _parse_dt_flexible(start_dt) if start_dt else None
        end_obj = _parse_dt_flexible(end_dt) if end_dt else None
        if end_dt and re.fullmatch(r"\d{4}-\d{2}-\d{2}", end_dt):
            end_obj = end_obj.replace(hour=23, minute=59, second=59)

        lines = []
        next_offset = offset

        import codecs
        decoder = codecs.getincrementaldecoder('utf-8')('replace')

        with open(target, "rb") as f:
            f.seek(offset)
            chunk = f.read()
            next_offset = f.tell()

        text = decoder.decode(chunk, final=True)
        raw_lines = text.splitlines()

        for raw in raw_lines:
            clean_for_match = _strip_ansi_for_parse(raw)

            # 正向关键字（包含）
            if keyword and keyword not in clean_for_match:
                continue

            lv = _parse_level(clean_for_match)

            # 级别过滤
            if level and level != "ALL" and lv != level:
                continue

            # 忽略 UNKNOWN
            if ignore_unknown and lv == "UNKNOWN":
                continue

            # 忽略关键字（命中任意一个就忽略）
            if ignore_keywords and any(k in raw for k in ignore_keywords):
                continue

            # 时间过滤
            t = _parse_line_time(raw)
            if start_obj and t and t < start_obj:
                continue
            if end_obj and t and t > end_obj:
                continue

            lines.append({
                "time": t.strftime("%Y-%m-%d %H:%M:%S") if t else "",
                "level": lv,
                "text": raw
            })

        return jsonify({
            "success": True,
            "file": file_name,
            "offset": offset,
            "next_offset": next_offset,
            "lines": lines
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"tail失败：{str(e)}"})

def run_flask():
    app.run(host=HOST, port=PORT, debug=False)

if XiuConfig().web_status:
    # 创建并启动线程
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True  # 设置为守护线程，主程序退出时会自动结束
    flask_thread.start()
    logger.info(f"修仙管理面板已启动：{HOST}:{PORT}")