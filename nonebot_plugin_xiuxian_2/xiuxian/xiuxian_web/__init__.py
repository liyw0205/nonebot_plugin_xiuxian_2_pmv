import sqlite3
import os
import json
import re
import platform
import psutil
import time
from pathlib import Path
from nonebot.log import logger
from datetime import datetime
from nonebot import get_driver
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file
from ..xiuxian_utils.item_json import Items
from ..xiuxian_config import XiuConfig, Xiu_Plugin, convert_rank
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.download_xiuxian_data import UpdateManager
from ..xiuxian_utils.xiuxian2_handle import config_impart

items = Items()
update_manager = UpdateManager()
app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # 用于会话加密

# 配置
DATABASE = Path() / "data" / "xiuxian" / "xiuxian.db"
IMPART_DB = Path() / "data" / "xiuxian" / "xiuxian_impart.db"
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
    """从预设配置类获取表结构信息"""
    tables = {
        "主数据库": {
            "path": DATABASE,
            "tables": get_config_table_structure(XiuConfig())
        },
        "虚神界数据库": {
            "path": IMPART_DB,
            "tables": get_impart_table_structure(config_impart)
        }
    }
    return tables

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
    conn = sqlite3.connect(db_path)
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
        conn.close()

def get_table_data(db_path, table_name, page=1, per_page=10, search_field=None, search_value=None):
    """获取表数据（分页和搜索）"""
    offset = (page - 1) * per_page
    
    # 获取表信息以确定主键
    tables = get_database_tables(db_path)
    table_info = tables.get(table_name, {})
    primary_key = table_info.get('primary_key', 'id')
    
    # 基础查询
    sql = f"SELECT * FROM {table_name}"
    params = []
    
    # 添加搜索条件 - 支持多值搜索
    if search_field and search_value:
        # 单字段搜索逻辑（保持不变）
        values = search_value.split()
        if len(values) > 1:
            placeholders = " OR ".join([f"{search_field} LIKE ?" for _ in values])
            sql += f" WHERE ({placeholders})"
            params.extend([f"%{value}%" for value in values])
        else:
            sql += f" WHERE {search_field} LIKE ?"
            params.append(f"%{search_value}%")
    
    elif search_value:  # 全字段搜索
        # 获取所有字段
        tables = get_database_tables(db_path)
        table_fields = tables.get(table_name, {}).get('fields', [])
        
        if table_fields:
            conditions = []
            search_params = []
            
            # 对每个字段添加LIKE条件
            for field in table_fields:
                # 排除主键字段（可选）
                if field != tables[table_name].get('primary_key'):
                    conditions.append(f"{field} LIKE ?")
                    search_params.append(f"%{search_value}%")
            
            # 只有当有搜索条件时才添加WHERE子句
            if conditions:
                sql += f" WHERE ({' OR '.join(conditions)})"
                params.extend(search_params)
            else:
                # 如果没有可搜索的字段，返回空结果
                sql += " WHERE 1=0"  # 确保不返回任何结果
    
    # 添加分页
    sql += f" LIMIT ? OFFSET ?"
    params.extend([per_page, offset])
    
    # 执行查询
    data = execute_sql(db_path, sql, params)
    
    # 获取总数（需要相应修改）
    count_sql = f"SELECT COUNT(*) FROM {table_name}"
    count_params = []
    
    if search_field and search_value:
        # 单字段搜索的计数逻辑
        values = search_value.split()
        if len(values) > 1:
            placeholders = " OR ".join([f"{search_field} LIKE ?" for _ in values])
            count_sql += f" WHERE ({placeholders})"
            count_params = [f"%{value}%" for value in values]
        else:
            count_sql += f" WHERE {search_field} LIKE ?"
            count_params = [f"%{search_value}%"]
    
    elif search_value:
        # 全字段搜索的计数逻辑
        tables = get_database_tables(db_path)
        table_fields = tables.get(table_name, {}).get('fields', [])
        
        if table_fields:
            conditions = []
            for field in table_fields:
                if field != tables[table_name].get('primary_key'):
                    conditions.append(f"{field} LIKE ?")
                    count_params.append(f"%{search_value}%")
            
            if conditions:
                count_sql += f" WHERE ({' OR '.join(conditions)})"
            else:
                count_sql += " WHERE 1=0"
    
    total_result = execute_sql(db_path, count_sql, count_params)
    total = total_result[0]['COUNT(*)'] if total_result else 0
    
    return {
        "data": data,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page
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
        backup_dir = Path() / "data" / "config_backups"
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
        backup_dir = Path() / "data" / "config_backups"
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
        
        backup_path = Path() / "data" / "config_backups" / backup_filename
        
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
    
    backup_path = Path() / "data" / "backups" / filename
    
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
        
        backup_path = Path() / "data" / "backups" / backup_filename
        
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

        backup_path = Path() / "data" / "config_backups" / backup_filename
        
        if not backup_path.exists():
            return jsonify({"success": False, "error": f"备份文件不存在: {backup_filename}"})
        
        # 删除文件
        backup_path.unlink()
        
        logger.info(f"配置备份文件已删除: {backup_filename}")
        return jsonify({"success": True, "message": f"配置备份文件删除成功: {backup_filename}"})
        
    except Exception as e:
        logger.error(f"删除配置备份失败: {str(e)}")
        return jsonify({"success": False, "error": f"删除配置备份失败: {str(e)}"})

@app.route('/database')
def database():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    all_tables = get_tables()
    return render_template('database.html', tables=all_tables)

@app.route('/table/<table_name>')
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
        return "表不存在", 404
    
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    search_field = request.args.get('search_field')
    search_value = request.args.get('search_value')
    
    table_data = get_table_data(
        db_path, table_name, 
        page=page, per_page=per_page,
        search_field=search_field, search_value=search_value
    )
    
    return render_template(
        'table_view.html',
        table_name=table_name,
        table_info=table_info,
        data=table_data,
        search_field=search_field,
        search_value=search_value,
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
    
    for db_name, db_info in all_tables_grouped.items():
        if table_name in db_info["tables"]:
            db_path = db_info["path"]
            table_info = db_info["tables"][table_name]
            break
    
    if not db_path:
        return "表不存在", 404
    
    # 特殊处理复合主键表
    if table_name == "impart_cards":
        # 解析复合主键（格式：user_id_card_name）
        key_parts = row_id.split('_')
        if len(key_parts) < 2:
            return "无效的主键格式", 400
            
        # 构建复合主键条件
        primary_conditions = {
            "user_id": key_parts[0],
            "card_name": "_".join(key_parts[1:])
        }
    elif table_name == "back" and "composite_key" in table_info and table_info["composite_key"]:
        # 其他复合主键表的处理
        primary_keys = table_info["primary_key"]
        key_parts = row_id.split('_')
        if len(key_parts) != len(primary_keys):
            return "无效的主键格式", 400
            
        primary_conditions = {}
        for i, key in enumerate(primary_keys):
            primary_conditions[key] = key_parts[i]
    else:
        # 普通单主键处理
        primary_key = table_info.get('primary_key', 'id')
        primary_conditions = {primary_key: row_id}
    
    # 确定数据库路径
    db_path = IMPART_DB if table_name in get_database_tables(IMPART_DB) else DATABASE
    
    if request.method == 'POST':
        # 处理更新或删除
        action = request.form.get('action')
        
        if action == 'update':
            # 获取表单数据并进行空值转换
            update_data = {}
            for field in table_info['fields']:
                if field in request.form:
                    value = request.form[field]
                    # 将空字符串转换为None（NULL）
                    if value == '':
                        update_data[field] = None
                    else:
                        update_data[field] = value
            
            # 构建UPDATE语句
            set_clause = ", ".join([f"{field} = ?" for field in update_data.keys()])
            
            # 构建WHERE条件（支持复合主键）
            where_conditions = " AND ".join([f"{key} = ?" for key in primary_conditions.keys()])
            
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_conditions}"
            
            # 执行更新
            params = list(update_data.values()) + list(primary_conditions.values())
            result = execute_sql(db_path, sql, params)
            
            if 'error' in result:
                return jsonify({"success": False, "error": result['error']})
            
            return jsonify({"success": True, "message": "更新成功"})
        
        elif action == 'delete':
            # 构建DELETE语句（支持复合主键）
            where_conditions = " AND ".join([f"{key} = ?" for key in primary_conditions.keys()])
            sql = f"DELETE FROM {table_name} WHERE {where_conditions}"
            result = execute_sql(db_path, sql, list(primary_conditions.values()))
            
            if 'error' in result:
                return jsonify({"success": False, "error": result['error']})
            
            return jsonify({"success": True, "message": "删除成功"})
    
    # GET请求，获取行数据
    where_conditions = " AND ".join([f"{key} = ?" for key in primary_conditions.keys()])
    sql = f"SELECT * FROM {table_name} WHERE {where_conditions}"
    row_data = execute_sql(db_path, sql, list(primary_conditions.values()))
    
    if not row_data:
        return "记录不存在", 404

    display_data = {}
    for key, value in row_data[0].items():
        if value is None:
            display_data[key] = ''
        else:
            display_data[key] = value
    
    return render_template(
        'row_edit.html',
        table_name=table_name,
        table_info=table_info,
        row_data=display_data,
        primary_key=primary_conditions  # 传递主键信息给模板
    )

@app.route('/batch_edit/<table_name>', methods=['POST'])
def batch_edit(table_name):
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    # 获取表单数据
    search_field = request.form.get('search_field')
    search_value = request.form.get('search_value')
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
    
    # 确定数据库路径
    db_path = IMPART_DB if table_name in get_database_tables(IMPART_DB) else DATABASE
    
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
        
        # 添加WHERE条件
        if not apply_to_all:
            if search_field and search_value:  # 指定字段搜索
                values = search_value.split()
                if len(values) > 1:
                    condition = " OR ".join([f"{search_field} LIKE ?" for _ in values])
                    sql += f" WHERE ({condition})"
                    params.extend([f"%{v}%" for v in values])
                else:
                    sql += f" WHERE {search_field} LIKE ?"
                    params.append(f"%{search_value}%")
            elif search_value:  # 全字段搜索
                # 获取所有字段
                tables = get_database_tables(db_path)
                table_fields = tables.get(table_name, {}).get('fields', [])
            
                if table_fields:
                    conditions = []
                    for field in table_fields:
                        if field != tables[table_name].get('primary_key'):
                            conditions.append(f"{field} LIKE ?")
                            params.append(f"%{search_value}%")
                    
                    # 确保有搜索条件时才添加WHERE
                    if conditions:
                        sql += f" WHERE ({' OR '.join(conditions)})"
                    else:
                        # 如果没有可搜索的字段，不执行任何操作
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
        return jsonify({"success": False, "error": f"执行错误: {str(e)}"})

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
                sql_item = "SELECT * FROM back WHERE goods_id = ? LIMIT 1"
                item_check = execute_sql(DATABASE, sql_item, (goods_id,))
                if not item_check:
                    return jsonify({"success": False, "error": f"物品ID {goods_id} 不存在"})
            else:
                # 按名称查找物品
                sql_item = "SELECT goods_id FROM back WHERE goods_name = ? LIMIT 1"
                item_check = execute_sql(DATABASE, sql_item, (item_input,))
                if not item_check:
                    return jsonify({"success": False, "error": f"物品 {item_input} 不存在"})
                goods_id = item_check[0]['goods_id']
            
            # 获取物品信息
            sql_item_info = "SELECT goods_name, goods_type FROM back WHERE goods_id = ? LIMIT 1"
            item_info = execute_sql(DATABASE, sql_item_info, (goods_id,))[0]
            goods_name = item_info['goods_name']
            goods_type = item_info['goods_type']
            
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
                sql_item = "SELECT * FROM back WHERE goods_id = ? LIMIT 1"
                item_check = execute_sql(DATABASE, sql_item, (goods_id,))
                if not item_check:
                    return jsonify({"success": False, "error": f"物品ID {goods_id} 不存在"})
            else:
                # 按名称查找物品
                sql_item = "SELECT goods_id FROM back WHERE goods_name = ? LIMIT 1"
                item_check = execute_sql(DATABASE, sql_item, (item_input,))
                if not item_check:
                    return jsonify({"success": False, "error": f"物品 {item_input} 不存在"})
                goods_id = item_check[0]['goods_id']
            
            # 获取物品信息
            sql_item_info = "SELECT goods_name FROM back WHERE goods_id = ? LIMIT 1"
            item_info = execute_sql(DATABASE, sql_item_info, (goods_id,))[0]
            goods_name = item_info['goods_name']
            
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
        "description": "负责接收消息的QQ号列表",
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
        "category": "群聊设置"
    },
    "response_group": {
        "name": "反转屏蔽",
        "description": "是否反转屏蔽的群聊（仅响应这些群的消息）",
        "type": "bool",
        "category": "群聊设置"
    },
    "shield_private": {
        "name": "屏蔽私聊",
        "description": "是否屏蔽私聊消息",
        "type": "bool",
        "category": "私聊设置"
    },
    "admin_debug": {
        "name": "管理员调试模式",
        "description": "开启后只响应超管指令",
        "type": "bool",
        "category": "调试设置"
    },
    "at_response": {
        "name": "艾特响应命令",
        "description": "是否只接收艾特命令",
        "type": "bool",
        "category": "消息设置"
    },
    "at_sender": {
        "name": "消息是否艾特",
        "description": "发送消息是否艾特",
        "type": "bool",
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
    "private_chat_enabled": {
        "name": "私聊功能",
        "description": "私聊功能开关",
        "type": "bool",
        "category": "私聊设置"
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
    "sign_in_lingshi_lower_limit": {
        "name": "签到灵石下限",
        "description": "每日签到灵石下限",
        "type": "int",
        "category": "经济设置"
    },
    "sign_in_lingshi_upper_limit": {
        "name": "签到灵石上限",
        "description": "每日签到灵石上限",
        "type": "int",
        "category": "经济设置"
    },
    "beg_max_level": {
        "name": "奇缘最高境界",
        "description": "仙途奇缘能领灵石最高境界",
        "type": "select",
        "options": LEVELS,
        "category": "经济设置"
    },
    "beg_max_days": {
        "name": "奇缘最多天数",
        "description": "仙途奇缘能领灵石最多天数",
        "type": "int",
        "category": "经济设置"
    },
    "beg_lingshi_lower_limit": {
        "name": "奇缘灵石下限",
        "description": "仙途奇缘灵石下限",
        "type": "int",
        "category": "经济设置"
    },
    "beg_lingshi_upper_limit": {
        "name": "奇缘灵石上限",
        "description": "仙途奇缘灵石上限",
        "type": "int",
        "category": "经济设置"
    },
    "tou": {
        "name": "偷灵石惩罚",
        "description": "偷灵石惩罚金额",
        "type": "int",
        "category": "经济设置"
    },
    "tou_lower_limit": {
        "name": "偷灵石下限",
        "description": "偷灵石下限（百分比）",
        "type": "float",
        "category": "经济设置"
    },
    "tou_upper_limit": {
        "name": "偷灵石上限",
        "description": "偷灵石上限（百分比）",
        "type": "float",
        "category": "经济设置"
    },
    "auto_select_root": {
        "name": "自动选择灵根",
        "description": "默认开启自动选择最佳灵根",
        "type": "bool",
        "category": "灵根设置"
    },
    "remake": {
        "name": "重入仙途消费",
        "description": "重入仙途的消费灵石",
        "type": "int",
        "category": "经济设置"
    },
    "remaname": {
        "name": "修仙改名消费",
        "description": "修仙改名的消费灵石",
        "type": "int",
        "category": "经济设置"
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
    "merge_forward_send": {
        "name": "消息发送方式",
        "description": "1=长文本,2=合并转发,3=合并转长图",
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
    """保存配置到文件"""
    config_file_path = Xiu_Plugin / "xiuxian" / "xiuxian_config.py"
    
    if not config_file_path.exists():
        return False, "配置文件不存在"
    
    try:
        # 读取原文件内容
        with open(config_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 更新配置值
        for field_name, new_value in new_values.items():
            if field_name in CONFIG_EDITABLE_FIELDS:
                field_type = CONFIG_EDITABLE_FIELDS[field_name]["type"]
                
                # 根据类型格式化值
                if field_type == "list[int]":
                    # 清理输入：移除所有非数字字符（除了逗号和空格）
                    if isinstance(new_value, str):
                        # 移除方括号、引号等字符
                        cleaned_value = re.sub(r'[\[\]\'"\s]', '', new_value)
                        if cleaned_value:
                            # 分割并转换为整数列表
                            try:
                                int_list = [int(x.strip()) for x in cleaned_value.split(',') if x.strip()]
                                formatted_value = f"[{', '.join(map(str, int_list))}]"
                            except ValueError:
                                formatted_value = "[]"
                        else:
                            formatted_value = "[]"
                    else:
                        formatted_value = str(new_value)
                
                elif field_type == "list[str]":
                    # 清理输入：移除方括号和多余的引号
                    if isinstance(new_value, str):
                        # 移除方括号
                        cleaned_value = re.sub(r'[\[\]]', '', new_value)
                        # 分割并清理每个元素
                        str_list = []
                        for item in cleaned_value.split(','):
                            item = item.strip()
                            # 移除两端的引号（单引号或双引号）
                            item = re.sub(r'^[\'"]|[\'"]$', '', item)
                            if item:
                                str_list.append(f'"{item}"')
                        formatted_value = f"[{', '.join(str_list)}]"
                    else:
                        formatted_value = str(new_value)
                
                elif field_type == "bool":
                    formatted_value = "True" if str(new_value).lower() in ('true', '1', 'yes') else "False"
                
                elif field_type == "select":
                    formatted_value = f'"{new_value}"'
                
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
                
                else:
                    # 字符串类型：确保有引号包围
                    if not (new_value.startswith('"') and new_value.endswith('"')) and \
                       not (new_value.startswith("'") and new_value.endswith("'")):
                        formatted_value = f'"{new_value}"'
                    else:
                        formatted_value = new_value
                
                # 在文件中查找并替换配置项
                pattern = rf"self\.{field_name}\s*=\s*.+"
                replacement = f"self.{field_name} = {formatted_value}"
                content = re.sub(pattern, replacement, content)
        
        # 写入新内容
        with open(config_file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return True, "配置保存成功"
    
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
        "群聊设置": "fas fa-users",
        "私聊设置": "fas fa-user",
        "调试设置": "fas fa-bug",
        "消息设置": "fas fa-comment",
        "Web设置": "fas fa-globe",
        "修炼设置": "fas fa-medal",
        "渡劫设置": "fas fa-bolt",
        "宗门设置": "fas fa-landmark",
        "经济设置": "fas fa-coins",
        "灵根设置": "fas fa-seedling",
        "体力设置": "fas fa-heart",
        "轮回设置": "fas fa-infinity"
    }
    return icon_map.get(category, "fas fa-cog")

@app.route('/get_stats')
def get_stats():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        # 获取总用户数
        total_users_result = execute_sql(DATABASE, "SELECT COUNT(*) FROM user_xiuxian")
        total_users = total_users_result[0]['COUNT(*)'] if total_users_result else 0
        
        # 获取宗门数量
        total_sects_result = execute_sql(DATABASE, "SELECT COUNT(*) FROM sects WHERE sect_owner IS NOT NULL")
        total_sects = total_sects_result[0]['COUNT(*)'] if total_sects_result else 0
        
        # 获取今日活跃用户数（今天有操作记录的用户）
        today = datetime.now().strftime('%Y-%m-%d')
        active_users_result = execute_sql(DATABASE, 
            "SELECT COUNT(DISTINCT user_id) FROM user_cd WHERE date(create_time) = ?", (today,))
        active_users = active_users_result[0]['COUNT(DISTINCT user_id)'] if active_users_result else 0
        
        return jsonify({
            "success": True,
            "total_users": total_users,
            "total_sects": total_sects,
            "active_users": active_users
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/get_system_info')
def get_system_info():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        
        # 获取CPU使用率
        cpu_usage = psutil.cpu_percent(interval=1)
        
        # 获取内存使用率
        memory = psutil.virtual_memory()
        memory_usage = memory.percent
        
        # 获取磁盘使用率
        disk = psutil.disk_usage('/')
        disk_usage = disk.percent
        
        return jsonify({
            "success": True,
            "cpu_usage": round(cpu_usage, 1),
            "memory_usage": round(memory_usage, 1),
            "disk_usage": round(disk_usage, 1)
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})

@app.route('/get_system_info_extended')
def get_system_info_extended():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        
        # 获取系统信息
        system_info = {
            "平台": platform.platform(),
            "系统": platform.system(),
            "版本": platform.version(),
            "机器": platform.machine(),
            "处理器": platform.processor(),
            "Python版本": platform.python_version(),
        }
        
        # 获取CPU信息
        try:
            cpu_info = {
                "物理核心数": psutil.cpu_count(logical=False),
                "逻辑核心数": psutil.cpu_count(logical=True),
                "CPU使用率": f"{psutil.cpu_percent()}%",
                "CPU频率": f"{psutil.cpu_freq().current:.2f}MHz" if hasattr(psutil, "cpu_freq") else "未知"
            }
        except Exception:
            cpu_info = {"CPU信息": "获取失败"}
        
        # 获取内存信息
        try:
            mem = psutil.virtual_memory()
            mem_info = {
                "总内存": f"{mem.total / (1024**3):.2f}GB",
                "已用内存": f"{mem.used / (1024**3):.2f}GB",
                "内存使用率": f"{mem.percent}%"
            }
        except Exception:
            mem_info = {"内存信息": "获取失败"}
        
        # 获取磁盘信息
        try:
            disk = psutil.disk_usage('/')
            disk_info = {
                "总磁盘空间": f"{disk.total / (1024**3):.2f}GB",
                "已用空间": f"{disk.used / (1024**3):.2f}GB",
                "磁盘使用率": f"{disk.percent}%"
            }
        except Exception:
            disk_info = {"磁盘信息": "获取失败"}
        
        # 获取系统启动时间
        try:
            boot_time = psutil.boot_time()
            current_time = time.time()
            uptime_seconds = current_time - boot_time
            
            system_uptime_info = {
                "系统启动时间": f"{datetime.fromtimestamp(boot_time):%Y-%m-%d %H:%M:%S}",
                "系统运行时间": format_time(uptime_seconds)
            }
        except Exception:
            system_uptime_info = {"系统运行时间": "获取失败"}
        
        return jsonify({
            "success": True,
            "system_info": system_info,
            "cpu_info": cpu_info,
            "mem_info": mem_info,
            "disk_info": disk_info,
            "system_uptime": system_uptime_info
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"获取系统信息失败: {str(e)}"})

@app.route('/get_process_info')
def get_process_info():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
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

import threading

def run_flask():
    app.run(host=HOST, port=PORT, debug=False)

if XiuConfig().web_status:
    # 创建并启动线程
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True  # 设置为守护线程，主程序退出时会自动结束
    flask_thread.start()
    logger.info(f"修仙管理面板已启动：{HOST}:{PORT}")