import sqlite3
import os
import json
from pathlib import Path
from datetime import datetime
from nonebot import get_driver
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from ..xiuxian_config import convert_rank
from ..xiuxian_utils.item_json import Items
from ..xiuxian_utils.xiuxian2_handle import XiuxianDateManage
from ..xiuxian_utils.xiuxian2_handle import XIUXIAN_IMPART_BUFF
from ..xiuxian_utils.data_source import jsondata

items = Items()
sql_message = XiuxianDateManage()
xiuxian_impart = XIUXIAN_IMPART_BUFF()
app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # 用于会话加密

# 配置
DATABASE = Path() / "data" / "xiuxian" / "xiuxian.db"
IMPART_DB = Path() / "data" / "xiuxian" / "xiuxian_impart.db"
ADMIN_IDS = get_driver().config.superusers
PORT = 5888
HOST = '0.0.0.0'

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

def get_database_tables(db_path):
    """动态获取数据库中的所有表及其字段信息，包括主键"""
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

def get_tables():
    """获取所有数据库的表结构，按数据库分组"""
    databases = {
        "主数据库": DATABASE,
        "虚神界数据库": IMPART_DB
    }
    
    tables = {}
    for db_name, db_path in databases.items():
        # 获取数据库的表结构
        db_tables = get_database_tables(db_path)
        tables[db_name] = {
            "path": db_path,
            "tables": db_tables
        }
    
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

# 在get_table_data函数中添加多值搜索支持
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
        # 分割搜索值为多个条件
        values = search_value.split()
        if len(values) > 1:
            # 多个值使用OR连接
            placeholders = " OR ".join([f"{search_field} LIKE ?" for _ in values])
            sql += f" WHERE ({placeholders})"
            params.extend([f"%{value}%" for value in values])
        else:
            # 单个值
            sql += f" WHERE {search_field} LIKE ?"
            params.append(f"%{search_value}%")
    
    # 添加分页
    sql += f" LIMIT ? OFFSET ?"
    params.extend([per_page, offset])
    
    # 执行查询
    data = execute_sql(db_path, sql, params)
    
    # 获取总数
    count_sql = f"SELECT COUNT(*) FROM {table_name}"
    if search_field and search_value:
        values = search_value.split()
        if len(values) > 1:
            placeholders = " OR ".join([f"{search_field} LIKE ?" for _ in values])
            count_sql += f" WHERE ({placeholders})"
            count_params = [f"%{value}%" for value in values]
        else:
            count_sql += f" WHERE {search_field} LIKE ?"
            count_params = [f"%{search_value}%"]
    else:
        count_params = None
    
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
    
    # 获取主键字段名
    primary_key = table_info.get('primary_key', 'id')
    
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
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key} = ?"
            
            # 执行更新
            params = list(update_data.values()) + [row_id]
            result = execute_sql(db_path, sql, params)
            
            if 'error' in result:
                return jsonify({"success": False, "error": result['error']})
            
            return jsonify({"success": True, "message": "更新成功"})
        
        elif action == 'delete':
            # 构建DELETE语句
            sql = f"DELETE FROM {table_name} WHERE {primary_key} = ?"
            result = execute_sql(db_path, sql, (row_id,))
            
            if 'error' in result:
                return jsonify({"success": False, "error": result['error']})
            
            return jsonify({"success": True, "message": "删除成功"})
    
    # GET请求，获取行数据
    sql = f"SELECT * FROM {table_name} WHERE {primary_key} = ?"
    row_data = execute_sql(db_path, sql, (row_id,))
    
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
        primary_key=primary_key
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
    
    if not all([batch_field, operation, value]):
        return jsonify({"success": False, "error": "参数不完整"})
    
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
        
        # 添加WHERE条件（如果不应用到整张表且有搜索条件）
        if not apply_to_all and search_field and search_value:
            values = search_value.split()
            if len(values) > 1:
                condition = " OR ".join([f"{search_field} LIKE ?" for _ in values])
                sql += f" WHERE ({condition})"
                params.extend([f"%{v}%" for v in values])
            else:
                sql += f" WHERE {search_field} LIKE ?"
                params.append(f"%{search_value}%")
        
        # 执行更新
        result = execute_sql(db_path, sql, params)
        
        if 'error' in result:
            return jsonify({"success": False, "error": result['error']})
        
        # 获取受影响的行数
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
            level_data = execute_sql(DATABASE, sql_level, (level,))
            if not level_data:
                return jsonify({"success": False, "error": f"无法获取境界 {level} 的数据"})
            
            max_exp = int(level_data[0]['power'])
            
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
    app.run(host='0.0.0.0', port=PORT, debug=False)

# 创建并启动线程
flask_thread = threading.Thread(target=run_flask)
flask_thread.daemon = True  # 设置为守护线程，主程序退出时会自动结束
flask_thread.start()

# 你的主程序可以继续执行其他代码
print("修仙管理面板已启动：127.0.0.1:5888")