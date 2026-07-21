from .core import (
    ACTIVITY_DB,
    PLAYER_DB,
    TRADE_DB,
    app,
    execute_sql,
    get_db_connection,
    get_dynamic_activity_tables,
    get_dynamic_player_tables,
    get_dynamic_trade_tables,
    get_table_data,
    get_tables,
    jsonify,
    logger,
    redirect,
    render_template,
    request,
    session,
    sql_ident,
    sql_like_text,
    url_for,
)
from ..xiuxian_utils.numeric_bind import format_plain_number, parse_web_number

def _maybe_migrate_pet_storage():
    try:
        from ..xiuxian_utils.pet_system import migrate_pet_storage_once

        migrated = migrate_pet_storage_once()
        if migrated:
            logger.info(f"已迁移 player_pet 旧版大字段记录：{migrated} 条")
    except Exception as e:
        logger.warning(f"player_pet 存储迁移检查失败：{e}")


@app.route('/database')
def database():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    _maybe_migrate_pet_storage()
    all_tables = get_tables()
    return render_template('database.html', tables=all_tables)


def _find_dynamic_table(table_name):
    for db_path, dynamic_tables in (
        (PLAYER_DB, get_dynamic_player_tables()),
        (TRADE_DB, get_dynamic_trade_tables()),
        (ACTIVITY_DB, get_dynamic_activity_tables()),
    ):
        if table_name in dynamic_tables:
            return db_path, dynamic_tables[table_name]
    return None, None


@app.route('/table/<table_name>', methods=['GET'])
def table_view(table_name):
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    if table_name in ("player_pet", "player_pet_item"):
        _maybe_migrate_pet_storage()
    
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
        db_path, table_info = _find_dynamic_table(table_name)
    
    if not db_path:
        return "表不存在", 404
    
    try:
        page = max(1, int(request.args.get('page', 1)))
    except Exception:
        page = 1
    try:
        per_page = min(200, max(1, int(request.args.get('per_page', 20))))
    except Exception:
        per_page = 20
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
    if table_name in ("player_pet", "player_pet_item"):
        _maybe_migrate_pet_storage()
    
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
        db_path, table_info = _find_dynamic_table(table_name)
        is_dynamic_table = bool(db_path)
    
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
                        # 支持纯数字 / 科学计数法；超大整数走 number_count 绑定
                        update_data[field] = parse_web_number(value)
            
            if update_data:
                set_clause = ", ".join([f"{sql_ident(field)} = %s" for field in update_data.keys()])
                where_conditions = " AND ".join([f"{sql_ident(key)} = %s" for key in primary_conditions.keys()])
                sql = f"UPDATE {sql_ident(table_name)} SET {set_clause} WHERE {where_conditions}"
                params = list(update_data.values()) + list(primary_conditions.values())
                result = execute_sql(db_path, sql, params)
                
                if 'error' in result:
                    return jsonify({"success": False, "error": result['error']})
            
            return jsonify({"success": True, "message": "更新成功"})
        
        elif action == 'delete':
            where_conditions = " AND ".join([f"{sql_ident(key)} = %s" for key in primary_conditions.keys()])
            sql = f"DELETE FROM {sql_ident(table_name)} WHERE {where_conditions}"
            result = execute_sql(db_path, sql, list(primary_conditions.values()))
            
            if 'error' in result:
                return jsonify({"success": False, "error": result['error']})
            
            return jsonify({"success": True, "message": "删除成功"})
    
    # GET 请求，获取行数据
    where_conditions = " AND ".join([f"{sql_ident(key)} = %s" for key in primary_conditions.keys()])
    sql = f"SELECT * FROM {sql_ident(table_name)} WHERE {where_conditions}"
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
            # 大数科学计数法（如 5.71e+19）转纯数字展示，便于编辑
            display_data[key] = format_plain_number(value)
    
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
    if table_name in ("player_pet", "player_pet_item"):
        _maybe_migrate_pet_storage()

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
        db_path, table_info = _find_dynamic_table(table_name)
        is_dynamic_table = bool(db_path)
    
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

    fields = table_info.get('fields', [])
    if batch_field not in fields:
        return jsonify({"success": False, "error": "批量修改字段不存在"})
    if search_field and search_field not in fields:
        return jsonify({"success": False, "error": "搜索字段不存在"})
    
    # 如果是全字段搜索但未选择批量修改字段
    if (not search_field or search_field == '') and not batch_field:
        return jsonify({"success": False, "error": "全字段搜索时请选择要修改的字段"})
    
    # 验证表是否存在
    try:
        conn = get_db_connection(db_path)
        if not conn.table_exists(table_name):
            conn.close()
            return jsonify({"success": False, "error": f"表不存在：{table_name}"})
        conn.close()
    except Exception as e:
        return jsonify({"success": False, "error": f"检查表失败：{str(e)}"})

    # 批量数值支持纯数字 / 科学计数法
    parsed_value = parse_web_number(value)
    if parsed_value is None:
        return jsonify({"success": False, "error": "修改值不能为空"})
    
    try:
        table_sql = sql_ident(table_name)
        batch_field_sql = sql_ident(batch_field)
        # 构建更新语句
        if operation == "set":
            sql = f"UPDATE {table_sql} SET {batch_field_sql} = %s"
            params = [parsed_value]
        elif operation == "add":
            sql = f"UPDATE {table_sql} SET {batch_field_sql} = {batch_field_sql} + %s"
            params = [parsed_value]
        elif operation == "subtract":
            sql = f"UPDATE {table_sql} SET {batch_field_sql} = {batch_field_sql} - %s"
            params = [parsed_value]
        else:
            return jsonify({"success": False, "error": "无效的操作类型"})
        
        # 添加 WHERE 条件
        if not apply_to_all:
            if search_field and search_value:
                if search_condition == '=':
                    values = search_value.split()
                    if len(values) > 1:
                        condition = " OR ".join([sql_like_text(search_field) for _ in values])
                        sql += f" WHERE ({condition})"
                        params.extend([f"%{v}%" for v in values])
                    else:
                        sql += f" WHERE {sql_like_text(search_field)}"
                        params.append(f"%{search_value}%")
                elif search_condition in ('>', '<'):
                    values = search_value.split()
                    if len(values) == 1:
                        if not search_value.replace('.', '', 1).isdigit():
                            return jsonify({"success": False, "error": "搜索值必须是数值"})
                        sql += f" WHERE {sql_ident(search_field)} {search_condition} %s"
                        params.append(float(values[0]))
                    else:
                        if not values[0].replace('.', '', 1).isdigit():
                            return jsonify({"success": False, "error": "第一个搜索值必须是数值"})
                        if not values[1]:
                            return jsonify({"success": False, "error": "第二个搜索值不能为空"})
                        primary_key = table_info.get('primary_key', 'user_id')
                        primary_keys = set(primary_key if isinstance(primary_key, list) else [primary_key])
                        searchable_fields = [f for f in fields if f not in primary_keys]
                        if searchable_fields:
                            sql += f" WHERE {sql_ident(search_field)} {search_condition} %s AND ({' OR '.join([sql_like_text(field) for field in searchable_fields])})"
                            params.extend([float(values[0])] + [f"%{values[1]}%" for field in searchable_fields])
                        else:
                            sql += f" WHERE {sql_ident(search_field)} {search_condition} %s"
                            params.append(float(values[0]))
                else:
                    return jsonify({"success": False, "error": "无效的搜索条件"})
            elif search_value:
                # 全字段搜索
                primary_key = table_info.get('primary_key', 'user_id')
                primary_keys = set(primary_key if isinstance(primary_key, list) else [primary_key])
                searchable_fields = [f for f in fields if f not in primary_keys]
                
                if searchable_fields:
                    conditions = []
                    for field in searchable_fields:
                        conditions.append(sql_like_text(field))
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
