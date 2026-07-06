from .core import *  # noqa: F401,F403

from .config import get_root_rate
from ..xiuxian_utils import db_backend


def _execute_many_in_transaction(db_path, statements):
    with db_backend.transaction(db_path) as conn:
        cur = conn.cursor()
        for sql, params in statements:
            cur.execute(sql, params)


@app.route('/commands')
def commands():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('commands.html', commands=ADMIN_COMMANDS)

@app.route('/execute_command', methods=['POST'])
def execute_command():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    data = request.get_json() or {}
    command_name = data.get('command_name')
    
    if not command_name:
        return jsonify({"success": False, "error": "未指定命令"})

    def _safe_json_load(s, default):
        try:
            if s is None:
                return default
            if isinstance(s, (dict, list)):
                return s
            return json.loads(s)
        except Exception:
            return default

    def _safe_json_dump(obj):
        return json.dumps(obj, ensure_ascii=False)

    def _ensure_player_accessory_row(conn, user_id: str):
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_accessory (
                user_id TEXT PRIMARY KEY,
                equipped TEXT DEFAULT NULL,
                bag TEXT DEFAULT NULL
            )
        """)

        cur.execute("SELECT equipped, bag FROM player_accessory WHERE user_id = %s", (str(user_id),))
        row = cur.fetchone()
        if row is None:
            equipped = {"手镯": None, "戒指": None, "手环": None, "项链": None}
            bag = []
            cur.execute(
                "INSERT INTO player_accessory (user_id, equipped, bag) VALUES (%s, %s, %s)",
                (str(user_id), _safe_json_dump(equipped), _safe_json_dump(bag))
            )
            return equipped, bag
        else:
            equipped = _safe_json_load(row[0], {"手镯": None, "戒指": None, "手环": None, "项链": None})
            bag = _safe_json_load(row[1], [])
            if not isinstance(equipped, dict):
                equipped = {"手镯": None, "戒指": None, "手环": None, "项链": None}
            if not isinstance(bag, list):
                bag = []
            return equipped, bag

    def _roll_affixes_for_quality(quality: int):
        # 和你饰品系统保持一致的字段
        WASH_RANGE = {
            1: {"气血": (0.02, 0.05), "抗暴": (0.01, 0.03), "防御": (0.01, 0.03), "会心": (0.01, 0.03), "会心伤害": (0.02, 0.05), "攻击": (0.02, 0.05)},
            2: {"气血": (0.04, 0.08), "抗暴": (0.02, 0.05), "防御": (0.02, 0.05), "会心": (0.02, 0.05), "会心伤害": (0.04, 0.08), "攻击": (0.04, 0.08)},
            3: {"气血": (0.06, 0.12), "抗暴": (0.03, 0.07), "防御": (0.03, 0.07), "会心": (0.03, 0.07), "会心伤害": (0.06, 0.12), "攻击": (0.06, 0.12)},
            4: {"气血": (0.08, 0.16), "抗暴": (0.04, 0.10), "防御": (0.04, 0.10), "会心": (0.04, 0.10), "会心伤害": (0.08, 0.16), "攻击": (0.08, 0.16)},
            5: {"气血": (0.10, 0.20), "抗暴": (0.05, 0.12), "防御": (0.05, 0.12), "会心": (0.05, 0.12), "会心伤害": (0.10, 0.20), "攻击": (0.10, 0.20)},
        }
        AFFIX_TYPES = ["气血", "抗暴", "防御", "会心", "会心伤害", "攻击"]
        quality = max(1, min(5, int(quality)))
        chosen = random.sample(AFFIX_TYPES, 2)
        out = []
        for t in chosen:
            lo, hi = WASH_RANGE[quality][t]
            out.append({"type": t, "value": round(random.uniform(lo, hi), 4)})
        return out

    def _grant_accessory_sql(user_id: str, item_id: int, amount: int, quality: int = 1):
        quality = max(1, min(5, int(quality)))
        amount = max(1, int(amount))

        item_info = items.get_data_by_item_id(int(item_id))
        if not item_info:
            return False, 0, "饰品配置不存在"

        try:
            with db_backend.transaction(PLAYER_DB) as conn:
                equipped, bag = _ensure_player_accessory_row(conn, str(user_id))

                for _ in range(amount):
                    uid = f"acc_{int(time.time() * 1000)}_{random.randint(1000,9999)}_{uuid.uuid4().hex[:4]}"
                    ins = {
                        "uid": uid,
                        "item_id": int(item_id),
                        "name": item_info.get("name", "未知饰品"),
                        "part": item_info.get("part", item_info.get("item_type", "未知部位")),
                        "set_type": item_info.get("set_type", "未知套装"),
                        "quality": quality,
                        "affixes": _roll_affixes_for_quality(quality)
                    }
                    bag.append(ins)

                cur = conn.cursor()
                cur.execute(
                    "UPDATE player_accessory SET equipped = %s, bag = %s WHERE user_id = %s",
                    (_safe_json_dump(equipped), _safe_json_dump(bag), str(user_id))
                )
            return True, amount, ""
        except Exception as e:
            return False, 0, str(e)

    def _remove_accessory_sql(user_id: str, item_id: int, amount: int):
        # 只从bag扣除，不动equipped
        amount = max(1, int(amount))
        try:
            with db_backend.transaction(PLAYER_DB) as conn:
                equipped, bag = _ensure_player_accessory_row(conn, str(user_id))

                kept = []
                removed = 0
                need = amount

                for acc in bag:
                    if need > 0 and int(acc.get("item_id", 0)) == int(item_id):
                        removed += 1
                        need -= 1
                    else:
                        kept.append(acc)

                if removed <= 0:
                    return False, 0, "背包中无可扣除饰品（已装备不参与扣除）"

                cur = conn.cursor()
                cur.execute(
                    "UPDATE player_accessory SET equipped = %s, bag = %s WHERE user_id = %s",
                    (_safe_json_dump(equipped), _safe_json_dump(kept), str(user_id))
                )
            return True, removed, ""
        except Exception as e:
            return False, 0, str(e)

    try:
        if command_name == "gm_command":
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            
            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                
                sql = "UPDATE user_xiuxian SET stone = stone + %s WHERE user_id = %s"
                execute_sql(DATABASE, sql, (amount, user_info['user_id']))
                
                return jsonify({
                    "success": True, 
                    "message": f"成功向 {username} {'增加' if amount >= 0 else '减少'} {abs(amount)} 灵石"
                })
            else:
                sql = "UPDATE user_xiuxian SET stone = stone + %s"
                execute_sql(DATABASE, sql, (amount,))
                return jsonify({
                    "success": True, 
                    "message": f"全服{'发放' if amount >= 0 else '扣除'} {abs(amount)} 灵石成功"
                })
        
        elif command_name == "adjust_exp_command":
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            
            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                
                if amount > 0:
                    sql = "UPDATE user_xiuxian SET exp = exp + %s WHERE user_id = %s"
                    execute_sql(DATABASE, sql, (amount, user_info['user_id']))
                    return jsonify({"success": True, "message": f"成功向 {username} 增加 {amount} 修为"})
                else:
                    sql = "UPDATE user_xiuxian SET exp = exp - %s WHERE user_id = %s"
                    execute_sql(DATABASE, sql, (abs(amount), user_info['user_id']))
                    return jsonify({"success": True, "message": f"成功从 {username} 减少 {abs(amount)} 修为"})
            else:
                if amount > 0:
                    sql = "UPDATE user_xiuxian SET exp = exp + %s"
                else:
                    sql = "UPDATE user_xiuxian SET exp = exp - %s"
                execute_sql(DATABASE, sql, (abs(amount),))
                return jsonify({
                    "success": True, 
                    "message": f"全服{'增加' if amount >= 0 else '减少'} {abs(amount)} 修为成功"
                })
        
        elif command_name == "gmm_command":
            username = data.get('username')
            root_type = data.get('root_type')
            
            if not username:
                return jsonify({"success": False, "error": "请指定用户名"})
            
            user_info = get_user_by_name(username)
            if not user_info:
                return jsonify({"success": False, "error": f"用户 {username} 不存在"})
            
            root_names = {
                "1": "全属性灵根", "2": "融合万物灵根", "3": "月灵根", "4": "言灵灵根",
                "5": "金灵根", "6": "轮回千次不灭，只为臻至巅峰", "7": "轮回万次不灭，只为超越巅峰",
                "8": "轮回无尽不灭，只为触及永恒之境", "9": f"轮回命主·{username}"
            }
            
            root_name = root_names.get(root_type, "未知灵根")
            root_type_name = ROOTS.get(root_type, "混沌灵根")
            
            root_rate = get_root_rate(root_type_name, user_info['user_id'])
            _execute_many_in_transaction(
                DATABASE,
                [
                    (
                        "UPDATE user_xiuxian SET root = %s, root_type = %s WHERE user_id = %s",
                        (root_name, root_type_name, user_info['user_id']),
                    ),
                    (
                        "UPDATE user_xiuxian SET power = round(exp * %s * (SELECT spend FROM level_data WHERE level = user_xiuxian.level), 0) WHERE user_id = %s",
                        (root_rate, user_info['user_id']),
                    ),
                ],
            )
            
            return jsonify({"success": True, "message": f"成功将 {username} 的灵根修改为 {root_name}"})
        
        elif command_name == "zaohua_xiuxian":
            username = data.get('username')
            level = data.get('level')
            
            if not username:
                return jsonify({"success": False, "error": "请指定用户名"})
            
            user_info = get_user_by_name(username)
            if not user_info:
                return jsonify({"success": False, "error": f"用户 {username} 不存在"})
            
            levels = convert_rank('江湖好手')[1]
            if level not in levels:
                return jsonify({"success": False, "error": f"无效的境界: {level}"})
            
            level_data = jsondata.level_data()
            if not level_data or level not in level_data:
                return jsonify({"success": False, "error": f"无法获取境界 {level} 的数据"})
            
            max_exp = int(level_data[level]['power'])
            root_rate = get_root_rate(user_info['root_type'], user_info['user_id'])
            _execute_many_in_transaction(
                DATABASE,
                [
                    (
                        "UPDATE user_xiuxian SET exp = %s, level = %s WHERE user_id = %s",
                        (max_exp, level, user_info['user_id']),
                    ),
                    (
                        "UPDATE user_xiuxian SET hp = exp / 2, mp = exp, atk = exp / 10 WHERE user_id = %s",
                        (user_info['user_id'],),
                    ),
                    (
                        "UPDATE user_xiuxian SET power = round(exp * %s * (SELECT spend FROM level_data WHERE level = %s), 0) WHERE user_id = %s",
                        (root_rate, level, user_info['user_id']),
                    ),
                ],
            )
            
            return jsonify({"success": True, "message": f"成功将 {username} 的境界修改为 {level}"})
        
        elif command_name == "cz":
            # 创造力量（饰品专门SQL处理）
            target = data.get('target')
            username = data.get('username')
            item_input = data.get('item')
            amount = int(data.get('amount', 1))
            quality = int(data.get('quality', 1))  # 饰品可选

            if not item_input:
                return jsonify({"success": False, "error": "请指定物品"})
            if amount <= 0:
                return jsonify({"success": False, "error": "数量必须大于0"})

            quality = max(1, min(5, quality))

            goods_id, item_data = items.get_data_by_item_name(str(item_input))
            if not goods_id or not item_data:
                return jsonify({"success": False, "error": f"物品 {item_input} 不存在"})

            goods_id = int(goods_id)
            goods_name = item_data['name']
            goods_type = item_data.get('type', '未知类型')
            is_accessory = item_data.get("item_type") == "饰品"

            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})

                user_id = str(user_info['user_id'])

                if is_accessory:
                    ok, grant_num, reason = _grant_accessory_sql(user_id, goods_id, amount, quality)
                    if not ok:
                        return jsonify({"success": False, "error": f"饰品发放失败: {reason}"})
                    return jsonify({
                        "success": True,
                        "message": f"成功向 {username} 发放【{goods_name}】饰品 x{grant_num}（{quality}阶）"
                    })
                else:
                    now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    sql_check = "SELECT * FROM back WHERE user_id = %s AND goods_id = %s"
                    existing_item = execute_sql(DATABASE, sql_check, (user_id, goods_id))

                    if existing_item:
                        sql_update = "UPDATE back SET goods_num = goods_num + %s, update_time = %s WHERE user_id = %s AND goods_id = %s"
                        execute_sql(DATABASE, sql_update, (amount, now_time, user_id, goods_id))
                    else:
                        sql_insert = """
                            INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 0)
                        """
                        execute_sql(DATABASE, sql_insert, (user_id, goods_id, goods_name, goods_type, amount, now_time, now_time))

                    return jsonify({"success": True, "message": f"成功向 {username} 发放 {goods_name} x{amount}"})
            else:
                sql_users = "SELECT user_id FROM user_xiuxian"
                all_users = execute_sql(DATABASE, sql_users, ())
                success_count = 0

                now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                for user in all_users:
                    try:
                        user_id = str(user['user_id'])
                        if is_accessory:
                            ok, grant_num, _ = _grant_accessory_sql(user_id, goods_id, amount, quality)
                            if ok and grant_num > 0:
                                success_count += 1
                        else:
                            sql_check = "SELECT * FROM back WHERE user_id = %s AND goods_id = %s"
                            existing_item = execute_sql(DATABASE, sql_check, (user_id, goods_id))

                            if existing_item:
                                sql_update = "UPDATE back SET goods_num = goods_num + %s, update_time = %s WHERE user_id = %s AND goods_id = %s"
                                execute_sql(DATABASE, sql_update, (amount, now_time, user_id, goods_id))
                            else:
                                sql_insert = """
                                    INSERT INTO back (user_id, goods_id, goods_name, goods_type, goods_num, create_time, update_time, bind_num)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, 0)
                                """
                                execute_sql(DATABASE, sql_insert, (user_id, goods_id, goods_name, goods_type, amount, now_time, now_time))
                            success_count += 1
                    except Exception:
                        continue

                if is_accessory:
                    return jsonify({
                        "success": True,
                        "message": f"全服发放【{goods_name}】饰品 x{amount}（{quality}阶）成功，影响 {success_count} 名用户"
                    })
                else:
                    return jsonify({
                        "success": True,
                        "message": f"全服发放 {goods_name} x{amount} 成功，影响 {success_count} 名用户"
                    })
        
        elif command_name == "hmll":
            # 毁灭力量（饰品专门SQL处理，仅扣bag）
            target = data.get('target')
            username = data.get('username')
            item_input = data.get('item')
            amount = int(data.get('amount', 1))

            if not item_input:
                return jsonify({"success": False, "error": "请指定物品"})
            if amount <= 0:
                return jsonify({"success": False, "error": "数量必须大于0"})

            goods_id, item_data = items.get_data_by_item_name(str(item_input))
            if not goods_id or not item_data:
                return jsonify({"success": False, "error": f"物品 {item_input} 不存在"})

            goods_id = int(goods_id)
            goods_name = item_data['name']
            is_accessory = item_data.get("item_type") == "饰品"

            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                user_id = str(user_info['user_id'])

                if is_accessory:
                    ok, removed, reason = _remove_accessory_sql(user_id, goods_id, amount)
                    if not ok:
                        return jsonify({"success": False, "error": f"扣除失败：{reason}"})
                    msg = f"成功从 {username} 扣除【{goods_name}】饰品 x{removed}"
                    if removed < amount:
                        msg += "（数量不足，已按实际可扣执行）"
                    return jsonify({"success": True, "message": msg})
                else:
                    sql_check = "SELECT goods_num FROM back WHERE user_id = %s AND goods_id = %s"
                    user_item = execute_sql(DATABASE, sql_check, (user_id, goods_id))

                    if not user_item or int(user_item[0]['goods_num']) <= 0:
                        return jsonify({"success": False, "error": f"用户 {username} 没有足够的 {goods_name}"})

                    current_num = int(user_item[0]['goods_num'])
                    deduct = min(amount, current_num)

                    sql_update = "UPDATE back SET goods_num = goods_num - %s WHERE user_id = %s AND goods_id = %s"
                    execute_sql(DATABASE, sql_update, (deduct, user_id, goods_id))

                    sql_clean = "DELETE FROM back WHERE user_id = %s AND goods_id = %s AND goods_num <= 0"
                    execute_sql(DATABASE, sql_clean, (user_id, goods_id))

                    msg = f"成功从 {username} 扣除 {goods_name} x{deduct}"
                    if deduct < amount:
                        msg += "（数量不足，已按实际可扣执行）"
                    return jsonify({"success": True, "message": msg})
            else:
                sql_users = "SELECT user_id FROM user_xiuxian"
                all_users = execute_sql(DATABASE, sql_users, ())
                success_count = 0
                total_removed = 0

                for user in all_users:
                    try:
                        user_id = str(user['user_id'])
                        if is_accessory:
                            ok, removed, _ = _remove_accessory_sql(user_id, goods_id, amount)
                            if ok and removed > 0:
                                success_count += 1
                                total_removed += removed
                        else:
                            sql_check = "SELECT goods_num FROM back WHERE user_id = %s AND goods_id = %s"
                            user_item = execute_sql(DATABASE, sql_check, (user_id, goods_id))
                            if user_item and int(user_item[0]['goods_num']) > 0:
                                current_num = int(user_item[0]['goods_num'])
                                deduct = min(amount, current_num)

                                sql_update = "UPDATE back SET goods_num = goods_num - %s WHERE user_id = %s AND goods_id = %s"
                                execute_sql(DATABASE, sql_update, (deduct, user_id, goods_id))

                                sql_clean = "DELETE FROM back WHERE user_id = %s AND goods_id = %s AND goods_num <= 0"
                                execute_sql(DATABASE, sql_clean, (user_id, goods_id))

                                success_count += 1
                                total_removed += deduct
                    except Exception:
                        continue

                if is_accessory:
                    return jsonify({
                        "success": True,
                        "message": f"全服扣除【{goods_name}】完成，影响 {success_count} 名用户，累计扣除 {total_removed} 件（仅背包，已装备未扣除）"
                    })
                else:
                    return jsonify({
                        "success": True,
                        "message": f"全服扣除 {goods_name} 完成，影响 {success_count} 名用户，累计扣除 {total_removed} 个"
                    })

        elif command_name == "ccll_command":
            target = data.get('target')
            username = data.get('username')
            amount = int(data.get('amount', 0))
            
            if target == "指定用户" and username:
                user_info = get_user_by_name(username)
                if not user_info:
                    return jsonify({"success": False, "error": f"用户 {username} 不存在"})
                
                sql_check = "SELECT * FROM xiuxian_impart WHERE user_id = %s"
                impart_data = execute_sql(IMPART_DB, sql_check, (user_info['user_id'],))
                
                if impart_data:
                    sql_update = "UPDATE xiuxian_impart SET stone_num = stone_num + %s WHERE user_id = %s"
                    execute_sql(IMPART_DB, sql_update, (amount, user_info['user_id']))
                else:
                    sql_insert = "INSERT INTO xiuxian_impart (user_id, stone_num) VALUES (%s, %s)"
                    execute_sql(IMPART_DB, sql_insert, (user_info['user_id'], amount))
                
                return jsonify({
                    "success": True, 
                    "message": f"成功向 {username} {'增加' if amount >= 0 else '减少'} {abs(amount)} 思恋结晶"
                })
            else:
                sql_users = "SELECT user_id FROM user_xiuxian"
                all_users = execute_sql(DATABASE, sql_users, ())
                success_count = 0
                
                for user in all_users:
                    try:
                        user_id = user['user_id']
                        sql_check = "SELECT * FROM xiuxian_impart WHERE user_id = %s"
                        impart_data = execute_sql(IMPART_DB, sql_check, (user_id,))
                        
                        if impart_data:
                            sql_update = "UPDATE xiuxian_impart SET stone_num = stone_num + %s WHERE user_id = %s"
                            execute_sql(IMPART_DB, sql_update, (amount, user_id))
                        else:
                            sql_insert = "INSERT INTO xiuxian_impart (user_id, stone_num) VALUES (%s, %s)"
                            execute_sql(IMPART_DB, sql_insert, (user_id, amount))
                        
                        success_count += 1
                    except Exception:
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
