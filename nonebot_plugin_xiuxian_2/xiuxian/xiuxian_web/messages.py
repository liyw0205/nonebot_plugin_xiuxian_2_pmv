import base64
from urllib.parse import quote, urlparse

from .core import *  # noqa: F401,F403
from ..broadcast_manager import format_broadcast_status, start_broadcast
from ..xiuxian_utils import message_db as message_db_config


def _parse_message_config_int(data, key: str, minimum: int, maximum: int) -> int:
    try:
        return min(maximum, max(minimum, int(data.get(key))))
    except Exception:
        raise ValueError(f"{key} 必须是 {minimum} 到 {maximum} 的整数")


def _prepare_message_rows(rows: list[dict]) -> list[dict]:
    rows = fill_private_username_from_group(rows)
    rows = fill_message_display_profiles(rows)

    for r in rows:
        raw_content = r.get("content") or ""
        display_content, content_format = extract_markdown_content_from_repr(raw_content)
        display_content = normalize_message_display_content(display_content)

        r["display_content"] = display_content
        r["content_format"] = content_format
        r["can_revoke"] = bool(
            r.get("direction") == "send"
            and r.get("message_id")
        )

        # msg_id 回复窗口和 reference_id 引用回复是两种能力，前端分别展示。
        r["can_reply"] = False
        r["can_reply_msg_id"] = False
        r["can_quote_reference"] = False
        r["can_quote"] = False
        r["reply_expired"] = True
        r["reply_count_limited"] = False

        if r.get("adapter") == "QQ" and r.get("direction") == "recv":
            scene = r.get("scene", "")
            message_id = str(r.get("message_id") or "")
            reference_id = str(r.get("reference_id") or "")
            valid_seconds = get_qq_reply_valid_seconds(r.get("scene", ""))
            can_time = is_message_within_seconds(r.get("created_at", ""), valid_seconds)
            can_count = int(r.get("reply_used_count") or 0) < 5
            can_reply_msg_id = bool(message_id and can_time and can_count)
            can_quote_reference = bool(reference_id or (scene in ("channel_group", "channel_private") and message_id))

            r["can_reply_msg_id"] = can_reply_msg_id
            r["can_quote_reference"] = can_quote_reference
            r["can_quote"] = bool(can_reply_msg_id or can_quote_reference)
            r["can_reply"] = r["can_quote"]
            r["reply_expired"] = not can_time
            r["reply_count_limited"] = not can_count

    for r in rows:
        group_title = r.get("group_name") or r.get("group_id") or ""
        user_title = r.get("username") or r.get("nickname") or r.get("user_id") or ""

        r["group_avatar_text"] = group_title[:1] if group_title else "群"
        r["user_avatar_text"] = user_title[:1] if user_title else "人"

    return rows


def _prepare_session_rows(rows: list[dict], conn=None) -> list[dict]:
    rows = fill_session_display_profiles(rows)

    for r in rows:
        # 修复私聊标题显示成 Bot 的问题
        if r.get("scene") in ("private", "channel_private"):
            title = str(r.get("title") or "").strip()
            if not title or title.lower() == "bot":
                human_name = ""
                if conn is not None:
                    human_name = get_latest_human_name_by_user_id(conn, str(r.get("target_id") or ""))
                r["title"] = human_name or str(r.get("target_id") or "未知会话")

        preview_source = {
            "scene": r.get("scene"),
            "direction": r.get("direction"),
            "content": r.get("last_content"),
            "username": r.get("username"),
            "nickname": r.get("nickname"),
            "user_id": r.get("user_id"),
        }
        r["last_preview"] = build_session_preview(preview_source)

        title = r.get("title") or r.get("target_id") or ""
        r["avatar_text"] = str(title)[:1] if title else "?"

    return rows


def _run_qq_send_with_optional_msg_ref(send_func, **kwargs):
    try:
        return run_async(send_func(**kwargs))
    except TypeError as e:
        if "msg_ref_id" not in str(e):
            raise
        kwargs.pop("msg_ref_id", None)
        return run_async(send_func(**kwargs))


@app.route('/messages')
def messages_page():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('messages.html')

@app.route('/api/messages/config', methods=['GET'])
def api_messages_config():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    config = message_db_config.get_message_db_config()
    return jsonify({
        "success": True,
        "config": config,
        "record_enabled": message_db_config.is_message_record_enabled(),
    })

@app.route('/api/messages/config', methods=['POST'])
def api_messages_config_save():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        data = request.get_json(silent=True) or {}
        config = {
            "message_db_max_size_mb": _parse_message_config_int(data, "message_db_max_size_mb", 0, 10000),
            "message_group_keep_days": _parse_message_config_int(data, "message_group_keep_days", 0, 36500),
            "message_private_keep_days": _parse_message_config_int(data, "message_private_keep_days", 0, 36500),
        }
        config = message_db_config.update_message_db_config(config)
        return jsonify({
            "success": True,
            "config": config,
            "record_enabled": message_db_config.is_message_record_enabled(),
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)})
    except Exception as e:
        return jsonify({"success": False, "error": f"保存消息配置失败: {e}"})

@app.route('/api/messages/list')
def api_messages_list():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    conn = None

    try:
        scene = request.args.get("scene", "ALL").strip()
        direction = request.args.get("direction", "ALL").strip()
        keyword = request.args.get("keyword", "").strip()
        group_id = request.args.get("group_id", "").strip()
        user_id = request.args.get("user_id", "").strip()
        adapter = request.args.get("adapter", "").strip()

        start = request.args.get("start", "").strip()
        end = request.args.get("end", "").strip()
        date = request.args.get("date", "").strip()

        page = max(1, int(request.args.get("page", 1)))
        page_size = min(max(int(request.args.get("page_size", 50)), 10), 300)
        include_total = str(request.args.get("include_total", "1")).strip().lower() not in ("0", "false", "no")
        offset = (page - 1) * page_size

        where = []
        params = []

        if scene and scene != "ALL":
            where.append("scene = %s")
            params.append(scene)

        if direction and direction != "ALL":
            where.append("direction = %s")
            params.append(direction)

        if keyword:
            where.append("""
                (
                    content LIKE %s
                    OR username LIKE %s
                    OR nickname LIKE %s
                    OR group_name LIKE %s
                    OR group_id LIKE %s
                    OR user_id LIKE %s
                )
            """)
            kw = f"%{keyword}%"
            params.extend([kw, kw, kw, kw, kw, kw])

        if group_id:
            where.append("group_id = %s")
            params.append(group_id)

        if user_id:
            where.append("user_id = %s")
            params.append(user_id)

        if adapter:
            where.append("adapter = %s")
            params.append(adapter)

        if start:
            where.append("created_at >= %s")
            params.append(start.replace("T", " "))

        if end:
            where.append("created_at <= %s")
            params.append(end.replace("T", " "))
        
        if date:
            where.append(f"{db_backend.date_expression('created_at')} = %s")
            params.append(date)

        where_sql = " WHERE " + " AND ".join(where) if where else ""

        conn = get_message_db_connection()
        cur = conn.cursor()

        total = None
        if include_total:
            cur.execute(f"SELECT COUNT(*) AS c FROM messages {where_sql}", params)
            total = cur.fetchone()["c"]

        cur.execute(f"""
            SELECT *
            FROM messages
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s
        """, params + [page_size if include_total else page_size + 1, offset])

        fetched_rows = [dict(r) for r in cur.fetchall()]
        has_more = False
        if not include_total and len(fetched_rows) > page_size:
            has_more = True
            fetched_rows = fetched_rows[:page_size]
        elif include_total:
            has_more = offset + len(fetched_rows) < int(total or 0)

        rows = _prepare_message_rows(fetched_rows)

        return jsonify({
            "success": True,
            "total": total if include_total else len(rows),
            "has_more": has_more,
            "page": page,
            "page_size": page_size,
            "rows": rows
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取消息列表失败: {e}"})

    finally:
        if conn is not None:
            conn.close()

@app.route('/api/messages/dates')
def api_messages_dates():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    conn = None

    try:
        scene = request.args.get("scene", "").strip()
        adapter = request.args.get("adapter", "").strip()
        target_id = request.args.get("target_id", "").strip()
        include_counts = str(request.args.get("include_counts", "1")).strip().lower() not in ("0", "false", "no")

        if scene not in ("group", "private", "channel_group", "channel_private"):
            return jsonify({"success": False, "error": "无效 scene"})

        if not target_id:
            return jsonify({"success": False, "error": "缺少 target_id"})

        where = ["scene = %s"]
        params = [scene]

        if adapter:
            where.append("adapter = %s")
            params.append(adapter)

        if scene in ("group", "channel_group"):
            where.append("group_id = %s")
            params.append(target_id)
        else:
            where.append("user_id = %s")
            params.append(target_id)

        conn = get_message_db_connection()
        cur = conn.cursor()

        today = datetime.now().strftime("%Y-%m-%d")
        rows = []

        if include_counts:
            created_at_date = db_backend.date_expression("created_at")
            cur.execute(f"""
                SELECT {created_at_date} AS d, COUNT(*) AS c
                FROM messages
                WHERE {' AND '.join(where)}
                GROUP BY {created_at_date}
                ORDER BY d DESC
                LIMIT 60
            """, params)
            date_rows = cur.fetchall()
        else:
            cur.execute(f"""
                SELECT created_at
                FROM messages
                WHERE {' AND '.join(where)}
                ORDER BY created_at DESC, id DESC
                LIMIT 5000
            """, params)

            seen_dates = set()
            date_rows = []
            for r in cur.fetchall():
                d = str(r["created_at"] or "")[:10]
                if not d or d in seen_dates:
                    continue
                seen_dates.add(d)
                date_rows.append({"d": d, "c": None})
                if len(date_rows) >= 60:
                    break

        for r in date_rows:
            d = r["d"]
            if d == today:
                label = "今天"
            else:
                try:
                    dt = datetime.strptime(d, "%Y-%m-%d")
                    label = dt.strftime("%m月%d日")
                except Exception:
                    label = d

            rows.append({
                "date": d,
                "label": label,
                "count": r["c"],
            })

        return jsonify({
            "success": True,
            "rows": rows
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取日期失败: {e}"})

    finally:
        if conn is not None:
            conn.close()

@app.route('/api/messages/sessions')
def api_messages_sessions():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    conn = None

    try:
        scene = request.args.get("scene", "group").strip()
        adapter = request.args.get("adapter", "").strip()

        conn = get_message_db_connection()
        cur = conn.cursor()

        adapter_sql = ""
        params = [scene]

        if adapter:
            adapter_sql = " AND adapter = %s "
            params.append(adapter)

        if scene in ("group", "channel_group"):
            cur.execute(f"""
                WITH latest AS (
                    SELECT
                        adapter,
                        scene,
                        group_id AS target_id,
                        MAX(id) AS latest_id
                    FROM messages
                    WHERE scene = %s
                      AND group_id IS NOT NULL
                      AND group_id != ''
                      {adapter_sql}
                    GROUP BY adapter, scene, group_id
                )
                SELECT
                    l.adapter,
                    l.scene,
                    l.target_id,
                    m.id AS last_row_id,
                    COALESCE(NULLIF(m.group_name, ''), l.target_id) AS title,
                    m.bot_id AS bot_id,
                    m.created_at AS last_time,
                    m.content AS last_content,
                    m.direction AS direction,
                    m.username AS username,
                    m.nickname AS nickname,
                    m.avatar AS avatar,
                    m.user_id AS user_id
                FROM latest l
                JOIN messages m ON m.id = l.latest_id
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT 300
            """, params)

        elif scene in ("private", "channel_private"):
            cur.execute(f"""
                WITH latest AS (
                    SELECT
                        adapter,
                        scene,
                        user_id AS target_id,
                        MAX(id) AS latest_id
                    FROM messages
                    WHERE scene = %s
                      AND user_id IS NOT NULL
                      AND user_id != ''
                      {adapter_sql}
                    GROUP BY adapter, scene, user_id
                )
                SELECT
                    l.adapter,
                    l.scene,
                    l.target_id,
                    m.id AS last_row_id,
                    COALESCE(NULLIF(m.username, ''), NULLIF(m.nickname, ''), l.target_id) AS title,
                    m.bot_id AS bot_id,
                    m.created_at AS last_time,
                    m.content AS last_content,
                    m.direction AS direction,
                    m.username AS username,
                    m.nickname AS nickname,
                    m.avatar AS avatar,
                    m.user_id AS user_id
                FROM latest l
                JOIN messages m ON m.id = l.latest_id
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT 300
            """, params)

        else:
            return jsonify({"success": False, "error": "无效 scene"})

        rows = _prepare_session_rows([dict(r) for r in cur.fetchall()], conn)
        last_row_id = max([int(r.get("last_row_id") or 0) for r in rows] or [0])

        return jsonify({
            "success": True,
            "last_row_id": last_row_id,
            "rows": rows
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取会话失败: {e}"})

    finally:
        if conn is not None:
            conn.close()


@app.route('/api/messages/sessions_since')
def api_messages_sessions_since():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    conn = None

    try:
        scene = request.args.get("scene", "group").strip()
        adapter = request.args.get("adapter", "").strip()
        try:
            after_id = max(0, int(request.args.get("after_id", 0)))
        except Exception:
            after_id = 0

        conn = get_message_db_connection()
        cur = conn.cursor()

        adapter_sql = ""
        params = [after_id, scene]

        if adapter:
            adapter_sql = " AND adapter = %s "
            params.append(adapter)

        if scene in ("group", "channel_group"):
            cur.execute(f"""
                WITH latest AS (
                    SELECT
                        adapter,
                        scene,
                        group_id AS target_id,
                        MAX(id) AS latest_id
                    FROM messages
                    WHERE id > %s
                      AND scene = %s
                      AND group_id IS NOT NULL
                      AND group_id != ''
                      {adapter_sql}
                    GROUP BY adapter, scene, group_id
                )
                SELECT
                    l.adapter,
                    l.scene,
                    l.target_id,
                    m.id AS last_row_id,
                    COALESCE(NULLIF(m.group_name, ''), l.target_id) AS title,
                    m.bot_id AS bot_id,
                    m.created_at AS last_time,
                    m.content AS last_content,
                    m.direction AS direction,
                    m.username AS username,
                    m.nickname AS nickname,
                    m.avatar AS avatar,
                    m.user_id AS user_id
                FROM latest l
                JOIN messages m ON m.id = l.latest_id
                ORDER BY m.id DESC
                LIMIT 300
            """, params)

        elif scene in ("private", "channel_private"):
            cur.execute(f"""
                WITH latest AS (
                    SELECT
                        adapter,
                        scene,
                        user_id AS target_id,
                        MAX(id) AS latest_id
                    FROM messages
                    WHERE id > %s
                      AND scene = %s
                      AND user_id IS NOT NULL
                      AND user_id != ''
                      {adapter_sql}
                    GROUP BY adapter, scene, user_id
                )
                SELECT
                    l.adapter,
                    l.scene,
                    l.target_id,
                    m.id AS last_row_id,
                    COALESCE(NULLIF(m.username, ''), NULLIF(m.nickname, ''), l.target_id) AS title,
                    m.bot_id AS bot_id,
                    m.created_at AS last_time,
                    m.content AS last_content,
                    m.direction AS direction,
                    m.username AS username,
                    m.nickname AS nickname,
                    m.avatar AS avatar,
                    m.user_id AS user_id
                FROM latest l
                JOIN messages m ON m.id = l.latest_id
                ORDER BY m.id DESC
                LIMIT 300
            """, params)

        else:
            return jsonify({"success": False, "error": "无效 scene"})

        rows = _prepare_session_rows([dict(r) for r in cur.fetchall()], conn)
        last_row_id = max([after_id] + [int(r.get("last_row_id") or 0) for r in rows])

        return jsonify({
            "success": True,
            "last_row_id": last_row_id,
            "rows": rows
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取会话增量失败: {e}"})

    finally:
        if conn is not None:
            conn.close()


@app.route('/api/messages/list_since')
def api_messages_list_since():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    conn = None

    try:
        scene = request.args.get("scene", "").strip()
        target_id = request.args.get("target_id", "").strip()
        adapter = request.args.get("adapter", "").strip()
        date = request.args.get("date", "").strip()
        try:
            last_row_id = int(request.args.get("last_row_id", 0))
        except Exception:
            last_row_id = 0

        if scene not in ("group", "private", "channel_group", "channel_private"):
            return jsonify({"success": False, "error": "无效 scene"})
        if not target_id:
            return jsonify({"success": False, "error": "缺少 target_id"})

        where = ["id > %s", "scene = %s"]
        params = [last_row_id, scene]

        if adapter:
            where.append("adapter = %s")
            params.append(adapter)

        if date:
            where.append(f"{db_backend.date_expression('created_at')} = %s")
            params.append(date)

        if scene in ("group", "channel_group"):
            where.append("group_id = %s")
            params.append(target_id)
        else:
            where.append("user_id = %s")
            params.append(target_id)

        conn = get_message_db_connection()
        cur = conn.cursor()

        cur.execute(f"""
            SELECT *
            FROM messages
            WHERE {' AND '.join(where)}
            ORDER BY id ASC
            LIMIT 200
        """, params)

        rows = _prepare_message_rows([dict(r) for r in cur.fetchall()])

        return jsonify({
            "success": True,
            "rows": rows,
            "last_row_id": rows[-1]["id"] if rows else last_row_id
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取增量消息失败: {e}"})
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/messages/list_before')
def api_messages_list_before():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    conn = None

    try:
        scene = request.args.get("scene", "").strip()
        target_id = request.args.get("target_id", "").strip()
        adapter = request.args.get("adapter", "").strip()
        keyword = request.args.get("keyword", "").strip()
        date = request.args.get("date", "").strip()
        try:
            before_row_id = max(0, int(request.args.get("before_row_id", 0)))
        except Exception:
            before_row_id = 0
        try:
            page_size = min(max(int(request.args.get("page_size", 300)), 50), 300)
        except Exception:
            page_size = 300

        if scene not in ("group", "private", "channel_group", "channel_private"):
            return jsonify({"success": False, "error": "无效 scene"})
        if not target_id:
            return jsonify({"success": False, "error": "缺少 target_id"})
        if before_row_id <= 0:
            return jsonify({"success": True, "rows": [], "has_more": False})

        where = ["id < %s", "scene = %s"]
        params = [before_row_id, scene]

        if adapter:
            where.append("adapter = %s")
            params.append(adapter)

        if date:
            where.append(f"{db_backend.date_expression('created_at')} = %s")
            params.append(date)

        if keyword:
            where.append("""
                (
                    content LIKE %s
                    OR username LIKE %s
                    OR nickname LIKE %s
                    OR group_name LIKE %s
                    OR group_id LIKE %s
                    OR user_id LIKE %s
                )
            """)
            kw = f"%{keyword}%"
            params.extend([kw, kw, kw, kw, kw, kw])

        if scene in ("group", "channel_group"):
            where.append("group_id = %s")
            params.append(target_id)
        else:
            where.append("user_id = %s")
            params.append(target_id)

        conn = get_message_db_connection()
        cur = conn.cursor()

        cur.execute(f"""
            SELECT *
            FROM messages
            WHERE {' AND '.join(where)}
            ORDER BY id DESC
            LIMIT %s
        """, params + [page_size + 1])

        fetched_rows = [dict(r) for r in cur.fetchall()]
        has_more = len(fetched_rows) > page_size
        rows = _prepare_message_rows(fetched_rows[:page_size])

        return jsonify({
            "success": True,
            "rows": rows,
            "has_more": has_more,
            "oldest_row_id": rows[-1]["id"] if rows else before_row_id
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取历史消息失败: {e}"})
    finally:
        if conn is not None:
            conn.close()

@app.route('/api/messages/send', methods=['POST'])
def api_messages_send():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        # 兼容 JSON 和 multipart/form-data
        if request.content_type and request.content_type.startswith("multipart/form-data"):
            data = request.form
            upload_file = request.files.get("file")
        else:
            data = request.get_json() or {}
            upload_file = None

        adapter = str(data.get("adapter", "")).strip()
        scene = str(data.get("scene", "")).strip()
        target_id = str(data.get("target_id", "")).strip()
        content = str(data.get("content", "") or "")
        send_mode = str(data.get("send_mode", "plain") or "plain").strip()
        media_type = str(data.get("media_type", "") or "").strip()
        media_url = str(data.get("media_url", "") or "").strip()
        reply_message_id = str(data.get("reply_message_id", "") or "").strip()
        quote_message_id = str(data.get("quote_message_id", "") or "").strip()
        quote_reference_id = str(data.get("quote_reference_id", "") or "").strip()
        active_send = str(data.get("active_send", "") or "").strip().lower() in ("1", "true", "yes", "on")

        if (
            quote_message_id
            and not quote_message_id.startswith("REFIDX")
            and not reply_message_id
            and not quote_reference_id
        ):
            reply_message_id = quote_message_id

        if send_mode not in ("plain", "markdown"):
            send_mode = "plain"

        if media_type and media_type not in ALLOWED_MEDIA_TYPES:
            return jsonify({"success": False, "error": "无效 media_type"})

        if not adapter:
            return jsonify({"success": False, "error": "缺少 adapter"})

        if scene not in ("group", "private", "channel_group", "channel_private"):
            return jsonify({"success": False, "error": "无效 scene"})

        if not target_id:
            return jsonify({"success": False, "error": "缺少 target_id"})

        if not content and not media_url and not upload_file:
            return jsonify({"success": False, "error": "消息不能为空"})

        bot = get_bot_by_adapter(adapter)
        if not bot:
            return jsonify({"success": False, "error": f"未找到在线 {adapter} Bot"})

        bot_id = get_bot_id(bot)

        # 处理媒体输入
        media_input = None
        saved_file_path = None

        if media_url:
            media_input = media_url
        elif upload_file:
            saved_file_path = save_uploaded_media(upload_file)
            media_input = saved_file_path

        # Markdown 模式不混媒体，有媒体时自动降级普通消息
        if send_mode == "markdown" and media_input is not None:
            send_mode = "plain"

        # =========================================================
        # OneBot V11
        # =========================================================
        if is_ob11_adapter_name(adapter):

            # -----------------------------------------------------
            # OB11 Markdown：不走 markdown 消息段，改走单节点合并转发
            # -----------------------------------------------------
            if send_mode == "markdown":
                merged_content = content or " "

                node_name = "聊天记录"
                node_uin = str(bot_id or getattr(bot, "self_id", "10000") or "10000")

                messages = [
                    {
                        "type": "node",
                        "data": {
                            "name": node_name,
                            "uin": node_uin,
                            "content": merged_content,
                        },
                    }
                ]

                if scene == "group":
                    result = run_async(
                        bot.call_api(
                            "send_group_forward_msg",
                            group_id=int(target_id),
                            messages=messages,
                        )
                    )

                    message_id = extract_result_message_id(result)

                    record_web_send_message(
                        bot,
                        scene="group",
                        message_id=message_id,
                        source_message_id="",
                        group_id=target_id,
                        user_id="",
                        message=content,
                    )

                    return jsonify({
                        "success": True,
                        "message": "Markdown 已通过合并转发发送",
                        "message_id": message_id,
                    })

                elif scene == "private":
                    result = run_async(
                        bot.call_api(
                            "send_private_forward_msg",
                            user_id=int(target_id),
                            messages=messages,
                        )
                    )

                    message_id = extract_result_message_id(result)

                    record_web_send_message(
                        bot,
                        scene="private",
                        message_id=message_id,
                        source_message_id="",
                        group_id="",
                        user_id=target_id,
                        message=content,
                    )

                    return jsonify({
                        "success": True,
                        "message": "Markdown 已通过私聊合并转发发送",
                        "message_id": message_id,
                    })

                else:
                    return jsonify({
                        "success": False,
                        "error": "OneBot V11 Markdown 合并转发暂只支持 group/private",
                    })

            # -----------------------------------------------------
            # OB11 普通消息：统一使用 call_api
            # -----------------------------------------------------
            message_obj = build_web_message_segment(
                bot,
                content=content,
                send_mode="plain",
                media_type=media_type,
                media_input=media_input,
            )

            if scene == "group":
                result = run_async(
                    bot.call_api(
                        "send_group_msg",
                        group_id=int(target_id),
                        message=message_obj,
                    )
                )

                message_id = extract_result_message_id(result)

                record_web_send_message(
                    bot,
                    scene="group",
                    message_id=message_id,
                    source_message_id="",
                    group_id=target_id,
                    user_id="",
                    message=content or f"[{media_type}]",
                )

                return jsonify({
                    "success": True,
                    "message": "发送成功",
                    "message_id": message_id,
                })

            elif scene == "private":
                result = run_async(
                    bot.call_api(
                        "send_private_msg",
                        user_id=int(target_id),
                        message=message_obj,
                    )
                )

                message_id = extract_result_message_id(result)

                record_web_send_message(
                    bot,
                    scene="private",
                    message_id=message_id,
                    source_message_id="",
                    group_id="",
                    user_id=target_id,
                    message=content or f"[{media_type}]",
                )

                return jsonify({
                    "success": True,
                    "message": "发送成功",
                    "message_id": message_id,
                })

            return jsonify({
                "success": False,
                "error": "OneBot V11 暂只支持 group/private 主动发送",
            })

        # =========================================================
        # 非 OB11：先构造消息段
        # QQ Markdown 仍然可以走 MessageSegment.markdown
        # =========================================================
        message_obj = build_web_message_segment(
            bot,
            content=content,
            send_mode=send_mode,
            media_type=media_type,
            media_input=media_input,
            quote_message_id="" if adapter == "QQ" else quote_message_id,
        )

        # =========================================================
        # QQ：主动发送 / 回复式发送
        # =========================================================
        if adapter == "QQ":
            def build_qq_message_obj(reference_id: str = ""):
                return build_web_message_segment(
                    bot,
                    content=content,
                    send_mode=send_mode,
                    media_type=media_type,
                    media_input=media_input,
                    quote_message_id=reference_id,
                )

            def resolve_qq_quote_reference_id() -> tuple[str, str]:
                if quote_reference_id:
                    ref_candidate = get_specific_reference_candidate_for_qq(
                        scene=scene,
                        target_id=target_id,
                        reference_id=quote_reference_id,
                    )
                    if ref_candidate:
                        return str(ref_candidate.get("reference_id") or quote_reference_id), ""
                    return "", "指定引用消息不可用：可能不属于当前会话，或消息记录已不存在"

                if not quote_message_id:
                    return "", ""

                if quote_message_id.startswith("REFIDX"):
                    return quote_message_id, ""

                ref_candidate = get_specific_reference_candidate_for_qq(
                    scene=scene,
                    target_id=target_id,
                    message_id=quote_message_id,
                )
                if ref_candidate:
                    ref_id = str(ref_candidate.get("reference_id") or "")
                    if ref_id:
                        return ref_id, ""
                    if scene in ("channel_group", "channel_private"):
                        return str(ref_candidate.get("message_id") or ""), ""

                if scene in ("channel_group", "channel_private"):
                    return quote_message_id, ""

                return "", ""

            message_reference_id, reference_error = resolve_qq_quote_reference_id()
            if reference_error:
                return jsonify({
                    "success": False,
                    "error": reference_error,
                })

            if active_send:
                try:
                    source_message_id = ""
                    if reply_message_id:
                        candidate = get_specific_reply_candidate_for_qq(
                            scene=scene,
                            target_id=target_id,
                            message_id=reply_message_id,
                        )
                        if not candidate:
                            return jsonify({
                                "success": False,
                                "error": "指定 msg_id 不可用：可能已过期、超过回复次数，或不属于当前会话",
                            })
                        source_message_id = str(candidate.get("message_id") or "")

                    qq_message_obj = build_qq_message_obj(message_reference_id)

                    if scene == "group":
                        send_kwargs = {
                            "group_openid": target_id,
                            "message": qq_message_obj,
                            "msg_seq": random.randint(1, 900000),
                            "msg_ref_id": message_reference_id or None,
                        }
                        if source_message_id:
                            send_kwargs["msg_id"] = source_message_id
                        result = _run_qq_send_with_optional_msg_ref(bot.send_to_group, **send_kwargs)

                        group_id = target_id
                        user_id = ""

                    elif scene == "private":
                        send_kwargs = {
                            "openid": target_id,
                            "message": qq_message_obj,
                            "msg_seq": random.randint(1, 900000),
                            "msg_ref_id": message_reference_id or None,
                        }
                        if source_message_id:
                            send_kwargs["msg_id"] = source_message_id
                        result = _run_qq_send_with_optional_msg_ref(bot.send_to_c2c, **send_kwargs)

                        group_id = ""
                        user_id = target_id

                    elif scene == "channel_group":
                        send_kwargs = {
                            "channel_id": target_id,
                            "message": qq_message_obj,
                        }
                        if source_message_id:
                            send_kwargs["msg_id"] = source_message_id
                        result = run_async(bot.send_to_channel(**send_kwargs))

                        group_id = target_id
                        user_id = ""

                    elif scene == "channel_private":
                        send_kwargs = {
                            "guild_id": target_id,
                            "message": qq_message_obj,
                        }
                        if source_message_id:
                            send_kwargs["msg_id"] = source_message_id
                        result = run_async(bot.send_to_dms(**send_kwargs))

                        group_id = ""
                        user_id = target_id

                    else:
                        return jsonify({
                            "success": False,
                            "error": "无效 QQ scene",
                        })

                    message_id = extract_result_message_id(result)
                    result_reference_id = extract_result_reference_id(result)

                    record_web_send_message(
                        bot,
                        scene=scene,
                        message_id=message_id,
                        reference_id=result_reference_id,
                        source_message_id=source_message_id,
                        group_id=group_id,
                        user_id=user_id,
                        message=content or f"[{media_type}]",
                        raw_result=result,
                    )

                    return jsonify({
                        "success": True,
                        "message": "QQ 主动发送成功",
                        "message_id": message_id,
                        "reference_id": result_reference_id,
                        "source_message_id": source_message_id,
                        "quote_reference_id": message_reference_id,
                    })

                except Exception as e:
                    logger.warning(
                        f"QQ Web 主动发送失败: scene={scene}, "
                        f"target_id={target_id}, error={e}"
                    )
                    return jsonify({
                        "success": False,
                        "error": f"QQ 主动发送失败：{e}",
                    })

            if reply_message_id:
                candidate = get_specific_reply_candidate_for_qq(
                    scene=scene,
                    target_id=target_id,
                    message_id=reply_message_id,
                )

                if not candidate:
                    return jsonify({
                        "success": False,
                        "error": "指定回复消息不可用：可能已过期、超过回复次数，或不属于当前会话",
                    })

                candidates = [candidate]

            else:
                candidates = get_latest_reply_candidates_for_qq(
                    scene=scene,
                    target_id=target_id,
                    limit=3,
                )

            if not candidates:
                return jsonify({
                    "success": False,
                    "error": "QQ 适配器无法发送：非主动发送需要 4 分钟内可用 msg_id，请选择“使用 msg_id”或开启主动发送",
                })

            last_error = ""

            for candidate in candidates:
                source_message_id = str(candidate.get("message_id", "") or "")
                if not source_message_id:
                    continue

                try:
                    source_reference_id = str(candidate.get("reference_id") or "")
                    qq_message_obj = build_qq_message_obj(message_reference_id)

                    if scene == "group":
                        result = _run_qq_send_with_optional_msg_ref(
                            bot.send_to_group,
                            group_openid=target_id,
                            message=qq_message_obj,
                            msg_id=source_message_id,
                            msg_seq=random.randint(1, 900000),
                            msg_ref_id=message_reference_id or None,
                        )

                        group_id = target_id
                        user_id = ""

                    elif scene == "private":
                        result = _run_qq_send_with_optional_msg_ref(
                            bot.send_to_c2c,
                            openid=target_id,
                            message=qq_message_obj,
                            msg_id=source_message_id,
                            msg_seq=random.randint(1, 900000),
                            msg_ref_id=message_reference_id or None,
                        )

                        group_id = ""
                        user_id = target_id

                    elif scene == "channel_group":
                        result = run_async(
                            bot.send_to_channel(
                                channel_id=target_id,
                                message=qq_message_obj,
                                msg_id=source_message_id,
                            )
                        )

                        group_id = target_id
                        user_id = ""

                    elif scene == "channel_private":
                        result = run_async(
                            bot.send_to_dms(
                                guild_id=target_id,
                                message=qq_message_obj,
                                msg_id=source_message_id,
                            )
                        )

                        group_id = ""
                        user_id = target_id

                    else:
                        return jsonify({
                            "success": False,
                            "error": "无效 QQ scene",
                        })

                    message_id = extract_result_message_id(result)
                    result_reference_id = extract_result_reference_id(result)

                    record_web_send_message(
                        bot,
                        scene=scene,
                        message_id=message_id,
                        reference_id=result_reference_id,
                        source_message_id=source_message_id,
                        group_id=group_id,
                        user_id=user_id,
                        message=content or f"[{media_type}]",
                        raw_result=result,
                    )

                    return jsonify({
                        "success": True,
                        "message": "发送成功",
                        "message_id": message_id,
                        "reference_id": result_reference_id,
                        "source_message_id": source_message_id,
                        "source_reference_id": source_reference_id,
                        "quote_reference_id": message_reference_id,
                    })

                except Exception as e:
                    last_error = str(e)
                    logger.warning(
                        f"QQ Web 发送失败，尝试下一条候选: "
                        f"scene={scene}, target_id={target_id}, "
                        f"source_message_id={source_message_id}, error={e}"
                    )
                    continue

            return jsonify({
                "success": False,
                "error": f"QQ 回复式发送失败，最后错误: {last_error}",
            })

        return jsonify({
            "success": False,
            "error": f"暂不支持适配器: {adapter}",
        })

    except Exception as e:
        logger.error(f"Web 消息发送失败: {e}")
        return jsonify({
            "success": False,
            "error": f"发送失败: {e}",
        })

@app.route('/api/messages/broadcast', methods=['POST'])
def api_messages_broadcast():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        data = request.get_json() or {}

        adapter = str(data.get("adapter", "") or "").strip()
        kind = str(data.get("kind", "") or "").strip()
        content = str(data.get("content", "") or "").strip()
        duration_minutes = str(data.get("duration_minutes", "1440") or "1440").strip()

        if not adapter:
            return jsonify({"success": False, "error": "请选择适配器"})

        if kind not in ("group", "private", "global"):
            return jsonify({"success": False, "error": "无效广播类型"})

        if not content:
            return jsonify({"success": False, "error": "广播内容不能为空"})

        try:
            duration_minutes_int = int(duration_minutes)
        except Exception:
            duration_minutes_int = 1440

        if duration_minutes_int <= 0:
            duration_minutes_int = 1440

        bot = get_bot_by_adapter(adapter)
        if not bot:
            return jsonify({"success": False, "error": f"未找到在线 {adapter} Bot"})

        message = run_async(
            start_broadcast(
                bot=bot,
                kind=kind,
                content=content,
                duration_minutes=duration_minutes_int,
            )
        )

        return jsonify({
            "success": True,
            "message": message,
        })

    except Exception as e:
        logger.error(f"Web 创建广播失败: {e}")
        return jsonify({
            "success": False,
            "error": f"创建广播失败: {e}",
        })

@app.route('/api/messages/broadcast/status')
def api_messages_broadcast_status():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        return jsonify({
            "success": True,
            "message": format_broadcast_status(),
        })

    except Exception as e:
        logger.error(f"Web 查看广播失败: {e}")
        return jsonify({
            "success": False,
            "error": f"查看广播失败: {e}",
        })

@app.route('/api/messages/revoke', methods=['POST'])
def api_messages_revoke():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        data = request.get_json() or {}

        adapter = str(data.get("adapter", "") or "").strip()
        scene = str(data.get("scene", "") or "").strip()
        message_id = str(data.get("message_id", "") or "").strip()
        group_id = str(data.get("group_id", "") or "").strip()
        user_id = str(data.get("user_id", "") or "").strip()
        row_id = str(data.get("row_id", "") or "").strip()

        if not adapter:
            return jsonify({"success": False, "error": "缺少 adapter"})
        if not scene:
            return jsonify({"success": False, "error": "缺少 scene"})
        if not message_id:
            return jsonify({"success": False, "error": "缺少 message_id"})

        bot = get_bot_by_adapter(adapter)
        if not bot:
            return jsonify({"success": False, "error": f"未找到在线 {adapter} Bot"})

        run_async(
            delete_message_compat(
                bot,
                scene=scene,
                message_id=message_id,
                group_id=group_id,
                user_id=user_id,
            )
        )

        # 撤回成功后，更新 message.db 展示内容
        try:
            conn = get_message_db_connection()
            cur = conn.cursor()

            if row_id:
                cur.execute(
                    """
                    UPDATE messages
                    SET content = %s
                    WHERE id = %s
                    """,
                    ("[该消息已撤回]", row_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE messages
                    SET content = %s
                    WHERE adapter = %s
                      AND scene = %s
                      AND message_id = %s
                    """,
                    ("[该消息已撤回]", adapter, scene, message_id),
                )

            conn.commit()
        except Exception as e:
            logger.warning(f"更新撤回消息记录失败: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

        return jsonify({
            "success": True,
            "message": "撤回成功"
        })

    except Exception as e:
        logger.error(f"Web 撤回消息失败: {e}")
        return jsonify({
            "success": False,
            "error": f"撤回失败: {e}"
        })

@app.route('/api/messages/bots')
def api_messages_bots():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        bots = []
        for bot_id, bot in get_bots().items():
            adapter = ""
            try:
                adapter = bot.adapter.get_name()
            except Exception:
                adapter = "未知"

            display_bot_id = get_bot_id(bot) or str(bot_id)

            bots.append({
                "bot_id": str(bot_id),
                "adapter": adapter,
                "nickname": get_web_bot_nickname(),
                "avatar": build_bot_avatar_url(adapter, display_bot_id, bot),
            })

        return jsonify({
            "success": True,
            "bots": bots
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        })


def is_allowed_media_proxy_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url or "").strip())
        if parsed.scheme not in ("http", "https"):
            return False

        host = (parsed.hostname or "").lower()
        if not host:
            return False

        exact_hosts = {
            "multimedia.nt.qq.com.cn",
            "q.qlogo.cn",
            "q1.qlogo.cn",
        }
        if host in exact_hosts:
            return True

        return host.endswith(".qpic.cn")
    except Exception:
        return False


def guess_image_mimetype(data: bytes, fallback: str = "") -> str:
    fallback = str(fallback or "").split(";", 1)[0].strip().lower()
    if fallback.startswith("image/"):
        return fallback

    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"

    return "application/octet-stream"


@app.route('/api/messages/media_proxy')
def api_messages_media_proxy():
    if 'admin_id' not in session:
        abort(403)

    url = str(request.args.get("url", "") or "").strip()
    if not is_allowed_media_proxy_url(url):
        abort(400)

    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                ),
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
                "Referer": "https://im.qq.com/",
            },
            timeout=15,
            allow_redirects=True,
        )
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"[web.message] 代理下载媒体失败: {url} {e}")
        abort(502)

    data = resp.content
    if len(data) > 30 * 1024 * 1024:
        abort(413)

    content_type = guess_image_mimetype(data, resp.headers.get("Content-Type", ""))
    proxy_resp = Response(data, mimetype=content_type)
    proxy_resp.headers["Cache-Control"] = "private, max-age=300"
    return proxy_resp


def get_web_bot_nickname() -> str:
    try:
        nicknames = getattr(get_driver().config, "nickname", None)

        if isinstance(nicknames, str):
            nickname = nicknames.strip()
            if nickname:
                return nickname

        for nickname in nicknames or []:
            nickname = str(nickname or "").strip()
            if nickname:
                return nickname
    except Exception:
        pass

    return "Bot"


def strip_qq_face_markup(text: str) -> str:
    text = str(text or "")

    def replace_face(match):
        ext = str(match.group("ext1") or match.group("ext2") or match.group("ext3") or "").strip()
        label = ""

        if ext:
            try:
                padded = ext + ("=" * (-len(ext) % 4))
                raw = base64.b64decode(padded).decode("utf-8", errors="ignore")
                data = json.loads(raw)
                label = str(data.get("text") or "").strip() if isinstance(data, dict) else ""
            except Exception:
                label = ""

        return f"[表情:{label}]" if label else ""

    text = re.sub(
        r"<faceType=\d+,\s*faceId=(?:\"[^\"]*\"|'[^']*'|[^,>]+),\s*ext=(?:\"(?P<ext1>[^\"]*)\"|'(?P<ext2>[^']*)'|(?P<ext3>[^>]+))>\s*",
        replace_face,
        text,
    )
    return text.replace("\ufffc", "").strip()


def normalize_logged_http_url(url: str) -> str:
    return re.sub(r"\s+", "", str(url or "").strip())


def normalize_attachment_type(media_type: str) -> str:
    media_type = str(media_type or "").strip().lower()
    if media_type in ("record", "voice"):
        return "audio"
    return media_type or "attachment"


def get_attachment_label(media_type: str) -> str:
    media_type = normalize_attachment_type(media_type)
    label_map = {
        "image": "图片消息",
        "file": "文件消息",
        "audio": "语音消息",
        "video": "视频消息",
        "attachment": "附件消息",
    }
    return label_map.get(media_type, f"{media_type}附件")


def extract_http_url_from_repr_data(data_body: str) -> str:
    data_body = str(data_body or "")

    try:
        data_obj = ast.literal_eval(data_body)
        if isinstance(data_obj, dict):
            for key in ("url", "file", "path", "src"):
                value = data_obj.get(key)
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    return normalize_logged_http_url(value)
    except Exception:
        pass

    m = re.search(
        r"['\"](?:url|file|path|src)['\"]\s*:\s*(['\"])(?P<url>https?://[\s\S]*?)\1",
        data_body,
        re.S,
    )
    if m:
        return normalize_logged_http_url(m.group("url"))

    return ""


def normalize_text_segment_repr(text: str) -> str:
    def replace_text_segment(match):
        data_body = match.group("data")
        try:
            data_obj = ast.literal_eval(data_body)
            if isinstance(data_obj, dict):
                return str(data_obj.get("text") or "")
        except Exception:
            pass

        m = re.search(
            r"['\"]text['\"]\s*:\s*(['\"])(?P<text>[\s\S]*?)\1",
            data_body,
            re.S,
        )
        return m.group("text") if m else ""

    return re.sub(
        r"Text\(\s*type=['\"]text['\"]\s*,\s*data=(?P<data>\{[\s\S]*?\})\s*\)",
        replace_text_segment,
        str(text or ""),
        flags=re.S,
    )


def normalize_attachment_repr(text: str) -> str:
    def replace_attachment(match):
        media_type = normalize_attachment_type(match.group("type"))
        url = extract_http_url_from_repr_data(match.group("data"))
        if not url:
            return ""
        return f"<attachment[{media_type}]:{url}>"

    s = re.sub(
        r"Attachment\(\s*type=['\"](?P<type>[^'\"]+)['\"]\s*,\s*data=(?P<data>\{[\s\S]*?\})\s*\)",
        replace_attachment,
        str(text or ""),
        flags=re.S,
    )

    def replace_labeled_attachment(match):
        label = match.group("label")
        url = normalize_logged_http_url(match.group("url"))
        type_map = {
            "图片消息": "image",
            "语音消息": "audio",
            "视频消息": "video",
            "文件消息": "file",
            "附件消息": "attachment",
        }
        return f"<attachment[{type_map.get(label, 'attachment')}]:{url}>"

    return re.sub(
        r"\[(?P<label>图片消息|语音消息|视频消息|文件消息|附件消息)\]\s*(?P<url>https?://[^\s<>'\"]+)",
        replace_labeled_attachment,
        s,
    )


def cleanup_message_segment_repr(text: str) -> str:
    s = str(text or "").strip()

    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1].strip()

    s = re.sub(r"^\s*,\s*", "", s)
    s = re.sub(r"\s*,\s*$", "", s)
    s = re.sub(r"\s*,\s*(?=<attachment\[)", "\n", s)
    s = re.sub(r"(<attachment\[[^\]]+\]:https?://[^>]+>)\s*,\s*", r"\1\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def normalize_message_display_content(text: str) -> str:
    raw = str(text or "")
    had_segment_repr = bool(re.search(r"\b(?:Text|Attachment)\(", raw))

    display = normalize_text_segment_repr(raw)
    display = normalize_attachment_repr(display)
    display = strip_qq_face_markup(display)

    if had_segment_repr:
        display = cleanup_message_segment_repr(display)

    return display


def attachment_tokens_to_preview(text: str) -> str:
    return re.sub(
        r"<attachment\[([^\]]+)\]:https?://[^>]+>",
        lambda m: f"[{get_attachment_label(m.group(1))}]",
        str(text or ""),
    )


def is_qq_adapter_name(adapter: str) -> bool:
    return str(adapter or "").strip().lower() == "qq"


def q_number_avatar_url(qq: str) -> str:
    qq = str(qq or "").strip()
    if not qq:
        return ""

    return f"https://q1.qlogo.cn/g?b=qq&nk={quote(qq, safe='')}&s=640"


def qq_openid_avatar_url(appid: str, openid: str) -> str:
    appid = str(appid or "").strip()
    openid = str(openid or "").strip()
    if not appid or not openid:
        return ""

    return f"https://q.qlogo.cn/qqapp/{quote(appid, safe='')}/{quote(openid, safe='')}/0"


def is_http_url(value: str) -> bool:
    value = str(value or "").strip()
    return value.startswith("http://") or value.startswith("https://")


def get_bot_for_message_row(row: dict):
    row_bot_id = str(row.get("bot_id") or "").strip()

    if row_bot_id:
        for key, bot in get_bots().items():
            try:
                if str(key) == row_bot_id or str(get_bot_id(bot) or "") == row_bot_id:
                    return bot
            except Exception:
                continue

    return get_bot_by_adapter(str(row.get("adapter") or ""))


def get_configured_qq_appid() -> str:
    try:
        qq_bots = getattr(get_driver().config, "qq_bots", None)

        if isinstance(qq_bots, str):
            try:
                qq_bots = json.loads(qq_bots)
            except Exception:
                qq_bots = []

        if isinstance(qq_bots, dict):
            qq_bots = [qq_bots]

        for item in qq_bots or []:
            if isinstance(item, dict):
                value = item.get("id") or item.get("appid") or item.get("app_id")
            else:
                value = getattr(item, "id", None) or getattr(item, "appid", None) or getattr(item, "app_id", None)

            if value:
                return str(value)
    except Exception:
        pass

    return ""


def get_qq_appid(row: dict, bot=None) -> str:
    for value in (
        row.get("bot_id"),
        get_bot_id(bot) if bot is not None else "",
        get_configured_qq_appid(),
    ):
        value = str(value or "").strip()
        if value:
            return value

    return ""


def get_qq_bot_uin(bot=None) -> str:
    candidates = []

    for obj in (
        bot,
        getattr(bot, "self", None) if bot is not None else None,
        getattr(bot, "bot_info", None) if bot is not None else None,
        getattr(bot, "self_info", None) if bot is not None else None,
    ):
        if obj is None:
            continue

        for attr in ("bot_uin", "uin", "qq", "qq_number"):
            candidates.append(getattr(obj, attr, None))

    try:
        candidates.append(getattr(XiuConfig(), "bot_uin", None))
    except Exception:
        pass

    try:
        candidates.append(getattr(get_driver().config, "bot_uin", None))
    except Exception:
        pass

    for value in candidates:
        value = str(value or "").strip()
        if value and value != "0":
            return value

    return ""


def build_user_avatar_url(adapter: str, bot_id: str, user_id: str, existing: str = "", bot=None) -> str:
    existing = str(existing or "").strip()
    if is_http_url(existing):
        return existing

    user_id = str(user_id or "").strip()
    if not user_id:
        return ""

    if is_qq_adapter_name(adapter):
        appid = get_qq_appid({"bot_id": bot_id}, bot)
        return qq_openid_avatar_url(appid, user_id)

    if is_ob11_adapter_name(adapter):
        return q_number_avatar_url(user_id)

    return ""


def build_bot_avatar_url(adapter: str, bot_id: str = "", bot=None) -> str:
    if is_qq_adapter_name(adapter):
        bot_uin = get_qq_bot_uin(bot)
        return q_number_avatar_url(bot_uin)

    if is_ob11_adapter_name(adapter):
        return q_number_avatar_url(bot_id or (get_bot_id(bot) if bot is not None else ""))

    return ""


def is_placeholder_user_name(name, user_id: str = "") -> bool:
    name = str(name or "").strip()
    user_id = str(user_id or "").strip()

    if not name:
        return True

    if name.lower() == "bot":
        return True

    return bool(user_id and name == user_id)


def pick_human_display_name(username, nickname="", user_id: str = "") -> str:
    for value in (username, nickname):
        value = str(value or "").strip()
        if not is_placeholder_user_name(value, user_id):
            return value

    return ""


def fill_message_display_profiles(rows: list[dict]) -> list[dict]:
    if not rows:
        return rows

    bot_nickname = get_web_bot_nickname()

    for r in rows:
        adapter = str(r.get("adapter") or "")
        bot_id = str(r.get("bot_id") or "")

        if r.get("direction") == "send":
            bot = get_bot_for_message_row(r)
            display_name = bot_nickname or r.get("username") or r.get("nickname") or "Bot"
            r["username"] = display_name
            r["nickname"] = display_name
            r["avatar"] = build_bot_avatar_url(adapter, bot_id, bot) or str(r.get("avatar") or "")
        else:
            r["avatar"] = build_user_avatar_url(
                adapter,
                bot_id,
                str(r.get("user_id") or ""),
                str(r.get("avatar") or ""),
            )

    return rows


def fill_session_display_profiles(rows: list[dict]) -> list[dict]:
    if not rows:
        return rows

    conn = None
    try:
        for r in rows:
            scene = str(r.get("scene") or "")
            if scene not in ("private", "channel_private"):
                continue

            adapter = str(r.get("adapter") or "")
            bot_id = str(r.get("bot_id") or "")
            target_id = str(r.get("target_id") or "")

            title = str(r.get("title") or "").strip()
            username = str(r.get("username") or "").strip()
            nickname = str(r.get("nickname") or "").strip()

            if (
                is_placeholder_user_name(title, target_id)
                or is_placeholder_user_name(username, target_id)
                or is_placeholder_user_name(nickname, target_id)
            ):
                if conn is None:
                    conn = get_message_db_connection()
                human_name = get_latest_human_name_by_user_id(conn, target_id)
                if human_name:
                    if is_placeholder_user_name(title, target_id):
                        r["title"] = human_name
                    if is_placeholder_user_name(username, target_id):
                        r["username"] = human_name
                    if is_placeholder_user_name(nickname, target_id):
                        r["nickname"] = human_name

            r["avatar"] = build_user_avatar_url(
                adapter,
                bot_id,
                target_id,
                str(r.get("avatar") or ""),
            )
    finally:
        if conn is not None:
            conn.close()

    return rows

@app.route('/api/messages/markdown_preview', methods=['POST'])
def api_messages_markdown_preview():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        data = request.get_json() or {}
        text = str(data.get("text", "") or "")

        try:
            import markdown

            html = markdown.markdown(
                text,
                extensions=[
                    "extra",
                    "tables",
                    "fenced_code",
                    "nl2br"
                ]
            )

        except Exception:
            import html as html_lib
            html = "<pre>" + html_lib.escape(text) + "</pre>"

        return jsonify({
            "success": True,
            "html": html
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Markdown 预览失败: {e}"
        })


def fill_private_username_from_group(rows: list[dict]) -> list[dict]:
    """
    私聊消息 username 为空时：
    1. 优先从 user_nicknames 表读取缓存昵称
    2. 找不到再从同 user_id 的群聊记录中找最近 username
    3. 仍找不到则回退 nickname/user_id
    """
    if not rows:
        return rows

    user_ids = {
        str(r.get("user_id") or "")
        for r in rows
        if r.get("scene") in ("private", "channel_private")
        and str(r.get("user_id") or "")
    }

    if not user_ids:
        return rows

    conn = get_message_db_connection()
    try:
        cur = conn.cursor()
        name_map = {}

        # 1. 优先读取昵称缓存表
        for uid in user_ids:
            cur.execute(
                """
                SELECT username
                FROM user_nicknames
                WHERE user_id = %s
                LIMIT 1
                """,
                (uid,),
            )
            row = cur.fetchone()
            if row and pick_human_display_name(row["username"], user_id=uid):
                name_map[uid] = row["username"]

        # 2. 没缓存的，再从群聊消息中找
        for uid in user_ids:
            if uid in name_map:
                continue

            cur.execute("""
                SELECT username, nickname
                FROM messages
                WHERE user_id = %s
                  AND scene IN ('group', 'channel_group')
                  AND direction = 'recv'
                  AND (
                    (username IS NOT NULL AND username != '' AND username != %s AND username != 'Bot')
                    OR (nickname IS NOT NULL AND nickname != '' AND nickname != %s AND nickname != 'Bot')
                  )
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (uid, uid, uid))
            row = cur.fetchone()
            if row:
                name = pick_human_display_name(row["username"], row["nickname"], uid)
                if name:
                    name_map[uid] = name

        for r in rows:
            if r.get("scene") in ("private", "channel_private"):
                uid = str(r.get("user_id") or "")
                fallback_name = name_map.get(uid) or r.get("nickname") or uid
                if is_placeholder_user_name(r.get("username"), uid):
                    r["username"] = fallback_name
                if is_placeholder_user_name(r.get("nickname"), uid):
                    r["nickname"] = r["username"]

        return rows

    finally:
        conn.close()


def extract_markdown_content_from_repr(text: str) -> tuple[str, str]:
    """
    提取 Markdown 消息内容。

    支持：
    1. QQ 适配器格式：
       <markdown:MessageMarkdown(..., content='xxx')>

    2. OneBot V11 CQ markdown 格式：
       [CQ:markdown,data={'markdown': {'content': 'xxx'}}]
       [CQ:markdown,data={"markdown": {"content": "xxx"}}]

    返回:
    - display_content
    - content_format: plain / markdown
    """
    if not text:
        return "", "plain"

    s = str(text)

    # =========================================================
    # 1) OneBot V11 CQ Markdown:
    #    [CQ:markdown,data={'markdown': {'content': '666'}}]
    # =========================================================
    if "[CQ:markdown" in s:
        try:
            # 提取 data= 后面的内容
            m = re.search(
                r"\[CQ:markdown\s*,\s*data=(?P<data>.+?)\]",
                s,
                re.S,
            )

            if m:
                raw_data = m.group("data").strip()

                # 有些实现可能会在结尾多带空格
                raw_data = raw_data.rstrip()

                # 尝试 Python 字面量解析：支持单引号 dict
                try:
                    data_obj = ast.literal_eval(raw_data)
                except Exception:
                    # 再尝试 JSON：支持双引号 dict
                    data_obj = json.loads(raw_data)

                if isinstance(data_obj, dict):
                    markdown_obj = data_obj.get("markdown") or {}

                    if isinstance(markdown_obj, dict):
                        content = markdown_obj.get("content")

                        if content is not None:
                            return str(content), "markdown"

                    # 兼容某些实现直接 data={'content':'xxx'}
                    content = data_obj.get("content")
                    if content is not None:
                        return str(content), "markdown"

                # 如果解析不到 content，但确认是 markdown，仍标记 markdown
                return s, "markdown"

        except Exception:
            # fallback：正则硬提 content
            try:
                m2 = re.search(
                    r"['\"]content['\"]\s*:\s*(?P<val>'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\")",
                    s,
                    re.S,
                )
                if m2:
                    raw_literal = m2.group("val")
                    content = ast.literal_eval(raw_literal)
                    return str(content), "markdown"
            except Exception:
                pass

            return s, "markdown"

    # =========================================================
    # 2) QQ Markdown:
    #    <markdown:MessageMarkdown(..., content='xxx')>
    # =========================================================
    if "<markdown:MessageMarkdown" not in s:
        return s, "plain"

    m = re.search(
        r"content=(?P<val>'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\")",
        s,
        re.S,
    )

    if not m:
        return s, "markdown"

    raw_literal = m.group("val")

    try:
        content = ast.literal_eval(raw_literal)
        return "" if content is None else str(content), "markdown"
    except Exception:
        # fallback：去掉外层引号，手动反转义
        quote = raw_literal[0]
        body = raw_literal[1:-1]

        body = body.replace("\\\\", "\0BACKSLASH\0")
        body = body.replace("\\r", "\r")
        body = body.replace("\\n", "\n")
        body = body.replace("\\t", "\t")

        if quote == "'":
            body = body.replace("\\'", "'")
        else:
            body = body.replace('\\"', '"')

        body = body.replace("\0BACKSLASH\0", "\\")

        return body, "markdown"

def get_latest_human_name_by_user_id(conn, user_id: str) -> str:
    """
    获取某个 user_id 最近一次可用的人类昵称：
    优先取群聊/频道群聊中的昵称，其次取私聊中的昵称。
    排除 Bot。
    """
    if not user_id:
        return ""

    cur = conn.cursor()
    # 优先从昵称缓存表查
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
    if row:
        name = pick_human_display_name(row["username"], user_id=user_id)
        if name:
            return name

    # 从群聊消息里找昵称
    cur.execute("""
        SELECT
            username,
            nickname
        FROM messages
        WHERE user_id = %s
          AND direction = 'recv'
          AND scene IN ('group', 'channel_group')
          AND (
                (username IS NOT NULL AND username != '' AND username != %s AND username != 'Bot')
             OR (nickname IS NOT NULL AND nickname != '' AND nickname != %s AND nickname != 'Bot')
          )
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """, (str(user_id), str(user_id), str(user_id)))
    row = cur.fetchone()
    if row:
        name = pick_human_display_name(row["username"], row["nickname"], user_id)
        if name:
            return name

    # 再从私聊消息里找
    cur.execute("""
        SELECT
            username,
            nickname
        FROM messages
        WHERE user_id = %s
          AND direction = 'recv'
          AND scene IN ('private', 'channel_private')
          AND (
                (username IS NOT NULL AND username != '' AND username != %s AND username != 'Bot')
             OR (nickname IS NOT NULL AND nickname != '' AND nickname != %s AND nickname != 'Bot')
          )
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """, (str(user_id), str(user_id), str(user_id)))
    row = cur.fetchone()
    if row:
        name = pick_human_display_name(row["username"], row["nickname"], user_id)
        if name:
            return name

    return ""


def build_session_preview(row: dict) -> str:
    raw = row.get("content") or ""
    display_content, _ = extract_markdown_content_from_repr(raw)

    text = normalize_message_display_content(display_content)
    text = attachment_tokens_to_preview(text).replace("\r", " ").replace("\n", " ").strip()
    if not text:
        text = "[空消息]"
    if len(text) > 60:
        text = text[:60] + "..."

    scene = row.get("scene", "")
    direction = row.get("direction", "")

    # Bot 自己发的，不加前缀
    if direction == "send":
        return text

    if scene in ("group", "channel_group"):
        sender = row.get("username") or row.get("nickname") or row.get("user_id") or "未知用户"
        if sender and str(sender).strip().lower() != "bot":
            return f"{sender}：{text}"

    return text
