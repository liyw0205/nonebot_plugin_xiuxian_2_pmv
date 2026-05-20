from .core import *  # noqa: F401,F403
from ..broadcast_manager import format_broadcast_status, start_broadcast

@app.route('/messages')
def messages_page():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('messages.html')

@app.route('/api/messages/list')
def api_messages_list():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

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
        offset = (page - 1) * page_size

        where = []
        params = []

        if scene and scene != "ALL":
            where.append("scene = ?")
            params.append(scene)

        if direction and direction != "ALL":
            where.append("direction = ?")
            params.append(direction)

        if keyword:
            where.append("""
                (
                    content LIKE ?
                    OR username LIKE ?
                    OR nickname LIKE ?
                    OR group_name LIKE ?
                    OR group_id LIKE ?
                    OR user_id LIKE ?
                )
            """)
            kw = f"%{keyword}%"
            params.extend([kw, kw, kw, kw, kw, kw])

        if group_id:
            where.append("group_id = ?")
            params.append(group_id)

        if user_id:
            where.append("user_id = ?")
            params.append(user_id)

        if adapter:
            where.append("adapter = ?")
            params.append(adapter)

        if start:
            where.append("created_at >= ?")
            params.append(start.replace("T", " "))

        if end:
            where.append("created_at <= ?")
            params.append(end.replace("T", " "))
        
        if date:
            where.append("date(created_at) = ?")
            params.append(date)

        where_sql = " WHERE " + " AND ".join(where) if where else ""

        conn = get_message_db_connection()
        cur = conn.cursor()

        cur.execute(f"SELECT COUNT(*) AS c FROM messages {where_sql}", params)
        total = cur.fetchone()["c"]

        cur.execute(f"""
            SELECT *
            FROM messages
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
        """, params + [page_size, offset])

        rows = [dict(r) for r in cur.fetchall()]
        rows = fill_private_username_from_group(rows)
        
        for r in rows:
            raw_content = r.get("content") or ""
            display_content, content_format = extract_markdown_content_from_repr(raw_content)
        
            r["display_content"] = display_content
            r["content_format"] = content_format
            r["can_revoke"] = bool(
                r.get("direction") == "send"
                and r.get("message_id")
            )
        
            # 前端不需要展示 msg_id，但需要用来点击回复
            r["can_reply"] = False
            r["reply_expired"] = True
        
            if r.get("adapter") == "QQ" and r.get("direction") == "recv" and r.get("message_id"):
                valid_seconds = get_qq_reply_valid_seconds(r.get("scene", ""))
                can_time = is_message_within_seconds(r.get("created_at", ""), valid_seconds)
                can_count = int(r.get("reply_used_count") or 0) < 5
        
                r["can_reply"] = bool(can_time and can_count)
                r["reply_expired"] = not can_time

        for r in rows:
            group_title = r.get("group_name") or r.get("group_id") or ""
            user_title = r.get("username") or r.get("nickname") or r.get("user_id") or ""

            r["group_avatar_text"] = group_title[:1] if group_title else "群"
            r["user_avatar_text"] = user_title[:1] if user_title else "人"

        return jsonify({
            "success": True,
            "total": total,
            "page": page,
            "page_size": page_size,
            "rows": rows
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取消息列表失败: {e}"})

    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.route('/api/messages/dates')
def api_messages_dates():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        scene = request.args.get("scene", "").strip()
        adapter = request.args.get("adapter", "").strip()
        target_id = request.args.get("target_id", "").strip()

        if scene not in ("group", "private", "channel_group", "channel_private"):
            return jsonify({"success": False, "error": "无效 scene"})

        if not target_id:
            return jsonify({"success": False, "error": "缺少 target_id"})

        where = ["scene = ?"]
        params = [scene]

        if adapter:
            where.append("adapter = ?")
            params.append(adapter)

        if scene in ("group", "channel_group"):
            where.append("group_id = ?")
            params.append(target_id)
        else:
            where.append("user_id = ?")
            params.append(target_id)

        conn = get_message_db_connection()
        cur = conn.cursor()

        cur.execute(f"""
            SELECT date(created_at) AS d, COUNT(*) AS c
            FROM messages
            WHERE {' AND '.join(where)}
            GROUP BY date(created_at)
            ORDER BY d DESC
            LIMIT 60
        """, params)

        today = datetime.now().strftime("%Y-%m-%d")
        rows = []

        for r in cur.fetchall():
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
        try:
            conn.close()
        except Exception:
            pass

@app.route('/api/messages/sessions')
def api_messages_sessions():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    try:
        scene = request.args.get("scene", "group").strip()
        adapter = request.args.get("adapter", "").strip()

        conn = get_message_db_connection()
        cur = conn.cursor()

        adapter_sql = ""
        params = [scene]

        if adapter:
            adapter_sql = " AND adapter = ? "
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
                    WHERE scene = ?
                      AND group_id IS NOT NULL
                      AND group_id != ''
                      {adapter_sql}
                    GROUP BY adapter, scene, group_id
                )
                SELECT
                    l.adapter,
                    l.scene,
                    l.target_id,
                    COALESCE(NULLIF(m.group_name, ''), l.target_id) AS title,
                    m.created_at AS last_time,
                    m.content AS last_content,
                    m.direction AS direction,
                    m.username AS username,
                    m.nickname AS nickname,
                    m.user_id AS user_id,
                    (
                        SELECT COUNT(*)
                        FROM messages x
                        WHERE x.adapter = l.adapter
                          AND x.scene = l.scene
                          AND x.group_id = l.target_id
                    ) AS msg_count
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
                    WHERE scene = ?
                      AND user_id IS NOT NULL
                      AND user_id != ''
                      {adapter_sql}
                    GROUP BY adapter, scene, user_id
                )
                SELECT
                    l.adapter,
                    l.scene,
                    l.target_id,
                    COALESCE(NULLIF(m.username, ''), NULLIF(m.nickname, ''), l.target_id) AS title,
                    m.created_at AS last_time,
                    m.content AS last_content,
                    m.direction AS direction,
                    m.username AS username,
                    m.nickname AS nickname,
                    m.user_id AS user_id,
                    (
                        SELECT COUNT(*)
                        FROM messages x
                        WHERE x.adapter = l.adapter
                          AND x.scene = l.scene
                          AND x.user_id = l.target_id
                    ) AS msg_count
                FROM latest l
                JOIN messages m ON m.id = l.latest_id
                ORDER BY m.created_at DESC, m.id DESC
                LIMIT 300
            """, params)

        else:
            return jsonify({"success": False, "error": "无效 scene"})

        rows = [dict(r) for r in cur.fetchall()]

        for r in rows:
            # 修复私聊标题显示成 Bot 的问题
            if r.get("scene") in ("private", "channel_private"):
                title = str(r.get("title") or "").strip()
                if not title or title.lower() == "bot":
                    human_name = get_latest_human_name_by_user_id(conn, str(r.get("target_id") or ""))
                    r["title"] = human_name or str(r.get("target_id") or "未知会话")

            # 生成预览文本（支持 markdown 提取）
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

        return jsonify({
            "success": True,
            "rows": rows
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取会话失败: {e}"})

    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.route('/api/messages/list_since')
def api_messages_list_since():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

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

        where = ["id > ?", "scene = ?"]
        params = [last_row_id, scene]

        if adapter:
            where.append("adapter = ?")
            params.append(adapter)

        if date:
            where.append("date(created_at) = ?")
            params.append(date)

        if scene in ("group", "channel_group"):
            where.append("group_id = ?")
            params.append(target_id)
        else:
            where.append("user_id = ?")
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

        rows = [dict(r) for r in cur.fetchall()]
        rows = fill_private_username_from_group(rows)

        for r in rows:
            raw_content = r.get("content") or ""
            display_content, content_format = extract_markdown_content_from_repr(raw_content)

            r["display_content"] = display_content
            r["content_format"] = content_format
            r["can_revoke"] = bool(
                r.get("direction") == "send"
                and r.get("message_id")
            )

            r["can_reply"] = False
            r["reply_expired"] = True

            if r.get("adapter") == "QQ" and r.get("direction") == "recv" and r.get("message_id"):
                valid_seconds = get_qq_reply_valid_seconds(r.get("scene", ""))
                can_time = is_message_within_seconds(r.get("created_at", ""), valid_seconds)
                can_count = int(r.get("reply_used_count") or 0) < 5
                r["can_reply"] = bool(can_time and can_count)
                r["reply_expired"] = not can_time

        return jsonify({
            "success": True,
            "rows": rows,
            "last_row_id": rows[-1]["id"] if rows else last_row_id
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"获取增量消息失败: {e}"})
    finally:
        try:
            conn.close()
        except Exception:
            pass

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
        active_send = str(data.get("active_send", "") or "").strip().lower() in ("1", "true", "yes", "on")

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
        )

        # =========================================================
        # QQ：主动发送 / 回复式发送
        # =========================================================
        if adapter == "QQ":
            if active_send:
                try:
                    if scene == "group":
                        result = run_async(
                            bot.send_to_group(
                                group_openid=target_id,
                                message=message_obj,
                                msg_seq=random.randint(1, 900000),
                            )
                        )

                        group_id = target_id
                        user_id = ""

                    elif scene == "private":
                        result = run_async(
                            bot.send_to_c2c(
                                openid=target_id,
                                message=message_obj,
                                msg_seq=random.randint(1, 900000),
                            )
                        )

                        group_id = ""
                        user_id = target_id

                    elif scene == "channel_group":
                        result = run_async(
                            bot.send_to_channel(
                                channel_id=target_id,
                                message=message_obj,
                            )
                        )

                        group_id = target_id
                        user_id = ""

                    elif scene == "channel_private":
                        result = run_async(
                            bot.send_to_dms(
                                guild_id=target_id,
                                message=message_obj,
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

                    record_web_send_message(
                        bot,
                        scene=scene,
                        message_id=message_id,
                        source_message_id="",
                        group_id=group_id,
                        user_id=user_id,
                        message=content or f"[{media_type}]",
                    )

                    return jsonify({
                        "success": True,
                        "message": "QQ 主动发送成功",
                        "message_id": message_id,
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
                    "error": "QQ 适配器无法主动发送：未找到 4 分钟内可回复且未达回复次数上限的消息",
                })

            last_error = ""

            for candidate in candidates:
                source_message_id = str(candidate.get("message_id", "") or "")
                if not source_message_id:
                    continue

                try:
                    if scene == "group":
                        result = run_async(
                            bot.send_to_group(
                                group_openid=target_id,
                                message=message_obj,
                                msg_id=source_message_id,
                                msg_seq=random.randint(1, 900000),
                            )
                        )

                        group_id = target_id
                        user_id = ""

                    elif scene == "private":
                        result = run_async(
                            bot.send_to_c2c(
                                openid=target_id,
                                message=message_obj,
                                msg_id=source_message_id,
                                msg_seq=random.randint(1, 900000),
                            )
                        )

                        group_id = ""
                        user_id = target_id

                    elif scene == "channel_group":
                        result = run_async(
                            bot.send_to_channel(
                                channel_id=target_id,
                                message=message_obj,
                                msg_id=source_message_id,
                            )
                        )

                        group_id = target_id
                        user_id = ""

                    elif scene == "channel_private":
                        result = run_async(
                            bot.send_to_dms(
                                guild_id=target_id,
                                message=message_obj,
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

                    record_web_send_message(
                        bot,
                        scene=scene,
                        message_id=message_id,
                        source_message_id=source_message_id,
                        group_id=group_id,
                        user_id=user_id,
                        message=content or f"[{media_type}]",
                    )

                    return jsonify({
                        "success": True,
                        "message": "发送成功",
                        "message_id": message_id,
                        "source_message_id": source_message_id,
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
                    SET content = ?
                    WHERE id = ?
                    """,
                    ("[该消息已撤回]", row_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE messages
                    SET content = ?
                    WHERE adapter = ?
                      AND scene = ?
                      AND message_id = ?
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

            bots.append({
                "bot_id": str(bot_id),
                "adapter": adapter
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
                WHERE user_id = ?
                LIMIT 1
                """,
                (uid,),
            )
            row = cur.fetchone()
            if row and row["username"]:
                name_map[uid] = row["username"]

        # 2. 没缓存的，再从群聊消息中找
        for uid in user_ids:
            if uid in name_map:
                continue

            cur.execute("""
                SELECT username, nickname
                FROM messages
                WHERE user_id = ?
                  AND scene IN ('group', 'channel_group')
                  AND direction = 'recv'
                  AND (
                    (username IS NOT NULL AND username != '')
                    OR (nickname IS NOT NULL AND nickname != '')
                  )
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            """, (uid,))
            row = cur.fetchone()
            if row:
                name_map[uid] = row["username"] or row["nickname"] or uid

        for r in rows:
            if r.get("scene") in ("private", "channel_private"):
                uid = str(r.get("user_id") or "")
                if not r.get("username"):
                    r["username"] = name_map.get(uid) or r.get("nickname") or uid
                if not r.get("nickname"):
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
        WHERE user_id = ?
        LIMIT 1
        """,
        (str(user_id),),
    )
    row = cur.fetchone()
    if row and row["username"]:
        return str(row["username"])

    # 从群聊消息里找昵称
    cur.execute("""
        SELECT
            COALESCE(NULLIF(username, ''), NULLIF(nickname, '')) AS name
        FROM messages
        WHERE user_id = ?
          AND direction = 'recv'
          AND scene IN ('group', 'channel_group')
          AND (
                (username IS NOT NULL AND username != '' AND username != 'Bot')
             OR (nickname IS NOT NULL AND nickname != '' AND nickname != 'Bot')
          )
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """, (str(user_id),))
    row = cur.fetchone()
    if row and row["name"]:
        return str(row["name"])

    # 再从私聊消息里找
    cur.execute("""
        SELECT
            COALESCE(NULLIF(username, ''), NULLIF(nickname, '')) AS name
        FROM messages
        WHERE user_id = ?
          AND direction = 'recv'
          AND scene IN ('private', 'channel_private')
          AND (
                (username IS NOT NULL AND username != '' AND username != 'Bot')
             OR (nickname IS NOT NULL AND nickname != '' AND nickname != 'Bot')
          )
        ORDER BY created_at DESC, id DESC
        LIMIT 1
    """, (str(user_id),))
    row = cur.fetchone()
    if row and row["name"]:
        return str(row["name"])

    return ""


def build_session_preview(row: dict) -> str:
    raw = row.get("content") or ""
    display_content, _ = extract_markdown_content_from_repr(raw)

    text = str(display_content or "").replace("\r", " ").replace("\n", " ").strip()
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
