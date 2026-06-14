from .core import *  # noqa: F401,F403
from .messages import (  # noqa: E402
    _prepare_message_rows as _prepare_web_message_rows,
    build_user_avatar_url,
    get_latest_human_name_by_user_id,
    pick_human_display_name,
)

@app.route('/logs')
def logs():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    return render_template('logs.html')


def _safe_int(value, default: int, minimum: int, maximum: int) -> int:
    try:
        return min(maximum, max(minimum, int(value)))
    except Exception:
        return default


def _get_xiuxian_user_candidates(query: str, limit: int) -> list[dict]:
    """
    从修仙主库按道号/ID 搜索用户。
    """
    conn = None
    try:
        conn = get_db_connection(DATABASE)
        if not conn.table_exists("user_xiuxian"):
            return []

        cur = conn.cursor()
        if query:
            like = f"%{query}%"
            cur.execute(
                """
                SELECT user_id, user_name, level, root_type
                FROM user_xiuxian
                WHERE CAST(user_id AS TEXT) = %s
                   OR CAST(user_id AS TEXT) LIKE %s
                   OR COALESCE(user_name, '') LIKE %s
                ORDER BY
                    CASE
                        WHEN CAST(user_id AS TEXT) = %s THEN 0
                        WHEN user_name = %s THEN 1
                        ELSE 2
                    END,
                    user_name ASC,
                    user_id ASC
                LIMIT %s
                """,
                (query, like, like, query, query, limit),
            )
        else:
            cur.execute(
                """
                SELECT user_id, user_name, level, root_type
                FROM user_xiuxian
                WHERE user_id IS NOT NULL
                ORDER BY user_id ASC
                LIMIT %s
                """,
                (limit,),
            )

        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _get_message_user_candidates(query: str, limit: int) -> list[dict]:
    """
    从 message.db 搜索出现过消息记录的用户，用于补齐没有修仙档案的会话用户。
    """
    conn = None
    try:
        conn = get_message_db_connection()
        cur = conn.cursor()

        where = ["user_id IS NOT NULL", "user_id != ''"]
        params = []
        if query:
            like = f"%{query}%"
            where.append(
                """
                (
                    user_id = %s
                    OR user_id LIKE %s
                    OR username LIKE %s
                    OR nickname LIKE %s
                )
                """
            )
            params.extend([query, like, like, like])

        cur.execute(
            f"""
            SELECT user_id, MAX(id) AS last_row_id, MAX(created_at) AS last_time
            FROM messages
            WHERE {' AND '.join(where)}
            GROUP BY user_id
            ORDER BY last_row_id DESC
            LIMIT %s
            """,
            params + [limit],
        )
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []
    finally:
        if conn is not None:
            conn.close()


def _get_log_user_message_summary(conn, user_id: str) -> dict:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COUNT(*) AS message_count,
            SUM(CASE WHEN direction = 'recv' THEN 1 ELSE 0 END) AS recv_count,
            SUM(CASE WHEN direction = 'send' THEN 1 ELSE 0 END) AS send_count,
            MAX(created_at) AS last_time,
            MAX(id) AS last_row_id
        FROM messages
        WHERE user_id = %s
           OR (
                direction = 'send'
                AND COALESCE(source_message_id, '') != ''
                AND source_message_id IN (
                    SELECT message_id
                    FROM messages
                    WHERE user_id = %s
                      AND direction = 'recv'
                      AND COALESCE(message_id, '') != ''
                )
           )
        """,
        (user_id, user_id),
    )
    row = cur.fetchone()
    return dict(row) if row else {}


def _get_log_user_latest_profile(conn, user_id: str) -> dict:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT adapter, bot_id, user_id, username, nickname, avatar
        FROM messages
        WHERE user_id = %s
          AND direction = 'recv'
        ORDER BY id DESC
        LIMIT 1
        """,
        (user_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else {}


def _build_log_user_row(base: dict, conn) -> dict:
    user_id = str(base.get("user_id") or "").strip()
    user_name = str(base.get("user_name") or "").strip()
    profile = _get_log_user_latest_profile(conn, user_id) if user_id else {}
    summary = _get_log_user_message_summary(conn, user_id) if user_id else {}

    human_name = ""
    if user_id:
        human_name = get_latest_human_name_by_user_id(conn, user_id)
    if not human_name:
        human_name = pick_human_display_name(
            profile.get("username"),
            profile.get("nickname"),
            user_id,
        )

    title = user_name or human_name or user_id or "未知用户"
    subtitle_bits = []
    if user_name:
        subtitle_bits.append(f"道号: {user_name}")
    if human_name and human_name != user_name:
        subtitle_bits.append(f"昵称: {human_name}")
    if user_id:
        subtitle_bits.append(f"ID: {user_id}")

    adapter = str(profile.get("adapter") or "")
    bot_id = str(profile.get("bot_id") or "")
    avatar = build_user_avatar_url(
        adapter,
        bot_id,
        user_id,
        str(profile.get("avatar") or ""),
    )

    return {
        "user_id": user_id,
        "user_name": user_name,
        "title": title,
        "subtitle": " ｜ ".join(subtitle_bits),
        "level": base.get("level") or "",
        "root_type": base.get("root_type") or "",
        "adapter": adapter,
        "bot_id": bot_id,
        "avatar": avatar,
        "avatar_text": str(title)[:1] if title else "人",
        "message_count": int(summary.get("message_count") or 0),
        "recv_count": int(summary.get("recv_count") or 0),
        "send_count": int(summary.get("send_count") or 0),
        "last_time": summary.get("last_time") or base.get("last_time") or "",
        "last_row_id": int(summary.get("last_row_id") or base.get("last_row_id") or 0),
    }


@app.route('/api/logs/users')
def api_logs_users():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    query = request.args.get("query", "").strip()
    limit = _safe_int(request.args.get("limit", 20), 20, 5, 50)

    conn = None
    try:
        xiuxian_rows = _get_xiuxian_user_candidates(query, limit)
        message_rows = _get_message_user_candidates(query, limit * 2)

        ordered = xiuxian_rows + message_rows if query else message_rows + xiuxian_rows
        merged: dict[str, dict] = {}
        order: list[str] = []
        for row in ordered:
            user_id = str(row.get("user_id") or "").strip()
            if not user_id:
                continue
            if user_id not in merged:
                merged[user_id] = dict(row)
                order.append(user_id)
            else:
                merged[user_id].update({k: v for k, v in row.items() if v not in (None, "")})

        conn = get_message_db_connection()
        rows = [_build_log_user_row(merged[user_id], conn) for user_id in order[:limit]]

        if query:
            rows.sort(
                key=lambda r: (
                    0 if r["user_id"] == query else 1,
                    0 if r["user_name"] == query else 1,
                    -int(r.get("message_count") or 0),
                    str(r.get("title") or ""),
                )
            )
        else:
            rows.sort(key=lambda r: int(r.get("last_row_id") or 0), reverse=True)

        return jsonify({"success": True, "rows": rows[:limit]})
    except Exception as e:
        return jsonify({"success": False, "error": f"搜索用户失败：{e}"})
    finally:
        if conn is not None:
            conn.close()


@app.route('/api/logs/user_messages')
def api_logs_user_messages():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    user_id = request.args.get("user_id", "").strip()
    if not user_id:
        return jsonify({"success": False, "error": "缺少 user_id"})

    scene = request.args.get("scene", "ALL").strip()
    direction = request.args.get("direction", "ALL").strip()
    keyword = request.args.get("keyword", "").strip()
    adapter = request.args.get("adapter", "").strip()
    start = request.args.get("start", "").strip()
    end = request.args.get("end", "").strip()

    page = _safe_int(request.args.get("page", 1), 1, 1, 1000000)
    page_size = _safe_int(request.args.get("page_size", 200), 200, 20, 500)
    offset = (page - 1) * page_size

    conn = None
    try:
        where = [
            """
            (
                user_id = %s
                OR (
                    direction = 'send'
                    AND COALESCE(source_message_id, '') != ''
                    AND source_message_id IN (
                        SELECT message_id
                        FROM messages
                        WHERE user_id = %s
                          AND direction = 'recv'
                          AND COALESCE(message_id, '') != ''
                    )
                )
            )
            """
        ]
        params = [user_id, user_id]

        if scene and scene != "ALL":
            if scene not in ("group", "private", "channel_group", "channel_private"):
                return jsonify({"success": False, "error": "无效 scene"})
            where.append("scene = %s")
            params.append(scene)

        if direction and direction != "ALL":
            if direction not in ("recv", "send"):
                return jsonify({"success": False, "error": "无效 direction"})
            where.append("direction = %s")
            params.append(direction)

        if adapter:
            where.append("adapter = %s")
            params.append(adapter)

        if keyword:
            where.append(
                """
                (
                    content LIKE %s
                    OR username LIKE %s
                    OR nickname LIKE %s
                    OR group_name LIKE %s
                    OR group_id LIKE %s
                    OR user_id LIKE %s
                )
                """
            )
            like = f"%{keyword}%"
            params.extend([like, like, like, like, like, like])

        if start:
            where.append("created_at >= %s")
            params.append(start.replace("T", " "))

        if end:
            where.append("created_at <= %s")
            params.append(end.replace("T", " "))

        where_sql = " WHERE " + " AND ".join(where)
        conn = get_message_db_connection()
        cur = conn.cursor()

        cur.execute(f"SELECT COUNT(*) AS c FROM messages {where_sql}", params)
        total = int(cur.fetchone()["c"] or 0)

        cur.execute(
            f"""
            SELECT *
            FROM messages
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT %s OFFSET %s
            """,
            params + [page_size, offset],
        )
        rows = _prepare_web_message_rows([dict(r) for r in cur.fetchall()])
        user_info = get_user_by_id(user_id) or {}

        return jsonify({
            "success": True,
            "user": {
                "user_id": user_id,
                "user_name": user_info.get("user_name", ""),
                "level": user_info.get("level", ""),
                "root_type": user_info.get("root_type", ""),
            },
            "total": total,
            "page": page,
            "page_size": page_size,
            "rows": rows,
        })
    except Exception as e:
        return jsonify({"success": False, "error": f"获取用户消息失败：{e}"})
    finally:
        if conn is not None:
            conn.close()


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
