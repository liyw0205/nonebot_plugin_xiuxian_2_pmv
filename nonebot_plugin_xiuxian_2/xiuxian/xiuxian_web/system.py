from .core import (
    DATABASE,
    IS_WINDOWS,
    MessageSegment,
    Response,
    abort,
    api_error,
    api_success,
    app,
    datetime,
    format_time,
    get_bots,
    get_paths,
    is_local_web_request,
    jsonify,
    logger,
    nb_version,
    os,
    psutil,
    psutil_available,
    redirect,
    render_template,
    request,
    run_async,
    safe_path_under,
    select,
    send_file,
    session,
    terminal_authorization_is_valid,
    time,
    url_for,
    web_auth_is_enabled,
)
from ...features.status.application import StatusApplication
from ..xiuxian_utils.utils import search_users as search_users_application


status_application = StatusApplication(
    DATABASE,
    message_database=get_paths().message_db,
)


def _collect_dashboard_stats():
    """Build the legacy dashboard response from feature-owned counters."""
    stats = status_application.dashboard_stats(now=datetime.now())

    bot_info_list = []
    for bid, bot in get_bots().items():
        adapter = "未知"
        try:
            adapter = bot.adapter.get_name()
        except Exception:
            pass
        bot_info_list.append({"bot_id": bid, "adapter": adapter})

    bot_uptime = "未知"
    if psutil_available:
        try:
            process_create_time = psutil.Process(os.getpid()).create_time()
            bot_uptime = format_time(time.time() - process_create_time)
        except Exception:
            pass

    return {
        **stats,
        "bot_count": len(bot_info_list),
        "bots": bot_info_list,
        "bot_uptime": bot_uptime,
        "nb_version": nb_version,
    }


def _collect_system_snapshot():
    sections = dict(status_application.system_info().sections)
    return {
        "system_info": dict(sections["系统信息"]),
        "cpu_info": dict(sections["CPU信息"]),
        "mem_info": dict(sections["内存信息"]),
        "disk_info": dict(sections["磁盘信息"]),
        "system_uptime": dict(sections["运行时间"]),
    }


def _collect_process_snapshot(limit=5):
    return status_application.process_info(limit=limit)


@app.route('/get_stats')
def get_stats():
    if 'admin_id' not in session:
        return api_error("未登录")

    try:
        return api_success(**_collect_dashboard_stats())

    except Exception as e:
        logger.exception("统计信息获取失败")
        return api_error(str(e) or e.__class__.__name__)

@app.route('/get_system_info_extended')
def get_system_info_extended():
    """获取详细系统信息，对psutil是否可用进行适配"""
    if 'admin_id' not in session:
        return api_error("未登录")

    return api_success(**_collect_system_snapshot())
        
@app.route('/get_process_info')
def get_process_info():
    """获取进程信息，对psutil是否可用进行适配"""
    if 'admin_id' not in session:
        return api_error("未登录")
    
    if not status_application.process_info_available:
        return api_error("psutil未安装，无法获取进程信息", processes=[])

    try:
        return api_success(processes=_collect_process_snapshot(5))
    except Exception as e:
        return api_error(f"获取进程信息失败: {str(e)}")


@app.route('/api/dashboard/summary')
def api_dashboard_summary():
    if 'admin_id' not in session:
        return api_error("未登录")

    try:
        return api_success(
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            stats=_collect_dashboard_stats(),
            system=_collect_system_snapshot(),
            processes=_collect_process_snapshot(5),
        )
    except Exception as e:
        logger.exception("仪表盘聚合信息获取失败")
        return api_error(str(e) or e.__class__.__name__)

@app.route('/search_users')
def search_users():
    if 'admin_id' not in session:
        return jsonify([])

    query = request.args.get('query', '')
    try:
        results = search_users_application(query)
    except ValueError:
        # Preserve the legacy response shape for invalidly long queries.
        return jsonify([])
    return jsonify(results)

@app.route('/download/<path:filepath>')
def download_file(filepath):
    cache_dir = get_paths().cache
    try:
        full_path = safe_path_under(cache_dir, filepath)
    except ValueError:
        abort(403)

    if not full_path.exists():
        abort(404)
    if not full_path.is_file():
        abort(403)

    return send_file(str(full_path))

# 全局存储终端会话：admin_id -> {'fd': master_fd, 'pid': child_pid}
terminal_sessions = {}

def get_terminal_session(admin_id):
    """获取或创建一个持久的 bash 会话，仅支持 Linux/Unix"""
    if IS_WINDOWS:
        raise RuntimeError("Web终端功能仅支持 Linux/Unix 环境，Windows 不支持。")

    if admin_id in terminal_sessions:
        # 检查进程是否还在运行
        pid = terminal_sessions[admin_id]['pid']
        try:
            os.kill(pid, 0)
            return terminal_sessions[admin_id]
        except OSError:
            # 进程已死，清理
            try:
                os.close(terminal_sessions[admin_id]['fd'])
            except Exception:
                pass
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
        env["PS1"] = "\\[\\033[01;32m\\]\\u\\[\\033[00m\\]:\\[\\033[01;34m\\]\\w\\[\\033[00m\\]\\$ "

        os.execve("/bin/bash", ["/bin/bash", "--login", "-i"], env)

    # 父进程
    os.close(slave_fd)

    # 设置非阻塞
    fl = fcntl.fcntl(master_fd, fcntl.F_GETFL)
    fcntl.fcntl(master_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    session_data = {'fd': master_fd, 'pid': pid}
    terminal_sessions[admin_id] = session_data
    return session_data


@app.route('/terminal/confirm', methods=['GET', 'POST'])
def terminal_confirm():
    if not web_auth_is_enabled():
        return redirect(url_for('terminal'))
    if terminal_authorization_is_valid():
        return redirect(url_for('terminal'))
    session['terminal_authorized_until'] = time.time() + 300
    return redirect(url_for('terminal'))


@app.route('/terminal')
def terminal():
    if 'admin_id' not in session:
        return redirect(url_for('login'))
    if IS_WINDOWS:
        return "Web终端功能仅支持 Linux/Unix，Windows 暂不支持", 400
    return render_template('terminal.html', admin_id=session['admin_id'])


@app.route('/terminal/output')
def terminal_output():
    """流式读取终端输出的 Generator"""
    if 'admin_id' not in session:
        return "Unauthorized", 401
    if IS_WINDOWS:
        return "Windows 不支持该功能", 400

    admin_id = session['admin_id']
    term = get_terminal_session(admin_id)

    def generate():
        fd = term['fd']
        while True:
            r, _, _ = select.select([fd], [], [], 0.5)
            if r:
                try:
                    data = os.read(fd, 1024 * 16)
                    if not data:
                        break
                    yield data.decode('utf-8', errors='replace')
                except (OSError, Exception):
                    break
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
    if IS_WINDOWS:
        return jsonify({"success": False, "error": "Windows 不支持该功能"})

    admin_id = session['admin_id']
    data = request.get_json() or {}
    input_str = data.get('input', '')

    term = get_terminal_session(admin_id)
    try:
        os.write(term['fd'], input_str.encode('utf-8'))
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route('/terminal/pwd')
def terminal_pwd():
    if 'admin_id' not in session:
        return jsonify({"cwd": "/"})
    if IS_WINDOWS:
        return jsonify({"cwd": "Windows not supported"})

    admin_id = session['admin_id']
    if admin_id in terminal_sessions:
        pid = terminal_sessions[admin_id]['pid']
        try:
            cwd = os.readlink(f"/proc/{pid}/cwd")
            return jsonify({"cwd": cwd})
        except Exception:
            pass
    return jsonify({"cwd": "~"})

@app.route('/upload_image', methods=['POST'])
def upload_api_image():
    """
    供外部/其他插件调用的图片上传接口
    """
    local_upload_allowed = is_local_web_request()
    if 'admin_id' not in session and not local_upload_allowed:
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
