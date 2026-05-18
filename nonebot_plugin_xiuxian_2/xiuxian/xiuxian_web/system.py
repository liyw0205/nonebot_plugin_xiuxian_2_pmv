from .core import *  # noqa: F401,F403

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
        
        recv_count, sent_count = get_message_stats_from_db()
        
        return jsonify({
            "success": True,
            "total_users": total_users,
            "total_sects": total_sects,
            "active_users": active_users,
            "yesterday_users": yesterday_users,
            "seven_days_avg": seven_days_avg,
            # 消息统计
            "msg_received": recv_count,
            "msg_sent": sent_count,
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
            except:
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
        except:
            pass
    return jsonify({"cwd": "~"})

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
