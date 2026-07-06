from .core import *  # noqa: F401,F403

from .config import get_config_values

DB_SELECTION_ALIASES = {
    "xiuxian": "xiuxian.db",
    "xiuxian.db": "xiuxian.db",
    "xiuxian_impart": "xiuxian_impart.db",
    "xiuxian_impart.db": "xiuxian_impart.db",
    "player": "player.db",
    "player.db": "player.db",
    "trade": "trade.db",
    "trade.db": "trade.db",
}


def normalize_db_selection(selected_dbs):
    normalized = []
    seen = set()
    for db_name in selected_dbs or []:
        safe_name = Path(str(db_name)).name
        normalized_name = DB_SELECTION_ALIASES.get(safe_name)
        if not normalized_name:
            continue
        if normalized_name not in seen:
            seen.add(normalized_name)
            normalized.append(normalized_name)
    return normalized


def db_path_for_selection(db_name):
    safe_name = Path(str(db_name)).name
    return Path() / "data" / "xiuxian" / safe_name


def safe_request_filename(value) -> str:
    name = Path(str(value or "")).name
    if not name or name in {".", ".."} or "\x00" in name:
        return ""
    return name


def backup_path_under(*parts) -> Path:
    return safe_path_under(Path() / "data" / "xiuxian" / "backups", *parts)


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
    filename = safe_request_filename(data.get('filename'))
    overwrite = data.get('overwrite', False) # 是否允许覆盖
    
    if not filename:
        return jsonify({"success": False, "error": "文件名不能为空"})
    
    local_path = backup_path_under(filename)
    
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
    filename = safe_request_filename(data.get('filename'))
    if not filename:
        return jsonify({"success": False, "error": "无效文件名"})
    
    local_path = backup_path_under(filename)
    
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
        filename = safe_request_filename(data.get('filename'))
        overwrite = data.get('overwrite', False)

        if not filename:
            return jsonify({"success": False, "error": "文件名不能为空"})

        local_path = backup_path_under("config_backups", filename)
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
        filename = safe_request_filename(data.get('filename'))
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
        backup_filename = safe_request_filename(data.get('backup_filename'))
        
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
    backup_filename = safe_request_filename(data.get("backup_filename"))
    selected_dbs = normalize_db_selection(data.get("selected_dbs", []))
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
    filename = safe_request_filename(data.get("filename"))
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
    filename = safe_request_filename(data.get("filename"))
    selected_dbs = normalize_db_selection(data.get("selected_dbs", []))
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

        deleted, failed = [], []

        for name in filenames:
            safe_name = safe_request_filename(name)
            if not safe_name:
                failed.append({"filename": str(name), "reason": "无效文件名"})
                continue
            try:
                f = backup_path_under(safe_name)
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
            safe_name = safe_request_filename(filename)
            if not safe_name:
                failed_list.append({"filename": str(filename), "reason": "无效文件名"})
                continue
            local_path = backup_path_under(safe_name)

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

        deleted, failed = [], []

        for name in filenames:
            safe_name = safe_request_filename(name)
            if not safe_name:
                failed.append({"filename": str(name), "reason": "无效文件名"})
                continue
            f = backup_path_under("db_backup", safe_name)
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
            safe_name = safe_request_filename(filename)
            if not safe_name:
                failed.append({"filename": str(filename), "reason": "无效文件名"})
                continue
            local_path = backup_path_under("db_backup", safe_name)

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
            safe_name = safe_request_filename(name)
            if not safe_name:
                failed.append({"filename": str(name), "reason": "无效文件名"})
                continue
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
            safe_name = safe_request_filename(name)
            if not safe_name:
                failed.append({"filename": str(name), "reason": "无效文件名"})
                continue
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
        backup_filename = safe_request_filename(data.get('backup_filename'))
        
        if not backup_filename:
            return jsonify({"success": False, "error": "未指定备份文件"})
        
        backup_path = backup_path_under("config_backups", backup_filename)
        
        if not backup_path.exists():
            return jsonify({"success": False, "error": f"备份文件不存在: {backup_filename}"})
        if not backup_path.is_file():
            return jsonify({"success": False, "error": f"无效备份文件: {backup_filename}"})
        
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

    safe_name = safe_request_filename(filename)
    if not safe_name:
        return "无效文件名", 400
    try:
        backup_path = backup_path_under(safe_name)
    except ValueError:
        return "无效文件名", 400
    
    if not backup_path.exists():
        return "备份文件不存在", 404
    if not backup_path.is_file():
        return "无效备份文件", 400
    
    return send_file(
        str(backup_path.absolute()),
        as_attachment=True,
        download_name=safe_name,
        mimetype='application/zip'
    )

@app.route('/delete_backup', methods=['POST'])
def delete_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json()
        backup_filename = safe_request_filename(data.get('backup_filename'))
        
        if not backup_filename:
            return jsonify({"success": False, "error": "未指定备份文件"})
        
        backup_path = backup_path_under(backup_filename)
        
        if not backup_path.exists():
            return jsonify({"success": False, "error": f"备份文件不存在: {backup_filename}"})
        if not backup_path.is_file():
            return jsonify({"success": False, "error": f"无效备份文件: {backup_filename}"})
        
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
        backup_filename = safe_request_filename(data.get('backup_filename'))
        
        if not backup_filename:
            return jsonify({"success": False, "error": "未指定备份文件"})

        backup_path = backup_path_under("config_backups", backup_filename)
        
        if not backup_path.exists():
            return jsonify({"success": False, "error": f"备份文件不存在: {backup_filename}"})
        
        # 删除文件
        backup_path.unlink()
        
        logger.info(f"配置备份文件已删除: {backup_filename}")
        return jsonify({"success": True, "message": f"配置备份文件删除成功: {backup_filename}"})
        
    except Exception as e:
        logger.error(f"删除配置备份失败: {str(e)}")
        return jsonify({"success": False, "error": f"删除配置备份失败: {str(e)}"})
