from .core import (
    Path,
    app,
    datetime,
    config_backup_application,
    database_backup_application,
    get_paths,
    json,
    jsonify,
    logger,
    manual_backup_application,
    plugin_backup_cloud_application,
    plugin_backup_file_application,
    plugin_backup_restore_application,
    redirect,
    render_template,
    request,
    runtime_clock,
    safe_path_under,
    send_file,
    session,
    url_for,
)

from ...features.plugin_backups import InvalidPluginBackupFile, PluginBackupFileNotFound

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
    return get_paths().data / safe_name


def safe_request_filename(value) -> str:
    name = Path(str(value or "")).name
    if not name or name in {".", ".."} or "\x00" in name:
        return ""
    return name


def backup_path_under(*parts) -> Path:
    return safe_path_under(get_paths().backups, *parts)


@app.route('/get_cloud_backups')
def get_cloud_backups():
    """获取云端备份列表"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    success, result = plugin_backup_cloud_application.list_cloud_backups()
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
    
    success, result = plugin_backup_cloud_application.sync_cloud_backup(
        filename, overwrite=overwrite
    )
    if not success and result == "FILE_EXISTS":
        return jsonify({
            "success": False, 
            "error": "FILE_EXISTS", 
            "message": f"本地已存在同名备份文件 {filename}，是否覆盖下载？"
        })
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
    
    # A local archive wins; only fetch remotely when it is absent.
    try:
        local_exists = plugin_backup_cloud_application.local_backup_exists(filename)
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)})
    if not local_exists:
        logger.info(f"本地无备份 {filename}，正在从云端拉取并准备恢复...")
        success, err = plugin_backup_cloud_application.sync_cloud_backup(
            filename, overwrite=False
        )
        if not success:
            return jsonify({"success": False, "error": f"下载失败: {err}"})
    else:
        logger.info(f"本地已存在备份 {filename}，直接进行本地恢复流程")

    # 步骤2：执行恢复
    success, message = plugin_backup_restore_application.restore_backup(filename)
    if success:
        return jsonify({"success": True, "message": message})
    else:
        return jsonify({"success": False, "error": message})

@app.route('/cloud_backup_config', methods=['POST'])
def cloud_backup_config():
    """本地配置备份 + 上传云端"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    success, result = config_backup_application.backup_cloud_config()
    if not success:
        return jsonify({"success": False, "error": str(result)})
    return jsonify({"success": True, "message": f"配置云备份成功：{Path(result).name}"})


@app.route('/get_cloud_config_backups')
def get_cloud_config_backups():
    """获取云端配置备份列表"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    success, result = config_backup_application.list_cloud_backups()
    if success:
        return jsonify({"success": True, "backups": result})
    return jsonify({"success": False, "error": result})


@app.route('/sync_cloud_config_backup', methods=['POST'])
def sync_cloud_config_backup():
    """同步云端配置备份到本地（支持覆盖检测）"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    data = request.get_json(silent=True) or {}
    filename = safe_request_filename(data.get('filename'))
    overwrite = data.get('overwrite', False)
    if not filename:
        return jsonify({"success": False, "error": "文件名不能为空"})
    success, result = config_backup_application.sync_cloud_backup(
        filename, overwrite=overwrite
    )
    if success:
        return jsonify({"success": True, "message": f"同步成功: {filename}"})
    if result == "FILE_EXISTS":
        return jsonify({
            "success": False,
            "error": "FILE_EXISTS",
            "message": f"本地已存在同名配置备份 {filename}，是否覆盖下载？"
        })
    return jsonify({"success": False, "error": str(result)})


@app.route('/cloud_restore_config_backup', methods=['POST'])
def cloud_restore_config_backup():
    """云端配置恢复（返回配置数据给前端，前端点击保存再落地）"""
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})

    data = request.get_json(silent=True) or {}
    filename = safe_request_filename(data.get('filename'))
    if not filename:
        return jsonify({"success": False, "error": "未指定备份文件"})
    success, result = config_backup_application.restore_cloud_backup(filename)
    if not success:
        return jsonify({"success": False, "error": result})
    return jsonify({
        "success": True,
        "data": result["data"],
        "metadata": result.get("metadata", {}),
        "message": "云端配置已加载，请点击保存所有配置应用。"
    })

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
        success, message = plugin_backup_restore_application.restore_backup(backup_filename)
        
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
    ok, msg = database_backup_application.create_backup()
    return jsonify({"success": ok, "message": msg if ok else "", "error": "" if ok else msg})


@app.route('/get_db_backups')
def get_db_backups():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    try:
        backups = database_backup_application.list_local_backups()
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
    ok, msg = database_backup_application.restore_local_backup(backup_filename, selected_dbs)
    return jsonify({"success": ok, "message": msg if ok else "", "error": "" if ok else msg})


@app.route('/get_cloud_db_backups')
def get_cloud_db_backups():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    ok, result = database_backup_application.list_cloud_backups()
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

    ok, result = database_backup_application.sync_cloud_backup(filename, overwrite=overwrite)
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

    ok, msg = database_backup_application.restore_cloud_backup(filename, selected_dbs)
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

        deleted, failed = plugin_backup_file_application.delete_plugin_backups(filenames)

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

        success_list, exists_list, failed_list = (
            plugin_backup_cloud_application.sync_cloud_backups(
                filenames, overwrite=overwrite
            )
        )

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

        deleted, failed = database_backup_application.delete_local_backups(filenames)

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

        synced, exists, failed = database_backup_application.sync_cloud_backups(
            filenames, overwrite=overwrite
        )

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

        deleted, failed = plugin_backup_cloud_application.delete_cloud_backups(
            filenames
        )

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

        deleted, failed = database_backup_application.delete_cloud_backups(filenames)

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
        data = request.get_json(silent=True) or {}
        export_data, filename = config_backup_application.export_config(
            data.get('selected_fields', []), export_all=data.get('export_all', False)
        )
        return jsonify({"success": True, "data": export_data, "filename": filename})
        
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
        
        config_data = config_backup_application.import_config(file.filename, file.stream)
        
        return jsonify({
            "success": True,
            "data": config_data,
            "message": "配置导入成功，请点击保存按钮应用配置"
        })
        
    except json.JSONDecodeError:
        return jsonify({"success": False, "error": "文件格式错误，不是有效的JSON"})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)})
    except Exception as e:
        return jsonify({"success": False, "error": f"导入配置失败: {str(e)}"})

@app.route('/backup_config', methods=['POST'])
def backup_config():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json(silent=True) or {}
        backup_path = config_backup_application.create_local_backup(
            data.get('selected_fields', []), backup_all=data.get('backup_all', False)
        )
        
        return jsonify({
            "success": True,
            "message": f"配置备份成功: {backup_path.name}",
            "backup_path": str(backup_path)
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"备份配置失败: {str(e)}"})

@app.route('/get_config_backups')
def get_config_backups():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        return jsonify({
            "success": True,
            "backups": config_backup_application.list_local_backups()
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
        
        success, result = config_backup_application.restore_local_backup(backup_filename)
        if not success:
            return jsonify({"success": False, "error": result})
        
        return jsonify({
            "success": True,
            "data": result["data"],
            "metadata": result["metadata"],
            "message": "配置恢复成功，请点击保存按钮应用配置"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": f"恢复配置失败: {str(e)}"})

@app.route('/manual_backup', methods=['POST'])
def manual_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        result = manual_backup_application.create_backup()
        payload = {
            "success": result.success,
            "plugin_backup": str(result.plugin_backup)
            if isinstance(result.plugin_backup, Path)
            else result.plugin_backup,
            "config_backup": str(result.config_backup)
            if isinstance(result.config_backup, Path)
            else result.config_backup,
        }
        if result.success:
            payload["message"] = "手动备份成功完成"
        else:
            payload["error"] = result.error
        return jsonify(payload)
    except Exception as e:
        return jsonify({"success": False, "error": f"备份过程中出现错误: {str(e)}"})

@app.route('/download_backup/<filename>')
def download_backup(filename):
    if 'admin_id' not in session:
        return redirect(url_for('login'))

    try:
        backup_file = plugin_backup_file_application.open_plugin_backup(filename)
    except PluginBackupFileNotFound:
        return "备份文件不存在", 404
    except InvalidPluginBackupFile:
        return "无效备份文件", 400
    return send_file(
        backup_file,
        as_attachment=True,
        download_name=filename,
        mimetype='application/zip'
    )

@app.route('/delete_backup', methods=['POST'])
def delete_backup():
    if 'admin_id' not in session:
        return jsonify({"success": False, "error": "未登录"})
    
    try:
        data = request.get_json() or {}
        backup_filename = data.get('backup_filename')
        
        if not backup_filename:
            return jsonify({"success": False, "error": "未指定备份文件"})
        
        try:
            plugin_backup_file_application.delete_plugin_backup(str(backup_filename))
        except PluginBackupFileNotFound:
            return jsonify({"success": False, "error": f"备份文件不存在: {backup_filename}"})
        except InvalidPluginBackupFile:
            return jsonify({"success": False, "error": f"无效备份文件: {backup_filename}"})
        
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

        success, message = config_backup_application.delete_local_backup(backup_filename)
        if not success:
            return jsonify({"success": False, "error": message})
        
        logger.info(f"配置备份文件已删除: {backup_filename}")
        return jsonify({"success": True, "message": message})
        
    except Exception as e:
        logger.error(f"删除配置备份失败: {str(e)}")
        return jsonify({"success": False, "error": f"删除配置备份失败: {str(e)}"})
