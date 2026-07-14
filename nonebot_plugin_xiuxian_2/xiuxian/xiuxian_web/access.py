from __future__ import annotations

from enum import Enum
from typing import Any


class WebPermission(str, Enum):
    PUBLIC = "public"
    READ = "read"
    DATABASE_WRITE = "database_write"
    MESSAGE = "message"
    BACKUP = "backup"
    UPDATE = "update"
    SCHEDULER = "scheduler"
    TERMINAL_CONFIRM = "terminal_confirm"
    TERMINAL = "terminal"
    LOCAL_UPLOAD = "local_upload"


WEB_ENDPOINT_PERMISSIONS = {
    "static": WebPermission.PUBLIC,
    "login": WebPermission.PUBLIC,
    "home": WebPermission.READ,
    "logout": WebPermission.READ,
    "activity_management": WebPermission.READ,
    "api_activity_config": {"GET": WebPermission.READ, "POST": WebPermission.DATABASE_WRITE},
    "api_activity_template": WebPermission.READ,
    "api_activity_gameplay_template": WebPermission.READ,
    "api_activity_data": WebPermission.READ,
    "api_activity_data_reset": WebPermission.DATABASE_WRITE,
    "api_activity_data_adjust": WebPermission.DATABASE_WRITE,
    "get_cloud_backups": WebPermission.READ,
    "sync_cloud_backup": WebPermission.BACKUP,
    "cloud_restore_backup": WebPermission.BACKUP,
    "cloud_backup_config": WebPermission.BACKUP,
    "get_cloud_config_backups": WebPermission.READ,
    "sync_cloud_config_backup": WebPermission.BACKUP,
    "cloud_restore_config_backup": WebPermission.BACKUP,
    "restore_backup": WebPermission.BACKUP,
    "backups": WebPermission.READ,
    "manual_db_backup": WebPermission.BACKUP,
    "get_db_backups": WebPermission.READ,
    "restore_db_backup": WebPermission.BACKUP,
    "get_cloud_db_backups": WebPermission.READ,
    "sync_cloud_db_backup": WebPermission.BACKUP,
    "cloud_restore_db_backup": WebPermission.BACKUP,
    "batch_delete_backups": WebPermission.BACKUP,
    "batch_sync_cloud_backups": WebPermission.BACKUP,
    "batch_delete_db_backups": WebPermission.BACKUP,
    "batch_sync_cloud_db_backups": WebPermission.BACKUP,
    "batch_delete_cloud_backups": WebPermission.BACKUP,
    "batch_delete_cloud_db_backups": WebPermission.BACKUP,
    "export_config": WebPermission.READ,
    "import_config": WebPermission.BACKUP,
    "backup_config": WebPermission.BACKUP,
    "get_config_backups": WebPermission.READ,
    "restore_config_backup": WebPermission.BACKUP,
    "manual_backup": WebPermission.BACKUP,
    "download_backup": WebPermission.READ,
    "delete_backup": WebPermission.BACKUP,
    "delete_config_backup": WebPermission.BACKUP,
    "command_registry": WebPermission.READ,
    "api_command_registry_toggle": WebPermission.DATABASE_WRITE,
    "api_command_registry_bulk_toggle": WebPermission.DATABASE_WRITE,
    "scheduler_management": WebPermission.SCHEDULER,
    "api_scheduler_jobs": WebPermission.SCHEDULER,
    "api_scheduler_job_enabled": WebPermission.SCHEDULER,
    "api_scheduler_job_schedule": WebPermission.SCHEDULER,
    "api_scheduler_job_run": WebPermission.SCHEDULER,
    "api_scheduler_run": WebPermission.SCHEDULER,
    "commands": WebPermission.READ,
    "execute_command": WebPermission.DATABASE_WRITE,
    "config_management": WebPermission.READ,
    "save_config": WebPermission.DATABASE_WRITE,
    "database": WebPermission.READ,
    "table_view": WebPermission.READ,
    "row_edit": {"GET": WebPermission.READ, "POST": WebPermission.DATABASE_WRITE},
    "batch_edit": WebPermission.DATABASE_WRITE,
    "economy_logs": WebPermission.READ,
    "economy_logs_export": WebPermission.READ,
    "logs": WebPermission.READ,
    "api_logs_users": WebPermission.READ,
    "api_logs_user_messages": WebPermission.READ,
    "api_logs_files": WebPermission.READ,
    "api_logs_read": WebPermission.READ,
    "api_logs_tail": WebPermission.READ,
    "messages_page": WebPermission.READ,
    "api_messages_config": WebPermission.READ,
    "api_messages_config_save": WebPermission.DATABASE_WRITE,
    "api_messages_list": WebPermission.READ,
    "api_messages_dates": WebPermission.READ,
    "api_messages_sessions": WebPermission.READ,
    "api_messages_sessions_since": WebPermission.READ,
    "api_messages_list_since": WebPermission.READ,
    "api_messages_list_before": WebPermission.READ,
    "api_messages_send": WebPermission.MESSAGE,
    "api_messages_broadcast": WebPermission.MESSAGE,
    "api_messages_broadcast_status": WebPermission.READ,
    "api_messages_revoke": WebPermission.MESSAGE,
    "api_messages_bots": WebPermission.READ,
    "api_messages_media_proxy": WebPermission.READ,
    "api_messages_markdown_preview": WebPermission.READ,
    "update": WebPermission.UPDATE,
    "check_update": WebPermission.UPDATE,
    "get_releases": WebPermission.UPDATE,
    "perform_update": WebPermission.UPDATE,
    "get_backups": WebPermission.READ,
    "reward_center": WebPermission.READ,
    "api_reward_records": WebPermission.READ,
    "api_save_reward_record": WebPermission.DATABASE_WRITE,
    "api_delete_reward_record": WebPermission.DATABASE_WRITE,
    "api_clear_reward_records": WebPermission.DATABASE_WRITE,
    "get_stats": WebPermission.READ,
    "get_system_info_extended": WebPermission.READ,
    "get_process_info": WebPermission.READ,
    "api_dashboard_summary": WebPermission.READ,
    "search_users": WebPermission.READ,
    "download_file": WebPermission.READ,
    "terminal_confirm": WebPermission.TERMINAL_CONFIRM,
    "terminal": WebPermission.TERMINAL,
    "terminal_output": WebPermission.TERMINAL,
    "terminal_write": WebPermission.TERMINAL,
    "terminal_pwd": WebPermission.TERMINAL,
    "upload_api_image": WebPermission.LOCAL_UPLOAD,
}


def resolve_endpoint_permission(endpoint: str | None, method: str) -> WebPermission | None:
    configured = WEB_ENDPOINT_PERMISSIONS.get(endpoint or "")
    if not isinstance(configured, dict):
        return configured
    request_method = method.upper()
    if request_method == "HEAD":
        request_method = "GET"
    if request_method == "OPTIONS":
        return configured.get("GET") or next(iter(configured.values()), None)
    return configured.get(request_method)


def undeclared_endpoints(app: Any) -> set[str]:
    return {
        rule.endpoint
        for rule in app.url_map.iter_rules()
        if rule.endpoint not in WEB_ENDPOINT_PERMISSIONS
    }
