# 数据库备份与恢复

## 用户流程

管理员在 Web 控制台对 SQLite 业务库做整包备份、查看本地与云端备份列表、把本地备份同步到 WebDAV、或选择部分库从备份恢复。可选库固定为四个：`xiuxian.db`、`xiuxian_impart.db`、`player.db`、`trade.db`，别名（如 `player`）与文件名（`player.db`）都接受；任何其它名称在打开压缩包之前被拒绝。恢复按“暂存—落盘”两段执行，逐库覆盖后统一回报。

## 命令与别名

无命令，`commands.py` 声明 `COMMANDS = ()`。写入口只有 Web 控制台，但触发面不止浏览器：`status` 切片的超级用户命令 `检测更新` 走 `UpdateApplication.perform_update_with_backup`（`features/updater/application.py:47`），其中 `backup_db_files` 这一步会调用本切片 `DatabaseBackupApplication.create_backup`。

## Web API

`web.py` 的 `ROUTES` 为空：以下路由目前仍由旧模块 `xiuxian/xiuxian_web/backups.py`（管理员会话 + CSRF）注册后转发到 `DatabaseBackupApplication`。

- `POST /manual_db_backup` → `DatabaseBackupApplication.create_backup`
- `GET /get_db_backups` → `DatabaseBackupApplication.list_local_backups`
- `POST /restore_db_backup` → `DatabaseBackupApplication.restore_local_backup`
- `GET /get_cloud_db_backups` → `DatabaseBackupApplication.list_cloud_backups`
- `POST /sync_cloud_db_backup` → `DatabaseBackupApplication.sync_cloud_backup`
- `POST /cloud_restore_db_backup` → `DatabaseBackupApplication.restore_cloud_backup`
- `POST /batch_delete_db_backups` → `DatabaseBackupApplication.delete_local_backups`
- `POST /batch_sync_cloud_db_backups` → `DatabaseBackupApplication.sync_cloud_backups`
- `POST /batch_delete_cloud_db_backups` → `DatabaseBackupApplication.delete_cloud_backups`

## 数据模型与迁移

无表、无迁移，`migrations.py` 声明 `MIGRATIONS = ()`。产物是 `data/backups/db_backup` 下匹配 `DATABASE_BACKUP_ARCHIVE_PATTERN` 的 zip 包。`schemas.py` 集中定义契约：`DATABASE_ALIASES` 白名单、批量上限 `MAX_DATABASE_BACKUP_BATCH`、云端列举 `MAX_DATABASE_BACKUP_CLOUD_LIST_BYTES`/`MAX_DATABASE_BACKUP_CLOUD_LIST_ENTRIES`、下载上限 `MAX_DATABASE_BACKUP_DOWNLOAD_BYTES`、恢复成员与体积上限 `MAX_DATABASE_RESTORE_MEMBERS`/`MAX_DATABASE_RESTORE_BYTES`，以及归档前后缀 `DATABASE_BACKUP_PREFIX`/`DATABASE_BACKUP_SUFFIX`（`DATABASE_BACKUP_ARCHIVE_PATTERN`、目录 glob 与文件名生成都由它们派生）；应用与仓储都从这里导入。

## 事务与失败回滚

备份与恢复前都调用 `infrastructure.database.backup_capacity.preflight_capacity`，按目标盘剩余空间加预留量决定是否开工，空间不足直接失败且不写半个包。恢复先把成员解到暂存目录（校验成员数量、解压体积、文件名与 Windows 绝对路径），成功后才逐库落盘；中途失败抛 `PartialDatabaseRestoreError` 并携带已恢复库清单，未落盘的库保持原文件，常规路径只在解压前按全部选中库的合计体积做一次空间预检（`_preflight_restore_space`），逐库单独的 `preflight_capacity` 只在 `database_backup_validate_sqlite` 校验失败后的重建分支里触发。SQLite 文件级恢复不是事务，恢复完成后需要按运行时的对账流程确认状态。

## 定时任务

`jobs.py` 声明 `JOBS = ()`：本切片不注册任务。但运行时有真实定时器驱动本切片——`legacy_scheduler` 拥有 APScheduler 任务 `backup_database_files`（`cron hour="*/4" minute=10`，`xiuxian/xiuxian_scheduler/__init__.py:523-544`），经 `UpdateManager.backup_db_files` 调用 `DatabaseBackupApplication.create_backup`；该 job 的 ID 声明在 `compatibility/legacy_manifest.py`，标题在 `features/scheduler/apscheduler_manager.py:87`，归属 `legacy_scheduler`，等 scheduler owner 迁移时才搬走。本地保留期清理在备份动作内由 `cleanup_local_backups(keep_days)` 同步执行，云端清理由端口完成。

## 配置项

本切片不拥有配置键。云备份开关、WebDAV 参数与 `local_backup_keep_days` 属于 `plugin_config` 切片，经 `DatabaseBackupRuntime`/`DatabaseBackupProvider` 端口（`database_backup_*` 方法）注入。

## 适配器差异

应用层不导入 NoneBot 或 Flask；时间、保留天数、云开关与 WebDAV 交互都是端口，`WebDAV` 的 HTTP 调用可注入替换。旧 Web 层只做鉴权、参数解析和消息文案。恢复后的库重连与缓存刷新通过端口回调，切片自身不持有连接。

## 测试与手工验收

注意本目录混用两种写法：函数式（pytest）用例不会被 `unittest discover` 收集，必须用 `python -m pytest -p no:cacheprovider <目录>` 运行；`unittest discover` 只收集到 unittest 写法的契约用例。函数式用例是 `test_application.py`、`test_repository.py`，unittest 写法的只有 `test_slice_contract.py`。

`python -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/database_backups/tests` 覆盖别名白名单、压缩包名校验、批量上限、空间预检拒绝、成员数与解压体积上限、部分恢复失败回报和本地清理。手工验收：在隔离数据目录做一次备份、一次部分库恢复，断言只改动被选中的 `.db` 文件，并在预留空间不足时确认整次操作零写入。
