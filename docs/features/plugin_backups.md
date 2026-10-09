# 插件整包备份与恢复

## 用户流程

管理员对整个插件目录做 zip 快照、查看本地与云端快照、下载或删除单个/多个快照、把快照同步到 WebDAV，或用某个快照回滚整包（含把压缩包内的数据库落回运行目录）。创建快照时跳过备份自身、缓存、图片字体等资源目录和瞬时运行文件，只保留可部署内容。

## 命令与别名

无命令，`commands.py` 声明 `COMMANDS = ()`。写入口只有 Web 控制台，但触发面不止浏览器：`status` 切片的超级用户命令 `检测更新` 走 `UpdateApplication.perform_update_with_backup`（`features/updater/application.py:47`），其中 `enhanced_backup_current_version` 一步会调用本切片的 `PluginBackupCreationApplication.create_backup`。

## Web API

`ROUTES` 为空，以下路由仍由旧模块 `xiuxian/xiuxian_web/backups.py`（管理员会话 + CSRF）注册并转发到本切片的 cloud/creation/file/restore 应用：

- `GET /get_backups` → `PluginBackupCatalogApplication.list_plugin_backups`
- `GET /get_cloud_backups` → `PluginBackupCloudApplication.list_cloud_backups`
- `POST /sync_cloud_backup` → `PluginBackupCloudApplication.sync_cloud_backup`
- `POST /cloud_restore_backup` → `PluginBackupRestoreApplication.restore_backup`
- `POST /restore_backup` → `PluginBackupRestoreApplication.restore_backup`
- `POST /batch_delete_backups` → `PluginBackupFileApplication.delete_plugin_backups`
- `POST /batch_sync_cloud_backups` → `PluginBackupCloudApplication.sync_cloud_backups`
- `POST /batch_delete_cloud_backups` → `PluginBackupCloudApplication.delete_cloud_backups`
- `GET /download_backup/<filename>` → `PluginBackupFileApplication.open_plugin_backup`
- `POST /delete_backup` → `PluginBackupFileApplication.delete_plugin_backup`

`GET /get_backups` 由旧模块 `xiuxian/xiuxian_web/pages.py` 注册，但读取的是本切片的备份目录，因此计入清单。`GET /backups` 目前是双轨：旧模块 `xiuxian/xiuxian_web/backups.py:232` 仍注册同名页面，`bootstrap/platform_manifest.py:48` 也已把它声明给 `runtime_web`（`adapters/web/blueprints/legacy.py` 以 308 重定向到 `/pages/backups`）；两条都不属于本切片，等 `runtime_web` 收口时消重。`/api/v1/backups`、`/api/v1/backups/restore` 由 `adapters/web/blueprints/backups.py` 提供，同属 `runtime_web` owner。以上路径都不写进本切片清单，以免注册表重复声明。

## 数据模型与迁移

无表、无迁移，`migrations.py` 声明 `MIGRATIONS = ()`。产物是 `data/backups` 下的 zip，快照名匹配 `ARCHIVE_NAME_PATTERN`，内部时间戳片段由 `ARCHIVE_TIMESTAMP_PATTERN` 识别，版本号经 `VERSION_SAFE_PATTERN` 清洗。`schemas.py` 是唯一契约来源：`MAX_CLOUD_BACKUP_BATCH`、`MAX_CLOUD_LIST_BYTES`、`MAX_CLOUD_LIST_ENTRIES`、`MAX_PLUGIN_BACKUP_DOWNLOAD_BYTES`、`RESTORE_DISK_RESERVE_BYTES`（原先在云端与恢复仓储重复定义，现已合并）、恢复归档根 `PLUGIN_ARCHIVE_ROOT`、成员上限 `MAX_ARCHIVE_MEMBERS`，以及打包排除项 `SKIP_DIRECTORY_NAMES` 与 `TRANSIENT_DATA_PATHS`。归档文件名的前后缀由 `ARCHIVE_PREFIX`/`ARCHIVE_SUFFIX` 单点定义，`ARCHIVE_NAME_PATTERN`、云端与本地列举、文件名校验都从这两个常量派生，不再散写 `"backup_"` 与 `".zip"` 字面量。

## 事务与失败回滚

创建快照在备份目录内写临时文件（`tempfile.mkstemp(dir=备份目录)`）完成后 `os.replace` 原子落位；文件名一律按快照模式与 `..`/绝对路径规则校验。恢复采用两段式：先解到暂存根（校验成员数、路径穿越、Windows 盘符绝对路径与目标根 `PLUGIN_ARCHIVE_ROOT`），再逐文件原子复制覆盖；覆盖前用 `RESTORE_DISK_RESERVE_BYTES` 与目标盘剩余空间做预留校验，磁盘不足时不开始覆盖。数据库回灌由注入端口完成，端口异常按失败返回，不宣称跨库原子性。

## 定时任务

无任务，`jobs.py` 声明 `JOBS = ()`。云端列举、上传、删除和保留期清理都在请求调用栈内完成。

## 配置项

本切片不拥有配置键：云备份开关、WebDAV 参数与保留天数属于 `plugin_config` 切片，经 `PluginBackupCreationRuntime`/`PluginBackupCloudRuntime`/`PluginBackupRestoreProvider` 等端口注入。

## 适配器差异

五个应用（catalog、cloud、creation、file、restore）都不导入 NoneBot/Flask；WebDAV 的 `PROPFIND/PUT/GET`、时间格式化、SQLite 库名单和恢复后回调都是端口。旧 Web 层负责鉴权、CSRF、文件名解析、流式下载和错误文案。

## 测试与手工验收

注意本目录混用两种写法：函数式（pytest）用例不会被 `unittest discover` 收集，必须用 `python -m pytest -p no:cacheprovider <目录>` 运行；`unittest discover` 只收集到 unittest 写法的契约用例。函数式用例是 `test_creation.py`，其余（含 `test_slice_contract.py`）是 unittest 写法。

`python -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/plugin_backups/tests` 覆盖目录清点、排除项与瞬时文件、云端列举与批量上限、下载体积上限、删除与批量删除的部分失败、恢复归档校验与原子覆盖。手工验收：在隔离目录创建快照后解压，确认不含 `SKIP_DIRECTORY_NAMES` 与 `TRANSIENT_DATA_PATHS` 列出的内容，再执行一次回滚并确认覆盖只发生在预期根目录内。
