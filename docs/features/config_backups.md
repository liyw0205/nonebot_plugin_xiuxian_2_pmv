# 配置文件备份

## 用户流程

管理员在 Web 控制台导出当前配置、生成 JSON 快照、按需同步到 WebDAV，或选择某个历史快照恢复。导出可以只挑选字段，也可以整份导出；恢复会把快照内容交回配置写入端口，成功后配置源文件按原格式重写。快照文件命名固定为 `config_backup_<时间戳>.json`，`is_config_backup_filename` 负责路径安全与 `.json` 后缀（拒绝路径分隔符、`.`/`..`、空字节、非自身 basename 与超过 255 字节的名称），`config_backup_` 前缀则由目录列举和恢复入口单独校验。

## 命令与别名

无命令，`commands.py` 显式声明 `COMMANDS = ()`，本切片不注册任何 NoneBot matcher。写入口只有 Web 控制台，但触发面不止浏览器：`status` 切片的超级用户命令 `检测更新` 走 `UpdateApplication.perform_update_with_backup`（`features/updater/application.py:47`），其中 `backup_all_configs` 一步会调用 `ConfigBackupApplication.backup_all_configs`。

## Web API

新适配器尚未注册这些路由（`web.py` 的 `ROUTES` 为空），当前全部由旧模块 `xiuxian/xiuxian_web/backups.py` 以管理员会话加 CSRF 守卫注册后转发到 `ConfigBackupApplication`：

- `POST /cloud_backup_config` → `ConfigBackupApplication.backup_cloud_config`
- `GET /get_cloud_config_backups` → `ConfigBackupApplication.list_cloud_backups`
- `POST /sync_cloud_config_backup` → `ConfigBackupApplication.sync_cloud_backup`
- `POST /cloud_restore_config_backup` → `ConfigBackupApplication.restore_cloud_backup`
- `POST /export_config` → `ConfigBackupApplication.export_config`
- `POST /import_config` → `ConfigBackupApplication.import_config`
- `POST /backup_config` → `ConfigBackupApplication.create_local_backup`
- `GET /get_config_backups` → `ConfigBackupApplication.list_local_backups`
- `POST /restore_config_backup` → `ConfigBackupApplication.restore_local_backup`
- `POST /delete_config_backup` → `ConfigBackupApplication.delete_local_backup`

迁移到新适配器时，`web.py` 的 `LEGACY_ROUTES` 是逐条搬迁清单，搬完后必须同时删除旧注册并从 `web.py` 移除对应条目。

## 数据模型与迁移

无数据表、无迁移，`migrations.py` 声明 `MIGRATIONS = ()`。持久产物是 `data/backups/config_backups` 下的 JSON 文件。`schemas.py` 是唯一契约来源：单份快照上限 `MAX_CONFIG_BACKUP_BYTES`、云端列举上限 `MAX_CONFIG_CLOUD_LIST_BYTES` 与 `MAX_CONFIG_CLOUD_LIST_ENTRIES`、文件名前缀 `CONFIG_BACKUP_PREFIX`、后缀 `CONFIG_BACKUP_SUFFIX`，以及 `is_config_backup_filename` 校验函数；前缀与后缀只在这些常量上出现一次，仓储和应用都从这里导入，改一处即全局生效。

## 事务与失败回滚

`create_local_backup`、`backup_all_configs_with_details`、`backup_cloud_config` 共享 `_create_lock` 非阻塞单飞锁，抢不到锁直接返回“正在执行”结果，不排队也不并发写同一目录。写快照先落临时文件再原子替换，写入前用 `MAX_CONFIG_BACKUP_BYTES` 拒绝超限内容；恢复只在快照校验通过后调用配置写入端口，端口返回失败时本切片不写任何文件，已校验的快照原样保留。配置落盘本身属于 `plugin_config` owner：`xiuxian/xiuxian_utils/config_literal.py:233-249` 先 `compile()` 语法校验再写入，组合出非法内容时不会落笔，但真正写入用的是裸 `write_text`，不是临时文件加 `os.replace`，因此进程在写入中途被杀仍可能截断配置文件——这条边界归 `plugin_config` 收口，本切片不声称配置文件写入是原子的。

## 定时任务

无任务，`jobs.py` 声明 `JOBS = ()`，也没有定时器调用本切片。清理只在部分入口内同步发生：`backup_all_configs_with_details` 与 `backup_cloud_config` 会执行本地保留期清理，云端清理经 `configuration_backup_cleanup_cloud` 端口在同一调用栈执行；`create_local_backup`（`POST /backup_config`）只写快照，不触发清理。

## 配置项

本切片不拥有配置键，配置项属于 `plugin_config` 切片：云备份开关、WebDAV 地址与账号口令、`local_backup_keep_days` 保留天数都由 `xiuxian/xiuxian_config.py` 定义，经 `ConfigBackupRuntime`/`ConfigBackupRepository` 端口（`configuration_backup_*` 方法）注入，切片自身不读环境变量、不读配置文件。

## 适配器差异

应用与仓储不导入 NoneBot 或 Flask：时间、版本号、配置读写、云备份开关与 WebDAV 调用全部是注入端口，`ConfigBackupRepository` 的 HTTP 方法可按测试替换。旧 Web 层只负责鉴权、CSRF、文件名解析和响应封装。

## 测试与手工验收

`python -m unittest discover -s nonebot_plugin_xiuxian_2/features/config_backups/tests -t . -q` 覆盖导出挑选、快照命名与前缀校验、超限拒绝、单飞锁、原子写入失败、云端列举上限与恢复回滚；`tests/test_backup_slice_manifests.py` 校验切片契约单一来源、空表面声明与注册表登记。手工验收：在一个隔离数据目录执行一次备份和一次恢复，确认只新增/覆盖 `config_backup_*.json`，且关闭云备份时不发生任何网络请求。
