# 数据层与 Web 面板治理说明

本项目运行期使用本地 SQLite 数据库，统一数据层入口放在
`nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/db_backend.py`。

## 数据层约定

- 新增查询优先使用 `db_backend.query_one()` / `db_backend.query_all()`。
- 新增单条写入优先使用 `db_backend.execute_write()`。
- 多步写入必须使用 `db_backend.transaction()`，避免中途失败导致部分落库。
- Web 层兼容入口为 `db_backend.execute_sql_safely()`，返回旧接口兼容的 dict/list。
- 动态表名、字段名必须通过 `db_backend.quote_ident()` 或 Web 层 `sql_ident()` 处理。
- 不要直接在 Web 路由中拼接用户提交的表名、字段名或文件路径。

## 迁移约定

核心迁移入口为 `xiuxian_utils/db_migrations.py`：

- 迁移记录表：`xiuxian_schema_migrations`。
- 启动时由 `XiuxianDateManage.__init__()` / `reconnect()` 触发。
- 迁移必须幂等；索引、列、表创建需要使用存在性检查或 `IF NOT EXISTS`。
- 热点索引可以放在 `_ensure_core_indexes()`，即使迁移已应用也会幂等补齐。

## Web 安全配置

Web 面板新增了运行期安全配置，均可在 `xiuxian_config.py` 或 Web 配置页调整：

- `web_require_csrf`：写请求 CSRF 校验，默认开启。
- `web_allowed_hosts`：Host 白名单，留空不限制。
- `web_session_cookie_secure`：HTTPS 反代场景开启 Secure Cookie。
- `web_session_lifetime_minutes`：登录会话有效期。
- `web_enable_terminal`：Web 终端，默认关闭。
- `web_enable_update`：在线更新，默认关闭。
- `web_enable_database_write`：数据库编辑、指令中心、活动数据、发放记录写入。
- `web_enable_backup_restore`：备份、同步、恢复、下载、删除。
- `web_enable_message_send`：消息主动发送、广播、撤回。
- `web_allow_local_upload`：本机免登录上传图片，默认关闭。

## 文件路径约定

备份、下载、恢复、云端同步相关入口必须只接受文件名，不接受目录：

- 本地备份限定在 `data/xiuxian/backups/` 下。
- 数据库备份限定在 `data/xiuxian/backups/db_backup/` 下。
- 配置备份限定在 `data/xiuxian/backups/config_backups/` 下。
- 解压 zip/tar 包必须逐成员校验，禁止绝对路径、`..`、链接和设备文件。
