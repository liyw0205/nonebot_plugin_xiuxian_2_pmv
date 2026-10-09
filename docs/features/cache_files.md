# 运行缓存文件下载

## 用户流程

管理员在旧 Web 控制台点某个缓存产物链接，浏览器请求 `GET /download/<path:filepath>`；本切片只负责把调用方传入的缓存根目录与用户给出的相对路径解析成一个确定文件，旧 Web 层拿到路径后直接 `send_file`。切片本身不生成文件、不写缓存、不做缩略图，也不记录访问：它的全部职责是“这个路径是否允许被这个部署读出来”。需要说明的是，模板与前端脚本里都没有指向 `/download/` 的链接（`xiuxian/xiuxian_web/templates/*.html` 零命中），该路由的真实消费者未证实，因此本次只固化边界，不声称任何界面依赖它。

## 命令与别名

无命令，`commands.py` 声明 `COMMANDS = ()`，本切片不注册任何 NoneBot matcher，聊天里也不存在下载缓存文件的命令或别名。权限不来自 matcher permission，而来自旧端点权限表 `xiuxian/xiuxian_web/access.py:134` 的 `"download_file": WebPermission.READ`，强制点在 `xiuxian/xiuxian_web/core.py` 的 `before_request` 鉴权链。

## Web API

`web.py` 的 `ROUTES` 为空，新适配器尚未接管这条路径；当前由旧模块注册并转发，`LEGACY_ROUTES` 记录的就是这一条：

- `GET /download/<path:filepath>` → `CacheFileApplication.resolve_download`（注册点 `xiuxian/xiuxian_web/system.py:165`，解析在 `:169`，`CacheFileOutsideRoot`/`CacheFileNotRegular` 映射 `403`、`CacheFileNotFound` 映射 `404`，见 `:170-173`）

`GET /download_backup/<filename>`（`xiuxian/xiuxian_web/backups.py:595`）名字相近但走 `plugin_backups` owner 的 `open_plugin_backup`，不经过本切片。迁移到新适配器时按 `LEGACY_ROUTES` 逐条搬，搬完必须同时删掉旧注册。

## 数据模型与迁移

无数据表、无迁移，`migrations.py` 声明 `MIGRATIONS = ()`；`grep -E "CREATE TABLE|ALTER TABLE|sqlite3|db_backend|migration"` 在本目录（排除 tests）零命中。契约在 `schemas.py` 里只有一个：解析失败时的三个拒绝类型 `CacheFileOutsideRoot`、`CacheFileNotFound`、`CacheFileNotRegular`，`repository.py` 只把它们再导出（旧 Web 层 `system.py:34-38` 仍按 `features.cache_files.repository` 这个路径导入）。没有大小、存活期、扩展名白名单这类契约，因为本切片不管理缓存生命周期。

## 事务与失败回滚

只读切片，没有需要回滚的写入。`CacheFileRepository.resolve_download` 先对缓存根与候选路径都 `resolve()`（会跟随 symlink），再用 `relative_to` 判定包含关系：越界抛 `CacheFileOutsideRoot`，不存在抛 `CacheFileNotFound`，不是普通文件（目录、FIFO、设备）抛 `CacheFileNotRegular`。三条分支都在任何读取之前返回，调用方 `send_file` 只在解析成功后执行，因此拒绝路径不产生文件描述符、不改动任何状态。

## 定时任务

无任务，`jobs.py` 声明 `JOBS = ()`。语义相邻的 `cleanup_media_parser_cache_job` 不属于本切片：它注册在 `xiuxian/xiuxian_scheduler/__init__.py:338-352`，执行 `entertainment_application.media_parser.cleanup_cache`，落点是 `features/entertainment/media_parser_cache.py:24`，清理根是 `data/cache/media_parser` 与旧根 `data/media_parser_cache`。也就是说它与本切片共享同一个 `cache` 根目录，但整条链路不调用本切片一行代码。

## 配置项

本切片不读环境变量、不读配置文件，缓存根是每次调用由调用方注入的参数（`application.py` 的 `resolve_download(cache_root, filepath)`）。根的 owner 是 `nonebot_plugin_xiuxian_2/paths.py:57-59`（`cache = data / "cache"`），上游受 `XIUXIAN_DATA_DIR` 与 NoneBot 键 `xiuxian_data_dir` 影响。决定这条路由能否被访问的键都归旧 Web 层：`XIUXIAN_WEB_STATUS`、`XIUXIAN_WEB_PORT`、`web_require_csrf`、`web_allowed_hosts`、`superusers`。备份侧的既有事实是 `features/plugin_backups/schemas.py` 的 `SKIP_DIRECTORY_NAMES` 含 `cache`，即缓存内容不进插件备份。

## 适配器差异

`application.py`、`repository.py`、`schemas.py` 都不导入 Flask 或 NoneBot，纯 `pathlib` 逻辑，因此与适配器类型无关。新适配器侧零命中：`bootstrap/platform_manifest.py` 的 51 条 `RouteSpec` 里没有下载路径，`adapters/web/blueprints/` 也没有对应 blueprint。旧 Web 层承担的只是鉴权、CSRF、异常到状态码的映射和响应封装。

## 测试与手工验收

`python -m unittest discover -s nonebot_plugin_xiuxian_2/features/cache_files/tests -t . -q` 覆盖正常解析、缺失、越界三态与切片契约（异常类型单一来源、空表面声明、注册门面一致）；顶层 `tests/test_cache_file_routes.py` 是 unittest，覆盖旧路由的 200/403/404 映射。手工验收：在隔离数据目录的 `data/xiuxian/cache` 放一个文件，请求正常路径得到 `200`，请求 `../` 越界得到 `403`，请求不存在文件名得到 `404`，且三种情况都不产生新文件。
