# 表情包目录与安装

## 用户流程

管理员在旧控制台的“表情包”页浏览贴纸目录，点安装后由后台线程拉取固定 release 的 zip，校验摘要与成员清单再解包换入，页面轮询安装进度直到 `complete` 或 `error`。聊天侧没有贴纸命令；发送贴纸是 `messages` 切片经 `resolve_sticker_path` 把 `<pack_id>/<file>` 令牌解析成本地 `.webp` 后当作图片发送。目录读取优先用本地已安装清单与远端清单缓存，`?refresh=1` 才强制回源。

## 命令与别名

无命令，`commands.py` 声明 `COMMANDS = ()`，本切片不注册 matcher，也不存在 legacy 贴纸包（`find` 全仓只有 `features/stickers` 这一个贴纸目录）。可以称得上“别名”的只有入参层：`pack_id`/`force` 同时接受 JSON body 与 query string（`xiuxian/xiuxian_web/stickers.py:46`、`:49`），布尔值接受 `1/true/yes/on` 字面量（`:28-33`、`:49-54`），发送令牌接受 `sticker` 与 `sticker_token` 两个键，令牌可省略扩展名并由 `resolve_sticker_path` 补 `.webp`（前端脚本 `templates/messages.html:4312` 另有一份同样的补全）。

## Web API

`web.py` 的 `ROUTES` 为空，四条路径都还挂在旧模块 `xiuxian/xiuxian_web/stickers.py`，每条 handler 自带 `_require_admin()`，POST 另需 CSRF，权限表在 `xiuxian/xiuxian_web/access.py:115-118`：

- `GET /api/messages/stickers` → `StickerApplication.catalog`（`stickers.py:22`）
- `POST /api/messages/stickers/install` → `StickerApplication.start_install`（`stickers.py:39`；作业仍在跑时返回 `202`，否则 `200`）
- `GET /api/messages/stickers/install/<job_id>` → `StickerApplication.install_status`（`stickers.py:62`；未知作业 `404`）
- `GET /api/messages/stickers/file/<pack_id>/<path:filename>` → `StickerApplication.resolve_file`（`stickers.py:73`；`mimetype=image/webp`、`Cache-Control: private, max-age=86400`、`Content-Disposition: inline`）

`LEGACY_ROUTES` 按这四条逐条登记，迁移时搬一条删一条。

## 数据模型与迁移

无数据表、无迁移，`migrations.py` 声明 `MIGRATIONS = ()`；`grep -E "CREATE TABLE|ALTER TABLE|sqlite3|db_backend|migration"` 在本目录（排除 tests）零命中。持久产物是文件，落在 `get_paths().data / "stickers"`：`packs/<pack_id>/pack.json` 与 `*.webp`、根级 `manifest.json`（本地已安装清单）、`remote-manifest.json`（远端清单缓存）。契约全部在 `schemas.py`：仓库坐标 `FILE_REPO_OWNER`、`FILE_REPO_NAME`、`STICKERS_RELEASE_TAG`、`STICKERS_MANIFEST_NAME`；上限 `MAX_STICKER_ARCHIVE_BYTES`、`MAX_MANIFEST_BYTES`、`MAX_STICKER_FILES`、`MAX_STICKER_ARCHIVE_MEMBERS`、`MAX_STICKER_FILE_BYTES`、`MAX_STICKER_UNCOMPRESSED_BYTES` 与 `DOWNLOAD_CHUNK_BYTES`；超时 `MANIFEST_TIMEOUT_SECONDS`、`ARCHIVE_TIMEOUT_SECONDS`；名字校验 `PACK_ID_PATTERN`、`STICKER_FILE_PATTERN`、`ZIP_NAME_PATTERN`、`SHA256_PATTERN`、`STICKER_TOKEN_PATTERN`；网络边界 `ALLOWED_DOWNLOAD_HOSTS`、`INITIAL_REQUEST_HOSTS`、`DOWNLOAD_PROXY_PREFIX`、`DOWNLOAD_USER_AGENT`。`repository.py` 现在只导入这些名字（部分沿用模块内原有的私有拼写），改一处即全局生效。

## 事务与失败回滚

安装以进程内 `_INSTALL_LOCK`（`repository.py` 模块级 `RLock`）互斥，同一进程不会并发换入同一个包。流程是：拉清单校验 → 下载归档到临时 `.{pack_id}-*.zip` → 逐个成员校验名称、数量、压缩前后大小与 sha256 → 解包到 `.{pack_id}.staging-*` → `os.replace` 目录换入，旧目录先改名成 `.{pack_id}.backup-<uuid>` → 重写本地清单。清单与 `pack.json` 都经 `infrastructure/filesystem/atomic.py` 的 `atomic_write` 落盘（写临时文件、`fsync`、`os.replace`、目录 `fsync`）。任一步失败即回滚：换入失败把 backup 改回原名，成功或失败都在 `finally` 删除临时归档与 staging 目录，绝不留下半个包目录。发送侧只读解析不参与这套事务。

## 定时任务

无任务，`jobs.py` 声明 `JOBS = ()`，`features/scheduler` 与 `xiuxian/xiuxian_scheduler` 对贴纸零命中。安装进度只存在于 `StickerApplication._jobs` 这个进程内 dict 加 `_jobs_lock`，进程重启即丢失，且当前没有淘汰策略，这是一个已知的内存增长边界；贴纸文件本身也没有过期回收，只靠上面那套同步清理。

## 配置项

本切片不读配置键、不读环境变量，也不声明 `ConfigSpec`。唯一取值来源是数据根：`factory.py:12-20` 接受 `root`/`open_url`/`thread_starter` 三个注入口，默认根是 `get_paths().data / "stickers"`，受 `XIUXIAN_DATA_DIR` 与 NoneBot 键 `xiuxian_data_dir` 影响。`runtime.py:4` 在 import 期就固化该根，之后的 `configure_paths` 不会影响已建单例。可达性仍取决于旧 Web 层的 `XIUXIAN_WEB_STATUS`、`XIUXIAN_WEB_PORT`、`web_require_csrf`、`web_allowed_hosts`、`superusers`。

## 适配器差异

切片不导入 Flask 或 NoneBot；HTTP 抓取与线程启动都是注入点（默认 `_open_scoped_url` 与 daemon 线程），测试可换成假远端。已知的重复实现有两处，都不归本 owner 因而保持原样：旧层 `xiuxian/xiuxian_web/stickers.py:12-13` 自己又编译了一份 pack-id 与文件名正则，目录 JSON 里的 `url` 字段也仍按 `/api/messages/stickers/file/{pack_id}/{filename}` 拼装（`features/stickers/repository.py:322`、`:335`），路由一旦改路径必须同步 catalog 输出。新适配器零命中：`adapters/`、`bootstrap/`、`compatibility/` 对 sticker 全无引用。

## 测试与手工验收

`python -B -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/stickers/tests -q` 覆盖目录合并、安装成功与失败回滚、成员数量与大小上限、摘要不符、令牌解析与私有端口注入（9 例，`tmp_path`/`monkeypatch` fixture）；同目录 `tests/test_slice_contract.py` 是 unittest，断言契约单一来源、别名指向同一对象、release URL 仍由声明的坐标拼出、空表面与四条 `LEGACY_ROUTES`。注意本目录既有测试是 pytest 函数式写法，`unittest discover` 收集不到它们，两条命令都要跑。顶层 `tests/test_web_stickers_routes.py` 与 `tests/test_phase2_legacy_path_gate.py` 覆盖四条旧路由到 `sticker_application.<method>` 的源码绑定。手工验收：在隔离数据目录安装一个包，确认 `packs/<pack_id>` 落地、`manifest.json` 版本号递增、中断一次安装后不留 `.{pack_id}.staging-*` 与临时 zip，且 `GET /api/messages/stickers/file/...` 能取回 `.webp`。
