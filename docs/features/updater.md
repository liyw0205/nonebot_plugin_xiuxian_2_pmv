# updater：版本检查与更新执行

## 用户流程

管理员在旧 Web 更新页点“检查更新”，或从 release 列表里挑一个 tag 点“更新”。本切片是这条链唯一的应用边界：读当前版本、拉 release 列表、判定是否有新版，以及在一次调用里跑完“预检、备份、下载、解包、回滚、清理”。`manifest.py:6` 的标题「版本检查与更新执行」就是职责本身。它自己不碰网络、不读 tar、不写数据库，一切经 `UpdateProvider` 端口（`repository.py:21-44`）落到 legacy `UpdateManager`（`xiuxian/xiuxian_utils/download_xiuxian_data.py:191`）。

聊天侧的 `版本查询`、`检测更新`、`版本更新` 走的是同一批用例（`xiuxian/xiuxian_status/__init__.py:237`、`:253`、`:305-312`），不存在第二套更新实现。更新成功只覆盖文件与版本标记，进程不会自动重启：`templates/update.html:350` 明确提示“请重启机器人”。

## 命令与别名

`commands.py:10` 声明 `COMMANDS = ()`，本切片不注册 matcher。三条 `permission=SUPERUSER` 命令注册在 `xiuxian/xiuxian_status/__init__.py:90-92`，owner 是 status 清单（`features/status/manifest.py:6-8` 的 `key="status"`、`owner="gameplay"`，命令由 `:9` 的 `commands_for("status")` 经 AST 扫描得到，扫描实现 `compatibility/command_inventory.py:105-118`、`:123`）。在这里重复声明会被 `FeatureRegistry.validate()` 判为重复命令。

Web 侧权限集中在一张表：`xiuxian/xiuxian_web/access.py:13` 定义 `WebPermission.UPDATE`，`:119-122` 把 `update`、`check_update`、`get_releases`、`perform_update` 全部映射到它，强制点在 `core.py:361-377` 的 `before_request` 链；POST 的 CSRF 校验在 `core.py:324-340`，页面靠 `templates/base.html:33-34` 注入 `X-CSRF-Token`。

## Web API

`web.py:10` 的 `ROUTES` 为空：这三条路径仍由旧模块 `xiuxian/xiuxian_web/pages.py` 注册，本切片只在 `web.py:11-15` 的 `LEGACY_ROUTES` 里记下委托关系。

- `GET /check_update` -> `UpdateApplication.check_update`（注册 `xiuxian/xiuxian_web/pages.py:59`，handler `:60`，转发 `:65`，响应里再取 `current_version` `:71`）
- `GET /get_releases` -> `UpdateApplication.latest_releases`（注册 `:89`，handler `:90`，转发 `:95` 固定要 10 条）
- `POST /perform_update` -> `UpdateApplication.perform_update_with_backup`（注册 `:106`，handler `:107`，取 body 的 `release_tag` `:113`、缺值直接返回错误 `:115-116`，转发 `:118`）

`GET /update`（`pages.py:53-57`）只 `render_template('update.html')`，不经过任何 use case，因此不构成委托、不进 `LEGACY_ROUTES`，权限同样是 `WebPermission.UPDATE`。`/pages/update`（`bootstrap/platform_manifest.py:62`）与 `/api/v1/**`（`:27-43`）都属 `runtime_web` 清单（owner 为 `platform`，`:6-8`）；该清单里没有 `/api/v1/update` 这一条，新面的 update 目前只有页面重定向：`adapters/web/legacy.py:23` 把 `/update` 按 308 指向 `/pages/update`（重定向码 `:40`）。application 单例在 `core.py:101-102` 构造。

## 数据模型与迁移

`migrations.py:3` 声明 `MIGRATIONS = ()`，`manifest.py:4-9` 未给 `migration_version`，取 `bootstrap/registry.py:84` 的默认 `None`，本切片不拥有任何表。唯一的持久状态是一行纯文本版本标记 `data/xiuxian/version.txt`：读在 `download_xiuxian_data.py:203`，写在 `:552`，经 `mkstemp` + `fsync` + `os.replace` 原子替换（`:554-561`）。

三个契约值由 `features/updater/schemas.py` 单点声明：必需资产名 `UPDATE_ASSET_NAME = "project.tar.gz"`（`:16`）、tag 守卫 `RELEASE_TAG_PATTERN = [A-Za-z0-9][A-Za-z0-9._+-]{0,127}\Z`（`:17`）、列表默认条数 `DEFAULT_RELEASE_LIST_COUNT = 10`（`:18`）；`application.py:9-10` 只导入不再自定义。

## 事务与失败回滚

`perform_update_with_backup` 是一条固定顺序（`application.py:43-109`），每步失败只回滚它之前的东西。

1. tag 先过 `is_valid_release_tag`（`:44-45`，实现 `:15-16`），不合法直接返回“无效 release 标签”，此时 provider 一次都没被调用。
2. 非阻塞抢锁（`:47-48`），抢不到返回“已有更新任务正在执行”，不会排队。
3. `prepare_release_asset` 预检（`:52-54`）；返回的资产名不是 `project.tar.gz` 也在这一步拒掉（`:55-59`）。此步失败什么都没发生，备份尚未开始。
4. 三份备份按 插件、数据库、配置 的固定顺序执行（`:62-73`），任一失败立即返回 `{label}备份失败: ...`，后续备份与下载全部不做，已做成的前几份保留在备份目录里。
5. 下载失败（`:75-79`）只返回错误：备份保留，磁盘上不留半成品归档。
6. `extract_update` 失败（`:82-88`）返回其消息，配置不会被回盖。
7. 只有更新成功才把配置盖回来（`:90-98`），恢复失败仅 `warning`，不把成功翻成失败。
8. `finally`（`:103-109`）无论成败都 `cleanup_download` 并释放锁。

回滚边界必须说清：解包是按目录合并覆盖 `data_root` 与插件目录（`download_xiuxian_data.py:532-534`），任何失败都不会把文件盖回旧版，唯一被恢复的是配置。版本标记只在合并全部完成后才写（`:535`、`:548-568`），不会出现“文件没盖完但版本号已跳走”。锁 `_update_lock` 是类属性（`application.py:20`），只在单进程内串行，多 worker 与多实例不互斥，跨进程并发更新得由部署侧保证。

## 定时任务

`jobs.py:10` 声明 `JOBS = ()`。语义最近的是 `backup_database_files`（每日数据库备份），owner 是 `compatibility/legacy_manifest.py` 的 `legacy_scheduler`：`:13-54` 的 40 个 legacy id 含它，`:61-71` 为每个 id 生成 `JobSpec(owner="compatibility", schedule="legacy")`。它恰好调用与本切片手动更新同一批 provider 方法（`download_xiuxian_data.py:1391-1393` 只是委托给 database_backups owner），但 schedule 声明只能有一份，所以本切片不重复登记。

## 配置项

本切片不读环境变量、不声明 `ConfigSpec`。发布源写死在 provider：`repo_owner`/`repo_name` 与 `api_url`（`download_xiuxian_data.py:193-195`），releases 请求超时 10 秒（`:215`）。`XiuConfig` 只能间接影响备份侧：`cloud_backup_enabled`（`:855`）、`local_backup_keep_days`（`:1396-1400`）、WebDAV 参数（`:632` 起）。路由与鉴权受 `XIUXIAN_WEB_STATUS`、`XIUXIAN_WEB_PORT`、`web_require_csrf`、`web_allowed_hosts`、`superusers` 影响。版本标记位置归 `paths.py`（数据根 `:78`，受 `XIUXIAN_DATA_DIR` 影响）。另外要说清：本切片不持有任何发布周期证据，仓库的实际发版节奏与 tag 命名规约只能从 provider 里写死的 repo 反推，本文不声称已验证。

## 适配器差异

`application.py`、`repository.py`、`schemas.py`、`web.py`、`jobs.py`、`commands.py` 都不导入 Flask、NoneBot、requests、tarfile，provider 以 Protocol 注入，测试塞一个 FakeProvider 就能跑完整更新流；`UpdateManager` 侧的副作用则是实打实的（`download_xiuxian_data.py:1-20` 的 requests、wget、tarfile、tempfile、sqlite3），下载还会先测代理再回落直连（`:303-311`、`:330`），临时目录只在系统 temp 根下（`:300`、`:340-342`），清理时再做一次越界校验（`:869-874`）。

两条与呈现和门禁相关的既有事实：release 元数据必须以文本渲染（`templates/update.html:307` 的 `createTextNode`），以及 `scripts/check_full_refactor_progress.py:3044-3082` 的 `updater_owner` 字面量契约段，钉住了 status 命令走 application、三条 Web 路由走 application、资产预检与备份顺序、文本渲染，以及若干测试函数名（键分别在 `:3045`、`:3051`、`:3059`、`:3067`、`:3074`）。`/api/v1/**` 属 `runtime_web`，与本切片没有委托关系。

## 测试与手工验收

- `./.venv/bin/python -B -m unittest discover -s nonebot_plugin_xiuxian_2/features/updater/tests -t . -q`
- `./.venv/bin/python -B -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/updater/tests -q`

本目录 `__init__.py` 齐备，两条命令收到的是同一批 unittest 用例。`test_application.py` 覆盖 tag 校验、只读委托与 `check_update` 判定、preflight 失败停在备份之前、错误资产被拒、备份顺序与首败即停、透传已验证资产与精确 tag 并清理归档、配置恢复失败仍算更新成功、解包失败仍清理归档、锁非阻塞且跨实例共享、provider 抛错仍释放锁。`test_manager_adapter.py` 覆盖 preflight 要求官方资产、拒绝非官方下载地址、下载失败删除临时目录、坏归档不写版本也不建目标目录、版本标记记录请求的 tag，以及三组备份兼容方法委托给各自 owner。`test_slice_contract.py` 是切片契约 8 例：三个值只在 `schemas.py` 声明一次、`UpdateProvider` 只在 `repository.py` 声明一次、`COMMANDS/ROUTES/JOBS/MIGRATIONS` 全空且 `LEGACY_ROUTES` 恰为那 3 条、manifest 的 `key/test_tag/owner` 与 `migration_version is None`、委托目标确实是 `UpdateApplication` 上的可调用方法、tag 守卫接受线上真实 tag 而拒绝带路径的输入、列表默认条数取声明值、资产名正是拒绝规则本身。

根级相关测试：`tests/test_updater_web.py`（unittest，14 例，含 `test_update_routes_require_admin_and_keep_update_permission`、`test_update_page_does_not_interpolate_release_metadata_as_html` 以及备份路由组）与 `tests/test_phase2_legacy_path_gate.py`（`:1114-1129` 把 `pages.py:60`、`:90`、`:107` 到 `UpdateApplication` 的三条调用边钉成“已迁移”），两者都是 unittest，`unittest discover -s tests -q` 会一并收到。

手工验收：在隔离实例打开更新页，`GET /check_update` 在最新版本下应返回 `update_available=false` 与当前版本；`GET /get_releases` 应返回不超过 10 条且 release 名称以纯文本渲染；`POST /perform_update` 传不合法 tag（例如含 `/`）应在不产生任何备份的前提下返回“无效 release 标签”，传一个不存在的合法 tag 应停在预检且同样不产生备份，传合法且存在 `project.tar.gz` 的 tag 应依次生成插件、数据库、配置三份备份、更新后 `version.txt` 等于该 tag、临时归档被清空，且机器人不会自己重启。
