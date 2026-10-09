# database_console：数据库控制台

## 用户流程

管理员在旧 Web 控制台进「数据库管理」页看库与表的清单（`GET /database`），点某张表进数据页翻页与搜索（`GET /table/<table_name>`），点某一行改字段或删行（`GET,POST /table/<table_name>/<row_id>`），也可以在数据页发起一次批量改值（`POST /batch_edit/<table_name>`）。数据库控制台负责三件事：把注入的库表目录变成可选清单，把管理员的读、改、批量请求翻译成一条参数绑定的 SQL，再把结果整形交回旧模板渲染。

它不建表、不改表结构、不迁移、不维护连接池，也不记录谁改了哪一行。写入面只有三个：单行 `UPDATE`、单行 `DELETE`、按筛选条件的一条批量 `UPDATE`。批量编辑额外要求「有显式筛选条件」或「显式勾选应用到整张表」，这是本切片最重要的护栏。

## 命令与别名

无命令：`commands.py` 声明 `COMMANDS = ()`，`manifest.py` 也没有 `CommandSpec`，聊天里不存在数据库命令或别名。旧面板「命令执行」页的 `execute_command`（`xiuxian/xiuxian_web/access.py:74`）属 command registry / admin owner，不经过本切片。

四个表面全空是事实而不是缺项：`web.py` 声明 `ROUTES = ()`、`commands.py` 声明 `COMMANDS = ()`、`jobs.py` 声明 `JOBS = ()`、`migrations.py` 声明 `MIGRATIONS = ()`，`features/database_console/manifest.py:4-9` 只给 `key/title/owner/test_tag`，于是按 `bootstrap/registry.py:80-84` 的默认值 `migration_version=None`。真实归属在别处：路由注册点是旧模块 `xiuxian/xiuxian_web/database.py:49,56,96,146`，表结构由各业务 owner 的迁移与启动期建表逻辑拥有，`GET /database` 更是 `runtime_web` 已声明的路径。因此不能声称任何路由、命令或定时任务由本切片声明，`web.py` 的 `LEGACY_ROUTES` 只是搬迁清单。

权限不来自 matcher permission，而来自旧端点权限表：`xiuxian/xiuxian_web/access.py:82` 的 `database` 是 `READ`、`:83` 的 `table_view` 是 `READ`、`:84` 的 `row_edit` 是 `GET READ` + `POST DATABASE_WRITE`、`:85` 的 `batch_edit` 是 `DATABASE_WRITE`；逐 method 解析在 `access.py:144-153`，强制点在 `xiuxian/xiuxian_web/core.py` 的鉴权链（`core.py:270-305`）与 CSRF 校验（`core.py:324-340`）。`DATABASE_WRITE` 只是共享权限档，`access.py:28,32-33,65-66,74,76-77,99,126-128` 的端点同档但属别的 owner。

## Web API

`web.py` 的 `ROUTES` 为空：四条路径都由旧模块 `xiuxian/xiuxian_web/database.py` 注册（管理员会话 + CSRF）后转发到 `DatabaseConsoleApplication`，`LEGACY_ROUTES` 登记的就是这四跳。

- `GET /database` -> `DatabaseConsoleApplication.list_tables`（注册 `database.py:49`，调用 `:53`）
- `GET /table/<table_name>` -> `DatabaseConsoleApplication.table_data`（注册 `database.py:56`，目录解析 `:61`，取数 `:75-83`）
- `POST /table/<table_name>/<row_id>` -> `DatabaseConsoleApplication.update_row`（注册 `database.py:96`，`action=update` 分支 `:112-123`）
- `POST /batch_edit/<table_name>` -> `DatabaseConsoleApplication.batch_edit`（注册 `database.py:146`，未登录直接 `{"success": false}` 于 `:148-149`，转发 `:150`）

同一条 `GET,POST /table/<table_name>/<row_id>` 上，两种方法都先用 `row_key` 还原主键（`database.py:105-107`，格式非法返回 `400`）；GET 分支走 `read_row` 并渲染 `row_edit.html`（`database.py:127-143`，无记录 `404`），POST 分支才写：`action=update` 走 `update_row`，`action=delete` 走 `delete_row`（`database.py:124-125`）。

`GET /database` 同时被新适配器的 `runtime_web` 拥有：`bootstrap/platform_manifest.py:46` 已声明 `RouteSpec("/database", permission="admin")`，本切片不得再声明同名 `RouteSpec`（`web.py:3-6` 的注释记的就是这条约束）。`/pages/database`（`platform_manifest.py:51`）、`/api/v1/database`（`:32`）、`/api/v1/database/<key>/tables`（`:33`）、`/api/v1/reconcile`（`:34-35`）也都属 `runtime_web`，由 `adapters/web/blueprints/database.py:28,36,46,58` 提供，是另一套只读实现，不调用本切片一行代码。

## 数据模型与迁移

无 SQLite schema、无迁移：`migrations.py` 声明 `MIGRATIONS = ()`、`migration_version=None`，因为表都是别的 owner 建的，本切片只读写既有表内容。

契约面是注入的 7 个端口（`repository.py:26-43`）：`tables_provider`（库表目录）、`dynamic_table_providers`（4 组 `(库路径, provider)`）、`database_tables_provider`（按库取 `table_info`）、`connection_factory`、`execute_sql`、`sql_ident`、`sql_like_text`。旧层在 `xiuxian/xiuxian_web/database.py:30-46` 用 `core.py` 的 `get_tables`（`:852-854` → `get_config_tables`，`:639-667`）、四个动态 provider（`core.py:669` 起）、`get_database_tables`（`:856-883`）、`get_db_connection`（`:885-889`）、`execute_sql`（`:891-893`）装配；`connection_factory` 的返回值除 `cursor()` 外还必须带 `table_exists()`（`repository.py:187-192` → `db_backend.py:264-269`）。

表白名单不硬编码，来自 provider 返回的目录（`repository.py:54-63`）；字段必须出现在 `table_info.fields`，否则读页返回「搜索字段不存在」（`repository.py:92-96`），批量编辑返回「批量修改字段不存在」/「搜索字段不存在」（`application.py:120-124`）。

主键取 `table_info.primary_key`，可以是字符串也可以是列表；`impart_cards` 用 `user_id` + `card_name` 组合主键（`core.py:843-848`），`row_key` 对这张表按下划线切分 `row_id`：首段作 `user_id`，其余用 `_` 重新拼回 `card_name`（`application.py:43-48`，表名与主键名来自 `schemas.py` 的 `IMPART_CARDS_TABLE`/`IMPART_CARDS_PRIMARY_KEYS`）。落盘文件是 `data/xiuxian/xiuxian.db`、`xiuxian_impart.db`、`player.db`、`trade.db` 与 `data/xiuxian/activity/activity.db`（`paths.py:24-38`、`core.py:409-414`，其中 `ACTIVITY_DB` 就是主库同一个文件）。数值读取经 `format_plain_number`、写入经 `parse_web_number`（`xiuxian/xiuxian_utils/numeric_bind.py:360,393`），空串写 `NULL`（`application.py:83`）。

## 事务与失败回滚

值一律 `%s` 绑定、标识符一律 `quote_identifier`：`sql_ident` 落到 `db_backend.quote_ident`（`core.py:84-85`、`xiuxian_utils/db_backend.py:68-69`），`%s` 由后端统一改写成 `?`（`db_backend.py:130,206-208`）。唯一被拼进 SQL 文本的用户输入是 `search_condition`，白名单只有 `"="` 与 `">"`/`"<"`（`schemas.py` 的 `LIKE_SEARCH_OPERATOR`、`RANGE_SEARCH_OPERATORS`），落到白名单外就返回「无效的搜索条件」（`repository.py:138-139`、`application.py:186-187`）；数值区间的取值个数上限为 2（`MAX_RANGE_SEARCH_VALUES`），读页超出即「搜索值过多」（`repository.py:116-117`）。批量分支只取前两个值、多余 token 静默忽略（`application.py:163-185`），这条与读页不一致，属已记录差异。

三个写用例都是单条语句，SQLite 单语句天然原子。`execute_sql` 端口在写成功时 `commit` 并返回 `{"affected_rows": n}`，异常被 `execute_sql_safely` 吞成 `{"error": ...}`（`db_backend.py:386-405`），而 `connection()` 只在 finally `close()`（`db_backend.py:323-332`），未提交内容随连接关闭丢弃。

批量编辑最重要的护栏是空筛选拒绝：「请填写搜索内容，或勾选应用到整张表」（`application.py:125-126`）；`apply_to_all=on` 是唯一允许不带 `WHERE` 的通道（`application.py:117,150`），模板 `table_view.html:94-100` 已把风险写在勾选项下方。批量只接受 `set`/`add`/`subtract`，其它值返回「无效的操作类型」（`application.py:141-148`）。失败统一回 `{"success": False, "error": ...}`，`table_data` 的异常也走空结果（`repository.py:181-182,194-205`）。本切片没有跨语句事务、没有撤销、没有审计写入，误操作的恢复路径是 `database_backups` owner 的库备份。

## 定时任务

无任务：`jobs.py` 声明 `JOBS = ()`，`manifest.py` 无 `JobSpec`。表内容只在一次 Web 请求里变化；`xiuxian/xiuxian_scheduler` 与各业务 owner 的 job 会写业务库，但都不经过数据库控制台。

## 配置项

本切片不拥有任何配置键（`manifest.py` 无 `ConfigSpec`）。决定这条链路能否使用的键都归别人：`web_enabled/web_host/web_port/web_secret_key/web_admin_ids/web_allowed_hosts`（`bootstrap/platform_manifest.py:11-16`）、`web_require_csrf`（`core.py:325`）、`web_allowed_hosts` 的 Host 校验（`core.py:343-345`）、`superusers` → `ADMIN_IDS`（`core.py:415`）、`XIUXIAN_WEB_PORT`（`core.py:417-426`）、`XIUXIAN_WEB_STATUS`（`xiuxian_web/web_runtime.py:9-16`），以及 `XIUXIAN_DATA_DIR`（`paths.py`），最后一个直接决定可编辑库文件的位置。

分页与区间边界由 `features/database_console/schemas.py` 单点声明：每页上限 `MAX_TABLE_PAGE_SIZE = 200`、默认 `DEFAULT_TABLE_PAGE_SIZE = 10`、下限 `MIN_TABLE_PAGE_SIZE = 1`、区间值上限 `MAX_RANGE_SEARCH_VALUES = 2`，`repository.py:76-84,116` 只用这些名字而不再写裸数字。旧层 `xiuxian/xiuxian_web/database.py:69` 仍有一份写死的 `200`（默认 `20`），属已知重复，本切片不动旧层。

## 适配器差异

`application.py` 与 `repository.py` 都不导入 Flask 或 NoneBot，只依赖 `xiuxian_utils/numeric_bind.py` 的纯函数，因此与适配器类型无关；会话、CSRF、模板、JSON 信封与状态码映射都在旧层。

新适配器上没有这套控制台的写面：`GET /database` 在 `adapters/web/legacy.py:11-24` 的 `LEGACY_REDIRECTS` 里以 308 指向 `/pages/database`，后者渲染 `adapters/web/templates/pages/admin.html` 并打 `/api/v1/database`（`adapters/web/blueprints/pages.py:13-24,92-97`），列库/表走 `catalog.readonly_query`（`adapters/web/blueprints/database.py:36-44`），权限也从 `WebPermission` 换成 `guard("admin", ...)`。挂新适配器时本切片整体不可达：`/table/<table_name>`、`/table/<table_name>/<row_id>`、`/batch_edit/<table_name>` 都没有注册。

## 测试与手工验收

`./.venv/bin/python -B -m unittest discover -s nonebot_plugin_xiuxian_2/features/database_console/tests -t . -q` 与 `./.venv/bin/python -B -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/database_console/tests -q` 跑同一批用例：两条命令都跑同一目录的两个 unittest 文件：`test_application.py` 覆盖 `impart_cards` 组合主键的下划线切分、空筛选必须拒绝且不落到 `execute`、`add` 操作的绑定值；`test_slice_contract.py` 断言分页与区间边界只在 `schemas.py` 一处定义、四个表面为空、`LEGACY_ROUTES` 与清单一致且每条 target 都是真实公开方法、`per_page` 在下任何查询前先被夹到 `MAX_TABLE_PAGE_SIZE`。

根级相关测试：`tests/test_operations_console_slice_manifests.py`（注册表与文档契约：要求本文出现标题、九个小节标题、以及四条委托的 path 与 target，并禁止本切片与 `runtime_web` 双声明同一路径）、`tests/test_web_database_batch_edit_guard.py`（空或空白筛选必须零执行且不建连接、单字段与全字段 `LIKE` 绑定、`apply_to_all` 时无 `WHERE`）、`tests/test_web_auth.py:123` 的 `test_every_web_endpoint_declares_permission`、`tests/test_db_backend.py`（`%s` 到 `?` 的改写与 `execute_sql` 的返回形状）。

手工验收：在隔离数据目录起单进程面板，进 `/database` 选一张动态表，翻页与 `=`、`>` 两种搜索各跑一次；改一行、删一行；再发一次带搜索词的 `set` 与一次勾选 `apply_to_all` 的 `subtract`，确认返回的影响行数与库里实际行数一致。把 `search_value` 留空提交，必须看到「请填写搜索内容，或勾选应用到整张表」且前后行数不变。
