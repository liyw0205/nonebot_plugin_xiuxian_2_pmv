# scheduler：任务调度管理面

## 用户流程

管理员在旧 Web 控制台打开调度页，页面拉一次全量任务清单，渲染中文名、触发摘要、下次触发时间与启停开关，然后按需暂停、改计划、手动跑一次并轮询那条手动执行的结局。本切片是这条链的管理面而不是执行面：`features/scheduler/application.py:8-27` 的五个用例方法全是单行委托，不改参数、不改返回值、不吞异常，`features/scheduler/tests/test_scheduler_application.py` 正是按“同一个返回对象、同一个异常实例”把它们钉住的。真正读写 APScheduler、落盘覆盖配置的是 `features/scheduler/apscheduler_manager.py` 的 `SchedulerJobManager`，`manifest.py:6` 的标题「任务调度管理面」指的就是这层控制面职责。

边界要说清楚：本切片不执行任何 job 函数、不在 APScheduler 上注册任何 job，也不决定 job 的业务语义。它只保证管理员能看到清单、能改启停与触发、能排队一次手动执行并查到结局。

## 命令与别名

无命令，`commands.py:3` 声明 `COMMANDS = ()`，本切片不注册任何 NoneBot matcher，聊天里也不存在调度管理命令或别名，管理员唯一的入口是 Web。

权限不来自 matcher permission，而来自旧端点权限表：`xiuxian/xiuxian_web/access.py:14` 定义 `WebPermission.SCHEDULER`，`:67-72` 把 `scheduler_management` 与 5 个 `api_scheduler_*` endpoint 全部映射到它，强制点在 `xiuxian/xiuxian_web/core.py:361-377` 的 `before_request` 链。写操作的 CSRF 由 `core.py:324-340` 校验，页面侧靠 `templates/base.html:33-34` 给 fetch 统一注入 `X-CSRF-Token`。

## Web API

`web.py:10` 的 `ROUTES` 为空：这 5 条 JSON 路径仍由旧模块 `xiuxian/xiuxian_web/scheduler.py` 注册，在同一个 owner 上再声明一遍会造成一条路径两个 owner，所以本切片只把它们记进 `web.py:11-17` 的 `LEGACY_ROUTES`。

- `GET /api/scheduler/jobs` -> `SchedulerAdminApplication.list_jobs`（注册 `xiuxian/xiuxian_web/scheduler.py:12`，转发在 `:14`）
- `POST /api/scheduler/jobs/<job_id>/enabled` -> `SchedulerAdminApplication.set_enabled`（注册 `:17`，`enabled` 校验 `:21-22`，转发 `:24`）
- `POST /api/scheduler/jobs/<job_id>/schedule` -> `SchedulerAdminApplication.reschedule`（注册 `:29`，转发 `:33`）
- `POST /api/scheduler/jobs/<job_id>/run` -> `SchedulerAdminApplication.queue_manual_run`（注册 `:38`，转发 `:41`）
- `GET /api/scheduler/runs/<run_id>` -> `SchedulerAdminApplication.get_run`（注册 `:46`，转发 `:49`）

`GET /scheduler`（`xiuxian/xiuxian_web/scheduler.py:7-9`）只 `render_template("scheduler.html")`，不经过任何 use case，因此不构成委托，不进 `LEGACY_ROUTES`，`web.py:6-7` 对此有明确说明。新适配器侧的 `/scheduler` 与 `/pages/scheduler`（`bootstrap/platform_manifest.py:47`、`:52`）以及 `/api/v1/scheduler`、`/api/v1/scheduler/<job_id>/run`（`:36-37`）都记在 `runtime_web` 清单下（owner 为 `platform`，`platform_manifest.py:6-8`）；`adapters/web/legacy.py:14` 把 `/scheduler` 按 308 重定向到 `/pages/scheduler`（重定向码在 `:40`）。这几条本切片都不声明。

## 数据模型与迁移

无数据表、无迁移：`migrations.py:3` 声明 `MIGRATIONS = ()`，`manifest.py:4-9` 未给 `migration_version`，取 `bootstrap/registry.py:84` 的默认 `None`。持久化也不是 SQLite，而是一个 JSON 文件：`apscheduler_manager.py:34` 的 `SCHEDULE_STORE = get_paths().data / SCHEDULE_STORE_NAME`，文件名与 shape 版本由 `schemas.py:15-16` 单点声明为 `scheduler_overrides.json` 与 `version 1`，缺省结构 `{"version": 1, "jobs": {}}` 在 `:35`，`_load_store`（`:233-236`）读到坏文件也会把版本复位成 1。写盘走 `json_store.save_json_file`（`:238-239`），即临时文件 + `fsync` + `replace`（`xiuxian/xiuxian_utils/json_store.py:69-83`）。`jobs` 映射只存两样东西：`enabled` 布尔与序列化后的 `trigger`。

`repository.py:17-26` 的 `SchedulerAdminManager` Protocol 是本切片的全部依赖面，`schemas.py:17-29` 另外声明了手动 run 前缀 `web-manual:`、运行历史上限 100、cron 字段白名单与最小触发间隔 1 秒这四个契约值。

## 事务与失败回滚

五个用例都在 `SchedulerJobManager` 的 `RLock` 内执行（`:187`、`:478`、`:486`、`:499`、`:516`、`:562`），不存在半读半写。失败路径与状态码一一对应：未知或已消失的 `job_id` 由 `_get_job`（`:438-442`）抛 `ValueError("定时任务不存在")`，`web-manual:` 前缀的临时 job 也在同一处拒绝，管理员无法把手动执行本身当成被管对象；`enabled` 不是 bool 直接 400（`xiuxian/xiuxian_web/scheduler.py:21-22`）；trigger 非法抛 `ValueError` 映射 400（`:26`、`:35`）；`run_id` 未知抛 `ValueError` 映射 404（`:51`）。

写序是先改 APScheduler 内存态、再落 `scheduler_overrides.json`（`apscheduler_manager.py:485-496`、`:498-513`），落盘失败会抛给路由层，但已生效的内存态不回滚，重启后以文件为准——这是既有行为，记录在此而非当作缺陷。`reschedule` 会先记下 `next_run_time is None`（`:501`），重排后若原本是暂停态就重新 `pause_job`（`:507-508`），避免改计划顺手把任务激活。唯一带补偿的是手动 run：`add_job` 抛错时回收 `_manual_jobs` 与 `_runs` 再重抛（`:550-553`）。

## 定时任务

`jobs.py:11` 声明 `JOBS = ()`，本目录一个 job 都不注册，它是调度管理面而非调度执行面。40 个 legacy job id 的 manifest owner 是 `compatibility/legacy_manifest.py` 的 `legacy_scheduler`：`:13-54` 列出这 40 个 id，`:57-73` 为每个 id 生成 `JobSpec(owner="compatibility", schedule="legacy", retry_policy="legacy", idempotency_key="{job_id}:{scheduled_at}")`，该 manifest 在 `plugin.py:230` 导入、`:777` 注册。在这里重复任一 id 都会被 `FeatureRegistry.validate()` 判为重复 job，因此清单只有一份。

真正的注册发生在 `xiuxian/xiuxian_scheduler/__init__.py:33-34`：取 `nonebot_plugin_apscheduler` 的 scheduler 并构造 `SchedulerJobManager(scheduler)`；`xiuxian/xiuxian_scheduler/job_manager.py:3-8` 只是把实现从本目录再导出，不是第二份实现。启动时 `xiuxian/xiuxian_scheduler/__init__.py:84-86` 以 `@register_legacy_startup` 调 `apply_persisted_overrides()`（`apscheduler_manager.py:568-589`）回放覆盖，坏条目只 warn 后跳过（`:589-590`）。手动 run 用 `web-manual:` 前缀建一次性 date job（`:518`、`:539-549`），运行历史按 FIFO 截断到 100 条（`:525-526`），cron 字段名受白名单约束（`:417-418`），interval 下限 1 秒、上限 31 天（`:433-434`）。这四个值由 `features/scheduler/schemas.py:17-29` 单点声明，manager 侧只导入使用（`:21-28`）。

## 配置项

本切片不读环境变量、不读配置文件、不声明 `ConfigSpec`。覆盖文件的位置 owner 是 `nonebot_plugin_xiuxian_2/paths.py`：数据根在 `:78`，受 `XIUXIAN_DATA_DIR`（`:10`）与 NoneBot 键 `xiuxian_data_dir` 影响，落点因此是 `data/xiuxian/scheduler_overrides.json`。决定这条路由能否被访问的键全在旧 Web 层：`XIUXIAN_WEB_STATUS`、`XIUXIAN_WEB_PORT`、`web_require_csrf`、`web_allowed_hosts`、`superusers`。触发器的时区不归本切片，取自 APScheduler 实例（`apscheduler_manager.py:197` 的 `getattr(self._scheduler, "timezone", None)`），由部署侧的 `nonebot_plugin_apscheduler` 配置决定。

## 适配器差异

`application.py`、`repository.py`、`schemas.py`、`web.py`、`jobs.py`、`commands.py` 都不导入 Flask、NoneBot 或 APScheduler，用例只是端口调用；实现由 `runtime.py:6-13` 注入，缺省取 legacy `xiuxian.xiuxian_scheduler.job_manager`，`runtime.py:16` 造出模块级单例，测试可以直接塞一个 Mock。APScheduler 的依赖集中在 `apscheduler_manager.py:10-17`，与 Web 适配器类型无关。

新适配器不是同一份视图：`platform_manifest.py:36-37` 的 `/api/v1/scheduler` 走 `infrastructure/scheduler` 的 `JobRegistry`/`JobExecutor`（`plugin.py:235`），与本切片看到的 APScheduler job 表不是同一个清单，迁移时不能把两者当作等价替换，也不能假设切到新面就自动继承 `scheduler_overrides.json` 里的覆盖。

## 测试与手工验收

本切片的 tests 混着两种风格，两条命令都要跑：`test_scheduler_application.py` 是 pytest 函数式（`@pytest.mark.parametrize`），`unittest` 的 loader 只认 `TestCase`，收不到它；`test_slice_contract.py` 是 unittest，两种收法都能拿到。

- `./.venv/bin/python -B -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/scheduler/tests -q`
- `./.venv/bin/python -B -m unittest discover -s tests -q`

pytest 那条覆盖 5 个用例的参数与返回值透传、以及 manager 异常原样上抛；unittest 那条在本目录收到切片契约 7 例（常量单一来源、`SchedulerAdminManager` 只在 `repository.py` 声明一次、`COMMANDS/ROUTES/JOBS/MIGRATIONS` 全空且 `LEGACY_ROUTES` 恰为那 5 条、manifest 的 `key/test_tag/owner` 与 `migration_version is None`、委托目标确实是 `SchedulerAdminApplication` 上的可调用方法、`_load_store()` 以声明的版本号落盘、`interval` 序列化不会写出 0 秒），在根级收到的相关测试是 `tests/test_scheduler_job_manager.py`（unittest，8 例：启停与改计划落盘、暂停态下手动执行、reschedule 保持 disabled、非法 trigger 被拒、手动 run 如实回报成功与失败、同一 job 二次排队被拒、未知 run_id 被拒）与 `tests/test_scheduler_facade_lazy_reader.py`（unittest，2 例：门面延迟构造）。另有 `tests/test_web_scheduler_routes.py`（pytest 函数式，4 例）覆盖旧路由的管理员门禁、`enabled` 布尔校验、成功与错误信封、queued DTO 与 404 契约，需单独用 pytest 跑。

手工验收：在隔离数据目录起实例，`GET /api/scheduler/jobs` 应列出已注册 job 且不含 `web-manual:` 条目；对某个 job `POST .../enabled` 传 `false` 应返回 `enabled=false` 并落盘，重启后 `apply_persisted_overrides()` 仍保持暂停；`POST .../schedule` 传 `{"type":"interval","seconds":0}` 应 400，传合法 cron 后应看到新摘要且原暂停态不变；`POST .../run` 应立刻返回 `queued` 与 `run_id`，轮询 `GET /api/scheduler/runs/<run_id>` 从 `queued` 走到 `succeeded` 或 `failed`，且再次排队同一 job 应得到 400。
