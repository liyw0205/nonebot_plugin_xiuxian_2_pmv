# 重构当前进度

状态：2026-10-10 C1-C4 技术收口完成、待发布；当前发布 P7 独立受阻。
唯一执行入口：[重构有限收口协议](refactor_slice_execution_protocol.md)。
固定目标：[refactor-closeout-v1](refactor_closeout_scope_v1.md)。
完整旧账本：[历史进度](archive/refactor-2026-10-09/full_refactor_progress.md)，
历史方案：[历史协议](archive/refactor-2026-10-09/refactor_slice_execution_protocol.md)。
历史“权威状态”“下一片”“自动继续”等文字均不驱动当前执行。

当前结果：八项 collection、两项 Dongfu 旧断言及全量揭示的必要失败已修复，
默认根目录全量为 6148 passed、304 subtests passed；P0-P6、inventory 和 Phase 2 通过。
CONTRIBUTING 检查已通过，本提交按白名单交付 `origin/refactor/full-bottom-layer`；
普通推送及远端 HEAD 核验以最终交付回执记录，核验成功后才 complete。
用户 `boss_info.json` 不暂存；P7 独立受阻，不称全部重构或当前发布已完成。
下方分开记录 2026-10-09 失败历史和 2026-10-10 实际复验，不把旧失败状态当作当前状态。

## 当前证据

- 停止时分支为 `refactor/full-bottom-layer`，HEAD
  `e17cbec8fb6645c806288fe99e4a93b553f5613e`；保留未提交批次和用户修改。
  主线程核对该分支相对 `main` 已有 2120 个提交（HEAD 历史共 3566 个）；
  提交数只说明现状，不是收口指标，也不授权继续增加玩法切片。
- 本轮 P0-P6 均为 `ready=true/errors=[]`；inventory freshness 已修复并复验。
  Phase 2 `496` 项：已迁移 `338`、允许兼容 `139`、不可达 `19`、受阻 `0`；
  `integrity_errors=[]`、membership/source provenance 有效，
  `phase2_complete=true/exit_ready=true/exit_blockers=[]`。这些结果不证明全部重构或 P7。
- 普通闭关的真实拒绝重放和消息消费者预算泄漏均已先复现、再最小修复，相关回归通过。
  2026-10-09 根目录基线因 8 项 collection error 中止，另有 2 项旧 Dongfu 源码断言失败。
  2026-10-10 修复后保留原全量句柄至退出，获得 28 failed、6119 passed 的完整失败集合；
  必要修复后根目录全量实际为 6148 passed、304 subtests passed，零失败、零收集错误、零跳过。
- 隔离五库/config 迁移、迁移失败回滚、readiness、迁移前后快照恢复与 reconcile 已通过。
  当前发布版本、正式数据目录和真实发布恢复证据仍未提供；P7 `ready=false`，
  completion audit 总体 `ready=false`。合成恢复测试和旧 `v1.1.0` 记录不能代替当前 P7。

## 固定清单状态

| ID | 当前状态 | 证据及限制 |
|:--|:--|:--|
| C1 | 通过 | 既有工作树整合审查、接口和文件边界核对完成；用户修改保留 |
| C2 | 通过 | 九个固定边界及本轮完整失败集合均经回归验证；旧四项报告原始 traceback 缺失的历史事实保留 |
| C3 | 通过 | 6148 passed、304 subtests passed；unittest 2946 tests OK；编译/架构/P0-P6/inventory/Phase 2/diff 通过 |
| C4 | 通过 | 本轮最终全量复跑五库/config 恢复及两项 Activity recovery smoke；仅隔离合成数据，不作为发布证据 |
| C5 | P7 受阻；本地交付验收通过 | 白名单和用户文件边界已审查；本提交交付当前重构分支，普通 push 和远端 HEAD 由最终回执核验 |

后续直接更新本表和结果段，注明具体命令、结果、环境和未运行项；
不重新把历史流水搬回本文件，不为延期/backlog自动创建下一切片。
若资源阈值、真实发布证据或环境阻塞验收，记录缺失条件并停止无变化的重复尝试。

## C1 文件边界

初始 `git status` 为 32 个已跟踪改动，`git ls-files --others --exclude-standard`
展开为 75 个未跟踪文件。既有批次包括七 owner 契约/文档/注册、RAM/md/map 修改、
sign_in/compensation 证据和 Phase 2 行锚；用户文档整理及两份历史归档原样保留。
七 owner 的 schema/public exports、manifest 注册、迁移 owner、空 surface 声明及既有
adapter 委托与真实 application/repository 一致，未添加第二套 matcher、Web route 或 runner。

2026-10-09 首轮新增写入白名单为以下 13 个文件，其余当时初始文件保持原样：

- `nonebot_plugin_xiuxian_2/xiuxian/xiuxian_buff/__init__.py`
- `nonebot_plugin_xiuxian_2/features/buff/tests/test_closing_enter.py`
- `tests/test_buff_closing_enter.py`
- `nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/message_db.py`
- `tests/test_message_db_queue_budget.py`
- `scripts/export_refactor_inventory.py`
- `tests/test_refactor_inventory.py`
- `docs/refactor_inventory.json`
- `scripts/check_full_refactor_progress.py`
- `nonebot_plugin_xiuxian_2/features/sign_in/tests/test_sign_in_adapter.py`
- `docs/features/sign_in.md`
- `tests/test_refactor_closeout_recovery.py`（2026-10-09 首轮唯一新增仓库文件）
- `docs/full_refactor_progress.md`

2026-10-09 首轮未暂存、提交、推送、合并或发版，结束时 HEAD 保持冻结值。
用户 `boss_info.json` SHA256 保持
`84b7ef679eb85ddc1a28b62846c7a134072da2d200fad9a689428a9bdb15e517`，
该文件始终排除自动暂存/提交。

2026-10-10 恢复时基线为 38 个 tracked dirty、76 个展开后的 untracked 文件。
当前提交白名单共 155 个文件：该基线排除 boss 后的 113 个相关文件，加以下 C3 必需修改。
使用逐文件 `git add -- <白名单>`，不使用 `git add -A`，不提交运行数据或任务临时产物。

- 运行源码 7 个：`__main__.py`、`features/activity/{admin_data_application,config_application,config_repository}.py`、`features/work/effects.py`、`xiuxian/xiuxian_activity/activity_config.py`、`xiuxian/xiuxian_entertainment/mod/media_parse_link.py`（均位于 `nonebot_plugin_xiuxian_2/`）。
- feature 既有测试 19 个：auction、daily_fortune、natal_treasure、past_life、simulator、tianti 的迁移夹具，以及 config/database/manual/plugin backups、scheduler、stickers 的内部导入维护；实际路径均在对应 `features/<owner>/tests/`。
- 根目录既有测试 14 个：`tests/architecture/test_legacy_application_contract.py`、`tests/test_{activity_admin_data,activity_config_application,base_facade_lazy_reader,bounded_http_json,cache_boundaries,entertainment_delete_contract,map_resource_reward_service,source_quality,status_lazy_reader,task_progress_event_transaction,tower_storage_lazy_reader,training_storage_lazy_reader,admin_broadcast_ingress}.py`。
- 新增测试包标记 2 个：`features/natal_treasure/tests/__init__.py`、`features/trade/tests/__init__.py`（同一 package 前缀）；没有新增玩法或扩大冻结 membership。

交付基线 SHA 不冒充交付 SHA；当前提交 SHA 和远端 HEAD 由最终回执及 `git log` 核验。

## C2 行为与修改

| 固定边界 | 本轮证据 |
|:--|:--|
| 七 owner | 契约及已有 application/repository 回归通过；另补跑 economy ledger、QQ bind、logs、admin config 的 Web/application 集合 33 passed、9 subtests passed，权限和委托保留 |
| 消息队列与限流 RAM | 队列 admission 的估算字节预算包含在途 batch；12 个 producer 并发不过量。发现真实消费者原先从不 release，且重建首个 tuple 会丢失计费 identity；新增 success/failure/disabled 三种消费回归修复前均失败，修复后均释放 reservation/size map，下一任务可继续入队，并在阻塞读取前释放 batch payload 引用 |
| Cooldown 关闭 | key 上限 65536、限流用户上限 100000、ID 长度 512；计数/reset 并发、超预算拒绝、TimerHandle 取消、状态/预算释放和晚到回调幂等回归通过；不声称 RSS 或多 worker 全局硬上限 |
| 管理员 md 模板 | UTF-8 输入 64 KiB、32 参数、列表 32 值上限在归一化前生效；模板/按钮别名与 URL 归一化回归通过 |
| map 交互开始 | SQL 默认仓储 5 项通过：成功/重放、未到期冷却、冷却陈旧快照拒绝、过期 active action 恢复、活动中拒绝；拒绝不扣体力 |
| sign_in 证据 | 正向 task/lottery/application 与显式 legacy fallback gate 回归通过；关闭新 feature 不会自动启用旧 matcher，回滚文档已说明需 `XIUXIAN_SIGN_IN_LEGACY_HANDLER=true`。Web 夹具原漏 platform/sign_in schema，补迁移准备后 3 passed，仍走原 CSRF/Idempotency-Key 接口 |
| compensation 聚合 | Web serializer -> common -> CompensationApplication -> repository -> COUNT/GROUP BY/LEFT JOIN；真实 SQL 聚合及只读 UoW 测试通过，未改业务入口或奖励玩法 |
| P0 inventory freshness | 首次结构比较唯一新增表 token 为 Python import 的 `weakref`。扫描改为 AST 非 docstring 字符串字面量，排除 import/comment/docstring；移除 87 个旧伪表 token，保留 568 个静态 token，其余 inventory sections 无本轮变化。仍是静态 SQL token 清单，不是运行库 schema 穷举；代表性 CREATE/SELECT/JOIN/f-string 回归、完整生成一致性和 `--check` 通过 |
| 普通闭关拒绝重放 | 新测试执行真实 BuffApplication、ledger、game/player UoW 和 AST 抽取的原 async handler/operation helper；busy/user_missing/state_changed 修复前 3 failed，成功重放 1 passed。成功回复加 `result.ok` 条件后 10 项 application/handler 回归通过，拒绝不发成功文案/日志、不改 cd/统计、不写成功 receipt；成功重放仍仅一次统计与日志。未修改全局 OperationOutcome.replay 语义 |

## C3 命令与结果

### 2026-10-09 首轮记录

所有项目 import 前先 `import tests`，设置任务专属 `XIUXIAN_TEST_DATA_DIR` 和 `TMPDIR`，
再初始化 NoneBot。进程使用 `.venv/bin/python -B`、`PYTHONDONTWRITEBYTECODE=1`；
pytest 使用 `-q -p no:cacheprovider --basetemp=<任务目录> --junitxml=<任务目录>`，串行执行。
专属目录为 `/tmp/xiuxian-closeout-v1-xb2H4OED`。以下为实际 argv 的可复放形式：

```bash
closeout_tmp=$(mktemp -d /tmp/xiuxian-closeout-v1-XXXXXXXX)
mkdir "$closeout_tmp/scratch"
export PYTHONDONTWRITEBYTECODE=1 TMPDIR="$closeout_tmp/scratch"
export XIUXIAN_TEST_DATA_DIR="$closeout_tmp/import-data"
# Each independent run uses a fresh import-data and basetemp directory.
.venv/bin/python -B -c 'import sys, tests, nonebot; nonebot.init(); import pytest; raise SystemExit(pytest.main(sys.argv[1:]))' \
  -q -p no:cacheprovider --basetemp="$closeout_tmp/pytest" \
  nonebot_plugin_xiuxian_2/features/{economy_ledger,fallback,game_events,group_lifecycle,logs,plugin_config,qq_bind}/tests \
  nonebot_plugin_xiuxian_2/features/map/tests/test_map_interactive_start_repository.py \
  nonebot_plugin_xiuxian_2/features/buff/tests/test_closing_enter.py \
  nonebot_plugin_xiuxian_2/features/{sign_in,compensation}/tests \
  tests/test_buff_closing_enter.py tests/test_message_db_queue_budget.py \
  tests/test_layout_rate_limit_state.py tests/test_admin_markdown_template_budget.py \
  tests/test_refactor_inventory.py tests/test_refactor_progress.py tests/test_phase2_legacy_path_gate.py \
  tests/test_sign_in_progress_contract.py tests/test_sign_in_lottery_audit_contract.py \
  tests/test_sign_in_effects_wiring.py tests/test_sign_in_effects_boundary.py \
  tests/test_sign_in_task_effects.py tests/test_sign_in_legacy_switch.py \
  tests/test_compensation_reward_claim_schema.py tests/test_plugin_config_owner.py \
  tests/test_group_lifecycle_notice_application.py
```

| 命令/范围 | 实际结果 |
|:--|:--|
| `runner.py red features/buff/tests/test_closing_enter.py -k handler`（feature 路径均含 package 前缀） | 修复前 3 failed、1 passed、6 deselected；失败为 busy、missing user、failed CAS 重放文案 |
| 上述固定范围 focused 集合 | 213 passed、1 failed、91 subtests passed；唯一失败为 sign_in Web 夹具缺 `operation_ledger` |
| `runner.py focused-recheck <features/sign_in/tests/test_sign_in_adapter.py>` | 修复夹具后 3 passed，未降低接口断言 |
| `runner.py queue-red tests/test_message_db_queue_budget.py -k writer_releases` | 消费者修复前 3 subtest failures（各残留 1312 charged bytes） |
| `runner.py queue-green tests/test_message_db_queue_budget.py tests/test_message_db_migration.py` | 12 passed、3 subtests passed |
| `runner.py owner-adapters tests/test_economy_ledger_web.py tests/test_qq_bind_routes.py tests/test_qq_bind_application.py tests/test_web_qq_bind.py tests/test_logs_routes.py tests/test_admin_config_owner.py` | 33 passed、9 subtests passed |
| `runner.py root`（`pytest.main` 无 targets，默认根目录入口） | exit 2；8 collection errors；全量用例未执行，没有改 import mode、ignore 或 skip 绕过 |
| `runner.py historical-failures tests/test_source_quality.py::SourceQualityTests::<下面四项> --tb=line` | 2 failed、2 passed；仅核验历史失败归属，未改清单外源码或断言 |
| `refactor_completion_audit.audit()`，无真实 P7 参数 | P0-P6 全 true；P7 false；整体 ready=false |
| `phase2_legacy_path_gate.load_phase2_scope_report()` | ready=true、path_count=496、blocked_count=0、integrity_errors=[] |
| `check_full_refactor_progress.main()`，`--json` | exit 0；phase2_complete/exit_ready=true；P7 explicitly independent |
| `export_refactor_inventory.main(["--check"])`、`git diff --check` | exit 0；没有降低 gate 或修改冻结 membership |

静态工具的隔离复放入口示例：

```bash
.venv/bin/python -B -c 'import tests, nonebot; nonebot.init(); from scripts.export_refactor_inventory import main; raise SystemExit(main(["--check"]))'
.venv/bin/python -B -c 'import tests, nonebot, sys; nonebot.init(); from scripts.check_full_refactor_progress import main; sys.argv=["check_full_refactor_progress.py","--json"]; raise SystemExit(main())'
.venv/bin/python -B -c 'import tests, nonebot, json; nonebot.init(); from scripts.refactor_completion_audit import audit; print(json.dumps(audit(), ensure_ascii=False))'
```

Phase 2 membership SHA256 仍为
`7787a74a3e0c15c220a5f257921693b14706ef23b224bac9f3dad55a1ff51fac`；
两份冻结 JSON 的本轮前后文件 SHA256 也一致。

### 2026-10-09 根目录收集失败

八项均为 `ImportError: attempted relative import with no known parent package`：

| 路径前缀 | 文件 |
|:--|:--|
| `nonebot_plugin_xiuxian_2/features/natal_treasure/tests/` | `test_effect_upgrade_repository.py` |
| 同上 | `test_engraving_repository.py` |
| 同上 | `test_forget_repository.py` |
| 同上 | `test_natal_treasure_application.py` |
| 同上 | `test_reawaken_repository.py` |
| 同上 | `test_training_repository.py` |
| `nonebot_plugin_xiuxian_2/features/trade/tests/` | `test_guishi_query_repository.py` |
| 同上 | `test_trade_application.py` |

两个 tests 目录均缺 `__init__.py`；`git ls-tree HEAD` 也缺这些包标记，相关源码/测试
没有工作树 diff。这是当前默认 pytest 的收集问题，未证明产品行为错误，
但实际阻塞统一技术验收，不能作为“历史失败可忽略”勾完 C3。
2026-10-09 未修这些目录；2026-10-10 用户已授权作为 C3 必需测试修复，无需再冻结玩法范围。

### 2026-10-09 历史四项失败核验

归档可定位到 Dongfu 两项、Activity config 和 Boss 旧源码锚点的描述，
没有停止时原始四项完整 traceback。按该描述定向核验以下四项：

- `test_dongfu_infiltrate_success_handler_uses_feature_repository`：失败，
  `tests/test_source_quality.py:220` 的旧 `operation_id = f"dongfu-infiltrate-success:` substring 已不存在。
- `test_dongfu_infiltrate_failure_handler_uses_feature_repository`：失败，
  `tests/test_source_quality.py:241` 的旧 `operation_id = f"dongfu-infiltrate-failure:` substring 已不存在。
- `test_activity_config_uses_lazy_event_service`：本轮通过。
- `test_world_boss_full_refresh_uses_lazy_service`：本轮通过。

Dongfu 当前仍调用 `dongfu_application.infiltrate_failure/infiltrate_success`，
operation identity 位于 action 列表（1281-1282 行）；上述两项在断言业务调用前即因
旧源码切片锚点报 `ValueError: substring not found`。源码与测试均无本轮 diff，
2026-10-09 当时归属 Dongfu 证据维护 backlog，不是普通闭关拒绝重放的三条行为失败。
当时未改断言或恢复旧实现，也未声称猜测的四项等同原报告全集。
2026-10-10 已按当前 `_settle_infiltration_plan` 维护证据，保留 repository/无旧 settle 要求，
增加 operation identity 和 matcher 委托断言；两项当前均通过。

### 2026-10-10 C3 恢复验收

本轮隔离临时根为 `/tmp/xiuxian-closeout-c3-rj5cIdtf`。沿用上述 bootstrap，
每个 mode 独立设置 `TMPDIR=<临时根>/<mode>/scratch`、
`XIUXIAN_TEST_DATA_DIR=<临时根>/<mode>/import-data`，pytest 的 basetemp、JUnit
也使用该 mode；禁用 pyc 和 cacheprovider。共享夹具、全量和恢复任务串行。
原全量 session `96431` 一直轮询至自然退出，没有重复启动同一轮。

| 命令/范围 | 实际结果 |
|:--|:--|
| 已知 collection/Dongfu/natal 必需 focused | 19 passed |
| `runner.py root --tb=short`（原句柄） | exit 1；28 failed、6119 passed、300 subtests passed，514.44 秒；无 collection errors 或 skipped |
| `runner.py fixes <必要修复集合> --tb=short` | 400 passed、6 failed、22 subtests passed；剩余为导入/CLI/recovery 问题 |
| `runner.py fixes-recheck <剩余集合> --tb=short` | 25 passed、2 failed、20 subtests passed；仅剩 CLI migrate/health |
| `runner.py cli-green2 <CLI/Activity/work/task-progress 集合> --tb=short` | 109 passed、20 subtests passed；15.67 秒 |
| `runner.py gates` | exit 0；P0-P6 ready、inventory freshness、Phase 2/progress ready；P7 独立 false |
| `runner.py root-final --tb=short`（无 targets） | exit 0；6148 passed、304 subtests passed、47 warnings；532.17 秒；JUnit 6452 cases、errors/failures/skipped 均为 0 |
| `runner.py unittest`（等效 `unittest discover -s tests -v`，隔离 bootstrap） | 2947 tests、1 import error；广播 ingress 的相对导入不兼容该 discovery 入口 |
| `runner.py broadcast-green tests/test_admin_broadcast.py tests/test_admin_broadcast_ingress.py --tb=short` | 修正唯一测试导入后 40 passed，9.36 秒；消息入口/屏蔽/发送/状态断言全部保留 |
| `runner.py unittest-green`（同一默认 discovery，新隔离目录） | exit 0；2946 tests、OK；302.595 秒；首次 failed-import placeholder 消除，不跳过真实用例 |
| `runner.py compile`（等效 compileall package/tests） | exit 0；两个 compile_dir 均 true，pycache_prefix 仅指向任务目录 |
| `runner.py architecture`、`git diff --check` | exit 0；架构 violations 为空，diff 无空白错误 |
| `git diff --cached -B -M --check` | exit 0；识别两份历史原文 99% copy 和入口 rewrite，新增改动无空白错误 |

默认 staged diff 把新归档按整文件新增，曾提示历史进度原文的一处 Markdown 两空格换行；
HEAD 原文同一行已有该空白。`-B -M` 识别原文复制后检查通过，归档 SHA256 保持原值；
未删除归档字节，也未修改 Git whitespace 规则或行为门禁。

`cli-green` 曾因误写不存在的 `tests/test_work_settlement.py` target 而 exit 4，未执行测试；
实际使用现有 `test_work_settlement_service.py` 和 `test_work_refresh_settlement.py` 的是
`cli-green2`。该失败命令不作为通过证据。CLI 诊断探针只用于定位导入链，也不替代验收。

首轮 28 项失败全部有明确处理：

| 失败集合 | 处理与行为要求 |
|:--|:--|
| auction/daily_fortune/past_life/simulator/tianti 10 项 | 夹具先执行真实 platform/feature migration；默认 Auction SQL owner 断言同时验证构造不创建 DB。回滚、重试、冲突、拒绝重放和 API 403 断言保留 |
| generic legacy application 1 项 | AST 选择实际调用 `self._action` 的注入 port 方法；native SQL owner 由其真实状态夹具回归覆盖；拒绝/异常断言从首个 action 加强到每个 action |
| Activity overview 1 项 | legacy 与 migrated projection 固定同一天，保留完整 dict 比较 |
| Activity 只读 JSON 1 项 | 使用中央 `safe_json_loads`，不使用会写回修复文件的 loader；新增缺失/坏 JSON/非 dict/坏 UTF-8/有效 JSON 不改文件、不建 backup 的行为回归 |
| internal import 1 项 | feature 测试和真实 media parser 导入改为 package relative；保留 buff 子进程脚本所需 absolute import，不改脚本语义 |
| lazy reader/HTTP/sticker/delete/map/admin/task-event 10 项 | 定位当前 application/repository owner，保留旧 manager/直接写入禁令、只读 UoW、下载预算/finally cleanup、operation identity 和 durable outbox 要求；HTTP mock 移到实际被调用的 port |
| CLI 2 项、recovery smoke 2 项 | Activity 配置/时间 provider 及 work effects 默认依赖延迟到使用时加载；维护命令日志不污染 JSON stdout，serve 保留日志；独立子进程无需初始化 NoneBot 即 migrate/health ready，恢复前旧源回填回归通过 |

根全量后的唯一测试变更为广播 ingress 导入改用现有 `tests.test_admin_broadcast` 共享夹具，
上述 40 项 pytest 回归和默认 unittest discovery 负责验证两个入口；业务源码此后未变。
没有新增 skip/xfail/ignore、删行为断言或降低门槛。冻结 Phase 2 membership、
两份 scope JSON 与恢复基线指纹保持一致；相关 C1/C2 未变证据复用，C4 已实际复跑。

## C4 隔离迁移恢复

2026-10-10 最终根全量实际复跑
`test_closeout_five_database_migration_backup_restore_and_reconcile`，通过，2.083 秒。
JUnit `closeout_recovery` 记录 `synthetic_only=true`，data 路径为
`/tmp/xiuxian-closeout-c3-rj5cIdtf/root-final/pytest/test_closeout_five_database_mi0/data`；
before/after backups 分别为同一测试根的 `backups-before/20261010T000356Z` 和
`backups-after/20261010T000357Z`。两个 Activity 历史 claim/task receipt recovery smoke
也在本轮全量中通过；维护 CLI 的独立进程回归在必要修复后通过。
以下初轮路由/回滚/快照要求仍全部成立，本轮 JUnit 再次记录了对应实际结果。

2026-10-09 实际命令为 `runner.py recovery-tests tests/test_refactor_closeout_recovery.py -o junit_family=legacy`，
等价于上述 Python/pytest bootstrap 加此单一测试 target。最终 `1 passed`（7.23 秒）。
首次失败是快照夹具使用 sqlite3 context manager 却未显式 close，修正为 `closing(...)`
后复放通过；未改 BackupService 或降低恢复比较。

隔离 data 路径为
`/tmp/xiuxian-closeout-v1-xb2H4OED/recovery-tests/pytest/test_closeout_five_database_mi0/data`；
前/后备份分别位于同一 test 根的
`backups-before/20261009T151322Z`、`backups-after/20261009T151322Z`。
全部数据和 config 是测试合成输入，没有复制正式运行数据库、业务回执或凭据。

| 数据库 | preview/apply 后历史条数 | 路由与校验 |
|:--|--:|:--|
| game_db | 223 | `economy_ledger.001` 在 game；无 `game_events.001/buff.013` |
| player_db | 65 | `game_events.001/buff.013` 在 player；无 `economy_ledger.001` |
| trade_db | 9 | 与 `migrations_for_database` 精确一致 |
| impart_db | 4 | 与路由精确一致 |
| message_db | 1 | 仅 `platform.001` |

唯一 catalog 291 个版本，路由执行共 302 条历史；另验证 player 的三个
`accessory_package.player_data.001/.002/.003` attached migration receipts。
每库在真实路由迁移 batch 末尾注入异常，证明 DDL/数据/metadata 全部回滚到原始逻辑快照；
read-only preview 不改 schema/数据。正常启动与旧快照恢复后的再启动均为 ready，
filesystem/database/migrations/repositories/jobs/web 六项全 true；显式 `legacy_startup=False`，
维护模式不创建 Web 服务或执行资源下载 hooks，测试生命周期均已 shutdown。

前后备份各覆盖五库加 config，restore dry-run 不改变已修改状态；实际 restore 恢复全部
逻辑快照/config 内容。恢复后各库 `PRAGMA integrity_check=ok`、pending migrations 为空。
五库各放一个合成 pending outbox event，reconcile 正好处理一次，再跑不重复投递；
最终 operations/outbox/dead_events 均为 0、clean=true。最后恢复迁移前快照并重新启动，
证明旧快照的再迁移路由与 readiness 可复放。这些证据只证明 C4 的隔离技术路径。

## C5 发布证据与停止

当前发布版本未指定，正式 `--data-dir` 未指定，当前发布的 `--evidence` 未提供。
审计明确报：`real release-cycle evidence is required: --data-dir, --current-release and --evidence`。
尚未核验当前发布周期的兼容命中/日志、真实备份恢复和迁移 history/checksum；
本轮不伪造 release tag、不把合成 recovery property 传给 P7，不使用历史版本通过记录。

2026-10-09 交付是“固定清单核验与阻塞报告”，当时不是技术收口完成。
上轮将根 collection 和 Dongfu 旧证据划为清单外；2026-10-10 已作为 C3 必需项修复，
当前根全量、恢复回归和必需交付检查实际通过，技术收口完成、待发布。
本提交按白名单交付 `refactor/full-bottom-layer`，只普通推送，不强推、不合 main、不发版；
最终回执记录当前提交和 `git ls-remote --heads origin refs/heads/refactor/full-bottom-layer`
的同一 SHA，成功后结束本轮 goal，不自动开启旧“下一片”。
发布证据仍需当前发布流程提供，不能冒充 P7 通过，也不免除本地验收或提交推送；
无范围/证据变化不重跑全仓审计，不自动开启 v2/v3 出关、其它玩法或历史下一片。

## 清理与保护复核

### 2026-10-09 初轮

初轮唯一临时根 `/tmp/xiuxian-closeout-v1-xb2H4OED` 记录了各进程 import-data、
pytest basetemp、临时五库/config/合成备份、runner 和 JUnit/JSON 结果。
结果归纳于本文件，相关进程全部退出后仅删除该临时根；不清系统 page cache，
不停止其它项目/系统服务，不删除正式数据/WAL/SHM、业务备份或 Codex IPC。
测试禁用 pyc/pytest cache；排除 `.venv/.git/data` 的工作区检查未发现新缓存目录。
正式五库及 config 的 inode/size/mtime、HEAD、用户 boss 文件和清单外初始文件指纹均保持不变。
2026-10-09 23:29（Asia/Shanghai）收尾复核：所有本轮执行句柄均已终止，
专属 runner 进程查询为空；仅删除上述临时根（删除前 44 MiB），并确认路径不再存在。
RAM available 为 1338 MiB，磁盘可用约 13 GiB，inode 使用 16%；
没有保留本轮测试/恢复进程占用，也未清空其它服务或业务共享缓存。
当前文档 5 个本地链接、`git diff --check`、HEAD 与空暂存区复核通过。
2026-10-09 当时按“交付后停止”结束开发、测试和审计；C3/P7 仍受阻，未标记技术收口完成。

### 2026-10-10 恢复轮

临时根仅为 `/tmp/xiuxian-closeout-c3-rj5cIdtf`，包括本轮隔离库/config/合成备份、
runner、JUnit/JSON/日志及 compileall 的专属 pycache。结果已归纳于本文件。
所有全量、focused、诊断探针及 unittest/编译/架构句柄均已自然退出；
按任务路径参数复核无所属活跃进程，删除前该目录为 169 MiB。
仅删除此精确目录，核对路径不再存在；不清系统缓存，不停止其它项目/系统服务，
不删除 Codex IPC/socket/lock、正式数据/WAL/SHM、业务备份或用户 boss 文件。
正式五库/config 元数据和 boss SHA256 复核未变；冻结 scope 与两份归档指纹未变。
删除后复核路径不存在，RAM available 为 1413 MiB、磁盘可用约 11 GiB、inode 使用 17%；
没有保留本轮测试/恢复进程占用。16 份当前文档的 48 个本地链接和 10 个边界 token
静态检查通过；文档收尾只补状态与资源结果，不重复业务长链。

## 本轮文档整理

两份旧文档按停止时原始字节归档，SHA256 见[归档说明](archive/refactor-2026-10-09/README.md)。
本轮只调整执行入口、状态、固定清单和文档导航；源码、门禁、JSON、
运行数据及已有用户改动均不由文档整理修改。
静态检查通过：7 份入口/导航文档的 48 个本地链接、10 个旧 source-contract token、
冻结项数量 496；6 份门禁/JSON 文件指纹保持不变，两份归档 SHA256 与停止时原文一致。
以上仅为文档整理验证，不替代 C2-C5 的业务、技术和发布验收。
