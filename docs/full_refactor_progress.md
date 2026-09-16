# 全面底层重构第二阶段进度

状态：进行中。`v1.1.0` 的 P0-P7 发布证据继续保留，但不作为底层全面重构完成证明。

记录日期：2026-09-13  
基线提交：`4083da4` (`v1.1.0`)

## 1. 真实执行路径审计

上一阶段已经建立了 `plugin.py`、`bootstrap/`、manifest、compatibility facade、架构检查和发布门禁；这些模块目前主要提供组合、边界和验证能力。它们没有自动替换旧玩法实现。当前仍真实执行的旧路径包括：

- `nonebot_plugin_xiuxian_2/xiuxian/xiuxian_utils/xiuxian2_handle.py`：全局玩家状态、旧数据访问、玩法公共逻辑仍被旧 matcher 和 service 使用。
- `nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base/transaction_service.py`：包含灵石赠送、签到、改名以及其他资产事务的旧实现；compatibility facade 不是删除它的证据。
- `nonebot_plugin_xiuxian_2/xiuxian/*/transaction_service.py`：仍有 33 个文件、合计 48,505 行，需要逐个证明 handler 已切换后才能删除或隔离。
- `nonebot_plugin_xiuxian_2/xiuxian/xiuxian_base/__init__.py` 等旧 `__init__`：仍进行 NoneBot matcher 注册和旧 service 实例化；新 `features/*/commands.py` 不能仅凭存在视为真实入口。
- `db_backend.connect`、`sqlite3.connect`、原始 SQL、JSON store、随机数和系统时间仍分散在旧玩法中。任何新切片必须先把这些依赖收敛到 ports/infrastructure，再减少旧命中。

2026-09-13 的静态计数基线（使用仓库 Python 文件，不含 `.venv`）：

| 指标 | 当前数量 | 第二阶段含义 |
|:--|--:|:--|
| Python 文件 | 1154 | 仅用于规模跟踪，不作为完成率 |
| `xiuxian2_handle.py` | 179,415 bytes | 必须按状态/领域切分，不能继续作为新用例依赖 |
| `transaction_service.py` | 33 个，48,505 行 | 每个真实切片必须减少旧执行路径 |
| `db_backend.connect` | 47 个文件命中 | 新代码目标为 0；旧路径逐片下降 |
| `sqlite3.connect` | 8 个文件命中 | 新代码目标为 0；旧路径逐片下降 |
| 旧 `transaction_service` import | 134 个文件命中 | handler/application 切换后的剩余数 |
| 直接 `random.` | 69 个文件命中 | 领域和 application 新代码目标为 0 |
| `datetime.now` | 100 个文件命中 | 领域和 application 新代码目标为 0 |
| `time.time` | 67 个文件命中 | operation/id/超时逻辑必须注入 |

这些数字是审计快照，不是静态检查通过数，也不代表已有代码已经迁移。

## 2. 切片排序

按数据风险和可验证性排序：

1. player/economy/inventory 与 operation ledger。
2. cultivation/breakthrough/training。
3. combat/map/dungeon/arena/tower/boss。
4. sect/impart/pet/trade/auction。
5. 其余玩法、scheduler、Web 和命令入口。

首个实施切片为 `stone_gift`（player/economy）：它已有新 domain/application/repository 草稿和 operation ledger，但真实 `送灵石` handler 仍位于旧 `xiuxian_base/__init__.py`，旧完整实现仍位于 `xiuxian_base/transaction_service.py`。因此当前状态标记为“壳已建、真实路径未切换”。

## 3. “真实迁移”定义

一个切片只有同时满足以下条件才计入完成：

1. 至少一个真实 NoneBot handler 和一个真实 Web route 从事件/请求进入新 application；测试必须通过真实注册表或 Flask app 调用，而不是直接实例化 facade。
2. 新 application 不调用旧 `transaction_service`、`xiuxian2_handle` 或兼容 facade 处理核心业务。
3. domain 不依赖 NoneBot、Flask、SQLite、原始文件路径、系统时间或全局随机源。
4. repository/infrastructure 负责 SQL、事务和数据库连接；跨库动作有 operation ledger、outbox 和 reconcile 证据。
5. 请求 DTO、Clock、RandomSource、IdGenerator 和 operation_id 注入均有失败/重试/幂等测试。
6. 旧实现从执行路径移除：删除、移动到明确的 compatibility-only 模块，或由守门检查证明不再被真实入口调用；仅保留 import shim 不计入完成。
7. 真实数据目录执行 backup、migration dry-run、恢复和 reconcile；不得使用伪造日志、临时标签或手工数据库修改。
8. 更新本文件，记录旧路径、新路径、删除/隔离量、测试、风险和回滚点。

## 4. 量化退出标准

全面底层重构不得以“测试通过”或“P7 已关闭”作为替代。结束第二阶段前必须满足：

- 主要核心切片全部有真实 handler/route 切换证据；“壳已建、真实路径未切换”的切片数量为 0。
- `xiuxian2_handle.py` 不再被新 application、domain、repository 或 adapter 作为核心业务依赖；旧调用者已逐片删除或隔离。
- 旧 `transaction_service.py` 不再是已完成切片的执行路径；完成切片对应的旧实现删除/隔离量与依赖图一致。
- 新增代码中 `db_backend.connect`、`sqlite3.connect`、直接系统时间、直接全局随机和动态原始 SQL 命中为 0；遗留旧代码的剩余数量逐片记录并持续下降。
- 每个完成切片具备 domain/application/ports/repository/DTO/adapter/迁移/回滚/行为测试和真实数据 dry-run 证据。
- 全量 unittest、compileall、git diff --check、架构检查、inventory、恢复和远端冒烟持续通过。
- 至少一次真实正式发布周期验证每批迁移后的数据兼容、备份恢复和 reconcile；不能用上一阶段的 `v1.1.0` P7 回执替代第二阶段切片证据。

## 5. 切片记录

| 切片 | 旧真实路径 | 新目标路径 | 状态 | 删除/隔离 | 测试/证据 | 风险/回滚 |
|:--|:--|:--|:--|:--|:--|:--|
| `stone_gift` | `xiuxian_base/__init__.py` 的旧 `送灵石` handler；原 `xiuxian_base/transaction_service.py::StoneGiftService` | `features/stone_gift/{domain,application,repository,commands.py}`；`adapters/nonebot/commands.py::_build_stone`；`adapters/web/api.py::create_stone_gift_blueprint` | 默认真实路径已切换；旧实现已隔离到 compatibility-only 回滚模块；切片仍需全量行为回归 | 从旧 `transaction_service.py` 删除约 7,156 bytes（约 188 行）；回滚实现为 `compatibility/legacy_stone_gift.py` | 14 个 command/Web/application/legacy-switch/旧回滚/质量测试通过；新切片 compileall、架构、inventory、diff 通过 | 默认行为走新 application；`XIUXIAN_STONE_GIFT_LEGACY_HANDLER=true` 为回滚点；需在真实 live 数据完成备份/恢复后才可继续删除兼容 shim |
| `sign_in` | `xiuxian_base/__init__.py` 的旧 `修仙签到/签到` handler；原 `xiuxian_base/transaction_service.py::SignInService`；旧 lottery/statistics/task side effects | `features/sign_in/{application,repository,domain,commands.py}`；`adapters/nonebot/commands.py::_build_sign`；`features/sign_in/effects.py` | 默认真实路径已切换；旧资产 service 已隔离到 compatibility-only；side effects 仍为兼容实现，切片未完成 | 旧 matcher 默认为不可达占位；旧 service 已移至 `compatibility/legacy_sign_in.py`；lottery/statistics/task side effects 仍在 `compatibility/sign_in_effects.py` | SignInEffects post-commit port、持久化 side-effect ledger、wiring/legacy-switch/source-quality/compile 通过；真实 live backup/dry-run/reconcile/recovery 全绿 | `XIUXIAN_SIGN_IN_LEGACY_HANDLER=true` 为回滚点；必须继续将 lottery/statistics/task adapters 迁入新 application-owned ports/use cases，才能完成签到切片 |
| `package_reward` | `xiuxian_back/__init__.py` 的礼包解析/随机奖励/饰品展示 handler；`xiuxian_back/package_reward_service.py` facade | `features/package_reward/{domain,application,repository}.py`；`adapters/web/blueprints/package_reward.py`；`adapters/web/app.py` | Web 真实 route 已直达 `PackageRewardApplication`；旧 NoneBot 命令仍通过 compatibility facade，切片未完成 | 新 application/repository/ledger 已存在；新 Web route 已注册；旧 command parser/reward presentation 未删除 | Web boundary test、package manifest route/docs、compileall、inventory、architecture；live backup/dry-run/reconcile/recovery 全绿 | 旧命令回滚仍保留在 `xiuxian_back`; 需要先拆随机奖励解析/饰品实例 DTO，再切真实 handler，不能把 Web route 当作命令切换完成 |

2026-09-13 首个切片证据：`tests/test_stone_gift_application_real.py` 直接使用 `StoneGiftApplication` + `DatabaseUnitOfWork`，验证双边余额、手续费、`stone_gift_limits`、operation replay、operation payload conflict 和余额不足回滚；`tests/test_stone_gift_matcher_boundary.py` 验证真实 adapter builder 将 sender/recipient/amount/每日限额传入新 application。5 个测试全部通过。边界扫描确认新切片源码不包含 `transaction_service`、`xiuxian2_handle`、`stone_limit`、NoneBot 或 Flask 依赖；同时确认旧 `@give_stone.handle` 和 `stone_gift_service = StoneGiftService(...)` 仍存在，因此没有把该切片标为完成。

2026-09-13 部署验证：提交 `54af620` 已通过 SSH 推送到 `origin/main`，并以归档形式部署到受控 live 容器 `/srv/src`；旧源保留于 `/tmp/remote-host/src-v2-pre-54af620`。针对真实 `/srv/old/data` 执行 `migrate --dry-run` 返回 `pending=[]`，`reconcile` 返回 `clean=true`、`operations=0`、`outbox_events=0`、`dead_events=0`，`/health/ready` 的 database/filesystem/jobs/migrations/repositories/web 全部为 `true`。该证据证明新切片可在真实数据结构上加载，但不证明旧 handler 已被移除。

2026-09-13 真实入口切换：旧 `xiuxian_base/__init__.py` 的 `送灵石` matcher 默认改为 `__legacy_stone_gift_disabled__`，只有显式 `XIUXIAN_STONE_GIFT_LEGACY_HANDLER=true` 才恢复旧入口；新 `adapters/nonebot/commands.py` matcher 和 Web `/api/v1/stone-gift` 均调用 `StoneGiftApplication`。提交 `700b63d` 已推送并以归档部署到 `/srv/src`，针对真实 `/srv/old/data` 的 migration dry-run 为 `pending=[]`、reconcile clean、live health 全绿。该改动保留可回滚开关，不删除旧 service；因此这是“默认执行路径已切换、旧实现待删除”的切片状态，不是全面重构完成。

2026-09-16 stone gift request-DDL boundary：`StoneGiftRepository.operation` 移除 request-time `ensure_schema`，`stone_gift.001/002` 成为 operation/limit schema 的启动前置；real application fixture改为显式 migration。stone-gift/source共158 tests、2 subtests、catalog=103、compileall、architecture、diff check通过；旧 NoneBot handler与 legacy compatibility service保留为显式 rollback。

2026-09-16 stone gift request-DDL live safety：提交 `abfe590` 部署后 backup `/srv/old/data/backups/20260915T205209Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行灵石转赠或账户资产写入。

2026-09-13 sign-in 入口审计：旧 `sign_in` matcher 默认规则为 `__legacy_sign_in_disabled__`，新 adapter 规则为 `修仙签到/签到`；旧 handler 的签到后抽奖、统计和任务进度仍是真实旧逻辑，尚未宣称完成。最初探针在完整插件导入后再次手动调用 `register_migrated_matchers`，因此人为制造了重复 prefix 警告；在全新 NoneBot 进程中只执行正式包初始化没有重复警告。该误报已纠正，不把它当作生产缺陷；旧实现隔离和副作用迁移仍未完成。

2026-09-13 sign-in live 验证：提交 `72173ea` 的归档已部署到受控 live 容器 `/srv/src`，旧源保留于 `/tmp/remote-host/src-pre-72173ea`。使用真实 `/srv/old/data` 重启实例后，`migrate --dry-run` 返回 `pending=[]`，`reconcile` 为 clean 且 operations/outbox/dead events 全为 0，`/health/ready` 的 database/filesystem/jobs/migrations/repositories/web 全部为 true。该结果只证明入口隔离版本可加载旧数据；签到后的 lottery/statistics/task-progress 仍需独立 application 化。

2026-09-13 sign-in effects 边界：`SignInApplication` 新增 `SignInEffects` port 和默认 `NullSignInEffects`。资产 UoW 先显式 `commit()`，再调用 `on_signed(user_id, operation_id, stone, replayed)`，避免副作用开启第二个 SQLite 写事务时锁死；拒绝不会通知。`LegacySignInEffects` 增加持久化 `sign_in_effect_operations` ledger，防止 ledger replay 与旧 projection replay 多层重放导致 statistics/task 重复；lottery 继续使用自身 operation_id 幂等。新增/更新 `tests/test_sign_in_effects_boundary.py`、`tests/test_legacy_sign_in_effects.py` 覆盖提交、replay、持久化去重和拒绝。该 adapter 尚未接入默认 runtime，旧副作用仍不计入迁移完成。
2026-09-13 sign-in compatibility wiring：`plugin.py` 在 `context.legacy_startup=true` 的真实 NoneBot 启动路径构造 `LegacySignInEffects`，注入当前 game DB、Clock、旧 lottery settlement、statistics、task-progress、log 适配器；CLI/纯 application 路径保持 `NullSignInEffects`。事务边界测试、启动/插件入口测试、compileall 和架构守门通过；更新 inventory 后 `check_architecture.py` 为 true。该 wiring 明确保留旧副作用以保持用户行为，尚不等于 lottery/statistics/task 已底层迁移。

2026-09-13 sign-in wiring live 修复：首次部署发现 `plugin.py` compatibility wiring 缺少 `Path` import，导致真实旧数据实例 repositories 阶段 not-ready；提交 `ff21601` 修复后重新部署，真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T072219Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿均通过。该回归证明兼容 wiring 仍必须在真实数据上验证，不能以本机 application 测试替代。

2026-09-13 sign-in wiring 回归测试：`tests/test_sign_in_effects_wiring.py` 使用完整静态资源隔离目录验证 `legacy_startup=true` 的 lifecycle 将 `SignInApplication.effects` 注入为 `LegacySignInEffects`，`legacy_startup=false` 保持 `NullSignInEffects`；两项测试通过。测试期间 legacy startup 的资源更新仅写入临时目录，未修改仓库或 live 数据。

2026-09-13 sign-in lottery domain 提取：将纯规则 `lottery_tier(number)` 和 `lottery_prize(pool, tier)` 提取到 `features/sign_in/domain.py`，旧 `LotterySettlementService` 只委托这些规则，事务/JSON 迁移仍未完成。新增 `tests/test_sign_in_lottery_domain.py`，并与既有 lottery settlement、legacy effects 测试合计 13 个通过；该改动只迁移纯规则，不把旧 lottery service 误计为 application-owned。

2026-09-13 lottery domain live 验证：提交 `03a0094` 归档部署到 `/srv/src` 后，真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T073439Z` 成功，migration dry-run `pending=[]`，reconcile clean，readiness 的 database/filesystem/jobs/migrations/repositories/web 全部为 true，runtime log 未发现 Traceback/ERROR/Exception。该证据只覆盖纯规则提取的回归，不代表旧 lottery persistence 已迁移。

2026-09-13 sign-in service 隔离：从 `xiuxian_base/transaction_service.py` 删除约 113 行旧 `SignInService`，实现移动到 `compatibility/legacy_sign_in.py`；`compatibility/sign_in.py`、`features/base/repository.py` 和旧对照测试改为显式引用 compatibility-only 模块。旧 transaction service 的 `SignInService` import 扫描为空；11 个 sign-in/application/effects/wiring/base 测试、compileall 和架构检查通过。旧签到资产实现已不再位于大 transaction service，但新 application 仍通过 compatibility effects 保留 lottery/statistics/task 行为，side effects 尚未完成独立 application 化。

2026-09-13 sign-in service live 验证：提交 `e7b29d2` 归档部署后，真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T074055Z` 成功，旧 `transaction_service.py` 不含 `class SignInService`，migration dry-run `pending=[]`，reconcile clean，readiness 全绿且 runtime log 无 Traceback/ERROR/Exception。随后停止实例执行 recovery smoke，真实回执覆盖 53 个迁移、`game_db/player_db/trade_db/impart_db/message_db` 五库 restore，reconcile clean；恢复后 readiness 仍全绿。该证据只证明旧资产 service 隔离可恢复，lottery/statistics/task side effects 仍在 compatibility boundary。

2026-09-13 sign-in statistics projection：新增 `features/sign_in/statistics.py::SignInStatisticsRepository`、`sign_in.002` migration 和 `sign_in_statistics_events/projection` 表；`LegacySignInEffects` 在真实 legacy startup wiring 下优先写持久化统计投影，使用 `statistics:{operation_id}` 去重，旧 JSON statistics callback 仅作显式 fallback。`tests/test_sign_in_statistics_repository.py` 验证重复 operation 不重复计数；与 effects/wiring 测试合计 8 个通过，compileall、inventory、architecture 通过。lottery persistence、task progress 和日志仍未迁移，不能把该子切片当作 sign-in 完成。

2026-09-13 sign-in statistics live 验证：提交 `2b5d30d` 部署到真实 `/srv/src` 后，`migrate --dry-run` 真实返回 `pending=["sign_in.002"]`，实际 apply 返回 `applied=["sign_in.002"]`；`sign_in_statistics_events` 与 `sign_in_statistics_projection` 表存在，backup `/srv/old/data/backups/20260913T075109Z`、reconcile clean、readiness 全绿。恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 readiness 全绿。该子切片已有真实 schema/恢复证据，但 lottery persistence、task progress 和 JSON 日志仍在兼容边界。

2026-09-13 sign-in task boundary：新增 `features/sign_in/tasks.py::SignInTaskEffects`，将已提交签到映射为带 `task-progress:{operation_id}` 的 task event，并在 legacy startup wiring 注入；旧 task service 仍作为明确边界实现。`tests/test_sign_in_task_effects.py` 验证 operation_id 映射，连同 effects/statistics/wiring 聚焦测试共 7 个通过；task core、lottery persistence 和 JSON 日志仍未迁移。

2026-09-13 sign-in task boundary live 验证：提交 `1612518` 归档部署到 `/srv/src` 后，真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T075730Z` 成功，migration dry-run `pending=[]`，reconcile clean，readiness 的 database/filesystem/jobs/migrations/repositories/web 全部为 true。该证据只覆盖 task adapter wiring 的加载安全，旧 task core、lottery persistence 和 JSON 日志仍未迁移。

2026-09-13 lottery follow-up audit：旧 `LotterySettlementService` 仍位于 `xiuxian_base/transaction_service.py`，包含 legacy JSON pool migration、五张 lottery 表、奖池事务和 user wallet settlement；当前 `LegacySignInEffects` 仍从 compatibility startup wiring 注入它。由于它是独立的大事务切片，未将其复制成 facade 或空壳；下一步需先建立 lottery application/repository/Clock/Random port，再把该旧实现隔离，保持真实 JSON migration、operation replay 和恢复证据。

2026-09-13 lottery application first slice：新增 `features/sign_in/lottery.py` DTO、`lottery_repository.py` persistence port 和 `lottery_application.py` use case。新路径在已存在 lottery 表结构上独立执行 injected Clock/Random、participant 唯一性、wallet/prize 原子更新、operation replay/conflict；不读取 legacy JSON，也不接管旧 handler。`tests/test_lottery_application.py` 与 domain 规则测试合计 4 个通过，compileall/diff 通过。当前状态是“新 use case 可执行、旧 `LotterySettlementService` 仍是真实 compatibility execution path”，未计入 lottery 切片完成。

2026-09-13 lottery first slice live verification：提交 `6afb626` 归档部署到 `/srv/src` 后，真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T080956Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 readiness 全绿。该提交只证明新 use case 文件不会破坏真实数据加载；`LegacySignInEffects` 尚未切到 `LotteryApplication`，旧 lottery handler/service 仍是真实执行路径。

2026-09-13 lottery runtime cutover：`plugin.py` 的 compatibility startup 默认注入 `LotteryApplication` + `LotteryRepository` + runtime Clock/Random；只有显式 `XIUXIAN_SIGN_IN_LEGACY_LOTTERY=true` 才注入旧 `LotterySettlementService`。`LegacySignInEffects` 统一使用 keyword settlement contract，兼容两种实现。聚焦 lottery/application/effects/wiring 测试、compileall 和 architecture 通过；真实命令行为、legacy JSON migration 和旧 lottery service 删除仍未完成，因此该切片仍未关闭。

2026-09-13 lottery runtime cutover live：提交 `efb05ae` 默认未设置 legacy rollback 环境变量部署到 `/srv/src`；真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T082021Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿、runtime errors=0。恢复 smoke 覆盖 54 个迁移、五库 restore、reconcile clean，恢复后 readiness 全绿。该证据证明新 lottery application wiring 可加载真实数据；旧 handler 行为对照、legacy JSON pool migration 接管和旧 service 删除仍未完成。

2026-09-13 lottery schema safety audit：进一步读取真实 live `/srv/old/data/xiuxian.db`，确认不存在任何 `lottery_*` 表；当前 `/srv/src` 和 `/srv/old` 也没有可验证的 `lottery_pool.json`。因此新 `LotteryApplication` 现在在缺 schema 时明确拒绝，不会静默创建空奖池；composition root 在真实 schema 缺失时自动 fallback 到旧 `LotterySettlementService`，保留用户行为。该安全 fallback 是真实阻塞证据，不是切片完成；必须先找回并验证 legacy pool 数据/迁移来源，再应用新 lottery path。

2026-09-13 package_reward Web boundary：`features/package_reward` 已补充 manifest route `POST /api/v1/package-reward/open`、标准 feature 文档和 transport-neutral web declaration；真实 Flask 实现位于 `adapters/web/blueprints/package_reward.py` 并在 `adapters/web/app.py` 注册，直接调用 `PackageRewardApplication`。`tests/test_package_reward_web_boundary.py` 通过，compileall、inventory、architecture 通过。旧 NoneBot 礼包命令仍通过 compatibility facade，消息解析/展示尚未迁移，因此该 inventory slice 仍未完成。

2026-09-13 package_reward Web live verification：提交 `7cabcf8` 归档部署到 `/srv/src` 后，真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T085040Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 readiness 全绿。该证据只覆盖 application-owned Web route 的部署安全；旧 NoneBot 命令仍通过 compatibility facade，尚未计入 inventory 切片完成。

2026-09-13 package_reward resolver：新增纯 `PackageRewardResolver`，通过注入 RandomSource 解析 roll_pool/固定 buff_N 配置为 DTO，拒绝空 roll_pool/非法数量；不依赖 NoneBot、Flask、数据库或旧 service。`tests/test_package_reward_resolver.py` 与 Web boundary 测试共 4 个通过，compileall/diff 通过。旧 `xiuxian_back` handler 的消息解析、饰品实例化和奖励展示仍未切换，仅记录为下一步真实命令切片准备。

2026-09-13 package_reward command partial cutover：旧 `xiuxian_back/__init__.py` 的礼包 handler 已改为调用 `PackageRewardResolver(random)`，移除原地 `random.choice`、`buff_N/name_N` 解析循环，保留既有饰品容量检查、DTO 转换和 application 资产事务。`tests.test_source_quality` 137 个通过，compileall、architecture、diff 通过。旧 handler 仍负责事件解析、饰品实例化、消息展示和 compatibility service 初始化；因此这是“奖励解析子路径已迁移”，不是 package_reward 整体完成。

2026-09-16 package reward request-DDL boundary：`PackageRewardRepository.operation` 移除 request-time `ensure_schema`，唯一 schema 前置为已注册 `package_reward.001` migration；新增显式 migration与缺表不建表测试。package/source共153 tests、catalog=103、compileall、architecture、diff check通过。旧 NoneBot 礼包命令与消息解析/展示仍是未完成边界。

2026-09-16 package reward request-DDL live safety：提交 `e79dda5` 部署后 backup `/srv/old/data/backups/20260915T204522Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行礼包消耗、奖励或饰品写入。

2026-09-13 package_reward resolver live verification：提交 `c7a200c` 归档部署后，真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T090234Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 readiness 全绿。该证据只证明 resolver 替换未破坏旧数据加载；旧 command 的随机奖励之外的解析、饰品实例化、展示和 facade 仍是真实路径。

2026-09-13 accessory instance factory：新增 `features/accessory_package/factory.py::AccessoryInstanceFactory`，将 accessory DTO 构造的 item lookup、Clock、IdGenerator、affix roller 变为注入依赖；旧 `accessory_helpers.create_accessory_instance` 现在只作为 compatibility adapter 委托 factory，保留旧 JSON 字段和行为。`tests/test_accessory_instance_factory.py` 与 source-quality 共 134 个通过，compileall、architecture、diff 通过。旧 adapter 仍提供系统 Clock/随机 UID 和全局 Items，尚未计入 accessory/package_reward 完成。

2026-09-13 accessory factory live verification：提交 `b823df8` 归档部署到 `/srv/src` 后，真实 `/srv/old/data` backup `/srv/old/data/backups/20260913T091122Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 readiness 全绿。该证据只覆盖 accessory DTO factory 的 compatibility adapter 加载安全；旧全局 Items、系统 Clock/随机 UID 和 accessory JSON store 仍未完全迁移。

2026-09-13 accessory adapter boundary audit：`AccessoryInstanceFactory` 的新接口已显式注入 Clock、IdGenerator、item lookup、affix roller；`xiuxian_back/accessory_helpers.py::create_accessory_instance` 仍作为 compatibility adapter 使用 `SystemClock`、旧全局 `Items` 和旧随机 UID 格式，以保留现有用户数据标识。当前未改变 UID 格式，避免不可逆的数据引用断裂；下一步需要先建立真实 accessory JSON 到持久化 repository 的迁移/对账方案，再移除该 adapter 的系统时间和全局随机依赖。

2026-09-13 accessory namespace safety guard：只读审计真实 `/srv/old/data` 确认 `player.db` 仅有 migration 表，`player_accessory` 不存在；旧 transaction service 通过 game DB 的 `ATTACH ... AS player_data` 运行时创建/写入 `player_data.player_accessory`，而新 `AccessoryPackagePlayerRepository` 原先会在独立 player.db 创建同名表，存在数据分叉风险。新增 `schema_policy=require_existing`，真实 Web `AccessoryPackageApplication` 使用该 policy，缺少已对账 schema 时明确拒绝 `accessory player schema migration and namespace reconciliation are required`，不会静默创建错误 namespace；隔离测试仍可显式使用 `create`。新增 schema policy 测试、5 个 accessory application/factory 测试、compileall、architecture、diff 通过。该 guard 是阻塞保护，不是 accessory 切片完成；必须先完成 attached namespace 的备份、迁移和 reconcile。

2026-09-13 accessory namespace guard live verification：提交 `7126891` 归档部署后，真实 `/srv/old/data/player.db` 只读确认 `player_accessory=0`，没有被新 Web runtime 静默创建；backup `/srv/old/data/backups/20260913T092925Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 readiness 全绿。该证据证明 namespace guard 可部署且不破坏数据；attached `player_data.player_accessory` 的真实迁移/对账仍未完成。

2026-09-13 attached database infrastructure：新增 `AttachedDatabaseUnitOfWork`，提供显式 SQLite attachment、alias 校验、缺失文件拒绝和跨库事务 rollback；`tests/test_attached_database_uow.py` 验证 game DB 与 attached player DB 的 DDL/写入共同回滚。该 UoW 暂未接入 accessory application，因为真实 live `player.db` 尚无 `player_accessory`，且旧路径的表属于 `player_data` attached namespace；必须先完成真实 namespace migration/reconcile，再切换 application，避免把 standalone player.db 当作旧数据。

2026-09-13 attached database infrastructure live verification：提交 `a85216e` 部署后，真实 `/srv/old/data/player.db` 仍为 `player_accessory=0`，backup `/srv/old/data/backups/20260913T093608Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 readiness 全绿。该证据仅证明新 attached UoW 不会改变未迁移的真实 namespace；accessory application 仍未切换到 attached path。

2026-09-13 attached accessory migration scaffold：新增 `features/accessory_package/attached_migrations.py::apply_attached_player_accessory`，明确目标为 `player_data.player_accessory`，并通过 `ATTACHED_SCHEMA_VERSION` 标记独立迁移历史；测试验证 schema 创建与跨库 rollback。该 migration 尚未加入单库 `build_migrations()` catalog，因为当前系统没有 attached namespace 的 durable ledger；不能把 scaffold 或测试通过当作 live migration。真实 live 数据仍保持未修改，等待备份/对账后再执行。

2026-09-13 attached migration durable ledger：为 `player_data` attached namespace 增加 `attached_schema_migrations(version,name,checksum,applied_at)`，`apply_attached_player_accessory` 首次执行写入 ledger，重复执行返回 false，checksum/name 漂移拒绝；schema 与 ledger 在同一 `AttachedDatabaseUnitOfWork` 中共同 rollback。新增迁移幂等/rollback 测试共 5 个相关测试通过，compileall、architecture、diff 通过。该 ledger 仍未接入全局启动/catalog，也未对真实 `/srv/old/data` 执行迁移，避免没有用户数据快照与 reconcile 证据时改变 live namespace。

2026-09-13 attached migration clock injection：`apply_attached_player_accessory` 增加显式 `clock` 依赖，ledger `applied_at` 不再强制读取系统时间；固定 Clock 测试验证时间值可控。attached migration/schema/UoW 相关测试共 6 个通过，compileall、architecture、diff 通过。默认 fallback 仍保持 UTC clock 兼容行为；真实 migration 尚未执行。

2026-09-13 attached namespace audit tool：新增只读 `scripts/audit_attached_accessory.py`，使用显式 SQLite ATTACH 检查 `player_data.attached_schema_migrations`、`player_data.player_accessory`、行数及 expected checksum，不执行 DDL/写入；空数据库 smoke 输出 `read_only=true` 且不创建表。工具入口已补仓库根路径注入，compileall、architecture、diff 通过；真实 live 输出待部署后记录。

2026-09-13 attached namespace audit live verification：提交 `4129224` 部署后运行只读 audit，真实输出 `tables=[]`、`accessory_rows=0`、`migration.applied=false`、`read_only=true`；backup `/srv/old/data/backups/20260913T095422Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 audit 仍为未迁移且只读。该证据确认 live 当前没有可迁移 accessory 行，也确认审计工具不会隐式创建 schema；不能据此宣称 accessory migration 完成。

2026-09-13 attached audit machine-output fix：`scripts/audit_attached_accessory.py` 的 feature import 诊断已重定向到 stderr，stdout 现在保证为单一 JSON 文档；临时空数据库 audit 通过 `json.loads(stdout)` 紧验证，read_only/tables/accessory_rows 状态保持不变，compileall、architecture、diff 通过。该改动只提高证据可消费性，不改变 live 数据或迁移状态。

2026-09-13 attached audit machine-output live verification：提交 `17474b8` 部署后，容器内直接 `json.loads` 真实 audit stdout 成功；输出保持 `tables=[]`、`accessory_rows=0`、`migration.applied=false`、`read_only=true`。backup `/srv/old/data/backups/20260913T095925Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 audit JSON 仍一致。该证据提升了 live 状态读取的机器可验证性，不改变 accessory 未迁移事实。

2026-09-13 attached migration drift gate：新增 checksum drift 回归测试，确认历史 checksum 不匹配时在创建 `player_accessory` 前拒绝；attached migration/schema/UoW 相关测试共 7 个通过。只读 audit 增加 `migration.checksum_valid` 三态字段：未迁移为 `null`、匹配为 `true`、漂移为 `false`；临时空库 JSON 解析验证通过，compileall、architecture、diff 通过。该 gate 继续不执行 live migration。

2026-09-13 attached migration metadata gate：只读 audit 增加 `name_valid` 和综合 `metadata_valid`，与 migration apply 的 name/checksum 拒绝条件一致；未迁移状态三个 metadata flag 均为 `null`。新增 subprocess JSON 状态测试，attached audit/migration/UoW 相关测试共 7 个通过，compileall、architecture、diff 通过。该改动只提高 gate 一致性，仍不执行 live migration。

2026-09-13 attached migration metadata gate live verification：提交 `9265bec` 部署后真实 audit 输出 `name_valid=null`、`checksum_valid=null`、`metadata_valid=null`、`tables=[]`、`accessory_rows=0`、`read_only=true`；backup `/srv/old/data/backups/20260913T100944Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，恢复后 `metadata_valid=null`。该证据确认未迁移状态与执行 gate 一致，未执行 live schema migration。

2026-09-13 bank namespace audit tool：新增只读 `scripts/audit_bank_namespace.py`，通过 game DB 的 `player_data` attachment 检查旧 `bankinfo` 表、行数和 `bank_*_operations` ledger 行数；空库 smoke 可直接 JSON parse，脚本不执行 DDL/写入，compileall、architecture、diff 通过。该工具用于确认 bank economy slice 是否存在可迁移真实状态，尚未改变 bank runtime。

2026-09-13 bank namespace audit live verification：提交 `9a0a5e8` 部署后真实 audit 输出 `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T101548Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，恢复后 bank audit 仍一致。该事实表明当前 live 数据没有旧 bank 账户/操作可迁移；`BankApplication` 默认仍经 `LegacyBankRepository`，下一步只能先建立明确的新 bank schema/first-use 初始化和行为测试，不能把空状态当作旧数据迁移完成。

2026-09-13 bank first-use deposit slice：新增纯规则 `features/bank/rules.py::decide_deposit`、新 game-db-owned `bank_accounts`/`bank_account_operations` repository 和 `BankDepositApplication`；新路径不读取旧 attached `player_data.bankinfo`，首次账户从 saved=0 开始，operation_id replay/conflict、钱包不足、余额上限和事务 rollback 有隔离 SQLite 测试。`tests.test_bank_deposit_rules` + `tests.test_bank_deposit_application` 共 6 个通过，compileall、architecture、diff 通过；尚未接入真实 Web/NoneBot bank handler，旧 `BankApplication -> LegacyBankRepository -> transaction_service` 仍是真实路径，因此不计为 bank 切片完成。

2026-09-13 bank first-use deposit live safety：提交 `de4c1d7` 部署后真实 bank audit 仍为 `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T102551Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，恢复后 bank audit 不变。该证据证明新 first-use 代码未接 runtime、未改变空 live 状态；Web/NoneBot handler 切换和新 schema migration 仍未完成。

2026-09-13 bank first-use Web boundary：新增 `POST /api/v1/bank/v2/deposit` adapter，直接解析新 first-use DTO、Idempotency-Key 和 CSRF；只在 `RuntimeContext.services["bank_first_use"]` 显式注入时注册，默认旧四个 bank route/旧 handler 不变。manifest/docs 声明新 route；`tests.test_bank_first_use_web`、bank application/rules 共 7 个通过，compileall、inventory、architecture、diff 通过。该 route 是可灰度的真实新入口，但未注入 live runtime，bank slice 仍未完成。

2026-09-13 bank first-use Web live safety：提交 `e1af6c1` 部署后未注入 `bank_first_use` service，真实 bank audit 仍为 `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T103558Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，恢复后 bank audit 不变。该证据证明新 v2 route 默认不改变旧 bank runtime；仍需明确 migration/灰度数据窗口后才可启用真实新 route。

2026-09-13 bank.002 migration：新增正式 game-db migration `bank.002`，只创建 `bank_accounts` 与 `bank_account_operations`，不触碰旧 attached `player_data.bankinfo`；migration 与 first-use application/repository 共 8 个测试通过，compileall、inventory、architecture、diff 通过。该 migration 预计随下一次 live 部署实际 apply，必须记录 pending/applied、backup、reconcile、恢复结果；新 route 仍保持 opt-in 未注入。

2026-09-13 bank.002 live migration：提交 `c30d985` 部署前完成真实 backup `/srv/old/data/backups/20260913T104238Z`；dry-run 真实返回 `pending=["bank.002"]`，随后实际 migration 返回 `applied=["bank.002"]`。只创建 game DB 的 `bank_accounts`、`bank_account_operations`，真实 `player.db` 仍 `bankinfo=0`；reconcile clean、readiness 全绿。恢复 smoke 覆盖 55 个迁移、五库 restore、reconcile clean，恢复后 `bank.002` ledger 与两张新表存在。新 bank v2 route 仍未注入，旧 bank runtime 未切换。

2026-09-13 bank first-use composition flag：`RuntimeContext` 配置新增 `bank_first_use_enabled` / `XIUXIAN_BANK_FIRST_USE_ENABLED`，默认 `false`；只有显式开启时 plugin 才注入 `BankDepositApplication` 为 `bank_first_use` service，Web 才注册 `/api/v1/bank/v2/deposit`。默认旧 `BankApplication -> LegacyBankRepository -> transaction_service` 不变；8 个 bank migration/application/Web 测试、compileall、inventory、architecture、diff 通过。该灰度接线仍未在 live 开启，不能视为 bank handler 切片完成。

2026-09-13 bank first-use composition flag live safety：提交 `a4cfafd` 部署时未设置 `XIUXIAN_BANK_FIRST_USE_ENABLED`，真实 bank audit 仍 `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。该证据确认默认灰度关闭不会切换旧 bank runtime；新 v2 route/application 尚未在 live 启用。

2026-09-13 bank first-use command boundary：新增纯 `features/bank/commands.py::parse_first_use_deposit`，将金额校验、Clock 注入、settled_at 和 operation_id 生成边界从旧 NoneBot `xiuxian_bank/__init__.py` 抽出；`tests.test_bank_deposit_commands` 与 bank application/Web/migration 共 7 个通过，compileall、inventory、architecture、diff 通过。旧 `灵庄` handler 仍负责真实命令注册、旧 readf/get_give_stone 和消息展示，未切换到新 parser/application，故 bank slice 仍未完成。

2026-09-13 bank first-use withdrawal slice：新增纯 `decide_withdraw`、`BankWithdrawalApplication` 和 repository attached to game-db `bank_accounts`/`bank_account_operations`；覆盖 saved balance、interest、wallet update、operation replay/conflict、insufficient balance and rollback in isolated SQLite. Bank deposit/withdrawal/command/Web/migration tests共 11 个通过，compileall、inventory、architecture、diff 通过。新 withdrawal 未接入旧 `灵庄` handler 或 live runtime，旧 withdrawal service 仍是真实路径，bank slice 未完成。

2026-09-13 bank first-use withdrawal live safety：提交 `ec9d37b` 部署后未接入旧 `灵庄` handler，真实 bank audit `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。该证据证明新 withdrawal 核心不会改变旧 runtime，旧 withdrawal service 仍为真实执行路径。

2026-09-13 bank first-use upgrade slice：新增纯 `decide_upgrade`、`BankUpgradeApplication` 和 repository `save_upgrade`，复用 `bank_accounts`/`bank_account_operations`，覆盖等级乐观锁、钱包扣款、operation replay/conflict、余额不足和失败回滚。新增 opt-in `POST /api/v1/bank/v2/upgrade` route，仅在 `bank_first_use_upgrade` service 注入时注册；bank upgrade/application/Web 相关 8 个测试通过，compileall、inventory、architecture、diff 通过。旧 `灵庄升级会员` handler 和旧 `BankUpgradeService` 仍是真实路径，未计为 bank slice 完成。

2026-09-13 bank first-use upgrade wiring live safety：提交 `0c67a10` 默认 upgrade flag 关闭部署后，真实 bank audit `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T135334Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。该证据确认 opt-in upgrade service/route 未改变旧 bank runtime。

2026-09-13 bank first-use upgrade application completion boundary：`BankUpgradeApplication` 已在新 `bank_accounts`/`bank_account_operations` 上实现等级乐观锁、钱包扣款、operation replay/conflict 和失败回滚；`POST /api/v1/bank/v2/upgrade` 及 `bank_first_use_upgrade` opt-in wiring 已具备。相关升级/route/application 测试共 8 个通过。旧 `灵庄升级会员` NoneBot handler 和 `BankUpgradeService` 仍是真实默认路径，upgrade slice 只有在完成真实命令灰度、旧 attached `bankinfo` 行为对照及旧 service 隔离后才能关闭。

2026-09-13 bank first-use interest slice：新增纯 `decide_interest`、`BankInterestApplication` 和 repository `save_interest`，复用 game-db `bank_accounts`/`bank_account_operations`，显式接收 interest/settled_at，覆盖账户缺失、bank level 状态变化、operation replay/conflict、钱包入账与 ledger 记录。新增 opt-in `POST /api/v1/bank/v2/interest` route 和 `bank_first_use_interest` flag；interest/upgrade/withdraw/application/Web 相关 8 个测试通过，compileall、inventory、architecture、diff 通过。旧 `灵庄结算` handler 与 `BankInterestService` 仍是真实路径，未计为 bank slice 完成。

2026-09-13 bank first-use interest live safety：提交 `25cb6f4` 部署时 interest flag 默认关闭，真实 bank audit `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T154952Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。该证据证明新 interest application/route 默认未切换旧 bank runtime。

2026-09-14 bank first-use interest/command Clock boundary：修正新 `灵庄新存灵石` adapter，Clock 现在由 runtime composition 显式传入 builder，不再在 adapter 内隐式构造 `SystemClock`；interest application 同时具备 opt-in Web route/service。bank interest/upgrade/withdrawal/application/Web/command 相关 9 个测试通过，compileall、architecture、diff 通过。旧 `灵庄`/`灵庄结算` handler 仍默认执行，bank 全切片尚未完成。

2026-09-14 bank extended NoneBot matcher：扩展 matcher 现在按 `bank_first_use_withdrawal`、`bank_first_use_upgrade`、`bank_first_use_interest` service 分别注册 `灵庄新取灵石`、`灵庄新升级`、`灵庄新结息`，未启用的 service 不会留下不可用 matcher；三个 parser 均使用 runtime Clock/operation_id。14 个 bank/application/Web 相关测试、全包 compileall、inventory、architecture、diff 通过；旧 `灵庄` matcher 仍未切换。

2026-09-14 bank extended matcher live safety：提交 `b9d797c` 默认 extended flags 关闭部署，真实 bank audit `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T163059Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。该证据确认按 service 条件注册的 opt-in matcher 未改变旧 `灵庄` 执行路径。

2026-09-14 bank first-use info read slice：新增 `BankAccountInfoApplication` 和只读 `existing_account` repository 查询，读取新 `bank_accounts`/玩家钱包但不执行 DDL、写 ledger 或修改资产；新增 opt-in `GET /api/v1/bank/v2/info` 与 `bank_first_use_info` flag。info 加 bank interest/upgrade/withdrawal/application/Web 测试共 14 个通过，compileall、inventory、architecture、diff 通过。旧 `灵庄信息` 仍读取 legacy `readf`/attached `bankinfo` 并是真实默认路径，未计为 bank 完成。

2026-09-14 bank first-use info live safety：提交 `7a74d7b` 默认 info flag 关闭部署，真实 bank audit `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T165338Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。

2026-09-14 bank info conditional legacy-handler cutover：旧 `xiuxian_bank/__init__.py::bank_` 的 `信息` 分支现在先读取新 `BankAccountInfoApplication`；仅当新 `bank_accounts` 存在该用户时使用新 read model，否则继续 legacy `readf`/attached `bankinfo`，保护未迁移历史账户。该改动实际进入旧 matcher 执行链路，不是仅新增 facade；12 个 bank info/application/asset 测试、compileall、architecture、diff 通过。回滚点为移除该新查询分支或恢复旧源快照；live 尚无新 bank 账户，因此需用真实新账户灰度后才能扩大写操作切换。

2026-09-14 bank info conditional cutover live safety：提交 `91c2ff7` 部署后真实 bank audit `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T170103Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 audit 不变。由于 live 没有新 bank 账户，旧 info fallback 保持可用，未伪造新账户或迁移结果。

2026-09-14 bank deposit conditional cutover：旧 `bank_` 的 `存灵石` 分支现在先检查新 `bank_accounts`；已迁移账户走 `BankDepositApplication`，无新账户继续 legacy attached `bankinfo` 路径，避免把未迁移历史账户当成新账户。141 个 source-quality/bank asset 测试、compileall、inventory、architecture、diff 通过。该过渡分支仍在旧 adapter 中使用 `SystemClock` 生成 settled_at，旧计息与消息路径尚未移除；真实 live 验证和 Clock 完整注入仍是关闭 bank slice 的前置条件。

2026-09-14 bank deposit conditional cutover live safety：提交 `f948cbc` 部署后真实 bank audit `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T171129Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 audit 不变。由于 live 没有新账户，存款请求继续走 legacy fallback，未伪造新账户或业务回执。

2026-09-14 bank upgrade conditional cutover：旧 `bank_` 的 `升级会员` 分支现在先检查新 `bank_accounts`；已迁移账户走 `BankUpgradeApplication`，无新账户继续 legacy attached `bankinfo`/`BankApplication`，保留历史行为。该改动实际进入旧 matcher 执行链路；144 个 source-quality/bank asset 测试、compileall、inventory、architecture、diff 通过。过渡分支仍使用旧 adapter 的 `SystemClock` 和 `BANKLEVEL` 展示配置，旧会员逻辑及 service 尚未删除，不能计为 bank slice 完成。

2026-09-14 bank upgrade conditional cutover live safety：提交 `861d191` 部署后真实 bank audit `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T171958Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 audit 不变。live 没有新 bank 账户，升级请求继续 legacy fallback，未伪造账户或升级回执。

2026-09-14 bank withdrawal conditional cutover：旧 `bank_` 的 `取灵石` 分支现在先检查新 `bank_accounts`；已迁移账户走 `BankWithdrawalApplication`，无新账户继续 legacy attached `bankinfo`/`BankApplication`。过渡分支显式使用 `interest=0`，不假装替代尚未迁移的 legacy 按小时计息；144 个 source-quality/bank asset 测试、compileall、inventory、architecture、diff 通过。旧 withdrawal service、计息和消息路径仍保留，bank slice 未完成。

2026-09-14 bank withdrawal conditional cutover live safety：提交 `a97b349` 部署后真实 bank audit `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T172903Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 audit 不变。live 没有新账户，取款请求继续 legacy fallback，未伪造账户或取款回执。

2026-09-14 bank interest conditional cutover：旧 `bank_` 的 `结算` 分支现在先检查新 `bank_accounts`；已迁移账户使用纯 `calculate_interest` 和 `BankInterestApplication`，无新账户继续 legacy attached `bankinfo`/`BankApplication`。145 个 source-quality/bank asset 测试、compileall、inventory、architecture、diff 通过。过渡分支仍在旧 adapter 使用 `SystemClock` 和 `BANKLEVEL`，历史账户、旧按小时计息和旧 service 尚未隔离，bank slice 未完成。

2026-09-14 bank interest conditional cutover live safety：提交 `a6d7bbd` 部署后真实 bank audit `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T173739Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 audit 不变。live 无新账户，结算请求继续 legacy fallback，未伪造结息回执。

2026-09-14 bank interest rule deduplication：旧 `get_give_stone` 现在委托 `features.bank.interest_rules.calculate_interest`，保留旧返回结构和 legacy 时间格式，消除按小时利息公式分叉；新账户结算仍由 `BankInterestApplication` 处理，旧账户仍走 attached `bankinfo`。bank/source-quality/service 回归共 153 个通过，compileall、inventory、architecture、diff 通过。该改动未移除旧系统时间、JSON/PlayerDataManager 或 legacy handler，不计为 bank 完成。

2026-09-14 bank interest rule deduplication live safety：提交 `9e1d1e7` 部署后真实 bank audit `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`、`read_only=true`；backup `/srv/old/data/backups/20260913T174610Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 audit 不变。

2026-09-14 bank asset rowcount guard：`BankAccountRepository.save_deposit/save_withdrawal` 现在校验钱包与账户更新的 rowcount，外部状态在事务内变化时抛错并由 UoW 回滚，禁止写入“账本成功但资产未变”的 operation；144 个 bank/source-quality 测试、compileall、inventory、architecture、diff 通过。该改动只加强新账户路径，旧 attached `bankinfo` service 仍未删除。

2026-09-14 bank asset rowcount guard live safety：提交 `9359f47` 部署后真实 bank audit `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`；backup `/srv/old/data/backups/20260913T175453Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 audit 不变。

2026-09-14 bank migrated-account Clock wiring：新增 bank Clock context provider，旧 matcher 的新账户 deposit/withdrawal/upgrade/interest 分支改用 startup 注入的 runtime Clock；shutdown 时 reset，未注入时才保留兼容 SystemClock fallback。新增 context scope 测试；bank/source-quality 回归 145 个通过，compileall、architecture、diff 通过。legacy attached `bankinfo` fallback 的系统时间仍未迁移，不能计为 bank 完成。

2026-09-14 bank migrated-account Clock wiring live safety：提交 `f0923cc` 部署后真实 bank audit `bankinfo=false`、`bankinfo_rows=0`、`operation_ledgers={}`；backup `/srv/old/data/backups/20260913T181830Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 audit 不变。

2026-09-14 sign-in application-owned effects：新增 `features/sign_in/application_effects.py`，将签到 post-commit 的 statistics/task 编排从 compatibility effects 移入 feature-owned `SignInApplicationEffects`；lottery 仍通过显式 port，task core 仍使用已有 `SignInTaskEffects(record_task_progress)`，lottery schema 缺失时仍可 fallback legacy settlement。更新 wiring 测试断言默认 legacy startup 使用 feature-owned effects；相关 9 个 wiring/effects/task/statistics/lottery 测试通过，compileall、architecture、source-quality 通过。该子切片降低了 compatibility 编排，但不代表 task/lottery core 或 sign-in 整体完成。

2026-09-14 sign-in application-owned effects live safety：提交 `f91d16b` 部署后 backup `/srv/old/data/backups/20260913T183540Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，reconcile clean。恢复后 attached accessory audit 仍 `tables=[]`、`accessory_rows=0`、`read_only=true`、`metadata_valid=null`。该证据证明 feature-owned effects wiring 可加载真实数据，不代表 lottery/task core 已全部迁移。

2026-09-14 sign-in task projection slice：新增 game-db `sign_in_task_events` 与 `sign_in_task_projection`，实现签到 daily 1 次和 weekly 6 次的持久化进度、operation_id 去重及完成提示；新增 `sign_in.003` migration，默认 `SignInApplicationEffects` 改用 `ApplicationSignInTaskEffects`，不再导入旧 `record_task_progress`。task/wiring/effects/statistics/lottery 相关 11 个测试、compileall、architecture、source-quality 通过。该 slice 只覆盖签到任务，不覆盖其它旧任务定义或奖励领取，lottery fallback 仍可能存在。

2026-09-14 sign-in task projection live safety：提交 `247afec` 后真实预检 backup `/srv/old/data/backups/20260913T192136Z`、dry-run 仅 `sign_in.003`、reconcile clean；`sign_in.003` 实际应用成功，readiness 全绿。recovery smoke 覆盖 56 个迁移、五库 restore、reconcile clean；恢复后只读确认 `sign_in_task_events` 与 `sign_in_task_projection` 均存在，bank audit 仍为空且未写入业务数据。

2026-09-14 sign-in lottery runtime contract：修复 `SignInApplicationEffects` 向 `LotteryApplication.settle` 传递 `occurred_at` 时的契约缺口；新 lottery application 现在复用 effects 提供的同一 Clock 时间，并保留直接调用时的 Clock fallback。lottery/application/effects/task/wiring 14 个测试、compileall、architecture 通过；该修复确保默认新 sign-in composition 在 lottery schema 已迁移时不会运行时 `TypeError`，不代表 lottery legacy pool/fallback 已迁移完成。

2026-09-14 sign-in lottery runtime contract live safety：提交 `758cc04` 部署后 backup `/srv/old/data/backups/20260913T193857Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 56 个迁移、五库 restore、reconcile clean。恢复后只读确认 `sign_in_task_events`、`sign_in_task_projection` 存在；`lottery_pool_state` 不存在，故 live 仍保留 lottery legacy fallback，未伪造 lottery schema 或业务回执。

2026-09-14 lottery schema ownership slice：新增 `lottery.001` migration，并扩充 `LotteryRepository.ensure_schema` 创建 `lottery_pool_state`、settlement、participants、winner history、legacy migration ledger；仓库和 live 均确认不存在 `lottery_pool.json`，因此 migration 只建立空权威 schema，不回填或伪造历史 pool/participant/winner。`LotteryApplication` 在 schema 存在时接管默认 sign-in lottery，旧 `LotterySettlementService` 仅保留显式 fallback/rollback。lottery/application/legacy/wiring 16 个测试、57 个 migration 单调性检查、compileall、architecture、source-quality 通过；尚未完成真实新 lottery 奖池灰度行为对照。

2026-09-14 lottery schema ownership live safety：提交 `ff21dff` 后真实预检 backup `/srv/old/data/backups/20260913T195719Z`、dry-run 仅 `lottery.001`、reconcile clean；`lottery.001` 实际应用成功，五张 lottery 表存在且 `pool_amount=0`，readiness 全绿。recovery smoke 覆盖 57 个迁移、五库 restore、reconcile clean；由于无源 `lottery_pool.json`，未执行历史回填，旧 JSON settlement 仍保留为显式 rollback 路径。

2026-09-14 sign-in lottery vertical slice：新增隔离数据库端到端测试，真实调用 `SignInApplication.claim -> SignInApplicationEffects -> LotteryApplication/StatisticsRepository/ApplicationSignInTaskEffects`；固定 Clock/Random 下验证签到资产、operation ledger、lottery settlement、statistics、daily/weekly task projection 及同 operation replay 均幂等，最终只产生一条 lottery/task/statistics 事件。该测试与 lottery legacy/application/wiring 共 22 个测试通过，compileall、architecture、diff 通过；测试使用临时数据库，未向 live 用户写入业务数据，因此不替代 live 用户灰度行为对照。

2026-09-14 sign-in lottery vertical slice live safety：提交 `a8d65cc` 部署后真实预检 backup `/srv/old/data/backups/20260913T201640Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 57 个迁移、五库 restore、reconcile clean，恢复后 lottery 五张表存在且 `pool_amount=0`。该 live 证据只验证当前默认 composition 可启动、恢复和对账，不代表已在 live 用户上执行签到或 lottery 业务写入。

2026-09-14 lottery random provider：`LotteryApplication` 的默认随机源改为 infrastructure `SystemRandom`，移除 feature 内 `import random`/模块级 global random；显式 `RandomSource` 仍可注入，保留 `SystemClock` 兼容默认。lottery/sign-in/task/effects 垂直测试 23 个、source-quality 133 个、compileall、architecture、diff 通过。该切片只消除新 lottery 执行路径的 global random，不代表 legacy lottery service 或全局随机调用已清理。

2026-09-14 lottery random provider live safety：提交 `b391740` 部署后 backup `/srv/old/data/backups/20260913T202616Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 57 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`lottery_settlement_operations` 为 0。live 无 lottery 业务写入，因此该证据只证明 provider wiring 可启动和恢复，不代表 legacy random 已全量清理。

2026-09-14 lottery winner/audit consistency：新 `LotteryRepository.insert` 在同一 Unit of Work 内写入 `lottery_winner_history` 与 `economy_log`，保持中奖钱包、中奖历史和经济审计一致；`lottery.002` migration 显式创建 `economy_log`，避免业务路径隐式建表。隔离端到端 lottery/sign-in/legacy 测试 23 个、migration 单调性 58 个、source-quality 133 个、compileall、architecture 通过；若任一写入失败，事务整体回滚。

2026-09-14 lottery winner/audit consistency live safety：提交 `292a2b7` 部署后真实预检 backup `/srv/old/data/backups/20260913T204039Z`、dry-run 仅 `lottery.002`、reconcile clean；`lottery.002` 实际应用，`economy_log` schema 九列存在。readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `economy_log` 行数为 0、lottery pool 为 0。live 无 lottery 用户写入，未伪造中奖或审计回执。

2026-09-14 lottery legacy entry cutover：旧 `xiuxian_base.handle_lottery` 现在先检查 `lottery_pool_state`；已迁移 schema 的真实旧入口走 `LotteryApplication`，schema 缺失才回退 `LotterySettlementService`。保留旧文案和 fallback，未删除旧 service；`hongyun` 快照查询和旧模块 `datetime.now` 仍待迁移。该入口切换经 lottery/sign-in/legacy 17 个测试、compileall、architecture、source-quality 验证，未在 live 用户上执行业务写入。

2026-09-14 lottery legacy entry cutover live safety：提交 `d48c31a` 部署后 backup `/srv/old/data/backups/20260913T205510Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`economy_log` 行数为 0。live 无 lottery 用户写入，因此该证据只证明旧入口的 schema-gated cutover 可启动和恢复，不代表 fallback service 已删除。

2026-09-14 lottery snapshot entry cutover：新增 `LotterySnapshot/LotteryWinner` DTO、`LotteryApplication.snapshot` 和 repository 只读查询；真实 `hongyun` handler 在 `lottery_pool_state` 存在时使用新 snapshot，schema 缺失才 fallback 旧 `get_snapshot`。空池和中奖历史隔离测试覆盖，lottery/sign-in/legacy 18 个测试、compileall、architecture、source-quality 通过；旧模块 `datetime.now` 和 fallback service 仍保留。

2026-09-14 lottery snapshot entry cutover live safety：提交 `30ba706` 部署后 backup `/srv/old/data/backups/20260913T210227Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`economy_log` 行数为 0。live 无 lottery 用户写入，因此该证据只验证新 snapshot 查询路径可启动和恢复。

2026-09-14 lottery legacy lazy loading：移除 `xiuxian_base` 导入期 `LotterySettlementService` singleton；`hongyun` 与 `handle_lottery` 仅在 lottery schema 缺失时通过 `_legacy_lottery_service()` 懒加载 fallback，迁移安装的默认路径不再初始化旧 transaction service。旧 fallback 保留用于回滚；相关 18 个测试、compileall、architecture、source-quality 通过。

2026-09-14 lottery legacy lazy loading live safety：提交 `7415ab2` 部署后 backup `/srv/old/data/backups/20260913T211111Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`economy_log` 行数为 0。live 无 lottery 用户写入，未触发 fallback service。

2026-09-14 lottery runtime Clock：新增 sign-in Clock context provider；composition root 在 jobs/lifecycle 阶段注入 `context.clock`，shutdown 时 reset；旧 `hongyun` 与 `handle_lottery` 使用 `sign_in_clock()`，新 lottery application 继续接收显式 `occurred_at`。该切片移除 lottery 旧入口对系统时间的默认依赖；相关 18 个测试、compileall、architecture、source-quality 通过。

2026-09-14 lottery runtime Clock live safety：提交 `c29b345` 部署后 backup `/srv/old/data/backups/20260913T212433Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`economy_log` 行数为 0。live 无 lottery 用户写入，Clock 注入只验证启动、关闭和恢复路径。

2026-09-14 lottery command formatter：新增 feature-owned `format_lottery_snapshot`，将空奖池/中奖历史文案规则移出 legacy handler，并补 2 个 formatter 行为测试；lottery/sign-in/legacy 合计 20 个测试、compileall、architecture、source-quality 通过。当前 `hongyun` handler 尚未切换到该 formatter，本记录不宣称 command adapter 已完成迁移。

2026-09-14 lottery command formatter live safety：提交 `9aa68ce` 部署后 backup `/srv/old/data/backups/20260913T213648Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`economy_log` 行数为 0。live 无 lottery 用户写入；formatter 已验证可加载，不代表 `hongyun` handler 已切换。

2026-09-14 lottery command adapter cutover：旧 `hongyun` handler 现在真实调用 `features.sign_in.commands.format_lottery_snapshot`，legacy 模块只负责事件、snapshot 选择和发送；文案拼装已移出执行 adapter。formatter/lottery/sign-in/legacy 20 个测试、compileall、architecture、source-quality 通过。

2026-09-14 lottery command adapter cutover live safety：提交 `dd0620f` 部署后 backup `/srv/old/data/backups/20260913T214410Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`economy_log` 行数为 0。live 无 lottery 用户写入，command formatter cutover 仅验证启动、关闭和恢复路径。

2026-09-14 lottery result adapter cutover：旧 `handle_lottery` 现在真实调用 `features.sign_in.commands.format_lottery_result`，legacy 模块只负责 settlement application/fallback 选择；冲突、缺用户、重复参与、中奖和未中奖文案均移出旧执行模块。formatter/lottery/sign-in/legacy 22 个测试、compileall、architecture、source-quality 通过。

2026-09-14 lottery result adapter cutover live safety：提交 `b19f61b` 部署后 backup `/srv/old/data/backups/20260913T215208Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`economy_log` 行数为 0。live 无 lottery 用户写入，result formatter cutover 仅验证启动、关闭和恢复路径。

2026-09-14 lottery settlement command cutover：`features.sign_in.commands.settle_lottery` 现在负责 schema 判断、Clock 时间、`LotteryApplication` 新路径和显式 legacy fallback callable；`handle_lottery` 仅解析用户、传入兼容 callable 并格式化结果。旧 service 仍只作为 fallback，且由 lambda 懒加载；lottery/sign-in/legacy 22 个测试、compileall、architecture、source-quality 通过。

2026-09-14 lottery settlement command cutover live safety：提交 `cb18845` 部署后 backup `/srv/old/data/backups/20260913T220119Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 `pool_amount=0`、`economy_log` 行数为 0。live 无 lottery 用户写入，settlement command cutover 仅验证启动、关闭和恢复路径。

2026-09-14 tianti stone-training domain rule：新增纯 `decide_stone_training` 领域决策，实际 `StoneTrainingService.train` 调用该规则计算气血上限、实际气血增量和实际灵石扣款；补 cap/zero-charge 行为测试。tianti training application/Web 入口仍通过 legacy repository 执行，数据库 attach、legacy JSON 配置和双库写入尚未迁移；本切片不宣称 service/I/O 完成。

2026-09-16 tianti training composition boundary：确认 `TiantiTrainingApplication` 的 stone training、medicine bath、breakthrough、qiaoxue、item reward真实方法均分别使用已注册 SQL repository；移除 plugin 中未使用的 `LegacyTiantiTrainingRepository` import，并向 application 注入 context Clock。settlement legacy repository保留为显式兼容边界；tianti/source共179 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 tianti training composition live safety：提交 `3504bc6` 部署后 backup `/srv/old/data/backups/20260915T195845Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行天梯训练、药浴、突破或窍穴资产写入。

2026-09-14 tianti stone-training domain rule live safety：提交 `f71b1ab` 部署后 backup `/srv/old/data/backups/20260913T221654Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；recovery smoke 覆盖 58 个迁移、五库 restore、reconcile clean，恢复后 lottery `pool_amount=0`、`economy_log` 行数为 0。live 未执行 tianti 训练写入；本证据只验证规则改动不影响启动、关闭、恢复和现有对账状态。

2026-09-14 tianti stone-training repository cutover：新增 `StoneTrainingSqlRepository`，真实 Web `/api/v1/tianti/train` 默认走 feature-owned UoW；使用受控 `DatabaseUnitOfWork.attach_database` 完成 game/player 双库读写、余额条件扣款、`tianti_info` upsert、operation idempotency 和异常回滚。`TiantiTrainingApplication` 中 bath/breakthrough/qiaoxue 仍保留 legacy repository，stone training 的 `TiantiDataManager` 配置/JSON 投影暂未迁移；本切片不宣称整个 tianti feature 完成。

2026-09-14 tianti stone-training repository cutover live safety：提交 `b5887f5` 部署后 backup `/srv/old/data/backups/20260913T224245Z`、dry-run 仅 `tianti_training.002`、apply 成功、reconcile clean；readiness 全绿，recovery smoke 覆盖 59 个迁移、五库 restore，恢复后 `tianti_stone_training_operations` 存在、lottery `pool_amount=0`，reconcile 仍 `operations=0/outbox_events=0/dead_events=0`。live 未执行 tianti 用户训练写入。

2026-09-14 tianti player schema migration：新增 `tianti_training.003`，由 player_db 专属 migration runner 创建/补齐 `tianti_info` 字段；game_db 继续由 `tianti_training.002` 管理 stone-training operation 表。repository 保留 `IF NOT EXISTS`/兼容补列以支持旧安装，但默认启动先完成 schema。旧 `TiantiDataManager` JSON 投影仍是下一步边界；rollback 使用本次 backup，不执行数据回填。

2026-09-14 tianti migration routing correction：live 验证发现 maintenance CLI 原先只对 game_db 运行完整 migration catalog，导致 `tianti_training.003` 被错误记录到 game_db、player_db 未建 `tianti_info`。现已统一 CLI/startup 分库过滤：game_db 排除 player-only `.003`，player_db 执行 `title.001` 与 `.003`；既有 game_db migration 历史不篡改，纠正动作只在 player_db 补 schema。rollback 使用部署前 backup，不执行玩家数据回填。

2026-09-14 tianti migration routing correction live safety：提交 `6435932` 部署后 backup `/srv/old/data/backups/20260913T230023Z`；dry-run 真实返回 game_db `[]`、player_db `[tianti_training.003]`，apply 仅写入 player_db。reconcile clean，readiness 全绿；recovery smoke 恢复后 game_db migrations=60、player_db migrations=2、`tianti_info` 存在，`operations=0/outbox_events=0/dead_events=0`。未执行 tianti 用户训练写入，未篡改 game_db 已有迁移历史。

2026-09-14 tianti profile reader：`StoneTrainingSqlRepository` 默认改用 feature-owned `TiantiProfileReader` 读取运行数据目录的炼体境界 profile，移除新 Web train 路径对 `TiantiDataManager`、`xiuxian2_handle` 和 legacy `get_tianti_cap` 的运行时依赖；profile reader 实例持有缓存，缺失/非法 profile 明确失败，不伪造等级数据。测试可显式注入 profile/data manager/cap provider；legacy repository 仍供 bath/breakthrough/qiaoxue 使用。

2026-09-14 tianti profile path correction：首次 live 预检发现 profile 实际位于部署源码 `/srv/src/data/xiuxian/炼体/炼体境界.json`，而 repository 初版错误查找 `/srv/old/data/炼体/炼体境界.json`；当次未执行用户训练，backup `/srv/old/data/backups/20260913T231537Z`、migration dry-run 全库 pending 为空、reconcile clean。现将默认根修正为 `player_db` 同级 `xiuxian/`，并在下一次 live 预检验证真实 profile load；失败回滚使用上述 backup。

2026-09-14 tianti profile path correction live safety：提交 `6e2410a` 部署后 backup `/srv/old/data/backups/20260913T232004Z`、migration dry-run 全库 pending 为空、reconcile clean；使用 `/srv/venv/bin/python` 真实加载 profile，90 个等级、首级 `淬体境一重`、cap=18000。readiness 全绿；recovery smoke 恢复后 game_db migrations=60、player_db migrations=2、`tianti_info` 存在，reconcile clean。一次使用容器系统 `python3` 的 profile 查询因缺少 venv 依赖失败，未改变数据，后用部署 venv 重试成功。

2026-09-14 tianti runtime schema boundary：`StoneTrainingSqlRepository` 删除请求路径 `CREATE/ALTER`，只读取 migration 创建的 operation/`tianti_info` schema；缺表/缺列明确抛出 schema-not-ready，不隐式修改结构。补缺迁移失败且不扣款测试；默认 Web train 依赖 `tianti_training.002/.003` 启动完成 schema。

2026-09-14 tianti runtime schema boundary live safety：提交 `be404c8` 部署后 backup `/srv/old/data/backups/20260913T233037Z`、全库 migration dry-run pending 为空、reconcile clean；使用部署 venv 真实加载 90-level profile，readiness 全绿。recovery smoke 恢复后 game_db migrations=60、player_db migrations=2、`tianti_info` 存在，reconcile `operations=0/outbox_events=0/dead_events=0`。live 未执行 tianti 用户训练写入。

2026-09-14 tianti breakthrough domain rule：新增纯 `decide_breakthrough`，实际 `TiantiBreakthroughService.attempt` 调用该规则处理无下一境界、修仙境界不足、气血不足、成功升级、失败掷骰保留等级和 5% 气血消耗；15 个 focused tests、compileall、architecture、source-quality 通过。突破 repository 的 legacy SQLite/JSON I/O 尚未迁移，本切片不宣称突破 service 或 Web 已完成切换。

2026-09-14 tianti breakthrough domain rule live safety：提交 `8dd82bd` 部署后 backup `/srv/old/data/backups/20260913T234522Z`、全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 恢复后 game_db migrations=60、player_db migrations=2、`tianti_info` 存在，reconcile `operations=0/outbox_events=0/dead_events=0`。live 未执行玩家突破写入。

2026-09-14 tianti breakthrough repository cutover：新增 `TiantiBreakthroughSqlRepository`，默认 Web `/api/v1/tianti/breakthrough` 改为 player_db UoW、feature profile、`decide_breakthrough` 和 `tianti_breakthrough_operations`；覆盖幂等、成功升级、失败掷骰、气血不足和 schema-not-ready。旧 `TiantiBreakthroughService` 仍保留给兼容回滚，bath/qiaoxue 仍走 legacy repository；新增 player migration `tianti_training.004`，待 live 应用。

2026-09-14 tianti breakthrough migration routing correction：首次 `.004` live dry-run 发现 game_db 过滤仅排除了 `.003`，误将 player-only `.004` 列为 game_db pending；未执行 apply、未改变数据。现已统一 startup/CLI 排除 `.003/.004`，player_db 执行两者；此类分库路由错误保留在回滚记录中，不篡改既有 migration 历史。

2026-09-14 tianti breakthrough repository cutover live safety：提交 `740b93d` 部署后 backup `/srv/old/data/backups/20260914T002130Z`；dry-run 真实返回 game_db `[]`、player_db `[tianti_training.004]`，apply 仅写入 player_db。player_db migrations=3，`tianti_breakthrough_operations` 与 `tianti_info` 均存在，readiness 全绿；recovery smoke 覆盖当前 61-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家突破写入。

2026-09-14 tianti breakthrough persistence completion：`TiantiBreakthroughSqlRepository` 已完成本地真实行为验证并接入默认 Web/application 路径；新增 player migration `.004` 创建突破 operation 表，repository 不执行运行期 DDL，使用 feature profile 和 domain decision 完成幂等读写/失败拒绝。覆盖突破成功、失败掷骰、气血不足、重复请求和 schema-not-ready；14 个 focused tests、compileall、architecture、source-quality 通过。旧 `TiantiBreakthroughService` 仅保留兼容回滚，未宣称 tianti bath/qiaoxue 或整个第 2 阶段完成。

2026-09-14 tianti breakthrough persistence live safety：补提交 `9386383`（仅补台账）部署后 backup `/srv/old/data/backups/20260914T003203Z`、全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖当前 61-entry catalog，player_db migrations=3，`tianti_breakthrough_operations` 存在，reconcile `operations=0/outbox_events=0/dead_events=0`。live 未执行玩家突破写入。

2026-09-14 tianti qiaoxue repository cutover：新增 `TiantiQiaoxueSqlRepository`，默认 Web `/api/v1/tianti/qiaoxue` 使用 player_db UoW、feature-owned qiaoxue pool、显式 roll 和 `.005` operation migration；覆盖首次开窍、候选去重、10% 气血扣除、上限、气血不足、operation replay 与 schema-not-ready。旧 `QiaoxueService` 仍是兼容回滚，bath 仍未迁移。

2026-09-14 tianti qiaoxue repository cutover live safety：提交 `9e9411a` 部署后 backup `/srv/old/data/backups/20260914T005042Z`；dry-run 真实返回 game_db `[]`、player_db `[tianti_training.005]`，apply 仅写入 player_db。player_db migrations=4，`tianti_qiaoxue_operations` 存在，readiness 全绿；recovery smoke 覆盖当前 62-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家开窍写入。

2026-09-14 tianti qiaoxue persistence completion：`TiantiQiaoxueSqlRepository` 完成默认 application/Web 接线，使用 player_db UoW、feature-owned pool、已迁移 schema 和 operation replay；21 个 qiaoxue/breakthrough/stone/application/legacy focused tests、compileall、architecture、source-quality 通过。旧 `QiaoxueService` 仅保留兼容回滚，tianti bath 仍未迁移。

2026-09-14 tianti qiaoxue persistence live safety：补提交 `fd94c84` 部署后 backup `/srv/old/data/backups/20260914T005717Z`、全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 62-entry catalog，player_db migrations=4，`tianti_qiaoxue_operations` 存在，reconcile `operations=0/outbox_events=0/dead_events=0`。live 未执行玩家开窍写入。

2026-09-14 tianti bath activation rule：新增纯 `decide_medicine_bath_activation`，使用显式 now/duration/effect 判断 active/expired/invalid；真实 `MedicineBathService.apply` 调用该规则，同时保留旧 active adapter 作为兼容测试 seam。14 个 focused tests、legacy bath 回归、compileall、architecture、source-quality 通过；药浴跨库库存/结算 repository 尚未迁移。

2026-09-14 tianti bath activation live safety：提交 `231733c` 部署后 backup `/srv/old/data/backups/20260914T011656Z`、全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 62-entry catalog，player_db migrations=4，`tianti_info` 存在，reconcile `operations=0/outbox_events=0/dead_events=0`。live 未执行药浴库存或玩家状态写入。

2026-09-14 tianti settlement window rule：新增纯 `decide_tianti_settlement_window`，将 init/empty/elapsed-minute floor 规则接入真实 `settle_tianti_gain`；固定时间覆盖 16 个 focused tests、legacy bath/settlement 回归、compileall、architecture、source-quality 通过。收益公式、世界事件 multiplier、JSON/SQLite I/O 仍在 legacy service，药浴跨库 repository 尚未切换。

2026-09-14 tianti settlement window live safety：提交 `b365087` 部署后 backup `/srv/old/data/backups/20260914T014024Z`、全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 62-entry catalog，player_db migrations=4，reconcile `operations=0/outbox_events=0/dead_events=0`。live 未执行药浴或结算写入。

2026-09-14 tianti gain decision rule：新增纯 `TiantiGainDecision/decide_tianti_gain`，真实 `_apply_tianti_minutes` 调用该规则计算倍率、收益和上限裁剪；固定数值/零分钟、settlement window、药浴回归共 16 tests 通过。旧 world-event multiplier、profile JSON 和跨库 I/O 仍由 legacy service 读取，本切片不宣称 bath repository 完成。

2026-09-14 tianti gain decision live safety：提交 `ea2d1dd` 部署后 backup `/srv/old/data/backups/20260914T015237Z`、全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 62-entry catalog，player_db migrations=4，reconcile `operations=0/outbox_events=0/dead_events=0`。live 未执行玩家结算或药浴写入。

2026-09-14 tianti medicine bath repository cutover：新增 `TiantiMedicineBathSqlRepository`，默认 Web `/api/v1/tianti/bath` 改走 game_db 主 UoW + player_db attach；使用 feature profile、settlement window/gain decisions，完成 active 检查、库存不足、跨库材料扣减、player 炼体状态写回、operation replay 和异常回滚。新增 game migration `tianti_training.006`；28 个 focused tests、legacy bath/settlement 回归、compileall、architecture、source-quality 通过。旧 `MedicineBathService` 仅保留兼容回滚；world-event multiplier 默认通过显式 provider，尚未完成全部 world-event/JSON 配置迁移。

2026-09-14 tianti medicine bath repository cutover live safety：提交 `4cd63bf` 部署后 backup `/srv/old/data/backups/20260914T021809Z`；首次 schema 核对误用了不存在的 `/srv/old/data/game.db`，未改变数据，随后按 `XiuxianPaths` 核对真实 `/srv/old/data/xiuxian.db`，确认 game migrations=63、`tianti_medicine_bath_operations` 存在、player migrations=4。readiness 全绿，recovery smoke 覆盖 63-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行药浴库存或玩家状态写入。

2026-09-14 tianti settlement repository cutover：新增 `TiantiSettlementSqlRepository`，默认 Web `/api/v1/tianti/settle` 使用 player_db UoW、feature profile、settlement-window/gain decisions 和 operation replay；新增 player migration `tianti_settlement.002`。覆盖 init/elapsed settlement、旧 JSON 字段投影、replay、schema-not-ready；11 个 focused tests、compileall、architecture、source-quality 通过。旧 `TiantiSettlementService` 保留兼容回滚，world-event multiplier 仍显式 provider，未宣称全部 settlement/world-event 迁移完成。

2026-09-14 tianti settlement migration routing correction：首次 `.002` live dry-run 误将 player-only `tianti_settlement.002` 同时列入 game_db/player_db，未执行 apply、未改变数据；现已统一 startup/CLI 将其排除于 game_db，仅 player_db 执行，并通过 64-entry catalog ordering 检查。

2026-09-14 tianti settlement repository cutover live safety：提交 `7b15247` 部署后 backup `/srv/old/data/backups/20260914T025022Z`；dry-run 真实返回 game_db `[]`、player_db `[tianti_settlement.002]`，apply 仅写入 player_db。player_db migrations=5，`tianti_settlement_operations` 存在，readiness 全绿；recovery smoke 覆盖 64-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家结算写入。

2026-09-16 tianti settlement composition cleanup：确认 `TiantiSettlementApplication` 默认使用已注册 `TiantiSettlementSqlRepository`，移除 plugin 中未使用的 `LegacyTiantiSettlementRepository` import；legacy settlement service保留为显式 rollback。tianti/source共179 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 tianti settlement composition live safety：提交 `1792153` 部署后 backup `/srv/old/data/backups/20260915T200609Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行天梯结算写入。

2026-09-14 tianti NoneBot/application wiring cutover：移除 `xiuxian_tianti/__init__.py` 与 composition root 对 `LegacyTiantiSettlementRepository/LegacyTiantiTrainingRepository` 的显式注入，默认命令/application 使用 feature-owned repositories；旧 service 实例仅作为兼容变量保留。23 个 tianti/settlement/bath/item focused tests、compileall、architecture、source-quality 通过。

2026-09-14 tianti NoneBot/application wiring live safety：提交 `38f2908` 部署后 backup `/srv/old/data/backups/20260914T032022Z`、全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 64-entry catalog，真实 game_db migrations=64、player_db migrations=5，默认 tianti application wiring 可启动。live 未执行玩家资产写入。

2026-09-14 tianti item reward migration routing correction：首次 `.007` live dry-run 发现 game/player exclusion 集合都排除了 game-only `tianti_training.007`，导致 pending 静默为空且 operation 表未创建；未执行 apply、未改变数据。现已只从 game exclusion 移除 `.007`，player runner 仍排除，补迁移前保留 backup/rollback 点。

2026-09-14 tianti item reward live safety：提交 `fd0ef45` 部署后 backup 未改变业务数据；修正后的 dry-run 真实返回 game_db `[tianti_training.007]`、player_db `[]`，apply 仅写入 game_db。game_db migrations=65、player_db migrations=5，`tianti_item_reward_operations` 存在，readiness 全绿；recovery smoke 覆盖 65-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家物品奖励写入。

2026-09-14 combat settlement repository cutover：新增 `CombatSettlementSqlRepository`，默认 `CombatSettlementApplication`/Web `/api/v1/combat/settle` 不再显式使用 legacy adapter；game 主库 attach player_db，覆盖每日计数、snapshot 防重放、stone/item 奖励、库存上限、operation replay 与回滚。`combat_settlement.001` 预创建 `map_combat_settlement_operations`，3 个 repository tests、combat/application/legacy 回归、compileall、architecture、source-quality 通过。修复 mapping row 被按列名迭代导致所有结算误报 `state_changed` 的真实 bug；旧 `MapCombatSettlementService` 保留兼容回滚。

2026-09-14 combat migration checksum correction：首次 live 预检正确拒绝了修改已应用 `combat_settlement.001` 的 checksum drift，未执行 apply；恢复 `.001` 原实现并新增 `combat_settlement.002` 专门创建 `map_combat_settlement_operations`，通过 66-entry migration ordering/checksum 门禁，保留原 backup/rollback 点。

2026-09-14 combat settlement live safety：提交 `fa1eaf0` 部署后 backup `/srv/old/data/backups/20260914T054921Z`；dry-run 真实返回 game_db `[combat_settlement.002]`、player_db `[]`，apply 仅写入 game_db。game_db migrations=66、player_db migrations=5，`map_combat_settlement_operations` 存在，readiness 全绿；recovery smoke 覆盖 66-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家战斗结算写入。

2026-09-16 combat settlement composition cleanup：复核 `CombatSettlementApplication` 默认使用 `CombatSettlementSqlRepository`/`DaoBattleSqlRepository`，移除 plugin 中未使用的 `LegacyCombatSettlementRepository` import；旧 MapCombatSettlementService 保留为显式 rollback。combat/source共149 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 combat settlement composition live safety：提交 `f03683f` 部署后 backup `/srv/old/data/backups/20260915T203651Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行战斗结算或道具资产写入。

2026-09-14 dao battle settlement cutover：新增 `DaoBattleSqlRepository` 与 `CombatSettlementApplication.settle_dao_battle`，真实 NoneBot `dao_qc` handler 改走 application；player_db 主库 attach game_db，覆盖双方位置校验、对称胜负统计、operation replay/state conflict、失败回滚。新增 player-only migration `combat_settlement.003` 创建 `map_dao_battle_operations`；142 个 combat/dao/application/source tests、compileall、architecture 通过。旧 `MapDaoBattleSettlementService` 保留兼容回滚，`dao_view` 读路径仍待迁移。

2026-09-14 dao battle schema correction：发现新 repository 仍在请求路径创建 `dao_record`，已移入 checksum-safe player migration `combat_settlement.004`；恢复已应用 `.003` operation-only 实现，未修改其 checksum，142 tests、66-entry catalog ordering 和 architecture 通过。

2026-09-14 dao battle schema live safety：提交 `58aacdb` 部署后 backup `/srv/old/data/backups/20260914T064514Z`；dry-run 真实返回 game_db `[]`、player_db `[combat_settlement.004]`，apply 仅写入 player_db。game_db migrations=68、player_db migrations=7，`map_dao_battle_operations` 与 `dao_record` 存在，readiness 全绿；recovery smoke 覆盖 68-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家道战写入。

2026-09-14 dao battle read cutover：`dao_view` NoneBot handler 已改用 `CombatSettlementApplication.get_dao_record` 与 feature query repository，不再直接读取 `PlayerDataManager`；道战写入仍通过 `settle_dao_battle`，读写路径均位于 combat feature 边界。142 个 dao/combat/source tests、compileall、architecture 通过。

2026-09-14 dao battle read live safety：提交 `5d3d5d4` 部署后 backup `/srv/old/data/backups/20260914T110006Z`、migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 68-entry catalog，game_db migrations=68、player_db migrations=7，`map_dao_battle_operations` 与 `dao_record` 均存在。live 未执行玩家道战读写。

2026-09-14 map movement repository cutover：新增 `MapMovementSqlRepository`，默认 `MapApplication.move` 与 NoneBot 跨界/跨天/节点移动三条真实路径切换到 game_db 主库 attach player_db；新增 game migration `map.002` 创建 `map_movement_operations`，覆盖位置/visited/stamina 原子更新、snapshot replay、过期状态拒绝。其它 map application 动作仍保留 legacy fallback。

2026-09-14 map movement live safety：提交 `bd4ecf9` 部署后 backup `/srv/old/data/backups/20260914T114641Z`；dry-run 真实返回 game_db `[map.002]`、player_db `[]`，apply 仅写入 game_db。game_db migrations=69、player_db migrations=7，`map_movement_operations` 存在，readiness 全绿；recovery smoke 覆盖 69-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家移动写入。

2026-09-14 map home return cutover：新增 `MapHomeReturnSqlRepository`，默认 `MapApplication.return_home` 与 NoneBot `go_home` 使用 player_db UoW，覆盖洞府/位置校验、visited 更新、replay、operation conflict 与失败状态；新增 player migration `map.003` 创建 `map_home_return_operations`。home-return legacy/application 回归、source-quality、compileall、architecture 通过，旧 `MapHomeReturnService` 保留兼容回滚。

2026-09-14 map home return live safety：提交 `25c38aa` 部署后 backup `/srv/old/data/backups/20260914T121539Z`；dry-run 真实返回 game_db `[]`、player_db `[map.003]`，apply 仅写入 player_db。game_db migrations=70、player_db migrations=8，`map_home_return_operations` 存在，readiness 全绿；recovery smoke 覆盖 70-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家回府写入。

2026-09-14 map home return verification rerun：提交 `7e7abb5` 部署后 backup `/srv/old/data/backups/20260914T122802Z`，全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 再次确认 catalog=70、game_db migrations=70、player_db migrations=8、`map_home_return_operations` 存在。live 未执行玩家回府写入。

2026-09-14 map interactive action decision rule：新增纯 `decide_interactive_action`，将 start handler 的等待时间、ready/expire 时间、成功随机结果移出 NoneBot 规则，改用 map runtime `SystemClock/SystemRandom` provider；143 个 map/interactive/source tests、compileall、architecture 通过。旧 `MapInteractiveActionService` 的 start/finish persistence、active query 和 resource settlement 仍未切换，未宣称 interactive 完整迁移。

2026-09-14 map interactive decision live safety：提交 `b044a87` 部署后 backup `/srv/old/data/backups/20260914T125638Z`、migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 70-entry catalog，game_db migrations=70、player_db migrations=8。该切片仅迁移时间/随机领域决策，live 未执行资源行动写入，interactive persistence/finish 仍保留 legacy rollback。

2026-09-14 map interactive active query cutover：新增 `MapInteractiveSqlQueryRepository.get_active` 与 `MapApplication.get_active`，start/ready/resolve 三条真实路径不再直接调用 legacy `get_active`；兼容 JSON 投影读取由 feature query 负责。143 个 interactive/map/source tests、compileall、architecture 通过。start/finish persistence、settlement 和 failure terminal 仍保留 legacy rollback。

2026-09-14 map interactive active query live safety：提交 `3ea7f7a` 部署后 backup `/srv/old/data/backups/20260914T131630Z`、migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 70-entry catalog，game_db migrations=70、player_db migrations=8。live 未执行资源行动写入，start/finish persistence 仍保留 legacy rollback。

2026-09-14 map interactive start persistence cutover：新增 `MapInteractiveStartSqlRepository` 与 `MapApplication.interactive_start`，真实节点行动 start handler 改用 game_db 主库 attach player_db；新增 game migration `map.004`（`map_interactive_start_operations`）和 player migration `map.005`（active action/terminal/cooldown schema）。覆盖 operation replay/conflict、用户/位置/每日次数/冷却/体力校验、active action 和跨库状态写入；143 个 interactive/map/source tests、compileall、architecture 通过。旧 `MapInteractiveActionService` 的 finish/save_settlement/failure/resource settlement 仍作为兼容回滚。

2026-09-14 map interactive start live safety：提交 `7a18580` 部署后 backup `/srv/old/data/backups/20260914T134306Z`；dry-run 真实返回 game_db `[map.004]`、player_db `[map.005]`，apply 两库分别完成。game_db migrations=72、player_db migrations=9，`map_interactive_start_operations` 与 `map_interactive_actions` 存在，readiness 全绿；recovery smoke 覆盖 72-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行资源行动写入，finish/save_settlement/failure/settlement 仍为 legacy rollback。

2026-09-14 map interactive failure terminal cutover：新增 `MapInteractiveFailureSqlRepository` 与 `MapApplication.interactive_failure`，ready notice 超时、非法状态、resolve 超时、失败四条真实路径统一写 player_db `map_interactive_actions/map_cooldown/map_interactive_terminal_operations`，覆盖 replay/conflict/state_changed/rollback；143 个 interactive/map/source tests、compileall、architecture 通过。成功 `save_settlement`、resource reward 与其它 finish 结算仍保留 legacy rollback。

2026-09-14 map interactive failure terminal live safety：提交 `4e76988` 部署后 backup `/srv/old/data/backups/20260914T140426Z`、migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 72-entry catalog，game_db migrations=72、player_db migrations=9，`map_interactive_terminal_operations` 存在。live 未执行资源行动写入，成功 settlement 仍为 legacy rollback。

2026-09-14 map interactive settlement snapshot cutover：新增 `MapInteractiveSettlementSqlRepository` 与 `MapApplication.interactive_settlement`，成功 resolve 的 settlement JSON 保存、duplicate 和 state_changed 已移出 legacy `save_settlement`；operation_id 显式为 `map-interactive-settlement:<action_id>`。143 个 interactive/map/source tests、compileall、architecture 通过。奖励随机生成、背包/灵石发放、每日计数和 completed/cooldown 最终结算仍为 legacy rollback。

2026-09-14 map resource reward persistence cutover：新增 `MapResourceRewardSqlRepository`，默认 `MapApplication.resource_reward` 与真实 interactive resolve handler 不再调用 `MapResourceRewardService.settle`；game_db 主库 attach player_db，在同一 UoW 中校验 settlement snapshot/每日次数/背包上限，原子更新灵石、背包、每日计数、action completed、cooldown 与 operation replay。新增 game migration `map.006` 创建 `map_resource_reward_operations`；3 个新 repository 行为测试与 legacy reward/interactive/source 回归共 149 tests、73-entry catalog、compileall、architecture 通过。奖励池随机生成仍在旧 handler，旧 service 仅保留兼容回滚；跨文件当前依赖 SQLite ATTACH 原子事务，尚未增加独立 outbox 补偿模式。

2026-09-14 map resource reward persistence live safety：提交 `12d9e1d` 部署后 backup `/srv/old/data/backups/20260914T150809Z`；dry-run 真实返回 game_db `[map.006]`、player_db `[]`，apply 仅写入 game_db。game_db migrations=73、player_db migrations=9，`map_resource_reward_operations` 存在，readiness 全绿；recovery smoke 覆盖 73-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家资源奖励写入，奖励随机生成仍在 handler。

2026-09-15 map interactive reward decision cutover：新增纯 `decide_interactive_reward`，成功 resolve 的低收益/幸运/普通三分支移出 NoneBot；`SystemRandom` 补 `choice` port，reward pool、洞府材料和额外装备 helper 接收显式 random source，真实 interactive resolve 全部传 `runtime_random`。4 个 domain tests 与 reward/interactive/source 回归共 153 tests、compileall、architecture 通过。全局 Items lookup 与其它 map 奖励 helper 仍为 legacy 边界，未宣称 map 全部随机依赖完成。

2026-09-15 map interactive reward decision live safety：提交 `5b51d52` 部署后 backup `/srv/old/data/backups/20260914T161020Z`，全库 migration dry-run pending 为空、reconcile clean、readiness 全绿；recovery smoke 覆盖 73-entry catalog，game_db migrations=73、player_db migrations=9。live 未执行玩家奖励写入。

2026-09-15 map explore start cutover：新增 `MapExploreStartSqlRepository`，默认 `MapApplication.explore_start` 与真实 `开始探索` handler 不再调用 `MapExploreStartService.start`；handler 时间/operation fallback 改用 runtime Clock/UUIDGenerator。新增 game migration `map.007` 创建 start operation，player migration `map.008` 创建/补齐 explore status 与 cooldown schema；repository 在同一 attached UoW 校验位置、idle snapshot、daily、cooldown、stamina并原子扣体力/写 active状态/operation。3 个新 repository tests 与 legacy/source 回归共 143 tests、75-entry catalog、compileall、architecture通过。旧 service保留兼容回滚，explore settlement 仍 legacy。

2026-09-15 map explore start live safety：提交 `785e267` 部署后 backup `/srv/old/data/backups/20260914T162230Z`；dry-run game `[map.007]`、player `[map.008]`，apply 两库完成。game migrations=74、player migrations=10，start/status schema存在；readiness全绿，75-entry recovery reconcile clean。live 未执行玩家探索写入。

2026-09-15 map explore settlement cutover：新增 `MapExploreSettlementSqlRepository`，默认 `MapApplication.explore_settle` 与真实 `探索结算` handler 不再调用 legacy settlement service；game主库attach player，在同一UoW校验探索/daily snapshot和背包，原子清空探索、增加计数/灵石/物品并记录operation，Clock显式注入。新增game migration `map.009`；3个新repository tests及新旧start/settlement/source共152 tests、76 catalog、compileall、architecture通过。探索事件随机生成仍在handler，旧service保留回滚。

2026-09-15 map explore settlement live safety：提交 `38e17c9` 部署后 backup `/srv/old/data/backups/20260914T163244Z`，dry-run/apply仅game `[map.009]`；game migrations=76，readiness全绿，76-entry recovery reconcile clean。live未执行玩家探索结算写入。

2026-09-15 map mission claim cutover：新增 `MapMissionClaimSqlRepository`，默认 `MapApplication.mission_claim` 与真实 `委托完成` handler 不再调用 legacy service；白名单 progress key，game主库attach player原子校验mission/progress/claimed/背包并发放灵石物品、标记领取和记录operation，Clock显式注入。新增game migration `map.010`；3个repository tests加legacy/source共141 tests、77 catalog、compileall、architecture通过。任务奖励随机snapshot仍在handler，旧service保留回滚。

2026-09-15 map mission claim live safety：提交 `7c26de2` 部署后 backup `/srv/old/data/backups/20260914T164030Z`，dry-run/apply仅game `[map.010]`；readiness全绿，77-entry recovery reconcile clean。live未执行玩家委托领取。

2026-09-15 map seed purchase cutover：新增 `MapSeedPurchaseSqlRepository`，默认 `MapApplication.purchase_seed` 与真实 `购买种子` handler 不再调用 legacy service；game UoW 原子校验stone/inventory、扣款/入包/operation replay，Clock/UUID显式注入。新增game migration `map.011`；legacy seed/source共141 tests、78 catalog、compileall、architecture通过。旧service保留回滚。

2026-09-15 map seed purchase live safety：提交 `fbca7bd` 部署后 backup `/srv/old/data/backups/20260914T165007Z`，dry-run/apply仅game `[map.011]`；readiness全绿，78-entry recovery reconcile clean。live未执行购买写入。

2026-09-15 map dongfu build cutover：新增 `MapDongfuBuildSqlRepository`，默认 `MapApplication.build_dongfu` 与真实 `建设洞府` handler 不再调用legacy service；game主库attach player原子校验位置/余额/已有洞府、扣款、upsert洞府和operation，UUID显式注入。新增game `map.012` 与player `map.013` schema migrations；legacy dongfu/source共141 tests、80 catalog、compileall、architecture通过。旧service保留回滚。

2026-09-15 map dongfu build live safety：提交 `19b2f0c` 部署后 backup `/srv/old/data/backups/20260914T165849Z`，dry-run/apply game `[map.012]`、player `[map.013]`；readiness全绿，80-entry recovery reconcile clean。live未执行洞府建设写入。

2026-09-15 map combat lifecycle query cutover：新增 `MapCombatLifecycleQueryRepository` 与 `MapApplication.combat_pending/combat_replay`，节点战斗真实handler的pending/replay读取不再访问legacy service；新增game `map.014` start-operation与player `map.015` combat snapshot/cooldown schema，覆盖pending解析、replay duplicate/conflict和缺失状态。148 tests、82 catalog、compileall、architecture通过。combat start/save_plan写入仍是下一独立slice，旧service保留回滚。

2026-09-15 map combat lifecycle query live safety：提交 `2e0909f` 部署后 backup `/srv/old/data/backups/20260914T171527Z`，dry-run/apply game `[map.014]`、player `[map.015]`；readiness全绿，82-entry recovery reconcile clean。live未执行节点战斗写入。

2026-09-15 map combat lifecycle start cutover：新增 `MapCombatLifecycleStartSqlRepository` 与 `MapApplication.combat_start`，节点战斗真实handler的体力、位置、每日次数、冷却、active snapshot和operation写入已离开legacy service；复用 `map.014/.015` 固定schema，不在请求建表。新增路径含duplicate/limit/already-running/rollback测试，163 tests、82 catalog、compileall、architecture通过。`save_plan`及战斗结算仍为后续独立slice。

2026-09-15 map combat lifecycle plan cutover：新增 `MapCombatLifecyclePlanSqlRepository` 与 player operation ledger `map_combat_plan_operations`，真实战斗handler的 planned snapshot 写入已离开legacy `save_plan`；校验 task/running 状态并支持 operation duplicate/conflict/state_changed，固定migration `map.016`，161 tests、83 catalog、compileall、architecture通过。战斗资产结算仍为下一slice。

2026-09-15 map combat lifecycle plan live safety：`c343f7d` 首次部署发现遗漏的 `apply_map_combat_plan` 未进入提交，远端 import 失败；修复提交 `f241cb8` 补齐函数并重新部署。backup `/srv/old/data/backups/20260914T173611Z`，dry-run/apply 仅 player `[map.016]`，reconcile clean，readiness全绿，83-entry recovery clean。首次失败未执行迁移，未产生玩家写入。

2026-09-15 map combat lifecycle plan/random closure：真实节点战斗的 `save_plan` 已经由 `MapApplication.combat_save_plan` 写入 player operation ledger；敌人生成、胜负大胜判定和三类奖励helper改用注入 `runtime_random`，`SystemRandom.uniform`补齐。146 tests、compileall、architecture通过；计划写入和随机边界共用本次handler改动，旧service仅保留回滚。

2026-09-15 map explore event random closure：`_pick_explore_event`、`_roll_explore_event`及其真实探索结算循环改用显式 `runtime_random`，包括weighted choices、empty文本、奖励池、掉落与洞府契约；`SystemRandom.choices`补齐。144 tests、compileall、architecture通过，无新增schema，旧随机路径仅保留非默认兼容调用。

2026-09-15 map interactive replay query cutover：`_process_node_action` 的 interactive start replay 查询改用 `MapApplication.interactive_replay` 与 `MapInteractiveSqlQueryRepository`，不再访问legacy action service；复用既有 `map.004/.005` schema，147 tests、compileall、architecture通过。interactive start/failure/success写入仍按已记录 application路径执行。

2026-09-15 map mission generation/operation-id boundary：公共 `_map_operation_id` 改用注入 UUID，地图委托生成与奖励随机 helper 改用 `runtime_random`，委托真实接取/领取入口传入 `runtime_clock/runtime_random`；避免 fallback 到系统时间/全局random。140 tests、compileall、architecture通过，旧非默认兼容调用仍待后续清理。

2026-09-15 map mission generation live safety：提交 `0d72056` 部署后 backup `/srv/old/data/backups/20260914T180408Z`，dry-run/apply无pending，readiness全绿，83-entry recovery reconcile clean。live未执行玩家委托生成/领取。

2026-09-15 map initialization/trial random boundary：地图初始位置、全局试炼节点和按界试炼节点选择均改用显式 `random_source`，默认绑定 composition root 的 `runtime_random`；公共 operation ID继续使用注入 UUID。新增 deterministic initialization test，map/source共140 tests、compileall、architecture通过。现有非默认兼容调用仍待后续统一清理。

2026-09-15 map initialization/trial random live safety：提交 `e6c5b8f` 部署后 backup `/srv/old/data/backups/20260914T181145Z`，dry-run/apply无pending，readiness全绿，83-entry recovery reconcile clean。live未执行玩家初始化或试炼节点写入。

2026-09-15 map clock boundary：地图 interactive ready/expiry、combat cooldown、explore elapsed及冷却helper的默认执行路径统一使用注入 `runtime_clock`，`xiuxian_map/__init__.py` 不再直接调用 `datetime.now()`；map lifecycle/source共159 tests、compileall、architecture通过，无schema变更。

2026-09-15 map Dao battle random boundary：真实 `道战` handler 的随机目标和胜负roll改用注入 `runtime_random`，资产/战绩写入继续由 `CombatSettlementApplication.settle_dao_battle` 负责；143 tests、compileall、architecture通过，无schema变更。

2026-09-15 map direct-random/legacy-grant cleanup：附近道友抽样改用 `runtime_random.sample`，三个已无调用的 `_grant_*` 直接资产写/global-random helper删除，所有 `_roll_*` fallback绑定composition root `runtime_random`；map模块不再出现非 `runtime_random` 的直接random调用。164 tests、compileall、architecture通过。

2026-09-15 map JSON adapter boundary：新增 infrastructure `JsonDocumentReader`，`xiuxian_map._load_map_data` 不再直接 `open/json.load`，缺失/非法/非object均有failure tests；159 tests、compileall、architecture通过。地图静态文档仍由同一 `MAP_FILE` 提供，rollback可恢复旧loader。

2026-09-15 dungeon purchase cutover：新增 `DungeonPurchaseSqlRepository`，默认 `DungeonApplication.purchase/operation_result` 与真实 `副本兑换` handler 不再使用 compatibility purchase service；game UoW 原子校验钱包/背包绑定、扣款入包并记录成功和拒绝结果，UUID显式注入。新增game migration `dungeon.002`，new+legacy+source共154 tests、84 catalog、compileall、architecture通过。dungeon explore/team/session仍为后续独立slice。

2026-09-15 dungeon purchase live safety：提交 `81b9cab` 部署后 backup `/srv/old/data/backups/20260914T184103Z`，dry-run/apply仅game `[dungeon.002]`；readiness全绿，84-entry recovery reconcile clean。live未执行副本兑换写入。

2026-09-15 dungeon session exit cutover：新增 `DungeonSessionSqlRepository` 与 `DungeonApplication.session_operation/session_transition`，真实 `退出副本` handler不再调用legacy session service，replay先于可变状态读取，UUID显式注入；player migration `dungeon.003`创建session operation ledger。new+legacy+source共162 tests、85 catalog、compileall、architecture通过。session enter与探索结算仍为后续slice。

2026-09-15 dungeon session exit live safety：提交 `657ea4a` 部署后 backup `/srv/old/data/backups/20260914T184927Z`，dry-run/apply仅player `[dungeon.003]`；readiness全绿，85-entry recovery reconcile clean。live未执行副本退出写入。

2026-09-15 dungeon explore replay cutover：新增 `dungeon.004` game operation schema，`DungeonSessionSqlRepository.replay` 与真实 `探索副本` handler在任何可变副本/队伍/战斗读取前通过 `DungeonApplication.replay`恢复missing/prepared/completed/conflict；UUID显式注入。new+legacy+source共160 tests、86 catalog、compileall、architecture通过。prepare/settle/rejection仍为连续后续slice。

2026-09-15 dungeon explore prepare/rejection cutover：`DungeonSessionSqlRepository.prepare/resolve_rejection`接管真实探索handler的resolved plan和固定拒绝响应写入，支持prepared/completed replay和operation conflict；复用 `dungeon.004`，161 tests、compileall、architecture通过。prepared资产settle仍为下一slice，旧service仅保留该边界回滚。

2026-09-15 arena challenge purchase cutover：新增 `ArenaChallengePurchaseSqlRepository`，真实 `竞技场购买次数` handler继续使用 `ArenaApplication.purchase_challenges` 但默认 repository已改为SQL跨库事务；game wallet、player arena日购买/额外次数与 `arena_challenge_purchase_operations` 在同一UoW中校验和写入，支持重放/冲突、跨日标准化、限额/余额拒绝和触发器回滚。新增game migration `arena.002`；151 tests、87 catalog、compileall、architecture通过。竞技场普通商店购买、战斗结算和票券消费仍为后续slice。

2026-09-15 arena challenge purchase live safety：提交 `2d8883b` 部署后 backup `/srv/old/data/backups/20260914T191903Z`，dry-run/apply仅game `[arena.002]`；readiness全绿，87-entry recovery reconcile clean。live未执行竞技场购买写入。

2026-09-15 arena shop purchase cutover：`ArenaChallengePurchaseSqlRepository`扩展为普通商店 `purchase`，真实 `竞技场兑换` handler默认通过 `ArenaApplication.purchase`执行；跨库校验honor/weekly快照、限购、库存容量，原子更新player arena、game背包和operation ledger，UUID fallback注入。新增game migration `arena.003`；156 tests、88 catalog、compileall、architecture通过。竞技场结算、票券消费和默认普通商店之外的旧服务仍为后续slice。

2026-09-15 arena shop purchase live safety：提交 `9286e04` 部署后 backup `/srv/old/data/backups/20260914T192619Z`，dry-run/apply仅game `[arena.003]`；readiness全绿，88-entry recovery reconcile clean。live未执行竞技场商店写入。

2026-09-15 arena challenge ticket cutover：新增 `ArenaChallengePurchaseSqlRepository.use_challenge_ticket` 与 `ArenaApplication.use_challenge_ticket`，真实挑战券使用路径不再调用 legacy ticket service；跨库原子校验/扣减背包券、更新竞技场已用次数并记录 operation，支持 duplicate/conflict、缺券和事务回滚。新增game migration `arena.004`；150 tests、89 catalog、compileall、architecture通过。竞技场结算仍为后续slice。

2026-09-15 arena challenge ticket live safety：提交 `9edf82c` 部署后 backup `/srv/old/data/backups/20260914T193611Z`，dry-run/apply仅game `[arena.004]`；readiness全绿，89-entry recovery reconcile clean。追加直接SQL repository成功/duplicate/rollback测试后，arena ticket slice共149 tests、compileall、architecture、diff check通过；live未执行挑战券写入。

2026-09-15 arena settlement replay cutover：新增 `ArenaChallengePurchaseSqlRepository.settlement_result` 与 `ArenaApplication.settlement_result`，真实竞技场挑战 handler在匹配/战斗前通过feature query读取已有结算，UUID fallback注入；新增game migration `arena.005` operation schema。new+legacy+source共150 tests、90 catalog、compileall、architecture通过；完整结算写入仍为下一slice。

2026-09-15 arena settlement replay live safety：提交 `c488984` 部署后 backup `/srv/old/data/backups/20260914T194658Z`，dry-run/apply仅game `[arena.005]`；readiness全绿，90-entry recovery reconcile clean。live未执行竞技场结算写入。

2026-09-15 arena settlement cutover：`ArenaChallengePurchaseSqlRepository.settle`接管默认 `ArenaApplication.settle`，真实竞技场挑战 handler通过application完成双方arena状态、挑战次数、双方玩家HP/MP/体力与积分/段位更新；所有快照采用CAS校验，operation replay/conflict、limit/stamina rejection及operation/vitals failure rollback均有测试。复用 `arena.005` settlement ledger；165 tests、90 catalog、compileall、architecture、diff check通过。live未执行真实竞技场战斗写入，旧战斗service仅保留显式rollback路径。

2026-09-15 arena settlement implementation closure：新SQL repository已补齐对手arena/player快照校验、双方更新rowcount检查及双方玩家体力/HP/MP CAS，防止并发对手状态覆盖；真实 handler默认路径保持 `ArenaApplication.settle`，旧 settlement service只作为显式rollback adapter。完整arena settlement回归165 tests，catalog=91，compileall、architecture、diff check通过；live deployment follows this commit，未执行真实竞技场战斗写入。

2026-09-15 tower shop purchase cutover：新增 `TowerPurchaseSqlRepository` 与 `TowerApplication`默认SQL repository，真实 `通天塔兑换` handler的积分/weekly限购、背包容量、库存写入和operation ledger均由跨库UoW完成，UUID fallback注入；新增game migration `tower.002`。new+legacy+source共152 tests、91 catalog、compileall、architecture、diff check通过。tower settlement仍为后续独立slice。

2026-09-15 tower shop purchase live safety：提交 `1dc30a0` 部署后 backup `/srv/old/data/backups/20260914T200624Z`，dry-run/apply仅game `[tower.002]`；readiness全绿，91-entry recovery reconcile clean。live未执行通天塔兑换写入。

2026-09-15 tower settlement cutover：新增 `TowerPurchaseSqlRepository.settle/settlement_result` 和game migration `tower.003`；真实 `tower_battle` 单层/连续挑战默认composition改为SQL repository，replay与settle均经 `TowerApplication`，UUID fallback注入。跨库UoW原子处理tower层数/积分、玩家stone/exp/HP/MP/体力、奖励背包与operation replay/conflict，成功/失败挑战、库存拒绝、状态冲突和operation trigger rollback有测试；156 tests、92 catalog、compileall、architecture、diff check通过。旧settlement service只保留显式rollback引用。

2026-09-15 tower settlement live safety：提交 `2015d93` 部署后 backup `/srv/old/data/backups/20260914T202158Z`，dry-run/apply仅game `[tower.003]`；readiness全绿，92-entry recovery reconcile clean。live未执行通天塔战斗或奖励写入。

2026-09-16 tower repository clock boundary：`TowerApplication` 将 Clock 传入 SQL repository，通天塔结算日期及商店 weekly key 默认值不再使用 `date.today()`；tower/source共165 tests、catalog=103、compileall、architecture、diff check通过。跨库 UoW/CAS保持不变，legacy bridge仍明确保留。

2026-09-16 tower repository clock live safety：提交 `8e937a1` 部署后 backup `/srv/old/data/backups/20260915T182242Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行通天塔购买/结算写入。

2026-09-15 boss shop purchase cutover：新增 `BossPurchaseSqlRepository`、`BossApplication`默认SQL repository与game migration `boss.002`；真实 `世界BOSS兑换` handler不再调用compatibility purchase service，UUID fallback注入。跨库UoW原子处理boss_limit积分、boss weekly、game背包和operation replay/conflict，new+legacy+source共153 tests、93 catalog、compileall、architecture、diff check通过。world boss battle settlement仍为后续slice。

2026-09-16 boss repository clock boundary：`BossApplication` 将 Clock 传入 `BossPurchaseSqlRepository`，boss weekly purchase 默认日期不再直接使用 `date.today()`；world-boss/boss/source共188 tests、catalog=103、compileall、architecture、diff check通过。purchase 跨库 UoW保持，world-boss settlement bridge仍为阻塞边界。

2026-09-16 boss repository clock live safety：提交 `8be46b0` 部署后 backup `/srv/old/data/backups/20260915T183033Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行世界BOSS兑换写入。

2026-09-16 composition root SQL cutover：`plugin.py` 移除 arena/tower/boss 默认 `Legacy*Repository` 注入，改由各 Application 默认 SQL repository并传入 context Clock；三模块真实 startup graph不再默认命中这三组 legacy bridge，legacy 类保留为显式 rollback。arena/tower/boss/source共253 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 composition root SQL cutover live safety：提交 `af76fcb` 部署后 backup `/srv/old/data/backups/20260915T191347Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行 arena/tower/boss 资产写入。

2026-09-16 sect composition SQL cutover：`plugin.py` 移除 sect 默认 `LegacySectRepository` 注入，改由 `SectApplication` 默认 `SectRenameSqlRepository` 并传入 context Clock；sect 真实 startup graph不再默认命中旧宗门 service，legacy 类保留为显式 rollback。sect/source共298 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 sect coverage correction：复核 `SectRenameSqlRepository` 方法覆盖，确认其实现 `join/purchase/learn_main/learn_secondary/claim_elixir/donate/change_position/kick/leave/rename` 全部十个 `SectRepository` 操作；此前将其余宗门操作描述为 legacy 的台账文字已更正。`LegacySectRepository` 仅保留为显式 rollback，未改变 runtime graph。

2026-09-16 sect composition SQL cutover live safety：提交 `04e85e2` 部署后 backup `/srv/old/data/backups/20260915T192316Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行宗门资产写入。

2026-09-15 boss shop purchase live safety：提交 `34d91fc` 部署后 backup `/srv/old/data/backups/20260914T203127Z`，dry-run/apply仅game `[boss.002]`；readiness全绿，93-entry recovery reconcile clean。live未执行世界BOSS兑换写入。

2026-09-15 boss purchase verification closure：补齐 `BossPurchaseSqlRepository` 独立双库测试及真实 handler source-quality，确认积分/weekly/back/operation 在同一UoW内完成，duplicate/conflict、限购、积分不足、库存容量和 operation replay 边界均有证据。boss.002 已在上一提交 live 应用；当前工作树随后仅用于下一 world-boss settlement slice。

2026-09-15 world-boss settlement replay cutover：新增 game migration `boss.003` 与 `BossPurchaseSqlRepository.settlement_result`，真实世界BOSS讨伐 handler的 operation replay 查询及无event fallback ID改用 `BossApplication`/UUID。既有三库 settlement 行为与 source 回归144 tests、catalog=94、compileall、architecture、diff check通过；完整 game/player/activity 结算写入仍保留为后续独立slice，未执行真实玩家讨伐写入。

2026-09-15 world-boss settlement replay live safety：提交 `20e3016` 部署后 backup `/srv/old/data/backups/20260914T204212Z`，dry-run/apply仅game `[boss.003]`；readiness全绿，94-entry recovery reconcile clean。live未执行世界BOSS讨伐写入。

2026-09-15 world-boss replay verification closure：修正 source-quality 对旧 segmented reward 注释和错误状态断言的依赖，真实 battle handler仅保留 composite settlement service调用，replay已走 `BossApplication.settlement_result`；world-boss行为5 tests、source-quality143、catalog=94、compileall、architecture、diff check通过。三库 settle 写入仍为下一独立slice。

2026-09-15 world-boss three-database settlement boundary：尝试将完整 game/player/activity settlement移入feature repository时确认旧事务包含 activity schema初始化、damage caps、milestone、statistics、task progress及三库共享rollback；简单包装旧 service不符合真实runtime cutover，因此撤回伪迁移并记录为阻塞边界。replay query保持已闭环，后续需独立重写三库 repository。

2026-09-15 sect rename cutover：新增 `SectRenameSqlRepository`、`SectApplication.rename` 与 game migration `sect.002`，真实宗门改名 handler改经application，原子更新宗门名称/灵石/card/operation，校验 owner、同名、余额、卡牌、duplicate/replay/rollback。new+legacy+source共153 tests、95 catalog、compileall、architecture、diff check通过；其它 sect membership paths仍保留legacy rollback。

2026-09-15 sect rename live safety：提交 `f93a6c3` 部署后 backup `/srv/old/data/backups/20260914T213946Z`，dry-run/apply仅game `[sect.002]`；readiness全绿，95-entry recovery reconcile clean。live未执行宗门改名写入。

2026-09-15 sect member join cutover：新增 `SectRenameSqlRepository.join` 与 `SectApplication.join`默认SQL路径、game migration `sect.003`，真实加入宗门 handler切换 application；原子校验 user membership、sect open/closed、member limit、operation conflict/replay和CAS update。rename/join/new-source共154 tests、96 catalog、compileall、architecture、diff check通过；其它sect membership paths保留legacy rollback。

2026-09-15 sect member join live safety：提交 `18f047e` 部署后 backup `/srv/old/data/backups/20260914T214811Z`，dry-run/apply仅game `[sect.003]`；readiness全绿，96-entry recovery reconcile clean。live未执行加入宗门写入。

2026-09-15 sect member leave cutover：新增 `SectRenameSqlRepository.leave` 与 `SectApplication.leave`默认SQL路径、game migration `sect.004`，真实退出宗门 handler切换 application；原子清除 membership/contribution并记录 removal operation，覆盖 owner guard、not-in-sect、duplicate、state change/rollback。rename/join/leave/new-source共155 tests、97 catalog、compileall、architecture、diff check通过。

2026-09-15 sect member leave live safety：提交 `1607ed9` 部署后 backup `/srv/old/data/backups/20260914T215607Z`，dry-run/apply仅game `[sect.004]`；readiness全绿，97-entry recovery reconcile clean。live未执行退出宗门写入。

2026-09-15 sect member kick cutover：复用 `sect.004` removal ledger，新增 `SectRenameSqlRepository.kick` 与 `SectApplication.kick`，真实宗门踢出 handler切换application；原子校验管理职位、同宗/目标职位、CAS清除membership/contribution、duplicate/conflict/rollback。rename/join/leave/kick/source共156 tests、97 catalog、compileall、architecture、diff check通过。

2026-09-15 sect member kick live safety：提交 `6e51ed9` 部署后 backup `/srv/old/data/backups/20260914T220207Z`，dry-run无pending、reconcile clean、readiness全绿，97-entry recovery clean。live未执行宗门踢出写入。

2026-09-15 sect position change cutover：新增 `SectRenameSqlRepository.change_position`、`SectApplication.change_position` 与 game migration `sect.005`，真实职位变更 handler切换application；原子校验 manager rank、同宗、职位层级/容量、CAS update与operation replay。rename/join/leave/kick/position/source共157 tests、98 catalog、compileall、architecture、diff check通过。

2026-09-15 sect position change live safety：提交 `795418b` 部署后 backup `/srv/old/data/backups/20260914T221008Z`，dry-run/apply仅game `[sect.005]`；readiness全绿，98-entry recovery reconcile clean。live未执行职位变更写入。

2026-09-15 sect donation cutover：新增 `SectRenameSqlRepository.donate`、`SectApplication.donate` 与 game migration `sect.006`，真实宗门捐献 handler切换application；同一UoW原子扣玩家灵石/增贡献、增宗门stone/scale/materials并写operation，覆盖 amount/materials、membership、余额、duplicate/replay/rollback。rename/join/leave/kick/position/donation/source共158 tests、99 catalog、compileall、architecture、diff check通过。

2026-09-15 sect donation live safety：提交 `8750680` 部署后 backup `/srv/old/data/backups/20260914T221709Z`，dry-run/apply仅game `[sect.006]`；readiness全绿，99-entry recovery reconcile clean。live未执行宗门捐献写入。

2026-09-15 sect shop purchase cutover：新增 `SectRenameSqlRepository.purchase`、复用/默认 `SectApplication.purchase`与game migration `sect.007`，真实宗门兑换 handler切换application；同一UoW处理贡献/资材、weekly purchase、inventory与operation replay/conflict，覆盖 membership/closed/limit/contribution/materials/inventory rejection。全部sect rename/join/leave/kick/position/donation/shop/source共159 tests、100 catalog、compileall、architecture、diff check通过。

2026-09-15 sect shop purchase live safety：提交 `a20be5b` 部署后 backup `/srv/old/data/backups/20260914T222601Z`，dry-run/apply仅game `[sect.007]`；readiness全绿，100-entry recovery reconcile clean。live未执行宗门兑换写入。

2026-09-15 sect main-buff learning cutover：新增 `SectRenameSqlRepository.learn_main`、复用 `SectApplication.learn_main`与game migration `sect.008`，真实主功法学习handler切换application；原子CAS扣宗门资材、更新BuffInfo、记录operation，覆盖 membership/position/catalog/materials/buff/replay/state change。全部sect suite/source共160 tests、101 catalog、compileall、architecture、diff check通过。

2026-09-15 sect main-buff learning live safety：提交 `d66f3c0` 部署后 backup `/srv/old/data/backups/20260914T223700Z`，dry-run/apply仅game `[sect.008]`；readiness全绿，101-entry recovery reconcile clean。live未执行功法学习写入。

2026-09-15 sect secondary-buff learning cutover：新增 `SectRenameSqlRepository.learn_secondary`、`SectApplication.learn_secondary`与game migration `sect.009`，真实神通学习handler切换application；CAS扣资材/更新BuffInfo.sec_buff/operation replay。全部sect suite/source共161 tests、102 catalog、compileall、architecture、diff check通过。

2026-09-15 sect secondary-buff learning live safety：提交 `5f40f63` 部署后 backup `/srv/old/data/backups/20260914T224514Z`，dry-run/apply仅game `[sect.009]`；readiness全绿，102-entry recovery reconcile clean。live未执行神通学习写入。

2026-09-15 sect elixir claim cutover：新增 `SectRenameSqlRepository.claim_elixir`、`SectApplication.claim_elixir`与game migration `sect.010`，真实宗门丹房领取handler切换application；原子校验membership/position/room/contribution/materials/claim flag/inventory并写operation replay。全部sect suite/source共162 tests、103 catalog、compileall、architecture、diff check通过。

2026-09-15 sect elixir claim live safety：提交 `dba0ba7` 部署后 backup `/srv/old/data/backups/20260914T225419Z`，dry-run/apply仅game `[sect.010]`；readiness全绿，103-entry recovery reconcile clean。live未执行丹药领取写入。

2026-09-15 sign-in application cutover：真实修仙签到handler改用现有 `SignInApplication.lookup/claim`、Clock/Random/OperationLedger和`SignInRepository`，不再默认调用 `SignInService.sign`；lottery副作用与task progress仍记录为独立legacy边界。sign-in vertical/legacy-switch/effects/source共149 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 lottery boundary verification：签到后 `settle_lottery` 在正式 lottery schema 存在时执行 `LotteryApplication.settle`，仅未迁移/旧数据路径保留显式 legacy fallback；lottery application/domain/snapshot/task repository与sign-in source共151 tests，catalog=103，compileall、architecture、diff check通过。task progress与legacy lottery fallback继续作为独立后续slice。

2026-09-15 sign-in effects wiring verification：`SignInApplicationEffects`已在legacy runtime context组合 lottery application、statistics repository与`SignInTaskRepository`，并对 replay跳过重复统计/task；handler保留显式兼容边界用于未迁移运行环境。签到/lottery/task wiring与source共151 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 sign-in effects handler cutover：lifecycle-owned `SignInApplication`通过 `configure_sign_in_application` 回注真实 NoneBot handler；签到handler不再重复调用 lottery/statistics/task legacy副作用，仅消费 application outcome message，replay由 application effects幂等处理。sign-in/source/effects共148 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 sign-in lifecycle wiring fix：回注 handler 仅在 NoneBot driver 已初始化时执行，避免 CLI `serve`/maintenance context 导入 handler 触发 `ValueError: NoneBot has not been initialized` 并卡在 repositories readiness。修复后 sign-in wiring/source共148 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 sign-in lifecycle wiring live safety：`6f1b4b3` 部署后 backup `/srv/old/data/backups/20260914T235737Z`，dry-run pending为空、reconcile clean；真实 `startup` 返回 `phase=ready`，filesystem/database/migrations/repositories/jobs/web 六项全绿，103-entry recovery clean。首次验证脚本在 shutdown 后断言导致假失败，已按正确顺序重跑并确认通过。

2026-09-16 sign-in lottery/statistics boundary：确认 lottery migration `lottery.001/002` 与 statistics migration 已纳入统一 catalog；`SignInStatisticsRepository.record/value` 移除 request-time `ensure_schema`，缺表显式失败。同步更新过时 lottery source guard为 `sign_in_application.claim`/`settle_lottery`真实路径；sign-in/lottery/source共179 tests、catalog=103、compileall、architecture、diff check通过。旧安装 lottery fallback 与 task/lottery side-effect compatibility边界仍待正式迁移。

2026-09-16 sign-in lottery/statistics live safety：提交 `0fc0bdc` 部署后 backup `/srv/old/data/backups/20260915T184839Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行签到、lottery、statistics或任务奖励写入。

2026-09-16 sign-in task projection DDL boundary：`SignInTaskRepository.record` 不再 request-time 建表，`apply_sign_in_tasks` 作为唯一 schema 前置，新增缺表拒绝测试；sign-in/task/lottery/source共180 tests、catalog=103、compileall、architecture、diff check通过。lottery legacy fallback与 task side-effect compatibility边界仍待正式收口。

2026-09-16 sign-in task projection DDL live safety：提交 `4880137` 部署后 backup `/srv/old/data/backups/20260915T185548Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行签到任务进度或奖励写入。

2026-09-16 sign-in no-request-DDL boundary：`SignInRepository.operation` 移除 request-time `ensure_schema`，statistics/task projection均要求 startup migration；更新 effects/statistics/task测试 fixtures为显式 schema migration并保留缺表拒绝验证。sign-in/lottery/source共180 tests、catalog=103、compileall、architecture、diff check通过。lottery legacy side-effect fallback仍待正式替换。

2026-09-16 sign-in no-request-DDL live safety：提交 `7d9bc2a` 部署后 backup `/srv/old/data/backups/20260915T190538Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行签到、lottery、statistics或任务进度写入。

2026-09-16 sign-in lottery helper blocked：`features/sign_in/commands.py::settle_lottery` 仍按 `LotteryRepository.schema_exists` 在 legacy base handler内选择 `LotteryApplication`或旧 settle adapter；默认 startup已创建 lottery schema，但 base handler尚未接收 lifecycle-owned LotteryApplication实例。直接删除分支会丢失显式旧安装 rollback，保留为后续 wiring slice。

2026-09-16 sign-in lottery lifecycle wiring：新增 `configure_lottery_application`，真实 base `handle_lottery` 在 lifecycle注入 `LotteryApplication`时直接使用 feature-owned settlement；未注入时保留 schema probe与显式 legacy rollback。修复 isolated runtime 缺少 player.db 时 attached migration无法启动的问题。sign-in/lottery/source共180 tests、catalog=104、compileall、architecture、diff check通过。

2026-09-16 sign-in lottery explicit fallback：startup `apply_lottery`已创建正式 lottery schema，移除 plugin 默认的 `LotteryRepository.schema_exists` request-time选择；默认 migrated runtime直接使用 `LotteryApplication`，仅显式 `XIUXIAN_SIGN_IN_LEGACY_LOTTERY=true`启用旧 settlement rollback。sign-in/lottery/source共180 tests、catalog=104、compileall、architecture、diff check通过。

2026-09-16 sign-in handler wiring correction：`xiuxian_base.handle_lottery`已将模块级 lifecycle-injected `lottery_application`传入 `settle_lottery`；真实 lifecycle默认因此不进入 schema probe/legacy branch。`lottery_application is None`仅覆盖模块独立导入、测试或显式旧安装兼容场景，保留为 rollback，不计作默认 runtime fallback。

2026-09-16 sign-in lottery explicit fallback live safety：提交 `14d772e` 部署后 backup `/srv/old/data/backups/20260916T013317Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行签到、lottery或资产写入。

2026-09-16 sign-in effects audit correction：确认默认 lifecycle wiring已使用 `SignInStatisticsRepository` 与 `ApplicationSignInTaskEffects`，task/statistics projection不再默认走旧 side-effect service；剩余 sign-in compatibility blocker缩小为旧安装/显式 lottery fallback与base handler兼容分支。

2026-09-16 sign-in lottery lifecycle live safety：提交 `5154b34` 部署后 backup `/srv/old/data/backups/20260916T002828Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行签到、lottery或资产写入。

2026-09-16 feature provider audit：当前 `features/` 层 direct Clock/Random 命中仅为已注入的 `PackageRewardResolver` 与 `SignInApplication`，未发现新的未注入系统来源；`LotteryApplication` 已显式使用 Clock/Random。剩余可执行风险集中在 legacy handler/service graph，未对旧实现做表面替换。

2026-09-16 tianti legacy time backlog：`xiuxian_tianti/transaction_service.py` 仍有若干 helper以 `datetime.now()` 作为兼容默认值；真实默认 training/settlement application已使用 feature-owned SQL repositories和注入 Clock，本轮不改 legacy helper以避免改变旧 Web/NoneBot兼容语义，保留为 legacy-only provider backlog。

2026-09-16 legacy time audit：全项目剩余 direct `datetime.now/date.today` 命中主要集中在 `xiuxian_map/transaction_service.py`、`xiuxian_arena/transaction_service.py`、`xiuxian_compensation/transaction_service.py`、`xiuxian_buff` relation/mentor service及 tianti legacy helpers；这些均仍处于旧 transaction/service graph，默认 application SQL paths已完成 Clock收口。未对旧兼容实现做表面替换，保留为后续真实 service/application migration blocker。

2026-09-16 facade audit：`features/base`、`features/status`及多个 `MigratedFeatureApplication` 目前仍是 `LegacyApplication/ServicePort` facade，未提供可验证的 feature-owned SQL repository；不将 facade当作完整迁移，保留 base/status legacy graph blocker并继续扫描独立真实边界。

2026-09-16 legacy graph audit：`xiuxian_buff/partner.py` 仍暴露双修/师徒等大量真实 legacy handlers，并直接依赖 `xiuxian2_handle`与 relation transaction services；无可验证 feature-owned repository/application，保留为 mentor/partner 多库事务 blocker。world-boss仍仅有 boss purchase/replay/Clock/random slices，完整 activity/game/player settlement保持未迁移。

2026-09-16 facade coverage audit：`features/entertainment`、`features/tasks`、`features/dufang`、`features/training`等 repository仍是 `ServicePort`/旧 service handler映射，不包含 feature-owned SQL schema或跨库事务；其 application只提供 operation ledger协调，不能计为完整迁移。保留为 legacy graph blocker，不做 facade-only cutover。

2026-09-16 map default graph audit：`MapApplication` 默认真实路径已覆盖 interactive start/failure/settlement query、combat lifecycle、explore start/settle、resource reward、mission claim、seed purchase、dongfu build等 SQL repositories；`interactive_finish`与`combat_settle`仍显式落回 LegacyMapRepository，分别保留为 interactive settlement/combat settlement后续 blocker。未将 MapApplication 的 LegacyApplication 基类误报为所有 map 操作未迁移。

2026-09-16 map interactive acceptance correction：source-quality调用图确认真实 interactive handler已使用 `map_application.interactive_replay/interactive_settlement/resource_reward`，奖励决策使用注入 `runtime_random`/`random_source`；`MapInteractiveActionService`仅作为兼容 facade与历史测试对象，未处于默认 handler graph。interactive finish/settlement主路径不再作为未切换 blocker。

2026-09-16 map legacy graph cleanup：调用图确认 `xiuxian_map/__init__.py` 中旧 `MapInteractiveActionService`、`MapResourceRewardService`、`MapExploreSettlementService`、`MapMissionClaimService`、`MapCombatSettlementService` 实例均无生产调用者；移除这些默认实例与imports，保留旧类供显式 rollback/历史测试。map/source共169 tests、compileall、architecture、diff check通过。

2026-09-16 map legacy graph cleanup continuation：调用图确认 `MapExploreStartService`、`MapHomeReturnService`、`MapMovementSettlementService`、`MapCombatLifecycleService`、`MapDongfuBuildService`、`SeedPurchaseService`、`MapDaoBattleSettlementService` 实例同样无生产调用者；移除默认实例与imports，保留 `MapApplication`/`CombatSettlementApplication` 真实路径及旧类显式 rollback。map/source共195 tests、compileall、architecture、diff check通过。

2026-09-16 map AST import sweep：对 `xiuxian_map/__init__.py` 执行未使用 import核查，仅发现并移除 `Path` dead import；结果类型和 runtime providers均有真实 handler引用。map/source共195 tests、compileall、architecture、diff check通过。

2026-09-16 map provider import cleanup：AST确认 `xiuxian_map/__init__.py` 顶层 `random`/`time` 均无引用；移除 dead imports，真实随机路径继续使用 `runtime_random`。map/source共195 tests、compileall、architecture、diff check通过；全局直接调用指标不变，剩余命中位于 legacy transaction graph。

2026-09-16 map runtime provider audit：复核真实 map handler的时间路径均经 `runtime_clock`（today/cooldown/expiry），随机路径均经 `runtime_random`（nearby/interactive reward/dao battle）；未发现新的直接系统 Clock/Random调用。剩余 transaction_service direct time命中仅在未调用 legacy implementations。

2026-09-16 map provider import cleanup live safety：提交 `1ba31d8` 部署后 backup `/srv/old/data/backups/20260916T014913Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行地图操作或资产写入。

2026-09-16 map AST import sweep live safety：提交 `cc7c260` 部署后 backup `/srv/old/data/backups/20260916T014326Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行地图操作或资产写入。

2026-09-16 combat settlement import cleanup：`CombatSettlementApplication` 默认使用 `CombatSettlementSqlRepository`/`DaoBattleSqlRepository`，移除未使用 `LegacyCombatSettlementRepository` import；legacy adapter保留在 repository作为显式 rollback。combat/map/source共149 tests、compileall、architecture、diff check通过。

2026-09-16 tianti training application cleanup：调用图确认 `TiantiTrainingApplication._repository` 无真实调用者，所有 training handler分别使用 StoneTraining/Breakthrough/Qiaoxue/MedicineBath/ItemReward SQL repositories；移除未使用 `LegacyTiantiTrainingRepository` import与fallback。tianti/source共179 tests、compileall、architecture、diff check通过。

2026-09-16 tianti settlement import cleanup：`TiantiSettlementApplication` 默认使用 `TiantiSettlementSqlRepository`，移除未使用 `LegacyTiantiSettlementRepository` import；legacy adapter继续保留在 repository作为显式 rollback。tianti/source共179 tests、compileall、architecture、diff check通过。

2026-09-16 tianti settlement import cleanup live safety：提交 `f9a4685` 部署后 backup `/srv/old/data/backups/20260916T010709Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行天梯结算、奖励或资产写入。

2026-09-16 arena wiring cleanup：`ArenaApplication` 默认使用 `ArenaChallengePurchaseSqlRepository`，移除未使用 `LegacyArenaRepository` import；legacy adapter继续保留在 repository作为显式 rollback。tower/boss同批复核无 legacy application import。arena/tower/boss/source共224 tests、compileall、architecture、diff check通过。

2026-09-16 sect wiring cleanup：`SectApplication` 默认使用 `SectRenameSqlRepository`，移除未使用 `LegacySectRepository` import；legacy adapter继续保留在 repository作为显式 rollback。sect/source共298 tests、compileall、architecture、diff check通过。SectFairyland仍是独立 legacy blocker。

2026-09-16 sect wiring cleanup live safety：提交 `c5458b1` 部署后 backup `/srv/old/data/backups/20260916T011712Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行宗门改名、资产或成员操作。

2026-09-16 arena wiring cleanup live safety：提交 `9f616e4` 部署后 backup `/srv/old/data/backups/20260916T011247Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行竞技场挑战、战斗或资产写入。

2026-09-16 tianti training application cleanup live safety：提交 `3eabc11` 部署后 backup `/srv/old/data/backups/20260916T010232Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行天梯修炼、药浴、窍穴、突破或物品写入。

2026-09-16 combat settlement import cleanup live safety：提交 `4e2c545` 部署后 backup `/srv/old/data/backups/20260916T005704Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行战斗结算、奖励或资产写入。

2026-09-16 map legacy graph cleanup continuation live safety：提交 `4b65c4a` 部署后 backup `/srv/old/data/backups/20260916T005240Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行地图移动、探索、种植、战斗或资产写入。

2026-09-16 map legacy graph cleanup live safety：提交 `ceee816` 部署后 backup `/srv/old/data/backups/20260916T004816Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行地图探索、奖励、战斗或资产写入。

2026-09-16 map interactive/combat correction：复核 `MapApplication.interactive_finish` 无真实调用者，实际 interactive settlement handler走 `interactive_settlement` 的 SQL repository；`MapApplication.combat_settle` 仍无 feature-owned SQL repository，repository中仅有 combat lifecycle start/query/plan classes，保留为 map combat settlement blocker。

2026-09-16 map combat wiring correction：复核真实 `xiuxian_map` 模块已持有 `CombatSettlementApplication`/`DaoBattleApplication`，Web combat route也从 `context.services["combat_settlement"]`获取 feature application；`MapApplication.combat_settle`仅为未调用的 compatibility method。保留旧 MapCombatSettlementService为 rollback，不将 dead method误报为默认 runtime blocker。

2026-09-16 final feature-schema sweep：生产 `features/` application/repository层已无新的 request-time `repository.ensure_schema`调用；剩余命中仅为 startup migration定义、infrastructure ledger、accessory player schema-policy guard或测试夹具。未将测试直连数据库命中误报为生产路径，保留 legacy transaction/facade blocker继续推进。

2026-09-16 blocker re-audit：`activity_reward`仅有 `LegacyActivityRewardRepository`，`admin_asset`仅有 legacy stone/item transaction adapters，`sect_fairyland`仅有 `LegacySectFairylandRepository`；三者均无 feature-owned SQL schema/repository，不能移除 plugin 默认 legacy 注入或以 application facade宣称切换。继续寻找不依赖这些资产事务的独立 slice。

2026-09-16 tower/boss wiring audit：`TowerApplication` 默认使用 `TowerPurchaseSqlRepository`，`BossApplication` 默认使用 `BossPurchaseSqlRepository`，application与plugin均无未使用 tower/boss legacy repository import；保留 repository层兼容边界，不重复做空切片。SectFairyland/WorldEvents仍为独立 legacy blockers。

2026-09-16 trade/back facade audit：`TradeApplication`、`BackApplication`、`BaseApplication`、`RiftApplication` 默认均通过 `LegacyApplication`/旧 transaction service mapping执行；`trade.001`仅为 marker，无 trade queue/history SQL schema，back也无 feature-owned asset repository。不能把这些 operation-ledger facade当作底层迁移，保留为 trade/back/base/rift legacy blockers。

2026-09-16 legacy import sweep：确认 `AuctionBidApplication`、`BankApplication`、`AdminAssetApplication`、`SectFairylandApplication`、`WorldEventApplication`、`PetApplication`、`MixelixirApplication`等仍在默认方法内真实调用对应 LegacyRepository；`MapApplication`的 LegacyMapRepository仅作为兼容基类，已切换方法走独立 SQL repositories但未切换操作仍需保留。未做广泛删除，避免破坏真实 rollback graph。

2026-09-16 web facade audit：Web registry确实注册了 base/back/trade/rift blueprints，但其 application底层仍分别映射 `LegacyBaseRepository`、`LegacyBackRepository`、`LegacyTradeFeatureRepository`、`LegacyRiftRepository`；route存在不等于底层切换，保留为 Web legacy execution blocker，继续检查 scheduler/worker调用图。

2026-09-16 scheduler/worker audit：bootstrap jobs与scheduler Web route主要管理 `JobRegistry`/`JobExecutor`，未发现可独立切换的正式玩法 job repository；交易 jobs仍为空兼容声明，auction job仍依赖 trade legacy settlement。`xiuxian_buff/partner.py` matcher仍直接调用 mentor/partner transaction services，保留为真实关系事务 blocker，不做 scheduler facade式改写。

2026-09-16 application legacy AST sweep：对全部 `features/*/application.py` 的 `Legacy*` imports执行 AST 引用核查；所有命中均在默认 repository/fallback或 `LegacyApplication` 基类中真实使用，没有可安全删除的死 import。保留 activity/admin/auction/bank/mixelixir/pet/puppet/sect-fairyland/work/world-events及 base/back/buff/map/natal/rift/trade 的真实兼容边界。

2026-09-16 operation ledger infrastructure blocker：`OperationLedger.get/finish/list_pending`仍在请求路径调用 `ensure_schema`；startup当前只在 `game_db`创建 ledger/outbox，而 legacy facade及部分 application会在 player/trade数据库使用 ledger。移除这些调用前必须新增 checksum-safe多数据库 ledger migration并明确各数据库路由，避免把 request-time DDL或跨库 ledger写入误迁移；本轮保留为 infrastructure blocker。

2026-09-16 platform ledger migration：新增 checksum-safe `platform.001` migration，将 operation_ledger、operation_audit、domain_outbox作为 startup-owned shared schema；首次测试发现 player/other DB routing排除 platform.001，修正后 game/player/trade/impart/message 五库均执行。platform-ledger/sign-in/architecture/source共160 tests、catalog=105、compileall、diff check通过。OperationLedger lazy ensure_schema暂保留为兼容，下一独立slice再收口调用。

2026-09-16 platform ledger no-request-DDL：移除 `OperationLedger.get/finish/record_failure/list_pending` 的 request-time `ensure_schema`；application fixtures显式执行 `apply_platform_schema`并保留空库读不建表契约。application/architecture/source共204 tests，focused ledger/app共181 tests，catalog=105、compileall、architecture、diff check通过。

2026-09-16 platform outbox no-request-DDL：`platform.001`已覆盖五库 domain_outbox，移除 `OutboxStore.append/pending` request-time `ensure_schema`；新增空库 pending不建表契约。application/architecture/source共205 tests，catalog=105、compileall、architecture、diff check通过。

2026-09-16 accessory/coordinator no-request-DDL：移除 AccessoryPackagePlayerRepository.apply、accessory reconcile failure与 CrossDatabaseCoordinator failure path的 request-time schema调用；新增 checksum-safe attached `accessory_package.player_data.002`创建 player-side replay table，保留 v1 checksum。附件v2 idempotency、失败补偿和重放均覆盖。accessory/platform/application/architecture/source共227 tests，catalog=105、compileall、architecture、diff check通过。

2026-09-16 platform schema ownership cleanup：移除 startup `ensure_database`与CLI migrate中对 game_db ledger/outbox的重复初始化，统一由 `platform.001` migration及五库路由负责；CLI player/other dry-run/apply路由与startup一致。platform/accessory/application/architecture/source共206 tests，catalog=105、compileall、architecture、diff check通过。

2026-09-16 final feature request-DDL sweep：生产 `features/` application/repository层剩余 `ensure_schema`命中均位于 migration函数；shared OperationLedger/OutboxStore、accessory player apply、coordinator failure paths均已迁移为startup-owned schema。`MigrationRunner`自身的 schema_migrations metadata初始化仅保留在startup/maintenance runner，dry-run preview继续只读。

2026-09-16 partner/mentor migration blocker：`xiuxian_buff/partner.py`真实 matcher仍直接调用 `PartnerProtection/Invite/Cultivation` 与 Mentor transaction services；其 `PartnerProtectionService.read_status`会在请求路径调用 legacy `ensure_schema`并修改 player status表，relation history/mentor字段也由旧 service管理。缺少 feature-owned migration/application/UoW边界，未做局部表面替换，保留为关系多库事务 blocker。

2026-09-16 tianti compatibility audit：`xiuxian_tianti/__init__.py` 中 stone training/medicine bath/breakthrough/qiaoxue/settlement旧 service实例仍为兼容导出，但真实 command handlers均调用 `TiantiTrainingApplication`/`TiantiSettlementApplication`；剩余 `transaction_service.py` direct datetime仅在未调用 legacy helper。CombatSettlement legacy adapter同样仅保留显式 rollback，默认 application使用SQL repository。

2026-09-16 tianti source-quality correction：更新 stale source gate，真实 handler断言 `TiantiTrainingApplication.train/apply_bath/breakthrough/open_qiaoxue` 与 `TiantiSettlementApplication.settle`，同时只拒绝旧 service的真实调用形态，允许兼容说明注释。tianti/source/architecture共193 tests、compileall、diff check通过。

2026-09-16 tianti default graph cleanup：删除 `xiuxian_tianti/__init__.py` 中五个未被生产调用的旧 service实例及imports（stone training、medicine bath、breakthrough、qiaoxue、settlement）；保留旧 service类文件作为显式rollback artifact，真实handlers继续使用 application。tianti/source/architecture共193 tests、compileall、diff check通过。

2026-09-16 tianti default graph cleanup live safety：提交 `25dc01a` 部署后 backup `/srv/old/data/backups/20260916T041555Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行天梯结算、修炼、药浴、突破或资产写入。

2026-09-16 progress audit correction：`check_full_refactor_progress.py` 修正 sign-in分类，默认 `SignInStatisticsRepository`/`ApplicationSignInTaskEffects` 不再标为 task legacy；`LotterySettlementService` 仅在显式 `XIUXIAN_SIGN_IN_LEGACY_LOTTERY` fallback路径中计为兼容残留。audit输出保留 `exit_ready=false`，不放宽完成门禁；source/architecture共157 tests、compileall、diff check通过。

2026-09-16 lottery audit precision：审计输出拆分 `lottery_core_default_legacy=false` 与 `lottery_compatibility_fallback=true`；源码确认 `legacy_lottery` false时构造 `LotteryApplication`，旧 service仅由显式环境变量选择。source/architecture共157 tests，audit仍保留 explicit fallback blocker。

2026-09-16 dungeon settlement blocker re-audit：NoneBot/Web真实入口的 purchase/prepare/replay/resolve_rejection多数通过 `DungeonApplication`；但 `xiuxian_dungeon/__init__.py:1114` prepared replay分支仍直接调用 `dungeon_explore_operation_service.settle()`。`DungeonSessionSqlRepository.settle()`未实现，当前继承 `LegacyDungeonRepository` 的旧 explore settlement；缺少正式SQL settlement算法与跨库资产事务，不能只切入口，保留为 dungeon prepared settlement blocker。

2026-09-16 pet/work migration blocker re-audit：Pet Web/application入口存在，但 `PetApplication`默认 `LegacyPetRepository`，travel/feed/hatch全部委托 `xiuxian_pet.transaction_service`；`pet.001`仅marker。Work claim/settlement application同样默认 LegacyWork repositories，`work.001`仅marker。二者均缺少 feature-owned SQL schema/repository与完整资产事务，不能将 application shell视为cutover。

2026-09-16 pet default graph cleanup：调用图确认 travel claim/feed handlers已实际调用 `PetApplication`；删除 `xiuxian_pet/__init__.py` 中无生产调用的 `PetTravelClaimService`、`PetFeedService` 默认实例及imports，保留 hatch/start/release/fusion/skill 等尚未迁移 service。pet/source/architecture共194 tests、compileall、diff check通过。

2026-09-16 sect fairyland default graph cleanup：调用图确认 fairyland claim handler已实际调用 `SectFairylandApplication.claim`；删除 `xiuxian_sect/__init__.py` 中无生产调用的 `FairylandClaimService` 默认实例/import，并修正 stale source gate。保留 `LegacySectFairylandRepository`作为未迁移rollback边界。sect/source/architecture共312 tests、compileall、diff check通过。

2026-09-16 admin default graph cleanup：AST调用图确认 stone adjustment与item grant handlers已调用 `AdminAssetApplication.adjust_stone/grant_item`，删除 `AdminStoneAdjustmentService`、`AdminItemGrantService` 默认实例及imports；`AdminItemDestroyService`仍有两处真实调用，未删除。admin/source/architecture共231 tests、compileall、architecture、diff check通过。

2026-09-16 boss default graph cleanup：调用图确认 boss shop purchase handler已使用 `BossApplication.purchase`/`BossPurchaseSqlRepository`；删除无生产调用的 `BossPurchaseService` 默认实例/import，保留 `WorldBossBattleSettlementService`（battle settlement仍真实legacy）。boss/source/architecture共173 tests、compileall、diff check通过。

2026-09-16 boss default graph cleanup live safety：提交 `50aeee0` 部署后 backup `/srv/old/data/backups/20260916T043549Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行世界BOSS商店购买、战斗或资产写入。

2026-09-16 admin default graph cleanup live safety：提交 `342b984` 部署后 backup `/srv/old/data/backups/20260916T043013Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行管理员资产调整、物品发放或计数写入。

2026-09-16 sect fairyland default graph cleanup live safety：提交 `30b60b7` 部署后 backup `/srv/old/data/backups/20260916T042524Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行宗门炼体堂领取、资产或计数写入。

2026-09-16 pet default graph cleanup live safety：提交 `947e1b2` 部署后 backup `/srv/old/data/backups/20260916T042002Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行宠物旅行、喂养、孵化或资产写入。

2026-09-16 bank main transaction blocker re-audit：`xiuxian_bank` matcher仅在已存在 `bank_accounts` first-use projection时走 `BankDeposit/Withdrawal/Upgrade/InterestApplication`；旧账户继续通过 `BankApplication → LegacyBankRepository → xiuxian_bank.transaction_service`执行 deposit/withdraw/upgrade/interest。`bank_accounts` schema未覆盖旧账户完整语义与迁移，不能把 first-use子路径当作主银行transaction cutover。

2026-09-16 activity reward blocker re-audit：`ActivityRewardApplication`真实Web入口存在，但 plugin默认注入 `LegacyActivityRewardRepository`，其 `claim_all`仍调用 `xiuxian_activity.service.claim_activity_rewards`；`activity_reward.001`仅marker，没有 feature-owned reward/claim schema与事务。不能将 application shell视为cutover。

2026-09-16 admin item-destroy blocker re-audit：管理员物品销毁两处真实handler仍调用 `AdminItemDestroyService.destroy`；`AdminAssetApplication`仅覆盖stone adjustment/item grant，`admin_asset.001`仅marker，没有 item-destroy application/repository/schema或幂等资产事务。保留为独立 admin asset blocker。

2026-09-16 mixelixir blocker re-audit：harvest handler通过 `MixelixirApplication`，但其默认 repository仍为 `LegacyMixelixirRepository`，继续调用 `MixelixirHarvestService/MixelixirSettlementService`；`mixelixir.001`仅marker。炼丹recipe、cost、reward recovery等另有真实 legacy services，缺少统一 feature-owned schema/UoW，未做表面切换。

2026-09-16 mixelixir default graph cleanup：AST调用图确认 `mixelixir_harvest_service`、`mixelixir_settlement_service`无可执行调用，harvest handler使用 `MixelixirApplication`；删除两个dead legacy实例及imports，保留 `LegacyMixelixirRepository`与recipe/cost/reward legacy services作为未迁移边界。mixelixir/source/architecture共183 tests、compileall、diff check通过。

2026-09-16 trade/rift blocker re-audit：Web `TradeApplication`仍默认 `LegacyTradeFeatureRepository`，Web/command trade模块的仙肆、鬼市、拍卖 queue/session handlers仍有真实 legacy service calls；`RiftApplication`仍默认 `LegacyRiftRepository`，rift entry/settlement handlers直接调用旧 services。两者缺少 feature-owned schema/repository与完整资产事务，未做 facade-only cutover。

2026-09-16 base/back/status boundary audit：Web base/back routes及旧命令仍使用 `BaseApplication`/`BackApplication` 的 `Legacy*Repository` facade，不能移除默认实例；`StatusApplication`虽有 migrated feature boundary，但未发现可替换 base/back handler graph。保留为 facade/legacy execution blocker，未做表面清理。

2026-09-16 base sign-in default graph cleanup：AST调用图确认 `sign_in_service`无可执行调用，真实签到 handler使用 lifecycle-bound `sign_in_application`；删除 dead `SignInService`默认实例/import，保留改名、送灵石、夺灵石等仍有真实调用的base services。顺带修复两处 isolated sign-in fixtures：在 feature schema前显式 `apply_platform_schema`，符合 shared ledger startup-only契约。exact regression与sign-in/source/architecture共177 tests、compileall、architecture、diff check通过。

2026-09-16 dongfu execution blocker audit：Map seed purchase/build已通过 `MapApplication` SQL repositories；但 `xiuxian_dongfu` expansion、plant、accelerate、patrol、array、visit、fertilize、infiltrate、harvest等十个旧 service实例均有真实AST调用。`DongfuApplication.execute_legacy_call`仍是 facade，未删除任何live instance，保留为 dongfu legacy execution blocker。

2026-09-16 beg default graph cleanup：AST调用图确认 `novice_gift_claim_service`、`beg_daily_reward_service`无可执行调用，daily/novice handlers均通过 `BegApplication.execute`进入 feature-owned `BegRepository`；删除两个dead legacy实例及imports，保留旧 service类文件作为rollback artifact。beg/source/architecture共163 tests、compileall、architecture、diff check通过。

2026-09-16 interactive default graph cleanup：AST调用图确认 stone daily reward、greeting claim、daily fortune三个旧 service实例无可执行调用，命令通过 `InteractiveApplication.execute`；删除三个dead实例及imports，保留 `InteractiveExpDailyRewardService`待其独立调用图审计。interactive/source/architecture共181 tests、compileall、architecture、diff check通过。

2026-09-16 interactive default graph cleanup continuation：独立AST确认 `InteractiveExpDailyRewardService`同样无可执行调用；删除最后一个Interactive旧 service实例/import。interactive/source/architecture共181 tests通过，legacy service import metric由134降至133。

2026-09-16 illusion default graph cleanup：`_run_illusion_action`已明确丢弃 legacy callback并通过 `IllusionApplication.execute`；删除 dead `IllusionChoiceService`实例/import及未执行callback，修正 stale source gate以断言应用adapter。illusion/source/architecture共167 tests、compileall、architecture、diff check通过。

2026-09-16 arena default graph cleanup：AST调用图确认 `arena_purchase_service`、`arena_challenge_purchase_service`、`arena_challenge_ticket_service`、`arena_challenge_settlement_service`无可执行调用，真实命令使用 `ArenaApplication`；删除四个dead service实例及class imports，保留结果dataclass、weekly rank/season reward真实服务。arena/source/architecture共200 tests、compileall、diff check通过。

2026-09-16 tower/dungeon default graph cleanup：AST调用图确认 `tower_purchase_service`、`dungeon_session_service`、`dungeon_purchase_service`无可执行调用，真实路径使用各自 application；删除dead实例/import，保留 `tower_settlement_service`、dungeon explore/team/settlement live services。tower+dungeon/source/architecture共272 tests、4 architecture subtests、compileall、diff check通过。

2026-09-16 pet default graph cleanup：AST调用图确认模块级 `pet_travel_start_service`无可执行调用，真实游历入口使用 `PetApplication`；删除dead module import/instance，保留 `LegacyPetRepository`内部按需兼容factory。pet/source/architecture共194 tests、compileall、diff check通过。

2026-09-16 training default graph cleanup：AST调用图确认 `training_completion_service`、`training_event_service`、`training_purchase_service`、`training_reset_service`无可执行调用，真实历练入口使用 `TrainingApplication`；删除四个dead service实例/import。training/source/architecture共184 tests、compileall、diff check通过。

2026-09-16 work default graph cleanup：AST调用图确认 `work_settlement_service`、`work_claim_service`无可执行调用，真实悬赏入口使用 `WorkClaimApplication`/`WorkSettlementApplication`；删除两个dead service实例/import及stale compatibility comments，保留item-use/refresh/abort/reset live services。work/source/architecture共183 tests、compileall、diff check通过。

2026-09-16 sect default graph cleanup：AST调用图确认 `sect_shop_purchase_service`、`sect_elixir_claim_service`、`sect_member_join_service`、`sect_mainbuff_learn_service`、`sect_secbuff_learn_service`无可执行调用，真实入口使用 `SectApplication`；删除五个dead compatibility实例/import，保留membership/open/close/disband/maintenance及fairyland live services。sect/source/architecture共312 tests、compileall、diff check通过。

2026-09-16 dungeon import cleanup：跨仓库调用图确认 `build_team_invite_message`只存在于 helper定义与测试直接调用，dungeon生产handler无引用；删除未绑定production import，保留helper实现供测试/兼容调用。dungeon/source/architecture共250 tests、4 architecture subtests、compileall、diff check通过。

2026-09-16 dungeon import cleanup live safety：提交 `b398f617` 部署后 backup `/srv/old/data/backups/20260916T165551Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行地牢组队、探索或资产写入。

2026-09-17 acceptance contract follow-up：补齐 legacy application/compensation/sign-in/Beg 临时fixture的 platform→feature migrations；统一 legacy contract action筛选排除查询/replay与必填业务参数，修正 stale source gates。web reconcile/dungeon/auction未迁移库现返回结构化 `503 migrations_required`，package reward接入统一 user/CSRF envelope，bank灰度v2路由从默认manifest移除。completion audit P0-P6全绿，P7按真实release证据缺失保持阻塞。

2026-09-17 task-progress/title acceptance slice：签到入口已由 `SignInApplication` 注入 application-owned task effects，删除 `xiuxian_base` 未使用的旧 task-progress import；source gate改为验证 `ApplicationSignInTaskEffects` 与 `SignInTaskRepository` 的 operation幂等及仍存活旧入口的显式 task operation。title batch transaction fixture补齐 platform ledger migration。focused 22 tests、compileall、inventory、diff check通过；未改变仍有真实调用的 buff/impart/work task-progress路径。

2026-09-17 task-progress/title live safety：提交 `41d278a0` 部署到受控容器后，backup manifest位于隔离 smoke data，包含 `game_db`、`player_db`、`trade_db`、`impart_db`、`message_db` 五库；migration dry-run五库均无 pending，旧实例 `5897` 停止后新实例 `5898` readiness通过，manifest只读命令通过，专用 operation marker写入并由rollback删除，reconcile `clean=true`、`operations=0`、`outbox_events=0`、`dead_events=0`，五库 restore完成且 checksum manifest可读；后置核验旧端口 healthy、新端口 closed、marker removed。未执行玩家任务、签到、称号或资产写入。

2026-09-17 trade legacy-import cleanup live safety：提交 `4b5e3675` 部署到受控容器后，backup manifest包含 `game_db`、`player_db`、`trade_db`、`impart_db`、`message_db` 五库；migration dry-run五库均无 pending，旧实例 `5897` 停止后新实例 `5898` readiness通过，manifest只读命令通过，专用 operation marker写入并由rollback删除，reconcile `clean=true`、`operations=0`、`outbox_events=0`、`dead_events=0`，五库 restore完成；后置核验旧端口 healthy、新端口 closed、marker removed。未执行交易、拍卖或资产写入。

2026-09-16 sect default graph cleanup live safety：提交 `b148dc28` 部署后 backup `/srv/old/data/backups/20260916T165029Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行宗门购买、入宗、丹药领取、主副 buff 学习或资产写入。

2026-09-16 work default graph cleanup live safety：提交 `6c5793fe` 部署后 backup `/srv/old/data/backups/20260916T164552Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行悬赏接取、结算、物品使用或资产写入。

2026-09-16 training default graph cleanup live safety：提交 `104012ff` 部署后 backup `/srv/old/data/backups/20260916T161921Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行历练购买、完成、事件或资产写入。

2026-09-16 pet default graph cleanup live safety：提交 `2feb89db` 部署后 backup `/srv/old/data/backups/20260916T161405Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行宠物游历、孵化、融合或资产写入。

2026-09-16 tower/dungeon default graph cleanup live safety：提交 `efea630` 部署后 backup `/srv/old/data/backups/20260916T160707Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行爬塔购买、地牢购买、会话或资产写入。

2026-09-16 arena default graph cleanup live safety：提交 `ebae8a0` 部署后 backup `/srv/old/data/backups/20260916T153608Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行竞技场购买、挑战、结算或资产写入。

2026-09-16 interactive acceptance correction：删除已不存在的 greeting compatibility comments，并更新 daily fortune/greeting source gates为断言 `InteractiveApplication.cleanup_before/execute`，拒绝真实 legacy调用形态。interactive/source/architecture共181 tests、compileall、architecture、diff check通过。

2026-09-16 interactive acceptance correction live safety：提交 `f0ef6a5` 部署后 backup `/srv/old/data/backups/20260916T152203Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行互动问候、运势、经验、奖励或资产写入。

2026-09-16 illusion default graph cleanup live safety：提交 `a5eedb6` 部署后 backup `/srv/old/data/backups/20260916T123203Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行幻境选择、奖励或资产写入。

2026-09-16 interactive default graph cleanup continuation live safety：提交 `ab5c523` 部署后 backup `/srv/old/data/backups/20260916T122650Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行互动经验奖励、每日奖励、运势或资产写入。

2026-09-16 interactive default graph cleanup live safety：提交 `47df707` 部署后 backup `/srv/old/data/backups/20260916T122258Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行互动问候、每日奖励、运势或资产写入。

2026-09-16 beg default graph cleanup live safety：提交 `e683c8d` 部署后 backup `/srv/old/data/backups/20260916T121853Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行仙途奇缘、新手礼包或资产写入。

2026-09-16 base sign-in default graph cleanup live safety：提交 `d7a6b95` 部署后 backup `/srv/old/data/backups/20260916T121019Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行签到、lottery、资产或计数写入。

2026-09-16 mixelixir default graph cleanup live safety：提交 `2d15bc8` 部署后 backup `/srv/old/data/backups/20260916T044201Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行炼丹收取、配方、药材或资产写入。

2026-09-16 lottery audit precision live safety：提交 `28c7943` 部署后 backup `/srv/old/data/backups/20260916T035645Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。运行期审计确认 `lottery_core_default_legacy=false`，仅显式 compatibility fallback保留。

2026-09-16 progress audit correction live safety：提交 `2de01de` 部署后 backup `/srv/old/data/backups/20260916T034555Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。audit仍诚实输出 `exit_ready=false` 与 legacy transaction/handle/explicit lottery fallback blockers。

2026-09-16 tianti source-quality correction live safety：提交 `c765eb5` 部署后 backup `/srv/old/data/backups/20260916T034056Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行天梯结算、修炼、药浴、突破或资产写入。

2026-09-16 platform schema ownership cleanup live safety：提交 `5e9df39` 部署后 backup `/srv/old/data/backups/20260916T031829Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行玩家资产、奖励、交易或计数写入。

2026-09-16 accessory/coordinator no-request-DDL live safety：提交 `8a15db2` 部署后 backup `/srv/old/data/backups/20260916T031105Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行饰品礼包、资产、奖励或计数写入。

2026-09-16 platform outbox no-request-DDL live safety：提交 `b216b28` 部署后 backup `/srv/old/data/backups/20260916T025628Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行玩家资产、奖励、交易或计数写入。

2026-09-16 platform ledger no-request-DDL live safety：提交 `c1dc625` 部署后 backup `/srv/old/data/backups/20260916T024523Z`，dry-run五库均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，105-entry recovery clean。live未执行玩家资产、奖励、交易或计数写入。

2026-09-16 platform ledger migration live safety：提交 `f3d474e` 部署后 backup `/srv/old/data/backups/20260916T021150Z`；首次 dry-run仅 game_db pending `platform.001`，真实 startup apply后 post-startup dry-run五库均为空，readiness六项全绿，105-entry recovery/reconcile clean。live未执行玩家资产、奖励或业务写入。

2026-09-16 auction settlement boundary audit：`AuctionSettlementApplication` 的 operation ledger/replay/read-only lookup已feature-owned，但 `settle_active` 默认仍调用 `LegacyAuctionSettlementRepository` 的 trade session algorithm；auction bid也同样依赖 legacy trade repository。未发现可独立切换的 queue/history/settlement SQL schema，保留为完整 auction migration blocker。

2026-09-16 marker-only feature audit：`puppet`、`natal_treasure`、`sect_fairyland` migrations均只创建 feature marker；对应 repositories分别只提供 LegacyPuppet、LegacyNatalTreasure、LegacySectFairyland adapters，没有 feature-owned asset schema/SQL transaction。三者保留为独立 migration/asset blocker，不做 facade-only切换。

2026-09-16 dungeon settlement audit：`DungeonSessionSqlRepository`真实覆盖 purchase/prepare/replay/resolve_rejection/session transition，但 `settle()`仍继承 `LegacyDungeonRepository` 的旧 explore settlement；未找到正式 prepared settlement SQL实现，保留为明确 legacy fallback blocker，不做不完整默认切换。

2026-09-16 auction bid audit：`AuctionBidApplication` 默认仍使用 `LegacyTradeRepository`，`auction.001`仅创建 feature marker，未创建 bid/queue/history SQL schema；详细 settlement/replay也仍依赖 trade legacy graph。未移除 plugin 默认 adapter，保留为 auction SQL repository/跨库资产事务 blocker。

2026-09-16 asset-transaction blocker re-audit：`PetApplication` 的 travel/feed/hatch、`WorkClaimApplication` claim/settle、`MixelixirApplication` harvest/settle、`DemonClaimApplication` claim均只有 legacy transaction adapters；对应 pet/work/mixelixir/world-events没有 feature-owned SQL schema/repository可安全切换，继续保留默认 rollback graph。

2026-09-15 task reward claim boundary：真实 `领取任务奖励` handler移除直接 `task_manager.reward_claim_service.get_result` replay读取，统一经 `TasksApplication.execute(operation_id,user_id,payload)`进入 application ledger；任务定义/奖励快照与跨game/player reward transaction仍保留为显式 legacy repository边界，未将ServicePort facade误报为SQL迁移。task reward/source共149 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 task claim live safety：提交 `52eb6d0` 部署后 backup `/srv/old/data/backups/20260915T000645Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行任务奖励领取写入。

2026-09-15 tianti operation ID boundary：已在真实 `tianti_training_application`/`tianti_settlement_application` handler的无事件 fallback中移除 `time.time_ns()`，改用注入 `UUIDGenerator`；154 training/settlement/source tests、catalog=103、compileall、architecture、diff check通过。旧 training/settlement services仍仅作兼容边界。

2026-09-15 tianti operation ID live safety：提交 `f8de4e5` 部署后 backup `/srv/old/data/backups/20260915T002629Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行炼体资产写入。

2026-09-15 tianti medicine bath operation ID boundary：真实 `tianti_training_application.apply_bath` handler的无事件 fallback改用注入 `UUIDGenerator`，不再使用 `time.time_ns()`；药浴/炼体/source共158 tests、catalog=103、compileall、architecture、diff check通过。药浴旧 service仍为兼容边界。

2026-09-15 tianti medicine bath live safety：提交 `1f47de5` 部署后 backup `/srv/old/data/backups/20260915T003100Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行药浴资产写入。

2026-09-15 tianti qiaoxue operation ID boundary：真实冲窍 handler已使用 `TiantiTrainingApplication.open_qiaoxue`，无事件 fallback改用注入 `UUIDGenerator`；突破/冲窍/source共155 tests、catalog=103、compileall、architecture、diff check通过。旧 `QiaoxueService`仅保留兼容入口。

2026-09-15 tianti qiaoxue live safety：提交 `61b520c` 部署后 backup `/srv/old/data/backups/20260915T003548Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行冲窍资产写入。

2026-09-15 tianti clock boundary：炼体结算、药浴、炼体状态和冲窍/突破 handler的时间读取统一使用注入 `runtime_clock`，operation fallback统一使用 `runtime_ids`，移除真实 handler中的 `datetime.now()`/`time.time_ns()`；tianti全套source/行为共170 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 tianti breakthrough operation ID closure：补齐突破 handler遗漏的无事件 operation ID fallback，统一使用注入 `runtime_ids.new_id()`；tianti全套source/行为共170 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 tianti breakthrough live safety：提交 `093398b` 部署后 backup `/srv/old/data/backups/20260915T014158Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行炼体突破写入。

2026-09-15 tianti clock live safety：提交 `bd436dd` 部署后 backup `/srv/old/data/backups/20260915T004302Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行炼体/药浴/冲窍写入。

2026-09-15 partner relation operation ID boundary：`xiuxian_buff/partner.py` 的关系操作ID统一从注入 `UUIDGenerator` 获取，移除 `_relation_operation_id` 的 `time.time_ns()` fallback；partner/source共166 tests、8 subtests、catalog=103、compileall、architecture、diff check通过。关系 transaction services仍保留为兼容边界，未伪报 application迁移。

2026-09-15 partner relation ID live safety：提交 `ddda831` 部署后 backup `/srv/old/data/backups/20260915T004838Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行关系绑定/解除或师徒资产写入。

2026-09-15 partner operation ID closure：partner token use、bind、unbind以及统一 relation operation helper的无事件 fallback全部改用注入 `UUIDGenerator`，不再使用 `time.time_ns()`；partner/source共166 tests、8 subtests、catalog=103、compileall、architecture、diff check通过。mentor/partner transaction service默认迁移仍未完成。

2026-09-15 partner operation ID live safety：提交 `cc128c2` 部署后 backup `/srv/old/data/backups/20260915T005404Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行关系资产写入。

2026-09-15 partner clock boundary：真实 partner/buff handler及关系helper的系统时间读取统一使用注入 `runtime_clock.now()`，覆盖邀请过期、绑定/解除、师徒冷却、历史记录与关系展示；partner/source共166 tests、8 subtests、catalog=103、compileall、architecture、diff check通过。底层关系 transaction service仍保留兼容边界。

2026-09-15 partner clock live safety：提交 `cfda164` 部署后 backup `/srv/old/data/backups/20260915T005916Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行关系时间状态写入。

2026-09-15 partner random boundary：双修结算事件选择、特殊事件概率、道侣突破概率统一使用注入 `runtime_random`，移除真实 partner handler中的全局 `random.choice/randint`；partner/source共166 tests、8 subtests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 partner random live safety：提交 `14817af` 部署后 backup `/srv/old/data/backups/20260915T010511Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行双修随机资产写入。

2026-09-15 trade/auction operation ID boundary：仙肆 removal/clear/name-removal/fast-buy、鬼市 order、auction session等真实 handler的无事件 fallback统一使用注入 `UUIDGenerator`，移除多处 `time.time_ns()`；trade/auction/source共209 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 trade/auction operation ID live safety：提交 `44c5f76` 部署后 backup `/srv/old/data/backups/20260915T011156Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行交易/拍卖资产写入。

2026-09-15 trade/auction clock boundary：仙肆 operation helper、鬼市周末/时段判断、拍卖展示与自动调度时间统一使用注入 `runtime_clock.now()`；trade/auction/source共209 tests、catalog=103、compileall、architecture、diff check通过。旧交易/拍卖 service仍作兼容边界。

2026-09-15 trade/auction clock live safety：提交 `c2033fd` 部署后 backup `/srv/old/data/backups/20260915T013238Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行交易时间状态写入。

2026-09-15 work settlement boundary：真实悬赏结算handler移除直接 `WorkSettlementService.get_result` replay读取，统一经 `WorkSettlementApplication.settle` 的 operation ledger；work operation ID、奖励浮动与展示时间分别使用注入 `runtime_ids/runtime_random/runtime_clock`。work settlement/refresh/source共151 tests、catalog=103、compileall、architecture、diff check通过；完整 WorkSettlementRepository 仍是下一步SQL迁移边界。

2026-09-15 work settlement live safety：提交 `b24d67e` 部署后 backup `/srv/old/data/backups/20260915T022522Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行悬赏奖励写入。

2026-09-15 work replay/clock boundary：悬赏结算与接取handler移除直接 legacy service replay读取，operation ID fallback统一使用注入 `runtime_ids`，奖励浮动使用 `runtime_random`，悬赏展示/接取时间使用 `runtime_clock`；work settlement/refresh/source共151 tests、catalog=103、compileall、architecture、diff check通过。完整 WorkSettlement/WorkClaim SQL repository仍为未迁移边界。

2026-09-15 work replay/clock live safety：提交 `0ff5cbd` 部署后 backup `/srv/old/data/backups/20260915T024903Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行悬赏接取/结算写入。

2026-09-15 work item operation boundary：悬赏加速/捕获 handler的无事件 operation ID改用注入 `runtime_ids`，捕获奖励倍率改用 `runtime_random`；work item/settlement/source共151 tests、catalog=103、compileall、architecture、diff check通过。`WorkItemUseService`完整SQL事务仍未提升为feature repository。

2026-09-15 work item live safety：提交 `29d7b46` 部署后 backup `/srv/old/data/backups/20260915T025940Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行悬赏加速/捕获写入。

2026-09-15 work clock boundary：悬赏刷新、提醒倒计时、提醒状态和每日刷新重置的真实 handler/job时间读取统一改用注入 `runtime_clock.now()`，移除该入口的 `datetime.now()`；work settlement/refresh/source共151 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 work clock live safety：提交 `f65657a` 部署后 backup `/srv/old/data/backups/20260915T030704Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行悬赏时间状态写入。

2026-09-15 work random boundary：`workmake`/`workhandle` 支持显式 `random_source` 与 `clock`，真实悬赏生成/结算调用移除全局 `random.seed/setstate` 包装，统一使用 `runtime_random/runtime_clock`；work settlement/refresh/source共151 tests、catalog=103、compileall、architecture、diff check通过。旧 work transaction service仍为未迁移 repository边界。

2026-09-15 work random live safety：提交 `b68bd65` 部署后 backup `/srv/old/data/backups/20260915T031844Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行悬赏生成/结算写入。

2026-09-15 work random live revalidation：因前次 live证据补丁超时，使用提交 `f2cd836` 重新部署并验证；backup `/srv/old/data/backups/20260915T032530Z`，dry-run pending为空、reconcile clean，startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行悬赏生成/结算写入。

2026-09-15 work helper clock fallback：`workhandle.do_work` 的兼容默认时间也改由 `SystemClock` 提供，真实 handler仍显式传入 `runtime_clock`；work settlement/refresh/source共151 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 work helper clock live safety：提交 `e10fabc` 部署后 backup `/srv/old/data/backups/20260915T040254Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行悬赏生成写入。

2026-09-15 dufang provider boundary：鉴石数据辅助路径的时间读取改用注入 `runtime_clock`，并在 dufang composition root 建立 `runtime_random/runtime_ids` provider，为后续随机/operation切换保留明确边界；dufang/source共153 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 dufang provider live safety：提交 `19f15b6` 部署后 backup `/srv/old/data/backups/20260915T011737Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行鉴石资产写入。

2026-09-15 dufang random boundary：鉴石共享用户抽样、封印物/过程/事件选择、结果权重和共享触发概率统一使用注入 `runtime_random`，移除真实鉴石handler的全局 `random.*` 调用；dufang/source共153 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 dufang random live safety：提交 `73caf60` 部署后 backup `/srv/old/data/backups/20260915T012437Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行鉴石随机资产写入。

2026-09-15 dufang clock/operation boundary：下注、 payout、共享事件结算及 unseal migration 辅助路径统一使用注入 `runtime_clock`，下注无事件 operation ID使用 `runtime_ids`；dufang/source共153 tests、catalog=103、compileall、architecture、diff check通过。下注/ payout/共享三段完整事务仍保留为兼容边界。

2026-09-15 dufang clock/operation live safety：提交 `f58773d` 部署后 backup `/srv/old/data/backups/20260915T033844Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行鉴石下注/支付/共享写入。

2026-09-15 dufang bet replay boundary：鉴石下注handler移除最前置的 bet/payout legacy replay直读，统一先经 `_run_dufang_action` 进入 dufang application ledger；二阶段 payout duplicate兼容分支保留以支持当前旧 settlement 合同。dufang/source共153 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 dufang bet replay live safety：提交 `fec39a7` 部署后 backup `/srv/old/data/backups/20260915T035119Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行鉴石下注/支付写入。

2026-09-15 mixelixir harvest boundary：真实灵田收取 handler移除 `MixelixirHarvestService.get_result` 前置 replay直读，统一经 `MixelixirApplication.harvest` ledger；收取时间、药材随机选择、无事件 operation ID分别使用注入 `runtime_clock/runtime_random/runtime_ids`。harvest/source共148 tests、catalog=103、compileall、architecture、diff check通过；跨 game/player harvest repository仍是下一步 SQL 迁移边界。

2026-09-15 mixelixir harvest live safety：提交 `93d90f1` 部署后 backup `/srv/old/data/backups/20260915T041208Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行灵田收取写入。

2026-09-15 mixelixir operation ID boundary：收取等级、控火升级、配方保存、炼丹奖励收回/领取及炼丹消耗路径的无事件 operation ID统一使用注入 `runtime_ids`，保留各自旧 transaction service 作为兼容边界；mixelixir全套行为/source共169 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 mixelixir operation ID live safety：提交 `98b440d` 部署后 backup `/srv/old/data/backups/20260915T041757Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行炼丹升级/配方/奖励写入。

2026-09-15 buff operation ID boundary：洞府购买、normal training、stone training和normal PvP真实入口的无事件 operation ID统一使用注入 `runtime_ids`；normal-training/source共146 tests、catalog=103、compileall、architecture、diff check通过。训练/PvP完整跨库 transaction service仍未提升为feature repository。

2026-09-15 buff operation ID live safety：提交 `4516f29` 部署后 backup `/srv/old/data/backups/20260915T042429Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行洞府/训练/PvP写入。

2026-09-15 normal training provider boundary：正常修炼真实 handler的经验倍率、伪灵根灵石奖励和训练周周期统一使用注入 `runtime_random/runtime_clock`；normal-training/source共146 tests、catalog=103、compileall、architecture、diff check通过。NormalTrainingLifecycleService完整跨库事务仍未提升为feature repository。

2026-09-15 normal training live safety：提交 `a87dba9` 部署后 backup `/srv/old/data/backups/20260915T043028Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行正常修炼写入。

2026-09-15 buff clock boundary：洞府购买、灵田状态、历练状态和银行兼容默认值的真实入口时间读取统一使用注入 `runtime_clock.now()`；normal-training/source共146 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 buff clock live safety：提交 `0a4f1cc` 部署后 backup `/srv/old/data/backups/20260915T043737Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行洞府/状态写入。

2026-09-15 bank provider boundary：灵庄存取、会员升级、结息真实入口的无事件 operation ID统一使用注入 `runtime_ids`，银行状态/利息兼容时间统一使用 `runtime_clock`；bank/source共192 tests、catalog=103、compileall、architecture、diff check通过。BankApplication当前仍使用 LegacyBankRepository，旧 replay分支保留为未完成事务迁移边界。

2026-09-15 bank provider live safety：提交 `e2b0048` 部署后 backup `/srv/old/data/backups/20260915T044602Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行灵庄资产写入。

2026-09-15 natal operation ID boundary：本命觉醒、重塑、养成、效果升阶、铭刻和遗忘真实入口的无事件 operation ID统一使用注入 `runtime_ids`；natal全套行为/source共177 tests、catalog=103、compileall、architecture、diff check通过。完整 NatalTreasureApplication 尚未接入真实 handler，旧 services/replay仍为未迁移边界。

2026-09-15 natal operation ID live safety：提交 `4f56c43` 部署后 backup `/srv/old/data/backups/20260915T045144Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行本命法宝资产写入。

2026-09-16 natal treasure config cleanup：删除 `natal_config.py` 未使用的 global random import；natal/source共177 tests、catalog=103、compileall、architecture、diff check通过。本命法宝尚无完整 application runtime wiring，transaction service边界保持不变。

2026-09-16 natal treasure config live safety：提交 `1d89674` 部署后 backup `/srv/old/data/backups/20260915T180412Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行本命法宝写入。

2026-09-15 sect weekly/provider guard：宗门周常无事件 operation ID改用注入 `runtime_ids`；同步修正四个已完成 application handler的过时 source guard（elixir/mainbuff/join/secbuff/shop），均明确要求 application调用并禁止旧 service。sect/source共298 tests、catalog=103、compileall、architecture、diff check通过；宗门周常 reward transaction仍是未迁移边界。

2026-09-15 sect weekly/provider live safety：提交 `76d79da` 部署后 backup `/srv/old/data/backups/20260915T051217Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行宗门周常奖励写入。

2026-09-15 NewAPI operation ID boundary：NewAPI绑定、删除、自动签到 command/store path的无事件 operation ID统一使用注入 `UUIDGenerator`，保持 `EntertainmentApplication.execute_legacy_call` 对历史JSON mutator的幂等封装；NewAPI/source共148 tests、catalog=103、compileall、architecture、diff check通过。JSON存储mutator尚未提升为独立SQL repository。

2026-09-15 NewAPI operation ID live safety：提交 `a47a511` 部署后 backup `/srv/old/data/backups/20260915T053932Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行账号绑定/删除/自动签到写入。

2026-09-15 base player/stone provider boundary：玩家改名、送石、偷石、抢石真实入口的无事件 operation ID统一使用注入 `runtime_ids`，注册/lottery业务日期使用 `runtime_clock`，偷石成功率及金额选择使用 `runtime_random`；base stone/player/source共197 tests、2 subtests、catalog=103、compileall、architecture、diff check通过。完整 player/stone transaction service仍是未迁移边界。

2026-09-15 base player/stone provider live safety：提交 `e5b731d` 部署后 backup `/srv/old/data/backups/20260915T054936Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行玩家/灵石资产写入。

2026-09-15 base sign operation ID boundary：签到真实入口的无事件 operation ID统一使用注入 `runtime_ids`；sign/source146 tests、stone/player54 tests及2 subtests、catalog=103、compileall、architecture、diff check通过。签到 task/lottery side effects仍按已记录 wiring/legacy blocker处理。

2026-09-15 base sign operation ID live safety：提交 `28f5528` 部署后 backup `/srv/old/data/backups/20260915T073714Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行签到/lottery写入。

2026-09-15 registration batch ID boundary：RegistrationBatcher内部请求标识从 `time.time_ns()` 改为注入 `UUIDGenerator`；队列等待仍使用 `time.monotonic()` 作为非业务超时钟。registration/source共144 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 registration batch ID live safety：提交 `b8d17dc` 部署后 backup `/srv/old/data/backups/20260915T083917Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行角色注册写入。

2026-09-15 base daohao random boundary：注册/随机改名共用的道号生成helper支持显式 `random_source`，词库选择、结构权重、连接符和递归重试不再直接使用全局 random；base stone/player/source共197 tests、2 subtests、catalog=103、compileall、architecture、diff check通过。玩家命名 transaction service仍为未迁移边界。

2026-09-15 base daohao random live safety：提交 `4b69070` 部署后 backup `/srv/old/data/backups/20260915T055934Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行注册/改名写入。

2026-09-15 compensation provider boundary：补偿 common 的随机ID、duration解析和 invitation 三处无事件 operation ID统一使用注入 `runtime_random/runtime_clock/runtime_ids`；修复相对过期时间使用 aware runtime clock 与固定日期/时间比较时的 aware/naive不一致。compensation/invitation/source共164 tests、catalog=103、compileall、architecture、diff check通过；JSON mutator 仍通过 `CompensationApplication` compatibility boundary。

2026-09-15 compensation provider live safety：提交 `abd3dac` 部署后 backup `/srv/old/data/backups/20260915T061541Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行补偿/邀请奖励写入。

2026-09-15 compensation clock boundary：补偿过期/生效判断、记录创建、兑换码列表和兼容 JSON 辅助路径统一使用注入 `runtime_clock`；common/invitation/redeem/source共164 tests、catalog=103、compileall、architecture、diff check通过。补偿定义/领取 JSON mutator 仍经 compatibility application，未伪报SQL迁移完成。

2026-09-15 compensation clock live safety：提交 `6fd222b` 部署后 backup `/srv/old/data/backups/20260915T063004Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行补偿/兑换码写入。

2026-09-15 beg daily boundary：日常奇缘真实 handler移除 `BegDailyRewardService.get_result` 前置 replay，统一经 `BegApplication.execute` 的 ledger；奖励金额/文案、账号年龄校验和无事件 operation ID分别使用注入 `runtime_random/runtime_clock/runtime_ids`。beg daily/source共148 tests、catalog=103、compileall、architecture、diff check通过；novice gift仍是下一独立边界。

2026-09-15 beg clock boundary：新手/奇缘帮助文本的当前时间统一使用注入 `runtime_clock`；beg/source共148 tests、catalog=103、compileall、architecture、diff check通过。daily/novice reward transaction legacy兼容边界保持不变。

2026-09-15 beg clock live safety：提交 `3e98504` 部署后 backup `/srv/old/data/backups/20260915T142921Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行新手/奇缘奖励写入。

2026-09-15 beg daily live safety：提交 `61955a4` 部署后 backup `/srv/old/data/backups/20260915T064025Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行日常奇缘奖励写入。

2026-09-15 beg novice boundary：新手礼包真实 handler移除 `NoviceGiftClaimService.get_result` 前置 replay，统一经 `BegApplication.execute` ledger；claimed_at及无事件 operation ID使用注入 `runtime_clock/runtime_ids`。beg daily/novice/source共148 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 beg novice live safety：提交 `6200770` 部署后 backup `/srv/old/data/backups/20260915T064723Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行新手礼包写入。

2026-09-16 beg request-DDL boundary：`BegRepository.daily_result/novice_result/settle_daily/claim_novice` 移除 request-time `ensure_schema`，`beg.001` migration成为两类 operation 表唯一启动前置；更新 legacy service fixture并新增缺表 replay拒绝测试。beg/source共149 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 beg request-DDL live safety：提交 `f467b0e` 部署后 backup `/srv/old/data/backups/20260915T210023Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行日常奇缘或新手礼包写入。

2026-09-15 arena clock boundary：竞技场挑战时间、对手缓存过期和赛季日期统一使用注入 `runtime_clock`；arena/source共186 tests、catalog=103、compileall、architecture、diff check通过。`_arena_fight` 全局 RNG seed/state 与完整 settlement service仍是后续迁移边界。

2026-09-16 arena repository clock boundary：`ArenaApplication` 将注入 Clock 传入默认 SQL repository，honor weekly purchase 与 challenge daily reset 默认日期不再直接使用 `date.today()`；arena/source共186 tests、catalog=103、compileall、architecture、diff check通过。跨库 UoW已保持，challenge settlement legacy bridge仍未迁移。

2026-09-16 arena repository clock live safety：提交 `84fbd75` 部署后 backup `/srv/old/data/backups/20260915T181409Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行竞技场购买/挑战写入。

2026-09-15 arena clock live safety：提交 `6eb94d0` 部署后 backup `/srv/old/data/backups/20260915T065429Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行竞技场挑战/缓存/赛季写入。

2026-09-15 tribulation operation ID boundary：突破、丹药融合和普通/天命/心魔渡劫真实入口的无事件 operation ID统一使用注入 `runtime_ids`；breakthrough/tribulation/source共159 tests、catalog=103、compileall、architecture、diff check通过。渡劫多库 transaction service 与 legacy JSON state migration仍未提升为feature application。

2026-09-15 tribulation operation ID live safety：提交 `2eec988` 部署后 backup `/srv/old/data/backups/20260915T070301Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行突破/渡劫资产写入。

2026-09-15 xiangyuan provider boundary：仙缘发送/领取真实入口的无事件 operation ID和随机灵石分配统一使用注入 `runtime_ids/runtime_random`；xiangyuan/source共151 tests、catalog=103、compileall、architecture、diff check通过。跨群仙缘 settlement service仍未提升为feature application/repository。

2026-09-15 xiangyuan provider live safety：提交 `83d01c2` 部署后 backup `/srv/old/data/backups/20260915T071220Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行仙缘发送/领取写入。

2026-09-15 blackhouse clock boundary：小黑屋 JSON 管理记录的更新时间统一使用注入 `runtime_clock`；admin blackhouse/source共148 tests、catalog=103、compileall、architecture、diff check通过。该路径为非资产管理同步，仍使用现有 JSON store。

2026-09-15 blackhouse clock live safety：提交 `b032554` 部署后 backup `/srv/old/data/backups/20260915T072114Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行小黑屋名单写入。

2026-09-15 activity operation ID boundary：活动主命令、活动任务/战令/兑换及 boss item/cooperative action 的无事件 operation ID统一使用注入 `UUIDGenerator`；activity/source共189 tests、catalog=103、compileall、architecture、diff check通过。ActivityApplication仍将历史 activity service 作为显式兼容执行器，尚未完成所有活动事务的独立 repository迁移。

2026-09-15 activity operation ID live safety：提交 `7cad020` 部署后 backup `/srv/old/data/backups/20260915T075751Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行活动领取/兑换/boss写入。

2026-09-15 dungeon provider boundary：DungeonManager/DungeonTemplate生成、怪物/BOSS随机属性和 handler team/reset时间与 operation ID统一使用注入 `runtime_random/runtime_clock/runtime_ids`；dungeon/source共236 tests、4 subtests、catalog=103、compileall、architecture、diff check通过。snapshot兼容读取和完整 dungeon settlement仍保留既有 transaction边界。

2026-09-15 dungeon provider live safety：提交 `2f1cf4f` 部署后 backup `/srv/old/data/backups/20260915T081427Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行副本生成/重置/结算写入。

2026-09-16 dungeon composition SQL cutover：`plugin.py` 移除 dungeon 默认 `LegacyDungeonRepository` 注入，改由 `DungeonApplication` 默认 `DungeonSessionSqlRepository`；legacy class保留为显式 rollback，prepared settlement仍是未迁移边界。dungeon/source共236 tests、4 subtests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 dungeon composition SQL cutover live safety：提交 `4ee08e2` 部署后 backup `/srv/old/data/backups/20260915T194527Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行副本资产写入。

2026-09-15 dungeon manager provider boundary：DungeonManager/DungeonTemplate的重置ID、模板/BOSS/怪物属性、事件和掉落随机统一使用实例 `runtime_ids/runtime_clock/runtime_random`；dungeon/source共236 tests、4 subtests、catalog=103、compileall、architecture、diff check通过。副本完整 settlement 与 snapshot兼容服务仍为未迁移边界。

2026-09-15 dungeon manager provider live safety：提交 `24628c1` 部署后 backup `/srv/old/data/backups/20260915T082225Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行副本生成/重置写入。

2026-09-15 rift operation ID boundary：秘境手动生成、普通入口和门票入口的无事件 operation ID统一使用注入 `runtime_ids`；rift/source共190 tests、catalog=103、compileall、architecture、diff check通过。秘境 deterministic RNG、完整事件/资产 settlement仍保留为未迁移边界。

2026-09-15 rift operation ID live safety：提交 `bed0b5d` 部署后 backup `/srv/old/data/backups/20260915T084745Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行秘境生成/进入写入。

2026-09-15 world-events operation ID boundary：恶魔/灵脉手动生命周期、恶魔攻击和领奖真实入口的无事件 operation ID统一使用注入 `runtime_ids`；demon/source共166 tests、catalog=103、compileall、architecture、diff check通过。攻击/领奖复杂 replay、事件随机与多库 settlement仍为未迁移边界。

2026-09-15 world-events operation ID live safety：提交 `3471689` 部署后 backup `/srv/old/data/backups/20260915T090030Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行恶魔/灵脉事件写入。

2026-09-15 pet operation ID boundary：宠物 active switch、fusion breakthrough、skill reroll/replace真实入口的无事件 operation ID统一使用注入 `runtime_ids`；pet/source共180 tests、catalog=103、compileall、architecture、diff check通过。上述复杂宠物事务仍保留既有 legacy service，hatch完整写事务阻塞边界不变。

2026-09-15 pet operation ID live safety：提交 `819babf` 部署后 backup `/srv/old/data/backups/20260915T093602Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行宠物切换/融合/技能写入。

2026-09-15 interactive provider boundary：互动经验/灵石结算、早晚安、运势真实入口的无事件 operation ID与清理窗口统一使用注入 `runtime_ids/runtime_clock`，统一经 `InteractiveApplication` action boundary；interactive/source共167 tests、catalog=103、compileall、architecture、diff check通过。历史 greeting/fortune service仅作为显式兼容目标保留。

2026-09-16 daily fortune request-DDL boundary：`DailyFortuneRepository.get/insert` 移除 request-time `ensure_schema`，已注册 `daily_fortune.001` migration成为 claims 表唯一启动前置；architecture fixture改为显式 migration。architecture/source共157 tests、catalog=103、compileall、architecture、diff check通过；旧 greeting/fortune compatibility边界仍保留。

2026-09-16 daily fortune request-DDL live safety：提交 `26da470` 部署后 backup `/srv/old/data/backups/20260915T210630Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行运势领取写入。

2026-09-16 interactive request-DDL blocked：`features/interactive/repository.py` 的六类奖励/greeting/fortune操作仍在请求路径调用 `ensure_schema`；`interactive.001` migration已注册，但现有 legacy service测试fixture广泛依赖 `_ensure_schema` 和隐式建表，需独立批量迁移测试契约后再收口。本轮不做表面替换，跳转至更小的正式 repository slice。

2026-09-16 interactive request-DDL boundary：`InteractiveRepository` 六类奖励/greeting/fortune操作移除 request-time `ensure_schema`，`interactive.001` migration成为全部 operation/claim/fortune表的启动前置；exp/stone/greeting/fortune legacy service fixtures改为显式 migration。interactive/source共167 tests、catalog=104、compileall、architecture、diff check通过；旧 service保留为显式 rollback。

2026-09-16 interactive request-DDL live safety：提交 `9496554` 部署后 backup `/srv/old/data/backups/20260915T214056Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行互动经验、灵石、问候或运势写入。

2026-09-16 title request-DDL boundary：新增 checksum-safe `title.002` migration，创建/补齐 `title`与`title_transaction_operations` 实际 schema，并加入 player_db startup migration集合；`TitleRepository` replay/equip/unlock/rename路径移除 request-time `ensure_schema`，legacy service fixtures改为显式 `apply_title_schema`。title/source共155 tests、catalog=104、compileall、architecture、diff check通过；旧 title service保留为显式 rollback。

2026-09-16 title migration routing correction：首次 `title.002` live dry-run发现其错误出现在 game_db pending；未执行 apply、未改变数据。修正 startup 分库过滤，将 `title.002` 排除于 game_db并保留于 player_db，重新通过 title/source 155 tests、catalog=104、compileall、architecture、diff check；待修复提交重新执行 live safety。

2026-09-16 title schema live safety：修复提交 `73b2f26` 部署后 backup `/srv/old/data/backups/20260915T213408Z`，game_db/player_db migration dry-run均无 pending、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行称号解锁/装备写入。

2026-09-16 accessory package request-DDL blocked：game-side `accessory_package.001` 已创建 operation schema，但 repository请求路径与 legacy service同时跨 player attached schema；player attached migration尚未接入 startup，不能只删除 game-side ensure而破坏跨库兼容。保留现状并待 attached migration runner完成后整体收口。

2026-09-16 accessory attached migration boundary：startup migration阶段在 game_db UoW 显式 attach player_db 为 `player_data`，执行 checksum-safe `accessory_package.player_data.001`；新增 runner wiring与首次/重复应用 checksum focused test。attached/accessory/source/architecture共179 tests、catalog=104、compileall、diff check通过；原 player attached migration blocker已解除，旧 accessory transaction compatibility仍保留。

2026-09-16 accessory game request-DDL boundary：`AccessoryPackageGameRepository.get/prepare/finalize` 移除 request-time `ensure_schema`，game-side `accessory_package.001`成为 operation表启动前置；player-side `schema_policy`与attached namespace guard保持不变。accessory/source共179 tests、catalog=104、compileall、architecture、diff check通过。

2026-09-16 accessory application request-DDL boundary：`AccessoryPackageApplication` replay guard移除 game repository `ensure_schema`调用，完全依赖 startup `accessory_package.001`；ledger failure schema初始化保持 infrastructure-owned。accessory/source共179 tests、catalog=104、compileall、architecture、diff check通过。

2026-09-16 accessory application request-DDL live safety：提交 `73fde1b` 部署后 backup `/srv/old/data/backups/20260915T223800Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行饰品礼包或资产写入。

2026-09-16 accessory game request-DDL live safety：提交 `6ab73e8` 部署后 backup `/srv/old/data/backups/20260915T215716Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行饰品礼包或资产写入。

2026-09-16 attached migration clock fallback：`apply_attached_player_accessory` 未显式传入 Clock 时改用 `SystemClock`，保留显式 runtime Clock优先和既有 checksum；attached/accessory/source共179 tests、catalog=104、compileall、architecture、diff check通过。

2026-09-16 attached migration clock live safety：提交 `5d842a3` 部署后 backup `/srv/old/data/backups/20260915T220352Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行饰品礼包或player accessory写入。

2026-09-16 accessory attached migration live safety：提交 `a9a7c48` 部署后 backup `/srv/old/data/backups/20260915T215030Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行饰品礼包、player accessory或资产写入。

2026-09-15 interactive provider live safety：提交 `1624de3` 部署后 backup `/srv/old/data/backups/20260915T091541Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行互动结算/领取写入。

2026-09-15 interactive random boundary：fortune、早晚安、互动奖励回复和日常文本选择统一使用注入 `runtime_random`；interactive/source共167 tests、catalog=103、compileall、architecture、diff check通过。InteractiveApplication及历史 greeting/fortune transaction兼容边界保持不变。

2026-09-15 interactive random live safety：提交 `cf45a83` 部署后 backup `/srv/old/data/backups/20260915T121233Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行互动随机奖励/领取写入。

2026-09-15 interactive clock boundary：互动时段文案、fortune/reward/greeting application business date统一使用注入 `runtime_clock`；interactive/source共167 tests、catalog=103、compileall、architecture、diff check通过。历史 transaction service 时间兼容边界仍保留。

2026-09-15 interactive clock live safety：提交 `df74bf8` 部署后 backup `/srv/old/data/backups/20260915T131445Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行互动时间窗口/奖励写入。

2026-09-15 sect main operation ID boundary：宗门主 handler统一使用既有 `sect_ids` UUID provider生成无事件 operation ID；sect/source共298 tests、catalog=103、compileall、architecture、diff check通过。宗门周常领取和未迁移管理事务仍保留显式 legacy service边界。

2026-09-15 sect main operation ID live safety：提交 `ffa5fad` 部署后 backup `/srv/old/data/backups/20260915T092123Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行宗门操作写入。

2026-09-15 sect clock boundary：宗门材料/每日维护 scheduler、inactive owner检查和 fairyland查询/领取真实入口的日期读取统一使用注入 `runtime_clock`；sect/source共298 tests、catalog=103、compileall、architecture、diff check通过。维护与 fairyland transaction service仍保留为未迁移边界。

2026-09-15 sect clock live safety：提交 `ba1c4d3` 部署后 backup `/srv/old/data/backups/20260915T132534Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行宗门维护/fairyland写入。

2026-09-15 sect random boundary：宗门丹药领取、主/副 buff 搜寻和创建名称随机选择统一使用注入 `runtime_random`；sect/source共298 tests、catalog=103、compileall、architecture、diff check通过。周常、维护和未迁移管理 transaction 仍保留显式 legacy service边界。

2026-09-15 sect random live safety：提交 `d56dc59` 部署后 backup `/srv/old/data/backups/20260915T145725Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行宗门随机奖励/创建写入。

2026-09-16 sect repository clock boundary：`SectApplication` 将注入 Clock 传给 `SectRenameSqlRepository`，claim elixir 与 sect shop 资产时间戳不再直接读取系统时间；sect/source共298 tests、catalog=103、compileall、architecture、diff check通过。其余 legacy repository fallback仍明确保留。

2026-09-16 sect repository clock live safety：提交 `48f065f` 部署后 backup `/srv/old/data/backups/20260915T162923Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行宗门丹药/商店资产写入。

2026-09-16 sect weekly-key clock boundary：宗门商店默认 ISO week key 从 `date.today()` 改为同一 `SectApplication` 注入 Clock，显式 week_key仍优先；sect/source共298 tests、catalog=103、compileall、architecture、diff check通过。legacy maintenance/weekly service边界保持不变。

2026-09-16 sect weekly-key clock live safety：提交 `83d29eb` 部署后 backup `/srv/old/data/backups/20260915T164556Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行宗门商店购买写入。

2026-09-16 sect tasks blocked：`sect_tasks.py` 仍在构造/读取路径调用 request-time `ensure_table()`，并直接持有 `XiuxianDateManage`、时间和随机任务状态；在正式 migration、UoW/application 设计完成前不做 provider-only 伪迁移，保留为 pending legacy slice。

2026-09-16 partner mentor blocked：`xiuxian_buff/partner.py` 的双修邀请、师徒绑定/收徒/出师和突破奖励仍直接依赖多组 legacy transaction service；仓库暂无对应 feature application，暂不做 provider-only 伪迁移，保留为 pending legacy slice。

2026-09-16 accessory package attached migration blocked：`attached_migrations.py` 定义了 checksum-safe 的 `player_data.player_accessory` migration，但当前源码没有 startup/runner 调用点；直接移除 request-time `player.ensure_schema()` 会使真实 player namespace 缺表。保留正式 migration与现有 guard，待 attached migration 接入 startup 后再收口。

2026-09-16 activity reward blocked：`ActivityRewardApplication` 当前仍默认桥接 `LegacyActivityRewardRepository`，`activity_reward.001` 只创建 feature marker，未提供 activity claim 的正式 SQL repository/schema；不能把 ledger facade当作完整迁移，保留为 pending legacy slice并跳转独立路径。

2026-09-16 admin asset blocked：`AdminAssetApplication` 的 stone/item 操作仍默认桥接 `LegacyAdminStoneRepository/LegacyAdminItemRepository`，`admin_asset.001` 目前只创建 feature marker，未提供正式资产 repository/schema；不将 ledger facade误报为迁移，保留为 pending legacy slice并跳转独立路径。

2026-09-16 sect fairyland blocked：`SectFairylandApplication` 当前仍默认桥接 `LegacySectFairylandRepository`，`sect_fairyland.001` 只创建 feature marker，未提供正式 SQL repository/schema；不做 facade/provider-only 伪迁移，保留为 pending legacy slice并跳转独立路径。

2026-09-16 work blocked：`WorkClaimApplication`/settlement 仍默认桥接 `LegacyWork*Repository`，`work.001` 只创建 feature marker，未提供正式 SQL repository/schema；不把 ledger facade当作完整迁移，保留 claim/settlement legacy边界并跳转独立路径。

2026-09-16 mixelixir blocked：`MixelixirApplication` 的 harvest/settle 仍默认桥接 `LegacyMixelixirRepository`，`mixelixir.001` 只创建 feature marker，未提供正式 SQL repository/schema；不做 facade/provider-only 伪迁移，保留升级/配方/奖励 transaction legacy边界并跳转独立路径。

2026-09-16 pet blocked：`PetApplication` 的 travel/feed/hatch/replay 仍默认桥接 `LegacyPetRepository`，`pet.001` 只创建 feature marker，未提供正式 SQL repository/schema；不能把已完成的 operation ID/provider/replay query边界误报为完整宠物资产迁移，保留 travel/hatch 跨库 transaction blocker并跳转独立路径。

2026-09-16 puppet blocked：`PuppetApplication` 的 purchase/upgrade 仍默认桥接 `LegacyPuppetRepository`，`puppet.001` 只创建 feature marker，未提供正式 SQL repository/schema；已有 operation ID/Clock 边界不等于资产事务迁移，保留 puppet asset transaction legacy边界并跳转独立路径。

2026-09-16 bank blocked：`BankApplication` 的 deposit/withdraw/upgrade/interest 仍默认桥接 `LegacyBankRepository`，当前正式 SQL 仅覆盖 first-use account 子应用，主 bank repository及四类资产事务尚未实现；保留 replay/结息 legacy边界，不做 facade式默认切换。

2026-09-16 bank account request-DDL boundary：`BankAccountRepository.account/operation` 移除 request-time `ensure_schema`，`apply_bank_accounts`成为 first-use bank projection唯一启动前置；新增缺表 focused test。bank/source共194 tests、catalog=104、compileall、architecture、diff check通过。主 `BankApplication` deposit/withdraw/upgrade/interest legacy transaction仍未迁移。

2026-09-16 bank account applications request-DDL boundary：first-use deposit/withdraw/upgrade/interest applications移除请求路径的 `repository.ensure_schema`，统一依赖 `apply_bank_accounts` startup migration；bank/source共194 tests、catalog=104、compileall、architecture、diff check通过。主 bank legacy repository边界保持不变。

2026-09-16 bank account applications request-DDL live safety：提交 `9412c46` 部署后 backup `/srv/old/data/backups/20260915T223241Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行 first-use bank账户、结息或资产写入。

2026-09-16 bank account request-DDL live safety：提交 `65b2e4c` 部署后 backup `/srv/old/data/backups/20260915T221847Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，104-entry recovery clean。live未执行银行账户或结息资产写入。

2026-09-16 world-events blocked：`DemonClaimApplication` 当前仍默认桥接 `LegacyWorldEventClaimRepository`，该 adapter 依赖旧的 game/player ATTACH transaction，尚无正式 SQL claim repository/schema；不做 facade式默认切换，保留 demon attack/claim 跨库事务边界并跳转独立路径。

2026-09-16 auction settlement replay boundary：`AuctionSettlementApplication.lookup` 改为 feature-local 只读查询 `operation_ledger`，缺表返回无记录且不触发 request-time DDL；保留详细 auction settlement 的 legacy adapter，因为尚无正式 SQL settlement repository。auction/trade/source共210 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 auction settlement replay live safety：提交 `fe7ded3` 部署后 backup `/srv/old/data/backups/20260915T202351Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行拍卖结算写入。

2026-09-16 auction bid blocked：`AuctionBidApplication` 当前默认使用 `LegacyTradeRepository`，feature层没有正式 SQL bid repository/schema；auction bid 的 operation ledger边界已存在，但不能将旧 TradeRepository facade误报为完整出价迁移，保留 bid/refund legacy transaction边界并跳转独立路径。

2026-09-15 impart PK operation ID boundary：虚神界训练、机器人/双人对决、探索出关真实入口的无事件 operation ID统一使用注入 `runtime_ids`；impart/source共177 tests、catalog=103、compileall、architecture、diff check通过。对战随机决策、跨玩家 settlement和旧 replay service仍为未迁移边界。

2026-09-15 impart PK operation ID live safety：提交 `ba4364f` 部署后 backup `/srv/old/data/backups/20260915T092945Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行虚神界训练/对战写入。

2026-09-15 impart PK random boundary：虚神界探索的成功率、时长、层级倍率、事件类型和文案选择统一使用注入 `runtime_random`，保留既有概率/消息池顺序；impart/source共177 tests、catalog=103、compileall、architecture、diff check通过。跨玩家 battle settlement与旧 service replay仍为未迁移边界。

2026-09-15 impart PK random live safety：提交 `0f8aba4` 部署后 backup `/srv/old/data/backups/20260915T130526Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行虚神界探索/资产写入。

2026-09-15 impart PK clock boundary：虚神界闭关进入和出关 cooldown 的默认时间读取统一使用注入 `runtime_clock`；impart/source共177 tests、catalog=103、compileall、architecture、diff check通过。跨玩家 battle/closing settlement仍保留为未迁移边界。

2026-09-15 impart PK clock live safety：提交 `cde2291` 部署后 backup `/srv/old/data/backups/20260915T135144Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行虚神界闭关/出关写入。

2026-09-15 lunhui operation ID boundary：轮回 reset、前世技能 recall、三库轮回 settlement和确认邀请真实入口的无事件 operation ID统一使用注入 `runtime_ids`；lunhui/source共151 tests、catalog=103、compileall、architecture、diff check通过。三库 reset/recall/settlement完整事务及旧 replay service仍为未迁移边界。

2026-09-15 lunhui operation ID live safety：提交 `18e18dc` 部署后 backup `/srv/old/data/backups/20260915T100658Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行轮回 reset/recall/settlement写入。

2026-09-15 dongfu operation ID boundary：洞府扩建、种植/收获、巡逻、施肥、加速、拜访、阵法和渗透真实入口的无事件 operation ID统一使用注入 `runtime_ids`；dongfu/source共174 tests、catalog=103、compileall、architecture、diff check通过。资源事务、随机产出和旧 replay service仍保留为未迁移边界。

2026-09-15 dongfu operation ID live safety：提交 `ac7a847` 部署后 backup `/srv/old/data/backups/20260915T101414Z`，dry-run pending为空、reconcile clean；startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行洞府写入。

2026-09-15 dongfu random boundary：洞府种植/收获、巡逻、拜访和渗透的随机产出、检测、偷取和延迟决策统一使用注入 `runtime_random`；dongfu/source共174 tests、catalog=103、compileall、architecture、diff check通过。资源 transaction/replay 与完整 application迁移仍为未迁移边界。

2026-09-15 dongfu random live safety：提交 `21aa08a` 部署后 backup `/srv/old/data/backups/20260915T120222Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行洞府随机产出/资产写入。

2026-09-15 dongfu clock boundary：洞府默认状态/成熟时间读取统一使用注入 `runtime_clock`；dongfu/source共174 tests、catalog=103、compileall、architecture、diff check通过。资源 transaction/replay 与完整 application迁移仍为未迁移边界。

2026-09-15 dongfu clock live safety：提交 `2627bad` 部署后 backup `/srv/old/data/backups/20260915T141942Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行洞府状态/成熟时间写入。

2026-09-15 fusion operation ID boundary：普通合成和强制合成真实 handler的无事件 operation ID统一使用注入 `runtime_ids`；fusion/pill/source共161 tests、catalog=103、compileall、architecture、diff check通过。合成成功率随机决策与历史 FusionService 资产事务仍保留为未迁移边界。

2026-09-15 fusion operation ID live safety：提交 `3025002` 部署后 backup `/srv/old/data/backups/20260915T102231Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行合成写入。

2026-09-15 fusion random boundary：普通/强制合成的成功率决策统一使用注入 `runtime_random`，概率、批量顺序和保护符语义保持不变；fusion/pill/source共161 tests、catalog=103、compileall、architecture、diff check通过。FusionService 资产事务仍为未迁移边界。

2026-09-15 fusion random live safety：提交 `b7e7a58` 部署后 backup `/srv/old/data/backups/20260915T124601Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行合成写入。

2026-09-15 impart operation ID boundary：传承抽卡、祈愿、思恋流沙、卡牌合成/拆解真实入口的无事件 operation ID统一使用注入 `runtime_ids`；impart/source共177 tests、catalog=103、compileall、architecture、diff check通过。抽卡/祈愿/流沙随机与跨库 asset transaction 仍保留为未迁移边界。

2026-09-15 impart operation ID live safety：提交 `ea3eb03` 部署后 backup `/srv/old/data/backups/20260915T103025Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行抽卡/祈愿/合成写入。

2026-09-15 training provider boundary：历练完成/购买真实入口的 operation ID与状态时间统一使用注入 `runtime_ids/runtime_clock`；training/source共170 tests、catalog=103、compileall、architecture、diff check通过。历练事件随机和历史 completion/purchase transaction service仍保留为未迁移边界；admin reset source guard同步匹配现有 `spawn_admin_job` 可恢复调度。

2026-09-15 training provider live safety：提交 `2054116` 部署后 backup `/srv/old/data/backups/20260915T104141Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行历练/购买/reset写入。

2026-09-15 bg jobs ID boundary：后台任务缺省 job key 从 `time.time_ns()` 改为注入 `UUIDGenerator`；显式 key 去重、`time.monotonic()`运行计时和 chunked worker语义保持不变。training-reset/source共149 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 bg jobs ID live safety：提交 `6534ccb` 部署后 backup `/srv/old/data/backups/20260915T123428Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行后台任务。

2026-09-15 past-life clock boundary：PastLifeLimit 的刷新窗口、可用时间和冷却剩余计算支持注入 `Clock`，默认使用 `SystemClock`；past-life/source共174 tests、catalog=103、compileall、architecture、diff check通过。三库 past-life transaction service 仍为未迁移边界。

2026-09-15 past-life clock live safety：提交 `3c4ee2f` 部署后 backup `/srv/old/data/backups/20260915T125640Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行前尘状态写入。

2026-09-15 rift clock boundary：秘境 elapsed-time、scheduled generation slot和默认 UTC兼容时间统一使用注入 `runtime_clock`；rift/source共190 tests、catalog=103、compileall、architecture、diff check通过。deterministic RNG与跨库 settlement仍保留为未迁移边界。

2026-09-15 rift lifecycle clock live safety：提交 `84f2282` 部署后 backup `/srv/old/data/backups/20260915T133320Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行秘境生命周期写入。

2026-09-15 boss reset clock boundary：世界BOSS每日额度重置默认 business date统一使用注入 `runtime_clock`；world-boss/boss/source共188 tests、catalog=103、compileall、architecture、diff check通过。完整 world-boss 三库 settlement 仍为明确阻塞边界。

2026-09-15 boss reset clock live safety：提交 `08445f9` 部署后 backup `/srv/old/data/backups/20260915T134412Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行世界BOSS额度重置。

2026-09-15 boss random boundary：世界BOSS掉落概率、掉落物选择和战斗奖励掉落判定统一使用注入 `runtime_random`；world-boss/boss/source共188 tests、catalog=103、compileall、architecture、diff check通过。完整 world-boss 三库 settlement仍为明确阻塞边界。

2026-09-15 boss random live safety：提交 `eca121a` 部署后 backup `/srv/old/data/backups/20260915T150625Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行世界BOSS奖励写入。

2026-09-15 boss settlement clock boundary：world-boss composite settlement handler的当前时间统一使用注入 `runtime_clock`；world-boss/boss/source共188 tests、catalog=103、compileall、architecture、diff check通过。activity/game/player 三库 settlement repository仍为明确阻塞边界。

2026-09-15 boss settlement clock live safety：提交 `7df291e` 部署后 backup `/srv/old/data/backups/20260915T153023Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行 world-boss settlement 写入。

2026-09-15 web economy clock boundary：经济日志只读 quick preset 的默认时间范围统一使用注入 `runtime_clock`；source共143 tests、catalog=103、compileall、architecture、diff check通过，仓库暂无专用 economy web behavior suite。web写入/导出路径保持未迁移边界。

2026-09-15 web economy clock live safety：提交 `314970e` 部署后 backup `/srv/old/data/backups/20260915T154019Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行经济日志查询写入。

2026-09-15 web core clock boundary：上传文件名、消息自动回复时间过滤和消息有效期判断统一使用注入 `runtime_clock`；naive persisted timestamp与aware UTC clock比较显式归一化。source共143 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 web core clock live safety：提交 `ddeb7c5` 部署后 backup `/srv/old/data/backups/20260915T155051Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行上传/消息数据写入。

2026-09-15 web messages clock boundary：消息统计查询默认日期统一使用 `core.runtime_clock`；source共143 tests、catalog=103、compileall、architecture、diff check通过。web commands 管理员饰品/物品直接 SQL 写入仍为未迁移阻塞边界。

2026-09-15 web messages clock live safety：提交 `5c128fa` 部署后 backup `/srv/old/data/backups/20260915T155919Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行消息统计/管理写入。

2026-09-16 web backups clock boundary：配置导出/备份文件名和元数据时间统一使用 `core.runtime_clock`；source共143 tests、catalog=103、compileall、architecture、diff check通过。管理员配置写入/导出仍保留既有 web compatibility 边界。

2026-09-16 web backups clock live safety：提交 `08a657c` 部署后 backup `/srv/old/data/backups/20260915T161215Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行配置导出/备份写入。

2026-09-16 web logs clock boundary：无年份日志记录的当前年份解析统一使用 `core.runtime_clock`；source共143 tests、catalog=103、compileall、architecture、diff check通过。日志写入和管理查询的 legacy web边界保持不变。

2026-09-16 web logs clock live safety：提交 `c93dfb6` 部署后 backup `/srv/old/data/backups/20260915T161728Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行日志写入/管理变更。

2026-09-16 message sequence random boundary：`MessageSequenceStrategy` 的初始序号、递增步长和上限回绕统一使用注入 `RandomSource`，默认 `SystemRandom`；新增2个 focused tests，source总145 tests、catalog=103、compileall、architecture、diff check通过。消息投递重试/外部 adapter边界未改变。

2026-09-16 message sequence random live safety：提交 `ffe0c9b` 部署后 backup `/srv/old/data/backups/20260915T163945Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未发送消息或改变外部 adapter状态。

2026-09-16 entertainment guess-number clock/random boundary：猜数字内存会话的答案生成统一使用注入 `runtime_random`，会话时间统一使用 `runtime_clock`；新增1个 `now_text` focused test，entertainment/source共149 tests、catalog=103、compileall、architecture、diff check通过。内存 session 与 asyncio timeout 仍保留现有边界。

2026-09-16 entertainment guess-number live safety：提交 `ec06c2e` 部署后 backup `/srv/old/data/backups/20260915T170604Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未启动游戏会话或发送娱乐消息。

2026-09-16 entertainment guess-number puzzle providers：猜数谜答案生成、鼓励文案和内存会话时间统一使用注入 `runtime_random/runtime_clock`；新增2个 focused tests，entertainment/source共151 tests、catalog=103、compileall、architecture、diff check通过。内存 session 与 asyncio timeout 仍保留现有边界。

2026-09-16 entertainment guess-number puzzle live safety：提交 `d510b39` 部署后 backup `/srv/old/data/backups/20260915T171314Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未启动猜数谜会话或发送娱乐消息。

2026-09-16 entertainment half-ten providers：十点半房间 ID、创建/结算时间和洗牌统一使用注入 `runtime_ids/runtime_clock/runtime_random`；新增1个 focused test，entertainment/source共152 tests、catalog=103、compileall、architecture、diff check通过。JSON 房间持久化和 asyncio timeout 仍保留现有边界。

2026-09-16 entertainment half-ten live safety：提交 `d3c8a81` 部署后 backup `/srv/old/data/backups/20260915T172204Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未创建房间、发牌或发送娱乐消息。

2026-09-16 entertainment minesweeper providers：扫雷房间 ID、棋局时间和首次点击后的埋雷洗牌统一使用 `runtime_ids/runtime_clock/runtime_random`；新增1个 focused test，entertainment/source共150 tests、catalog=103、compileall、architecture、diff check通过。JSON 持久化、图片渲染和 asyncio timeout 仍保留现有边界。

2026-09-16 entertainment minesweeper live safety：提交 `38aca5e` 部署后 backup `/srv/old/data/backups/20260915T172827Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未创建棋局、发雷或发送娱乐消息。

2026-09-16 entertainment gomoku providers：五子棋房间 ID、创建/开局/落子时间和超时判断统一使用 `runtime_ids/runtime_clock`；entertainment/source共151 tests、catalog=103、compileall、architecture、diff check通过。JSON 房间持久化、AI落子、图片渲染和 asyncio timeout 仍保留现有边界。

2026-09-16 entertainment gomoku live safety：提交 `1a71679` 部署后 backup `/srv/old/data/backups/20260915T173557Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未创建棋局、落子或发送娱乐消息。

2026-09-16 entertainment random-voice provider：随机语音类型选择统一使用注入 `runtime_random`；新增1个不触发外部 API 的 focused test，entertainment/source共150 tests、catalog=103、compileall、architecture、diff check通过。外部语音 API 与真实消息发送保留现有边界。

2026-09-16 entertainment random-voice live safety：提交 `fc0ef32` 部署后 backup `/srv/old/data/backups/20260915T174135Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未调用外部语音 API或发送消息。

2026-09-16 entertainment pokemon-box provider：Pokemon 随机查询 ID统一使用注入 `runtime_random`，新增1个不触发 PokeAPI 的 focused test；entertainment/source共150 tests、catalog=103、compileall、architecture、diff check通过。外部 PokeAPI、图片和真实消息发送保留现有边界。

2026-09-16 entertainment pokemon-box live safety：提交 `e058673` 部署后 backup `/srv/old/data/backups/20260915T174740Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未调用 PokeAPI、图片或发送消息。

2026-09-16 entertainment media random providers：anime reaction 默认类别选择和 random-girl video provider顺序统一使用 `runtime_random`，娱乐模块负向 global-random扫描为空；新增1个 focused test，source共150 tests、catalog=103、compileall、architecture、diff check通过。外部图片/视频 API 与真实媒体发送保留现有边界。

2026-09-16 entertainment media random live safety：提交 `f95c060` 部署后 backup `/srv/old/data/backups/20260915T175554Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未调用外部图片/视频 API或发送媒体。

2026-09-16 adapter sender sequence boundary：低层 QQ group/private sender 的缺省 msg_seq 统一复用注入式 `MessageSequenceStrategy`，显式 msg_seq、引用消息和 adapter API 参数保持不变；新增 focused tests后共145 tests、catalog=103、compileall、architecture、diff check通过。真实发送/外部 adapter副作用未执行。

2026-09-16 adapter sender sequence live safety：提交 `8d37eb8` 部署后 backup `/srv/old/data/backups/20260915T165649Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未发送消息或改变外部 adapter状态。

2026-09-15 world-events random boundary：恶魔生成、随机奖励、灵脉时长/触发与攻击领奖随机决策统一使用注入 `runtime_random`，时间默认使用 `runtime_clock`；demon/source共166 tests、catalog=103、compileall、architecture、diff check通过。复杂 replay与多库 settlement仍为未迁移边界。

2026-09-15 world-events random live safety：提交 `06dbcfe` 部署后 backup `/srv/old/data/backups/20260915T140006Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行恶魔/灵脉随机奖励写入。

2026-09-15 dufang random boundary：鉴石共享事件、封印物、收益/损失倍率和奖励随机选择统一使用注入 `runtime_random`；dufang/source共153 tests、catalog=103、compileall、architecture、diff check通过。bet/payout/share composite transaction仍由 legacy repository边界承载。

2026-09-15 dufang random live safety：提交 `f0e5c6f` 部署后 backup `/srv/old/data/backups/20260915T140956Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行鉴石共享/封印奖励写入。

2026-09-15 back operation ID boundary：装备操作、背包修复、灵石/物品/炼金/抽奖符/礼包/天梯奖励和批量物品使用 helper的无事件 operation ID统一使用注入 `runtime_ids`；back/source共151 tests、catalog=103、compileall、architecture、diff check通过。legacy inventory、alchemy和奖励 transaction service仍保留为未迁移边界。

2026-09-15 back invite ID boundary：技能确认缓存 invite ID从时间戳改为注入 `runtime_ids`，30秒 `asyncio.sleep` 过期机制保持不变；accessory/back/source共190 tests、catalog=103、compileall、architecture、diff check通过。技能学习 asset transaction仍保留既有 legacy service边界。

2026-09-15 back invite ID live safety：提交 `fb1a3bc` 部署后 backup `/srv/old/data/backups/20260915T143931Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行技能确认/资产写入。

2026-09-15 back operation ID live safety：提交 `7a1880e` 部署后 backup `/srv/old/data/backups/20260915T105238Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行背包/炼金/奖励写入。

2026-09-15 task operation ID boundary：任务奖励真实 handler的无事件 operation ID统一使用注入 `runtime_ids`，领取请求继续经 `TasksApplication.execute`；task/effects/source共151 tests、catalog=103、compileall、architecture、diff check通过。动态任务 snapshot、reward parsing和跨库 reward transaction仍为未迁移边界。

2026-09-15 task operation ID live safety：提交 `f1f7eaf` 部署后 backup `/srv/old/data/backups/20260915T122334Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行任务奖励写入。

2026-09-15 puppet operation ID boundary：灵田傀儡购买、升级、开关和 harvest 真实入口的无事件 operation ID统一使用注入 `runtime_ids`；puppet/source共153 tests、catalog=103、compileall、architecture、diff check通过。傀儡 harvest 与 legacy repository 事务仍保留为未迁移边界。

2026-09-15 puppet operation ID live safety：提交 `df950ed` 部署后 backup `/srv/old/data/backups/20260915T110230Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行傀儡购买/升级/harvest写入。

2026-09-15 puppet clock boundary：傀儡 harvest 的当前时间和灵田状态展示统一使用注入 `runtime_clock`；puppet/source共153 tests、catalog=103、compileall、architecture、diff check通过。harvest legacy transaction service仍为未迁移边界。

2026-09-15 puppet clock live safety：提交 `9e20c67` 部署后 backup `/srv/old/data/backups/20260915T144718Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行傀儡 harvest/状态写入。

2026-09-15 accessory operation ID boundary：饰品真实操作 helper的无事件 operation ID统一使用注入 `runtime_ids`；accessory相关测试/source共190 tests、catalog=103、compileall、architecture、diff check通过。饰品跨 game/player transaction service仍保留为未迁移边界。

2026-09-15 accessory operation ID live safety：提交 `b7bfcc9` 部署后 backup `/srv/old/data/backups/20260915T111211Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行饰品写入。

2026-09-15 status operation ID boundary：版本更新真实入口的无事件 operation ID统一使用注入 `runtime_ids`，外部 updater/backup副作用仍经 `StatusApplication` ledger wrapper；status/admin/source共165 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 status operation ID live safety：提交 `11da77a` 部署后 backup `/srv/old/data/backups/20260915T115406Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行版本更新副作用。

2026-09-15 auction start provider boundary：拍卖启动真实流程的 operation ID、系统拍卖品抽样、session开始时间和自动日期统一使用注入 `runtime_ids/runtime_random/runtime_clock`；trade/auction/source共209 tests、catalog=103、compileall、architecture、diff check通过。拍卖结束、排队和库存 transaction 的历史时间逻辑仍保留为未迁移边界。

2026-09-15 auction start provider live safety：提交 `d06788a` 部署后 backup `/srv/old/data/backups/20260915T094455Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行拍卖启动/商品写入。

2026-09-15 auction end/reconcile clock boundary：拍卖结束使用注入 `runtime_clock` 的 epoch 时间，重启对账将 session epoch 转为同一 UTC aware 时区；trade/auction/source共209 tests、catalog=103、compileall、architecture、diff check通过。queue/repository历史 asset transaction 仍保留为未迁移边界。

2026-09-15 auction lifecycle clock live safety：提交 `4ac208d` 部署后 backup `/srv/old/data/backups/20260915T152330Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行拍卖结束/对账写入。

2026-09-15 illusion operation ID boundary：幻境心境试炼真实入口的无事件 operation ID统一使用注入 `runtime_ids`，choice结果查询与写入继续经 `IllusionApplication`；illusion/data/source共153 tests、catalog=103、compileall、architecture、diff check通过。旧 `IllusionChoiceService` 仅保留兼容边界。

2026-09-15 illusion operation ID live safety：提交 `a48e9d1` 部署后 backup `/srv/old/data/backups/20260915T095343Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行幻境选择写入。

2026-09-16 illusion domain clock boundary：`period_key` 默认时间统一使用 `SystemClock`，`IllusionApplication` 显式 Clock 仍优先；illusion/source共153 tests、catalog=103、compileall、architecture、diff check通过。旧 choice transaction compatibility边界保持不变。

2026-09-16 illusion domain clock live safety：提交 `9c31a4a` 部署后 backup `/srv/old/data/backups/20260915T162303Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行幻境选择写入。

2026-09-16 illusion request-DDL boundary：`IllusionRepository.get_result/get_choice/choose` 移除 request-time `ensure_schema`，`illusion.001` migration成为 choices/operations schema启动前置；更新 legacy choice service fixture为显式 migration。illusion/source共153 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-16 illusion request-DDL live safety：提交 `254e339` 部署后 backup `/srv/old/data/backups/20260915T211831Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行幻境选择写入。

2026-09-15 rift settlement operation ID boundary：秘境结算、终止、钥匙事件、斩妖令战斗和加速真实入口的无事件 operation ID统一使用注入 `runtime_ids`；rift/source共190 tests、catalog=103、compileall、architecture、diff check通过。完整事件随机及跨库 settlement仍保留为未迁移边界。

2026-09-15 rift settlement operation ID live safety：提交 `5891aff` 部署后 backup `/srv/old/data/backups/20260915T095921Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行秘境结算/终止/加速写入。

2026-09-15 avatar provider boundary：身外化身真实入口的 operation ID、随机化身ID和创建时间统一使用注入 `runtime_ids/runtime_random/runtime_clock`，兼容 JSON mutation仍经 `InfoApplication` ledger；source共143 tests、catalog=103、compileall、architecture、diff check通过，仓库暂无专用 avatar behavior suite。

2026-09-15 avatar provider live safety：提交 `7ae1397` 部署后 backup `/srv/old/data/backups/20260915T083238Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行身外化身切换写入。

2026-09-15 sign-in task projection DDL fix：`SignInTaskRepository.record`移除 request-time `ensure_schema`，正式 schema仅由已有 `apply_sign_in_tasks` startup migration创建；测试fixture改为显式 migration后验证 daily/weekly progress idempotency。task/effects/source共149 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-15 sign-in task projection live safety：提交 `d845283` 部署后 backup `/srv/old/data/backups/20260915T001441Z`，dry-run pending为空、reconcile clean；真实 startup `phase=ready` 六项全绿，103-entry recovery clean。live未执行任务进度写入。

2026-09-15 sign-in task side-effect boundary：task/lottery adapters已存在且通过 wiring tests，但真实 `xiuxian_base` handler模块级 `sign_in_application` 与 runtime context service实例尚未共享同一 `SignInApplicationEffects`，直接移除 handler中的 legacy statistics/task calls会改变行为；该边界记录为阻塞，未伪迁移。转入独立 pet travel claim slice。

2026-09-15 pet travel claim cutover：真实宠物游历领奖handler改用 `PetApplication.claim_travel`，保留现有 prepare/story rendering和显式 statistics/game-event side-effect边界；旧 `PetTravelClaimService.claim`不再是默认handler调用。pet claim/source共149 tests、catalog=103、compileall、architecture、diff check通过；pet.001已有schema无需新增migration。

2026-09-15 pet travel claim live safety：提交 `a4bc8c9` 部署后 backup `/srv/old/data/backups/20260914T231749Z`，dry-run无pending、reconcile clean、readiness全绿，103-entry recovery clean。live未执行宠物游历奖励写入。

2026-09-15 pet travel start cutover：真实宠物游历开始handler改用 `PetApplication.start_travel`，fallback operation ID使用稳定 pet UID，旧 `PetTravelStartService.start`不再是默认调用；pet start/claim/source共152 tests通过，现有pet.001 schema复用。

2026-09-15 pet feed cutover：真实宠物喂养handler改用 `PetApplication.feed`，operation ID fallback改用 active pet UID，旧 `PetFeedService.feed`不再是默认调用；pet travel/feed/source共152 tests，catalog=103，compileall、architecture、diff check通过。

2026-09-15 pet hatch boundary：`PetApplication.hatch`已有跨库应用边界，但真实 hatch handler仍依赖旧 DTO字段（pets/updated_meta/pity）且尚未完成安全适配；未伪迁移，作为下一独立slice。当前 pet feed/start/claim已切换并验证。

2026-09-15 pet hatch investigation boundary：完整旧 hatch 事务还包含 game/player attach、legacy `player_pet` schema兼容、pity metadata CAS、预滚宠物批次及 result replay；当前 application/repository尚未拥有等价SQL实现。保持旧 hatch service为显式未迁移边界，未包装为伪application cutover。

2026-09-15 pet hatch replay cutover：hatch handler的已完成operation读取改用 `PetApplication.hatch_result`，operation fallback改用稳定 user ID；复杂跨库 hatch 写事务仍由旧 service作为显式未迁移边界。pet hatch/travel/source共157 tests、catalog=103、compileall、architecture、diff check通过。

2026-09-14 dao battle settlement live safety：提交 `f9129fc` 部署后 backup `/srv/old/data/backups/20260914T061542Z`；dry-run 真实返回 game_db `[]`、player_db `[combat_settlement.003]`，apply 仅写入 player_db。game_db migrations=67、player_db migrations=6，`map_dao_battle_operations` 存在，readiness 全绿；recovery smoke 覆盖 67-entry catalog，reconcile `clean=true/operations=0/outbox_events=0/dead_events=0`。live 未执行玩家道战写入。

2026-09-14 bank interest boundary live safety：提交 `34e0177` 部署时各 interest/upgrade/withdrawal first-use flag 默认关闭；真实 bank audit 仍 `bankinfo=false`、`operation_ledgers={}`、`read_only=true`，backup `/srv/old/data/backups/20260913T160333Z`、dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。

2026-09-13 bank first-use command boundary live safety：提交 `852cdfb` 部署后默认灰度仍关闭，真实 bank audit `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。该证据仅证明新 parser/first-use code 不改变旧 bank runtime；旧 NoneBot handler 仍是真实命令路径。

2026-09-13 bank first-use NoneBot boundary：新增 `register_bank_first_use_matcher`，只注册显式灰度命令 `灵庄新存灵石`，在 startup 完成且 `bank_first_use` service 注入后才安装；使用新 parser、Clock、operation_id 和 `BankDepositApplication`，旧 `灵庄` matcher 不变。新增 bank first-use command/application/Web/migration 聚焦测试共 7 个通过，compileall、inventory、architecture、diff 通过；默认灰度关闭，未把该 opt-in matcher 当作旧命令切换完成。

2026-09-13 bank first-use NoneBot live safety：提交 `cefac77` 部署时未设置 `XIUXIAN_BANK_FIRST_USE_ENABLED`，新 `灵庄新存灵石` matcher 未注入，旧 bank audit 仍 `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。该证据确认 opt-in matcher 默认不影响旧命令执行路径。

2026-09-13 bank first-use flag decision test：新增 `features/bank/feature_flag.py::bank_first_use_enabled`，将默认关闭/显式开启决策从 composition root 提取为可测试函数；默认 `None/{}` 为 false，显式 true 才开启。bank flag/command/application/Web/migration 共 9 个测试通过，compileall、inventory、architecture、diff 通过。该测试只锁定灰度决策，不改变默认关闭和旧 bank command 执行路径。

2026-09-13 bank first-use flag decision live safety：提交 `0c4208c` 部署时默认配置未开启 flag，真实 bank audit `bankinfo=false`、`operation_ledgers={}`、`read_only=true`；migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 55 个迁移、五库 restore，恢复后 bank audit 不变。该证据确认 flag decision helper 和 composition 接线默认不会注入新 matcher/service。

2026-09-13 attached migration drift gate live verification：提交 `e36a62f` 部署后，真实 audit JSON 输出 `migration.applied=false`、`checksum_valid=null`、`tables=[]`、`accessory_rows=0`、`read_only=true`；backup `/srv/old/data/backups/20260913T100509Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，恢复后 audit 仍为 `checksum_valid=null`。该证据确认未迁移状态被机器准确区分，未执行任何 live schema migration。

2026-09-13 attached migration ledger live verification：提交 `0e64366` 部署后，真实 `/srv/old/data/player.db` 只读确认 `attached_schema_migrations=0`、`player_accessory=0`，证明 ledger scaffold 未在启动时隐式执行；backup `/srv/old/data/backups/20260913T094512Z`、migration dry-run `pending=[]`、reconcile clean、readiness 全绿；恢复 smoke 覆盖 54 个迁移、五库 restore，reconcile clean，恢复后 readiness 全绿。该证据只证明 ledger 部署不改 live namespace；真实 migration 仍需明确的运维窗口、备份、namespace 对账和回滚演练。

2026-09-13 stone-gift 旧实现隔离：提交 `826ff4e` 将约 7,156 bytes、约 188 行的 `StoneGiftService` 从 `xiuxian_base/transaction_service.py` 删除，完整回滚实现移动到 `compatibility/legacy_stone_gift.py`；compatibility facade 和旧对照测试已改为显式引用该模块。真实 live 验证使用 `/srv/old/data`：备份 `/srv/old/data/backups/20260913T064308Z` 成功，migration dry-run `pending=[]`，reconcile clean，启动后的 readiness 全绿；随后停止实例执行 `recovery_smoke.py --evidence`，覆盖 53 个迁移和五库 restore，reconcile clean，恢复后实例 readiness 仍全绿。该切片的旧实现已不再位于大 transaction service，但 compatibility-only 回滚代码仍保留，不能把它等同于全仓兼容层删除。

2026-09-13 量化审计脚本：`scripts/check_full_refactor_progress.py --json` 输出当前计数与切片状态，确认 `stone_gift`、`sign_in` 的默认新入口均为 true，但 `old_service_removed=false`；报告 `exit_ready=false`，阻塞项明确包含旧 transaction service、`xiuxian2_handle`、sign-in 副作用和完整 driver 重复 prefix 快照。该脚本是进度证据，不是静态“完成”替代。

2026-09-13 NoneBot 注册快照：全新 Python 进程只执行 `nonebot.init(); import nonebot_plugin_xiuxian_2`，`scripts/snapshot_nonebot_registrations.py` 输出 9 个迁移命令/别名各恰好 1 个 matcher，且全部来自 `nonebot_plugin_xiuxian_2.adapters.nonebot.commands`；未发现旧 `xiuxian_base` matcher。此前重复 prefix 警告来自导入完成后再次手动注册 matcher 的测试探针，不是干净生产启动结果。

## 6. 下一步

先把 `stone_gift` 的真实 handler 从旧模块迁移到 `features/stone_gift/commands.py`，通过新 application 的 DTO/operation_id/Clock 路径；随后补真实 NoneBot 注册测试、删除旧 `StoneGiftService` 执行实现，并在真实部署数据上执行 dry-run/reconcile/恢复。若该切片遇到旧数据兼容阻塞，继续处理不依赖它的 player/economy 小切片，不把 facade 或静态 manifest 计入完成。