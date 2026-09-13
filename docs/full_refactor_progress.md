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