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

2026-09-13 stone-gift 旧实现隔离：提交 `826ff4e` 将约 7,156 bytes、约 188 行的 `StoneGiftService` 从 `xiuxian_base/transaction_service.py` 删除，完整回滚实现移动到 `compatibility/legacy_stone_gift.py`；compatibility facade 和旧对照测试已改为显式引用该模块。真实 live 验证使用 `/srv/old/data`：备份 `/srv/old/data/backups/20260913T064308Z` 成功，migration dry-run `pending=[]`，reconcile clean，启动后的 readiness 全绿；随后停止实例执行 `recovery_smoke.py --evidence`，覆盖 53 个迁移和五库 restore，reconcile clean，恢复后实例 readiness 仍全绿。该切片的旧实现已不再位于大 transaction service，但 compatibility-only 回滚代码仍保留，不能把它等同于全仓兼容层删除。

2026-09-13 量化审计脚本：`scripts/check_full_refactor_progress.py --json` 输出当前计数与切片状态，确认 `stone_gift`、`sign_in` 的默认新入口均为 true，但 `old_service_removed=false`；报告 `exit_ready=false`，阻塞项明确包含旧 transaction service、`xiuxian2_handle`、sign-in 副作用和完整 driver 重复 prefix 快照。该脚本是进度证据，不是静态“完成”替代。

2026-09-13 NoneBot 注册快照：全新 Python 进程只执行 `nonebot.init(); import nonebot_plugin_xiuxian_2`，`scripts/snapshot_nonebot_registrations.py` 输出 9 个迁移命令/别名各恰好 1 个 matcher，且全部来自 `nonebot_plugin_xiuxian_2.adapters.nonebot.commands`；未发现旧 `xiuxian_base` matcher。此前重复 prefix 警告来自导入完成后再次手动注册 matcher 的测试探针，不是干净生产启动结果。

## 6. 下一步

先把 `stone_gift` 的真实 handler 从旧模块迁移到 `features/stone_gift/commands.py`，通过新 application 的 DTO/operation_id/Clock 路径；随后补真实 NoneBot 注册测试、删除旧 `StoneGiftService` 执行实现，并在真实部署数据上执行 dry-run/reconcile/恢复。若该切片遇到旧数据兼容阻塞，继续处理不依赖它的 player/economy 小切片，不把 facade 或静态 manifest 计入完成。