# 重构切片执行与磁盘控制协议

状态：执行中
适用范围：全面底层重构第二阶段

## 目标

将重构拆成边界清晰的单功能切片。一个切片完成并通过验收后，先清理本轮产生的测试缓存和临时输出，再开始下一个切片，避免多个功能和多轮测试的产物同时占用磁盘。

## 单切片闭环

1. 读取进度、架构约束、当前工作树和磁盘使用情况。
2. 只选择一个真实未完成的 handler/route/application 边界。
3. 追踪旧入口和数据路径，先建立可失败的 focused 回归。
4. 按 domain/application/repository/migration/adapter 的实际边界实现，不在请求路径隐式建表。
5. 运行 focused tests、compileall、architecture、inventory 和 `git diff --check`。
6. 在显式临时数据目录执行 backup、migration dry-run/apply、readiness、reconcile 和 recovery smoke；不得触碰仓库 `data/` 或生产运行数据。
7. 更新进度文档，记录旧路径、新路径、测试、迁移路由、回滚点和未完成边界。
8. 验收成功后，清理本轮明确产生的 pytest basetemp、pytest cache、Python 字节码缓存和临时 receipt/log；清理范围不得包含 `.venv`、`.git`、`data/`、配置、备份或运行数据。
9. 重新检查 `df -hT`、`df -ih`、仓库和临时目录大小，确认工作树与运行数据未被误删。
10. 只有清理和磁盘复核完成后，才读取并执行下一个切片。

## 缓存清理允许范围

- 仓库内未跟踪的 `__pycache__/`、`*.pyc`、`.pytest_cache/`。
- 当前切片专用的 `/tmp/<slice>-pytest*`、`/tmp/<slice>-*` receipt 和日志目录。
- 已确认属于本轮测试的旧临时 smoke 目录。

以下目录默认保留：

- `/home/nonebot_plugin_xiuxian_2_pmv/.venv`
- `/home/nonebot_plugin_xiuxian_2_pmv/.git`
- `/home/nonebot_plugin_xiuxian_2_pmv/data`
- `.env`、配置文件、数据库、备份和任何运行态目录
- 无法确认所有权或用途的 `/tmp` 内容

## 最近完成切片

`mixelixir two-phase refine claim`：真实 `配方` handler 的扣材、补领查询和奖励领取统一经
`MixelixirApplication -> MixelixirRefineCostSqlRepository/MixelixirRefineRewardSqlRepository`。扣材时校验
每日次数、材料和丹炉并保存完整奖励及炼丹状态快照；领取时 CAS 校验修为状态和背包容量，在 attached UoW
中发放丹药、更新 `mix_elixir_info`、增加炼丹统计、完成任务并写入幂等结果。新增 game DB `mixelixir.002`
和 player DB `mixelixir.003`，请求路径不再建表；旧 transaction services 留作显式兼容对照。炼丹 feature/兼容/source
回归（含真实注册 matcher 端到端）`26 passed, 2 subtests`，compileall、architecture、inventory、progress 和 diff
check 通过。五库 recovery 完成 `158` 项迁移，路由命中数 `126/28/7/1/1`；backup/restore dry-run/restore、health
六项 readiness、migration dry-run（五库 pending 为空）、reconcile clean（operations/outbox/dead events 均为 `0`）。
专用 basetemp、recovery 数据、receipt 和字节码缓存已清理。

`work item accelerate`：真实注册 `道具使用` matcher 的 `20014` 分派改经
`WorkItemUseApplication -> WorkItemUseSqlRepository`；以库存数量、绑定数量和 `user_cd` 快照作 CAS，
事务内扣除一个悬赏令、把开始时间设为立即可结算并写入幂等操作结果。新增 game DB `work.003`，
加速请求路径不再建表。追捕令 `20015` 的随机 offer 生成和快照仍通过 `WorkItemUseService.capture`，
明确作为独立兼容边界。application/旧 service/source/registered matcher 聚焦套件 `256 passed`，compileall、
architecture、progress、inventory 与 `git diff --check` 通过。隔离五库 recovery 完成 `159` 项 migration，
路由计数 `127/28/7/1/1`；backup/restore dry-run/restore、五库 migration dry-run（pending 为空）、health 六项
readiness 全绿、reconcile clean（operations/outbox/dead events 均为 `0`）。专用 pytest basetemp、恢复数据、receipt
和 compileall 字节码缓存已清理；`.venv`、`.git`、`data/` 和运行数据保留。

`buff partner cultivation`：`xiuxian_buff.partner::direct_two_exp` 默认结算改为调用
`PartnerCultivationApplication -> PartnerCultivationSqlRepository`；修为/属性、双修次数、统计、亲密度、邀请接受与 operation ledger 在 attached UoW 内提交。新增 game DB `buff.004` 和 player DB `buff.005`，次数读取保留旧 JSON 一次性导入但不再请求时建表；非邀请 operation payload 与旧 ledger 保持兼容。聚焦回归覆盖回放、冲突、过期/保护、超大数、快照拒绝和跨库异常回滚；旧 `PartnerCultivationService` 仅留作兼容对照。
focused/source/progress/architecture/inventory 回归 `255 passed, 4 subtests`；compileall、architecture、inventory、progress 和 diff check 均通过。五库 recovery 完成 `157` 项迁移，`.004` 仅 game、`.005` 仅 player；backup/restore dry-run/restore、migration dry-run（五库 pending 为空）、health 六项和 reconcile clean 均通过，operations/outbox/dead events 为 `0`。根目录全量测试已启动但未完成：`1950 passed` 后有 5 个与本切片无关的 blessed-flag legacy service `TypeError`，并停在 legacy sign-in startup 测试；该进程已中断。指定 basetemp、recovery 数据、receipt 和源码字节码缓存均已清理。

`buff partner-token`：`道具使用 双修令牌` 默认 handler 调用
`PartnerTokenUseApplication -> PartnerTokenUseSqlRepository`；`buff.002` 在 game DB 创建 operation 表，
`buff.003` 在 player DB 创建次数 projection，跨库写入使用 attached UoW，不在请求时建表。旧
`PartnerTokenUseService` 保留作兼容对照，但不再被默认 handler 调用。聚焦测试 `10 passed`，五库 recovery
`154` 项、readiness 六项全绿；恢复数据、receipt 和缓存均已清理。

`work item capture`：真实注册 `道具使用` matcher 的 `20015` 追捕令扣除和 offer 持久化改经
`WorkItemUseApplication -> WorkItemUseSqlRepository`；随机 offer 与首次奖励倍率写入 game DB 并与背包扣减、幂等结果同事务提交，
同 operation 重放返回首个 offer 和倍率。新增 `work.004`，以 `CREATE TABLE IF NOT EXISTS` 建立兼容 snapshot 表并保留已有行；
成功后旧 JSON 只作展示投影，旧 service 留作兼容对照。聚焦 application/旧 service/真实注册 matcher/source/progress 测试
`235 passed`；compileall、architecture、inventory、progress 和 diff check 通过。隔离五库 recovery 完成 `160` 项 migration，
路由 `128/28/7/1/1`，`work.004` 仅 game DB；backup/restore dry-run/restore、五库 migration dry-run（pending 为空）、
health 六项全绿和 reconcile clean 均通过，operations/outbox/dead events 为 `0`。本轮临时测试、recovery、receipt 和字节码缓存
清理并复核后，下一步只做 6.2 目标 5 的单项真实入口审计。

`rift demon-token battle`：真实注册的 `道具使用` matcher 对 `20018` 的 replay/结算改走
`RiftApplication -> RiftDemonTokenBattleSqlRepository`；outcome 保持 handler 预滚，repository 在 attached game/player UoW
内提交道具、战斗资产、奖励、探索次数、统计与秘境状态。旧 operation payload 和 replay 兼容；新增 game `rift.002` 与 player
`rift.003` schema migration，请求路径不建表/补列。application、旧 service 互操作、真实 matcher、裂隙相邻/source/progress 回归
`71 passed`；compileall、architecture、inventory、progress 和 diff check 通过。五库 recovery 完成 `162` 项迁移，路由
`129/29/7/1/1`；backup/restore dry-run/restore、migration dry-run（pending 为空）、health 六项和 reconcile clean 均通过。
专用测试、recovery、receipt 和字节码缓存已清理，资源复核后仍有约 `24 GB` 磁盘和 `1.4 GB` 可用 RAM。

`impart prayer`：`20005` 真实 handler 在 adapter 预滚卡片后调用
`ImpartApplication.prayer_settle -> ImpartPrayerSqlRepository`，在 attached game/impart/player UoW 中提交道具、卡片、加成、统计和 replay；
game `impart.002`、player `impart.003` 启动迁移准备 schema，请求不建表。聚焦回归 `39 passed`；五库 recovery `169` 项，路由
`130/30/7/1/1`，reconcile clean。未做真实 live/P7 或根目录全量回归，专属 recovery 数据与 receipt 已清理。

`arena challenge-ticket legacy service removal`：真实挑战券 matcher 已使用
`ArenaApplication.use_challenge_ticket -> ArenaChallengePurchaseSqlRepository`；移除只剩旧测试和被 SQL 子类覆盖的
`LegacyArenaRepository` 回退调用的 `ArenaChallengeTicketService`，保留 adapter 结果 DTO。`arena.004` game DB 启动迁移不变，
请求不执行 DDL。matcher/application/repository/事务聚焦回归 `11 passed`；compileall、architecture、progress、inventory 和
diff check 通过。五库 recovery 完成 `164` 项 migration，路由 `130/30/7/1/1`，backup/restore dry-run/restore 与 reconcile clean。
本切片未做 live/P7 或根目录全量回归；专用测试、恢复数据、receipt 和字节码缓存已清理，保留运行数据与 Boss JSON 用户改动。

`impart love-sand request-schema boundary`：`20016` handler 继续预滚奖励，repository 移除请求期 DDL；新增 game
`impart.004` replay table 与 player `impart.005` statistics columns startup migrations，缺 migration 返回 `schema_missing`，
已有 replay/统计数据保留。focused `22 passed`（1 条既有 compatibility warning）；五库 recovery `166` 项、路由
`131/31/7/1/1`，`.004` game-only、`.005` player-only，reconcile clean；没有运行根目录全量测试或真实 live/P7。
恢复数据、receipt、pytest basetemp 和字节码缓存已清理。

`rift speedup default-repository and startup-schema boundary`：默认 `RiftApplication.speedup` 改走
`RiftSpeedupSqlRepository`，其他 Rift actions 仍使用 legacy compatibility repository。新增 game-only `rift.004`，
迁移创建/补齐 replay 表；repository 缺迁移时返回 `schema_missing`，请求路径不建表/补列，旧 compact/rich payload 均兼容。
默认 composition、缺迁移拒绝、compact/rich 旧 payload replay、绑定数量不变量、无绑定列兼容、事务回滚和路由聚焦测试通过；未运行根目录全量回归或真实 live/P7。

`rift termination`：真实 `秘境终止` handler 的 replay/终止结算改走 `RiftApplication -> RiftTerminationSqlRepository`；repository 在
game DB 单事务内校验 active entry、秘境快照与 cooldown，原子终止 entry、释放 cooldown 并保留旧 `user_id + rift_plan` replay payload。
新增 game-only `rift.006`，缺 migration 返回 `schema_missing`，请求/replay 路径不执行 DDL；重复请求回放首次结果，用户/快照冲突、未激活和晚期 SQL
异常均不改变资产或状态，旧 `RiftTerminationService` 不再位于真实 facade 路径。Rift 聚焦回归 `98 passed`，compileall、architecture、progress、
inventory、diff check 和五库 recovery 全部通过；recovery 共 `169` 项 migration，路由 `134/31/7/1/1`，`rift.006` 仅 game，reconcile clean。
专用测试/recovery/receipt/字节码缓存已清理，约 `24 GB` 磁盘可用、`1.3 GB` RAM 可用；`.venv`、`.git`、`data/`、运行数据库/备份及 Boss JSON 保留。

`rift key-event`：真实 `秘境钥匙` handler 的 replay/事件结算改走 `RiftApplication -> RiftKeyEventSqlRepository`；attached game/player UoW
原子校验并扣除钥匙、提交预滚事件和奖励、更新探索次数/统计、结束 entry 与 cooldown，沿用旧 operation payload。新增 game-only `rift.007`，
缺迁移或 player schema 返回 `schema_missing`，请求/replay 不执行 DDL；重复、快照冲突、库存不足和晚期 SQL 失败均保持幂等/回滚语义。
聚焦回归 `100 passed`、compileall、architecture、progress、inventory、diff check 和五库 recovery 全部通过；recovery 共 `170` 项 migration，
路由 `135/31/7/1/1`，`rift.007` 仅 game，reconcile clean。专用测试/recovery/receipt/字节码缓存已清理，约 `24 GB` 磁盘可用、
`1.2 GB` RAM 可用；`.venv`、`.git`、`data/`、运行数据库/备份及 Boss JSON 保留。下一片为 `秘境结算`。

`rift ordinary settlement`：真实 `秘境结算` handler 的 replay/普通事件结算改走 `RiftApplication -> RiftSettlementSqlRepository`；repository 注入
Clock，在 attached game/player UoW 内校验时间窗口、资源/快照/探索次数并原子提交奖励、统计、entry、cooldown 与旧 payload。新增 game-only `rift.008`，
启动时补历史 operation 表的 `message` 列；请求/replay 不执行 DDL。聚焦回归 `104 passed`，并完成 compileall、architecture、progress、inventory、
diff check 与五库 recovery；recovery 共 `171` 项 migration，路由 `136/31/7/1/1`，`rift.008` 仅 game，reconcile clean。专用测试/recovery/receipt/
字节码缓存已清理，约 `24 GB` 磁盘可用、`1.3 GB` RAM 可用；下一步审计剩余 Rift entry/兼容只读边界。

`rift entry`：真实普通进入和秘藏令进入改走 `RiftApplication -> RiftEntrySqlRepository`；game DB 单事务校验 world generation/revision、参与者、
体力、秘藏令和 cooldown，更新 entry/world participants/revision/count 并写入旧 replay payload。新增 game-only `rift.009`，启动时预建/补齐 entry
schema，请求路径不建表/补列；重复、world 冲突、资源不足和晚期 SQL 失败均保持幂等/回滚。聚焦回归 `109 passed`，完成 compileall、architecture、
progress、inventory、diff check 与五库 recovery；recovery 共 `172` 项 migration，路由 `137/31/7/1/1`，`rift.009` 仅 game，reconcile clean。专用
测试/recovery/receipt/字节码缓存已清理，约 `24 GB` 磁盘可用、`1.3 GB` RAM 可用；下一步审计 Rift 剩余兼容只读边界。

`rift active-entry read projection`：`jsondata.read_rift_data` 改经 `RiftEntrySqlRepository.read_entry` 的 read-only UoW；表/历史列缺失不执行 DDL，
仅在没有 active entry 时回退玩家 JSON。覆盖 active/inactive、缺表/缺列、对象校验和旧 fallback；Rift 聚焦 `113 passed`，compileall、architecture、progress、
inventory、diff check 与五库 recovery 均通过，recovery `172` 项、路由 `137/31/7/1/1`、reconcile clean。专用产物已清理；下一步审计 Rift cooldown 只读边界。

`rift cooldown read projection`：真实 `秘境结算` handler 的 cooldown 读取改经 `RiftApplication.read_cooldown -> RiftCooldownSqlRepository` read-only UoW；
缺表/缺列不插入用户、不执行 DDL，移除 facade 的 `XiuxianDateManage.get_user_cd`。cooldown/Rift/source/progress 聚焦 `118 passed`，compileall、
architecture、inventory、diff check 与五库 recovery 均通过；无新增 migration，recovery 仍 `172` 项、路由 `137/31/7/1/1`、reconcile clean。专用产物已清理；
下一步审计 Rift 残留 speedup legacy getter/只读投影边界。

`rift speedup legacy-construction cleanup`：真实 facade 移除未调用的 `RiftSpeedupService` import、instance 和 lazy getter；handler 继续经
`RiftApplication.speedup -> RiftSpeedupSqlRepository`，显式 compatibility repository 保留。speedup/Rift/source/progress 聚焦 `118 passed`，
compileall、architecture、inventory、diff check 与五库 recovery 通过；无新增 migration，recovery `172` 项、路由 `137/31/7/1/1`、reconcile clean。
专用产物已清理；下一步审计 Rift 显式 compatibility repository 与故事/资产只读边界。

## 当前切片与下一切片选择

最近完成 `pet transaction compatibility isolation`：宠物默认 handler 经 `PetApplication` 使用 feature SQL repositories；旧 `Pet*Service` 实现已移入
`compatibility/legacy_pet_transactions.py`，`xiuxian_pet/transaction_service.py` 只保留历史 API re-export，默认宠物 facade 不再导入未调用的旧 service/lazy getter。
`LegacyPetRepository` 仍保留显式回滚路径，宠物 JSON projection/read path 也未因此整体关闭。宠物/source/progress 聚焦回归 `310 passed`，排除一项已知无关的 Rift source assertion；
architecture、inventory、progress 与 diff check 均通过。五库隔离 recovery 的 173 项 migration、backup/restore 和 3 项 attached migration 均通过，reconcile clean；
临时数据与 receipt 已清理。本片无新增业务 migration。

最近完成 `generic task progress application cutover`：真实通用任务事件和状态读写经
`TaskProgressApplication -> TasksProgressRepository` 使用 player-db immediate UoW；`game_events`、work、buff、
impart 仍保留旧 task-data adapter 作为事件 DTO/任务映射边界，签到专属 projection 不变。新增 player-only
`tasks.001` 预建历史周期列和 operation 表，生产请求不做 DDL。聚焦任务进度、领奖、feature task 和签到 effects/wiring
回归 `26 passed`（1 条既有 ServicePort `DeprecationWarning`），compileall、architecture、progress、inventory、diff check
通过；隔离五库 backup/restore、迁移和 reconcile clean，恢复目录已清理。旧 service import 为兼容转发，但旧实现仍留在
`LegacyTaskProgressEventService`，不宣称其源码已隔离。真实发布应用和 P7 证据仍缺失。

`task reward claim startup-schema boundary` 已完成：新增 game-only `tasks.002`，启动迁移预建领奖 operation 表并补齐旧
`economy_log` 缺失列；player-only `tasks.001` 继续负责任务周期状态列，领奖生产请求不再创建/补充 schema。任务领奖、进度、
feature tasks 与签到 effects/wiring 聚焦回归 `29 passed`（1 条既有 ServicePort `DeprecationWarning`）；compileall、architecture、
progress、inventory freshness 和 `git diff --check` 通过。隔离 recovery 的五库 backup/restore dry-run/restore 均成功，
共覆盖 175 个不同 migration version，路由计数 `139/32/7/1/1`；`tasks.001` 仅路由到 player、`tasks.002` 仅路由到 game，
reconcile clean（operations/outbox/dead events 均为 `0`）。专用 pytest basetemp、恢复数据、receipt 和 compileall 字节码缓存
已清理；`.venv`、`.git`、`data/`、运行数据及 Boss JSON 用户修改保留。此切片只完成 schema 启动化，
`TasksRepository -> task_manager -> TaskRewardClaimService` 仍负责动态奖励快照、奖励库存、economy log、claimed 状态与
game/player attached transaction。下一片先审计 operation replay/conflict、外层 operation ledger 卡在 started 后的续跑，
并明确 WAL 环境下跨库崩溃原子性的真实保证，再迁移领奖事务 ownership；不可通过保留 facade 或单纯移动旧类把领奖标成完成。
之后继续按
6.2 逐项覆盖训练、洞府、地图、宗门、竞技场/副本、世界事件、Boss 和交易剩余 adapter；追捕令 `20015` 的随机 offer
仍由旧领域逻辑生成，不能把既有扣除/快照边界解释成 work 领域整体完成。
已切换的 partner cultivation、partner token、背包通用 item-use Web、宠物蛋、饰品礼包、炼丹两阶段领取、斩妖令、祈愿石、
挑战券事务、love-sand schema 边界、Rift speedup 和 Rift world-generation schema 边界不重复迁移。

`rift damage-event outcome`：真实 `_roll_rift_event` 的掉血事件改经
`RiftApplication.roll_damage_event -> RiftDamageEventResolver`；resolver 只接收显式 battle config、经验计算器、数字格式化器和 RandomSource，
返回可持久化的 delta/message DTO，不读取或写入用户状态。旧 `get_dxsj_info` 仅保留兼容适配，Boss/宝物随机与资产解析不在本片迁移。
damage domain/application/source 与 Rift 回归 `122 passed`，compileall、architecture、progress、inventory、diff check 均通过；无新增 migration。
下一片继续审计 Boss battle outcome 的战斗资产 provider，避免把 `Boss_fight` 或宝物 `Items` 读取误算成 feature-owned。

`rift Boss-battle outcome`：真实普通秘境事件和斩妖令事件改经
`RiftApplication.roll_boss_battle -> RiftBossBattleResolver`；resolver 接收显式 `Boss_fight` runner、Boss 配置、境界/经验 provider 与 RandomSource，
固定以 `type_in=0` 运行并只返回战斗结果及 delta/message outcome。旧 `get_boss_battle_info(persist=True)` 仅保留兼容适配和显式经验/灵石写入；
不迁移 Boss/宝物资产读取或 `Items` provider。Boss/domain/application/source 与 Rift 回归 `125 passed`，compileall、architecture、progress、inventory、
diff check 均通过；无新增 migration。下一片审计 Boss battle runner 的资产查询边界或宝物 resolver，继续保持旧战斗实现为显式 provider。

`rift treasure outcome`：真实普通秘境宝物事件改经
`RiftApplication.roll_treasure -> RiftTreasureResolver`；resolver 只编排随机类型、奖励消息和可结算 outcome，
`Items`、功法/装备查询保持显式兼容 provider，不把 provider 注入误算为底层资产迁移。旧 `get_treasure_info` 仅保留兼容适配；
本片不新增 migration，验收覆盖 treasure/domain/application/source 与 Rift 聚焦回归。

`rift Boss asset snapshot boundary`：`RiftBossBattleResolver` 通过显式
`RiftBossBattleAssetProvider` 预解析玩家战斗快照，再以 `player_data` 注入 `Boss_fight`；Boss 引擎对未注入快照的旧调用仍回退
`get_players_attributes`，因此 `Items`、功法/装备、宠物和本命法宝底层查询仍是兼容 provider，不能宣称资产迁移完成。Rift 聚焦回归 `130 passed`；
compileall、architecture、progress、inventory 和 diff check 通过；无新增 migration。下一步继续把该 provider 的单一资产查询边界拆成可验证的只读 provider，保持 Boss 引擎与结算事务解耦。

`rift Boss item lookup provider`：Rift 玩家战斗快照 provider 将 `Items.get_data_by_item_id` 作为显式
`item_provider` 传入 `get_players_attributes`；未注入时保留旧全局 Items fallback。该切片只拆出一个只读查询边界，
不改变 `get_final_attributes`、宠物、本命法宝或资产文件的兼容读取，也不新增 migration。

`rift Boss pet lookup provider`：Rift 玩家战斗快照 provider 同样显式注入
`get_user_pet_for_battle` 作为 `pet_provider`；未注入时保留旧宠物读取 fallback。该切片只拆出宠物只读查询边界，
不改变宠物数据模型、战斗状态写入或其他玩家资产读取。

`rift Boss final-attribute provider`：Rift 玩家战斗快照 provider 显式注入
`get_final_attributes` 作为 `attribute_provider`，并透传 `ratio` 与 `include_current=True`；未注入时保留旧属性计算 fallback。
该切片只拆出最终属性只读查询边界，其内部 buff/传承读取仍是兼容 provider，不新增 migration。

`task reward claim game/player saga`：默认领奖 matcher 改走
`TaskClaimApplication -> TaskClaimGameRepository/TaskClaimPlayerRepository`；player reservation、game item grant/checkpoint、player claim receipt
分阶段提交，复用同一 operation 可从中断阶段恢复。新增 game-only `tasks.003` 和 player-only `tasks.004`，请求不建表且不依赖 WAL 跨库 `ATTACH`。
静态任务定义、Items 查找和奖励快照仍由 `task_data` adapter 提供，旧 `TaskRewardClaimService` 保留为兼容对照。聚焦回归 `37 passed`
（1 条既有 ServicePort `DeprecationWarning`）；compileall、architecture、progress、inventory 和 diff check 通过。五库 recovery 覆盖 177 个
migration version，路由计数 `140/33/7/1/1`，`.003` 仅 game、`.004` 仅 player；backup/restore dry-run/restore、五库 migration dry-run
（pending 为空）、health 六项和 reconcile clean 均通过。专用 recovery 数据、receipt、pytest cache 与字节码缓存已清理；真实发布数据迁移和 P7 仍未完成。

`training administrator reset`：`重置历练` 默认后台分块路径切换至 feature-owned `TrainingResetSqlRepository`；新增 game-only `training.004`，首次 chunk 冻结用户集合，后续以同一 operation id 续跑 pending targets，保留 duplicate/conflict/skipped、previous state 和晚失败 rollback。请求路径只读已迁移 schema，普通 application ledger 不承载跨 chunk replay；兼容服务仅在缺少 player database 时保留。聚焦回归覆盖分块冻结、删除用户、重复 user id、空集合、旧 schema 补列、缺 schema 不建表、晚失败 rollback、Clock 注入和真实入口 source contract。恢复演练应验证 `training.001/.003/.004` 只进入 game_db、`training.002` 只进入 player_db，完成后清理 basetemp、字节码、pytest cache、receipt 和临时数据库，并复核 `df -h`/`free -h`。真实发布数据迁移/P7 仍需单独记录。

`sect weekly reward application cutover`：默认周常领取经 `SectApplication.claim_weekly -> SectWeeklyRewardSqlRepository`；game-only `sect.011`、player-only `sect.012` 启动预建 schema，operation receipt 与资产/领取标记在同一 attached transaction 提交。聚焦回归 `48 passed`，progress/inventory/architecture `18 passed`，recovery 路由和 reconcile clean；SQLite WAL 多库崩溃原子性仍不作保证。

`sect confirmed-disband application cutover`：`确认解散宗门` 经 `SectApplication.disband -> SectManualDisbandSqlRepository`；game-only `sect.013` 预建旧格式兼容回执，保留宗主复核、成员解绑、宗门删除、replay 和事务回滚。宗门筛选回归 `65 passed`，progress/inventory/architecture `18 passed`；隔离五库 backup/restore、migration routing 与 reconcile clean。用户原有 `boss_info.json` 修改保持未提交，仓库 Python/pytest 缓存和本片 recovery 临时数据已清理。下一片按宗门剩余边界审计 Fairyland claim 及其 Tianti profile/settlement 依赖；真实发布数据/P7 和全局 legacy blockers 继续开放。

`sect fairyland claim application cutover`：默认领取经 `SectFairylandApplication -> SectFairylandSqlRepository`，在 player-db immediate UoW 同步提交 tianti 气血、每日 marker、旧列兼容投影和 operation receipt；应用不再跨两个事务写外层 ledger。新增 player-only `sect_fairyland.002`，回填旧动态领取列；旧兼容 service 读取新 marker，运行时 Clock 与天降灵脉 multiplier 均显式注入。切片聚焦回归 `69 passed`，额外宽回归有 3 项已知无关失败；compileall、architecture、progress、inventory、diff check 通过。五库 recovery 覆盖 185 migrations，路由 `145/36/7/1/1`，`.002` 仅 player_db，reconcile clean。下一片先界定 Tianti profile 的写入/结算 owner 与 Fairyland upgrade 事务边界；本切片不代表宗门/Tianti 整体完成。

`tianti read-only profile legacy normalization`：feature-owned `TiantiProfileReader.clean` 恢复旧 manager 的境界/气血/药浴容错、JSON 字段清洗和窍穴配置校验；已开启列表去重保序，详情按列表重建并由配置补全，缺失窍穴配置按空配置处理。`TiantiProfileSqlReader` 仍只读，不回写历史脏行；无 schema/migration 变化。Tianti Training、Fairyland、只读 facade/source 与 architecture 聚焦回归 `77 passed`；Tianti/Fairyland、inventory/source-quality 与 architecture 扩展套件 `321 passed, 1 failed`，唯一失败是已知过期 Rift source-quality 断言仍要求旧 `_sql_message_instance` 字段；compileall、progress、inventory 与 diff check 通过。progress 仍报告 legacy transaction services 和 `xiuxian2_handle` 两项全局 blocker，不能将本切片视作重构完成。pytest 禁用 cacheprovider/字节码，compileall 专用 `/tmp` 缓存已清理，`data/` 与运行数据库未触碰。下一片继续界定 Tianti profile 写入/结算 owner 与 Fairyland upgrade 事务边界。

`sect fairyland upgrade startup-schema boundary`：真实 `宗门炼体堂升级` 继续经 `SectApplication -> SectFairylandSqlRepository`；新增 game-only `sect.014` 预建 `sect_fairyland_operations`，兼容旧版请求时建表产生的同结构操作表及回执。repository 不再请求时执行 DDL，缺 schema 返回 `schema_missing`；宗主/成员身份、等级只升一级、双资产余额与 CAS 更新、升级状态及回执在同一 UoW 中校验/提交。修复 `SectMutationResult.applied` 漏含 `upgraded` 导致升级成功后 handler 报失败，并将 duplicate 提示放在经济日志之前。宗门/Fairyland/source-quality/inventory/architecture 聚焦回归 `325 passed, 1 deselected`（排除已知过期 Rift source-quality 断言）；compileall、progress、inventory、architecture、diff check 通过。五库恢复演练覆盖 `186` 个 migration，路由 `146/36/7/1/1`，backup/restore dry-run/restore 均成功，`sect.014` 仅进入 game_db，reconcile clean。专用恢复目录和字节码缓存已清理；`data/`、运行数据库及用户 `boss_info.json` 修改保留。progress 仍有 legacy transaction services 与 `xiuxian2_handle` 全局 blocker；下一片审计 Tianti profile 写入/结算 owner，不能将宗门或整体重构视作完成。

`tianti profile writer ownership`：Tianti Training、Tianti Settlement、宗门炼体堂领取的默认 SQL repositories 共用 `upsert_tianti_profile` 序列化/UPSERT primitive，支持 main 与 attached `player_data` schema；各 feature 仍在自己的 UoW 内拥有资产、profile 和 operation receipt 原子提交。移除默认 Tianti command facade 中未使用的 `TiantiDataManager` 单例和 stone repository 的无效 manager 注入；保留 `get_user_tianti_info` 作为有明确标记的 legacy write-through API，Settlement/Training legacy adapters 仍显式保留，默认路径不使用它们。无 migration。Tianti/Fairyland/source-quality/inventory/architecture 回归 `356 passed, 1 deselected`（排除已知过期 Rift source-quality 断言）；compileall、progress、inventory、architecture、diff check 通过。五库恢复演练 186 migrations、路由 `146/36/7/1/1`，backup/restore dry-run/restore 成功，reconcile clean；专用恢复与字节码目录已清理。全局 legacy transaction services 和 `xiuxian2_handle` blocker 仍在；下一片审计 Tianti `transaction_service.py` 的结算/展示规则依赖与显式 legacy settlement adapter，不代表 Tianti 或全局重构完成。

`tianti gain-rule presentation ownership`：新增 training feature `presentation.py`，统一药浴有效时点、窍穴收益、宗门炼体堂加成和每分钟收益预览；状态命令及宗门展示改为直接使用 feature rules，Tianti Settlement SQL repository 与 Training SQL repository 复用同一规则，旧 `transaction_service.py` 保留同名兼容包装。调用审计确认默认 settlement 使用 feature application/repository，`TiantiSettlementService` 仅由显式 `LegacyTiantiSettlementRepository` 惰性实例化；状态页 `get_tianti_cap` 仍从 legacy module 读取，列为下一边界。无 migration。Tianti Training、Settlement、Fairyland 与历史兼容服务回归 `70 passed`；progress、inventory、source-quality、architecture 回归 `247 passed, 1 deselected`（排除已知过期 Rift source-quality 断言），progress 脚本通过且仍报告 legacy transaction services、`xiuxian2_handle` 两项全局 blocker。测试禁用 pytest/字节码缓存，未触碰 `data/`、运行数据库或用户 `boss_info.json`；下一片迁移状态页 cap 查询到 feature-owned profile/read rules，再审计可删除的显式 legacy adapter，不代表 Tianti 或全局重构完成。

`tianti profile cap query ownership`：`TiantiProfileReader.cap` 原有的等级上限规则通过 `TiantiProfileSqlReader` 与 `TiantiTrainingApplication.profile_cap` 对外提供；默认 Tianti facade 删除对整个 `transaction_service.py` 的 import，状态页上限改由 feature profile reader 计算。保留既有关闭倍率默认值 `1.5`，测试覆盖下一等级需求倍率及最高等级 `10**30` 上限，无 schema/migration。profile/application/progress/inventory 聚焦回归 `14 passed`，progress checker 通过；整体 blocker 仍为 legacy transaction services 和 `xiuxian2_handle`。测试禁用 pytest/字节码缓存，用户 `boss_info.json` 仍未暂存；下一片追踪 `grant_tianti_settle_minutes` 的真实宗门调用及 legacy training adapters 的运行时注入边界，不代表 Tianti 或全局重构完成。

`tianti legacy adapter service construction`：`LegacyTiantiTrainingRepository` 原先每次任一兼容调用都构造 Stone Training、Medicine Bath、Breakthrough、Qiaoxue 四个 service；现按实际 operation 延迟构造唯一对应 service，不改旧 APIs 或事务实现。追踪确认 `grant_tianti_settle_minutes` 仅由旧 Fairyland claim service 使用，默认领取已由 feature repository 接管；旧 claim service 继续保留给显式兼容 adapter。无 migration。Tianti Training 全套回归 `43 passed`，progress 断言及 adapter 调用测试通过；测试禁用 pytest/字节码缓存，`data/`、运行数据库及用户 `boss_info.json` 未触碰。全局 blocker 仍为 legacy transaction services 和 `xiuxian2_handle`；下一片清理宗门 facade 中已无调用点的 `SectMembershipService` lazy getter/import，保留其公开 legacy service 类及直接兼容测试。

`sect membership stale facade adapter removal`：全仓调用搜索确认 `_sect_membership_service()` 除定义外无调用，成员增删/职位调整默认入口已在 `SectApplication`；移除 facade 中旧 `SectMembershipService` import、单例和 lazy getter，service 类及其直接兼容测试仍保留。无 schema/migration。宗门 membership、progress、inventory、architecture 回归 `26 passed`；sect source-quality `12 passed`，progress/inventory/architecture 检查通过。测试禁用 pytest/字节码缓存，工作区 `boss_info.json` 保持未暂存；全局 blocker 仍为 legacy transaction services 和 `xiuxian2_handle`，下一片继续按 progress 文件追踪剩余默认 legacy service 连接，不代表宗门或整体重构完成。

`sect disconnected service facade removal`：全仓调用搜索确认 close mountain、owner inherit、open/close join、daily reset 五个 legacy getter 除定义外均无调用，默认 handler 均经 `SectApplication`；移除 facade getter、实例槽和只被 getter 使用的 service imports，保留 legacy service 类及其独立事务测试。无 schema/migration。宗门 service、join-state、progress、inventory、architecture 回归 `57 passed`；sect source-quality `12 passed`。测试禁用 pytest/字节码缓存，`boss_info.json` 用户修改仍保持未暂存；progress 全局 blocker 仍为 legacy transaction services 和 `xiuxian2_handle`，下一片盘点 sect facade 其余 service imports 与调用点，不代表宗门或整体重构完成。

`sect legacy service import audit`：核对当前 facade 后确认不存在任何 `.transaction_service` import；进度脚本新增约束防止旧 service import 回流。`XiuxianDateManage` 仍承担多处读模型及活动时间戳更新，属于 `xiuxian2_handle` 路径而非 transaction service；不在本审计中移除。progress/inventory/join-state 回归 `4 passed`，无 migration/数据变更；测试禁用 pytest/字节码缓存，`boss_info.json` 继续保持未暂存。下一片从 `update_last_check_info_time` 这类窄 side effect 选定 feature owner 并追踪调用契约，不代表 sect 或整体重构完成。

`sect elixir activity timestamp ownership`：丹房领取的活动时间更新现经 `SectApplication -> SectActivitySqlRepository` 写入 game-db `user_cd`；Clock 显式注入，保存格式兼容旧 reader，调用仍在成员资格判断前，缺失行不触发插入或 DDL。无 migration；聚焦行为/source/progress `7 passed`，architecture/inventory contract `17 passed`，compileall、architecture CLI、progress 与 diff check 通过。pytest 禁用 cacheprovider/字节码；pytest/pyc 与专用 `/tmp` 临时目录已清理，磁盘/RAM 已复核，`boss_info.json` 保持用户未提交修改。架构 CLI 初始化时写入 `data/xiuxian/compatibility_hits.json` 并刷新已有 `xiuxian_impart.db-shm`；发现运行中 NoneBot PID `115747` 共用数据库，故保留运行期文件，不手工清理。全局 blocker 仍为 legacy transaction services 与 `xiuxian2_handle`；下一片从宗门 facade 里其余直接使用 `XiuxianDateManage` 的状态读写中，继续选择窄、可独立验证的 feature-owned 边界。

`sect activity timestamp read ownership`：自动传位/解散 scheduler 的两处最后活跃时间读取现经 `SectApplication -> SectActivitySqlRepository`；只读现有 `user_cd`，旧时间字符串转为本地 aware datetime。`SectDisbandSqlRepository` 对 legacy naive 时间与 aware Clock 做兼容比较。无 migration；行为/progress/source 聚焦 `8 passed`，pytest 禁用 cacheprovider/字节码且本轮未启动 NoneBot/访问共享运行数据库。basetemp 已清理；共享 `xiuxian_impart.db-shm` 仍由运行中 bot 保护。下一片继续审计宗门 facade 中其他直接 `XiuxianDateManage` 状态读取。

`sect scheduled material startup-schema cutover`：定时发放目标集合改由 `SectApplication -> SectScheduledMaterialSqlRepository.list_targets` 只读获取，grant repository 不再请求时建表；新增 game-only `sect.015` 启动迁移预建回执表，既有表/回执由 `IF NOT EXISTS` 保留，缺 migration 不写资产且不 DDL。成功状态 `granted` 现在计入 application `.applied`。scheduled grant、progress、inventory、migration count 与 architecture contract `28 passed`；无运行数据迁移，本轮测试只使用临时 SQLite。pytest/pyc 缓存禁用，basetemp 清理后复核；下一片继续审计宗门 facade 的其他 manager 只读入口。
