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

最近完成 `rift speedup legacy-construction cleanup`：entry 写入、active 读取、cooldown 读取和 speedup handler 分别统一接到
`RiftApplication -> RiftEntrySqlRepository` / `RiftEntrySqlRepository.read_entry` / `RiftCooldownSqlRepository` / `RiftSpeedupSqlRepository`，旧 entry service、SQL manager 与 speedup getter 已从真实 facade 移除；剩余显式 compatibility repository 与故事/资产只读边界仍是
分开的路径。下一步在本轮缓存清理与磁盘复核后，回到
`docs/full_refactor_progress.md` 的 6.2 目标 5，只读审计一个剩余真实 Rift handler 的 composition、请求期 schema 写入与 transaction owner，
再选择单动作切片；不要将 facade 调用或静态路由当成底层 cutover。更广范围仍需按 6.2 逐个审计特殊道具、宠物、任务/修炼、洞府、地图、宗门、
副本、世界事件和 Boss handler；追捕令 `20015` 的随机 offer 仍由旧领域逻辑生成，不能把既有扣除/快照边界解释成 work 领域整体完成。
已切换的 partner cultivation、partner token、背包通用 item-use Web、宠物蛋、饰品礼包、炼丹两阶段领取、斩妖令、祈愿石、挑战券事务、
love-sand schema 边界、Rift speedup 和 Rift world-generation schema 边界不重复迁移。

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
