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

## 下一切片选择

回到 `docs/full_refactor_progress.md` 的 6.2 目标 5，下一片优先审计并迁移秘境斩妖令 `20018` 的真实 `use_rift_boss` handler：目前它直接调用旧 `RiftDemonTokenBattleSettlementService`，而 `RiftApplication` 尚无对应 action；已有回归覆盖资产、探索次数、统计、幂等和跨库回滚，可作为行为基线。该片只处理斩妖令结算，不扩大到整个秘境目录。追捕令 `20015` 已切换，但随机 offer 仍由旧领域逻辑生成；不能把现有 application 边界扩大解释成 work 领域整体完成。随后再核实其他 NoneBot 特殊道具、宠物、任务/修炼、洞府、地图、宗门、竞技场/副本、世界事件和 Boss handler。不可按目录整体迁移或把惰性 facade、静态 manifest、仅测试通过视为完成。已切换的 partner cultivation、partner token、背包通用 item-use Web、宠物蛋、饰品礼包和炼丹两阶段领取不重复迁移。
