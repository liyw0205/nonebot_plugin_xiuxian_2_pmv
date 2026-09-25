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

`buff partner cultivation`：`xiuxian_buff.partner::direct_two_exp` 默认结算改为调用
`PartnerCultivationApplication -> PartnerCultivationSqlRepository`；修为/属性、双修次数、统计、亲密度、邀请接受与 operation ledger 在 attached UoW 内提交。新增 game DB `buff.004` 和 player DB `buff.005`，次数读取保留旧 JSON 一次性导入但不再请求时建表；非邀请 operation payload 与旧 ledger 保持兼容。聚焦回归覆盖回放、冲突、过期/保护、超大数、快照拒绝和跨库异常回滚；旧 `PartnerCultivationService` 仅留作兼容对照。
focused/source/progress/architecture/inventory 回归 `255 passed, 4 subtests`；compileall、architecture、inventory、progress 和 diff check 均通过。五库 recovery 完成 `157` 项迁移，`.004` 仅 game、`.005` 仅 player；backup/restore dry-run/restore、migration dry-run（五库 pending 为空）、health 六项和 reconcile clean 均通过，operations/outbox/dead events 为 `0`。根目录全量测试已启动但未完成：`1950 passed` 后有 5 个与本切片无关的 blessed-flag legacy service `TypeError`，并停在 legacy sign-in startup 测试；该进程已中断。指定 basetemp、recovery 数据、receipt 和源码字节码缓存均已清理。

`buff partner-token`：`道具使用 双修令牌` 默认 handler 调用
`PartnerTokenUseApplication -> PartnerTokenUseSqlRepository`；`buff.002` 在 game DB 创建 operation 表，
`buff.003` 在 player DB 创建次数 projection，跨库写入使用 attached UoW，不在请求时建表。旧
`PartnerTokenUseService` 保留作兼容对照，但不再被默认 handler 调用。聚焦测试 `10 passed`，五库 recovery
`154` 项、readiness 六项全绿；恢复数据、receipt 和缓存均已清理。

## 下一切片选择

清理并复核磁盘后，回到 `docs/full_refactor_progress.md` 的 6.2 目标 5，按真实入口调用图选择下一个仍由旧 service 承载的高频资产动作。优先核实 NoneBot 特殊道具效果、宠物、任务/修炼、洞府、地图、宗门、竞技场/副本、世界事件和 Boss 的具体 handler；不可按目录整体迁移或把惰性 facade、静态 manifest、仅测试通过视为完成。已切换的 partner cultivation、partner token、背包通用 item-use Web、宠物蛋和饰品礼包入口不重复迁移。
