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

`buff partner-token`：`道具使用 双修令牌` 默认 handler 调用
`PartnerTokenUseApplication -> PartnerTokenUseSqlRepository`；`buff.002` 在 game DB 创建 operation 表，
`buff.003` 在 player DB 创建次数 projection，跨库写入使用 attached UoW，不在请求时建表。旧
`PartnerTokenUseService` 保留作兼容对照，但不再被默认 handler 调用。聚焦测试 `10 passed`，五库 recovery
`154` 项、readiness 六项全绿；恢复数据、receipt 和缓存均已清理。

## 下一切片选择

清理并复核磁盘后，回到 `docs/full_refactor_progress.md` 的 6.2 目标 5，优先评估
`xiuxian_buff/partner.py` 道侣双修结算 handler 对 `PartnerCultivationService.apply` 的真实默认调用；保留
随机结果预滚、双方修为/属性、affection、次数与保护状态快照、operation replay 和跨库回滚语义。不得把
惰性 facade、静态 manifest 或仅测试通过视为切片完成。背包通用 item-use Web、宠物蛋和饰品礼包的真实
入口已有独立 application 边界，不应重复迁移；NoneBot 特殊道具效果仍需按领域逐个审计。
