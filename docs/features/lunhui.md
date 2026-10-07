# Reincarnation

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 冻结命令批次（2026-10-07）
本批按 owner 分组收口 7 条冻结命令，没有按别名或 handler 拆成重复迁移：

- `自废修为` 经 `LunhuiApplication` 调用 `CultivationResetService`；重置和“自废修为次数”与 operation receipt 同事务提交。旧角色资料读取、回复和日志仍是适配层职责。
- `回忆前世` 的印记快照由 `LunhuiApplication` 只读查询，技能安装、取回标记、类型统计和 receipt 由 `LunhuiRecallService` 同事务提交。境界门槛继续使用兼容层的 `convert_rank` 和角色资料读取；成功重放只查一次 receipt，不重复计数。日志只在首次应用后写入。
- `确认轮回` 继续复用 `LunhuiSettlementService` 的 game/player/impart 状态、印记和统计事务；确认邀请缓存清理由兼容层管理。
- `轮回印记` 通过 `LunhuiApplication` 只读查询 player DB，缺表不创建 schema；兼容 handler 仅负责物品名称解析、格式化和发送。
- `进入轮回`、`进入无限轮回` 保留兼容层的境界/灵根准入及共享确认邀请。邀请是进程内 60 秒临时状态，不是持久业务写；持久变化仅由 `确认轮回` 的 settlement owner 执行。
- `轮回重修帮助` 是静态文本兼容回复，无玩家状态或资产副作用。

该批不迁移轮回等级门槛策略、消息模板/物品目录呈现，也不把进程内邀请描述为持久 owner。
三项 mutation service 使用 attached SQLite 文件；SQL 异常回滚有测试，但主库 WAL 模式下不承诺进程/系统崩溃时的跨文件原子性。

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.lunhui.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`lunhui_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 回忆前世`
- `alias: 取回记忆`
- `command: 确认轮回`
- `command: 自废修为`
- `command: 轮回印记`
- `command: 轮回重修帮助`
- `alias: 轮回帮助`
- `command: 进入无限轮回`
- `command: 进入轮回`
- `alias: 开始轮回`
