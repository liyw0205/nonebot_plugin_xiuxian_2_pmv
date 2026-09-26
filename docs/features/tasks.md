# Tasks

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
manifest 兼容标识仍为 `legacy.tasks.001`；实际 schema 由 `tasks.001` 在 `player_db` 预建任务进度、`tasks.002` 在 `game_db` 预建奖励 operation 与经济日志、`tasks.003` 为旧领奖 operation 增加恢复 checkpoint、`tasks.004` 在 `player_db` 建立领取回执和 task reservation。迁移保留既有进度与 operation 行。

## 事务与失败回滚
领奖调用携带 `operation_id`，并在 game operation ledger/outbox 先写入冻结请求。player transaction 预留 eligible task，game transaction 发放物品并推进 checkpoint，player transaction 再确认 `claimed`；各阶段可按同一 operation 重放，不使用 WAL 下不具备跨库崩溃原子性的 `ATTACH` 事务。`tasks.claim_rewards` 已注册到 reconcile operation handler，可恢复 started/granted operation。回滚使用迁移前备份，不删除已确认的 operation receipt。

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`tasks_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
默认领奖入口使用 `TaskClaimApplication -> TaskClaimGameRepository/TaskClaimPlayerRepository`；静态任务定义、物品元数据解析和奖励快照仍由 `task_data` adapter 提供，旧 `TaskRewardClaimService` 暂留作兼容对照且不再由默认领奖调用。上线前仍需在真实发布数据上执行迁移与恢复演练；全局旧 service 和 `xiuxian2_handle` 退出条件仍按 P7 发布周期门槛处理。

## Manifest 清单
- `command: 周常任务`
- `alias: 每周任务`
- `command: 我的任务`
- `alias: 任务列表`
- `alias: 修仙任务`
- `command: 每日任务`
- `alias: 今日任务`
- `command: 领取任务奖励`
- `alias: 任务奖励`
- `alias: 领取周常任务奖励`
- `alias: 领取每日任务奖励`
