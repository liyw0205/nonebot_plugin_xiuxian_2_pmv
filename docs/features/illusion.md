# Illusion choice

## 用户流程
The historical command remains available as a transport adapter, while the feature application owns the choice transaction.

## 命令与别名
The historical package still owns the command names during the adapter transition; the manifest records the complete command contract.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `illusion.001` records the slice in `game_db`; upgraded installations may also contain the historical `legacy.illusion.001` marker. The repository owns `illusion_choices`, `illusion_choice_stats`, and `illusion_choice_operations`.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger in the same `game_db` transaction as the choice and reward projection. Rejected choices leave balances, inventory, and statistics unchanged.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`illusion_enabled` controls the application boundary and defaults to true.

## 适配器差异
The command adapter translates the historical event into the application DTO; the application and repository do not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
The old service import path remains a thin facade for one release cycle. It does not own storage; compatibility hit counters determine when that facade can be removed.

## Manifest 清单
- `command: 幻境寻心`
- `command: 心境试炼`
- `command: 清空幻境`
- `command: 重置幻境`
