# Titles

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `title.001` records the slice in `player_db`; the title projection and its operation records are owned by the new repository.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`title_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
The old transaction service remains as a facade for one complete release cycle; the compatibility hit counter determines when the import shim can be removed.

## Manifest 清单
- `command: 刷新称号`
- `command: 卸下称号`
- `alias: 取消称号`
- `command: 我的成就`
- `alias: 成就列表`
- `alias: 查看成就`
- `command: 我的称号`
- `alias: 查看称号`
- `alias: 称号列表`
- `command: 检查成就`
- `alias: 检测成就`
- `command: 检查称号`
- `alias: 检测称号`
- `command: 称号帮助`
- `command: 称号详情`
- `command: 装备称号`
- `command: 赠送称号`
