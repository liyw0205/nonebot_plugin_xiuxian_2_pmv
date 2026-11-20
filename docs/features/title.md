# Titles

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

`装备称号 名称或ID` 只允许装备本人已解锁称号；`卸下称号`（别名 `取消称号`）取消当前装备。
已装备、未解锁、当前未装备、空参数和不存在名称保留各自的具体回复；同消息请求不同称号返回操作冲突。

`赠送称号 名称或ID [道号/@目标]` 仅限超级用户；目标省略时发给操作人。单人目标须已注册，@提及按消息中的用户 ID 解析。
称号状态、永久操作回执和操作账本在玩家库事务内核对；已知拒写会说明失败位置与称号并整体撤销，故障解除后可重试原消息。

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `title.001` records the slice in `player_db`; the title projection and its operation records are owned by the new repository.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

装备/卸下在同一 immediate player SQLite 事务内写状态、永久回执和应用 ledger/audit。
状态写入核影响行数及完整称号文档，回执核 payload/status/title_id，ledger/audit 完成后再次复核两者。
状态拒写保留 `title_equip_write_rejected`；状态改写、回执拒写/改写分别保留
`title_equipment_state_projection_mismatch`、`title_equipment_receipt_write_rejected`、
`title_equipment_receipt_projection_mismatch` 与称号 ID，整事务回滚且不留下不可重试的拒绝账本。
解除故障同消息可恢复，成功重放不重复写；SQLite ABORT 在应用层保留原异常，失败账本使用稳定请求哈希以允许重试。
命令层记录原异常并返回安全定位号，已知拒绝直接返回具体原因。未改历史失败账本哈希或解锁/赠送规则。

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`title_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

- `python -m pytest -q tests/test_title_equipment_recovery.py tests/test_title_equipment_parameter_dispatch.py`：
  隔离 SQLite 的装备/卸下拒写、改写、回执/审计末步副作用、ABORT、整笔回滚、恢复/重放、真实名称/ID 参数和
  OneBot/QQ Markdown/失败回退 mock；不代表真实 QQ 联调。
- `python -m pytest -q tests/test_title_grant_parameter_dispatch.py tests/test_title_grant_recovery.py tests/test_title_grant_transaction.py`：
  单人赠送的真实 matcher、权限/目标参数、隔离 SQLite 状态与回执拒写恢复、ABORT 原异常、同消息 replay 和 OneBot/QQ mock 合同。

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
