# Compensation

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migrations `legacy.compensation.001` through `legacy.compensation.006` record the slice in `game_db`. They pre-create the claim, invitation binding, reward catalog and versioned compensation definition ledgers; `.005` also adds `result_json` to older operation tables without dropping existing receipts, then imports compensation definitions and claims from JSON once. The import payload and timestamp are retained in `compensation_legacy_migrations`. `.006` imports gift/redeem definitions and claims once, stores source hashes rather than another full JSON snapshot, and reconciles redeem usage baselines with existing SQL claims. Gift/redeem definitions and claims are SQL-owned after migration; their request paths never read or write the legacy JSON. Definition and claim requests only inspect schema and migration receipts; they never create tables or retry imports. Binding, reward definition writes, claim, duplicate, usage-count, invitation replay and invitation threshold requests likewise do not create tables. A missing migration fails closed without mutating application data. Existing invitation JSON is imported idempotently on the first SQL mutation and remains a read fallback during compatibility.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`compensation_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Apply startup migrations before exercising the repository, then repeat the same operation ID to verify replay. On an un-migrated database, verify `schema_missing` and confirm no invitation binding, reward definition or claim tables are created by the request.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 兑换`
- `command: 兑换码列表`
- `command: 兑换码帮助`
- `command: 兑换码管理`
- `command: 删除兑换码`
- `command: 删除礼包`
- `command: 删除补偿`
- `command: 我的邀请`
- `command: 新增兑换码`
- `command: 新增礼包`
- `command: 新增补偿`
- `command: 清空兑换码`
- `command: 清空礼包`
- `command: 清空补偿`
- `command: 礼包列表`
- `command: 礼包帮助`
- `command: 礼包管理`
- `command: 补偿列表`
- `command: 补偿帮助`
- `command: 补偿管理`
- `command: 邀请人`
- `command: 邀请奖励列表`
- `command: 邀请奖励设置`
- `command: 邀请奖励领取`
- `command: 邀请帮助`
- `command: 邀请码`
- `command: 邀请管理`
- `command: 领取礼包`
- `command: 领取补偿`
