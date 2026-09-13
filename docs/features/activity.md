# Activity lifecycle

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Migration `legacy.activity.001` records the slice in `game_db`; the feature-owned repository and explicit service port now own the operation boundary while legacy tables remain the compatibility data source.

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`activity_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 关闭活动`
- `command: 开启活动`
- `command: 活动`
- `alias: 活动信息`
- `alias: 活动日程`
- `alias: 活动进度`
- `command: 活动任务`
- `alias: 活动日常`
- `alias: 活动目标`
- `command: 活动任务领取`
- `alias: 领取活动任务`
- `command: 活动兑换`
- `alias: 集字兑换`
- `command: 活动商店`
- `alias: 积分商店`
- `command: 活动奖励`
- `command: 活动帮助`
- `command: 活动战令`
- `alias: 活动活跃`
- `alias: 活动通行证`
- `command: 活动战令领取`
- `alias: 领取活动战令`
- `alias: 领取活动通行证`
- `command: 活动排行`
- `command: 活动玩法`
- `command: 活动积分`
- `alias: 积分活动`
- `command: 活动签到`
- `alias: 节日签到`
- `command: 活动管理`
- `command: 活动背包`
- `alias: 活动字牌`
- `alias: 集字背包`
- `command: 活动讨伐`
- `alias: 使用烟花`
- `alias: 使用爆竹`
- `alias: 活动首领攻击`
- `command: 活动购买`
- `alias: 活动兑换商品`
- `alias: 活动商城兑换`
- `command: 活动首领`
- `alias: 活动BOSS`
- `command: 活动首领排行`
- `alias: 活动BOSS排行`
- `alias: 首领伤害榜`
- `command: 活动首领领奖`
- `alias: 领取首领奖励`
- `alias: 首领领奖`
