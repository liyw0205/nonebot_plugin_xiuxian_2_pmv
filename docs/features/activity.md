# Activity lifecycle

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 当前垂直切片：活动中心

`活动中心`（别名 `活动大厅`）是本阶段的最小只读入口。它从版本化活动配置和一次
玩家活动快照生成活动卡片，显示 `未开始`、`进行中`、`已结束` 或当前玩家存在可领取
任务/战令奖励时的 `奖励领取`。活动名称、时间、描述和奖励摘要均来自配置数据，handler
不内嵌具体活动内容；奖励领取仍交给既有任务、战令和统一活动领取入口。

QQ 使用现有原生 Markdown、蓝字命令链接和可选键盘提取；OneBot 或 Markdown 不可用时，
同一文案经公共发送入口降级为纯文本。此切片只读，不新增表、不执行奖励发放、不引入
后台任务，也不覆盖集字、积分商店或活动首领的既有结算逻辑。

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
Manifest 保留历史 migration 标记 `legacy.activity.001`；当前启动 migration `activity_state.001/.002` 在 `game_db` 预建活动玩法表，并从 `data/activity/activity.db` 以只读、每批最多 200 行的方式回填。目标身份冲突、旧 schema 不完整或磁盘空间预检不通过时拒绝继续；迁移不会删除旧文件，也不在请求路径建表。活动模块导入不再抢先校验目标表，避免 startup migration 尚未运行时阻断新库/旧库升级。默认签到、集字、积分、道具、任务、战令和活动首领状态读写统一使用 `game_db`。

`活动背包` 通过 `ActivityApplication.read_model` 使用 feature-owned 只读查询，一次 game_db 快照批量读取当前集字活动的库存、兑换次数和保底进度；旧服务 builder 保留为兼容路径。`活动兑换` 的默认 matcher 经 `ActivityApplication -> ActivityRepository -> ActivityCollectExchangeSqlRepository` 结算，复用 `activity_collect_exchange_operations`，在同一 game_db `BEGIN IMMEDIATE` 中原子更新字牌、领取次数和玩家资产。默认请求不再调用 `ensure_activity_files()`，也不创建新 schema；旧 `ActivityCollectExchangeService` 仅保留显式兼容调用。

旧 `activity.db` 仍承载 `activity_config_*` 配置/事件数据及对应管理路径，并保留作备份与迁移核验来源；不能将整个文件描述为只读或缓存。活动积分、战令、集字掉落和首领战斗日志均参与每日上限、资格判断或审计，不得按缓存清理。

## 事务与失败回滚
Mutating calls carry an `operation_id` and are recorded in the operation ledger. Collect exchange also commits its existing business receipt in the same transaction as inventory and asset changes. Only this receipt-backed action opts into retrying a matching interrupted `started` operation; recovery checks the business receipt before revalidating current activity configuration, while a missing receipt proceeds through the current request once. Historical two-field receipts remain replayable after backfill. Disable the feature flag or restore the pre-migration backup to roll back.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`activity_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the feature application test and the full architecture gate. Repeat the same operation ID to verify replay.

本阶段轻量回归：`tests/test_activity_center.py` 覆盖四种生命周期状态、配置驱动活动卡片、
奖励可领取状态和 Markdown 链接的纯文本降级；未运行全量长测或真实 QQ 消息发送。

## 灰度开关、回滚和已知限制
Legacy algorithms and schemas remain behind the repository adapter for one complete release cycle; the compatibility hit counter determines when removal is safe.

## Manifest 清单
- `command: 活动中心`
- `alias: 活动大厅`
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
