# 宗门成员与资产操作

## 用户流程

宗门成员加入、宗门商店兑换、主/副功法学习、炼体堂领奖、`领取宗门周常` 和确认解散宗门通过 `SectApplication` 进入 feature-owned application/repository。周常可单项领取或一次领取所有已完成目标。

## 命令与 Web API

- `加入宗门` -> `POST /api/v1/sect/join`
- `宗门商店兑换` -> `POST /api/v1/sect/purchase`
- `学习宗门功法` -> `POST /api/v1/sect/learn-main`
- `POST /api/v1/sect/learn-secondary`
- `领取宗门炼体堂` -> `POST /api/v1/sect/elixir/claim`
- `领取宗门周常 [目标名]`（命令入口）
- `确认解散宗门`（命令入口）

所有接口权限为 `user`，写请求支持 `Idempotency-Key`，业务拒绝返回统一错误码。

## 数据、回滚与限制

`sect.011` 在 game DB 创建/补齐 `sect_weekly_goal` 和 `sect_weekly_reward_operations`；`sect.012` 仅在 player DB 创建/补齐 `boss_limit.integral`，保留已有周常进度与玩家记录；`sect.013` 在 game DB 预建手动解散回执并保留已有操作记录。请求路径只检查 schema，不建表或补列。周常奖励由 `SectWeeklyRewardSqlRepository` 使用 attached game/player transaction 校验宗门归属、目标进度、重复领取与背包容量，再更新玩家、宗门、背包、BOSS 积分及领取标记。晚期 SQL 异常会回滚两个库；SQLite WAL 下 attached 多库事务不承诺进程/主机崩溃时的跨库原子性。手动解散由 `SectManualDisbandSqlRepository` 在 game-db immediate UoW 内重验宗主身份、解绑全部成员、删除宗门并写幂等回执；旧 `SectDisbandService` 仅保留兼容对照。关闭 `XIUXIAN_SECT_ENABLED` 即回退旧实现；宗门维护、任务及其他未迁移动作仍在兼容层。

## 命令与别名

保留 `加入宗门`、`宗门商店兑换`、`学习宗门功法`、`领取宗门炼体堂`、`领取宗门周常` 及历史别名。

## Web API

接口为 `/api/v1/sect/join`、`purchase`、`learn-main`、`learn-secondary` 和 `elixir/claim`，权限为 `user`，写请求需要 CSRF 与幂等键。

## 数据模型与迁移

`sect.001` 写迁移标记和统一 ledger；`sect.011`、`sect.013` 属于 game DB，`sect.012` 属于 player DB。历史周常进度和已有手动解散回执由迁移保留。

## 事务与失败回滚

周常领取以 `sect_weekly_reward_operations` 作为唯一幂等回执，并与奖励状态在同一 attached transaction 提交；不额外写入会跨事务留下 started 状态的通用 ledger。重复领取可重放奖励摘要，operation 冲突、进度变化、库存不足和晚期 SQL 异常不发放部分奖励。

## 定时任务

宗门日常重置和任务刷新由兼容生命周期统一调度，使用稳定 job ID。

## 配置项

`sect_enabled` / `XIUXIAN_SECT_ENABLED` 控制新切片，默认开启。

## 适配器差异

命令层负责事件和文案，Web 层负责 DTO、权限、CSRF 和统一错误码；application 不依赖框架。

## 测试与手工验收

覆盖加入、兑换、功法学习、炼体堂、周常领奖和手动解散的成功、拒绝、异常回滚和重复请求；事务测试还检查 migration 路由、旧数据保留及请求期不建表。

## 灰度开关、回滚和已知限制

关闭开关即回到兼容入口；宗门维护、事件进度写入和任务结算仍有旧实现边界。周常跨库请求事务能回滚 SQL 异常，但 attached SQLite WAL 的崩溃原子性不作保证。
