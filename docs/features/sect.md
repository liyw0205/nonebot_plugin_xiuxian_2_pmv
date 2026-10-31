# 宗门成员与资产操作

## 用户流程

宗门成员加入、宗门商店兑换、主/副功法学习、炼体堂领奖、`领取宗门周常` 和确认解散宗门通过 `SectApplication` 进入 feature-owned application/repository。默认宗门任务状态查询、接取与刷新也通过 `SectApplication`，接取/刷新使用操作回执幂等；周常可单项领取或一次领取所有已完成目标。

## 命令与 Web API

- `加入宗门` -> `POST /api/v1/sect/join`
- `宗门商店兑换` -> `POST /api/v1/sect/purchase`
- `学习宗门功法` -> `POST /api/v1/sect/learn-main`
- `POST /api/v1/sect/learn-secondary`
- `领取宗门炼体堂` -> `POST /api/v1/sect/elixir/claim`
- `我的宗门贡献流水`（命令入口，只读个人贡献收支）
- `领取宗门周常 [目标名]`（命令入口）
- `确认解散宗门`（命令入口）

所有接口权限为 `user`，写请求支持 `Idempotency-Key`，业务拒绝返回统一错误码。

## 数据、回滚与限制

`sect.011` 在 game DB 创建/补齐 `sect_weekly_goal` 和 `sect_weekly_reward_operations`；`sect.012` 仅在 player DB 创建/补齐 `boss_limit.integral`，保留已有周常进度与玩家记录；`sect.013` 在 game DB 预建手动解散回执并保留已有操作记录；`sect.016` 预建宗门任务状态表；`sect.017` 在 game DB 预建接取/刷新幂等回执；`sect.018` 在 game DB 预建任务结算回执。周常、手动解散及任务状态/接取/刷新/结算请求只检查已登记 schema，不建表或补列。任务结算时间由注入的 Clock 提供。周常奖励由 `SectWeeklyRewardSqlRepository` 使用 attached game/player transaction 校验宗门归属、目标进度、重复领取与背包容量，再更新玩家、宗门、背包、BOSS 积分及领取标记。晚期 SQL 异常会回滚两个库；SQLite WAL 下 attached 多库事务不承诺进程/主机崩溃时的跨库原子性。手动解散由 `SectManualDisbandSqlRepository` 在 game-db immediate UoW 内重验宗主身份、解绑全部成员、删除宗门并写幂等回执；旧 `SectDisbandService` 仅保留兼容对照。关闭 `XIUXIAN_SECT_ENABLED` 即回退兼容入口。

## 命令与别名

保留 `加入宗门`、`宗门商店兑换`、`学习宗门功法`、`领取宗门炼体堂`、`领取宗门周常` 及历史别名。
新增玩家只读命令 `我的宗门贡献流水`（别名 `宗门贡献流水`、`贡献流水`，priority 7）：
展示本人近 7 天宗门贡献的获得/消耗/净变化/笔数、主要来源与主要去向各前 3、
最近 5 笔明细，以及当前贡献余额与所属宗门。数据来自共享 `economy_log` 的
`sect_contribution_delta`（由 `宗门捐献`、`宗门商店兑换`、宗门设施升级等真实写路径记录），
时间窗只用下界以避免 UTC/本地时间混用误删「刚刚发生」的行。`user_id` 只取登录会话，
不接受参数指定他人；只读、无新表/迁移/配置键，`economy_log` 缺列或库不可读时降级为提示文案。
该命令已登记在 `宗门帮助` 的「任务与商店」段，玩家可从帮助页直接发现。

## Web API

接口为 `/api/v1/sect/join`、`purchase`、`learn-main`、`learn-secondary` 和 `elixir/claim`，权限为 `user`，写请求需要 CSRF 与幂等键。

## 数据模型与迁移

`sect.001` 写迁移标记和统一 ledger；`sect.011`、`sect.013`、`sect.016`、`sect.017`、`sect.018` 属于 game DB，`sect.012` 属于 player DB。历史周常进度、已有手动解散回执和任务操作回执由幂等迁移保留。`sect_fairyland.002` 在 player DB 预建炼体堂领取回执与每日状态表，并将旧按宗门展开的领取日期回填到 `sect_fairyland_claim_days`；信息查询和领奖均读取/更新该 feature-owned 状态，不在请求期建表。

## 事务与失败回滚

周常领取以 `sect_weekly_reward_operations` 作为唯一幂等回执，并与奖励状态在同一 attached transaction 提交；不额外写入会跨事务留下 started 状态的通用 ledger。重复领取可重放奖励摘要，operation 冲突、进度变化、库存不足和晚期 SQL 异常不发放部分奖励。

## 宗门命令合同

主/副功法搜寻（`宗门功法搜寻`、`宗门神通搜寻`）经
`SectApplication.apply_buff_search(..., owner_position=...)` 进入
`SectBuffSearchSqlRepository`，在 `sects` 上写**真实列** `mainbuff` / `secbuff`
（映射常量 `_BUFF_COLUMNS = {"main": "mainbuff", "secondary": "secbuff"}`）。
历史上这里写成了两个并不存在的 `sect_` 前缀列名：SQLite 把未解析标识符在 SELECT
里当字符串字面量、在 UPDATE 里抛 `OperationalError`，因此命令在生产数据上必炸。
宗主校验比较调用方传入的 `owner_position`（不再硬编码 0），以便非 0 宗主位的宗门配置生效；
同一 `operation_id` 重放返回 `duplicate` 且不二次扣减宗门灵石与资材。

`sects` / `user_xiuxian` 的列集以 `xiuxian_config.py` 的 `sql_sects`、
`sql_user_xiuxian` 为权威：`sects` 为
`sect_id,sect_name,sect_owner,sect_scale,sect_used_stone,join_open,closed,combat_power,
sect_fairyland,sect_materials,mainbuff,secbuff,elixir_room_level`。

`SectMutationResult`（`dict` 子类）的成功谓词 `applied` 覆盖全部真实成功状态：
`applied`、`charged`、`created`、`opened`、`transferred`、`settled`，以及既有的
`claimed`、`closed`、`disbanded`、`duplicate`、`granted`、`inherited`、`upgraded`。
谓词遗漏真实成功状态会把成功渲染成失败并要求玩家重试（`创建宗门` 的 `created`、
`宗门改名` 计费的 `charged`、`开放宗门加入` 的 `opened`、`宗主传位` 的 `transferred`、
`宗门任务完成` 的 `settled` 都曾受影响）。命令层统一读 `.applied`；`宗主传位`
曾误读不存在的 `.succeeded`，会抛 `AttributeError`。

失败状态（`not_owner`、`stone_insufficient`、`materials_insufficient`、
`scale_insufficient`、`state_changed`、`name_exists`、`already_member`、
`task_missing`、`membership_changed`、`schema_missing` 等）一律不算成功，且不产生副作用。

## 定时任务

宗门日常重置和任务刷新由兼容生命周期统一调度，使用稳定 job ID。

## 配置项

`sect_enabled` / `XIUXIAN_SECT_ENABLED` 控制新切片，默认开启。

## 适配器差异

命令层负责事件和文案，Web 层负责 DTO、权限、CSRF 和统一错误码；application 不依赖框架。

## 测试与手工验收

覆盖加入、兑换、功法学习、炼体堂、任务接取/刷新、周常领奖和手动解散的成功、拒绝、异常回滚和重复请求；事务测试还检查 migration 路由、旧数据保留及请求期不建表。

## 灰度开关、回滚和已知限制

关闭开关即回到兼容入口；其他未迁移事务仍保留兼容实现。周常跨库请求事务能回滚 SQL 异常，但 attached SQLite WAL 的崩溃原子性不作保证。
