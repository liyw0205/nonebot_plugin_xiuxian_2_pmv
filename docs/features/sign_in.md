# 修仙签到

## 用户流程

用户通过 `修仙签到` 或 `签到` 领取当日灵石。角色必须存在且当天尚未签到；成功后 `user_xiuxian.is_sign` 置为 `1` 并增加灵石，同时在 `sign_in_daily` 写入当天回执。回复会追加本月签到日历与连续天数；同日已有回执时即使 `is_sign` 被重置清掉也不能再次领取。

## 命令与别名

- `修仙签到`
- `签到`
- `修仙补签 [日期]`（别名 `补签`、`签到补签`）
- `我的签到统计`（别名 `签到统计`、`签到记录`）

`我的签到统计` 是玩家只读视图：展示本人累计签到次数、本月签到天数与最近一次签到时间。
数据来自签到写路径本身维护的投影——`SignInApplicationEffects` 每次成功签到时
`statistics.record(..., event_key="修仙签到")` 写入 `sign_in_statistics_events`
并递增 `sign_in_statistics_projection.value`（两表均在 game_db）。
累计次数与 `SignInStatisticsRepository.value(user_id, "修仙签到")` 使用同一 SQL 与
同一 event_key，玩家视图与写入侧口径不可能分歧；本月天数按自然日去重，
`created_at` 用日期前缀匹配而非 `strftime`，避免毫秒/时区后缀解析失败。
`user_id` 只取登录会话，不接受参数查询他人；只读、不建表、不补列；
缺表或脏数据只降级不抛错。该命令同时登记在 `修仙帮助` 的「流水与总览」段。

旧 NoneBot handler 仅保留兼容消息适配；资产变更转发到 `SignInApplication`，签到后的鸿运、统计和任务投影由 feature-owned effects/application 处理。

### 补签（`修仙补签`）

`修仙补签` 补充本月内漏签的一天：空参数补**昨天**，`修仙补签 2026-11-01` 指定日期，
`修仙补签 1` 指定本月第 1 天。补签只修复签到日历与连续天数，**不发放签到灵石**，
也不参与鸿运、不触发签到任务、不改写 `is_sign`。

- 规则：只能补**当前业务月**内、**早于今天**的日期；今天请直接 `修仙签到`；未来日期、
  跨月日期、非法日期、已经有回执（真实签到或补签）的日期都会被拒绝，且不改变任何资产。
- 成本：`sign_in_makeup_cost`（默认 100000 灵石）在**同一事务**内从 `user_xiuxian.stone`
  扣减；余额不足直接拒绝，不会出现负余额。补签回执写入 `sign_in_daily`，
  `stone=0`、`source='makeup'`，因此与真实签到（`source='claim'`）可区分，
  唯一键 `(user_id, business_date)` 保证同一天只能签一次。
- 幂等：操作号 `sign_in.makeup` + 请求摘要进入统一 `operation_ledger`；同一操作号重放
  返回首次结果、不重复扣款；同一日期用不同操作号重试返回 `already_signed`。
- 投影：成功后 `SignInApplicationEffects.on_makeup` 只写
  `sign_in_statistics_events/projection`，`occurred_at` 取**补签日 12:00**，
  所以 `我的签到统计` 的累计次数与本月天数会把补签日计入正确月份。
- 降级：缺 `sign_in_daily` 或缺 `source` 列（未完成 `sign_in.005`）返回 `schema_missing`，
  请求路径不建表、不补列；旧签到领取不受影响。

## Web API

`POST /api/v1/sign-in`，权限为 `user`，写请求需要 `X-CSRF-Token`。请求体至少包含 `user_id`；可用 `Idempotency-Key`（或 `operation_id`）指定幂等操作号。响应遵循统一 `{ok, data, request_id}` / `{ok, error, request_id}` 格式。

## 数据模型与迁移

- `user_xiuxian.is_sign`、`user_xiuxian.stone`：既有玩家资产表。
- `sign_in_daily`：业务日签到回执（`user_id`、`business_date`、`stone`、`operation_id`），
  主键 `(user_id, business_date)`；按 `business_date` 建索引，供日历与重置保护读取。
- `sign_in_operations`：操作号、用户和发放灵石的兼容投影。
- `operation_ledger`、`operation_audit`：统一操作幂等、审计和重放记录。
- 迁移版本：`sign_in.001`、`sign_in.002`、`sign_in.003`、`sign_in.004`（每日回执）、
  `sign_in.005`（回执来源列）。

## 事务与失败回滚

同一 `DatabaseUnitOfWork` 中完成操作登记、签到状态更新、灵石增加、操作投影和
`sign_in_daily` 回执写入。相同操作号重试只返回首次结果；角色不存在、已经签到或当天已有
回执不会改变资产；写入异常回滚整笔事务并记录失败状态。`sign_in_daily` 缺失的旧库跳过日回执，
保持原有领取语义。

## 定时任务

每日 `is_sign` 重置由现有 scheduler job 委托给 `SignInApplication`，按 scheduler 业务日写入共享 ledger/audit；相同业务日重放不重复清零，缺 schema 时拒绝写入，不在请求期建表。`sign_in_daily` 存在时，重置只清没有当天回执的 `is_sign`，并在审计中记录保护行数；表缺失时保持旧的全表清零。

## 配置项

- `XIUXIAN_SIGN_IN_ENABLED`：新签到实现灰度开关，默认开启。
- `sign_in_lower_limit`（`XIUXIAN_SIGN_IN_LOWER_LIMIT`）：灵石下限，默认 `100000`。
- `sign_in_upper_limit`（`XIUXIAN_SIGN_IN_UPPER_LIMIT`）：灵石上限，默认 `500000`。
- `sign_in_makeup_cost`（`XIUXIAN_SIGN_IN_MAKEUP_COST`）：补签一天消耗的灵石，默认 `100000`；
  设为 0 时补签免费（仍受日期与回执唯一性约束）。

## 适配器差异

应用层不依赖 NoneBot 或 Flask。命令适配器负责上下文和消息，Web 适配器负责 CSRF、权限、DTO 和 JSON 序列化。

## 测试与手工验收

服务测试覆盖成功、幂等重放、已签到、角色不存在和异常回滚；适配器测试覆盖 CSRF 和校验错误。启动隔离实例后先访问 `/health/ready`，再携带 CSRF token 调用签到 API。

## 灰度、回滚和已知限制

关闭 `XIUXIAN_SIGN_IN_ENABLED` 后 registry 不注册新命令/路由，但旧命令仍由兼容包提供。
`sign_in_daily` 从 `sign_in.004` 上线起记录，不回填历史日期：迁移前的 `is_sign=1` 没有业务日期回执，
无法区分新旧签到；首次重置任务若延迟到用户已签到之后，没有当天回执的旧标记仍会被清零，
只有写入 `sign_in_daily` 的当天签到受保护。日历不会显示功能上线前的签到。日历与连续天数只展示，
不发放额外奖励；`修仙补签` 只用于**本月内**漏签的一天，不提供跨月、跨年或历史回填，
补签本身也不发放签到灵石。当前 scheduler timezone 由部署配置决定，业务日与
`sign_in_clock` 同源，代码没有新增固定时区。

## 灰度开关、回滚和已知限制

关闭 `XIUXIAN_SIGN_IN_ENABLED` 只禁用新签到入口，不会自动启用旧 matcher；兼容回滚还需显式设置 `XIUXIAN_SIGN_IN_LEGACY_HANDLER=true`。默认 lottery 使用 `LotteryApplication`。`settle_lottery` 仍接受显式 `legacy_settle` callable，供旧安装或人工回滚使用，不由默认 composition root 注入旧 lottery service。
