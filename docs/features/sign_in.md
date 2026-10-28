# 修仙签到

## 用户流程

用户通过 `修仙签到` 或 `签到` 领取当日灵石。角色必须存在且当天尚未签到；成功后 `user_xiuxian.is_sign` 置为 `1` 并增加灵石，同时在 `sign_in_daily` 写入当天回执。回复会追加本月签到日历与连续天数；同日已有回执时即使 `is_sign` 被重置清掉也不能再次领取。

## 命令与别名

- `修仙签到`
- `签到`

旧 NoneBot handler 仅保留兼容消息适配；资产变更转发到 `SignInApplication`，签到后的鸿运、统计和任务投影由 feature-owned effects/application 处理。

## Web API

`POST /api/v1/sign-in`，权限为 `user`，写请求需要 `X-CSRF-Token`。请求体至少包含 `user_id`；可用 `Idempotency-Key`（或 `operation_id`）指定幂等操作号。响应遵循统一 `{ok, data, request_id}` / `{ok, error, request_id}` 格式。

## 数据模型与迁移

- `user_xiuxian.is_sign`、`user_xiuxian.stone`：既有玩家资产表。
- `sign_in_daily`：业务日签到回执（`user_id`、`business_date`、`stone`、`operation_id`），
  主键 `(user_id, business_date)`；按 `business_date` 建索引，供日历与重置保护读取。
- `sign_in_operations`：操作号、用户和发放灵石的兼容投影。
- `operation_ledger`、`operation_audit`：统一操作幂等、审计和重放记录。
- 迁移版本：`sign_in.001`、`sign_in.002`、`sign_in.003`、`sign_in.004`（每日回执）。

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

## 适配器差异

应用层不依赖 NoneBot 或 Flask。命令适配器负责上下文和消息，Web 适配器负责 CSRF、权限、DTO 和 JSON 序列化。

## 测试与手工验收

服务测试覆盖成功、幂等重放、已签到、角色不存在和异常回滚；适配器测试覆盖 CSRF 和校验错误。启动隔离实例后先访问 `/health/ready`，再携带 CSRF token 调用签到 API。

## 灰度、回滚和已知限制

关闭 `XIUXIAN_SIGN_IN_ENABLED` 后 registry 不注册新命令/路由，但旧命令仍由兼容包提供。
`sign_in_daily` 从 `sign_in.004` 上线起记录，不回填历史日期：旧 `is_sign=1` 没有日期回执，
仍会被下一次重置按旧语义清零，日历也不会显示功能上线前的签到。日历与连续天数只展示，
不发放额外奖励；没有补签入口。当前 scheduler timezone 由部署配置决定，业务日与
`sign_in_clock` 同源，代码没有新增固定时区。

## 灰度开关、回滚和已知限制

关闭 `XIUXIAN_SIGN_IN_ENABLED` 只禁用新签到入口，不会自动启用旧 matcher；兼容回滚还需显式设置 `XIUXIAN_SIGN_IN_LEGACY_HANDLER=true`。默认 lottery 使用 `LotteryApplication`。`settle_lottery` 仍接受显式 `legacy_settle` callable，供旧安装或人工回滚使用，不由默认 composition root 注入旧 lottery service。
