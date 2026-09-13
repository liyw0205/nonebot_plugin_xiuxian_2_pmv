# 修仙签到

## 用户流程

用户通过 `修仙签到` 或 `签到` 领取当日灵石。角色必须存在且当天尚未签到；成功后 `user_xiuxian.is_sign` 置为 `1` 并增加灵石。

## 命令与别名

- `修仙签到`
- `签到`

旧 NoneBot handler 保留原消息格式和鸿运抽奖副作用，但资产变更转发到 `SignInApplication`。

## Web API

`POST /api/v1/sign-in`，权限为 `user`，写请求需要 `X-CSRF-Token`。请求体至少包含 `user_id`；可用 `Idempotency-Key`（或 `operation_id`）指定幂等操作号。响应遵循统一 `{ok, data, request_id}` / `{ok, error, request_id}` 格式。

## 数据模型与迁移

- `user_xiuxian.is_sign`、`user_xiuxian.stone`：既有玩家资产表。
- `sign_in_operations`：操作号、用户和发放灵石的兼容投影。
- `operation_ledger`、`operation_audit`：统一操作幂等、审计和重放记录。
- 迁移版本：`sign_in.001`。

## 事务与失败回滚

同一 `DatabaseUnitOfWork` 中完成操作登记、签到状态更新、灵石增加和操作投影写入。相同操作号重试只返回首次结果；角色不存在或已经签到不会改变资产；写入异常回滚整笔事务并记录失败状态。

## 定时任务

本切片不新增任务。每日 `is_sign` 重置仍由旧调度兼容层执行，待调度垂直迁移后再替换。

## 配置项

- `XIUXIAN_SIGN_IN_ENABLED`：新签到实现灰度开关，默认开启。
- `sign_in_lower_limit`（`XIUXIAN_SIGN_IN_LOWER_LIMIT`）：灵石下限，默认 `100000`。
- `sign_in_upper_limit`（`XIUXIAN_SIGN_IN_UPPER_LIMIT`）：灵石上限，默认 `500000`。

## 适配器差异

应用层不依赖 NoneBot 或 Flask。命令适配器负责上下文和消息，Web 适配器负责 CSRF、权限、DTO 和 JSON 序列化。

## 测试与手工验收

服务测试覆盖成功、幂等重放、已签到、角色不存在和异常回滚；适配器测试覆盖 CSRF 和校验错误。启动隔离实例后先访问 `/health/ready`，再携带 CSRF token 调用签到 API。

## 灰度、回滚和已知限制

关闭 `XIUXIAN_SIGN_IN_ENABLED` 后 registry 不注册新命令/路由，但旧命令仍由兼容包提供。旧调度仍负责跨日重置；在完整发布周期没有旧入口命中并完成迁移证据前不会删除兼容层。

## 灰度开关、回滚和已知限制

灰度开关关闭后保留旧签到入口；跨日重置和鸿运抽奖仍由兼容调度与适配器维护。
