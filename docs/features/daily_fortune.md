# 每日运势

## 用户流程

用户每天调用一次“今日运势”，应用层以 `user_id + 日期` 保证每日一次。请求可通过 `operation_id` 重试，重复请求返回首次结果。

## 命令与别名

`今日运势`、`运势`，权限为 `user`。旧入口通过 `compatibility.commands.forward_daily_fortune` 转发。

## Web API

`GET/POST /api/v1/daily-fortune?user_id=<id>`，可选 `Idempotency-Key` 请求头；POST 必须通过 CSRF；响应使用统一 `{ok, data, request_id}` 格式。

## 数据模型与迁移

`game_db.daily_fortune_claims` 按 `(user_id, fortune_date)` 唯一。版本 `daily_fortune.001`，仓储首次使用时确保表存在。

## 事务与失败回滚

领取、每日唯一约束和 `operation_ledger` 在同一 SQLite Unit of Work 中提交。业务拒绝不改变资产；异常自动回滚。

## 定时任务

无后台任务。

## 配置项

`daily_fortune_enabled`（`XIUXIAN_DAILY_FORTUNE_ENABLED`）控制新实现灰度，默认开启。

## 适配器差异

NoneBot 适配器使用消息 ID 作为默认操作号；Web 适配器使用 `Idempotency-Key` 或生成一次性操作号。

## 测试与手工验收

覆盖首次领取、重复操作号、同日重复领取、无效输入和数据库回滚；通过 Flask test client 检查统一 API 响应。

## 灰度开关、回滚和已知限制

可在 manifest 注册阶段移除该功能以关闭入口。删除表前先备份数据库；随机数结果不承诺跨日期稳定。

## Manifest 清单
- `alias: 占卜`
- `alias: 卜卦`
- `alias: 求签`
- `alias: 算命`
- `route: GET /api/v1/daily-fortune`
