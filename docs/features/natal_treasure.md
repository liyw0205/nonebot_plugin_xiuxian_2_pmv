# 本命法宝
## 用户流程
觉醒、重塑、养成、升级道纹、铭刻和遗忘均通过统一操作号提交，旧法宝数据由兼容仓储读取。
## 命令与别名
`本命法宝`、`觉醒本命法宝`、`重塑本命法宝`。
## Web API
`POST /api/v1/natal-treasure/{awaken,reawaken,train,upgrade,engrave,forget}`，权限 `user`，使用 `Idempotency-Key`。
## 数据模型与迁移
迁移标记 `natal_treasure.001`；历史表仍由旧服务维护。
## 事务与失败回滚
新层先写 operation ledger，旧事务失败时记录 failed 并可重试。
## 定时任务
无。
## 配置项
`natal_treasure_enabled`。
## 适配器差异
消息适配器只负责解析用户和回复计划。
## 测试与手工验收
应用层 fake repository、重复操作号和拒绝结果测试。
## 灰度开关、回滚和已知限制
关闭灰度开关即可回到旧入口；具体数值规则仍由兼容服务提供。

## Manifest 清单
- `route: POST /api/v1/natal-treasure/awaken`
- `route: POST /api/v1/natal-treasure/reawaken`
- `route: POST /api/v1/natal-treasure/train`
- `route: POST /api/v1/natal-treasure/upgrade`
- `route: POST /api/v1/natal-treasure/engrave`
- `route: POST /api/v1/natal-treasure/forget`
