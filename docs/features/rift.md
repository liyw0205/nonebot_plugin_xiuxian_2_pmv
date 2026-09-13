# 裂隙世界
## 用户流程
世界生成、进入、终止、事件奖励、加速和结算均有稳定操作号。
## 命令与别名
`裂隙`、`进入裂隙`、`裂隙探索`。
## Web API
`POST /api/v1/rift/{generate,enter,terminate,event_settle,speedup,settle}`，权限 `user`。
## 数据模型与迁移
迁移 `rift.001`；历史裂隙状态通过兼容仓储投影。
## 事务与失败回滚
跨 game/player 库动作由旧服务保证回滚，新层记录失败和重试。
## 定时任务
世界生成继续由兼容 scheduler 触发。
## 配置项
`rift_enabled`。
## 适配器差异
业务层不依赖 NoneBot/Flask。
## 测试与手工验收
覆盖世界版本冲突、重复进入、道具不足和结算重放。
## 灰度开关、回滚和已知限制
关闭开关回退旧裂隙入口；生成计划仍由旧配置提供。

## Manifest 清单
- `route: POST /api/v1/rift/generate`
- `route: POST /api/v1/rift/enter`
- `route: POST /api/v1/rift/terminate`
- `route: POST /api/v1/rift/event_settle`
- `route: POST /api/v1/rift/speedup`
- `route: POST /api/v1/rift/settle`
