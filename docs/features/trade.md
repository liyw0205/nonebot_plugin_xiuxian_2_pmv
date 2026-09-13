# 交易与拍卖
## 用户流程
鬼市存取、上架/撤回、场次开始/收尾和现世购买统一提交操作号。
## 命令与别名
`交易`、`寄售`、`鬼市`。
## Web API
`POST /api/v1/trade/{deposit,withdraw,enqueue,dequeue,session_start,session_finish,purchase}`，权限 `user`。
## 数据模型与迁移
迁移 `trade.001`；交易库仍由旧仓储作为权威来源。
## 事务与失败回滚
跨库动作由旧服务保持原有补偿策略，新层记录 operation ledger。
## 定时任务
场次调度继续由兼容 scheduler 管理。
## 配置项
`trade_enabled`。
## 适配器差异
Web/命令不直接连接 trade_db。
## 测试与手工验收
覆盖重复竞价、余额不足、跨库失败和恢复重试。
## 灰度开关、回滚和已知限制
可关闭新交易边界回退旧入口；拍卖策略仍由兼容服务提供。

## Manifest 清单
- `route: POST /api/v1/trade/deposit`
- `route: POST /api/v1/trade/withdraw`
- `route: POST /api/v1/trade/enqueue`
- `route: POST /api/v1/trade/dequeue`
- `route: POST /api/v1/trade/session_start`
- `route: POST /api/v1/trade/session_finish`
- `route: POST /api/v1/trade/purchase`
