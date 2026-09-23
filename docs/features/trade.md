# 交易与拍卖

`鬼市存灵石` 默认通过 `TradeApplication.guishi_deposit` 和 feature-owned
cross-database repository 结算。操作记录在 game DB 的
`trade_guishi_deposit_operations`，鬼市余额投影保留在 trade DB 的
`guishi_info`；启动迁移 `trade.002`/`trade.003` 先建立两端 schema。
`鬼市取灵石` 的周末和手续费结算仍是独立的兼容切片。
## 用户流程
鬼市存取、上架/撤回、场次开始/收尾和现世购买统一提交操作号。
## 命令与别名
`交易`、`寄售`、`鬼市`。
## Web API
`POST /api/v1/trade/{deposit,withdraw,enqueue,dequeue,session_start,session_finish,purchase}`，权限 `user`。
## 数据模型与迁移
`trade.001` 保留 feature 基线；`trade.002` 在 game DB 创建鬼市存入
operation 表，`trade.003` 只在 trade DB 创建或补齐 `guishi_info`。
## 事务与失败回滚
鬼市存入使用 game DB 主事务附加 trade DB，玩家扣款、余额投影和 operation
写入要么全部提交、要么全部回滚。取出、订单和拍卖路径仍由兼容服务持有。
## 定时任务
场次调度继续由兼容 scheduler 管理。
## 配置项
`trade_enabled`。
## 适配器差异
Web/命令不直接连接 trade_db。
## 测试与手工验收
覆盖重复竞价、余额不足、跨库失败和恢复重试。
## 灰度开关、回滚和已知限制
已完成的鬼市存入入口没有隐式回退；旧 `GuishiStoneService` 仅保留给鬼市
取出兼容路径。拍卖策略仍由兼容服务提供。

## Manifest 清单
- `route: POST /api/v1/trade/deposit`
- `route: POST /api/v1/trade/withdraw`
- `route: POST /api/v1/trade/enqueue`
- `route: POST /api/v1/trade/dequeue`
- `route: POST /api/v1/trade/session_start`
- `route: POST /api/v1/trade/session_finish`
- `route: POST /api/v1/trade/purchase`
