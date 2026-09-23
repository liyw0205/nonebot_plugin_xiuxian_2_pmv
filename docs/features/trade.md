# 交易与拍卖

`鬼市存灵石`、`鬼市取灵石`、`鬼市求购` 和 `鬼市摆摊` 默认通过 `TradeApplication` 的
feature-owned use case 结算。存取操作记录在 game DB 的
`trade_guishi_deposit_operations`/`trade_guishi_withdraw_operations`，求购创建操作记录
在 trade DB 的 `guishi_order_create_operations`；鬼市余额和订单投影保留在 trade DB
的 `guishi_info`/`guishi_item`。启动迁移 `trade.002`/`trade.003`/`trade.004`/`trade.005`
按数据库路由建立 schema。取出规则由注入 `Clock` 的 application 决定周末开放窗口，
并保留历史动态手续费公式。
## 用户流程
鬼市存取、求购创建、摆摊创建、上架/撤回、场次开始/收尾和现世购买统一提交操作号。
## 命令与别名
`交易`、`寄售`、`鬼市`。
## Web API
`POST /api/v1/trade/{deposit,withdraw,enqueue,dequeue,session_start,session_finish,purchase}`，权限 `user`。
## 数据模型与迁移
`trade.001` 保留 feature 基线；`trade.002`/`trade.004` 在 game DB 创建鬼市存入/
取出 operation 表；`trade.003` 只在 trade DB 创建或补齐 `guishi_info`；`trade.005`
只在 trade DB 创建求购创建 operation 表。生产请求路径不隐式建表。
## 事务与失败回滚
鬼市存取使用 game DB 主事务附加 trade DB，玩家钱包、余额投影和 operation
写入要么全部提交、要么全部回滚。四种操作均保留 canonical payload replay/conflict
和旧 `guishi_stone_operations` 回放兼容；求购创建在 trade DB 的 immediate transaction
中原子冻结余额、插入订单和记录 operation，并保留历史 numeric 订单 ID/payload 回放。
摆摊创建在 game DB 主事务附加 trade DB，原子扣减可交易库存、插入订单和记录 operation，
保留 `goods_num - state` 可交易量、`bind_num` 下限、库存 CAS 和历史 numeric 订单 ID。
摆摊、求购撤销、撮合、过期清理、取回寄存物品和拍卖队列/场次仍由兼容路径持有。
## 定时任务
场次调度继续由兼容 scheduler 管理。
## 配置项
`trade_enabled`。
## 适配器差异
Web/命令不直接连接 trade_db。
## 测试与手工验收
覆盖求购/摆摊重复请求、订单 ID 冲突、订单上限、余额/库存不足、跨库失败和恢复重试。
## 灰度开关、回滚和已知限制
已完成的鬼市存取、求购创建和摆摊创建入口没有隐式回退；旧 `GuishiStoneService` 仅保留给尚未
迁移的交易兼容路径。拍卖策略、订单后续生命周期和寄存物品处理仍由兼容服务提供。

## Manifest 清单
- `route: POST /api/v1/trade/deposit`
- `route: POST /api/v1/trade/withdraw`
- `route: POST /api/v1/trade/enqueue`
- `route: POST /api/v1/trade/dequeue`
- `route: POST /api/v1/trade/session_start`
- `route: POST /api/v1/trade/session_finish`
- `route: POST /api/v1/trade/purchase`
