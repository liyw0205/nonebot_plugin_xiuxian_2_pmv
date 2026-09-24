# 交易与拍卖

`鬼市存灵石`、`鬼市取灵石`、`鬼市求购`、`鬼市摆摊`、`鬼市取消求购` 和 `鬼市收摊`
默认通过 `TradeApplication` 的
feature-owned use case 结算。存取操作记录在 game DB 的
`trade_guishi_deposit_operations`/`trade_guishi_withdraw_operations`，求购创建操作记录
在 trade DB 的 `guishi_order_create_operations`，撤销操作记录在
`guishi_order_cancel_operations`；鬼市余额和订单投影保留在 trade DB 的
`guishi_info`/`guishi_item`。启动迁移 `trade.002`/`trade.003`/`trade.004`/`trade.005`/
`trade.006`/`trade.010`
按数据库路由建立 schema。取出规则由注入 `Clock` 的 application 决定周末开放窗口，
并保留历史动态手续费公式。

普通、自动、快速和系统 `仙肆上架` 已通过 `TradeApplication` 结算。game DB immediate transaction
原子创建 listing 并写入 operation；普通/快速上架还会扣除手续费和可交易库存，自动上架会先
校验并扣除 30 点体力及整份计划的费用/库存，快速上架会先校验并扣除 10 点体力。普通/快速操作记录在
`xianshi_listing_operations`，自动计划记录在 `xianshi_plan_listing_operations`。`trade.010`/
`trade.011` 只路由到 game DB，并为历史 operation 表补齐 `stamina_cost` 列。系统 listing 也记录
在 `xianshi_listing_operations`；`quantity=-1` 无限量和指定数量均保持原样，且不扣玩家资产。
## 用户流程
鬼市存取、求购/摆摊创建与撤销、上架/撤回、场次开始/收尾和现世购买统一提交操作号。
## 命令与别名
`交易`、`寄售`、`鬼市`。
## Web API
`POST /api/v1/trade/{deposit,withdraw,enqueue,dequeue,session_start,session_finish,purchase}`，权限 `user`。
## 数据模型与迁移
`trade.001` 保留 feature 基线；`trade.002`/`trade.004` 在 game DB 创建鬼市存入/
取出 operation 表；`trade.003` 只在 trade DB 创建或补齐 `guishi_info`；`trade.005`/
`trade.006` 只在 trade DB 创建求购创建/订单撤销 operation 表；`trade.010`/`trade.011` 只在
game DB 创建或升级仙肆普通、自动、快速、系统上架 operation 表，`trade.012` 只在 game DB
创建仙肆撤架 operation 表。生产请求路径不隐式建表。
## 事务与失败回滚
鬼市存取使用 game DB 主事务附加 trade DB，玩家钱包、余额投影和 operation
写入要么全部提交、要么全部回滚。已迁移操作均保留 canonical payload replay/conflict
和旧 `guishi_stone_operations` 回放兼容；求购创建在 trade DB 的 immediate transaction
中原子冻结余额、插入订单和记录 operation，并保留历史 numeric 订单 ID/payload 回放。
摆摊创建在 game DB 主事务附加 trade DB，原子扣减可交易库存、插入订单和记录 operation，
保留 `goods_num - state` 可交易量、`bind_num` 下限、库存 CAS 和历史 numeric 订单 ID。
求购撤销在 trade DB 原子退回未成交冻结灵石并删除订单；摆摊收摊在 game DB 主事务附加
trade DB，按背包上限原子退回未售库存、删除订单并记录 operation。求购/摆摊撮合、过期清理和
寄存物品取回，以及拍卖竞价、等待区、场次开始/交接与结算均已切换到 feature application。
## 定时任务
场次调度继续由兼容 scheduler 管理。
## 配置项
`trade_enabled`。
## 适配器差异
Web/命令不直接连接 trade_db。
## 测试与手工验收
覆盖求购/摆摊创建与撤销重复请求、订单 ID 冲突、订单上限、余额/库存不足、背包满、
跨库失败和恢复重试。
## 灰度开关、回滚和已知限制
已完成的鬼市存取、求购/摆摊创建和撤销入口没有隐式回退；旧 `GuishiStoneService` 仅保留给尚未
迁移的交易兼容路径。仙肆普通、自动、快速和系统上架已切换。拍卖竞价、等待区、场次开始/交接与
结算已由 feature application 处理；场次自动调度仍由兼容 scheduler 触发，其余未覆盖拍卖策略与
命令路径仍待迁移。

## Manifest 清单
- `route: POST /api/v1/trade/deposit`
- `route: POST /api/v1/trade/withdraw`
- `route: POST /api/v1/trade/enqueue`
- `route: POST /api/v1/trade/dequeue`
- `route: POST /api/v1/trade/session_start`
- `route: POST /api/v1/trade/session_finish`
- `route: POST /api/v1/trade/purchase`
