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

鬼市订单列表与账户信息读取由 `TradeApplication` 的只读 query repository 承载，短生命周期
打开 `trade_db`，不保留缓存连接，也不在请求中补建 schema；数据库或投影表缺失时维持历史空结果。

普通、自动、快速和系统 `仙肆上架` 已通过 `TradeApplication` 结算。game DB immediate transaction
原子创建 listing 并写入 operation；普通/快速上架还会扣除手续费和可交易库存，自动上架会先
校验并扣除 30 点体力及整份计划的费用/库存，快速上架会先校验并扣除 10 点体力。普通/快速操作记录在
`xianshi_listing_operations`，自动计划记录在 `xianshi_plan_listing_operations`。`trade.010`/
`trade.011` 只路由到 game DB，并为历史 operation 表补齐 `stamina_cost` 列。系统 listing 也记录
在 `xianshi_listing_operations`；`quantity=-1` 无限量和指定数量均保持原样，且不扣玩家资产。
## 用户流程
鬼市存取、求购/摆摊创建与撤销、上架/撤回、场次开始/收尾和现世购买统一提交操作号。

鬼市订单生命周期（求购与摆摊）由 feature 层闭环：

- 求购创建按 `单价 × 数量` 冻结鬼市余额；到期时刻取创建操作行的 `created_at`
  （`guishi_order_create_operations`，与订单同事务写入），规则是**创建日次日
  `GUISHI_BAITAN_END_HOUR`（默认 8）点**，由纯规则模块
  `features/trade/guishi_lifecycle.py` 计算。到期由
  `GuishiQiugouExpiryService` 逐条调用既有 `cancel_qiugou` 取消并按
  `(数量 - 已成交) × 求购价` 退款，操作号 `guishi-expire-qiugou:{order_id}`
  稳定可重放。清理触发点有两处：`clear_expired_baitan_orders_job`（同一 8 点
  任务，先做全服求购到期，再做摆摊清理）与 `鬼市信息` 的本人访问期收敛。
- 摆摊创建后立即调用 `GuishiListingFillService`，按同物品、求购价不低于摆摊价、
  非本人、`(数量 - 已成交) > 0` 选候选，按创建时刻升序（无锚点排最后）逐条走
  既有 `guishi_match`，操作号 `guishi-fill:{摆摊ID}:{求购ID}`。摆摊售罄或没有
  候选即停止；成交摘要与 `鬼市成交` 统计同时回到卖家回执。
- 成交按摆摊价结算（`amount = 成交数量 × 摆摊价`），求购冻结价高于成交价的
  差价 `price_refund` 在同事务退回买家鬼市余额，买家最终支出恒等于
  `成交数量 × 成交价`。`GuishiMatchResult.price_refund` 带默认值，历史
  operation 行重放按 0 处理。
- `鬼市信息` 用 `GuishiOrderStatusService` 一次只读快照取本人订单与锚点，再由
  纯渲染 `guishi_order_view.py` 输出到期时刻与剩余时长；无锚点历史订单显示
  `到期：未知（历史订单）`。

## 命令与别名
`交易`、`寄售`、`鬼市`。
## Web API
`POST /api/v1/trade/{deposit,withdraw,enqueue,dequeue,purchase}` 要求 `user`；`session_start` 和
`session_finish` 要求 `admin`。所有写请求需提供 CSRF token，以及 `Idempotency-Key` header 或
payload 中的 `operation_id`。

- `enqueue` 接收 `item_id`、`item_name`、`start_price`、`user_name`；每人上架数由服务端限制。
- `dequeue` 接收 `item_id`、`item_type`。
- `session_start` 接收 `system_items_config`、`duration_hours`，可选 `system_item_count`（默认 5）；
  管理员配置仅用于此次开场，不写入持久配置。
- `session_finish` 可接收 `end_time`、`fee_rate`、`item_types`；缺省结束时间取注入的 Clock，手续费率默认 0.2。
  结算委托给 `AuctionSettlementApplication`，统计与日志由其 outbox effects 处理。
## 数据模型与迁移
`trade.001` 保留 feature 基线；`trade.002`/`trade.004` 在 game DB 创建鬼市存入/
取出 operation 表；`trade.003` 只在 trade DB 创建或补齐 `guishi_info`；`trade.005`/
`trade.006` 只在 trade DB 创建求购创建/订单撤销 operation 表；`trade.010`/`trade.011` 只在
game DB 创建或升级仙肆普通、自动、快速、系统上架 operation 表，`trade.012` 只在 game DB
创建仙肆撤架 operation 表。鬼市只读查询不拥有 schema migration。生产请求路径不隐式建表。
## 事务与失败回滚
鬼市存取使用 game DB 主事务附加 trade DB，玩家钱包、余额投影和 operation
写入要么全部提交、要么全部回滚。已迁移操作均保留 canonical payload replay/conflict
和旧 `guishi_stone_operations` 回放兼容；求购创建在 trade DB 的 immediate transaction
中原子冻结余额、插入订单和记录 operation，并保留历史 numeric 订单 ID/payload 回放。
摆摊创建在 game DB 主事务附加 trade DB，原子扣减可交易库存、插入订单和记录 operation，
保留 `goods_num - state` 可交易量、`bind_num` 下限、库存 CAS 和历史 numeric 订单 ID。
求购撤销在 trade DB 原子退回未成交冻结灵石并删除订单；摆摊收摊在 game DB 主事务附加
trade DB，按背包上限原子退回未售库存、删除订单并记录 operation。求购/摆摊撮合、过期清理和
寄存物品取回，以及拍卖竞价、等待区、场次开始/交接与结算均已切换到 feature application。trade Web
队列与场次 route 也直接使用这些 application；旧 `AuctionSessionService` 已移入
`compatibility/legacy_trade_auction_sessions.py`，仅供显式注入的 rollback repository 使用，且内部复用
feature-owned session/settlement repositories。
## 定时任务
场次调度继续由兼容 scheduler 管理。

`clear_expired_baitan_orders_job`（每日 `GUISHI_BAITAN_END_HOUR` 点，job id 未改）
现在按同一顺序做两件事：先全服求购到期取消退款，再清理超时摆摊并退还未售出
物品。两段都是静默的（任务没有 bot/event 上下文，不发群消息），退款与清理结果
只写日志。求购到期同时在 `鬼市信息` 的本人访问期收敛，因此玩家不必等下一次
任务就能看到冻结灵石回到余额。
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

鬼市生命周期已知限制：

- 到期锚点只来自 `guishi_order_create_operations`。早于本功能创建的历史订单
  没有该行，因此**不会自动到期**，只能手动 `鬼市取消求购` 或管理员 `清空鬼市`；
  渲染显示 `到期：未知（历史订单）`。这是有意的 fail-safe：宁可不清理，也不猜
  时间误删玩家订单。
- 到期小时沿用既有 `GUISHI_BAITAN_END_HOUR` 与模块内 UTC `runtime_clock` 口径
  （与既有摆摊窗口/任务一致），本次不改摆摊窗口的时区语义。
- `price_refund` 只补「买家按求购价冻结、按摆摊价成交」的差价，不改变价格闸门
  （摆摊价高于求购价仍然不成交）、部分成交语义或手续费。
- legacy `TradeRepository.match_guishi_orders` 保留原语义供兼容测试使用，默认
  路径已停用（进度门禁 `legacy_guishi_matching_disabled`），新语义只由 feature
  路径承担。

## Manifest 清单
- `route: POST /api/v1/trade/deposit`
- `route: POST /api/v1/trade/withdraw`
- `route: POST /api/v1/trade/enqueue`
- `route: POST /api/v1/trade/dequeue`
- `route: POST /api/v1/trade/session_start`（admin）
- `route: POST /api/v1/trade/session_finish`（admin）
- `route: POST /api/v1/trade/purchase`
