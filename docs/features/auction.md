# 拍卖竞价

## 用户流程

竞拍适配器提交拍卖号、预期价格和预期竞价快照。应用层通过 operation ledger 防止重复扣款，再由 `AuctionBidSqlRepository` 原子完成扣款、前一竞价退款和当前拍卖更新。

## 命令与别名

`拍卖竞拍`、`竞拍`。旧命令仍由兼容包解析消息和权限。

## Web API

`POST /api/v1/auction/bids`，权限 `user`，需要 CSRF；支持 `Idempotency-Key`。请求字段为 `auction_id`、`bidder_id`、`bid_price`、`expected_price`、`expected_bids` 和 `bid_time`。

`POST /api/v1/auction/settle`，权限 `admin`，需要 CSRF；支持 `Idempotency-Key`。请求可提供 `end_time`、`fee_rate` 和 `item_types`，用于人工重试当前场次收尾。

## 数据模型与迁移

拍卖会话、当前拍品、历史和结算 operation 表由 `auction.002` 迁移创建；`auction.003` 在 game DB 创建排队 operation 表，`auction.004` 在 trade DB 创建玩家等待区表，`auction.005` 在 game DB 创建竞价 operation 表。`auction_feature_migrations` 保留边界版本标记。

展示、调度 guard、重启对账和竞价前拍品读取统一经过 `AuctionQueryApplication` 的只读查询；查询
不会在请求期创建表。最近成交使用 SQL `LIMIT`，避免为了展示少量记录载入整张历史表；缺失数据库
或旧表时返回空结果。

`拍卖上架`/`拍卖下架` 使用 `AuctionQueueApplication`；等待区操作由 feature-owned repository 执行。`auction.003` 与 `auction.004` 必须按 database route 分别应用。

## 事务与失败回滚

`AuctionSettlementSqlRepository` 在 game DB 的 `BEGIN IMMEDIATE` 事务中完成资产、背包、历史和拍卖状态变更。应用层先登记操作号，业务拒绝写入 rejected 审计，异常写入 failed；仓储异常不会返回成功结果。

`AuctionQueueSqlRepository` 在 game DB immediate UoW 中附加 trade DB；排队扣除可交易库存、队列插入和 operation 记录同事务提交，下架的背包返还、队列删除和 operation 记录同事务提交。

`AuctionSessionStartSqlRepository` 在同一跨库事务中将等待区项目装入当前场次、创建 session 和 start operation，再清空等待区。管理员和自动开场的默认路径由 `AuctionSessionStartApplication` 提供时钟、随机源和稳定 replay；旧 `AuctionSessionService` 仅为显式兼容 DTO 委托，同样复用 feature-owned start/settlement repositories。结束流程走 `AuctionSettlementApplication`，replay 不重复写统计/游戏事件。

`AuctionBidSqlRepository` 在 game DB 的 immediate UoW 中校验预期价格/竞价快照，锁定出价者灵石、退还上一位领先者并写入竞价 operation；application replay 不重复写交易统计。

## 结算投递合同（不丢物品 / 不阻塞整场）

结算的容量口径取 `XiuConfig().max_goods_num`（默认 1000）：`AuctionSettlementSqlRepository`
未显式注入 `max_goods_num` 时惰性读取该配置，只有配置不可读时才回落到同名默认值
`DEFAULT_MAX_GOODS_NUM`——**不得**再把「未传参」当作 1 件上限。`AuctionSettlementApplication`
可透传 `max_goods_num`，既有构造点不传即走配置解析。

投递只有三种结果，全部在同一结算事务内决定：

- `backpack`：背包有位置，物品直接入包（`back.goods_num/bind_num + 1`）。
- `pending`：背包该物品已达上限，写入 `auction_pending_items`（`owner_id`/`role`/
  `reason`），历史状态记为 `待领取`；**物品永不丢失**。中标者背包满不再
  `inventory_full` 拒绝整场，其余拍品、卖家收入与败者退款照常完成。
- `none`：无物品需要投递（系统拍品或异常路径）。

`item_missing`（物品类型无法解析）与 `participant_missing`（买卖方账号缺失）仍按原样
拒绝并整事务回滚，保持 fail-closed。`auction_pending_items` 缺失（未迁移旧库）时插入
抛错 → 整个结算回滚、场次保持 active、`auction_current` 与背包不变，绝不静默降级为
「已销毁」。

玩家侧读取与领取由 `AuctionPendingApplication` 承担：

- `list_for(user_id)` 只读返回本人未领取行（`auction_id`、物品、`role`（中标/流拍）、时间），
  缺库/缺表返回空且不建表。
- `claim(operation_id, user_id, auction_id=None)` 在单个 immediate UoW 内逐行校验
  `goods_num + 1 <= max_goods_num`，可领的原子入包并标记 `claimed_at/claim_operation_id`，
  背包满的跳过并保持待领取；结果写 `auction_pending_claim_operations`，同 operation_id
  重放返回首次结果、不重复发放；无待领取项返回 `empty`，无 schema 返回 `schema_missing`。
- 命令面：`我的拍卖` 同时列出等待区与待领取；`我的拍卖 领取 [拍卖ID]` 领取全部可领取项
  或指定物品，不新增命令名、路由或配置键。

## 竞价可见性与被超价通知

竞价资产写入时，`AuctionBidSqlRepository` 已在同一事务中锁定新出价并退回上一位领先者
（`refunded_bidder`/`refunded_amount` 随 `auction_bid_operations` 持久化）。
应用层把这两个字段放进 outbox payload，效果层据此补齐玩家可见面：

- `AuctionQuerySqlRepository.my_bids(user_id)` 只读解析 `auction_current.bids`，返回本人
  参与的拍品：`auction_id`、`item_name`、`my_price`、`current_price`（取
  `max(current_price, 最高出价)`）、`leading`（`my_price >= 最高出价`）、`bid_count`；
  缺库/缺表返回空且不建表，`AuctionQueryApplication` 同名方法直接委派。
- `我的拍卖` 在等待区与待领取之后追加 `【我的竞价】` 段，逐条给出
  `拍卖ID/物品/我的出价/当前价（领先|已被超价）` 与 `拍卖竞拍 <ID> <价格>` 加价提示；
  竞价投影读取异常时该段按空处理，不影响整条指令的其余内容。
- `LegacyAuctionBidEffects.on_bid` 在出价行之后，若确有被超价者且退款额大于 0，为其写
  `[拍卖被超价] {物品} 已被超越，原出价 {N} 灵石已退回，拍卖ID:{id}`，
  `event_id=f"auction.outbid:{operation_id}"`（与出价行 `auction.bid:{operation_id}`
  分开），因此 outbox 重放时两行各自按事件号去重，不会重复通知。

不改变扣款/退款金额、竞价校验、operation 结构或 outbox 语义；不新增表、迁移或配置键。

## 我的拍卖个人视图

`我的拍卖` 在等待区、待领取与竞价段之外，读取
`AuctionQuerySqlRepository.my_auction_snapshot(user_id)`；该方法在**同一个只读 UoW** 内返回
`lots` 与 `records`，缺库/缺表返回空且不建库、不建表：

- `lots`：`auction_current` 中 `seller_id` 为本人且 `is_system=0` 的拍品。`current_price`
  取列值与 `bids` 最高出价的较大者（与 `my_bids` 同口径）；`bid_count`/`bidder_count`
  来自 `auction_bid_operations`（表缺失时回退 `bids` 长度）；`remaining_seconds` 取
  `auction_sessions` 中 active 场次的 `end_time` 差额，缺表或非 active 为 `None`。
- `records`：`auction_history` 中本人为卖家或中标者的最近 5 条，按 `end_time DESC`
  由 SQL `LIMIT` 截断；字段含 `role`（seller/winner）、`status`（成交/流拍/待领取）、
  `final_price`、`fee`、`seller_earnings`、`counterparty`、`end_time`。
- 渲染由纯模块 `features/auction/my_auction_view.py` 承担；段序固定为
  等待区 →【本场拍品】→【拍卖待领取】→【我的竞价】→【最近拍卖记录】，
  五类来源全空才进入空态。开场把等待区搬进 `auction_current` 后，卖家仍能在
  【本场拍品】看到自己的拍品，不会误报「没有上架任何拍卖物品」。
- 只读；不改上架/下架/竞价/结算/投递/领取写路径，不新增表、迁移、命令、路由或配置键。

## 定时任务

`auction.settle` 是稳定的按需任务 ID，可由调度器或管理 API 执行。任务通过同一 `auction.settle` operation ledger 幂等；旧调度器在兼容周期内仍保留，但不再是新 application 的唯一入口。

## 配置项

`XIUXIAN_AUCTION_ENABLED` 控制新竞价入口，默认开启。

## 适配器差异

应用层不依赖 NoneBot 或 Flask；旧命令负责解析消息段和生成预期竞价快照。

## 测试与手工验收

覆盖成功、重复操作、库存/背包上限、状态冲突、跨库写入失败回滚和 migration route；Web client 检查 CSRF、权限和统一响应。

## 灰度开关、回滚和已知限制

关闭灰度后旧竞价入口继续工作。`LegacyAuctionSettlementRepository` 仅保留给显式兼容调用，并直接复用 feature-owned SQL repository；生产 scheduler/application 默认使用 `AuctionSettlementApplication`。
