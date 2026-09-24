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

`拍卖上架`/`拍卖下架` 使用 `AuctionQueueApplication`；等待区操作由 feature-owned repository 执行。`auction.003` 与 `auction.004` 必须按 database route 分别应用。

## 事务与失败回滚

`AuctionSettlementSqlRepository` 在 game DB 的 `BEGIN IMMEDIATE` 事务中完成资产、背包、历史和拍卖状态变更。应用层先登记操作号，业务拒绝写入 rejected 审计，异常写入 failed；仓储异常不会返回成功结果。

`AuctionQueueSqlRepository` 在 game DB immediate UoW 中附加 trade DB；排队扣除可交易库存、队列插入和 operation 记录同事务提交，下架的背包返还、队列删除和 operation 记录同事务提交。

`AuctionSessionStartSqlRepository` 在同一跨库事务中将等待区项目装入当前场次、创建 session 和 start operation，再清空等待区。管理员和自动开场的默认路径由 `AuctionSessionStartApplication` 提供时钟、随机源和稳定 replay；旧 `AuctionSessionService` 仅为兼容委托。结束流程走 `AuctionSettlementApplication`，replay 不重复写统计/游戏事件。

`AuctionBidSqlRepository` 在 game DB 的 immediate UoW 中校验预期价格/竞价快照，锁定出价者灵石、退还上一位领先者并写入竞价 operation；application replay 不重复写交易统计。

## 定时任务

`auction.settle` 是稳定的按需任务 ID，可由调度器或管理 API 执行。任务通过同一 `auction.settle` operation ledger 幂等；旧调度器在兼容周期内仍保留，但不再是新 application 的唯一入口。

## 配置项

`XIUXIAN_AUCTION_ENABLED` 控制新竞价入口，默认开启。

## 适配器差异

应用层不依赖 NoneBot 或 Flask；旧命令负责解析消息段和生成预期竞价快照。

## 测试与手工验收

覆盖成功、重复操作、库存/背包上限、状态冲突、跨库写入失败回滚和 migration route；Web client 检查 CSRF、权限和统一响应。

## 灰度开关、回滚和已知限制

关闭灰度后旧竞价入口继续工作。`LegacyAuctionSettlementRepository` 仅保留给显式兼容调用，生产 scheduler/application 默认使用 feature-owned SQL repository。
