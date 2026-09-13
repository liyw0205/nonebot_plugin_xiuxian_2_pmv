# 拍卖竞价

## 用户流程

竞拍适配器提交拍卖号、预期价格和预期竞价快照。应用层通过 operation ledger 防止重复扣款，再调用兼容仓储完成扣款、前一竞价退款和当前拍卖更新。

## 命令与别名

`拍卖竞拍`、`竞拍`。旧命令仍由兼容包解析消息和权限。

## Web API

`POST /api/v1/auction/bids`，权限 `user`，需要 CSRF；支持 `Idempotency-Key`。请求字段为 `auction_id`、`bidder_id`、`bid_price`、`expected_price`、`expected_bids` 和 `bid_time`。

`POST /api/v1/auction/settle`，权限 `admin`，需要 CSRF；支持 `Idempotency-Key`。请求可提供 `end_time`、`fee_rate` 和 `item_types`，用于人工重试当前场次收尾。

## 数据模型与迁移

历史拍卖表继续由 `TradeRepository` 维护，`auction_feature_migrations` 记录新边界版本 `auction.001`；统一 `operation_ledger` 和 `operation_audit` 记录应用层结果。

## 事务与失败回滚

旧仓储在自身数据库事务中完成资产和拍卖状态变更。应用层先登记操作号，业务拒绝写入 rejected 审计，异常写入 failed；仓储异常不会返回成功结果。

## 定时任务

`auction.settle` 是稳定的按需任务 ID，可由调度器或管理 API 执行。任务通过同一 `auction.settle` operation ledger 幂等；旧调度器在兼容周期内仍保留，但不再是新 application 的唯一入口。

## 配置项

`XIUXIAN_AUCTION_ENABLED` 控制新竞价入口，默认开启。

## 适配器差异

应用层不依赖 NoneBot 或 Flask；旧命令负责解析消息段和生成预期竞价快照。

## 测试与手工验收

覆盖成功、重复操作、状态冲突、余额不足和仓储异常回滚；Web client 检查 CSRF、权限和统一响应。

## 灰度开关、回滚和已知限制

关闭灰度后旧竞价入口继续工作。结算的详细 SQLite 算法仍由 `LegacyAuctionSettlementRepository` 承载，完成一个发布周期的兼容命中观测后再删除旧仓储。
