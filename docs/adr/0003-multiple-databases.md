# ADR-0003 多 SQLite 数据库的迁移边界
日期：2026-09-12
状态：已接受

## 背景

现有安装把玩家、交易、传承和消息数据分在多个 SQLite 文件中。一次合并会扩大
锁竞争和恢复风险，也会迫使兼容代码同时改变。

## 选项

1. 本轮立即合并为单库。
2. 保留文件边界，由 `DatabaseCatalog` 声明所有权，并以 operation ledger、outbox
   和 reconcile 处理跨库动作。

## 决策

采用选项 2。新 repository 只能写自己拥有的数据库；跨库动作必须携带同一
`operation_id`，主库先登记流水，再执行从属库步骤。失败保留
`needs_reconcile`，禁止用手工 SQL 静默修复。

## 代价与风险

跨库提交不是原子事务，需要补偿和对账；合并数据库的收益和锁模型留到有运行数据
后重新评估。

## 迁移和回滚

迁移前创建带校验和的全量备份，先 dry-run，再按库执行。异常时停止写入、恢复备份、
运行对账并保留失败流水。

## 影响的 feature / 数据 / API

影响 `game_db`、`player_db`、`trade_db`、`impart_db`、`message_db` 的 repository
和所有跨库资产 feature；不改变公开 API 路径。
