# Gambling

## 用户流程
The compatibility command remains available while the new application boundary is enabled.

## 命令与别名
The historical package owns command names during the compatibility release. New names are added only through this feature manifest.

## Web API
No new HTTP route is exposed in this migration slice. Existing URLs remain served by the legacy web adapter.

## 数据模型与迁移
`legacy.dufang.001` retains the feature marker. `legacy.dufang.002` prepares frozen share operations, per-recipient progress, and the economy log in `game_db`; `legacy.dufang.003` extends the existing `unseal_data` projection and creates idempotent share receipts in `player_db`; `legacy.dufang.004` prepares bet, bet-operation, and payout-operation tables in `game_db`. Existing rows are preserved. Bet/payout reuse the player-owned `unseal_data` from `.003`.

## 事务与失败回滚
Sharing is owned by `DufangApplication -> DufangShareSqlRepository`. Each recipient's wallet, game progress, and economy log commit together in `game_db`; player statistics are applied in a separate transaction with a unique receipt. A retry repairs missing player statistics without crediting the wallet twice. The application ledger hashes only the stable source identity for sharing, while the frozen operation row owns event and recipient identity.

Bet and payout remain feature-owned repositories. Their default request path only validates the game/player schemas and fails closed when either database or required table/column is missing; schema creation belongs to startup migration. The retained legacy share settlement service is compatibility-only and is not used by the default handler. Rollback requires restoring the pre-migration backup; the additive schema itself does not delete historical data.

## 定时任务
No new scheduled jobs. Legacy jobs stay registered through the compatibility scheduler.

## 配置项
`dufang_enabled` controls the application boundary and defaults to true.

## 适配器差异
Command and web adapters translate transport input into the application DTO; business code does not import NoneBot or Flask.

## 测试与手工验收
Run the bet/payout repository and migration tests plus progress/inventory gates. Verify migration routing/idempotency and legacy-row preservation; missing schemas must not create a database/table; repeat and race the same operation ID to verify a single debit/payout.

## 灰度开关、回滚和已知限制
No live migration or recovery rehearsal is claimed for this cutover. Bet/payout random-result replay and cross-database crash recovery remain a separate follow-up; the old transaction service is an explicit compatibility boundary and may still contain request-time DDL. Operation receipts are durable idempotency records, not disposable cache. The compatibility hit counter and a clean deployment recovery run are still required before removing legacy implementations.

## Manifest 清单
- `command: 同步鉴石`
- `command: 鉴石`
- `command: 鉴石信息`
- `command: 鉴石共享关闭`
- `command: 鉴石共享开启`
- `command: 鉴石帮助`
