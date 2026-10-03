# 副本探索与兑换

## 用户流程

副本商店兑换使用统一资产 application；探索先冻结 resolution intent，再准备、结算与重放。队伍状态由 player-owned application/repository 持有，兼容适配器只提供命令和战斗计算入口。

## 命令与别名

- `副本兑换`：兑换副本商店物品。
- `探索副本`：创建并结算一次探索操作。

## Web API

- `POST /api/v1/dungeon/purchase`，权限 `user`，支持 `Idempotency-Key`。
- `POST /api/v1/dungeon/explore/replay`、`prepare`、`settle`，权限 `user`。

## 数据模型与迁移

`dungeon.001` 在 `game_db` 创建 `dungeon_feature_migrations`；game-only `dungeon.008` 为探索回执补齐 `intent_json`。player-only `dungeon.006/.007` 预建队伍/global/status/reset schema，`dungeon.009` 以 JSON1 回填成员索引并由 mutation 同事务维护，`dungeon.010` 增加邀请通知路由和只覆盖未消费 pending 行的到期索引。历史邀请和持久回执保留，缺 schema/index fail closed，不做请求期 DDL。

## 事务与失败回滚

商店兑换由 application 的 operation ledger 和仓储事务共同保证幂等。探索随机 seed、模板、玩家/队伍、战斗属性、资源和库存输入先持久化，恢复禁止 live fallback；结算比较完整快照，冲突时只记录拒绝结果，不修改资产。队伍邀请过期状态与成功 receipt 同一 player UoW 提交；未到期不写终结 receipt，成功 replay 返回 duplicate。

## 定时任务

每日副本重置由兼容生命周期统一调度。

`dungeon_team_invite_expiry` 沿用 DeferredScheduler 的启动激活，每 5 秒直接运行单个 feature worker，`max_instances=1`、coalesce，每轮最多扫描 100 行。常量大小 keyset 游标让失败/损坏行不阻塞后续批次，扫完后回绕重试；重启从持久 pending 邀请恢复，不为每条邀请创建 sleeper task。扫描/过期不等待 SQLite 锁，锁冲突立即结束本轮，逐条让出事件循环；UoW 的 busy timeout 与显式 timeout 一致，默认仍为 30 秒。原接收 bot ID、消息 ID 和群/频道场景随邀请冻结，不保留 bot/event 或新增缓存。

先完成本轮状态事务，再串行最佳努力通知，单条 2 秒、整轮 10 秒预算。无路由旧邀请只过期；bot 断开、消息 ID 失效或发送失败不回滚状态、不无限补发。该路径不是 outbox，提交后崩溃可能丢通知。损坏行或冲突 receipt 保留并计数，需要受控修复，不能作为缓存删除。

## 配置项

`dungeon_enabled`（`XIUXIAN_DUNGEON_ENABLED`）控制新边界，默认启用。

## 灰度开关、回滚和已知限制

关闭开关后保留旧命令和数据格式。探索的战斗规则、队伍管理和奖励计算尚未移入新 domain，待后续发布周期完成迁移。

当前未完成边界：intent Web route 的 permission/manifest 声明、全局 legacy 门禁和正式运行数据 migration/restore/P7 证据。legacy reader/mapping 仍保留兼容实现，但默认 handler 未证明可达，不重复迁移死 helper。旧 writer 不维护 `.009` 投影；回滚发生旧写后再回默认入口，必须停机受控重建/核验投影。历史跨队重叠未清洗，当前按最小 team_id 单行选择。

## 适配器差异

命令适配器只组装探索快照，Web 适配器只解析 DTO；领域 application 不依赖 NoneBot、Flask 或 SQLite。

## 测试与手工验收

覆盖准备、结算、重放、快照冲突和库存拒绝；使用 Flask client 与恢复冒烟验证权限和幂等。

## Manifest 清单
- `route: POST /api/v1/dungeon/explore/prepare`
- `route: POST /api/v1/dungeon/explore/settle`
