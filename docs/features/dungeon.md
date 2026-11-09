# 副本探索与兑换

## 用户流程

副本商店兑换使用统一资产 application；探索先冻结 resolution intent，再准备、结算与重放。队伍状态由 player-owned application/repository 持有，兼容适配器只提供命令和战斗计算入口。

## 命令与别名

- `副本兑换`：兑换副本商店物品。
- `探索副本`：创建并结算一次探索操作。

## Web API

- `POST /api/v1/dungeon/purchase`，权限 `user`，支持 `Idempotency-Key`。
- `POST /api/v1/dungeon/explore/replay`、`intent`、`prepare`、`settle`，权限 `user`，支持 `Idempotency-Key`，优先于请求体的 `operation_id`。

五项 POST 路由均先执行 permission resolver，再验证当前会话 CSRF；拒绝时不调用 application。同 user 可重放持久探索结果，不同 user 返回 `409/operation_conflict`，不返回原 intent、plan 或结果。这里的 user 是操作 identity，不是认证 actor；现有全局 `user` resolver 默认允许，认证主体绑定仍是独立未完成目标。

## 数据模型与迁移

`dungeon.001` 在 `game_db` 创建 `dungeon_feature_migrations`；game-only `dungeon.008` 为探索回执补齐 `intent_json`。player-only `dungeon.006/.007` 预建队伍/global/status/reset schema，`dungeon.009` 以 JSON1 回填成员索引并由 mutation 同事务维护，`dungeon.010` 增加邀请通知路由和只覆盖未消费 pending 行的到期索引。pending 邀请、持久回执与历史探索/结算记录保留；终态邀请行（expired/joined 等，且过期已超保留期）由启动维护回收（见 `docs/features/maintenance.md`）。缺 schema/index fail closed，不做请求期 DDL。

## 事务与失败回滚

商店兑换由 application 的 operation ledger 和仓储事务共同保证幂等。探索随机 seed、模板、玩家/队伍、战斗属性、资源和库存输入先持久化，恢复禁止 live fallback；结算比较完整快照，冲突时只记录拒绝结果，不修改资产。队伍邀请过期状态与成功 receipt 同一 player UoW 提交；未到期不写终结 receipt，成功 replay 返回 duplicate。

## 定时任务

每日副本重置由兼容生命周期统一调度。

`dungeon_team_invite_expiry` 沿用 DeferredScheduler 的启动激活，每 5 秒直接运行单个 feature worker，`max_instances=1`、coalesce，每轮最多扫描 100 行。常量大小 keyset 游标让失败/损坏行不阻塞后续批次，扫完后回绕重试；重启从持久 pending 邀请恢复，不为每条邀请创建 sleeper task。扫描/过期不等待 SQLite 锁，锁冲突立即结束本轮，逐条让出事件循环；UoW 的 busy timeout 与显式 timeout 一致，默认仍为 30 秒。原接收 bot ID、消息 ID 和群/频道场景随邀请冻结，不保留 bot/event 或新增缓存。

先完成本轮状态事务，再串行最佳努力通知，单条 2 秒、整轮 10 秒预算。无路由旧邀请只过期；bot 断开、消息 ID 失效或发送失败不回滚状态、不无限补发。该路径不是 outbox，提交后崩溃可能丢通知。损坏行或冲突 receipt 保留并计数，需要受控修复，不能作为缓存删除。

## 配置项

`dungeon_enabled`（`XIUXIAN_DUNGEON_ENABLED`）控制新边界，默认启用。

## 待处理组队邀请面板

`查看队伍` 在玩家不在任何队伍时先读取 `DungeonTeamApplication.pending_invite(user_id, now)`：

- 命中未过期 `dungeon_team_invites` 行时，显示队伍名、邀请人道号、发起群与剩余分钟，
  并提示回复 `同意组队` 或 `拒绝组队`；快捷按钮为同意/拒绝/队伍帮助。
- 未命中、已过期、已消费或 schema 缺失时保持原“你不在任何队伍中”与创建队伍提示。
- 纯展示 `render_pending_invite` 只接收队伍名、邀请人、群和剩余秒数；空名称回退为
  `未知队伍`/`未知用户`，`remaining_seconds <= 0` 输出过期文案。
- 只读；复用既有 `dungeon_team_invites` 与 `pending_invite` 查询，不新增表、列、迁移、
  索引、配置键、命令、别名或路由；不改邀请 TTL、私聊通知、同意/拒绝或到期清理。

## 个人副本探索记录

`我的副本记录`（别名 `副本记录`、`副本探索记录`）补齐玩家侧逐次副本探索的可见性：
`我的副本状态` 只有当前层数与进度，看不到近几天探索过哪些层、结果与奖励摘要。

- 数据源是真实结算回执 `dungeon_explore_operations`：`request_identity` 为
  `{"action":"explore","user_id":...}` 规范 JSON，成功结算写 `phase='completed'`、
  `result_status='applied'`、`result_json`（`message` 与 `battle_messages`）、
  `current_layer`、`dungeon_status`、`updated_at`；不新增表、列、迁移、索引或写入路径。
- `DungeonExploreHistoryApplication` 在 `read_only=True` 的 `DatabaseUnitOfWork` 中按
  当前会话的精确 identity 查询近 7 天（只用下界）成功结算行，按
  `updated_at DESC, rowid DESC` 排序；被拒绝或未完成的探索不计入。
- 缺库、缺表、缺列、坏 JSON 或读失败返回空态，不建表、不补列；消息折叠为单行并截断，
  避免整段战斗日志刷屏。
- 输出：探索次数、通关次数、最高层、战斗场数，以及最近 10 次明细（时间、层数、
  通关/探索中、结果摘要）；空态显示“近7天没有副本探索记录。”。
- 帮助与发现性：`副本帮助` 增补条目，`修仙帮助`「流水与总览」登记；
  manifest 增加 `CommandSpec`（含别名），命令仍由 `xiuxian_dungeon` 注册。
- 只读展示；不改探索战斗、体力、掉落、队伍、副本状态或任何资产写路径。

## 灰度开关、回滚和已知限制

关闭开关后保留旧命令和数据格式。探索的战斗规则、队伍管理和奖励计算尚未移入新 domain，待后续发布周期完成迁移。

当前未完成边界：Web 认证主体绑定、全局 legacy 门禁和正式运行数据 migration/restore/P7 证据。intent Web route 已补齐 manifest 声明，运行时 guard 保持不变。legacy reader/mapping 仍保留兼容实现，但默认 handler 未证明可达，不重复迁移死 helper。旧 writer 不维护 `.009` 投影；回滚发生旧写后再回默认入口，必须停机受控重建/核验投影。历史跨队重叠未清洗，当前按最小 team_id 单行选择。

## 适配器差异

命令适配器只组装探索快照；成员资料由 `PlayerProfileApplication` 读取，CD 与背包由 `DungeonExploreSnapshotApplication` 的只读 repository 批量读取，缺 schema 时 fail closed；Web 适配器只解析 DTO。领域 application 不依赖 NoneBot 或 Flask，探索快照 repository 是明确的 SQLite 读取边界。

## 测试与手工验收

覆盖准备、结算、重放、快照冲突和库存拒绝；`tests/test_dungeon_explore_web_boundary.py` 通过隔离 Flask client 验证五项路由声明、注入拒绝 resolver、缺失/错误/跨会话 CSRF、幂等键优先和真实仓储 identity 冲突不泄露。不以注入权限测试宣称默认认证策略已加强。

## Manifest 清单
- `command: 我的副本记录`
- `alias: 副本记录`
- `alias: 副本探索记录`
- `route: POST /api/v1/dungeon/purchase`
- `route: POST /api/v1/dungeon/explore/replay`
- `route: POST /api/v1/dungeon/explore/intent`
- `route: POST /api/v1/dungeon/explore/prepare`
- `route: POST /api/v1/dungeon/explore/settle`
