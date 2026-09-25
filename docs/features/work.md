# 悬赏令接取与结算

## 用户流程

用户先刷新悬赏令，再执行接取编号。接取只改变当前悬赏状态，不消耗刷新次数；完成后结算会按固定的工作快照发放修为和额外物品。刷新次数、悬赏快照、用户冷却状态和结算奖励在各自事务中校验。

## 命令与别名

- `悬赏令接取`（旧入口使用 `悬赏令接取 1`）
- `悬赏令结算`
- `悬赏令`

## Web API

`POST /api/v1/work/claim`，权限为 `user`，请求体包含 `operation_id`、`user_id`、`expected_count`、`expected_offer`、`task_index` 和 `started_at`。支持 `Idempotency-Key`；成功响应返回任务名、开始时间和剩余刷新次数。

`POST /api/v1/work/settle`，权限为 `user`，请求体包含工作快照、修为增量、可选物品、等级上限和背包上限。支持 `Idempotency-Key`；成功响应返回实际修为、奖励类型和任务名。

## 数据模型与迁移

`work.001` 写入功能迁移标记；`work.003` 在 game DB 创建 `work_item_use_operations`，`work.004` 用 `CREATE TABLE IF NOT EXISTS` 建立兼容现有数据的 `work_offer_snapshots`。
悬赏令加速道具与追捕令都经 `WorkItemUseApplication -> WorkItemUseSqlRepository`；加速原子扣除一个道具并将已接取悬赏的开始时间置为立即可结算，追捕令原子扣除道具并保存随机 offer 与首次奖励倍率。两种动作都按库存快照校验，捕获令重放返回首次保存的 offer 和倍率。`work.002` 负责每日刷新重置；接取/结算兼容仓储维护 `work_claim_operations`、`work_active_snapshots` 与结算操作表，历史 JSON 仅作为兼容读取/展示投影。

## 事务与失败回滚

接取、结算 application 在 `game_db.operation_ledger` 记录请求；物品 repository 以一个 `BEGIN IMMEDIATE` 事务校验用户、道具库存和工作状态，再原子更新背包、`user_cd.create_time` 或 offer 快照及 operation 结果。追捕令随机结果与倍率只在首次请求持久化，随机重抽不破坏同 operation 重放；随后仅更新旧 JSON 展示投影。操作号冲突和状态变化不修改资产，异常完整回滚。两个物品动作请求路径均不建表；work.004 保留已存在的快照行。

## 定时任务

刷新、结算、过期清理和每日重置仍由兼容调度管理，本切片不新增任务。

## 配置项

- `work_claim_enabled` / `XIUXIAN_WORK_CLAIM_ENABLED`：默认启用，可关闭新应用接入。

## 适配器差异

命令适配器负责解析旧的正则命令和 JSON 投影；Web blueprint 负责 DTO、权限、CSRF 和统一响应；application 不发送消息。

## 测试与手工验收

- `python -m unittest nonebot_plugin_xiuxian_2.features.work.tests.test_work_application -q`
- `python -m unittest tests.test_work_claim_service tests.test_work_settlement_service tests.test_source_quality -q`
- `python -m pytest -p no:cacheprovider tests/test_work_item_use_application.py tests/test_work_item_use_service.py`
- 已注册的 `道具使用 追捕令` matcher 应扣除一个追捕令并保留首次生成的 offer。
- 重复提交相同操作号不应重复改变悬赏状态。

## 灰度开关、回滚和已知限制

关闭 `work_claim_enabled` 可恢复旧命令实现。悬赏刷新和终止仍是兼容动作，待后续垂直切片迁移；删除旧服务前需满足完整发布周期和恢复演练要求。
