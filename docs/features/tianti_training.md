# 炼体进阶

## 用户流程

`灵石炼体` 消耗灵石兑换炼体气血；`炼体药浴` 消耗当前时段有效药材并激活药浴；`炼体突破` 按修仙境界、炼体气血和随机结果推进炼体境界；`冲窍` 消耗气血随机开启未解锁窍穴。原有数值、物品规则和跨库事务仍由兼容 repository 承载。

## 命令与 Web API

命令分别为 `灵石炼体`、`炼体药浴`（别名 `药浴`）、`炼体突破`、`冲窍`。旧 handler 只负责事件解析和文案，写操作调用 `TiantiTrainingApplication`。Web 写接口均需要 `user` 权限、CSRF 和 `Idempotency-Key`（或请求体 `operation_id`）：

- `POST /api/v1/tianti/train`：`user_id`、`requested_stone`。
- `POST /api/v1/tianti/bath`：`user_id`、`consume_plan`、`effect`、`slot_name`、`started_at`、`duration_minutes`，可选 `sect_fairyland_level`。
- `POST /api/v1/tianti/breakthrough`：`user_id`、`cultivation_rank`、`roll_success`。
- `POST /api/v1/tianti/qiaoxue`：`user_id`、`roll`。

响应使用统一 operation envelope；业务拒绝返回 HTTP 409，输入错误返回 validation error。

## 数据模型与迁移

`tianti_training.001` 在 `game_db` 写入 `tianti_training_feature_migrations`。灵石炼体和药浴的旧仓储事务跨 `game_db`/`player_db`；突破和冲窍的状态在 `player_db`。统一 operation ledger/audit 按主库记录，旧 `tianti_*_operations` 表继续作为兼容幂等投影。

## 事务与失败回滚

application 先登记操作号，再调用旧的 `BEGIN IMMEDIATE` repository。灵石和药浴的跨库更新由旧仓储一次性提交；异常会把 application ledger 标记为 `failed`，同一请求可重试。突破和冲窍的境界、气血、窍穴与操作表在玩家库内原子更新。相同操作号重放首次成功或拒绝结果，参数变化会返回冲突。

## 定时任务与配置

四个动作均为用户触发，无新增后台任务。`XIUXIAN_TIANTI_TRAINING_ENABLED` 控制新 application/manifest/Web 边界，默认启用；关闭后旧命令兼容服务仍可运行，迁移表和旧操作表不删除。

## 适配器、测试与限制

核心 application 不导入 NoneBot、Flask 或 SQLite 驱动；`LegacyTiantiTrainingRepository` 是唯一旧事务适配点。application 测试覆盖四个动作的幂等重放和拒绝结果；旧 repository 测试覆盖跨库回滚、库存不足、境界限制和操作表失败。炼体状态查询、体窍查询及结算神物仍属于读/兼容路径，后续可拆为只读 query adapter。

## 命令与别名

命令为 `灵石炼体`、`炼体药浴`（别名 `药浴`）、`炼体突破`、`冲窍`。

## Web API

接口为 `/api/v1/tianti/train`、`bath`、`breakthrough`、`qiaoxue`，权限 `user`，需要 CSRF 和幂等键。

## 数据模型与迁移

迁移版本为 `tianti_training.001`；统一 ledger 与旧操作投影并存。

## 事务与失败回滚

跨库和玩家库事务失败均记录 `failed`，不重复扣除资产，同操作号可安全重试。

## 定时任务

四个动作均由用户触发，无新增后台任务。

## 配置项

`tianti_training_enabled` / `XIUXIAN_TIANTI_TRAINING_ENABLED` 控制灰度。

## 适配器差异

旧跨库服务仅由 repository adapter 调用；命令和 Web 层不包含业务规则。

## 测试与手工验收

覆盖四动作成功、拒绝、异常回滚和重放，以及 Web 权限、CSRF、迁移和 manifest 契约。

## 灰度开关、回滚和已知限制

关闭开关后保留旧命令；状态查询和结算神物仍属于兼容读路径。
