# 宗门成员与资产操作

## 用户流程

宗门成员加入、宗门商店兑换、主/副功法学习和炼体堂领奖通过 `SectApplication` 统一登记 operation ledger。旧命令通过兼容门面调用新 application。

## 命令与 Web API

- `加入宗门` -> `POST /api/v1/sect/join`
- `宗门商店兑换` -> `POST /api/v1/sect/purchase`
- `学习宗门功法` -> `POST /api/v1/sect/learn-main`
- `POST /api/v1/sect/learn-secondary`
- `领取宗门炼体堂` -> `POST /api/v1/sect/elixir/claim`

所有接口权限为 `user`，写请求支持 `Idempotency-Key`，业务拒绝返回统一错误码。

## 数据、回滚与限制

`sect.001` 创建 feature 迁移标记；宗门历史表继续由兼容 repository 维护。关闭 `XIUXIAN_SECT_ENABLED` 即回退旧实现。宗门建设、任务、传位和解散等剩余动作仍在兼容层，待后续垂直切片迁移。

## 命令与别名

保留 `加入宗门`、`宗门商店兑换`、`学习宗门功法`、`领取宗门炼体堂` 及历史别名。

## Web API

接口为 `/api/v1/sect/join`、`purchase`、`learn-main`、`learn-secondary` 和 `elixir/claim`，权限为 `user`，写请求需要 CSRF 与幂等键。

## 数据模型与迁移

`sect.001` 只写迁移标记和统一 ledger；历史宗门表由兼容 repository 持有。

## 事务与失败回滚

每个动作在 operation ledger 中记录请求哈希；失败和拒绝不修改成员、物品或灵石。

## 定时任务

宗门日常重置和任务刷新由兼容生命周期统一调度，使用稳定 job ID。

## 配置项

`sect_enabled` / `XIUXIAN_SECT_ENABLED` 控制新切片，默认开启。

## 适配器差异

命令层负责事件和文案，Web 层负责 DTO、权限、CSRF 和统一错误码；application 不依赖框架。

## 测试与手工验收

覆盖加入、兑换、功法学习、领奖的成功、拒绝、异常回滚和重复请求，并执行 Web client 与恢复冒烟。

## 灰度开关、回滚和已知限制

关闭开关即回到兼容入口；宗门建设、任务、传位和解散仍属于旧实现。
