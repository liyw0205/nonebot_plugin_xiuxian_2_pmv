# ADR-0005 兼容玩法使用显式生命周期桥接
日期：2026-09-12
状态：已接受

## 背景

旧玩法仍通过导入副作用注册 matcher、任务和数据维护。直接使用 NoneBot
`on_startup/on_shutdown` 会导致测试、热重载和新组合根重复执行。

## 选项

1. 保留旧 driver hook，并在新 hook 中额外去重。
2. 删除旧 hook，要求所有玩法立即迁移。
3. 旧模块只登记回调，由 `bootstrap.legacy` 和 `Lifecycle` 统一执行。

## 决策

采用选项 3。兼容模块通过 `register_legacy_startup`/
`register_legacy_shutdown` 登记；组合根在 `jobs` 阶段执行一次，关闭时逆序停止。
适配器 vendor 的传输层 shutdown hook 不属于玩法生命周期，继续由适配器自身管理。

## 代价与风险

兼容模块仍有导入副作用和直接数据库访问，不能因此提前删除兼容层。启动失败时桥接器会回滚已启动回调并将 Runtime 标记为 `not_ready`。

## 迁移和回滚

迁移新玩法后删除对应登记回调和兼容 manifest；若新实现异常，可重新启用旧包并保留
operation ledger。无需改动玩家数据库。

## 影响的 feature / 数据 / API

影响旧玩法的启动维护、旧 Web 服务和调度覆盖；不改变命令、数据库字段或公开 API。
