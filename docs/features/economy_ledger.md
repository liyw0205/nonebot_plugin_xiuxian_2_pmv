# 经济流水查询

## 用户流程

管理员在管理面板查询共享 `economy_log` 流水，可按用户、宗门、来源、动作、trace、时间和异常金额筛选，并分页查看汇总；导出沿用同一筛选条件。

## 命令与别名

无命令。`commands.py` 的 `COMMANDS = ()` 明确该 owner 只提供管理 Web 读模型。

## Web API

运行时 Web 适配器注册以下管理员只读接口：

- `GET /api/v1/economy-logs` → `EconomyLedgerApplication.query_page`
- `GET /api/v1/economy-logs/export` → `EconomyLedgerApplication.iter_export_rows`

旧 `/economy_logs` 和 `/economy_logs/export` 保留兼容重定向，旧页面与 CSV 参数、附件名和 14 列顺序不变。

## 数据模型与迁移

流水表及写入方属于既有经济 owner；本切片只添加一次启动迁移 `economy_ledger.001` 的只读索引。请求路径不建表、不建索引、不修改流水。

## 事务与失败回滚

查询使用只读 `DatabaseUnitOfWork`，缺库、缺表或缺列返回空结果并带提示；导出查询错误直接失败，不伪装为空 CSV。该 owner 不写资产，因此没有业务回滚事务。

## 定时任务

无任务，`jobs.py` 的 `JOBS = ()`。索引迁移由组合根启动迁移器执行，不由 Web 请求触发。

## 配置项

无 feature 配置项或环境变量；管理员权限由 Web adapter 的既有权限解析器提供。

## 适配器差异

application/repository 保持框架无关；Flask adapter 负责管理员鉴权、模板、JSON envelope 与 CSV response。旧 WSGI 入口仍负责兼容 URL，未复制查询实现。

## 测试与手工验收

运行 `.venv/bin/pytest -q tests/test_economy_ledger_web.py nonebot_plugin_xiuxian_2/features/economy_ledger/tests -p no:cacheprovider`，应覆盖匿名拒绝、管理员分页、兼容重定向、CSV 14 列和查询异常。手工检查同一筛选条件的页面与 CSV 行数一致。
