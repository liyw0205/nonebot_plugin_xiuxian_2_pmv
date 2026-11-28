# 开发与交付

## 本地开发与合并前验收

开发期间先运行与改动直接相关的聚焦测试；不要因单个测试文件变化就提交、推送或触发 CI。实现和测试稳定后，
按 [`docs/development_test_workflow.md`](docs/development_test_workflow.md) 完成一次聚焦回归、CI 同口径全量 unittest、
编译/架构/清单/whitespace 检查，以及本轮确实相关的性能矩阵。QQ Adapter 变化应包含对应适配器合同测试。

## 提交边界

- 一个提交只完成一个可验证目标，并同步更新相关开发文档和测试。
- 不提交运行数据库、缓存、日志、备份文件、Bot token、secret、用户 ID 或群 ID。
- 运行时数据统一通过 `XiuxianPaths`，主动发送统一通过 `MessageDeliveryService`。
- 可变 JSON 状态使用中央 JSON Store；协议专用网络客户端需在代码或路线文档说明理由。
- 修改玩法数值、经济资产或权限边界时必须有失败回滚、重试或幂等测试。

## 新架构接入

- 新玩法放在 `nonebot_plugin_xiuxian_2/features/<name>/`，必须提供 manifest、用例、仓储、适配器、迁移说明和测试。
- 资产写入必须在一个 `DatabaseUnitOfWork` 内记录 `operation_ledger`；跨库动作使用 outbox 并通过 `ReconcileService` 对账。
- `core/domain` 和 `core/ports` 不得导入 NoneBot、Flask、SQLite 或网络客户端；框架差异只允许出现在 `adapters/`。
- Web 新接口使用 `adapters.web.api` 的统一响应格式和 CSRF/权限装饰器，路由必须在 manifest 声明权限。
- manifest、job、route、command 的 ID 在启动前由 `FeatureRegistry` 校验，禁止导入副作用注册新任务。

## 发布

已经发布的迁移版本、名称和 checksum 必须保持不变；修复或新增迁移行为使用新版本，
不得通过改写历史记录绕过 checksum 校验。`tests/fixtures/published_migration_checksums_v1_3_5.json`
冻结该正式版本的中央和 attached 迁移，`tests/test_published_migration_checksum_contract.py`
验证历史兼容、新版本只执行一次、事务回滚和持有写锁时的并发幂等。快照之后新增的迁移
应作为独立版本验证，不得加入旧快照或重新计算旧 checksum。

`main` 分支推送会运行 Quality 工作流。发布工作流使用当前提交生成源码归档，不应包含本地
部署配置或测试凭据。真实 Bot 冒烟测试只读取 Git 忽略的本地配置，不打印任何凭据或标识。

兼容层的删除不是首个发布的自动副作用。发布 `v1.1.x` 前，在部署数据目录记录周期起点：

```sh
python scripts/check_compatibility_release.py \
  --data-dir "$XIUXIAN_DATA_DIR" begin --release v1.1.0
```

下一个正式版本发布前，先用隔离数据目录执行 `scripts/recovery_smoke.py --evidence`，再把
实际运行日志和恢复回执交给门禁检查：

```sh
python scripts/check_compatibility_release.py \
  --data-dir "$XIUXIAN_DATA_DIR" status --current-release v1.2.0 \
  --log "$XIUXIAN_DATA_DIR/logs/runtime.log" \
  --evidence "$XIUXIAN_DATA_DIR/recovery-receipt.json"
```

只有状态报告的五项检查全部为 `true`，才可执行 `close` 并提交删除兼容层的变更评审。
隔离演练、测试标签或手工创建的发布号不能替代真实部署周期证据。
