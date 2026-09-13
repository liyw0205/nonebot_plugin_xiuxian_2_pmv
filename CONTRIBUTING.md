# 开发与交付

## 本地验收

每个独立改动在提交前执行：

```sh
python -m unittest discover -s tests -v
python -m compileall -q nonebot_plugin_xiuxian_2 tests
git diff --check
python scripts/check_architecture.py
```

涉及 QQ Adapter 时额外执行：

```sh
python -m unittest tests.test_qq_adapter_contracts -v
```

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
