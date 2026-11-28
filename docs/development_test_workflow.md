# 开发与测试工作流

## 本地开发验证

- 先读 `docs/current_project_status.md`、相关功能文档和当前工作树；保留已有用户改动，明确本批代码、测试与验收边界。
- 实现与对应测试作为一个完整变更批次。开发过程中只跑直接覆盖当前改动的聚焦测试；根据共享调用链、数据边界或失败证据，少量增加必要回归。测试反复修改不等于每改一个测试文件就提交、推送或触发 CI。
- unittest 聚焦入口直接指定模块，避免每次开发都跑 6,000+ 项：
  `.venv/bin/python -m unittest tests.test_example -q`。pytest 用例直接指定文件并禁用缓存插件：
  `.venv/bin/python -m pytest -q -p no:cacheprovider tests/test_example.py`。
- 失败先在本机复现，区分产品、测试夹具和环境原因。修复代码与测试后一起完成受影响回归，不单独推送测试补丁来试 CI。

## 测试分组与隔离

- `scripts/run_unittest_shard.py --shard INDEX/2` 按完整 unittest 模块做确定性、按用例数贪心均衡分片；不拆同一模块的测试类或方法。当前发现 6,713 项、1,139 个模块，分配为 3,357/567 与 3,356/572。脚本输出各片用例数/模块数、各片最慢 15 项，并检查分片总数与发现总数一致。
- CI 的 quality job 与两个独立 unittest runner 可同时运行，最多三个 GitHub-hosted runner；`max-parallel: 2` 限制 unittest shards。runner 之间没有共享文件系统/内存/SQLite，各 runner 内测试仍串行；固定 `/tmp` 路径仅在各自 runner 上顺序访问，不会相互竞争。
- 本地不可在同一 checkout 并发启动 shards：少量测试仍使用固定 `/tmp` 路径，而且全套 unittest 内部顺序执行可以避免 SQLite、模块全局和高内存用例争抢。完整验收时在同一 `flock` 下依次运行 `--shard 0/2`、`--shard 1/2`，覆盖所有 unittest 一次；失败时用输出的模块/用例名直接跑聚焦回归。
- 不将 `pytest` 参数探针、共享文件/SQLite 测试或性能矩阵塞入并行 unittest shard。CI 的 focused pytest 留在质量 job 中串行运行；高资源脚本和性能矩阵单独受 `flock`、CPU/RAM/磁盘、CPU 时间及墙钟限额控制。现有记录没有逐用例耗时，静态上 114 个 unittest 模块包含子进程调用，但不能据此认定它们最慢。分片验收会记录最慢 15 项以识别后续优化目标；在有等价覆盖证据前不删测试。
- 本轮一次聚焦运行实测 `test_refactor_inventory` 的两个独立合同各自重建完整 inventory，耗时 10.53s 与 11.54s。保留两个方法和断言，仅让它们在需要时惰性复用同一个只读 inventory 结果；CTE 扫描的隔离单测仍不会额外触发全仓扫描。

## 合并前本地验收

功能和测试稳定后，只做一轮汇总验收，再提交推送：

1. 一次运行覆盖本批完整行为的聚焦回归。
2. 一次运行 CI 同口径的完整 unittest：在同一隔离环境和锁内依次执行两个 shard，不能只跑其中一片：
   ```bash
   flock -w 600 /tmp/nonebot-project-validation.lock bash -c '
     set -e
     timeout 1800 .venv/bin/python scripts/run_unittest_shard.py --shard 0/2
     timeout 1800 .venv/bin/python scripts/run_unittest_shard.py --shard 1/2
   '
   ```
3. 运行本仓库编译、架构、清单和 whitespace 检查：
   `python -m compileall -q nonebot_plugin_xiuxian_2 tests scripts`、
   `python scripts/check_architecture.py`、
   `python scripts/export_refactor_inventory.py --check`、
   `git diff --check`。
4. 仅对本轮性能改动运行相关性能矩阵；不相关的目标不为凑齐清单而压测。

共享数据库夹具、高内存用例、pytest 参数探针和矩阵不得额外并行；本地 full suite shards 串行，CI 仅在相互隔离的 runner 间并行。所有重型任务遵守既有 `flock`、CPU、内存、磁盘、CPU 时间和墙钟限额。性能矩阵优先复用同一 fixture 做 before/after；保留既定最大数据规模，不重复复制大型 fixture。报告至少包括实际命令/路径覆盖及其非穷举范围、每路径样本数、p50/p95、错误数/错误率、查询数与缓存命中/未命中、fixture 身份和资源限额/实际使用量。没有有效同 fixture before/after 证据时，不得把测试通过写成性能优化完成。

## 提交与 CI

- 只有整批本地验收全部通过后，才按白名单一次提交并普通推送当前开发分支；随后等待该提交对应的 CI 到达终态。不得为单个测试文件改动单独推送。
- CI 失败时先在本地按失败日志复现。产品与测试修正作为同一修复批次，完成必要聚焦回归及受影响验收后再推送；不通过连续试推测试补丁来探测 CI。
- 合并、发布、部署由当前 goal 的明确授权和门禁决定；CI 绿不自动授权合并或发布。
