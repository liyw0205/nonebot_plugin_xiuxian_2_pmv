# 修仙插件全面重构与新功能接入规范

> 状态：P0-P7 发布门禁已完成；全面底层重构第二阶段进行中。上一阶段的 facade、manifest、静态守门和发布证据不等于旧玩法底层已迁移。详细进度见 `docs/full_refactor_progress.md`。
> 适用版本：当前 `nonebot_plugin_xiuxian_2_pmv` 主分支。  
> 目标：在不一次性重写全部玩法、不丢失现有玩家数据的前提下，建立可测试、可回滚、可扩展的后端与 Web 结构。

## 1. 为什么要重构

当前代码“能运行”不等于“可持续开发”。本次盘点得到的基线如下（盘点日期：2026-09-12）：

| 项目 | 当前情况 | 直接风险 |
|:--|:--|:--|
| Python 文件 | 约 363 个，约 19.7 万行 | 修改影响面无法靠目录名判断 |
| 超大模块 | `xiuxian_utils/xiuxian2_handle.py` 约 4557 行；多个 `transaction_service.py` 超过 3000 行 | 领域规则、SQL、消息文案和副作用混在一起 |
| 玩法包 | 约 50 个，靠包导入副作用注册 | 导入顺序、循环依赖、重复 matcher 难以发现 |
| 数据访问 | 约 53 个文件直接调用 `db_backend.connect` 或 `sqlite3.connect` | 事务边界不统一，跨库失败时可能部分提交 |
| Web 路由 | 约 117 个 `@app.route`，全部共享一个 Flask app | 权限、输入校验、错误格式和业务调用容易漂移 |
| Web 模板 | `messages.html` 约 6871 行，约 25 个模板包含内联脚本 | 前后端无法独立测试，改动容易破坏移动端 |
| 生命周期 | `on_startup` 分散在约 7 个模块 | 初始化顺序、重复启动和关闭不确定 |
| 配置 | 环境变量、`xiuxian_config.py`、JSON 和运行期文件并存 | 配置来源和优先级不透明 |

### 1.1 当前基线不是绿色

首次重构前必须保存一份可复现基线。当前环境执行 `python -m unittest discover -s tests -q` 的观测结果为：

- 1640 个测试被执行；21 个失败，69 个错误。
- Web 插件导入会因当前环境缺少 `qrcode` 报错；仓库的 `requirements.txt` 已声明该依赖，但本机环境未安装。
- 部分测试直接文件加载模块，触发相对导入错误；部分断言依赖当前日期或运行期数据。
- 测试会初始化插件并产生运行日志，测试数据必须使用临时目录，不得连接生产数据目录。

这些结果不是本次重构要掩盖的失败，而是第 0 阶段要固定下来的问题清单。重构分支必须记录：Python 版本、依赖锁定文件、数据目录、测试命令、失败列表和环境变量摘要（不得包含 token、密码、用户 ID）。

## 2. 重构目标与边界

### 2.1 目标

1. **依赖方向可证明**：消息平台、Flask、SQLite 只能通过适配器进入应用层；领域规则不依赖 NoneBot、Flask 或 SQL 字符串。
2. **副作用可控**：每次资产变化都有明确事务、操作号、审计记录和幂等策略。
3. **启动可重复**：启动、热重载、测试和关闭都不重复注册 matcher、任务或后台线程。
4. **接口稳定**：旧命令、旧 Web URL、旧数据字段通过兼容层迁移，不在一次提交中删除。
5. **新增功能有模板**：新玩法必须按统一的 manifest、用例、仓储、迁移、命令、Web 和测试清单接入。
6. **运维可验证**：Termux 浏览器能完成核心 Web 流程，远端脚本能完成后端冒烟，失败可定位到 request/operation/job 级别。

### 2.2 不在本轮强制完成的事情

- 不强制立即把多个 SQLite 文件合并为一个数据库。先建立数据库目录和跨库一致性协议，再评估合并收益。
- 不立即更换 NoneBot、Flask、Jinja、APScheduler 或适配器。
- 不把现有所有中文文案、玩法数值和命令别名重新设计一遍；行为变化必须独立成变更单。
- 不把 Web 面板改成大型前端框架。第一阶段保留 Jinja + 原生模块化 JavaScript，先消除内联脚本和重复 API 逻辑。

## 3. 目标架构

### 3.1 推荐目录

迁移期间允许新旧目录并存，最终目标结构如下：

```text
nonebot_plugin_xiuxian_2/
  plugin.py                         # 唯一插件入口，只做 wiring
  bootstrap/
    lifecycle.py                    # 启停状态机
    registry.py                     # feature/command/job 注册表
    health.py                       # readiness/liveness 检查
  core/
    domain/                         # 纯领域模型、值对象、规则、领域事件
      player/
      economy/
      inventory/
      combat/
    application/                    # 用例；一个公开方法对应一个业务动作
    ports/                          # Protocol：仓储、时钟、消息、任务、配置
    errors.py                       # 可展示的业务错误与内部错误
    result.py                       # Result/operation outcome
  infrastructure/
    config/                         # typed settings、来源、校验、脱敏
    database/                       # SQLite 连接、UoW、迁移、锁、备份
    messaging/                      # MessageDeliveryService 的实际实现
    scheduler/                      # APScheduler 实现与 job registry
    observability/                  # 日志、metrics、trace id
    filesystem/                     # XiuxianPaths、JSON store、原子写
  adapters/
    nonebot/                        # event -> CommandContext，ReplyPlan -> send
    web/                            # Flask blueprint、DTO、鉴权、序列化
  features/
    <feature_name>/
      manifest.py                   # 唯一接入描述
      commands.py                   # matcher/wiring，不放规则
      application.py                # 该玩法用例
      domain.py                     # 该玩法规则（需要时）
      repository.py                 # 该玩法仓储接口与 SQLite 实现
      schemas.py                    # 输入/输出 DTO
      migrations.py                 # 版本化数据迁移
      jobs.py                       # 可选，返回稳定 job 定义
      web.py                        # 可选，注册 blueprint/API
      tests/
  compatibility/
    commands.py                     # 旧命令别名转发
    imports.py                      # 旧 import 的短期 shim
    data.py                         # 旧 JSON/字段投影读取
```

现有 `xiuxian_*` 包先映射到 `features/<feature_name>`，旧模块保留转发函数并标注删除版本。`xiuxian_utils` 不再继续扩大；通用能力按 `core`、`infrastructure` 或具体 feature 归属。

### 3.2 依赖规则

允许的依赖方向：

```text
NoneBot/Flask/APS/CLI adapters
              |
         application
              |
           domain
              |
       ports (Protocol)
              ^
       infrastructure implementations
```

强制规则：

- `core/domain` 不得导入 `nonebot`、`flask`、`sqlite3`、`requests` 或文件路径全局变量。
- matcher 只解析事件、调用 application、把 `ReplyPlan` 交给消息门面；不得在 handler 内直接写 SQL。
- Web 路由只做鉴权、解析 DTO、调用 application、序列化结果；不得拼接动态 SQL、读写玩法 JSON 或修改玩家资产。
- 新代码不得直接调用 `db_backend.connect`；只能依赖 `DatabaseUnitOfWork`、仓储接口或 `ReadOnlyQuery`。
- 新代码不得在 feature 模块注册 `on_startup`；任务统一由 `bootstrap.lifecycle` 组装。
- 所有时间、随机数、ID 生成通过 `Clock`、`RandomSource`、`IdGenerator` 注入，测试禁止依赖系统日期。
- `__init__.py` 只允许导出公共 API 和调用注册函数，不得包含数百行命令处理逻辑。

### 3.3 插件加载与生命周期

`plugin.py` 只执行以下顺序：

1. 读取并校验 typed settings，打印脱敏后的配置摘要。
2. 创建 `RuntimeContext`（paths、database catalog、clock、metrics、message gateway）。
3. 加载 feature manifest，检查重复 command、route、job ID。
4. 注册 NoneBot matcher、Web blueprints 和 scheduler jobs，但不执行数据库写入。
5. `on_startup` 中按状态机执行：`filesystem -> database -> migrations -> repositories -> jobs -> web -> ready`。
6. `on_shutdown` 逆序停止：`web -> jobs/drain -> message workers -> database`。

每个阶段有一个幂等的 `ensure_*` 函数；重复调用必须返回同一状态，不得重复线程、监听器、表或任务。启动失败要标记 `not_ready`，关闭已成功启动的资源，并返回明确错误。

## 4. 关键业务不变量

### 4.1 资产与事务

灵石、修为、物品、积分、等级、限购次数等所有可计量状态必须遵守：

1. 一个用户动作生成一个 `operation_id`，由入口传入或由应用层生成。
2. 相同 `operation_id + action` 重试必须返回首次结果，不得重复扣除或发放。
3. 业务拒绝（余额不足、次数耗尽、状态过期、权限不足）不得改变任何资产。
4. 成功结果必须包含变更前后摘要、消耗、获得、审计类别和时间。
5. 异常必须回滚同一 Unit of Work；跨数据库动作必须采用下节的 outbox/reconcile 协议。

建议的最小表：

```sql
operation_ledger(
  operation_id TEXT NOT NULL,
  action TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  status TEXT NOT NULL,              -- started/applied/rejected/failed
  result_json TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(operation_id, action)
)

domain_outbox(
  event_id TEXT PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL,              -- pending/sent/dead
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT
)
```

### 4.2 多 SQLite 文件

短期保留 `XiuxianPaths` 里的数据库文件，但由 `DatabaseCatalog` 统一声明所有权：

| 数据库 | 只能由谁写 |
|:--|:--|
| `game_db` | 核心玩家、玩法仓储 |
| `player_db` | 旧玩家资料兼容仓储；迁移后只读 |
| `trade_db` | 交易、拍卖仓储 |
| `impart_db` | 传承玩法仓储 |
| `message_db` | 消息记录仓储 |

涉及多个库的动作不得假装拥有跨文件原子事务。采用以下顺序：

1. 在主库 Unit of Work 中写入 operation ledger 和待处理 outbox。
2. 依次执行从属库变更，每一步带同一个 `operation_id`。
3. 任一步失败，记录 `failed/needs_reconcile`，由补偿任务重试或回滚可逆步骤。
4. Web 管理页提供“待对账”数量、操作号、失败原因和人工重试入口。

若未来合并数据库，保留仓储接口和 operation ledger，迁移只替换 infrastructure 实现。

### 4.3 消息与适配器

统一输入 `CommandContext`：`platform`、`scene`、`group_id`、`user_id`、`message_id`、`raw_event`。统一输出：

```python
ReplyPlan(
    content=...,          # text/message segments/media reference
    reference=True,
    revoke_after=None,
    audit_timeout=0,
)
```

业务层只返回 `ReplyPlan` 或 `DomainResult`，由 `adapters/nonebot` 调用现有 `MessageDeliveryService`。OneBot、QQ、频道差异集中在 adapter capability registry；禁止在每个玩法里出现 `if is_qq ...`。

### 4.4 配置

配置统一为：

```text
环境变量 > 部署运行配置 > data/xiuxian/config.json > 代码默认值
```

每个配置项定义 `name/type/default/secret/reloadable/description`。读取后生成不可变 `Settings`，修改配置只能通过 `ConfigService`，写入采用临时文件 + `fsync` + 原子替换。日志只打印来源和是否已配置，不打印值；secret 不能通过 Web API 回显。

### 4.5 定时任务

每个任务必须声明：`id`、`title`、`owner`、`schedule`、`misfire_policy`、`concurrency_policy`、`timeout`、`retry_policy`、`idempotency_key`。禁止用函数名或 UUID 作为稳定 ID，禁止在模块导入时隐式添加任务。Web 的修改只生成 override，不修改 feature 默认定义。

## 5. Web 前端与后端重构

### 5.1 后端 Web

保留 Flask，但按蓝图拆分：

```text
adapters/web/
  app.py                 # 创建 app、middleware、错误序列化
  auth.py                # session/CSRF/Host/permission
  api.py                 # API 版本与统一响应
  blueprints/
    dashboard.py
    config.py
    messages.py
    database.py
    scheduler.py
    backups.py
    activity.py
```

统一 JSON：

```json
{"ok": true, "data": {}, "request_id": "..."}
{"ok": false, "error": {"code": "validation_error", "message": "...", "details": {}}, "request_id": "..."}
```

现有 URL 在 `/api/v1` 下提供规范接口，旧 URL 保留 1 个发布周期的 308/兼容转发。所有写接口必须具备：权限声明、CSRF、输入 DTO、幂等键（适用时）、审计日志、错误码和测试。动态表名、文件名、路径不能来自未经白名单验证的请求参数。

### 5.2 前端

第一阶段不引入新框架：

- Jinja 只负责页面骨架和初始数据，不在模板中编写业务规则。
- 将 `messages.html` 等大模板拆成 `templates/pages`、`templates/components`；每个页面的 JS 移到 `static/pages/<page>.js`。
- 统一 `static/core/api.js`（CSRF、JSON、超时、request_id、错误提示）、`state.js`、`dom.js`；禁止每页重复 `fetch` 包装。
- 复用现有 `ui.js` 的响应式表格、侧栏和无障碍逻辑，组件必须能重复初始化。
- 所有异步请求具备 loading、empty、error、retry、权限拒绝和网络断开状态；按钮防重复提交。
- 保留桌面与手机断点；禁止把重要数据只放在 hover、颜色或图标中。
- 新 UI 不引入页面级内联脚本；必要的首屏数据使用 `data-*` 或 JSON script 节点传入。

### 5.3 Web 测试

每个页面至少有：匿名访问、管理员访问、CSRF 失败、权限拒绝、成功、后端 500、空数据、窄屏布局检查。优先使用 Flask test client；真实浏览器冒烟在 Termux 执行：

```sh
# 先启动隔离测试实例，端口不要使用生产端口
XIUXIAN_DATA_DIR="$(mktemp -d)" \
XIUXIAN_WEB_PORT=5889 \
XIUXIAN_WEB_STATUS=true \
  python -m nonebot_plugin_xiuxian_2 serve  # 或项目实际启动命令

# 另一个 Termux 会话
curl -fsS http://127.0.0.1:5889/ >/dev/null
termux-open-url http://127.0.0.1:5889/
```

浏览器检查顺序：登录 -> 首页 -> 配置只读 -> 消息列表 -> 定时任务只读 -> 一个写操作 -> 退出；桌面和手机各截图一次。不得在截图、日志和命令输出中暴露管理员 ID、Cookie、token 或远端密码。

## 6. 分阶段实施计划

每阶段独立提交、可部署、可回滚。阶段完成后才能进入下一阶段。

### P0：冻结基线与安全备份

**工作**

- 锁定 Python 和依赖（至少生成带哈希的 lock 文件或可重建的安装清单）。
- 增加 `tests/conftest` 或等价测试 bootstrap：每个测试使用临时 `XIUXIAN_DATA_DIR`。
- 记录现有命令、Web URL、数据库表、JSON 文件和定时任务清单。
- 把缺失依赖、导入错误、日期不稳定测试和真实数据库依赖列入基线报告。

**完成条件**

- `compileall`、静态导入、核心服务测试能在干净目录重复运行。
- 生成一次数据库和配置备份，并演练恢复；恢复失败不得进入 P1。

**回滚**：只删除测试 bootstrap 和锁文件，不触碰运行数据。

### P1：建立运行时边界

**工作**

- 创建 `RuntimeContext`、`Lifecycle`、`DatabaseCatalog`、`Settings`、`Clock`、`IdGenerator`。
- 把现有 `paths.py`、`runtime.py`、`db_backend.py`、`messaging/delivery.py` 包装为 infrastructure 实现。
- 入口只保留一个 startup/shutdown；旧启动钩子改为显式注册函数。
- 增加 readiness 检查：数据库、迁移、任务、Web 分别报告状态。

**完成条件**

- 连续启动/关闭两次不重复注册任务、线程和路由。
- `app.url_map` 无未声明权限端点；路由表和 job 表可导出为 JSON。

### P2：数据访问与迁移治理

**工作**

- 为每个数据库定义仓储接口和 Unit of Work；新代码禁止直接 connect。
- 统一 schema migration runner，迁移记录包含版本、校验和、时间、耗时。
- 增加 operation ledger/outbox 和 `reconcile` 命令。
- 将读路径与写路径分开；旧 JSON 只作为受控兼容投影，明确权威来源。

**完成条件**

- 资产类用例拥有成功、拒绝、异常回滚、重复请求四类测试。
- 备份恢复后运行迁移和对账，结果为零未处理操作。

### P3：迁移一个垂直切片

选择“签到”或“活动领取”作为样板，不选择跨多个库且正在高频改动的玩法。

**迁移顺序**：domain/value object -> repository -> application use case -> command adapter -> Web adapter -> compatibility shim -> tests。

**完成条件**：新旧命令结果一致；旧入口只做转发；核心规则没有 NoneBot/Flask/SQL 导入；灰度开关可切回旧实现。

### P4：经济与高风险玩法

按风险排序迁移：灵石/物品发放、交易/拍卖、活动奖励、战斗结算、管理员操作。每个动作必须使用 operation ledger、审计日志和补偿策略。先迁移读模型，再迁移写模型，避免一次同时替换所有资产写入点。

### P5：Web 蓝图与前端模块化

- 每次只迁移一个页面和它的 API。
- 旧 URL 代理到新 blueprint；保留同样的权限和响应兼容。
- 先拆 API/JS，再调整视觉，不在功能迁移提交里混入大规模 CSS 改版。
- 每个页面都通过 Flask client、Termux 浏览器和窄屏截图验收。

### P6：统一调度、消息和观测

- 所有 job 改为 manifest 定义，建立 stable ID 清单和冲突检查。
- 所有主动发送改走消息门面，记录 delivery outcome 和失败分类。
- 日志统一包含 `request_id`、`operation_id`、`job_id`、`user_scope`（脱敏）和耗时。

### P7：删除旧实现

满足以下条件才可删除兼容层：至少一个完整发布周期无旧入口调用；数据库迁移已覆盖历史安装；运行日志无旧 import/旧 URL 命中；备份恢复演练通过。删除前先把 shim 标为 `DeprecationWarning`，不要静默删除。

**当前证据状态（2026-09-13）**：P7 尚未满足删除门槛。历史安装迁移、备份恢复、浏览器双视口和远端隔离冒烟均已复验通过，兼容 shim 也已加入弃用告警与命中计数；但当前只有 `v1.0.0` 发布标签，尚未完成一个完整发布周期，真实启动仍会加载旧包，因而不能宣称旧 import/旧 URL 已经零命中。gate 现在还会校验发布号确实对应仓库中的 release 标签，因此演练无法用未打标签的版本号伪造"完整发布周期"。兼容层继续保留，待下一个完整发布周期收集运行日志后再按本节条件复核。

发布周期证据由 `nonebot_plugin_xiuxian_2/compatibility/release_gate.py` 持久化管理。发布开始时记录命中基线：

```sh
python scripts/check_compatibility_release.py --data-dir "$XIUXIAN_DATA_DIR" \
  begin --release v1.1.0
```

`--data-dir` 必须是非空的显式路径；维护脚本不会在参数为空时回退到仓库默认数据目录，避免演练误写运行数据。

下一版本发布前，使用 `scripts/recovery_smoke.py --evidence <receipt.json>` 生成迁移/备份恢复回执，再检查日志和命中计数：

```sh
python scripts/check_compatibility_release.py --data-dir "$XIUXIAN_DATA_DIR" \
  status --current-release v1.2.0 --log "$XIUXIAN_DATA_DIR/logs/runtime.log" \
  --evidence "$XIUXIAN_DATA_DIR/recovery-receipt.json"
```

只有状态报告所有检查为 `true`，才允许使用同样的参数执行 `close` 并进入删除兼容层的变更评审。没有日志、迁移缺项、恢复不干净或基线后出现命中时，gate 必须失败。
gate 只接受 `vMAJOR.MINOR.PATCH` 形式的发布号，并要求关闭时的版本严格高于起始版本；同版本、降级版本和任意测试字符串都会失败。
起始和关闭版本还必须对应仓库中真实存在的 release 标签（`git tag`）。临时目录里的演练没有自己的标签，因此无法用任意版本号伪造一个"完整发布周期"；未打标签的版本即使日志、迁移和恢复回执都干净，`release_cycle` 仍为 `false`。

`scripts/refactor_completion_audit.py` 汇总 P0-P7 的当前状态；不提供真实发布周期的
`--data-dir`、`--current-release` 和 `--evidence` 时，它会明确将 P7 标为 `ready=false`，
不会把静态门禁结果当成兼容层可删除的证据。

## 7. 新功能接入规范

新增功能必须提交以下文件，不能只新增一个 `__init__.py`：

```text
features/<name>/
  manifest.py
  application.py
  domain.py                 # 无领域规则时可省略
  repository.py
  schemas.py
  migrations.py             # 无数据时仍写明“无迁移”
  commands.py
  web.py                    # 无 Web 时写明“无 Web”
  jobs.py                   # 无任务时写明“无任务”
  tests/test_<name>_service.py
  tests/test_<name>_adapter.py
docs/features/<name>.md
```

### 7.1 接入步骤

1. **写 spec**：说明用户故事、权限、输入、成功/拒绝状态、资产变化、并发和幂等要求。
2. **定义模型**：使用值对象和 DTO，禁止把数据库 Row 直接作为业务对象。
3. **定义仓储**：声明读写方法、数据库所有权、索引和事务范围。
4. **定义用例**：一个公开方法一个动作；返回结构化结果，不发送消息、不渲染 HTML。
5. **定义 manifest**：登记 feature key、命令、路由、任务、配置和迁移版本。
6. **接入命令**：matcher 只做上下文解析和 `ReplyPlan`，检查重复命令和优先级冲突。
7. **接入 Web**：使用 blueprint、DTO、权限和统一错误格式；写 API contract 测试。
8. **接入调度**：任务使用稳定 ID、幂等 operation、超时和重试；提供手动运行说明。
9. **迁移旧数据**：先备份、dry-run、统计异常，再原子写入；保留可逆备份。
10. **补文档**：用户说明、管理员说明、配置、数据表、回滚、故障排查和变更记录。

### 7.2 Manifest 示例

```python
FEATURE = FeatureManifest(
    key="daily_fortune",
    title="每日运势",
    owner="gameplay",
    commands=(CommandSpec("今日运势", aliases=("运势",), permission="user"),),
    routes=(RouteSpec("/api/v1/daily-fortune", methods=("GET",), permission="user"),),
    jobs=(),
    config=(),
    migration_version="daily_fortune.001",
)
```

Manifest 加载器必须在启动时拒绝：重复 feature key、重复命令/路由/job ID、缺少权限声明、迁移版本倒退、没有 owner 或没有测试标签。

### 7.3 功能文档模板

`docs/features/<name>.md` 至少包含：

```markdown
# <功能名>
## 用户流程
## 命令与别名
## Web API（方法、路径、请求/响应、权限、幂等键）
## 数据模型与迁移
## 事务与失败回滚
## 定时任务
## 配置项
## 适配器差异
## 测试与手工验收
## 灰度开关、回滚和已知限制
```

## 8. 测试策略与验收门槛

### 8.1 测试分层

| 层级 | 内容 | 是否需要 NoneBot/网络 |
|:--|:--|:--:|
| Domain | 纯规则、数值、状态转换、时间边界 | 否 |
| Application | 仓储 fake、事务、幂等、错误映射 | 否 |
| Repository | 临时 SQLite、迁移、索引、回滚 | 否 |
| Adapter contract | OneBot/QQ 事件、消息段、权限、路由 | 可 mock |
| Web | Flask client、CSRF、权限、API schema | 否 |
| Browser smoke | 登录、核心页面、移动布局、写操作 | 是，隔离实例 |
| Remote smoke | 远端真实依赖、启动、数据库健康、关键命令 | 是，受控服务器 |

### 8.2 本机门槛

每个提交至少执行：

```sh
python -m compileall -q nonebot_plugin_xiuxian_2 tests
python -m unittest discover -s tests -v
git diff --check
```

重构阶段增加：

```sh
python -m unittest tests.test_infrastructure_runtime tests.test_db_backend -v
python -m unittest tests.test_adapter_selector tests.test_adapter_compat_records -v
python -m unittest tests.test_<feature>_service tests.test_<feature>_adapter -v
```

缺少可选依赖时，不允许简单吞掉导入异常；应在测试 bootstrap 安装锁定依赖，或把可选能力延迟导入并提供明确的 unavailable 状态。

### 8.3 远端后端冒烟

`/data/user/0/com.termux/files/home/.ssh/lmm-server.sh` 是连接封装，不在文档、日志或 CI 中复制其中的密码。命令示例只使用脚本路径和远端项目目录占位符：

```sh
SERVER=/data/user/0/com.termux/files/home/.ssh/lmm-server.sh

# 只查看工作目录和版本，不打印环境变量
"$SERVER" 'pwd; python --version; git rev-parse --short HEAD'

# 在远端项目目录执行只读检查
"$SERVER" 'cd <远端项目目录> && python -m compileall -q nonebot_plugin_xiuxian_2'

# 运行指定测试，使用远端专用 XIUXIAN_DATA_DIR
"$SERVER" 'cd <远端项目目录> && XIUXIAN_DATA_DIR=/tmp/xiuxian-smoke-data python -m unittest tests.test_infrastructure_runtime -v'
```

远端验收顺序：备份 -> 停止/隔离旧实例 -> 迁移 dry-run -> 启动新实例 -> 健康检查 -> 一个只读命令 -> 一个可回滚写命令 -> 查看 operation ledger -> 恢复旧实例（若是灰度）。禁止在生产数据上直接运行迁移测试；先复制数据库并校验备份哈希。

### 8.4 Definition of Done

功能只有同时满足以下条件才能合并：

- 业务规则、事务边界和错误码有测试；重复请求和失败回滚有测试。
- 新增 command/route/job/config/migration 都出现在 manifest 和文档中。
- 无新增跨层导入、直接数据库连接或未声明 Web 权限。
- `request_id`/`operation_id` 可从日志追踪到结果。
- Web 桌面和手机浏览器冒烟通过；后端远端冒烟通过。
- 有备份、迁移、回滚步骤，且不包含秘密或真实用户标识。

## 9. 代码审查和自动化守门

先加入只告警、后升级为失败的检查：

```text
check_no_direct_db_connect_in_features
check_no_flask_or_nonebot_import_in_core
check_no_startup_decorator_outside_bootstrap
check_all_web_endpoints_have_permission
check_manifest_ids_are_unique
check_migration_versions_are_monotonic
check_operation_id_on_asset_writes
check_no_secrets_or_runtime_data_in_git
```

建议限制（超过时必须在 PR 说明原因）：

- domain/application 单文件不超过 500 行；command adapter 不超过 250 行；Web blueprint 不超过 400 行。
- 一个函数不超过 60 行；嵌套分支超过 3 层时拆出策略函数。
- 新增业务 SQL 只能出现在 repository/infrastructure/database。
- 每个公开用例至少 1 个失败测试；资产用例至少 4 个事务测试。

PR 描述必须包含：影响 feature、数据迁移、兼容入口、权限变化、测试命令、浏览器截图/结果、远端冒烟结果、回滚方式。

## 10. 风险、优先级与回滚

| 优先级 | 风险 | 处理 |
|:--:|:--|:--|
| P0 | 资产重复发放/扣除 | 先上 operation ledger、幂等和对账，再迁移经济玩法 |
| P0 | 启动过程中重复注册或半初始化 | 统一 lifecycle，启动失败即 not-ready |
| P0 | Web 写接口越权或 CSRF 缺失 | blueprint 迁移时强制权限 decorator 和 client 测试 |
| P1 | 旧数据字段/JSON 不一致 | 只读兼容投影 + dry-run migration + 备份 |
| P1 | QQ/OneBot 行为差异 | adapter contract fixtures，不在业务层加平台判断 |
| P1 | 调度任务重跑 | 稳定 ID、幂等 key、并发策略、手动执行审计 |
| P2 | 前端重构引入移动端回归 | 页面逐个迁移，Termux 浏览器双断点截图 |

任何阶段发布都必须能执行以下回滚：

1. 关闭新 feature 或切换旧实现开关。
2. 停止写入并备份当前数据库、配置和 operation ledger。
3. 恢复旧代码，运行兼容迁移/回滚脚本。
4. 启动旧实例，执行只读健康检查和一条关键命令。
5. 保留失败操作记录，禁止用手工 SQL 静默“修平”流水。

## 11. 决策记录模板

每个有长期影响的选择单独记录 ADR（可先放在本文末尾或 `docs/adr/`）：

```markdown
# ADR-XXXX <标题>
日期：YYYY-MM-DD
状态：提案 / 已接受 / 已废弃

## 背景
## 选项
## 决策
## 代价与风险
## 迁移和回滚
## 影响的 feature / 数据 / API
```

至少需要记录：多数据库是否合并、前端是否引入框架、旧命令兼容周期、Web API 版本策略、operation ledger 保留周期、远端部署的停机窗口。

## 12. 推荐的首批实施顺序

不要从最大文件开始直接拆。建议顺序：

1. P0 基线、依赖和临时数据隔离。
2. P1 lifecycle + `RuntimeContext` + readiness。
3. P2 operation ledger/outbox + 一个简单资产动作。
4. 以“每日运势”或“活动签到”作为 P3 样板，完整走一遍新功能接入规范。
5. 迁移消息门面和一个 Web 页面，验证 QQ/OneBot 与手机浏览器。
6. 再处理灵石、物品、拍卖等高风险玩法。
7. 最后拆 `xiuxian2_handle.py`、大模板和兼容层，避免在未知边界上同时改变行为。

这条顺序的核心原则是：先让边界、事务和观测可用，再搬运业务代码；每搬一个垂直切片就能独立验证和回滚，新增功能从第一天开始按同一套规范进入系统。

## 13. 当前实现映射

本仓库已提供第一批可运行边界，作为后续迁移的基线：

- `nonebot_plugin_xiuxian_2/plugin.py`：新组合根，负责 manifest、RuntimeContext 和 Lifecycle wiring；旧 `__init__.py` 仅在真实 NoneBot driver 已初始化时加载兼容玩法。
- `nonebot_plugin_xiuxian_2/core/`：纯领域/端口/结果/错误模型；不依赖消息框架、Web 或数据库驱动。
- `nonebot_plugin_xiuxian_2/bootstrap/`：幂等生命周期、manifest 注册表和 readiness 报告；启动阶段失败时会清理当前半初始化阶段及已完成阶段，避免资源泄漏。
- `nonebot_plugin_xiuxian_2/infrastructure/database/`：DatabaseCatalog、SQLite Unit of Work、schema migration、operation ledger、outbox 和 reconcile。
- `nonebot_plugin_xiuxian_2/infrastructure/config/`：环境变量 > 部署配置 > JSON > 默认值的 typed settings，以及原子写入。
- `nonebot_plugin_xiuxian_2/features/daily_fortune/`：完整垂直样板，包含 domain、application、repository、命令/Web adapter、manifest、迁移说明和测试。
- `nonebot_plugin_xiuxian_2/features/sign_in/`：第二个完整资产垂直切片，包含签到领域模型、Unit of Work、operation ledger、兼容投影、命令/Web adapter、manifest、迁移和事务测试；旧 `修仙签到` 入口通过 `compatibility/sign_in.py` 转发。
- `nonebot_plugin_xiuxian_2/features/stone_gift/`：灵石赠送高风险资产切片，包含手续费规则、同事务扣款/入账/日额度、operation ledger、审计、幂等重放、CSRF Web API、旧命令 facade、迁移和失败回滚测试；旧 `送灵石` 入口通过 `compatibility/stone_gift.py` 转发，首次使用当天会把旧 `player.db` 额度作为一次性基线导入。仙缘次数仍属于旧 `stone_limit` 投影，随仙缘切片迁移。
- `nonebot_plugin_xiuxian_2/features/beg/`：新手仙途奇缘与新手礼包切片，包含每日灵石和礼包物品的 domain result、Unit of Work 仓储、operation ledger、状态拒绝、失败回滚、幂等回放、`beg.001` 迁移和灰度配置；旧 `xiuxian_beg.transaction_service` 只保留兼容 facade，历史操作表继续作为回放投影。
- `nonebot_plugin_xiuxian_2/features/activity_reward/`：活动一键领奖切片，统一子步骤协调器的 operation ledger、Web/命令契约和可重试结果；详细奖励规则仍由兼容 repository 投影提供。
- `nonebot_plugin_xiuxian_2/features/bank/`：灵庄存入、取出、会员升级和结息切片，统一跨库 application、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；旧 `Bank*Service` 仅作为惰性仓储适配器。
- `nonebot_plugin_xiuxian_2/features/combat_settlement/`：地图战斗结算切片，包装旧附加数据库事务，统一战斗快照、每日额度、背包容量的拒绝结果、operation ledger、审计和 Web 契约；旧地图 handler 已转发到应用层。
- `nonebot_plugin_xiuxian_2/features/admin_asset/`：管理员单人灵石调整切片，统一 admin 权限、条件余额更新、operation ledger、审计、幂等和 Web API；全服、物品、修为等其他管理员动作仍保留兼容服务。
- `nonebot_plugin_xiuxian_2/features/tianti_settlement/`：炼体按时间结算气血切片，统一 operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；旧 `TiantiSettlementService` 仅作为惰性仓储适配器，炼体其他动作仍保留兼容服务。
- `nonebot_plugin_xiuxian_2/features/tianti_training/`：灵石炼体、药浴、炼体突破和冲窍切片，统一 application、operation ledger、审计、幂等重放、四个 Web API、迁移标记和灰度开关；旧跨库事务服务仅通过惰性 repository adapter 调用。
- `nonebot_plugin_xiuxian_2/features/tower/`：通天塔积分兑换、单层挑战和连续挑战结算切片，统一 application、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；战斗算法与读模型仍由兼容命令适配器提供，旧跨库事务服务仅作为惰性仓储适配器。
- `nonebot_plugin_xiuxian_2/features/arena/`：竞技场荣誉兑换、购买挑战次数和挑战结算切片，统一 application、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；战斗匹配、排行榜、挑战券和赛季任务仍由兼容适配器提供。
- `nonebot_plugin_xiuxian_2/features/puppet/`：灵田傀儡购买和升级切片，统一 application、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；开启/关闭、自动收取和灵田读模型仍由兼容适配器提供。
- `nonebot_plugin_xiuxian_2/features/boss/`：世界BOSS积分兑换和讨伐结算切片，统一 application、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；刷新、天罚、排行榜和活动首领读模型仍由兼容适配器提供。
- `nonebot_plugin_xiuxian_2/features/dungeon/`：副本商店兑换及探索准备/结算/重放边界，统一 application、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；战斗算法、队伍管理和奖励计划仍由兼容适配器提供。
- `nonebot_plugin_xiuxian_2/features/pet/`：宠物游历派遣/领取、喂食和孵化切片，统一 application、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；融合、放生和技能动作仍由兼容适配器提供。
- `nonebot_plugin_xiuxian_2/features/sect/`：宗门成员加入、商店兑换、主/副功法学习和炼体堂领奖切片，统一 application、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；宗门建设、任务、传位和解散仍由兼容适配器提供。
- `nonebot_plugin_xiuxian_2/features/natal_treasure/`：本命法宝觉醒、重塑、养成、道纹升级、铭刻和遗忘切片；旧法宝事务服务通过惰性仓储适配器调用，应用层统一 operation ledger、审计、Web/命令契约、迁移标记和灰度开关。
- `nonebot_plugin_xiuxian_2/features/buff/`：功法、洞天福地、闭关和切磋结算切片；历史 Buff/玩家表仍由兼容仓储维护，所有新入口统一幂等和失败重试。
- `nonebot_plugin_xiuxian_2/features/base/`：突破、渡劫、改名、灵石争夺/抢夺和签到基础动作边界；旧数值算法只位于 repository，应用层负责操作号、审计和回滚。
- `nonebot_plugin_xiuxian_2/features/back/`：礼包、物品、装备、技能、修复、宠物蛋和炼丹库存动作切片；跨 game/player 库写入继续由旧事务服务完成并可重放。
- `nonebot_plugin_xiuxian_2/features/trade/`：鬼市存取、拍卖队列、场次和现世购买切片；交易库由兼容 repository 持有，Web/命令只解析 DTO。
- `nonebot_plugin_xiuxian_2/features/map/`：地图移动、回城、交互、战斗、探索、任务和资源奖励切片；地图算法和掉落计划仍由兼容适配器提供，统一 ledger 记录跨库结果。
- `nonebot_plugin_xiuxian_2/features/rift/`：裂隙生成、进入、终止、事件、加速和结算切片；世界状态及历史数据通过惰性 repository 适配，支持幂等重放和灰度回滚。
- `nonebot_plugin_xiuxian_2/features/sect_fairyland/`：宗门炼体堂领取切片，统一 application、请求/响应 DTO、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；旧宗门跨表事务服务仅通过惰性 repository adapter 调用。
- `nonebot_plugin_xiuxian_2/features/world_events/`：魔修入侵奖励领取切片，统一跨库领奖 application、请求/响应 DTO、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；贡献和随机奖励计算仍由兼容命令适配器提供，旧 `DemonClaimService` 仅作为惰性跨库仓储。
- `nonebot_plugin_xiuxian_2/features/work/`：悬赏令接取和结算切片，统一请求/响应 DTO、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；旧 `WorkClaimService`、`WorkSettlementService` 仅作为惰性仓储，刷新/终止继续由兼容服务负责。
- `nonebot_plugin_xiuxian_2/features/mixelixir/`：炼丹灵田收取与统一结算切片，统一请求/响应 DTO、operation ledger、审计、幂等重放、Web/命令契约、迁移标记和灰度开关；旧灵田跨库事务服务仅作为惰性仓储适配器，旧配方两阶段扣材/补领奖励流程仍由兼容服务维护。
- `nonebot_plugin_xiuxian_2/features/{activity,admin,beg,compensation,dongfu,dufang,entertainment,fusion,impart,impart_pk,info,lunhui,past_life,simulator,status,tasks,tianti,title,training}/`：剩余旧包的统一迁移边界；每个切片都有独立 manifest、DTO、application、repository、迁移、适配器、测试和文档，旧算法继续通过惰性兼容仓储提供，避免导入期打开数据库或注册 matcher。`illusion` 与 `interactive` 已从该集合提升为真实领域仓储切片，旧 import 仅保留兼容 facade。
- `nonebot_plugin_xiuxian_2/adapters/web/`：统一 API 响应、request_id、CSRF/权限边界和 Flask app factory；旧 Web app 保持兼容，旧 URL 的 308 转发命中会写入数据目录的 `compatibility_hits.json`。
- `nonebot_plugin_xiuxian_2/compatibility/release_gate.py` 与 `scripts/check_compatibility_release.py`：记录发布起点的兼容命中基线，校验后续版本、旧 import/URL 日志、历史迁移覆盖和备份恢复回执；未通过时禁止关闭兼容周期。
- `scripts/refactor_completion_audit.py`：汇总 P0-P7 阶段状态；缺少真实发布周期参数时显式报告 P7 pending。
- `nonebot_plugin_xiuxian_2/compatibility/scheduler.py`：旧 APScheduler decorator/add_job 声明通过延迟桥收集，组合根在 `jobs` 生命周期阶段统一激活；导入旧玩法包不再修改 live scheduler，任务 handler 仍保留在兼容包一个发布周期。
- `tests/__init__.py`、`tests/bootstrap.py` 和 `tests/conftest.py`：在 unittest/pytest 导入与执行边界设置隔离数据目录，叠加静态资源并排除数据库及运行态文件，避免测试触碰部署目录。
- `nonebot_plugin_xiuxian_2/adapters/web/blueprints/pages.py`：新 Web factory 提供 `/login`、`/`、`/logout` 会话流程；管理员 ID 优先读取 `XIUXIAN_WEB_ADMIN_IDS`，未配置时兼容 `SUPERUSERS`。
- `scripts/check_architecture.py`：CI 可执行的跨层导入守门；`docs/refactor_baseline.md` 和 `requirements.lock` 固定 P0 基线。
- `.github/workflows/main.yml`：main/PR/正式标签共用只读质量门禁，执行锁定依赖安装、两种 unittest 收集、compileall、架构守门、清单校验和空白检查；正式发布 job 显式依赖 quality 通过。
- `scripts/export_refactor_inventory.py` 与 `docs/refactor_inventory.json`：可重放记录命令、Web URL、数据库表、JSON 资产和定时任务清单；架构守门会拒绝过期清单。

本轮完成的运行时治理补充：

- 组合根在 `filesystem -> database -> migrations -> repositories -> jobs -> web -> ready` 阶段执行真实 wiring；`install_driver_hooks` 对同一 NoneBot driver 幂等。
- 所有新适配器路由、平台配置和兼容调度任务均有 manifest；架构脚本会用 Flask `url_map` 反查未声明端点。
- 启动、CLI 迁移和恢复演练共用 `plugin.build_migrations()` 的完整迁移清单（当前 144 项）；新 adapter 模板禁止页面级可执行内联脚本，旧模板仅作为兼容资产保留一个发布周期。
- `operation_ledger` 同时写入 `operation_audit`；异常在业务事务回滚后以 `failed` 记录，`ReconcileService.run` 支持重试和 dead 事件可见性。
- `BackupService` 和 `scripts/recovery_smoke.py` 提供带 SHA-256 manifest 的备份、校验和恢复演练；`--evidence` 回执可由兼容周期 gate 校验；CLI 提供 `manifest/health/migrate/reconcile/backup/restore`，其中 `migrate --dry-run` 只读预览待执行版本。
- `scripts/remote_smoke.sh` 提供受控远端后端冒烟；必须显式提供远端项目/数据目录、停机/启动、可逆写和回滚 hook，缺少安全前置条件时拒绝执行，不复制 SSH wrapper 凭据。
- 新 Web factory 提供统一 JSON、request ID、CSRF、权限、配置/数据库/调度/备份/对账接口，并将现有管理 URL 以 308 兼容转发；前端共享 API/state/DOM 模块。
- 拍卖竞价与场次收尾均经过 `features/auction` application；`auction.settle` 提供稳定任务 ID、管理员手工重试 API 和 operation ledger，详细历史 SQL 通过可替换 repository adapter 隔离。
- 地图战斗结算和管理员单人灵石调整已接入新 application；旧事务服务只作为惰性 repository adapter，均保留灰度开关、迁移标记、回滚入口和兼容测试。
- 通天塔积分兑换以及单双层挑战结算已接入 `features/tower` application；旧购买/结算 facade 只负责输入组装和消息适配，统一 ledger 位于 `game_db`，历史塔状态与跨库写入仍由兼容仓储维护。
- 旧 `xiuxian_*` 包已登记为 `compatibility/feature_inventory.py`，每项都声明迁移目标和删除发布版本；旧 shim 发出 `DeprecationWarning`，并把命中次数持久化到数据目录的 `compatibility_hits.json`（内存计数仅作为写入失败时的降级）。兼容模块的启动/关闭回调通过 `bootstrap.legacy` 显式登记，由组合根统一按顺序执行和逆序停止，不再直接向 NoneBot driver 注册 hook；旧调度声明也通过 `compatibility/scheduler.py` 延迟到 `jobs` 生命周期阶段激活。它们仍需至少一个完整发布周期的运行数据后才能按第 7 节删除，不在本轮静默移除。
- `features/_legacy_feature.py` 的通用兼容仓储也统一发出 `DeprecationWarning` 并记录 `feature:<key>` 命中，避免未包装的旧玩法 application 绕过 P7 观测。
- `features/_service_port.py` 的 `execute_callback` 为已预组装参数的旧事务提供显式端口；迁移 application 优先通过该端口执行，避免 callback 直接越过 repository，同时保留可观测的兼容命中记录。
- `scripts/check_architecture.py` 已提供可独立调用的门禁：核心层导入、feature 直连数据库、生命周期 hook、Web 权限、manifest ID、迁移版本、资产操作号、运行时文件、legacy 命令 AST/manifest 与迁移别名、兼容仓储占位结果和旧入口 application 边界检查；当前所有检查均为绿色。
- `scripts/recovery_smoke.py` 覆盖完整迁移清单（含 legacy slice），会执行备份、恢复、迁移和对账；当前恢复演练结果为零未处理 operation/outbox/dead event。
- `ReconcileService.run` 支持按 action 注册补偿处理器，能够实际收敛 `failed/needs_reconcile` 操作和 outbox 事件；`JobExecutor.run_sync` 可安全从 Flask/CLI 或已有事件循环调用。
