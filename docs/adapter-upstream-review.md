# 适配器上游修复审查

审查日期：2026-10-10。本文是只读核对结果，不是适配器升级方案；本轮没有覆盖
vendor 源码、升级依赖或连接真实平台。上游证据来自官方 GitHub API、官方 commit
源码 tarball 和本地三方合并检查。除明确写为“已核实”的内容外，不把部署猜测当作
事实。

## 结论

- 默认配置是 `vendor`。新进程中，`selector.configure_adapter_paths("vendor")`
  实际导入 QQ/OneBot 内置文件；`auto` 在本工作区选择已安装的 QQ、内置 OneBot。
  证据见 `xiuxian/xiuxian_adapter/selector.py:68-119` 和隔离来源探针结果。
- 入口存在一个重要的缓存边界：`docker/bot.py:2-13` 先导入两个规范适配器，随后
  `load_from_toml` 才加载插件。若 QQ pip 类已经在 `sys.modules`，
  `early_inject.py:17-49` 重排路径不会替换已加载的 `Bot`；它只在
  `early_inject.py:52-108,170-255` 尝试补 `group_members` bit 和成员事件。
  预加载隔离探针实测 `Bot` 仍来自 pip，路径诊断的 `effective=vendor` 不能单独证明
  实际类已换成 vendor。
- 本地 QQ vendor 确实是声明的官方固定提交加魔改；OneBot runtime 文件与声明的固定
  官方提交逐字相同。当前 `.venv` 有 `nonebot-adapter-qq==1.7.1`，没有可用的
  `nonebot-adapter-onebot` distribution metadata；Docker 的 OneBot/QQ 依赖未 pin，
  因而真实部署版本仍待核对。
- 上游 QQ #216（空群 at 消息保护）已在固定基线和 vendor 中；QQ #219、Gateway
  timeout、`Modal/group_id` 模型改动可在隔离副本中干净套用，但不能因此宣称真实平台
  已验。QQ 回复解析、群管理和消息模型改动与本地扩展存在冲突，需手工三方移植。
- OneBot 从固定基线到官方 `master` 的 runtime 行为改动只有导入排序；本地已含固定
  基线的 runtime，未发现缺失的 OneBot 行为修复。

## 固定基线和来源

| 适配器 | 本地声明 | 官方仓库/固定提交 | 官方当前 `master` 查询结果 | 本工作区实际来源 |
|---|---|---|---|---|
| QQ | `1.7.1` | [adapter-qq@1cdc342](https://github.com/nonebot/adapter-qq/commit/1cdc342babde46570711eea93f8c6c41ae1bfb4f) | `dac0b92f53c302d6339ce58502efd4755c133ef5`，版本 1.7.3 | `.venv` metadata 1.7.1；vendor 或已缓存的 pip，取决于导入时序 |
| OneBot | `2.4.6` | [adapter-onebot@6fe0113](https://github.com/nonebot/adapter-onebot/commit/6fe01137868375afdb73a1c31e0c72dee1249703) | `2f5930d4f6c7fb3a82cf3201639e8f0b0a6c124e`，版本 2.4.6 | 本 `.venv` 未安装；默认路径的 vendor |

固定提交、版本字段和仓库 URL 均由官方 commit API 与 tarball 中的 `pyproject.toml`
交叉核对。QQ 固定 commit 的 pyproject 版本为 1.7.1；它不是标签 `v1.7.1` 的对象，
所以报告使用 commit 而不把标签 SHA 冒充基线。OneBot 固定 commit 位于 v2.4.6 标签之后，
但 pyproject 仍为 2.4.6。vendor `UPSTREAM` 文件的声明分别见：

- `nonebot_plugin_xiuxian_2/xiuxian/xiuxian_adapter/vendor/adapter_qq/UPSTREAM`
- `nonebot_plugin_xiuxian_2/xiuxian/xiuxian_adapter/vendor/adapter_onebot/UPSTREAM`

隔离来源探针（不导入项目初始化、不创建 driver/数据库、不连平台）得到：

```text
vendor: qq=vendor, onebot=vendor; QQ Bot=vendor; intent bit24=true;
        GROUP_MEMBER_ADD/REMOVE 已注册
auto:   qq=installed, onebot=vendor; QQ Bot=.venv/site-packages;
        intent bit24=false; 成员事件为空
preloaded pip + early_inject: qq path diagnostic=vendor,
        但 QQ Bot 仍为 .venv/site-packages；intent/member event 被补上
```

因此应同时看 `diagnostics.adapters.*.file/source` 和 `selection.effective`，不能只看
后者。默认配置来源为 `xiuxian/xiuxian_config.py:106`；`adapter_compat.py` 是自写兼容
包装层，负责上下文、引用、msg_seq/retry 和发送路由，不是第三个协议实现。

## B/L/U 三方差异

本节中 `B` = 官方固定提交，`L` = 当前 vendor，`U` = 官方当前 master。QQ 的 runtime
文件集合相同（15 个）；OneBot runtime 文件集合相同（26 个）。本地 OneBot 文件集合
SHA 与 B 相同；QQ 的 B/L/U runtime manifest SHA（用于审查复现）分别为：

```text
QQ     B 8ae5d74e18c175dd23a01a191da7db8a61fdaef8f83f838b076edf319621feff
       L 9699cd38b26eaceb24a04f7896a6e2d83b2d34848e203900d6dfec9825263895
       U 09885de4a3a8987c8f8a0a63f9eeb034feb933eb8bbbbd2bc1c61e19ee561aa0
OneBot B/L 5cf2ff4193586a2b79de4780193890fc7637309622c288552eb048cf953bf10d
       U    cd78e396508e4e1122f7b8c9f5327d7e9e41ae978db3541203560c512f841b0b
```

### QQ 本地魔改（已核实）

- `vendor/.../qq/bot.py`：提取 `REFIDX/msg_idx`，只有引用 ID 时自动构造
  `message_reference`；扩展 stream/prompt keyboard/action button；本地附件分片阈值和
  默认扩展名策略。
- `vendor/.../qq/event.py`：C2C/group 在 openid 缺失时使用 `author.id`，并同步会话 ID。
- `vendor/.../qq/models/qq.py`：作者、scene、reply 字段可空/默认；新增
  `PostMessagesExtInfo`、更丰富的消息/文件返回字段。
- `vendor/.../qq/config.py`：`Intents.group_members=True`。
- 其余 QQ runtime 文件与 B 一致；声明的 #216 保护在 B、L 均存在。

### 上游后续修复分类

| 官方提交/主题 | 与 L 的结果 | 处理判断 |
|---|---|---|
| [0d8837f #219](https://github.com/nonebot/adapter-qq/commit/0d8837f23c63e9b6fd38d97abd7969bfd323644f)：按 `msg_idx` 选择 reply、群引用 at 校验 | `git apply --check` 在隔离 L 副本通过；但会改 QQ `bot.py` 的 `_check_reply` | **可直接移植代码块**，仍需短合同覆盖 reply/at；不要覆盖本地引用提取和发送扩展 |
| [2c05da3 #222](https://github.com/nonebot/adapter-qq/commit/2c05da30e8b9a6cfb080ab1334ed73d72f22e706)：群聊回复命令解析 | bot/model 补丁均不能直接套用 | **冲突，人工移植**；先保留本地 `_check_reply`、可空模型和 `REFIDX`，逐字段合并 |
| [7251976](https://github.com/nonebot/adapter-qq/commit/7251976addfea5db2dbbe3496836fb561b6fe0df)：移除 token/sandbox，更新 QQ API/auth base | 文本检查通过，但属于配置/平台策略变化 | **不要本轮直接移植**；需部署凭据、API 端点和真实平台验收后单独变更 |
| [efcdea5 #233](https://github.com/nonebot/adapter-qq/commit/efcdea52f0bcde9b3a4fed8362890758474244fa)：Gateway receive timeout | `adapter.py` 文本检查通过，当前 L 无该文件魔改 | **可直接移植**，移植后做连接/超时短合同；真实 Gateway 另验 |
| [2f7314f #228](https://github.com/nonebot/adapter-qq/commit/2f7314fee34e47f9b102f9c421f755e9497a1126)：reply `message_type/msg_idx` 可空 | 不能把上游 hunk 原样套到本地模型；本地已将相关字段和作者改为可空/默认 | **语义已部分存在**；逐字段对照后补缺，不能写成完整等价已证实 |
| [fc81f677 #234](https://github.com/nonebot/adapter-qq/commit/fc81f6776ddc253b43d7ba23ab8cfe1aab040b87)、[67d796f #235](https://github.com/nonebot/adapter-qq/commit/67d796f804c03d107b6cd5681b8dbaa2dcf27a42)：群申请/成员管理 | bot/event/models 多处冲突；本地有模型扩展 | **人工三方移植**；当前业务未证明使用这些 API，列为待验，不自动扩大范围 |
| [314c3df #239](https://github.com/nonebot/adapter-qq/commit/314c3dfe74efceffa7e7539eba69af3f83b491af)：inline keyboard `group_id/modal` | `models/common.py` 检查通过，本地无同段魔改 | **可直接移植模型 hunk**；补键盘序列化短合同 |

`0d8837f`、`7251976`、`efcdea5`、`314c3df` 的“通过”是隔离副本的补丁语法检查，
不是对业务或真实 QQ 的运行验收。官方当前 master 还包含版本、依赖和 API 变化，不能
用一次整包覆盖代替逐提交审查。

### OneBot

从 B 到 U 的 runtime 行为差异只有 `v11/__init__.py`、`v11/bot.py`、`v12/__init__.py`
的导入排序；`git apply --check` 通过，且 B/L runtime manifest 相同。此前 B 到固定
commit 的事件/权限文件变化已包含在 6fe0113，故没有发现缺失的 OneBot 运行修复。
OneBot 的 pip 版本、启动顺序和真实协议端点仍未在本工作区证明。

## 三方移植方法

每个候选提交均按以下方法处理，不直接解压覆盖 vendor：

1. 保留 B、L、U 三份只读树；以 `git merge-file --diff3 L B U` 生成审查结果。
2. 无冲突且属于本地未改区域的 hunk，可复制到独立补丁；每个 hunk 仍需对应短合同。
3. 冲突时以 L 的魔改行为为保留基线：`bot.py` 保留引用提取、扩展发送和附件策略；
   `event.py` 保留 openid fallback；`models/qq.py` 合并可空字段、扩展返回模型与上游
   新字段；`config.py` 不自动接受 API/token/sandbox 政策改变。
4. 重新运行纯模型/序列化和 fake-router 短合同后，才能考虑单独提交；真实平台端点、
   Gateway、权限和凭据必须另列联调，不由本地合同替代。

当前实际三方检查在 QQ `models/qq.py` 产生 3 个冲突区；`bot.py` 的 #222/#228/群管理
补丁也无法原样套用。这是“需人工移植”的证据，不是未知基线的推测。

## 合同测试选择

按接入差异选择短合同，共享业务长链只验一份：

| 差异 | 短合同 | 已有证据/边界 |
|---|---|---|
| vendor/installed/auto 选择和缓存来源 | `tests/test_adapter_selector.py`；隔离 `__file__`/类来源探针 | 已证明路径选择；预加载 pip 场景需保留来源断言 |
| OneBot WebSocket 共享业务长链 | 只选一条代表性链路，验证事件经 WS 接入共同 application/repository | 不因 QQ 接入重复跑相同业务长链；其它共享业务按变化选一份验收 |
| QQ 格式与富消息 | 消息段格式、Markdown、蓝字、按钮的本地短合同 | 不据此声称 QQ 官方客户端呈现或平台权限已通过；真实平台待验 |
| QQ 回调和能力降级 | interaction callback ACK、Markdown/按钮能力缺失时的 fallback 短合同 | `tests/test_qq_compat.py` 为本地模拟；不重复共享业务长链 |
| 统一 application/repository 的共享业务长链 | 受影响业务只完整验一份 | OneBot WS 代表性链路作为一次共享链验收；QQ 只验接入专属短合同 |

不得把本表的“已有证据”写成真实平台通过。QQ 官方客户端呈现、蓝字/按钮效果和权限
均未实测，不得宣称通过；真实平台待验还包括 Gateway/API/auth 端点、引用/附件/扩展消息、
成员事件 intent，及 OneBot 实际安装版本、driver 和 WebSocket 协议端点。

## 限制和后续边界

- 本轮未修改任何适配器源码、依赖、锁文件或运行数据；未升级 QQ 到 1.7.3，也未执行
  上游更新命令。临时官方源码/JSON 只在隔离目录使用，报告提交前清理。
- 未导入项目初始化、未连接平台、未读取 `.env`、数据库、WAL/SHM 或用户数据；真实
  部署的 plugin 加载顺序可能不同，必须用启动日志/诊断文件确认。
- 6148 pytest、304 subtests、2946 unittest 是既有通过证据，本轮不重跑；本报告和两份
  执行文档只做链接、token、diff 与来源审查。
- `boss_info.json` 保持原样，SHA256 为
  `84b7ef679eb85ddc1a28b62846c7a134072da2d200fad9a689428a9bdb15e517`，不纳入提交。
