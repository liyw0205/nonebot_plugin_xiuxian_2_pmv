# 重构基线

记录日期：2026-09-12  
分支：`main`（重构前基线）

## 环境

- Python：3.13（Termux）
- 数据目录：测试必须通过 `XIUXIAN_DATA_DIR` 指向临时目录；本报告不包含运行期数据。
- 依赖来源：`requirements.txt`；可重建版本清单见 `requirements.lock`。
- 编译命令：`python -m compileall -q nonebot_plugin_xiuxian_2 tests`
- 测试命令：`python -m unittest discover -s tests -q`
- 全仓库递归测试：`python -m unittest discover -s . -p 'test_*.py' -q`
- P0 清单导出：`python scripts/export_refactor_inventory.py --check`

## 已知失败

基线曾执行约 1640 个测试：21 个失败、69 个错误。主要类别：

1. 当前环境缺少 `qrcode`，导入 Web QQ 绑定模块失败；依赖文件已声明该包。
2. 少数测试使用文件加载模块，导致相对导入错误。
3. 部分日期断言依赖系统日期或运行期数据。
4. 旧测试会在默认数据目录初始化插件，尚未全部隔离到临时目录。

这些失败不被重构入口吞掉；每一类都应在迁移阶段转为明确的 bootstrap、延迟导入或依赖安装错误。

`docs/refactor_inventory.json` 由 `scripts/export_refactor_inventory.py` 从 manifest、旧
Web 路由、调度声明、源码 SQL 和仓库 JSON 资产生成，覆盖命令/别名、URL/权限、job ID、
数据库表、数据库文件和静态 JSON 文件。架构守门会重新生成并比较该文件，避免 P0 清单
在入口或数据迁移变化后失真。

## 重构后复测

2026-09-13 增补复测：`python -m unittest discover -s tests -q` 执行 1733 个测试，仓库根目录递归收集执行 1823 个测试，均通过；新增迁移服务事务锁和兼容 ServicePort 回归覆盖，并验证维护工具拒绝空数据目录。

2026-09-12 复测执行 `python -m unittest discover -s tests -q`：1725 个测试全部通过；执行仓库根目录递归收集命令，1808 个测试也全部通过。`tests/__init__.py` 在测试模块导入前创建临时数据目录，`tests/bootstrap.py` 只复制静态 JSON/图片/配置资源并排除数据库、备份、日志、缓存和玩家运行态目录，因此两条命令都不会初始化仓库 `data/xiuxian`。期间已修复惰性插件入口、仓储源文件加载、拍卖状态兼容、拍卖收尾 application、活动奖励领取 application、地图战斗结算 application、管理员灵石/物品发放 application、炼体结算 application、宗门炼体堂领取 application、世界事件领奖 application、悬赏令接取/结算 application、通天塔兑换与结算 application、灵庄四项资产 application、竞技场购买/买次数/挑战结算 application、灵田傀儡购买/升级 application、周限购日期快照、地图旧奖励字段迁移、QQ 适配器来源诊断、管理状态入口、统一 CLI/启动迁移清单以及 Web 登录入口。新架构测试、每日运势服务/适配器、炼体结算服务、宗门炼体堂服务、世界事件领奖服务、悬赏令接取/结算服务、通天塔 application、灵庄 application、竞技场 application、傀儡 application、兼容周期 gate（含 SemVer 和版本递增校验）、P0 命令/路由/任务/数据清单、IPv6 Host 白名单、稳定拍卖调度 operation、配置 Host 白名单、迁移 dry-run 只读预览、编译检查和架构守门均通过。

测试和维护命令使用显式数据目录时不会执行旧资源下载钩子；部署启动仍通过 NoneBot driver 显式启用兼容启动。旧资源下载也支持 `XIUXIAN_AUTO_DOWNLOAD_RESOURCES` 环境覆盖。

## 恢复演练

功能迁移标记写入 `game_db`；炼体结算的统一 ledger 与历史状态仍按兼容仓储写入 `player_db`，宗门炼体堂领取的 ledger 与历史状态也仍在 `player_db`，世界事件领奖、悬赏令接取/结算、炼丹灵田、通天塔、灵庄、竞技场和灵田傀儡统一 ledger 写入 `game_db`，跨库历史操作表继续由兼容仓储维护。恢复步骤为：停止实例、复制数据库与 `backups/`、校验文件哈希、运行 `MigrationRunner` 和 `ReconcileService`，确认未处理 operation/outbox 数为零。`scripts/recovery_smoke.py --evidence <receipt.json>` 会额外写入完整迁移、备份恢复和对账回执，供 P7 发布周期 gate 校验。

## 隔离 Web 冒烟

使用临时 `XIUXIAN_DATA_DIR` 和端口 `5889` 启动 `python -m nonebot_plugin_xiuxian_2 serve`，验证 `/health/live`、`/health/ready`、统一 registry、未登录拒绝、管理员 ID 登录、首页、管理页、CSRF 接口和退出流程。`chromium-browser --headless` 在 1440x900 与 390x844 下均生成登录页截图并检查无布局溢出；截图只保存在 Termux 临时目录，不进入仓库。

2026-09-12 复验记录：Chromium DevTools headless 会话依次完成登录、`/` 首页、`/pages/config` 配置只读、`/messages` 消息列表、`/pages/scheduler` 调度只读、带 CSRF 的配置写入和 `/logout`。桌面视口为 `1440x900`，手机视口为 `390x844`；`scrollWidth` 分别为 `1425` 和 `390`，没有横向溢出。各 API 均返回统一 envelope 和 `request_id`，写入返回 HTTP 200，退出后回到 `/login`。管理员 ID、Cookie 和截图均未写入仓库。

2026-09-13 复验记录：在临时 `XIUXIAN_DATA_DIR`、隔离端口 `5889` 上再次完成同一浏览器流程；Chromium 实际生成 `1440x900` 和 `390x844` 登录截图，页面滚动宽度分别为 `1425` 和 `390`，CSRF 配置写入返回 HTTP 200 且包含 `request_id`。截图、Cookie 和管理员 ID 均只保留在临时目录。

2026-09-13 可复现浏览器冒烟：新增 `scripts/browser_smoke.py`（Playwright + Chromium），在隔离数据目录和端口 `5892` 上以 `1440x900` 与 `390x844` 两个视口真实驱动登录页、登录、首页和管理页，逐页断言 `documentElement.scrollWidth <= innerWidth` 并输出截图与 JSON 报告。首次执行发现真实 P6 缺陷：手机视口首页 `scrollWidth=533 > innerWidth=390`，横向溢出。定位到管理页把约 141 KB 的 JSON 响应渲染进一个 `white-space: pre` 且无宽度约束的 `<pre>`（`pre_scrollWidth=525`、`pre_clientWidth=374`、`overflowX=visible`），而该页面此前完全没有样式表。修复方式是新增最小样式表 `nonebot_plugin_xiuxian_2/adapters/web/static/app.css`（约束输出面板为 `pre-wrap` + `overflow-wrap:anywhere`、限制 `max-width:100%`、图片与表格不超出视口）并在两个页面模板中通过 `url_for` 引用。复跑后两个视口 `scrollWidth == innerWidth`（`1440` 与 `390`），溢出元素数为 `0`，`<pre>` 内容仍完整渲染（`text_len=141570`），不是靠 `overflow:hidden` 掩盖。回归防护写入 `tests/test_refactored_web.py`，断言样式表被模板引用且包含 `pre-wrap` 约束。

2026-09-13 浏览器冒烟最终回执：`sh scripts/browser_smoke_hooks/run.sh` 在临时数据目录、随机空闲端口 `42439` 上真实启动 `serve --port 42439`，通过 `SUPERUSERS=12345` 完成未登录重定向、CSRF 登录和 dashboard 导航。回执为 `ok=true`、`errors=[]`、readiness=`http://127.0.0.1:42439/health/ready`；desktop `1440x900` 的 `scroll_width=1440`、`inner_width=1440`、`overflow=false`，mobile `390x844` 的 `scroll_width=390`、`inner_width=390`、`overflow=false`。截图和 receipt 保留在临时目录 `/tmp/xiuxian-browser-smoke-out-BlfK`，未写入仓库；退出钩子已停止隔离服务。


远端冒烟复验记录：由于远端没有本项目，先将当前源码复制到隔离目录并在专用虚拟环境安装锁定依赖，使用端口 `5898` 和独立数据目录执行 `scripts/remote_smoke.sh`。备份、迁移 dry-run、停止/启动、`/health/ready`、manifest、可逆 marker 写入、reconcile、停止新实例、哈希校验恢复和旧实例恢复均通过；结果为 `operations=0`、`outbox_events=0`、`dead_events=0`。冒烟结束后已删除远端临时副本、虚拟环境和数据目录，未触碰远端原有 `/root/xiu2` 服务。

2026-09-13 复验记录：远端原 `/root/xiu2` 仍是旧项目，因此将当前源码复制到 `/tmp` 隔离目录并按 `requirements.lock` 创建虚拟环境；使用端口 `5999` 执行 `scripts/remote_smoke.sh`，备份、全量迁移 dry-run、隔离启动、健康检查、manifest、可逆写、对账、停止和恢复均返回成功，`operations=0`、`outbox_events=0`、`dead_events=0`。远端临时源码、虚拟环境、数据和进程已清理，原项目未修改。

2026-09-13 受控主机复验记录：本轮在独立容器主机（`docker` 的 `python:3.11-slim`，与宿主进程、端口和文件系统隔离）上复现完整 8.3 顺序，作为可重复的受控部署实例，而不是临时演练。步骤为 `scripts/remote_smoke_hooks/provision.sh`（迁移 smoke 数据目录、复制一份作为旧部署、在 5897 端口启动旧实例并等待 `/health/ready`），再执行 `scripts/remote_smoke_hooks/run.sh`。实测输出：

```text
[remote-smoke] backup
[remote-smoke] migration dry-run          {"dry_run": true, "pending": []}
[remote-smoke] stop old instance          stopped pid=1026
[remote-smoke] start new instance         started pid=1086 port=5898
[remote-smoke] readiness health check     (通过)
[remote-smoke] read-only manifest command (通过)
[remote-smoke] reversible write hook      {"ok": true, "marker": "/srv/smoke-data/remote-smoke-marker.json", ...}
[remote-smoke] operation ledger           {"clean": true, "operations": 0, "outbox_events": 0, "dead_events": 0}
[remote-smoke] stop smoke instance        stopped pid=1086
[remote-smoke] restore backup             {"dry_run": false, "restored": ["game_db"], "sha256": "d56a010f..."}
rollback verified: backup restored and smoke marker removed
old instance restarted pid=1165 port=5897
[remote-smoke] PASS operation_id=refactor-smoke-20260913T001401Z-2982258
```

冒烟结束后独立复核（不依赖脚本自报）：旧实例 `/health/ready` 返回 `ready=true`（`database/filesystem/jobs/migrations/repositories/web` 全部为 `true`）且 PID `1165` 存活；5898 端口已关闭（`http_code=000`）；`remote-smoke-marker.json` 已不存在。恢复走 `BackupService.restore`，对 manifest 内每个文件重新计算 `sha256` 后才落盘，因此校验不一致会直接失败而不是静默覆盖数据库。容器主机、镜像、数据目录和端口均与宿主的 `/root/xiu2` 生产实例无关，未触碰该实例。

本轮同时修复了该路径上的一个真实缺陷：回滚 hook 原先断言「恢复后 marker 必须消失」，但备份只覆盖数据库，marker 是数据库之外的文件，恢复不可能删除它，因此冒烟必然在最后一步失败。修正后由 hook 负责撤销自己创建的可逆写入，数据库恢复仍由 `restore` 的哈希校验保证；新增 `tests/test_remote_smoke_hooks.py` 覆盖 stop 幂等、stop 终止 PID、回滚删除 marker、恢复失败即回滚失败，并把 hook 的存在性、可执行位和各自的安全属性纳入 `check_remote_smoke_contract`。

仓库现提供 `scripts/remote_smoke.sh` 作为可复现执行器。它要求显式设置
`REMOTE_PROJECT_DIR`、`REMOTE_DATA_DIR` 以及服务停止/启动、可逆写和回滚命令；
在未提供真实远端目录和这些 hook 前，脚本会失败并保持本机基线不变。
