# terminal：Web 终端与 PTY 会话

## 用户流程

管理员在旧 Web 控制台点「Web 终端」进终端页（`GET /terminal`），首次进入会被送去确认页（`GET /terminal/confirm`）输入专用终端密码；授权通过后浏览器用一条长连接拉输出（`GET /terminal/output`）、用 POST 写按键（`POST /terminal/write`）、每 5 秒轮询一次当前目录（`GET /terminal/pwd`）。前端把输入框内容原样送到 PTY，Ctrl 组合键在浏览器侧转成控制字符，输出经 ansi_up 上色（`xiuxian/xiuxian_web/templates/terminal.html:78,89,155,182`）。

Web 终端与 PTY 会话的真实边界是：一次授权只解开一段时间窗口，一个管理员对应一个 PTY 与一个子进程，且这套状态只活在创建它的那个 worker 里。切片不实现 shell 语义、不做命令审计、不做文件传输，也不持久化任何终端内容。

## 命令与别名

无命令：`commands.py` 声明 `COMMANDS = ()`，`manifest.py` 无 `CommandSpec`，聊天里不存在打开终端的命令或别名，入口只有 HTTP。

四个表面全空是事实而不是缺项：`web.py` 声明 `ROUTES = ()`、`commands.py` 声明 `COMMANDS = ()`、`jobs.py` 声明 `JOBS = ()`、`migrations.py` 声明 `MIGRATIONS = ()`，`features/terminal/manifest.py:4-9` 只给 `key/title/owner/test_tag`，按 `bootstrap/registry.py:80-84` 的默认值 `migration_version=None`。路由注册点在旧模块 `xiuxian/xiuxian_web/system.py:187,205,214,228,245`，终端状态活在 PTY、子进程和 Flask session 里而不是任何库表，所以没有任何迁移可声明；`web.py` 的 `LEGACY_ROUTES` 只是给后续适配器搬迁用的委托清单。

权限同样来自旧端点权限表：`xiuxian/xiuxian_web/access.py:135` 的 `terminal_confirm` 是 `TERMINAL_CONFIRM`，`:136-139` 的 `terminal`/`terminal_output`/`terminal_write`/`terminal_pwd` 都是 `TERMINAL`。`TERMINAL` 档在登录之上再叠一层二次确认：`xiuxian/xiuxian_web/core.py:301-304`，页面请求跳确认页、接口请求返回「Web 终端需要重新确认密码」的 `403`。

## Web API

`web.py` 的 `ROUTES` 为空，四条委托都由旧模块 `xiuxian/xiuxian_web/system.py` 注册后转发到 `TerminalApplication`，`LEGACY_ROUTES` 登记的就是这四跳。

- `POST /terminal/confirm` -> `TerminalApplication.authorize`（注册 `system.py:187`，转发 `:200-202`）
- `GET /terminal/output` -> `TerminalApplication.output`（注册 `system.py:214`，`:222-223` 用 `Response(..., mimetype='text/plain')` 流式回传，`TerminalUnavailable` 映射 `400` 于 `:224-225`）
- `POST /terminal/write` -> `TerminalApplication.write`（注册 `system.py:228`，JSON 体 `input` 于 `:236-239`）
- `GET /terminal/pwd` -> `TerminalApplication.cwd`（注册 `system.py:245`，转发 `:252`）

`GET /terminal`（`system.py:205-211`）与确认页的 `GET /terminal/confirm`（`system.py:193-194`）只渲染 `terminal.html` / `terminal_confirm.html`，不调用本切片任何方法，因此这两条渲染路径不进 `LEGACY_ROUTES`，只有写侧那一跳才登记。

权限逐条对应：`access.py:135`（`TERMINAL_CONFIRM`）与 `access.py:136-139`（`TERMINAL`）。`terminal_confirm` 的 POST 还要过 CSRF（`core.py:324-340`）；未登录时这两个 endpoint 都在跳登录页的白名单里（`core.py:292-300`）。`/pages/terminal` 之类的路径不存在，新适配器的 `bootstrap/platform_manifest.py` 里也没有任何 terminal `RouteSpec`。

## 数据模型与迁移

无表、无迁移：`migrations.py` 声明 `MIGRATIONS = ()`、`migration_version=None`，`grep -E "CREATE TABLE|ALTER TABLE|sqlite3|db_backend"` 在本目录（排除 tests）零命中。状态只有两处，都不落盘：Flask session 里的授权时间戳 `terminal_authorized_until`（写入 `features/terminal/application.py:166`，读取 `xiuxian/xiuxian_web/core.py:251-257`），以及进程内的会话字典 `self._sessions`（`application.py:146`，键是 `str(admin_id)`，值为 `TerminalSession(fd, pid, process, closed)`，`application.py:52-59`）。

注入端口在 `repository.py:20-52` 以 Protocol 声明：`PasswordProvider`、`PtySessionFactory`、`OsAdapterPort`（`close/read/write/readlink`）、`SelectPort`；生产实现是 `SubprocessPtyRunner`（`application.py:62-111`）、`os` 与 `select.select`，在 `application.py:137-144` 装配。

生命周期常量由 `features/terminal/schemas.py:18-31` 单点声明：授权 TTL `AUTHORIZATION_TTL_SECONDS = 300`、shell argv `PTY_SHELL_ARGUMENTS = ("/bin/bash", "--login", "-i")`、`PTY_TERMINAL_TYPE = "xterm-256color"`、`PTY_LANGUAGE = "zh_CN.UTF-8"`、`PTY_PROMPT`（即注入的 `PS1`）、读块 `PTY_READ_BYTES = 16 * 1024`、`PTY_SELECT_TIMEOUT_SECONDS = 0.5`、终止标记 `PTY_SESSION_TERMINATED_NOTICE = "\n[Session Terminated]\n"`、回收等待 `PROCESS_REAP_WAIT_SECONDS = 1`、`PROCESS_POLL_WAIT_SECONDS = 0`、`WORKING_DIRECTORY_LINK_TEMPLATE = "/proc/{pid}/cwd"`、回退值 `UNKNOWN_WORKING_DIRECTORY = "~"`。`application.py` 只引用这些名字，不再散写裸字面量。

## 事务与失败回滚

没有数据库事务，失败语义体现在授权与会话生命周期上，整体 fail closed。口令比对用 `hmac.compare_digest`（`application.py:164`），口令未配置或不对时直接返回 `False`，session 一个键都不写（`application.py:161-165`）；通过后只写时间戳 `terminal_authorized_until`（`:166`），口令既不落盘也不进 session。子进程环境里会显式 `env.pop` 掉口令（`application.py:75`），它绝不会变成 shell 数据。

PTY 建立失败各有出口：`os.name == "nt"` 或 `pty` 缺失时抛 `TerminalUnavailable("Web终端功能仅支持 Linux/Unix 环境，Windows 不支持。")`（`application.py:69-70`）；`Popen` 失败关 master 后原样抛出（`:93-97`）；`fcntl` 设非阻塞失败时关 fd、`terminate()` 并 `wait`，再抛出（`:99-110`）。子进程被判死后 `_refresh_locked` → `_close_locked` 会 `terminate`/`wait(1)`/必要时 `kill`、关 fd、`wait(0)` 回收并踢出字典（`application.py:174-215`）。

`output()` 是 generator（`application.py:233-268`）：每轮 `select` 等 0.5 秒，可读则一次读 16 KiB 并按 UTF-8 容错解码；读到 EOF 或子进程已退出时产出 `\n[Session Terminated]\n` 并回收会话（`:255-266`）。`write()` 只接受 `str`，非文本抛 `ValueError("input must be text")`，写失败先关会话再抛 `TerminalUnavailable("终端写入失败")`（`:270-279`）。`cwd()` 读 `/proc/<pid>/cwd`，会话缺失、已死或 `readlink` 失败一律回退 `~`（`:281-290`）。`close_all()` 逐个 `terminate` → `wait(1)` → `kill` → `wait(1)` → 关 fd（`:292-318`）。

会话是 process-local：`_check_owner()` 比对 `os.getpid()` 与 `_owner_pid`（`application.py:143,152-154`），fork 或第二个 worker 复用同一实例时抛 `TerminalUnavailable("Web终端仅支持单进程 worker，不能跨 worker 共享会话。")`，不会伪装成共享会话存储；`get_or_create`、`cwd`、`close_all` 都先过这道检查。

## 定时任务

无任务：`jobs.py` 声明 `JOBS = ()`。没有后台清扫，也没有“授权过期就断会话”的 job：授权过期只在下次请求被拒（`core.py:301-304`），已经建立的 PTY 会话不会因此被杀，回收只发生在子进程退出、fd 读写失败或进程关停。关停钩子留在旧层：`xiuxian/xiuxian_web/system.py:182-184` 用 `@register_legacy_shutdown` 注册 `terminal_application.close_all()`，经 `bootstrap/legacy.py:36` 入队、由 `plugin.py:1338` 的 `run_legacy_shutdown()` 逆序 best-effort 执行；`features/**` 内不得出现 `on_shutdown` 或 `get_driver`，本目录零命中。

## 配置项

本切片只有一个配置来源：进程环境变量 `XIUXIAN_WEB_TERMINAL_PASSWORD`（`schemas.py:18` 的 `TERMINAL_PASSWORD_ENVIRONMENT_VARIABLE`，旧层注入点 `system.py:177-179`，默认 provider `application.py:137`）。它故意不声明成 `ConfigSpec`：配置面板按 `features/plugin_config/schema.py` 的 `CONFIG_EDITABLE_FIELDS` 渲染、导出也只覆盖声明过的键（`features/plugin_config/application.py:48-58`），声明它就会让这个口令出现在面板、导出与配置备份里。

未配置不会阻断启动，而是让授权入口 fail closed：确认页 POST 直接渲染「终端密码尚未配置，请设置 XIUXIAN_WEB_TERMINAL_PASSWORD。」并返回 `503`（`system.py:195-199`），`password_configured()` 为假时 `authorize()` 一律返回 `False`。相邻键都不属本切片：`superusers` → `ADMIN_IDS`（`core.py:415`）决定要不要二次确认，`web_auth_is_enabled()` 为假时确认页直接放行进 `/terminal`（`system.py:189-190`、`core.py:251-253`）；`web_require_csrf`（`core.py:325`）覆盖确认页 POST；`XIUXIAN_WEB_STATUS`、`XIUXIAN_WEB_PORT` 只决定面板是否起监听。既有运维口径见 `docs/web_panel.md:24`。

## 适配器差异

`application.py` 只导入标准库（`hmac`/`os`/`select`/`subprocess`/`threading`/`dataclasses`/`typing`）与 `pty`，不导入 Flask 或 NoneBot；session 以 `MutableMapping` 传入，`os` 与 `select` 以端口注入，所以逻辑与适配器类型无关。Windows 差异有两层：切片侧 `pty` 导入失败即 `TerminalUnavailable`（`application.py:20-23,69-70`），旧 handler 侧再各自拦 `IS_WINDOWS`，分别返回 `400`、`400`、JSON 错误与 `"Windows not supported"`（`system.py:209-210,219-220,233-234,249-250`）。

未登录与失败的响应形状也归旧层：`/terminal/output` 未登录回 `Unauthorized` 401，`/terminal/write` 未登录回 `{"success": false, "error": "Not logged in"}`，`/terminal/pwd` 未登录回 `{"cwd": "/"}`（`system.py:217-218,231-232,247-248`）。注意这里的 `/` 与切片内部的 `~` 回退不是同一个值。新适配器目前完全没有终端路由，切过去等于关掉这个功能，而不是换一套实现。

## 测试与手工验收

`./.venv/bin/python -B -m unittest discover -s tests -q -k terminal` 收集根级 `tests/test_terminal_application.py`（unittest）：口令缺失/错误都不写 session、口令不会出现在 session 内容里、死会话被回收并替换、`output()` 先吐数据再吐 `\n[Session Terminated]\n`、`write`/`cwd` 走 feature 拥有的会话、`close_all` 之后 `cwd` 回退 `~`、fork 后复用直接抛 `TerminalUnavailable`。`./.venv/bin/python -B -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/terminal/tests -q` 收集同目录的 `test_slice_contract.py`（同为 unittest 写法，`unittest discover -s tests` 收不到它）：断言生命周期常量只在 `schemas.py` 一处定义、端口只声明一次、四个表面为空、四条 `LEGACY_ROUTES` 的 target 真实可调用、授权只在 session 写声明的 deadline 键、缺口令 fail closed 且不碰 session、读取块大小与 `select` 超时取 `PTY_READ_BYTES`/`PTY_SELECT_TIMEOUT_SECONDS`、子进程死亡时用 `PTY_SESSION_TERMINATED_NOTICE` 收尾。

相关根级测试还包括 `tests/test_operations_console_slice_manifests.py`（要求本文出现标题、九个小节标题与四条委托的 path、target，并证明本切片不会与适配器双声明路由）与 `tests/test_web_auth.py`：`test_terminal_confirmation_uses_superuser_session`（`tests/test_web_auth.py:249-290`）覆盖未登录跳确认页、口令未配置 `503`、口令错误 `401` 且不写 session、正确口令 302 且写入未来时间戳、把时间戳改成过去后 `/terminal/pwd` 得到 `403`；`test_every_web_endpoint_declares_permission`（`:123`）保证这四条端点的权限声明不会漏。

手工验收需要真实 Linux + PTY 与已配置的口令：在隔离数据目录导出 `XIUXIAN_WEB_TERMINAL_PASSWORD` 并以单 worker 启动面板，先故意输错口令确认 `401`，再正确授权进终端跑 `pwd`、`cd /tmp`、`ls` 与一次 Tab 补全，确认路径轮询跟着变；在会话里执行 `exit`，确认输出里出现 `[Session Terminated]` 且宿主进程侧不再有该 `bash` 子进程；停服时确认 `close_all` 收干净遗留 shell。
