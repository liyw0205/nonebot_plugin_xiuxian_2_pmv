# Web 消息发送

## 用户流程

管理员在旧控制台的“消息”页发一条消息：选适配器与场景（群/私聊/频道群/频道私聊），选普通或 markdown 模式，可带文本、媒体 URL、上传文件或直接选一张贴纸，并可引用/回复某条历史消息，再决定是否主动发送（`active_send`）。请求进入本切片后先做参数收敛与合法性判定，再依次解析 bot、解析引用目标、解析贴纸路径、必要时落盘上传文件，最后把真正的投递交给传输端口。本切片拥有的是 Web 发送策略，适配器调用与历史消息读写都是外部 owner。

## 命令与别名

无命令，`commands.py` 声明 `COMMANDS = ()`，本切片不注册 matcher；全仓 `rg "on_command|on_message|on_matcher|on_regex|on_startswith"` 在本目录零命中。参数层有两个事实上的别名：贴纸令牌同时接受 `sticker` 与 `sticker_token`，`active_send` 的真值拼写固定为 `ACTIVE_SEND_TRUE_VALUES`（`1/true/yes/on`，大小写不敏感、去空白）。

## Web API

`web.py` 的 `ROUTES` 为空，本 owner 只有旧模块注册的这一条，权限 `xiuxian/xiuxian_web/access.py:106` 的 `WebPermission.MESSAGE`，POST 需 CSRF：

- `POST /api/messages/send` → `WebMessageSendApplication.send`（注册点 `xiuxian/xiuxian_web/messages.py:487`，JSON 与 multipart 在 `:494-499` 分流，`:501-503` 经 `_message_send_application().send` 进入本切片，工厂在 `:69-82` 注入 11 个端口）

同文件的兄弟 `/api/messages/*` 不归本 owner：历史与撤回走 `logs` 切片，群备注/会话置顶/配置走 `admin` 切片，`/api/messages/broadcast*` 仍走 legacy `xiuxian/broadcast_manager`。新适配器侧 `GET /api/v1/messages` 与 `/messages`（`adapters/web/blueprints/messages.py`）目前只返回空列表和模板，没有委托任何 feature 方法，不能当作本切片的替身表面。

## 数据模型与迁移

无数据表、无迁移，`migrations.py` 声明 `MIGRATIONS = ()`；`grep -E "CREATE TABLE|ALTER TABLE|sqlite3|db_backend|migration"` 在本目录（排除 tests）零命中。`messages` 表确实存在，但 DDL 与读写都在别处：建表 `xiuxian/xiuxian_utils/message_db.py:285`、补列 `:321`、写入 `:610`（入口 `xiuxian/adapter_message_records.py`）、读取 `features/logs/message_reply_repository.py`，库路径 owner 是 `paths.py:41`。契约都在 `schemas.py`：允许的媒体类型 `ALLOWED_MEDIA_TYPES`、场景 `SCENES`、发送模式 `SEND_MODES` 与 `DEFAULT_SEND_MODE`、主动发送真值 `ACTIVE_SEND_TRUE_VALUES`、贴纸强制的媒体类型 `STICKER_MEDIA_TYPE`，以及响应信封 `MessageSendResult`（旧控制台仍在解析它的 dict 形状）。

## 事务与失败回滚

判定失败一律直接返回 `MessageSendResult.failure`，不带部分状态：非法 `media_type`、缺 `adapter`、非法 `scene`、缺 `target_id`、内容与媒体全空都会在这里被拒。贴纸分支若解析不到本地文件直接拒绝，并把 `media_type` 强制成 `STICKER_MEDIA_TYPE`、清空文本、退回普通模式，避免贴纸与文本混排产生歧义。引用解析只在“给了 `quote_message_id`、不是 `REFIDX` 前缀、且没有显式 `reply_message_id`/`quote_reference_id`”时才接管，markdown 模式会主动清掉引用相关字段。本切片没有自己的补偿逻辑：投递失败由传输端口决定，OB11 markdown 分支在直接调用 API 成功后才写发送记录，因此记录与实际投递同生同死。

## 定时任务

无任务，`jobs.py` 声明 `JOBS = ()`。名字相近的 `reset_message_rate_limits` 归 scheduler 的 legacy 清单（`compatibility/legacy_manifest.py:46` → `compatibility/legacy_jobs.py:36` → `xiuxian/xiuxian_utils/lay_out.py` 的限速重置），不调用本切片。

## 配置项

本切片不读配置键、不读环境变量、不声明 `ConfigSpec`。与发送相关的键都由端口实现方持有：上传落盘目录与配额在 `xiuxian/xiuxian_web/core.py:429-431`（`WEB_UPLOAD_CACHE`、上限 128 个、1 小时），消息库存量键 `message_db_max_size_mb`、`message_group_keep_days`、`message_private_keep_days` 在 `xiuxian/xiuxian_utils/message_db.py:168-174` 且写入口属 `admin` 切片，鉴权键 `superusers` 与端口变量 `XIUXIAN_WEB_PORT` 在 `core.py:415`、`:418`。

## 适配器差异

适配器差异集中在三处，全部经端口隔离：OneBot V11 才走 markdown 直发分支并写 `xiuxian/adapter_message_records.py` 的发送记录；QQ 侧的引用需要把 `REFIDX` 令牌换回真实消息 id，失败时降级为普通发送；主动发送在 QQ 上依赖 `send_group_forward_msg`/`send_private_forward_msg` 的聊天记录节点。切片不导入 Flask，`send_application.py` 只依赖注入端口与 `xiuxian/messaging` 的请求模型。实现文件仍是 `send_application.py`，`application.py` 只做门面再导出并有身份断言测试：Phase 2 冻结账本把 `POST /api/messages/send` 的证据与调用图锚在 `send_application.py` 上，账本条目重新对齐之前不能移动实现，否则门禁的源码绑定会整片失效。

## 测试与手工验收

`python -B -m pytest -p no:cacheprovider nonebot_plugin_xiuxian_2/features/messages/tests -q` 覆盖参数校验、引用解析、贴纸分支、markdown 清理、上传文件与两种适配器的投递路径（8 例，全部使用假端口）；同目录 `tests/test_slice_contract.py` 是 unittest，断言契约单一来源、四个端口的标注、门面与冻结实现是同一对象、空表面与单条 `LEGACY_ROUTES`。本目录既有测试是 pytest 函数式写法，`unittest discover` 收集不到，两条命令都要跑。顶层 `tests/test_web_message_send_routes.py` 与 `tests/test_source_quality.py`、`tests/test_phase2_legacy_path_gate.py` 覆盖旧 handler 必须调用 `_message_send_application().send` 这一绑定。手工验收：在只挂一个适配器的实例分别发送纯文本、贴纸与带引用的消息，确认失败分支只返回错误信封且不落上传文件，成功分支在历史列表里出现记录。
