# QQ 频道图片上传

## 用户流程

管理员在旧 Web 控制台选择本地图片上传到 QQ 频道。旧层读上传部件的字节，本切片先从当前在线 bot 里挑出 QQ 适配器的实例，再把图片交给注入的上传端口，拿回一个可直接引用的 URL。整个流程一次调用完成，没有队列、没有中间态，失败只表现为“没拿到 URL”。

## 命令与别名

无命令，`commands.py` 声明 `COMMANDS = ()`，本切片不注册 matcher，聊天侧也不存在对应命令或别名。唯一的触发面是 Web 端点，权限在 `xiuxian/xiuxian_web/access.py:140` 声明为 `WebPermission.LOCAL_UPLOAD`，并且只有回环地址请求被放行（`xiuxian/xiuxian_web/core.py:231-233` 的 `_is_local_request` 只认 `127.0.0.1`/`::1`/`localhost`，转发头不参与判定；`system.py:259-261` 在 handler 内再判一次）。

## Web API

`web.py` 的 `ROUTES` 为空，新适配器没有任何上传表面（`bootstrap/platform_manifest.py` 的 `RouteSpec` 清单与 `adapters/web/blueprints/` 都无上传项）。当前唯一路径由旧模块注册：

- `POST /upload_image` → `QqImageUploadApplication.upload_image`（注册点 `xiuxian/xiuxian_web/system.py:254`；handler 在 `:273` 先调 `QqImageUploadApplication.select_qq_bot`，再在 `:279-284` 调 `upload_image`，两个方法都属于本切片，`LEGACY_ROUTES` 记录对外那一跳）

## 数据模型与迁移

无数据表、无迁移，`migrations.py` 声明 `MIGRATIONS = ()`；`grep -E "CREATE TABLE|ALTER TABLE|sqlite3|db_backend|migration"` 在本目录（排除 tests）零命中，整条链路也不落文件——md5 摘要与回源 URL 都在 `xiuxian/adapter_compat.py:963-964` 内存生成。契约只有两个值，都在 `schemas.py`：判定 bot 可用的 `QQ_ADAPTER_NAME`（旧实现里是内联的 `"QQ"`）与调用 QQ 接口时的文件标识模式 `UPLOAD_FILE_MODE`（旧实现里是内联的 `"md5"`），`application.py` 只导入不再自定义。

## 事务与失败回滚

`application.py` 是无状态透传：`select_qq_bot` 是纯查找，找不到返回 `None`；`upload_image` 只把 `bot`、`str(channel_id)`、`image`、`mode` 交给端口，端口返回 `None` 或抛错都由旧层翻成失败响应。本切片既不重试也不补偿，因此不存在半成品状态。已知风险与重构无关但必须记录：`system.py:269` 用 `file.read()` 无界读入，本切片没有声明任何大小上限，收紧点应在旧层或端口实现。

## 定时任务

无任务，`jobs.py` 声明 `JOBS = ()`，调度清单 `compatibility/legacy_manifest.py` 与 `xiuxian/xiuxian_scheduler` 里都没有上传相关 job。

## 配置项

本切片不读配置键、不读环境变量，也不声明 `ConfigSpec`。真正影响行为的是端口实现里的默认审核超时 `audit_timeout: float = 30.0`（`xiuxian/adapter_compat.py:927`）与 md5 URL 模板（`:964`），两者都不归本 owner；鉴权依赖 `superusers`（`xiuxian/xiuxian_web/core.py:415`）与 `XIUXIAN_WEB_PORT`（`:418`）。

## 适配器差异

`application.py` 不导入 NoneBot：上传能力以 `repository.py` 的 `UploadImageAndResolve` 协议注入（`xiuxian/adapter_compat.py` 的 `MessageSegment.upload_image_and_get_url` 是其实现），测试可以直接塞一个协程替换。适配器差异体现在两处事实：只有 QQ 适配器的 bot 会被选中，其他适配器（含 OneBot V11）一律跳过；`xiuxian/xiuxian_web/core.py:515` 与 `xiuxian/qq_compat/bot_selector.py:23` 各自还有一份 bot 选择逻辑，后者大小写不敏感，语义与本切片并不相同，迁移时不能想当然合并。

## 测试与手工验收

`python -m unittest discover -s nonebot_plugin_xiuxian_2/features/qq_image_upload/tests -t . -q` 覆盖 QQ bot 选择、端口参数（`mode` 与 `channel_id` 的强制字符串化）与切片契约（常量单一来源、空表面声明、端口标注）；顶层 `tests/test_web_upload_image_route.py` 覆盖旧路由在回环与非回环下的行为。手工验收：在只挂 OneBot V11 的实例上传一次应报“未找到 QQ Bot”，挂上 QQ 适配器后同一请求应返回可访问 URL，且不落任何本地文件。
