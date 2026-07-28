# Web 修仙管理面板

浏览器运维入口，与 QQ 游戏共用 `data/xiuxian/` 数据。默认监听所有网络接口；是否允许公网访问由防火墙、反向代理和面板安全配置共同决定。

## 访问

| 项目 | 说明 |
|:-----|:-----|
| 开关 | `xiuxian_config.py` → `web_status = True`（默认开） |
| 地址 | `http://服务器地址:5888`（host 复用 NoneBot `HOST`，端口由 `XIUXIAN_WEB_PORT` 环境变量控制，默认 5888） |
| 登录 | 打开 `/login`，填写 `.env` 里 **`SUPERUSERS` 中任一 ID** |
| 认证关闭 | `SUPERUSERS` 为空时面板不要求登录（仅适合本机调试） |

> NoneBot 的 `PORT`（如 8080）是 OneBot / 适配器端口；**管理面板默认 5888**，不要混用。

远程访问时请：

1. 使用 HTTPS 反向代理或限制只允许可信内网访问
2. 使用防火墙限制来源和端口
3. 保持 CSRF、认证与 Host 白名单等安全开关开启

会话密钥：`XIUXIAN_WEB_SECRET_KEY` 环境变量优先，否则配置项，未配置时写入 `data/xiuxian/web_secret_key`。

## QQ 官方机器人扫码绑定

在管理面板 `/config` 的“QQ 官方机器人扫码绑定”区域点击开始。二维码由本机服务生成，使用 QQ 扫码并确认授权后，AppID/Secret 会合并写入当前项目 `.env.dev` 的 `QQ_BOTS`，并启用 `use_websocket=true`。

- 绑定任务仅保存在进程内，10 分钟过期。
- Secret 不会返回到浏览器、写入日志或保存到插件数据目录。
- 已有同 AppID 条目会更新凭据并保留其 `intent` 等配置；新 AppID 新增为 WebSocket 机器人。
- 完成后必须重启 NoneBot，适配器才会建立 QQ Gateway WebSocket 连接。


| 模块 | 路径 | 说明 |
|:-----|:-----|:-----|
| 首页 | `/` | Bot / 玩家 / 资源概览 |
| 数据库 | `/database` | 浏览编辑 SQLite（受写入开关约束） |
| 指令中心 | `/commands` | Web 执行管理类指令 |
| 指令开关 | `/command-registry` 等 | 按模块批量启停命令（若已启用） |
| 活动 | `/activity` | 活动与模板 |
| 发放中心 | `/reward-center` | 奖励发放记录 |
| 配置 | `/config` | 可视化改配置、QQ 官方机器人扫码绑定（写入本地 `.env.dev`，重启后 WebSocket 连接） |
| **定时任务** | `/scheduler` | 查看 / 启停 / 改计划 / 手动运行（见下） |
| 消息 | `/messages` | 会话、发送、撤回 |
| 经济流水 | `/economy_logs` | 灵石等日志 |
| 日志 | `/logs` | 运行日志 |
| 备份 | `/backups` | 本地/云端备份恢复 |
| 更新 | `/update` | GitHub Release 检查与更新（管理员权限） |
| 终端 | `/terminal` | 浏览器终端（管理员权限并需二次确认，高风险） |

关闭面板：`web_status = False` 后重启 NoneBot。

## 定时任务页

路径：`/scheduler`（移动端已做卡片布局）。

### 能做什么

- 查看全部 APScheduler 任务：中文名、下次执行、当前计划  
- 启用 / 禁用  
- **改计划**：点选常用周期（每小时 / 每 2 小时 / 每天 / 每周一…）后点「保存计划」  
- 也可手写一条 cron：`分 时 日 月 周`（例：`0 * * * *` = 每小时）  
- **立即运行**一次并查看结果  

### 名称说明

页面优先显示中文标题，例如：

| 内部 id / 函数 | 显示名 |
|:---------------|:-------|
| `auto_harvest` | 灵田傀儡自动收取 |
| `backup_database_files` | 数据库备份 |
| `materialsupdate_` / `sect_materials_grant` | 发放宗门资材 |
| `limit_all_stamina_` / `recover_user_stamina` | 体力恢复 |

旧版本未写 `id=` 的任务可能以 UUID 出现在列表；只要函数名可识别，仍会显示中文名。**重启 bot** 后会换成稳定 id。

### 计划文案

| 原始 | 显示 |
|:-----|:-----|
| 时=`*/4` 分=`10` | 每 4 小时的第 10 分 |
| 时=`*` 分=`0` | 每小时 |
| 时=`0` 分=`0` | 每天 00:00 |

计划覆盖持久化在数据目录 `scheduler_overrides.json`（运行期文件，勿当源码提交）。

## 安全控制

可在配置文件或 Web 配置页调整：

| 配置 | 默认 | 说明 |
|:-----|:----:|:-----|
| `web_require_csrf` | 开 | 写请求必须携带 CSRF Token |
| `web_allowed_hosts` | 空 | Host 白名单；空表示不限制 |
| `web_session_cookie_secure` | 关 | HTTPS 反向代理场景建议开启 |
| `web_session_lifetime_minutes` | `720` | 登录会话有效期（分钟） |

每个 Flask 端点必须在 `xiuxian_web/access.py` 声明权限类别。数据库写入、消息发送、备份恢复、更新、定时任务和终端分别使用独立权限类别；当前管理员登录后可访问对应类别。终端还需要二次确认，未声明端点一律拒绝。

本机上传接口仅允许本地请求免登录；其它页面和 API 依赖 `SUPERUSERS` 管理员会话。`SUPERUSERS` 为空会关闭面板认证，只适合受控本机调试环境。

更细的数据层与路径约定见 [database_web_governance.md](database_web_governance.md)。
