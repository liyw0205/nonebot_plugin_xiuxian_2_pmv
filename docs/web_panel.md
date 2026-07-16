# Web 修仙管理面板

浏览器运维入口，与 QQ 游戏共用 `data/xiuxian/` 数据。默认仅本机访问。

## 访问

| 项目 | 说明 |
|:-----|:-----|
| 开关 | `xiuxian_config.py` → `web_status = True`（默认开） |
| 地址 | `http://127.0.0.1:5888`（`web_host` / `web_port` 可改） |
| 登录 | 打开 `/login`，填写 `.env` 里 **`SUPERUSERS` 中任一 ID** |
| 认证关闭 | `SUPERUSERS` 为空时面板不要求登录（仅适合本机调试） |

> NoneBot 的 `PORT`（如 8080）是 OneBot / 适配器端口；**管理面板默认 5888**，不要混用。

远程访问时请：

1. 将 `web_host` 显式改为 `0.0.0.0` 或反代到面板端口  
2. 使用 HTTPS 与防火墙限制来源  
3. 保持 CSRF 等安全开关开启  

会话密钥：`XIUXIAN_WEB_SECRET_KEY` 环境变量优先，否则配置项，未配置时写入 `data/xiuxian/web_secret_key`。

## 功能一览

| 模块 | 路径 | 说明 |
|:-----|:-----|:-----|
| 首页 | `/` | Bot / 玩家 / 资源概览 |
| 数据库 | `/database` | 浏览编辑 SQLite（受写入开关约束） |
| 指令中心 | `/commands` | Web 执行管理类指令 |
| 指令开关 | `/command-registry` 等 | 按模块批量启停命令（若已启用） |
| 活动 | `/activity` | 活动与模板 |
| 发放中心 | `/reward-center` | 奖励发放记录 |
| 配置 | `/config` | 可视化改配置（含网络代理） |
| **定时任务** | `/scheduler` | 查看 / 启停 / 改计划 / 手动运行（见下） |
| 消息 | `/messages` | 会话、发送、撤回 |
| 经济流水 | `/economy_logs` | 灵石等日志 |
| 日志 | `/logs` | 运行日志 |
| 备份 | `/backups` | 本地/云端备份恢复 |
| 更新 | `/update` | GitHub Release 更新（默认可关） |
| 终端 | `/terminal` | 浏览器终端（默认可关，高风险） |

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

## 安全开关（摘要）

可在配置文件或 Web 配置页调整（默认偏安全）：

| 配置 | 默认 | 说明 |
|:-----|:----:|:-----|
| `web_require_csrf` | 开 | 写请求 CSRF |
| `web_allowed_hosts` | 空 | Host 白名单 |
| `web_session_cookie_secure` | 关 | HTTPS 时建议开 |
| `web_enable_terminal` | 关 | 终端 |
| `web_enable_update` | 关 | 在线更新 |
| `web_enable_database_write` | 视配置 | 库表/指令/活动写入 |
| `web_enable_backup_restore` | 视配置 | 备份恢复 |
| `web_enable_message_send` | 视配置 | 主动发消息 |

更细的数据层与路径约定见 [database_web_governance.md](database_web_governance.md)。
