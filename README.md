# 🎉 修仙2.2 魔改版

> ✨ 一款适用于 QQ 群的修仙文字游戏插件

由于大量魔改，已经不再是一个纯粹的修仙机器人，还包含一些非修仙的娱乐功能。  
设定持续征集中，有好的想法欢迎推送~

---

## 📚 详细文档

| 文档 | 说明 |
|:-----|:-----|
| [🧾 物品 ID 速查表](docs/items.md) | 所有物品类型、ID 范围、品阶总览 |
| [🔮 物品系统详解](docs/buff.md) | 功法 / 神通 / 装备 / 丹药 / 药材等完整说明 |
| [🔌 跨适配器兼容层](docs/adapter_compat.md) | OneBot v11 + QQ 适配器统一接口文档 |
| [🚦 Matcher 路由兼容层](docs/on_compat.md) | 空前缀命令环境下的 matcher 索引与路由说明 |
| **Web 修仙管理面板** | 见下文 [🖥️ Web 修仙管理面板](#-web-修仙管理面板)（默认 `http://IP:5888`，超管 QQ 登录） |

---

## 📌 支持平台

| 平台 | 状态 |
|:-----|:----:|
| 野生机器人（NapCat） | ✅ |
| 官方机器人（Gensokyo / NoneBot QQ） | ✅ |

---

## ⚙️ 配置

<details>
<summary>📝 env 相关配置</summary>

在 `.env.dev` 文件中设置超管与机器人昵称：

```dotenv
LOG_LEVEL=INFO       # 日志等级，INFO 即可
SUPERUSERS = [""]    # 野生 bot 填自己 QQ（非机器人 QQ）；官方 bot 填用户 id
COMMAND_START = [""] # 指令前缀，默认空
NICKNAME = [""]      # 机器人昵称

DEBUG = False
HOST = 127.0.0.1
PORT = 8080          # 反代端口，按需修改

# 数据库使用本地 SQLite 文件，无需额外配置
```

在 `.env.dev` 文件中添加 QQ 官方机器人配置（公域群机器人，测试通过）：

- 自动转发频道消息为群消息
- 自动转发频道私聊消息为私聊

```dotenv
QQ_BOTS='
[
  {
    "id": "xxx",
    "token": "xxx",
    "secret": "xxx",
    "intent": {
      "c2c_group_at_messages": true,
      "direct_message": true
    },
    "use_websocket": true
  }
]
'
```

env 文件基础配置：

```dotenv
ENVIRONMENT=dev
DRIVER=~fastapi+~websockets+~httpx  # 反代 + http 正向调试
```

</details>

<details>
<summary>🧙 修仙2 插件配置</summary>

在 `xiuxian_config.py` 中配置各项选项。

官方 bot 仅测试过 [Gensokyo](https://github.com/Hoshinonyaruko/Gensokyo)，野生机器人推荐使用：
[NapCat](https://github.com/NapNeko/NapCatQQ) /
[LLOneBot](https://github.com/LLOneBot/LLOneBot) /
[Lagrange](https://github.com/LagrangeDev/Lagrange.Core)

#### 常用配置项

```python
self.merge_forward_send = False   # 消息转发类型：True=合并转发，False=长图发送，建议长图
self.img_compression_limit = 80   # 图片压缩率：0=不压缩，最高 100
self.img_type = "webp"            # 图片类型：webp 或 jpeg（图片不显示请用 jpeg）
self.img_send_type = "io"         # 图片发送类型：默认 io，官方 bot 建议 base64
self.put_bot = []                 # 接收消息 QQ（主 QQ），不配置默认第一个链接的 QQ
self.main_bo = []                 # 负责发送消息的 QQ（使用 range_bot 时需填写）
self.shield_group = []            # 屏蔽的群聊（填群号）
self.layout_bot_dict = {}         # QQ 负责的群聊映射 {群号: bot}
```

#### layout_bot_dict 示例

```python
self.layout_bot_dict = {
    "111": "xxx",            # QQ xxx 单独负责 111 群
    "222": ["yyy", "zzz"]    # QQ yyy 和 zzz 共同负责 222 群
}
# 值为字符串 → 一对一；值为列表 → 多对一
```

#### 参数说明

| 参数 | 默认值 | 说明 |
|:-----|:------:|:-----|
| `put_bot` | `[]` | 接收消息 QQ（主 QQ），不配置则默认第一个链接的 QQ |
| `main_bo` | `[]` | 负责发送消息的 QQ，使用 `range_bot` 时需填写 |
| `shield_group` | `[]` | 屏蔽的群聊，参数为群号 |
| `layout_bot_dict` | `{}` | QQ 负责的群聊映射，格式 `{群号: bot}`，bot 为字符串或列表 |
| `web_status` | `True` | 是否启动 **Web 修仙管理面板**（Flask，与 NoneBot 的 `HOST`/`PORT` 独立） |
| `web_host` | `0.0.0.0` | 管理面板监听地址 |
| `web_port` | `5888` | 管理面板端口，默认 `http://<服务器IP>:5888` |
| `custom_proxy_enabled` | `False` | 是否启用自定义代理（Bangumi 等境外 API） |
| `custom_proxy` | `""` | 代理地址 |

也可在 **Web 面板 → 配置管理** 中在线修改上述项（保存后需重启 NoneBot 生效）。

</details>

#### WebSocket 客户端 URL

```
ws://127.0.0.1:8080/onebot/v11/ws
```

---

## 💿 安装

> ⭐ 新手请优先使用一键安装脚本
>
> 当前版本使用本地 SQLite 数据库，无需单独安装数据库服务。

<details>
<summary>🐧 Linux 一键安装</summary>

**安装：**

```bash
# 默认目录
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install.sh | bash

# 自定义目录（如 /root/xiuxian）
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install.sh | bash -s -- install /root/xiuxian
```

**更新：**

```bash
# 默认目录
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install.sh | bash -s -- update

# 自定义目录
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install.sh | bash -s -- update /root/xiuxian
```

**xiu2 命令：**

```
用法: xiu2 [start|stop|status|update-deps|format [log_file]]
  start              - 启动 xiu2（默认，无需参数）
  status             - 查看 xiu2 状态
  stop               - 停止 xiu2
  update-deps        - 更新 Python 依赖
  format [log_file]  - 格式化日志文件（默认: /root/xiu2.log）
```

</details>

<details>
<summary>📱 Termux 一键安装</summary>

`install_termux.sh` 面向安卓 Termux 原生环境，不使用 `/root`、`/bin`、`/etc`，默认安装到 `$HOME/xiu2`，虚拟环境为 `$HOME/myenv`，管理命令写入 `$PREFIX/bin/xiu2`。

**安装：**

```bash
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install_termux.sh | bash
```

**自定义目录：**

```bash
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install_termux.sh | bash -s -- install "$HOME/xiuxian"
```

**更新：**

```bash
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install_termux.sh | bash -s -- update
```

**单独更新依赖：**

```bash
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install_termux.sh | bash -s -- update-deps
```

**xiu2 命令：**

```
用法: xiu2 [start|stop|status|update|update-deps|format [log_file]]
  start              - 后台启动 xiu2（默认，无需参数）
  status             - 进入 screen 查看运行日志
  stop               - 停止 xiu2
  update             - 更新项目文件
  update-deps        - 更新 Python 依赖
  format [log_file]  - 格式化日志文件
```

安装完成后建议执行一次：

```bash
termux-wake-lock
```

如果 `termux-wake-lock` 执行失败，可安装 Termux:API 应用后重试；不使用也不影响安装，只影响后台保活。

NapCat 如果也运行在原生 Termux，请在 NapCat WebUI 中把 WebSocket 客户端 URL 填为：

```
ws://127.0.0.1:8080/onebot/v11/ws
```

</details>

<details>
<summary>🪟 Windows 一键安装</summary>

[📥 点我下载 install.bat](https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/download/v0.1/install.bat)

下载后双击运行即可。

</details>

<details>
<summary>🐧 Linux 手动安装（Debian）</summary>

**1. 安装依赖：**

```bash
apt update && apt upgrade -y && \
apt install screen curl wget git python3 python3-pip python3-venv -y
```

**2. 安装 NapCat：**

```bash
curl -o napcat.sh https://nclatest.znin.net/NapNeko/NapCat-Installer/main/script/install.sh && sudo bash napcat.sh
```

**3. 安装 nb-cli：**

```bash
cd ~
python3 -m venv myenv
source ~/myenv/bin/activate
pip install nb-cli==1.5.0
```

**4. 克隆项目：**

```bash
git clone --depth=1 -b main https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv.git
```

**5. 使用 nb 创建项目：**

```bash
source ~/myenv/bin/activate
nb
```

按提示选择：

| 步骤 | 选择 |
|:-----|:-----|
| 项目类型 | `Create a NoneBot project` |
| 模板 | `simple` |
| 项目名 | `xiu2` |
| 适配器 | `OneBot V11` + `QQ` |
| 驱动器 | `FastAPI` + `HTTPX` + `websockets` + `AIOHTTP` |
| 插件位置 | `In a "src" folder` |
| 安装依赖 | `Y` |
| 创建虚拟环境 | `n` |
| 默认插件 | `echo` |

**6. 移动项目文件：**

```bash
mv ~/nonebot_plugin_xiuxian_2_pmv/nonebot_plugin_xiuxian_2 ~/xiu2/src/plugins
mv ~/nonebot_plugin_xiuxian_2_pmv/data ~/xiu2
mv ~/nonebot_plugin_xiuxian_2_pmv/requirements.txt ~/xiu2
```

**7. 安装修仙2依赖：**

```bash
cd ~/xiu2
pip install -r requirements.txt
```

**8. 写入配置：**

```bash
cat > ~/xiu2/.env.dev << 'EOF'
LOG_LEVEL=INFO
SUPERUSERS = [""]
COMMAND_START = [""]
NICKNAME = [""]
DEBUG = False
HOST = 127.0.0.1
PORT = 8080
EOF
```

**9. 启动：**

```bash
source ~/myenv/bin/activate
cd ~/xiu2
nb run --reload
```

📺 [B站安装教程](https://m.bilibili.com/video/BV1ZuesekEYy)

</details>

<details>
<summary>🪟 Windows 手动安装</summary>

**1. 安装 Python：**

[下载 Python 3.11.0](https://www.python.org/ftp/python/3.11.0/python-3.11.0-amd64.exe)

**2. 安装 NapCat：**

[NapCat 安装指南](https://napneko.github.io/guide/napcat)

**3. 安装 nb-cli：**

在 C/D 盘根目录新建文件夹并打开 cmd：

```cmd
mkdir C:\nb
cd C:\nb
python -m venv myenv
call myenv\Scripts\activate
pip install nb-cli==1.5.0
```

**4. 下载项目：**

[下载最新 project.tar.gz](https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/releases/latest)

**5. 使用 nb 创建项目：**

```cmd
nb
```

按提示选择（同 Linux 手动安装第 5 步）。

**6. 解压并移动文件：**

| 文件 | 目标位置 |
|:-----|:---------|
| `nonebot_plugin_xiuxian_2` | `xiu2\src\plugins` |
| `data` | `xiu2` |
| `requirements.txt` | `xiu2` |

**7. 安装修仙2依赖：**

```cmd
cd xiu2
pip install -r requirements.txt
```

> 如果失败，可以删除 `psutil` 后重新安装。

**8. 写入配置：**

```cmd
(
echo LOG_LEVEL=INFO
echo SUPERUSERS = [""]
echo COMMAND_START = [""]
echo NICKNAME = [""]
echo DEBUG = False
echo HOST = 127.0.0.1
echo PORT = 8080
) > xiu2\.env.dev
```

**9. 启动：**

在 `C:\nb` 下新建 `.bat` 文件：

```bat
call myenv\Scripts\activate
cd xiu2
nb run --reload
```

双击运行即可。

📺 [B站安装教程](https://m.bilibili.com/video/BV1ZuesekEYy)

</details>

<details>
<summary>📱 安卓安装（Termux）</summary>

**1. 安装 Termux：**

- [ZeroTermux](https://github.com/hanxinhao000/ZeroTermux/releases)
- [Termux](https://github.com/termux/termux-app/releases)

### 原生 Termux 一键安装

如果你不使用 proot 容器，直接在 Termux 原生环境安装修仙2，执行：

```bash
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install_termux.sh | bash
```

安装完成后使用：

```bash
xiu2 start
xiu2 status
xiu2 stop
```

### proot 容器安装

如果你使用 NapCat Termux 安装脚本创建的容器，先安装 NapCat：

```bash
curl -o napcat.termux.sh https://nclatest.znin.net/NapNeko/NapCat-Installer/main/script/install.termux.sh && bash napcat.termux.sh
```

**3. 进入容器：**

```bash
proot-distro login napcat
```

> ⚠️ 之后每次启动都要先执行 `proot-distro login napcat` 进入容器。

进入容器后使用 Linux 一键安装 / 手动安装步骤，不要使用 `install_termux.sh`。`install_termux.sh` 只用于 Termux 原生环境。

📺 [B站安装教程](https://m.bilibili.com/video/BV1ZuesekEYy)

</details>

---

## 📡 连接 QQ（Linux）

<details>
<summary>🔗 NapCat</summary>

**启动 QQ：**

```bash
screen -dmS napcat bash -c 'xvfb-run -a /root/Napcat/opt/QQ/qq --no-sandbox'
```

**进入 / 退出 / 关闭：**

```bash
screen -r napcat    # 进入
# Ctrl + A + D      # 退出
screen -S napcat -X quit  # 关闭
```

**NapCat WebUI：**

```
http://IP:6099
```

查看默认 Token：

```bash
cat /root/Napcat/opt/QQ/resources/app/app_launcher/napcat/config/webui.json
```

**配置 WebSocket 连接：**

1. 进入 WebUI → 登录 QQ
2. 网络配置 → 新建 → **WebSocket 客户端**
3. 勾选【启用】，名称随意
4. URL 填写：

```
ws://127.0.0.1:8080/onebot/v11/ws
```

5. 保存

</details>

<details>
<summary>🎮 修仙2</summary>

**原生 Termux：**

```bash
xiu2 start   # 后台启动
xiu2 status  # 进入 screen 查看日志
xiu2 stop    # 停止
```

**proot 容器先进入容器：**

```bash
proot-distro login napcat
```

容器内启动 / 进入 / 退出 / 关闭：

```bash
# 后台启动
screen -dmS xiu2 bash -c 'source ~/myenv/bin/activate && cd ~/xiu2 && nb run'

# 进入
screen -r xiu2

# 退出：Ctrl + A + D

# 关闭
screen -S xiu2 -X quit
```

</details>

---

## 🖥️ Web 修仙管理面板

插件内置 **修仙管理面板**（后台 Web），与 QQ 游戏共用同一套 `data/xiuxian/` 数据，适合超管在浏览器里运维，无需记一堆管理指令。

### 访问与登录

| 项目 | 说明 |
|:-----|:-----|
| 开关 | `xiuxian_config.py` 中 `web_status = True`（默认开启） |
| 地址 | `http://<机器IP>:5888`（`web_host` / `web_port`，默认 `0.0.0.0:5888`） |
| 登录 | 打开 `/login`，填写 **`.env` 里 `SUPERUSERS` 中的 QQ 号**（与 NoneBot 超管一致） |
| 日志 | 启动成功会输出：`修仙管理面板已启动：<host>:<port>` |

> NoneBot 反代端口（如 `.env` 的 `PORT=8080`）用于 OneBot WebSocket；**管理面板端口默认 5888**，两者不要混用。本机调试可访问 `http://127.0.0.1:5888`；外网访问请自行做好防火墙与 HTTPS 反代。

### 功能一览

| 模块 | 路径 | 能力简述 |
|:-----|:-----|:---------|
| 首页 | `/` | 在线 Bot、玩家/宗门统计、CPU/内存等监控 |
| 数据库 | `/database` | 浏览/编辑 SQLite 表、批量改数 |
| 指令中心 | `/commands` | 在 Web 端执行修仙管理类指令 |
| 活动管理 | `/activity` | 活动配置、模板、玩法数据调整 |
| 发放中心 | `/reward-center` | 奖励发放记录维护 |
| 配置管理 | `/config` | 可视化改 `xiuxian_config`（含 **网络代理**） |
| 消息面板 | `/messages` | 会话列表、发消息/群发、Markdown 预览、撤回等 |
| 经济流水 | `/economy_logs` | 灵石等经济日志查询与导出 |
| 日志查看 | `/logs` | 运行日志、按用户查消息 |
| 备份管理 | `/backups` | 本地/云端备份与恢复（含配置、数据库） |
| 检测更新 | `/update` | 检查 GitHub Release、一键更新（带备份） |
| Web 终端 | `/terminal` | 浏览器内简易终端（高权限，慎用） |

关闭面板：将 `web_status` 设为 `False` 后重启 NoneBot。

---

## 🗄️ SQLite 数据库

当前版本使用本地 SQLite 数据库文件，默认位于 `data/xiuxian/`：

- `xiuxian.db`
- `xiuxian_impart.db`
- `player.db`
- `trade.db`
- `message.db` 会在运行期自动创建，用于消息记录。

**Web 修仙管理面板 → 备份管理** 会直接打包上述库；恢复时可按库选择覆盖。详见上一节。

---

## 📦 启动依赖自检

首次加载插件时，会按项目根目录 `requirements.txt` 检测缺失的 Python 包，并对 **当前运行 NoneBot 的解释器** 执行 `python -m pip install`（与 `nb run` / 虚拟环境一致）。

| 环境变量 | 说明 |
|:---------|:-----|
| `XIUXIAN_SKIP_AUTO_PIP=1` | 关闭启动时自动 pip |
| `XIUXIAN_PIP_INDEX=<url>` | 自定义 PyPI 源（默认清华镜像） |

**Termux 原生环境**会与一键脚本一致，跳过已由 `pkg` 提供的 `numpy` / `Pillow` / `psutil` 等。若自动安装失败，请手动：

```bash
source ~/myenv/bin/activate   # 或你的 venv
python -m pip install -r requirements.txt
```

Linux 一键安装里的 `xiu2 update-deps`、Termux 的 `xiu2 update-deps` / `install_termux.sh update-deps` 用于 **整包更新依赖**；日常小版本升级通常靠启动自检即可。

---

## 🎮 使用

| 指令 | 说明 |
|:-----|:-----|
| `修仙帮助` | 查看功能列表 |
| `修仙手册` | 查看管理员指令 |
| `娱乐帮助` | 娱乐模块总览（别名：`娱乐菜单` / `娱乐功能`）；番剧、点歌、NewAPI、链接解析、小游戏等见该帮助，支持 `娱乐帮助 页码` 翻页 |
| `小游戏帮助` | 五子棋 / 扫雷 / 十点半 / 猜数字 / 猜数谜等 |

### 娱乐 · 趣味接口

| 指令 | 别名示例 | 说明 |
|:-----|:---------|:-----|
| `答案之书 [问题]` | `答案书`、`问答案之书` | 随机给出一句答案 |
| `摸鱼日报` | `摸鱼日历`、`今日摸鱼` | 发送当天摸鱼日报图，失败时降级为文本 |
| `随机二次元` | `随机猫娘`、`随机老婆`、`随机狐娘`、`随机老公` | 随机 SFW 二次元图片 |
| `抱抱` / `贴贴` / `摸摸` | `拍头`、`亲亲`、`戳戳`、`击掌`、`挥手` | 随机动漫互动 GIF |

### 娱乐 · 番剧（Bangumi）

属 **娱乐模块** 子功能，指令触发、无定时推送。数据来自 Bangumi 放送日历；访问不畅时在 `xiuxian_config` 或 **Web 面板 → 配置管理 → 网络代理** 开启自定义代理（需 `PySocks`）。

| 指令 | 别名示例 | 说明 |
|:-----|:---------|:-----|
| `今日番剧` | `每日番剧`、`番剧日历` | 当日放送列表 |
| `番剧周表` | `每周番剧`、`番剧总表` | 一周番剧表（可 `番剧周表 页码` 翻页） |

### 娱乐 · NewAPI

按 QQ 隔离绑定多个 NewAPI 站点账号（Token 或 Cookie），支持签到与用户信息查询。

| 指令 | 别名示例 | 说明 |
|:-----|:---------|:-----|
| `newapi帮助` | `newapi`、`NewAPI帮助` | 绑定与签到说明 |
| `newapi绑定` | — | `newapi绑定 站点用户ID#令牌#接口`；Cookie：`newapi绑定 cookie 站点用户ID#session#接口`（字段用 `#` 分隔） |
| `newapi查看` | `newapi列表`、`newapi绑定列表` | 本 QQ 已绑定账号（序号、是否自动签到） |
| `newapi签到` | — | 默认全部账号；可 `newapi签到 1` / `1,3` / `2-4`（记入历史，最多 3 条） |
| `newapi签到历史` | `newapi签到记录` | 最近签到记录 |
| `newapi自动签到` | — | `newapi自动签到 序号` 切换开/关；开启后每日 **12:30** 自动签到 |
| `newapi信息` | — | 拉取站点用户信息，序号规则同签到 |
| `newapi删除` | `newapi解绑` | 须写序号（如 `1`、`1,3`）或 `全部` |

绑定数据：`xiuxian_entertainment/mod/data/newapi_bindings/<QQ>.json`；签到历史：`.../newapi_checkin_history/<QQ>.json`。

🌐 体验群：[144795954](https://qun.qq.com/universal-share/share?ac=1&authKey=JcaNbcnyFbgcjfffkakYujFwpYFJewe2mSFUtSNWi1mA6qap%2FHBQNsCl0D9olm4I&busi_data=eyJncm91cENvZGUiOiIxNDQ3OTU5NTQiLCJ0b2tlbiI6ImZKYXpKOVM3Z0pwek80ZlUzLzhzbWN1Y1daY0JIQy9BYXZFUlZGd1lGREJQUXJXWERLNlJCcFNjSjVGc3JZVWsiLCJ1aW4iOiIyNjUwMTE1MzE3In0%3D&data=5w52a2CkyEIX_t_INqS29fA4Sxl8eozGazmL-EIUo6ehG7ESdNgxtDnVmgXoLlLfaVeZ2SbPMW-1SJ4I9o7IeQ&svctype=4&tempid=h5_group_info)

> ⚠️ 使用官方机器人请记得修改对应配置。

---

## 🙏 特别感谢

| 项目 | 说明 |
|:-----|:-----|
| [NoneBot2](https://github.com/nonebot/nonebot2) | 本插件基于的开发框架，NB 天下第一可爱 |
| [nonebot_plugin_xiuxian](https://github.com/s52047qwas/nonebot_plugin_xiuxian) | 原版修仙 |
| [nonebot_plugin_xiuxian_2](https://github.com/QingMuCat/nonebot_plugin_xiuxian_2) | 原版修仙2 |
| [nonebot_plugin_xiuxian_2_pmv](https://github.com/MyXiaoNan/nonebot_plugin_xiuxian_2_pmv) | 修仙2魔改版 |

---

## 📄 许可证

本项目基于 [MIT](https://choosealicense.com/licenses/mit/) 许可证开源，无 CC 限制。
