
```markdown
# 🎉 修仙2.2 魔改版

> ✨ 一款适用于 QQ 群的修仙文字游戏插件

由于大量魔改，已经不再是一个纯粹的修仙机器人，还包含一些非修仙的娱乐功能。  
设定持续征集中，有好的想法欢迎推送~

---

## 📚 详细文档

| 文档 | 说明 |
|:-----|:-----|
| [🧾 物品 ID 速查表](items.md) | 所有物品类型、ID 范围、品阶总览 |
| [🔮 物品系统详解](buff.md) | 功法 / 神通 / 装备 / 丹药 / 药材等完整说明 |
| [🔌 跨适配器兼容层](adapter_compat.md) | OneBot v11 + QQ 适配器统一接口文档 |

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

</details>

#### WebSocket 客户端 URL

```
ws://127.0.0.1:8080/onebot/v11/ws
```

---

## 💿 安装

> ⭐ 新手请优先使用一键安装脚本

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
用法: xiu2 [start|stop|format [log_file]]
  start              - 启动 xiu2（默认，无需参数）
  status             - 查看 xiu2 状态
  stop               - 停止 xiu2
  format [log_file]  - 格式化日志文件（默认: /root/xiu2.log）
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

**2. 安装 NapCat：**

```bash
curl -o napcat.termux.sh https://nclatest.znin.net/NapNeko/NapCat-Installer/main/script/install.termux.sh && bash napcat.termux.sh
```

**3. 进入容器：**

```bash
proot-distro login napcat
```

> ⚠️ 之后每次启动都要先执行 `proot-distro login napcat` 进入容器。

**4. 剩余步骤**同 Linux 手动安装 / 一键安装（不需要再安装 NapCat）。

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

**Termux 先进入容器：**

```bash
proot-distro login napcat
```

**启动 / 进入 / 退出 / 关闭：**

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

## 🎮 使用

| 指令 | 说明 |
|:-----|:-----|
| `修仙帮助` | 查看功能列表 |
| `修仙手册` | 查看管理员指令 |

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
