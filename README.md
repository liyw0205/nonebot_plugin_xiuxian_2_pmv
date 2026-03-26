# 🎉 修仙2.2魔改版

### ✨ QQ群聊修仙文字游戏✨

# 📖 介绍

一款适用于QQ群的修仙插件,设定征集中，有好的想法可以推送给我哦~~~

# 支持
[✅] 野生机器人（napcat）

[✅] 官方机器人（gsk/nonebot qq）
 
# 💿 配置

<details>
<summary>(env相关)</summary>

- 在.env.dev文件中设置超管与机器人昵称

```
LOG_LEVEL=INFO # 日志等级INFO就行

SUPERUSERS = [""] # 野生bot填自己QQ号(不是机器人的QQ)，官方bot下的用户id自行获取，填的不对的话会出现指令无响应的情况

COMMAND_START = [""] # 指令前缀，默认空
NICKNAME = [""] # 机器人昵称

DEBUG = False
HOST = 127.0.0.1
PORT = 8080 # 反代的8080端口，有需要自己改
```

- 在.env.dev文件中添加QQ官方机器人来启动


私域频道机器人：未测试

公域群机器人示例：测试通过

- 自动转发频道为群消息
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

- env文件配置

```
ENVIRONMENT=dev
DRIVER=~fastapi+~websockets+~httpx # 这里用的是反代+http正向调试
```

 </details>
 
<details>
<summary>(修仙2相关)</summary>

- 在xiuxian_config.py中配置好各种选项,官方bot仅试过使用 [Gensokyo](https://github.com/Hoshinonyaruko/Gensokyo) 正常运行，野生机器人推荐使用[NapCat](https://github.com/NapNeko/NapCatQQ)，[LLOneBot](https://github.com/LLOneBot/LLOneBot) ,[Lagrange](https://github.com/LagrangeDev/Lagrange.Core) 等

```
一般来说，只需要关注下面几项：
self.merge_forward_send = False # 消息转发类型,True是合并转发，False是长图发送，建议长图  
self.img_compression_limit = 80 # 图片压缩率，0为不压缩，最高100
self.img_type = "webp" # 图片类型，webp或者jpeg，如果机器人的图片消息不显示请使用jpeg
self.img_send_type = "io" # 图片发送类型,默认io,官方bot建议base64
self.put_bot = []  # 接收消息qq,主qq,框架将只处理此qq的消息，不配置将默认设置第一个链接的qq为主qq
self.main_bo = []  # 负责发送消息的qq,调用lay_out.py 下range_bot函数的情况下需要填写
self.shield_group = []  # 屏蔽的群聊
self.layout_bot_dict = {{}}  # QQ所负责的群聊{{群 :bot}}   其中 bot类型 []或str
示例：
{
    "群123群号" : "对应发送消息的qq号"
    "群456群号" ： ["对应发送消息的qq号1","对应发送消息的qq号2"]
}
当后面qq号为一个字符串时为一对一，为列表时为多对一
```

```py
self.put_bot = [] 
self.main_bo = []
self.shield_group = []
self.layout_bot_dict = {{}}
```
参数：

- `self.put_bot：`
- 默认为空
- 接收消息QQ，主QQ，插件将只处理此QQ的消息，不配置将默认设置第一个链接的QQ为主QQ

- `self.main_bo：`
- 默认为空
- 负责发送消息的QQ，调用 lay_out.py 下 range_bot函数 的情况下需要填写

- `self.shield_group：`
- 默认为空
- 参数：群号
- 屏蔽的群聊

- `self.layout_bot_dict：`
- 默认为空
- 参数：{群 :bot}。其中 bot 类型为列表或字符串
- QQ所负责的群聊
- 例子：

```py
    self.layout_bot_dict = {{
        "111": "xxx",               # 由QQ号为xxx的机器人单独负责111群聊
        "222": ["yyy", "zzz"]       # 由QQ号为yyy和zzz的机器人同时负责222群聊
    }}

```

 </details>
 
- websockets客户端Url：
```
ws://127.0.0.1:8080/onebot/v11/ws
```

# 💿 安装
### 新手请优先使用一键安装脚本

<details>
<summary>(Linux一键安装)</summary>

安装命令
```
# 默认目录
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install.sh | bash
# 自定义目录 /root/xiuxian
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install.sh | bash -s -- install /root/xiuxian
```
更新命令
```
# 默认目录
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install.sh | bash -s -- update
# 自定义目录 /root/xiuxian
curl -fsSL https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/raw/refs/heads/main/install.sh | bash -s -- update /root/xiuxian 
```
xiu命令
```
用法: xiu2 [start|stop|format [log_file]]
  start     - 启动 xiu2（默认，无需参数）
  status    - 查看 xiu2
  stop      - 停止 xiu2
  format [log_file] - 格式化日志文件（默认: /root/xiu2.log）
```
 </details>

<details>
<summary>(Windows一键安装)</summary>

[点我下载](https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/download/v0.1/install.bat)bat文件执行
```
https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/download/v0.1/install.bat
```
 </details>

<details>
<summary>(Linux手动安装：Debian)</summary>

安装Python
```
apt update && apt upgrade -y && \
apt install screen curl wget git python3 python3-pip python3-venv -y
```
安装napcat
```
curl -o napcat.sh https://nclatest.znin.net/NapNeko/NapCat-Installer/main/script/install.sh && sudo bash napcat.sh
```
安装nb-cli
```
cd ~
python3 -m venv myenv
source ~/myenv/bin/activate
pip install nb-cli==1.5.0
```
克隆项目
```
git clone --depth=1 -b main https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv.git
```
nb安装插件
```
cd ~
source ~/myenv/bin/activate #进入虚拟环境
nb #打开nb命令行
```
- 选择 Create a NoneBot project.（创建项目）
- 选择 simple
- 输入项目名 xiu2
选择适配器
```
OneBot V11 (OneBot V11 协议)
QQ (QQ官方机器人)
```
选择驱动器
```
FastAPI (FastAPI 驱动器)
HTTPX (HTTPX 驱动器)
websockets (websockets 驱动器)
AIOHTTP (AIOHTTP 驱动器)
```
- 选择 In a "src" folder（在src文件夹里）
确定安装虚拟环境和依赖
```
Install dependencies now? (Y/n) y
Create virtual environment? (Y/n) n
```
选择 echo（默认安装插件）


- 移动项目
```
mv ~/nonebot_plugin_xiuxian_2_pmv/nonebot_plugin_xiuxian_2 ~/xiu2/src/plugins
mv  ~/nonebot_plugin_xiuxian_2_pmv/data ~/xiu2
mv  ~/nonebot_plugin_xiuxian_2_pmv/requirements.txt ~/xiu2
```
安装修仙2依赖
```
cd ~/xiu2
pip install -r requirements.txt
```
修改nb配置
```
echo 'LOG_LEVEL=INFO # 日志等级INFO就行

SUPERUSERS = [""] # 野生bot填自己QQ号(不是机器人的QQ)，官方bot下的用户id自行获取，填的不对的话会出现指令无响应的情况

COMMAND_START = [""] # 指令前缀，默认空
NICKNAME = [""] # 机器人昵称

DEBUG = False
HOST = 127.0.0.1
PORT = 8080 # 反代的8080端口，有需要自己改' > ~/xiu2/.env.dev
```
启动修仙2
```
source ~/myenv/bin/activate
cd ~/xiu2
nb run --reload
```

查看修仙2 [B站安装教程](https://m.bilibili.com/video/BV1ZuesekEYy)

 </details>
 
<details>
<summary>(Windows安装)</summary>

安装Python
```
https://www.python.org/ftp/python/3.11.0/python-3.11.0-amd64.exe
```
安装napcat
```
https://napneko.github.io/guide/napcat
```
安装nb-cli
- 在C/D盘根目录新建文件夹然后打开cmd
```
mkdir C:\nb
cd C:\nb #进入nb文件夹，
python -m venv myenv
call myenv\Scripts\activate
pip install nb-cli==1.5.0
```
下载最新的project.tar.gz
```
https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv/releases/latest
```
nb安装插件
```
nb #打开nb命令行
```
- 选择 Create a NoneBot project.（创建项目）
- 选择 simple
- 输入项目名 xiu2
选择适配器
```
OneBot V11 (OneBot V11 协议)
QQ (QQ官方机器人)
```
选择驱动器
```
FastAPI (FastAPI 驱动器)
HTTPX (HTTPX 驱动器)
websockets (websockets 驱动器)
AIOHTTP (AIOHTTP 驱动器)
```
- 选择 In a "src" folder（在src文件夹里）
确定安装虚拟环境和依赖
```
Install dependencies now? (Y/n) y
Create virtual environment? (Y/n) n
```
选择 echo（默认安装插件）


- 解压project.tar.gz
```
移动nonebot_plugin_xiuxian_2 
到xiu2/src/plugins

移动data 
到xiu2

移动requirements.txt 
到xiu2
```
安装修仙2依赖
```
cd xiu2
pip install -r requirements.txt #失败可以把psutil删去重新安装
```
修改nb配置
```
echo 'LOG_LEVEL=INFO # 日志等级INFO就行

SUPERUSERS = [""] # 野生bot填自己QQ号(不是机器人的QQ)，官方bot下的用户id自行获取，填的不对的话会出现指令无响应的情况

COMMAND_START = [""] # 指令前缀，默认空
NICKNAME = [""] # 机器人昵称

DEBUG = False
HOST = 127.0.0.1
PORT = 8080 # 反代的8080端口，有需要自己改' > xiu2/.env.dev
```
启动修仙2
- 新建文件在`C:\nb`，改后缀`.bat`
```
call myenv\Scripts\activate
cd xiu2
nb run --reload
```

查看修仙2 [B站安装教程](https://m.bilibili.com/video/BV1ZuesekEYy)

 </details>

<details>
<summary>(安卓安装：Termux)</summary>

安装Termux
- [ZeroTermux](https://github.com/hanxinhao000/ZeroTermux/releases)
- [Termux](https://github.com/termux/termux-app/releases)

安装napcat
```
curl -o napcat.termux.sh https://nclatest.znin.net/NapNeko/NapCat-Installer/main/script/install.termux.sh && bash napcat.termux.sh
```
进入容器
```
proot-distro login napcat
```
剩下看Linux安装/一键安装，不需要安装napcat

> 下次启动要先进入容器: proot-distro login napcat

查看修仙2 [B站安装教程](https://m.bilibili.com/video/BV1ZuesekEYy)

 </details>

# 💿 连接QQ（Linux）

<details>
<summary>(napcat)</summary>

后台启动QQ
```
screen -dmS napcat bash -c 'xvfb-run -a /root/Napcat/opt/QQ/qq --no-sandbox'
```
进入QQ
```
screen -r napcat
```
退出screen
```
ctrl + a + d
```
关闭QQ
```
screen -S napcat -X quit
```
- napcat WEBUI
```
http://IP:6099
```
- 查看默认token:
```
/root/Napcat/opt/QQ/resources/app/app_launcher/napcat/config/webui.json
```
进入WEBUI，登录QQ

网络配置 > 新建 > websockets客户端

打开【启用】名称随意

- 默认修仙url
```
ws://127.0.0.1:8080/onebot/v11/ws
```
- 保存

 </details>

<details>
<summary>(修仙2)</summary>

- Termux 进入容器
```
proot-distro login napcat
```
启动修仙2
```
screen -dmS xiu2 bash -c 'source ~/myenv/bin/activate && cd ~/xiu2 && nb run'
```
进入修仙2
```
screen -r xiu2
```
退出screen
```
ctrl + a + d
```
关闭修仙2
```
screen -S xiu2 -X quit
```

 </details>
 
# 💿 使用

发送 `修仙帮助` 查看功能

发送 `修仙手册` 查看管理员指令

可以来这体验[144795954](https://qun.qq.com/universal-share/share?ac=1&authKey=JcaNbcnyFbgcjfffkakYujFwpYFJewe2mSFUtSNWi1mA6qap%2FHBQNsCl0D9olm4I&busi_data=eyJncm91cENvZGUiOiIxNDQ3OTU5NTQiLCJ0b2tlbiI6ImZKYXpKOVM3Z0pwek80ZlUzLzhzbWN1Y1daY0JIQy9BYXZFUlZGd1lGREJQUXJXWERLNlJCcFNjSjVGc3JZVWsiLCJ1aW4iOiIyNjUwMTE1MzE3In0%3D&data=5w52a2CkyEIX_t_INqS29fA4Sxl8eozGazmL-EIUo6ehG7ESdNgxtDnVmgXoLlLfaVeZ2SbPMW-1SJ4I9o7IeQ&svctype=4&tempid=h5_group_info)

如果你使用的是官方机器人记得改配置

# 🎉 特别感谢

- [NoneBot2](https://github.com/nonebot/nonebot2)：本插件实装的开发框架，NB天下第一可爱。
- [nonebot_plugin_xiuxian](https://github.com/s52047qwas/nonebot_plugin_xiuxian)：原版修仙
- [nonebot_plugin_xiuxian_2](https://github.com/QingMuCat/nonebot_plugin_xiuxian_2)：原版修仙2
- [nonebot_plugin_xiuxian_2_pmv](https://github.com/MyXiaoNan/nonebot_plugin_xiuxian_2_pmv)：修仙2魔改版

# 🎉 许可证

本项目使用 [MIT](https://choosealicense.com/licenses/mit/) 作为开源许可证，并且没有cc限制
