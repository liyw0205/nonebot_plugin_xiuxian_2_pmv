<div align="center">
  <br>
</div>

<div align="center">

# 🎉 修仙2.2魔改版

_✨ QQ群聊修仙文字游戏✨_

<p align="center">
</p>
</div>

# 📖 介绍

一款适用于QQ群的修仙插件,设定征集中，有好的想法可以推送给我哦~~~

原插件地址：https://github.com/MyXiaoNan/nonebot_plugin_xiuxian_2_pmv

# 🎉 和原版有什么区别？

1、灵庄等级增加，利息收益增加

2、宗门任务上限修为增加，宗门丹药基础数增加

3、灵田可开垦到9田，结算时间为24小时

4、悬赏令时长缩短

5、灵根机械核心和异世界之力种类增多

6、秘境奖励正面事件概率增加

7、添加全服坊市功能：仙肆

8、修复传承抽卡次数，支持批量抽卡

9、提高闭关回状态100%

10、添加功能：挑战稻草人

11、添加功能：查看效果

12、坊市/仙肆查看区分类别

13、大部分功能支持私聊

# 💿 安装

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
剩下看Linux安装，不需要安装napcat

> 下次启动要先进入容器: proot-distro login napcat

查看修仙2 [B站安装教程](https://m.bilibili.com/video/BV1ZuesekEYy)

 </details>

<details>
<summary>(Linux安装：Debian)</summary>

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
pip install nb-cli
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
Create virtual environment? (Y/n) y
Install dependencies now? (Y/n) y
```
选择 echo（默认安装插件）


- 下载字体和卡图包
```
curl -L -o ~/xiuxian.zip https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv_file/releases/download/v0/xiuxian.zip
```
- 解压字体和卡图包
```
unzip ~/xiuxian.zip -d /root/xiu2/data/xiuxian
```
- 移动项目
```
mv ~/nonebot_plugin_xiuxian_2_pmv/nonebot_plugin_xiuxian_2 ~/xiu2/src/plugins
mv  ~/nonebot_plugin_xiuxian_2_pmv/data ~/xiu2
mv  ~/nonebot_plugin_xiuxian_2_pmv/requirements.txt ~/xiu2
```
安装修仙2依赖
```
cd ~/xiu2
source ~/xiu2/.venv/bin/activate
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

# 💿 连接QQ

<details>
<summary>(napcat)</summary>

后台启动QQ
```
screen -dmS napcat bash -c 'xvfb-run -a qq --no-sandbox'
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
- 默认token:
```
napcat
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

群聊发送 `启用修仙功能` 后根据引导来即可，不支持私聊

发送 `修仙帮助` 查看功能

如果你使用的是官方机器人记得改配置

# 🎉 特别感谢

- [NoneBot2](https://github.com/nonebot/nonebot2)：本插件实装的开发框架，NB天下第一可爱。
- [nonebot_plugin_xiuxian](https://github.com/s52047qwas/nonebot_plugin_xiuxian)：原版修仙
- [nonebot_plugin_xiuxian_2](https://github.com/QingMuCat/nonebot_plugin_xiuxian_2)：原版修仙2
- [nonebot_plugin_xiuxian_2_pmv](https://github.com/MyXiaoNan/nonebot_plugin_xiuxian_2_pmv)：修仙2魔改版

# 🎉 许可证

本项目使用 [MIT](https://choosealicense.com/licenses/mit/) 作为开源许可证，并且没有cc限制

