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

# 💿 安装

<details>
<summary>(Termux)</summary>

```安装napcat
curl -o napcat.termux.sh https://nclatest.znin.net/NapNeko/NapCat-Installer/main/script/install.termux.sh && bash napcat.termux.sh
```

```进入容器
proot-distro login napcat
```

```进入容器
proot-distro login napcat
```

```安装Python
apt update && apt upgrade -y && \
apt install screen git python3 python3-pip python3-venv -y
```

```安装nb-cli
cd ~
python3 -m venv myenv
source ~/myenv/bin/activate
pip install nb-cli
```

```克隆项目
git clone --depth=1 -b main https://github.com/liyw0205/nonebot_plugin_xiuxian_2_pmv.git
```

nb安装插件

```
cd ~
source ~/myenv/bin/activate #进入虚拟环境
nb #打开nb命令行
```

选择 Create a NoneBot project.（创建项目）
选择 simple
输入项目名 xiu2

```选择适配器
OneBot V11 (OneBot V11 协议)
```

```选择驱动器
FastAPI (FastAPI 驱动器)
HTTPX (HTTPX 驱动器)
websockets (websockets 驱动器)
AIOHTTP (AIOHTTP 驱动器)
```

选择 In a "src" folder（在src文件夹里）

```确定安装虚拟环境和依赖
Create virtual environment? (Y/n) y
Install dependencies now? (Y/n) y
```

选择 echo（默认安装插件）

- 移动 requirements.txt 文件 data 文件夹 到nb插件目录，如上面设置的项目名xiu2

- 移动 nonebot_plugin_xiuxian_2 文件夹 到nb插件目录的src文件夹里的插件文件夹里，src/plugins/nonebot_plugin_xiuxian_2

```安装修仙2依赖
cd ~/xiu2
pip install -r requirements.txt
```

```启动修仙2
source ~/myenv/bin/activate
cd ~/xiu2
nb run
```

不懂可查看修仙2 B站安装教程: BV1ZuesekEYy
 </details>

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

- Url：ws://127.0.0.1:8080/onebot/v11/ws
- websockets客户端

# 💿 使用

群聊发送 `启用修仙功能` 后根据引导来即可，不支持私聊
如果你使用的是官方机器人记得改配置

# 🎉 特别感谢

- [NoneBot2](https://github.com/nonebot/nonebot2)：本插件实装的开发框架，NB天下第一可爱。
- [nonebot_plugin_xiuxian](https://github.com/s52047qwas/nonebot_plugin_xiuxian)：原版修仙
- [nonebot_plugin_xiuxian_2](https://github.com/QingMuCat/nonebot_plugin_xiuxian_2)：原版修仙2
- [nonebot_plugin_xiuxian_2_pmv](https://github.com/MyXiaoNan/nonebot_plugin_xiuxian_2_pmv)：修仙2魔改版

# 🎉 许可证

本项目使用 [MIT](https://choosealicense.com/licenses/mit/) 作为开源许可证，并且没有cc限制
