try:
    import ujson as json
except ImportError:
    import json
import re
import os
from pathlib import Path
import random
import asyncio
from datetime import datetime
from nonebot.typing import T_State
from ..xiuxian_utils.lay_out import assign_bot, Cooldown, assign_bot_group
from nonebot import require, on_command, on_fullmatch, get_bot
from nonebot.adapters.onebot.v11 import (
    Bot,
    GROUP,
    Message,
    GROUP_ADMIN,
    GROUP_OWNER,
    GroupMessageEvent,
    PrivateMessageEvent,
    MessageSegment,
    ActionFailed
)
from nonebot.permission import SUPERUSER
from nonebot.log import logger
from nonebot.params import CommandArg
from ..xiuxian_utils.data_source import jsondata
from ..xiuxian_utils.player_fight import Boss_fight
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, XiuxianJsonDate, OtherSet, 
    UserBuffDate, XIUXIAN_IMPART_BUFF, leave_harm_time
)
from ..xiuxian_config import XiuConfig, JsonConfig, convert_rank
from ..xiuxian_utils.utils import (
    check_user, check_user_type,
    get_msg_pic, number_to,
    CommandObjectID,
    Txt2Img, send_msg_handler, handle_send, get_logs, log_message
)
from ..xiuxian_utils.item_json import Items
from ..xiuxian_back import BANNED_ITEM_IDS
from .stone_limit import stone_limit
from .lottery_pool import lottery_pool
items = Items()

# 定时任务
scheduler = require("nonebot_plugin_apscheduler").scheduler
cache_help = {}
cache_level_help = {}
cache_level1_help = {}
cache_level2_help = {}
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()
PLAYERSDATA = Path() / "data" / "xiuxian" / "players"
qqq = 144795954

gfqq = on_command("官群", aliases={"交流群"}, priority=8, block=True)
run_xiuxian = on_command("我要修仙", aliases={"开始修仙"}, priority=8, block=True)
restart = on_fullmatch("重入仙途", priority=7, block=True)
sign_in = on_command("修仙签到", priority=13, block=True)
hongyun = on_command("鸿运", aliases={"查看中奖", "奖池查询"}, priority=5, block=True)
help_in = on_command("修仙帮助", aliases={"菜单", "帮助"}, priority=12, block=True)
rank = on_command("排行榜", aliases={"修仙排行榜", "灵石排行榜", "战力排行榜", "境界排行榜", "宗门排行榜", "轮回排行榜"},
                  priority=7, block=True)
remaname = on_command("修仙改名", priority=5, block=True)
level_up = on_fullmatch("突破", priority=6, block=True)
level_up_dr = on_fullmatch("渡厄突破", priority=7, block=True)
level_up_drjd = on_command("渡厄金丹突破", aliases={"金丹突破"}, priority=7, block=True)
level_up_zj = on_command("直接突破", aliases={"破"}, priority=7, block=True)
level_up_lx = on_command("连续突破", aliases={"快速突破"}, priority=7, block=True)
give_stone = on_command("送灵石", priority=5, permission=GROUP, block=True)
steal_stone = on_command("偷灵石", aliases={"飞龙探云手"}, priority=4, permission=GROUP, block=True)
gm_command = on_command("神秘力量", permission=SUPERUSER, priority=10, block=True)
gmm_command = on_command("轮回力量", permission=SUPERUSER, priority=10, block=True)
ccll_command = on_command("传承力量", permission=SUPERUSER, priority=10, block=True)
zaohua_xiuxian = on_command('造化力量', permission=SUPERUSER, priority=15,block=True)
cz = on_command('创造力量', permission=SUPERUSER, priority=15,block=True)
rob_stone = on_command("抢灵石", aliases={"抢劫"}, priority=5, permission=GROUP, block=True)
restate = on_command("重置状态", permission=SUPERUSER, priority=12, block=True)
set_xiuxian = on_command("启用修仙功能", aliases={'禁用修仙功能'}, permission=GROUP and (SUPERUSER | GROUP_ADMIN | GROUP_OWNER), priority=5, block=True)
set_private_chat = on_command("启用私聊功能", aliases={'禁用私聊功能'}, permission=SUPERUSER, priority=5, block=True)
auto_root = on_command("自动选择灵根", aliases={'开启自动选择灵根', '关闭自动选择灵根'}, permission=SUPERUSER, priority=5, block=True)
user_leveluprate = on_command('我的突破概率', aliases={"突破概率", "概率"}, priority=5, block=True)
user_stamina = on_command('我的体力', aliases={'体力'}, priority=5, block=True)
xiuxian_updata_level = on_fullmatch('修仙适配', priority=15, permission=GROUP, block=True)
xiuxian_uodata_data = on_fullmatch('更新记录', priority=15, permission=GROUP, block=True)
level_help = on_command("灵根帮助", aliases={"灵根列表"}, priority=15, block=True)
level1_help = on_command("品阶帮助", aliases={"品阶列表"}, priority=15, block=True)
level2_help = on_command("境界帮助", aliases={"境界列表"}, priority=15, block=True)
view_logs = on_command("修仙日志", aliases={"查看日志", "我的日志", "查日志", "日志记录"}, priority=5, block=True)
give_xiangyuan = on_command("送仙缘", priority=5, block=True)
get_xiangyuan = on_command("抢仙缘", priority=5, block=True)
xiangyuan_list = on_command("仙缘列表", priority=5, block=True)
clear_xiangyuan = on_command("清空仙缘", permission=SUPERUSER, priority=5, block=True)
tribulation_info = on_command("渡劫", priority=5, block=True)
start_tribulation = on_command("开始渡劫", priority=6, block=True)
destiny_tribulation = on_command("天命渡劫", priority=6, block=True)
heart_devil_tribulation = on_command("渡心魔劫", priority=6, block=True)
fusion_destiny_tribulation_pill = on_command("融合天命渡劫丹", aliases={"合成天命渡劫丹"}, priority=5, block=True)
fusion_destiny_pill = on_command("融合天命丹", aliases={"合成天命丹"}, priority=5, block=True)

__xiuxian_notes__ = f"""
【修仙指令】✨
===========
🌟 核心功能
→ 启程修仙:发送"我要修仙"🏃
→ 状态查询:发送"我的修仙信息"📊
→ 每日签到:发送"修仙签到"📅
→ 突破境界:发送"突破"🚀
*支持"连续突破"五次
→ 灵石交互:送/偷/抢灵石+道号+数量💰
===========
🌈 角色养成
→ 修炼方式:闭关/出关/灵石出关/灵石修炼/双修🧘
→ 灵根重置:发送"重入仙途"（需10万灵石）💎
→ 功法体系:发送"境界/品阶/灵根帮助"📚
→ 轮回重修:发送"轮回重修帮助"🌀
===========
🏯 系统功能
→ 交易功能:发送"交易帮助"
→ 宗门体系:发送"宗门帮助"
→ 灵庄系统:发送"灵庄帮助"
→ 秘境探索:发送"秘境帮助"
→ 炼丹指南:发送"炼丹帮助"
→ 灵田管理:发送"灵田帮助"
→ 传承玩法:发送"传承帮助"
===========
🎮 特色玩法
→ 世界BOSS:发送"世界boss帮助"👾
→ 无限爬塔:发送"通天塔帮助"🏯
→ 明我心志:发送"幻境寻心"🌀
→ 历练之旅:发送"历练帮助"🌀
→ 仙缘奇遇:发送"仙途奇缘帮助"🌈
→ 物品合成:发送"合成帮助"🔧
→ 批量祈愿:发送"传承祈愿 1000"🙏
===========
⚙️ 系统设置
→ 修改道号:发送"修仙改名+道号"✏️
→ 悬赏任务:发送"悬赏令帮助"📜
→ 状态查看:发送"我的状态"📝
→ 加入官群:发送"官群"🎁
===========
🏆 排行榜单
修仙/灵石/战力/宗门/轮回/虚神界/通天塔/历练排行榜
""".strip()



__xiuxian_updata_data__ = f"""
详情：
#更新2023.6.14
1.修复已知bug
2.增强了Boss，现在的BOSS会掉落物品了
3.增加了全新物品
4.悬赏令刷新需要的灵石会随着等级增加
5.减少了讨伐Boss的cd（减半）
6.世界商店上新
7.增加了闭关获取的经验（翻倍）
#更新2023.6.16
1.增加了仙器合成
2.再次增加了闭关获取的经验（翻倍）
3.上调了Boss的掉落率
4.修复了悬赏令无法刷新的bug
5.修复了突破CD为60分钟的问题
6.略微上调Boss使用神通的概率
7.尝试修复丹药无法使用的bug
#更新2024.3.18
1.修复了三个模块循环导入的问题
2.合并read_bfff,xn_xiuxian_impart到dandle中
#更新2024.4.05（后面的改动一次性加进来）
1.增加了金银阁功能(调试中)
2.坊市上架，购买可以自定义数量
3.生成指定境界boss可以指定boss名字了
4.替换base64为io（可选），支持转发消息类型设置，支持图片压缩率设置
5.适配Pydantic,Pillow,更换失效的图片api
6.替换数据库元组为字典返回，替换USERRANK为convert_rank函数
7.群拍卖会可以依次拍卖多个物品了
8.支持用户提交拍卖品了，拍卖时优先拍卖用户的拍卖品
9.实现简单的体力系统
10.重构合成系统
""".strip()

__level_help__ = f"""
详情:
        --灵根帮助--
           命运道果
永恒道果—轮回道果—异界
  机械——混沌——融合
 超—龙—天—异—真—伪
""".strip()



__level1_help__ = f"""
详情:
       --功法品阶--
              无上
           仙阶极品
仙阶上品——仙阶下品
天阶上品——天阶下品
地阶上品——地阶下品
玄阶上品——玄阶下品
黄阶上品——黄阶下品
人阶上品——人阶下品

       --法器品阶--
              无上
           极品仙器
上品仙器——下品仙器
上品通天——下品通天
上品纯阳——下品纯阳
上品玄器——下品玄器
上品法器——下品法器
上品符器——下品符器
""".strip()

__level2_help__ = f"""
详情:
            --境界帮助--            
                江湖人
                  ↓
感气境 → 练气境 → 筑基境
结丹境 → 金丹境 → 元神境 
化神境 → 炼神境 → 返虚境
大乘境 → 虚道境 → 斩我境 
遁一境 → 至尊境 → 微光境
星芒境 → 月华境 → 耀日境
祭道境 → 自在境 → 破虚境 
无界境 → 混元境 → 造化境
                  ↓
                永恒境
                  ↓          
                 至高
""".strip()

# 重置每日签到
@scheduler.scheduled_job("cron", hour=0, minute=0)
async def xiuxian_sing_():
    sql_message.sign_remake()
    logger.opt(colors=True).info(f"<green>每日修仙签到重置成功！</green>")

@scheduler.scheduled_job("cron", hour=0, minute=0)
async def reset_lottery_participants():
    lottery_pool.reset_daily()
    logger.opt(colors=True).info(f"<green>每日借运参与者已重置！</green>")
    
@scheduler.scheduled_job("cron", hour=0, minute=0)
async def reset_stone_limits():
    stone_limit.reset_limits()
    logger.opt(colors=True).info(f"<green>每日灵石赠送额度已重置！</green>")
    
@xiuxian_uodata_data.handle(parameterless=[Cooldown(at_sender=False)])
async def mix_elixir_help_(bot: Bot, event: GroupMessageEvent):
    """更新记录"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __xiuxian_updata_data__
    await handle_send(bot, event, msg)
    await xiuxian_uodata_data.finish() 

@gfqq.handle(parameterless=[Cooldown(at_sender=False, cd_time=30)])
async def gfqq_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = f"{qqq}"
    await handle_send(bot, event, msg)
    
@remaname.handle(parameterless=[Cooldown(at_sender=False, cd_time=30)])
async def remaname_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """修改道号"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await remaname.finish()
    user_id = user_info['user_id']
    
    if user_info['stone'] < XiuConfig().remaname:
        msg = f"修改道号需要消耗{XiuConfig().remaname}灵石，你的灵石不足！"
        await handle_send(bot, event, msg)
        await remaname.finish()
    # 扣除灵石
    sql_message.update_ls(user_id, XiuConfig().remaname, 2)
    # 如果没有提供新道号，则生成随机道号
    user_name = args.extract_plain_text().strip()
    if not user_name:
        # 生成不重复的道号
        while True:
            user_name = generate_daohao()
            if not sql_message.get_user_info_with_name(user_name):
                break
        msg = f"你获得了随机道号：{user_name}\n"
    else:            
        # 检查易名符
        has_item = False
        back_msg = sql_message.get_back_msg(user_id)
        for item in back_msg:
            if item['goods_id'] == 20011 and item['goods_name'] == "易名符":
                has_item = True
                break
                
        if not has_item:
            msg = "修改道号需要消耗1个易名符！"
            await handle_send(bot, event, msg)
            await remaname.finish()
            
        # 检查名字长度（7个中文字符）
        if len(user_name) > 7:
            msg = "道号长度不能超过7个字符！"
            await handle_send(bot, event, msg)
            await remaname.finish()
            
        # 检查道号是否已存在
        if sql_message.get_user_info_with_name(user_name):
            msg = "该道号已被使用，请选择其他道号！"
            await handle_send(bot, event, msg)
            await remaname.finish()
        
        # 扣除易名符
        sql_message.update_back_j(user_id, 20011, use_key=1)
    
    result = sql_message.update_user_name(user_id, user_name)
    msg += result
    await handle_send(bot, event, msg)
    await remaname.finish()


@run_xiuxian.handle(parameterless=[Cooldown(at_sender=False)])
async def run_xiuxian_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我要修仙"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_id = event.get_user_id()
    
    # 生成不重复的道号
    while True:
        user_name = generate_daohao()
        if not sql_message.get_user_info_with_name(user_name):
            break
    
    root, root_type = XiuxianJsonDate().linggen_get()  # 获取灵根，灵根类型
    rate = sql_message.get_root_rate(root_type, user_id)  # 灵根倍率
    power = 100 * float(rate)  # 战力=境界的power字段 * 灵根的rate字段
    create_time = str(datetime.now())
    is_new_user, msg = sql_message.create_user(
        user_id, root, root_type, int(power), create_time, user_name
    )
    try:
        if is_new_user:
            await handle_send(bot, event, msg)
            isUser, user_msg, msg = check_user(event)
            if user_msg['hp'] is None or user_msg['hp'] == 0 or user_msg['hp'] == 0:
                sql_message.update_user_hp(user_id)
            await asyncio.sleep(1)
            msg = f"你获得了随机道号：{user_name}\n耳边响起一个神秘人的声音：不要忘记仙途奇缘！\n不知道怎么玩的话可以发送 修仙帮助 喔！！"
        await handle_send(bot, event, msg)
    except ActionFailed:
        await run_xiuxian.finish("修仙界网络堵塞，发送失败!", reply_message=True)


@sign_in.handle(parameterless=[Cooldown(at_sender=False)])
async def sign_in_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """修仙签到"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await sign_in.finish()
    user_id = user_info['user_id']
    
    # 1. 执行签到逻辑
    result = sql_message.get_sign(user_id)
    if result == "贪心的人是不会有好运的！":
        log_message(user_id, result)
        await handle_send(bot, event, result)
        await sign_in.finish()
     # 2. 自动参与"借运"抽奖
    lottery_result = await handle_lottery(user_info)
    
    # 3. 组合签到结果和抽奖结果
    msg = f"{result}\n{lottery_result}"
    
    try:
        log_message(user_id, msg)
        await handle_send(bot, event, msg)
        await sign_in.finish()
    except ActionFailed:
        await sign_in.finish("修仙界网络堵塞，发送失败!", reply_message=True)

async def handle_lottery(user_info: dict):
    """处理借运抽奖逻辑"""
    user_id = user_info['user_id']
    user_name = user_info['user_name']
    
    # 1. 每人每次签到存入100万灵石到奖池
    deposit_amount = 1000000
    lottery_pool.deposit_to_pool(deposit_amount)
    lottery_pool.add_participant(user_id)
    
    # 2. 生成1-100000的随机数，中奖号码为66666,6666,666,66,6
    lottery_number = random.randint(1, 100000)
    winning_numbers = [66666, 6666, 666, 66, 6]
    
    if lottery_number in winning_numbers:
        # 中奖逻辑
        prize = lottery_pool.get_pool()
        
        # 发放奖励
        sql_message.update_ls(user_id, prize, 1)
        
        # 记录中奖信息
        lottery_pool.set_winner(user_id, user_name, prize, lottery_number)
        
        return f"✨鸿运当头！道友借运成功，获得奖池中全部{number_to(prize)}灵石！✨"
    
    # 3. 未中奖情况
    return f"本次签到未中奖，奖池继续累积~"

@hongyun.handle(parameterless=[Cooldown(at_sender=False)])
async def hongyun_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看中奖记录和当前奖池"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    
    # 构建消息
    msg = "✨【鸿运当头】奖池信息✨\n"
    msg += f"当前奖池累计：{number_to(lottery_pool.get_pool())}灵石\n"
    msg += f"本期参与人数：{lottery_pool.get_participants()}位道友\n\n"
    
    last_winner = lottery_pool.get_last_winner()
    if last_winner:
        msg += "🎉🎉🎉🎉上期中奖记录🎉🎉🎉🎉\n"
        msg += f"中奖道友：{last_winner['name']}\n"
        msg += f"中奖时间：{last_winner['time']}\n"
        msg += f"中奖金额：{number_to(last_winner['amount'])}灵石\n"
    else:
        msg += "暂无历史中奖记录，道友快来签到吧！\n"
    
    msg += "\n※ 每次签到自动存入100万灵石到奖池，中奖号码将独享全部奖池！"
    
    await handle_send(bot, event, msg)
    await hongyun.finish()

def read_lottery_data():
    """读取奖池数据"""
    try:
        with open('xiuxian_lottery.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # 初始化数据
        return {
            'pool': 0,
            'participants': [],
            'last_winner': None
        }

def save_lottery_data(data):
    """保存奖池数据"""
    with open('xiuxian_lottery.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

@help_in.handle(parameterless=[Cooldown(at_sender=False)])
async def help_in_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """修仙帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_help:
        msg = cache_help[session_id]
        await handle_send(bot, event, msg)
        await help_in.finish()
    else:
        font_size = 32
        title = "修仙帮助"
        msg = __xiuxian_notes__
        img = Txt2Img(font_size)
        if XiuConfig().img:
            await handle_send(bot, event, msg)
        else:
            await handle_send(bot, event, msg)
        await help_in.finish()


@level_help.handle(parameterless=[Cooldown(at_sender=False)])
async def level_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """灵根帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_level_help:
        msg = cache_level_help[session_id]
        await handle_send(bot, event, msg)
        await level_help.finish()
    else:
        font_size = 32
        title = "灵根帮助"
        msg = __level_help__
        img = Txt2Img(font_size)
        if XiuConfig().img:
            await handle_send(bot, event, msg)
        else:
            await handle_send(bot, event, msg)
        await level_help.finish()

        
@level1_help.handle(parameterless=[Cooldown(at_sender=False)])
async def level1_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """品阶帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_level1_help:
        msg = cache_level1_help[session_id]
        await handle_send(bot, event, msg)
        await level1_help.finish()
    else:
        font_size = 32
        title = "品阶帮助"
        msg = __level1_help__
        img = Txt2Img(font_size)
        if XiuConfig().img:
            await handle_send(bot, event, msg)
        else:
            await handle_send(bot, event, msg)
        await level1_help.finish()
        
@level2_help.handle(parameterless=[Cooldown(at_sender=False)])
async def level2_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, session_id: int = CommandObjectID()):
    """境界帮助"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    if session_id in cache_level2_help:
        msg = cache_level2_help[session_id]
        await handle_send(bot, event, msg)
        await level2_help.finish()
    else:
        font_size = 32
        title = "境界帮助"
        msg = __level2_help__
        img = Txt2Img(font_size)
        if XiuConfig().img:
            await handle_send(bot, event, msg)
        else:
            await handle_send(bot, event, msg)
        await level2_help.finish()

@auto_root.handle()
async def auto_root_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """开关自动选择灵根功能"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    command = event.get_plaintext().strip()
    
    if "开启" in command:
        JsonConfig().write_data(5)  # 5对应开启自动选择灵根
        msg = "已开启自动选择最佳灵根功能！重入仙途时将自动为您选择最佳灵根。"
    elif "关闭" in command:
        JsonConfig().write_data(6)  # 6对应关闭自动选择灵根
        msg = "已关闭自动选择灵根功能！重入仙途时将需要手动选择灵根。"
    else:
        status = "开启" if JsonConfig().is_auto_root_selection_enabled() else "关闭"
        msg = f"当前自动选择灵根功能状态：{status}\n使用'开启自动选择灵根'或'关闭自动选择灵根'来修改设置"
    
    await handle_send(bot, event, msg)
    await auto_root.finish()

@restart.handle(parameterless=[Cooldown(at_sender=False)])
async def restart_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    """刷新灵根信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await restart.finish()

    if user_info['stone'] < XiuConfig().remake:
        msg = "你的灵石还不够呢，快去赚点灵石吧！"
        await handle_send(bot, event, msg)
        await restart.finish()

    user_id = user_info['user_id']
    user_root = user_info['root_type']
  
    if user_root == '轮回道果' or user_root == '真·轮回道果' or user_root == '永恒道果' or user_root == '命运道果':
        msg = f"道友已入轮回，拥有{user_root}无需重入仙途！"
        await handle_send(bot, event, msg)
        await restart.finish()

    # 生成10个随机灵根选项
    linggen_options = []
    for _ in range(10):
        name, root_type = XiuxianJsonDate().linggen_get()
        linggen_options.append((name, root_type))
    
    # 显示所有随机生成的灵根选项
    linggen_list_msg = "本次随机生成的灵根有：\n"
    linggen_list_msg += "\n".join([f"{i+1}. {name} ({root_type})" for i, (name, root_type) in enumerate(linggen_options)])
    
    # 自动选择最佳灵根
    if JsonConfig().is_auto_root_selection_enabled():
        # 按灵根倍率排序选择最佳灵根
        selected_name, selected_root_type = max(linggen_options, 
                                             key=lambda x: jsondata.root_data()[x[1]]["type_speeds"])
        msg = f"{linggen_list_msg}\n\n已自动为您选择最佳灵根：{selected_name} ({selected_root_type})"
        await handle_send(bot, event, msg)
        msg = sql_message.ramaker(selected_name, selected_root_type, user_id)
        await handle_send(bot, event, msg)
        await restart.finish()
    else:
        # 保留原来的手动选择逻辑
        state["user_id"] = user_id
        msg = f"{linggen_list_msg}\n\n请从以上灵根中选择一个:\n请输入对应的数字选择 (1-10):"
        state["linggen_options"] = linggen_options
        await handle_send(bot, event, msg)

@restart.receive()
async def handle_user_choice(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, state: T_State):
    user_choice = event.get_plaintext().strip()
    linggen_options = state["linggen_options"]
    user_id = state["user_id"]  # 从状态中获取用户ID
    selected_name, selected_root_type = max(linggen_options, key=lambda x: jsondata.root_data()[x[1]]["type_speeds"])

    if user_choice.isdigit(): # 判断数字
        user_choice = int(user_choice)
        if 1 <= user_choice <= 10:
            selected_name, selected_root_type = linggen_options[user_choice - 1]
            msg = f"你选择了 {selected_name} 呢！\n"
    else:
        msg = "输入有误，帮你自动选择最佳灵根了嗷！\n"

    msg += sql_message.ramaker(selected_name, selected_root_type, user_id)

    try:
        await handle_send(bot, event, msg)
    except ActionFailed:
        await bot.send_group_msg(group_id=event.group_id, message="修仙界网络堵塞，发送失败!")
    await restart.finish()


@rank.handle(parameterless=[Cooldown(at_sender=False)])
async def rank_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """排行榜"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    message = str(event.message)
    rank_msg = r'[\u4e00-\u9fa5]+'
    message = re.findall(rank_msg, message)
    if message:
        message = message[0]
    if message in ["排行榜", "修仙排行榜", "境界排行榜", "修为排行榜"]:
        p_rank = sql_message.realm_top()
        msg = f"\n✨位面境界排行榜TOP50✨\n"
        num = 0
        for i in p_rank:
            num += 1
            msg += f"第{num}位 {i[0]} {i[1]},修为{number_to(i[2])}\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message == "灵石排行榜":
        a_rank = sql_message.stone_top()
        msg = f"\n✨位面灵石排行榜TOP50✨\n"
        num = 0
        for i in a_rank:
            num += 1
            msg += f"第{num}位  {i[0]}  灵石：{number_to(i[1])}枚\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message == "战力排行榜":
        c_rank = sql_message.power_top()
        msg = f"\n✨位面战力排行榜TOP50✨\n"
        num = 0
        for i in c_rank:
            num += 1
            msg += f"第{num}位  {i[0]}  战力：{number_to(i[1])}\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message in ["宗门排行榜", "宗门建设度排行榜"]:
        s_rank = sql_message.scale_top()
        msg = f"\n✨位面宗门建设排行榜TOP50✨\n"
        num = 0
        for i in s_rank:
            num += 1
            msg += f"第{num}位  {i[1]}  建设度：{number_to(i[2])}\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message == "轮回排行榜":
        r_rank = sql_message.root_top()
        msg = f"\n✨轮回排行榜TOP50✨\n"
        num = 0
        for i in r_rank:
            num += 1
            msg += f"第{num}位  {i[0]}  轮回：{number_to(i[1])}次\n"
            if num == 50:
                break
        await handle_send(bot, event, msg)
        await rank.finish()

def get_user_tribulation_info(user_id):
    """获取用户渡劫信息"""
    user_id = str(user_id)
    file_path = PLAYERSDATA / user_id / "tribulation_info.json"
    
    default_data = {
        "current_rate": XiuConfig().tribulation_base_rate,
        "last_time": None,
        "next_level": None
    }
    
    if not file_path.exists():
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(default_data, f, ensure_ascii=False, indent=4)
        return default_data
    
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            # 确保所有字段都存在
            for key in default_data:
                if key not in data:
                    data[key] = default_data[key]
            return data
        except:
            return default_data

def save_user_tribulation_info(user_id, data):
    """保存用户渡劫信息"""
    user_id = str(user_id)
    file_path = PLAYERSDATA / user_id / "tribulation_info.json"
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def clear_user_tribulation_info(user_id):
    """清空用户渡劫信息(渡劫成功后调用)"""
    user_id = str(user_id)
    file_path = PLAYERSDATA / user_id / "tribulation_info.json"
    
    if file_path.exists():
        file_path.unlink()

@tribulation_info.handle(parameterless=[Cooldown(at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """查看渡劫信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await tribulation_info.finish()
    
    user_id = user_info['user_id']
    tribulation_data = get_user_tribulation_info(user_id)
    
    # 构建消息
    msg = "✨【渡劫信息】✨\n"
    msg += f"当前境界：{user_info['level']}\n"
    
    # 检查是否需要渡劫
    level_name = user_info['level']
    levels = convert_rank('江湖好手')[1]
    current_index = levels.index(level_name)
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) < levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}无需渡劫，请使用【突破】指令！"
        await handle_send(bot, event, msg)
        await tribulation_info.finish()

    if current_index == 0:  # 已经是最高境界
        msg += "道友已是至高境界，无需渡劫！"
        await handle_send(bot, event, msg)
        await tribulation_info.finish()
    else:
        next_level = levels[current_index + 1]
        next_level_data = jsondata.level_data()[next_level]
        current_exp = int(user_info['exp'])
        required_exp = int(next_level_data['power'])
        
        # 检查渡劫条件：境界圆满且修为达到下一境界要求
        need_tribulation = (
            level_name.endswith('圆满') and 
            current_exp >= required_exp
        )
        
        if need_tribulation:
            msg += (
                f"下一境界：{next_level}\n"
                f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
                f"渡劫成功率：{tribulation_data['current_rate']}%\n"
                f"════════════\n"
                f"【开始渡劫】尝试渡劫\n"
                f"【天命渡劫】使用天命渡劫丹\n"
                f"【渡心魔劫】挑战心魔\n"
                f"【融合天命渡劫丹】天命渡劫"
            )
        else:
            if not level_name.endswith('圆满'):
                msg += f"道友境界尚未圆满，无法渡劫！"
            else:
                # 计算还需要多少修为
                remaining_exp = max(0, required_exp - current_exp)
                msg += (
                    f"下一境界：{next_level}\n"
                    f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
                    f"还需修为：{number_to(remaining_exp)}\n"
                    f"════════════\n"
                    f"请继续修炼，待修为足够后再来渡劫！"
                )
    
    await handle_send(bot, event, msg)
    await tribulation_info.finish()

@fusion_destiny_pill.handle(parameterless=[Cooldown(at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """融合天命丹"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await fusion_destiny_pill.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().strip()
    
    # 解析数量参数
    try:
        num = int(args) if args else 2  # 默认2个渡厄丹合成1个天命丹
        num = max(2, min(num, 20))
    except ValueError:
        msg = "请输入有效的数量(2-20)！"
        await handle_send(bot, event, msg)
        await fusion_destiny_pill.finish()
    
    # 检查渡厄丹数量
    back_msg = sql_message.get_back_msg(user_id)
    elixir_count = 0
    for item in back_msg:
        if item['goods_id'] == 1999:  # 渡厄丹ID
            elixir_count = item['goods_num']
            break
    
    if elixir_count < num:
        msg = f"融合需要{num}个渡厄丹，你只有{elixir_count}个！"
        await handle_send(bot, event, msg)
        await fusion_destiny_pill.finish()
    
    # 计算成功率（每个渡厄丹5%）
    success_rate = min(100, num * 5)  # 上限100%
    roll = random.randint(1, 100)
    
    if roll <= success_rate:  # 成功
        # 扣除渡厄丹
        sql_message.update_back_j(user_id, 1999, use_key=num)
        
        # 获得天命丹
        destiny_count = 1  # 成功固定获得1个
        sql_message.send_back(user_id, 1996, "天命丹", "丹药", destiny_count, 1)
        
        msg = (
            f"✨融合成功！消耗{num}个渡厄丹获得1个天命丹✨"
        )
    else:  # 失败
        # 扣除渡厄丹
        sql_message.update_back_j(user_id, 1999, num)
        
        msg = (
            f"融合失败！消耗了{num}个渡厄丹\n"
            f"当前成功率：{success_rate}%\n"
            f"（每颗渡厄丹提供5%成功率，20颗必成功）"
        )
    
    await handle_send(bot, event, msg)
    await fusion_destiny_pill.finish()

@fusion_destiny_tribulation_pill.handle(parameterless=[Cooldown(at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """融合天命渡劫丹"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await fusion_destiny_tribulation_pill.finish()
    
    user_id = user_info['user_id']
    args = args.extract_plain_text().strip()
    
    # 解析数量参数
    try:
        num = int(args) if args else 2  # 默认2个天命丹合成1个天命渡劫丹
        num = max(2, min(num, 10))
    except ValueError:
        msg = "请输入有效的数量(2-10)！"
        await handle_send(bot, event, msg)
        await fusion_destiny_tribulation_pill.finish()
    
    # 检查天命丹数量
    back_msg = sql_message.get_back_msg(user_id)
    elixir_count = 0
    for item in back_msg:
        if item['goods_id'] == 1996:  # 天命丹ID
            elixir_count = item['goods_num']
            break
    
    if elixir_count < num:
        msg = f"融合需要{num}个天命丹，你只有{elixir_count}个！\n请发送【融合天命丹】获得"
        await handle_send(bot, event, msg)
        await fusion_destiny_tribulation_pill.finish()
    
    # 计算成功率（每个天命丹10%）
    success_rate = min(100, num * 10)  # 上限100%
    roll = random.randint(1, 100)
    
    if roll <= success_rate:  # 成功
        # 扣除天命丹
        sql_message.update_back_j(user_id, 1996, use_key=num)
        
        # 获得天命渡劫丹
        destiny_count = 1  # 成功固定获得1个
        sql_message.send_back(user_id, 1997, "天命渡劫丹", "丹药", destiny_count, 1)
        
        msg = (
            f"✨融合成功！消耗{num}个天命丹获得1个天命渡劫丹✨"
        )
    else:  # 失败
        # 扣除天命丹
        sql_message.update_back_j(user_id, 1996, num)
        
        msg = (
            f"融合失败！消耗了{num}个天命丹\n"
            f"当前成功率：{success_rate}%\n"
            f"（每颗天命丹提供10%成功率，10颗必成功）"
        )
    
    await handle_send(bot, event, msg)

@start_tribulation.handle(parameterless=[Cooldown(at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """开始渡劫"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await start_tribulation.finish()
    
    user_id = user_info['user_id']
    tribulation_data = get_user_tribulation_info(user_id)
    
    # 检查冷却时间
    if tribulation_data['last_time']:
        last_time = datetime.strptime(tribulation_data['last_time'], '%Y-%m-%d %H:%M:%S.%f')
        cd = OtherSet().date_diff(datetime.now(), last_time)
        if cd < XiuConfig().tribulation_cd:
            remaining = XiuConfig().tribulation_cd - cd
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            msg = f"渡劫冷却中，还需{hours}小时{minutes}分钟！"
            await handle_send(bot, event, msg)
            await start_tribulation.finish()
    
    # 检查境界是否可以渡劫
    level_name = user_info['level']
    levels = convert_rank('江湖好手')[1]
    current_index = levels.index(level_name)
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) < levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}无需渡劫，请使用【突破】指令！"
        await handle_send(bot, event, msg)
        await start_tribulation.finish()

    if current_index == 0:  # 已经是最高境界
        msg = "道友已是至高境界，无需渡劫！"
        await handle_send(bot, event, msg)
        await start_tribulation.finish()
    
    next_level = levels[current_index + 1]
    next_level_data = jsondata.level_data()[next_level]
    current_exp = int(user_info['exp'])
    required_exp = int(next_level_data['power'])
    
    # 检查渡劫条件：境界圆满且修为达标
    if not level_name.endswith('圆满'):
        msg = f"当前境界：{user_info['level']}\n道友境界尚未圆满，无法渡劫！"
        await handle_send(bot, event, msg)
        await start_tribulation.finish()
    if not (current_exp >= required_exp):
        remaining_exp = max(0, required_exp - current_exp)
        msg = (
            f"渡劫条件不足！\n"
            f"当前境界：{level_name}\n"
            f"下一境界：{next_level}\n"
            f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
            f"还需修为：{number_to(remaining_exp)}\n"
            f"════════════\n"
            f"请继续修炼，待修为足够后再来渡劫！"
        )
        await handle_send(bot, event, msg)
        await start_tribulation.finish()
    
    # 检查是否有天命丹
    has_destiny_pill = False
    back = sql_message.get_back_msg(user_id)
    for item in back:
        if item['goods_id'] == 1996:  # 天命丹ID
            has_destiny_pill = True
            break
    
    # 开始渡劫
    success_rate = tribulation_data['current_rate']
    roll = random.randint(1, 100)
    
    if roll <= success_rate:  # 渡劫成功
        sql_message.updata_level(user_id, next_level)
        sql_message.update_power2(user_id)
        clear_user_tribulation_info(user_id)
        
        msg = (
            f"⚡⚡⚡渡劫成功⚡⚡⚡️\n"
            f"历经九九雷劫，道友终成{next_level}！\n"
            f"当前境界：{next_level}"
        )
    else:  # 渡劫失败
        if has_destiny_pill:  # 使用天命丹避免概率降低
            sql_message.update_back_j(user_id, 1996, use_key=1)
            msg = (
                f"渡劫失败！\n"
                f"雷劫之下，道心受损！\n"
                f"幸得天命丹护体，下次渡劫成功率保持：{success_rate}%"
            )
        else:
            new_rate = max(
                success_rate - 10, 
                XiuConfig().tribulation_base_rate
            )
            
            tribulation_data['current_rate'] = new_rate
            tribulation_data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            save_user_tribulation_info(user_id, tribulation_data)
            
            msg = (
                f"渡劫失败！\n"
                f"雷劫之下，道心受损！\n"
                f"下次渡劫成功率：{new_rate}%"
            )
    
    await handle_send(bot, event, msg)
    await start_tribulation.finish()

@destiny_tribulation.handle(parameterless=[Cooldown(at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """天命渡劫"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await destiny_tribulation.finish()
    
    user_id = user_info['user_id']
    tribulation_data = get_user_tribulation_info(user_id)
    
    # 检查冷却时间
    if tribulation_data['last_time']:
        last_time = datetime.strptime(tribulation_data['last_time'], '%Y-%m-%d %H:%M:%S.%f')
        cd = OtherSet().date_diff(datetime.now(), last_time)
        if cd < XiuConfig().tribulation_cd:
            hours = (XiuConfig().tribulation_cd - cd) // 3600
            minutes = ((XiuConfig().tribulation_cd - cd) % 3600) // 60
            msg = f"渡劫冷却中，还需{hours}小时{minutes}分钟！"
            await handle_send(bot, event, msg)
            await destiny_tribulation.finish()
    
    # 检查是否有天命渡劫丹
    back = sql_message.get_back_msg(user_id)
    has_item = False
    for item in back:
        if item['goods_id'] == 1997:
            has_item = True
            break
    
    if not has_item:
        msg = f"道友天命渡劫丹不足！\n请发送【融合天命渡劫丹】获得"
        await handle_send(bot, event, msg)
        await destiny_tribulation.finish()
    
    # 检查境界是否可以渡劫
    level_name = user_info['level']
    levels = convert_rank('江湖好手')[1]
    current_index = levels.index(level_name)
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) < levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}无需渡劫，请使用【突破】指令！"
        await handle_send(bot, event, msg)
        await destiny_tribulation.finish()

    if current_index == 0:  # 已经是最高境界
        msg = "道友已是至高境界，无需渡劫！"
        await handle_send(bot, event, msg)
        await destiny_tribulation.finish()
    
    next_level = levels[current_index + 1]
    next_level_data = jsondata.level_data()[next_level]
    current_exp = int(user_info['exp'])
    required_exp = int(next_level_data['power'])
    
    # 检查渡劫条件：境界圆满且修为达标
    if not level_name.endswith('圆满'):
        msg = f"当前境界：{user_info['level']}\n道友境界尚未圆满，无法渡劫！"
        await handle_send(bot, event, msg)
        await destiny_tribulation.finish()
    if not (current_exp >= required_exp):
        remaining_exp = max(0, required_exp - current_exp)
        msg = (
            f"渡劫条件不足！\n"
            f"当前境界：{level_name}\n"
            f"下一境界：{next_level}\n"
            f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
            f"还需修为：{number_to(remaining_exp)}\n"
            f"════════════\n"
            f"请继续修炼，待修为足够后再来渡劫！"
        )
        await handle_send(bot, event, msg)
        await destiny_tribulation.finish()
    
    # 使用天命渡劫丹
    sql_message.update_back_j(user_id, XiuConfig().tribulation_item_id, use_key=1)
    
    # 必定成功
    sql_message.updata_level(user_id, next_level)
    sql_message.update_power2(user_id)
    clear_user_tribulation_info(user_id)
    
    msg = (
        f"✨天命所归，渡劫成功✨\n"
        f"借助天命渡劫丹之力，道友轻松突破至{next_level}！\n"
        f"当前境界：{next_level}"
    )
    
    await handle_send(bot, event, msg)
    await destiny_tribulation.finish()

@heart_devil_tribulation.handle(parameterless=[Cooldown(at_sender=False)])
async def _(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """渡心魔劫"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await heart_devil_tribulation.finish()
    
    user_id = user_info['user_id']
    tribulation_data = get_user_tribulation_info(user_id)
    
    # 检查冷却时间
    if tribulation_data['last_time']:
        last_time = datetime.strptime(tribulation_data['last_time'], '%Y-%m-%d %H:%M:%S.%f')
        cd = OtherSet().date_diff(datetime.now(), last_time)
        if cd < XiuConfig().tribulation_cd:
            hours = (XiuConfig().tribulation_cd - cd) // 3600
            minutes = ((XiuConfig().tribulation_cd - cd) % 3600) // 60
            msg = f"渡劫冷却中，还需{hours}小时{minutes}分钟！"
            await handle_send(bot, event, msg)
            await heart_devil_tribulation.finish()
    
    # 检查境界是否可以渡劫
    level_name = user_info['level']
    levels = convert_rank('江湖好手')[1]
    current_index = levels.index(level_name)
   
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) < levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}无需渡劫，请使用【突破】指令！"
        await handle_send(bot, event, msg)
        await heart_devil_tribulation.finish()

    if current_index == 0:  # 已经是最高境界
        msg = "道友已是至高境界，无需渡劫！"
        await handle_send(bot, event, msg)
        await heart_devil_tribulation.finish()
    
    next_level = levels[current_index + 1]
    next_level_data = jsondata.level_data()[next_level]
    current_exp = int(user_info['exp'])
    required_exp = int(next_level_data['power'])
    
    # 检查渡劫条件：境界圆满且修为达标
    if not (current_exp >= required_exp):
        remaining_exp = max(0, required_exp - current_exp)
        msg = (
            f"渡劫条件不足！\n"
            f"当前境界：{level_name}\n"
            f"下一境界：{next_level}\n"
            f"当前修为：{number_to(current_exp)}/{number_to(required_exp)}\n"
            f"还需修为：{number_to(remaining_exp)}\n"
            f"════════════\n"
            f"请继续修炼，待修为足够后再来渡劫！"
        )
        await handle_send(bot, event, msg)
        await heart_devil_tribulation.finish()
    
    # 心魔类型和属性 - 现在包含正面、负面和中性的描述
    heart_devil_types = [
        {"name": "贪欲心魔", "scale": 0.01, 
         "win_desc": "战胜贪念，道心更加坚定", 
         "lose_desc": "贪念缠身，欲壑难填"},
        {"name": "嗔怒心魔", "scale": 0.02, 
         "win_desc": "化解怒火，心境更加平和", 
         "lose_desc": "怒火中烧，理智全失"},
        {"name": "痴妄心魔", "scale": 0.03, 
         "win_desc": "破除执念，心境更加通透", 
         "lose_desc": "执念深重，难以自拔"},
        {"name": "傲慢心魔", "scale": 0.04, 
         "win_desc": "克服傲慢，更加谦逊有礼", 
         "lose_desc": "目中无人，狂妄自大"},
        {"name": "嫉妒心魔", "scale": 0.05, 
         "win_desc": "消除妒火，心境更加宽广", 
         "lose_desc": "妒火中烧，心怀怨恨"},
        {"name": "恐惧心魔", "scale": 0.08, 
         "win_desc": "战胜恐惧，勇气倍增", 
         "lose_desc": "畏首畏尾，胆小如鼠"},
        {"name": "懒惰心魔", "scale": 0.1, 
         "win_desc": "克服懒惰，更加勤奋", 
         "lose_desc": "懈怠懒散，不思进取"},
        {"name": "七情心魔", "scale": 0.15, 
         "win_desc": "调和七情，心境更加平衡", 
         "lose_desc": "七情六欲，纷扰不休"},
        {"name": "六欲心魔", "scale": 0.2, 
         "win_desc": "超脱欲望，心境更加纯净", 
         "lose_desc": "欲望缠身，难以解脱"},
        {"name": "天魔幻象", "scale": 0.25, 
         "win_desc": "识破幻象，道心更加稳固", 
         "lose_desc": "天魔入体，幻象丛生"},
        {"name": "心魔劫主", "scale": 0.3, 
         "win_desc": "战胜心魔之主，道心大进", 
         "lose_desc": "心魔之主，万劫之源"}
    ]
    
    # 随机选择心魔类型
    devil_data = random.choice(heart_devil_types)
    devil_name = devil_data["name"]
    scale = devil_data["scale"]
    
    # 准备玩家数据
    player = sql_message.get_player_data(user_id)
    
    # 生成心魔属性
    devil_info = {
        "气血": int(player['气血'] * 100),
        "总血量": int(player['气血'] * scale),
        "真元": int(player['真元'] * scale),
        "攻击": int(player['攻击'] * scale),
        "name": devil_name,
        "jj": "感气境",
        "desc": devil_data["lose_desc"]  # 默认显示负面描述
    }
    
    # 执行战斗
    result, victor, _, _ = await Boss_fight(player, devil_info, type_in=1, bot_id=bot.self_id)
    
    if victor == "群友赢了":  # 战斗胜利
        new_rate = min(tribulation_data['current_rate'] + 20, XiuConfig().tribulation_max_rate)
        tribulation_data['current_rate'] = new_rate
        tribulation_data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        save_user_tribulation_info(user_id, tribulation_data)
        
        msg = (
            f"⚔️战胜{devil_name}，道心升华⚔️\n"
            f"{devil_data['win_desc']}\n"
            f"经过艰苦战斗，道友战胜了{devil_name}！\n"
            f"渡劫成功率提升至{new_rate}%！"
        )
    else:  # 战斗失败
        new_rate = max(tribulation_data['current_rate'] - 20, XiuConfig().tribulation_base_rate)
        tribulation_data['current_rate'] = new_rate
        tribulation_data['last_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        save_user_tribulation_info(user_id, tribulation_data)
        
        msg = (
            f"💀败于{devil_name}，道心受损💀\n"
            f"{devil_data['lose_desc']}\n"
            f"道友不敌{devil_name}，渡劫成功率降低至{new_rate}%！"
        )
    
    await send_msg_handler(bot, event, result)
    await handle_send(bot, event, msg)
    await heart_devil_tribulation.finish()

@level_up.handle(parameterless=[Cooldown(stamina_cost=1, at_sender=False)])
async def level_up_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """突破"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await level_up.finish()
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    user_leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    level_cd = user_msg['level_up_cd']
    if level_cd:
        # 校验是否存在CD
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)  # 获取second
        if cd < XiuConfig().level_up_cd * 60:
            # 如果cd小于配置的cd，返回等待时间
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 12, 1)
            await handle_send(bot, event, msg)
            await level_up.finish()
    else:
        pass

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg)
        await level_up.finish()

    level_rate = jsondata.level_rate_data()[level_name]  # 对应境界突破的概率
    user_backs = sql_message.get_back_msg(user_id)  # list(back)
    items = Items()
    pause_flag = False
    elixir_name = None
    elixir_desc = None
    if user_backs is not None:
        for back in user_backs:
            if int(back['goods_id']) == 1999:  # 检测到有对应丹药
                pause_flag = True
                elixir_name = back['goods_name']
                elixir_desc = items.get_data_by_item_id(1999)['desc']
                break
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升，别忘了还有渡厄突破
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    if pause_flag:
        msg = f"由于检测到背包有丹药：{elixir_name}，效果：{elixir_desc}，突破已经准备就绪\n请发送 ，【渡厄突破】 或 【直接突破】来选择是否使用丹药突破！\n本次突破概率为：{level_rate + user_leveluprate + number}% "
        await handle_send(bot, event, msg)
        await level_up.finish()
    else:
        msg = f"由于检测到背包没有【渡厄丹】，突破已经准备就绪\n请发送，【直接突破】来突破！请注意，本次突破失败将会损失部分修为！\n本次突破概率为：{level_rate + user_leveluprate + number}% "
        await handle_send(bot, event, msg)
        await level_up.finish()

@level_up_zj.handle(parameterless=[Cooldown(at_sender=False)])
async def level_up_zj_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """直接突破"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await level_up_zj.finish()
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    level_cd = user_msg['level_up_cd']
    if level_cd:
        # 校验是否存在CD
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)  # 获取second
        if cd < XiuConfig().level_up_cd * 60:
            # 如果cd小于配置的cd，返回等待时间
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 6, 1)
            await handle_send(bot, event, msg)
            await level_up_zj.finish()
    else:
        pass

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg)
        await level_up_zj.finish()

    level_name = user_msg['level']  # 用户境界
    exp = user_msg['exp']  # 用户修为
    level_rate = jsondata.level_rate_data()[level_name]  # 对应境界突破的概率
    leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升，别忘了还有渡厄突破
    main_exp_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破扣修为减少
    exp_buff = main_exp_buff['exp_buff'] if main_exp_buff is not None else 0
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    le = OtherSet().get_type(exp, level_rate + leveluprate + number, level_name)
    if le == "失败":
        # 突破失败
        sql_message.updata_level_cd(user_id)  # 更新突破CD
        # 失败惩罚，随机扣减修为
        percentage = random.randint(
            XiuConfig().level_punishment_floor, XiuConfig().level_punishment_limit
        )
        now_exp = int(int(exp) * ((percentage / 100) * (1 - exp_buff))) #功法突破扣修为减少
        sql_message.update_j_exp(user_id, now_exp)  # 更新用户修为
        nowhp = user_msg['hp'] - (now_exp / 2) if (user_msg['hp'] - (now_exp / 2)) > 0 else 1
        nowmp = user_msg['mp'] - now_exp if (user_msg['mp'] - now_exp) > 0 else 1
        sql_message.update_user_hp_mp(user_id, nowhp, nowmp)  # 修为掉了，血量、真元也要掉
        update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
            level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
        sql_message.update_levelrate(user_id, leveluprate + update_rate)
        msg = f"道友突破失败,境界受损,修为减少{number_to(now_exp)}，下次突破成功率增加{update_rate}%，道友不要放弃！"
        await handle_send(bot, event, msg)
        await level_up_zj.finish()

    elif type(le) == list:
        # 突破成功
        sql_message.updata_level(user_id, le[0])  # 更新境界
        sql_message.update_power2(user_id)  # 更新战力
        sql_message.updata_level_cd(user_id)  # 更新CD
        sql_message.update_levelrate(user_id, 0)
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        msg = f"恭喜道友突破{le[0]}成功！"
        await handle_send(bot, event, msg)
        await level_up_zj.finish()
    else:
        # 最高境界
        msg = le
        await handle_send(bot, event, msg)
        await level_up_zj.finish()

@level_up_lx.handle(parameterless=[Cooldown(stamina_cost=15, at_sender=False)])  # 连续突破消耗15体力
async def level_up_lx_continuous(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """连续突破5次"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await level_up_lx.finish()
    
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        sql_message.update_user_hp(user_id)
    
    user_msg = sql_message.get_user_info_with_id(user_id)
    level_cd = user_msg['level_up_cd']
    
    # 检查突破CD
    if level_cd:
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)
        if cd < XiuConfig().level_up_cd * 60:
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 6, 1)
            await handle_send(bot, event, msg)
            await level_up_lx.finish()

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg)
        await level_up_lx.finish()

    level_name = user_msg['level']
    exp = user_msg['exp']
    level_rate = jsondata.level_rate_data()[level_name]
    leveluprate = int(user_msg['level_up_rate'])
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()
    main_exp_buff = UserBuffDate(user_id).get_user_main_buff_data()
    exp_buff = main_exp_buff['exp_buff'] if main_exp_buff is not None else 0
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    
    success = False
    result_msg = ""
    attempts = 0
    
    for i in range(5):
        attempts += 1
        le = OtherSet().get_type(exp, level_rate + leveluprate + number, level_name)
        
        if isinstance(le, str):
            if le == "失败":
                # 突破失败
                percentage = random.randint(
                    XiuConfig().level_punishment_floor, XiuConfig().level_punishment_limit
                )
                now_exp = int(int(exp) * ((percentage / 100) * (1 - exp_buff)))
                sql_message.update_j_exp(user_id, now_exp)
                exp -= now_exp
                
                nowhp = user_msg['hp'] - (now_exp / 2) if (user_msg['hp'] - (now_exp / 2)) > 0 else 1
                nowmp = user_msg['mp'] - now_exp if (user_msg['mp'] - now_exp) > 0 else 1
                sql_message.update_user_hp_mp(user_id, nowhp, nowmp)
                
                update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                    level_rate * XiuConfig().level_up_probability)
                leveluprate += update_rate
                sql_message.update_levelrate(user_id, leveluprate)
                
                result_msg += f"第{attempts}次突破失败，修为减少{number_to(now_exp)}，下次突破成功率增加{update_rate}%\n"
            else:
                # 修为不足或已是最高境界
                result_msg += le
                break
        elif isinstance(le, list):
            # 突破成功
            sql_message.updata_level(user_id, le[0])
            sql_message.update_power2(user_id)
            sql_message.update_levelrate(user_id, 0)
            sql_message.update_user_hp(user_id)
            result_msg += f"第{attempts}次突破成功，达到{le[0]}境界！"
            success = True
            break
    
    if not success and attempts == 5 and "修为不足以突破" not in result_msg:
        result_msg += "连续5次突破尝试结束，未能突破成功。"
    
    sql_message.updata_level_cd(user_id)  # 更新突破CD
    await handle_send(bot, event, result_msg)
    await level_up_lx.finish()
    
@level_up_drjd.handle(parameterless=[Cooldown(stamina_cost=1, at_sender=False)])
async def level_up_drjd_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """渡厄 金丹 突破"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await level_up_drjd.finish()
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    level_cd = user_msg['level_up_cd']
    if level_cd:
        # 校验是否存在CD
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)  # 获取second
        if cd < XiuConfig().level_up_cd * 60:
            # 如果cd小于配置的cd，返回等待时间
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 4, 1)
            await handle_send(bot, event, msg)
            await level_up_drjd.finish()
    else:
        pass

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg)
        await level_up_drjd.finish()

    elixir_name = "渡厄金丹"
    level_name = user_msg['level']  # 用户境界
    exp = user_msg['exp']  # 用户修为
    level_rate = jsondata.level_rate_data()[level_name]  # 对应境界突破的概率
    user_leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    le = OtherSet().get_type(exp, level_rate + user_leveluprate + number, level_name)
    user_backs = sql_message.get_back_msg(user_id)  # list(back)
    pause_flag = False
    if user_backs is not None:
        for back in user_backs:
            if int(back['goods_id']) == 1998:  # 检测到有对应丹药
                pause_flag = True
                elixir_name = back['goods_name']
                break

    if not pause_flag:
        msg = f"道友突破需要使用{elixir_name}，但您的背包中没有该丹药！"
        sql_message.update_user_stamina(user_id, 4, 1)
        await handle_send(bot, event, msg)
        await level_up_drjd.finish()

    if le == "失败":
        # 突破失败
        sql_message.updata_level_cd(user_id)  # 更新突破CD
        if pause_flag:
            # 使用丹药减少的sql
            sql_message.update_back_j(user_id, 1998, use_key=1)
            now_exp = int(int(exp) * 0.1)
            sql_message.update_exp(user_id, now_exp)  # 渡厄金丹增加用户修为
            update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
            sql_message.update_levelrate(user_id, user_leveluprate + update_rate)
            msg = f"道友突破失败，但是使用了丹药{elixir_name}，本次突破失败不扣除修为反而增加了一成，下次突破成功率增加{update_rate}%！！"
        else:
            # 失败惩罚，随机扣减修为
            percentage = random.randint(
                XiuConfig().level_punishment_floor, XiuConfig().level_punishment_limit
            )
            main_exp_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破扣修为减少
            exp_buff = main_exp_buff['exp_buff'] if main_exp_buff is not None else 0
            now_exp = int(int(exp) * ((percentage / 100) * exp_buff))
            sql_message.update_j_exp(user_id, now_exp)  # 更新用户修为
            nowhp = user_msg['hp'] - (now_exp / 2) if (user_msg['hp'] - (now_exp / 2)) > 0 else 1
            nowmp = user_msg['mp'] - now_exp if (user_msg['mp'] - now_exp) > 0 else 1
            sql_message.update_user_hp_mp(user_id, nowhp, nowmp)  # 修为掉了，血量、真元也要掉
            update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
            sql_message.update_levelrate(user_id, user_leveluprate + update_rate)
            msg = f"没有检测到{elixir_name}，道友突破失败,境界受损,修为减少{number_to(now_exp)}，下次突破成功率增加{update_rate}%，道友不要放弃！"
        await handle_send(bot, event, msg)
        await level_up_drjd.finish()

    elif type(le) == list:
        # 突破成功
        sql_message.updata_level(user_id, le[0])  # 更新境界
        sql_message.update_power2(user_id)  # 更新战力
        sql_message.updata_level_cd(user_id)  # 更新CD
        sql_message.update_levelrate(user_id, 0)
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        now_exp = int(int(exp) * 0.1)
        sql_message.update_exp(user_id, now_exp)  # 渡厄金丹增加用户修为
        msg = f"恭喜道友突破{le[0]}成功，因为使用了渡厄金丹，修为也增加了一成！！"
        await handle_send(bot, event, msg)
        await level_up_drjd.finish()
    else:
        # 最高境界
        msg = le
        await handle_send(bot, event, msg)
        await level_up_drjd.finish()


@level_up_dr.handle(parameterless=[Cooldown(stamina_cost=2, at_sender=False)])
async def level_up_dr_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """渡厄 突破"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await level_up_dr.finish()
    user_id = user_info['user_id']
    if user_info['hp'] is None:
        # 判断用户气血是否为空
        sql_message.update_user_hp(user_id)
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    level_cd = user_msg['level_up_cd']
    if level_cd:
        # 校验是否存在CD
        time_now = datetime.now()
        cd = OtherSet().date_diff(time_now, level_cd)  # 获取second
        if cd < XiuConfig().level_up_cd * 60:
            # 如果cd小于配置的cd，返回等待时间
            msg = f"目前无法突破，还需要{XiuConfig().level_up_cd - (cd // 60)}分钟"
            sql_message.update_user_stamina(user_id, 8, 1)
            await handle_send(bot, event, msg)
            await level_up_dr.finish()
    else:
        pass

    level_name = user_msg['level']  # 用户境界
    levels = convert_rank('江湖好手')[1]
    
    # 检查是否需要渡劫
    if level_name.endswith('圆满') and levels.index(level_name) >= levels.index(XiuConfig().tribulation_min_level):
        msg = f"道友当前境界{level_name}需要渡劫才能突破，请使用【渡劫】指令！"
        await handle_send(bot, event, msg)
        await level_up_dr.finish()

    elixir_name = "渡厄丹"
    level_name = user_msg['level']  # 用户境界
    exp = user_msg['exp']  # 用户修为
    level_rate = jsondata.level_rate_data()[level_name]  # 对应境界突破的概率
    user_leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升
    number = main_rate_buff['number'] if main_rate_buff is not None else 0
    le = OtherSet().get_type(exp, level_rate + user_leveluprate + number, level_name)
    user_backs = sql_message.get_back_msg(user_id)  # list(back)
    pause_flag = False
    if user_backs is not None:
        for back in user_backs:
            if int(back['goods_id']) == 1999:  # 检测到有对应丹药
                pause_flag = True
                elixir_name = back['goods_name']
                break
    
    if not pause_flag:
        msg = f"道友突破需要使用{elixir_name}，但您的背包中没有该丹药！"
        sql_message.update_user_stamina(user_id, 8, 1)
        await handle_send(bot, event, msg)
        await level_up_dr.finish()

    if le == "失败":
        # 突破失败
        sql_message.updata_level_cd(user_id)  # 更新突破CD
        if pause_flag:
            # todu，丹药减少的sql
            sql_message.update_back_j(user_id, 1999, use_key=1)
            update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
            sql_message.update_levelrate(user_id, user_leveluprate + update_rate)
            msg = f"道友突破失败，但是使用了丹药{elixir_name}，本次突破失败不扣除修为下次突破成功率增加{update_rate}%，道友不要放弃！"
        else:
            # 失败惩罚，随机扣减修为
            percentage = random.randint(
                XiuConfig().level_punishment_floor, XiuConfig().level_punishment_limit
            )
            main_exp_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破扣修为减少
            exp_buff = main_exp_buff['exp_buff'] if main_exp_buff is not None else 0
            now_exp = int(int(exp) * ((percentage / 100) * (1 - exp_buff)))
            sql_message.update_j_exp(user_id, now_exp)  # 更新用户修为
            nowhp = user_msg['hp'] - (now_exp / 2) if (user_msg['hp'] - (now_exp / 2)) > 0 else 1
            nowmp = user_msg['mp'] - now_exp if (user_msg['mp'] - now_exp) > 0 else 1
            sql_message.update_user_hp_mp(user_id, nowhp, nowmp)  # 修为掉了，血量、真元也要掉
            update_rate = 1 if int(level_rate * XiuConfig().level_up_probability) <= 1 else int(
                level_rate * XiuConfig().level_up_probability)  # 失败增加突破几率
            sql_message.update_levelrate(user_id, user_leveluprate + update_rate)
            msg = f"没有检测到{elixir_name}，道友突破失败,境界受损,修为减少{number_to(now_exp)}，下次突破成功率增加{update_rate}%，道友不要放弃！"
        await handle_send(bot, event, msg)
        await level_up_dr.finish()

    elif type(le) == list:
        # 突破成功
        sql_message.updata_level(user_id, le[0])  # 更新境界
        sql_message.update_power2(user_id)  # 更新战力
        sql_message.updata_level_cd(user_id)  # 更新CD
        sql_message.update_levelrate(user_id, 0)
        sql_message.update_user_hp(user_id)  # 重置用户HP，mp，atk状态
        msg = f"恭喜道友突破{le[0]}成功"
        await handle_send(bot, event, msg)
        await level_up_dr.finish()
    else:
        # 最高境界
        msg = le
        await handle_send(bot, event, msg)
        await level_up_dr.finish()
        

@user_leveluprate.handle(parameterless=[Cooldown(at_sender=False)])
async def user_leveluprate_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的突破概率"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await user_leveluprate.finish()
    user_id = user_info['user_id']
    user_msg = sql_message.get_user_info_with_id(user_id)  # 用户信息
    leveluprate = int(user_msg['level_up_rate'])  # 用户失败次数加成
    level_name = user_msg['level']  # 用户境界
    level_rate = jsondata.level_rate_data()[level_name]  # 
    main_rate_buff = UserBuffDate(user_id).get_user_main_buff_data()#功法突破概率提升
    number =  main_rate_buff['number'] if main_rate_buff is not None else 0
    msg = f"道友下一次突破成功概率为{level_rate + leveluprate + number}%"
    await handle_send(bot, event, msg)
    await user_leveluprate.finish()


@user_stamina.handle(parameterless=[Cooldown(at_sender=False)])
async def user_stamina_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """我的体力信息"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await user_stamina.finish()
    msg = f"当前体力：{user_info['user_stamina']}"
    await handle_send(bot, event, msg)
    await user_stamina.finish()


@give_stone.handle(parameterless=[Cooldown(at_sender=False)])
async def give_stone_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """送灵石"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await give_stone.finish()
        
    user_id = user_info['user_id']
    user_stone_num = user_info['stone']
    hujiang_rank = convert_rank("江湖好手")[0]
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    
    if len(arg_list) < 2:
        msg = f"请输入正确的指令，例如：送灵石 少姜 600000"
        await handle_send(bot, event, msg)
        await give_stone.finish()
        
    stone_num = arg_list[1]  # 灵石数
    nick_name = arg_list[0]  # 道号
    
    if not stone_num.isdigit():
        msg = f"请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await give_stone.finish()
        
    give_stone_num = int(stone_num)
    
    # 计算发送方每日赠送上限（基础100000000 + 每境界20000000）
    user_rank = convert_rank(user_info['level'])[0]
    daily_send_limit = 100000000 + (hujiang_rank - user_rank) * 20000000
    
    # 检查发送方今日已送额度
    already_sent = stone_limit.get_send_limit(user_id)
    remaining_send = daily_send_limit - already_sent
    
    if give_stone_num > remaining_send:
        msg = f"道友今日已送{number_to(already_sent)}灵石，还可赠送{number_to(remaining_send)}灵石！"
        await handle_send(bot, event, msg)
        await give_stone.finish()
        
    if give_stone_num > int(user_stone_num):
        msg = f"道友的灵石不够，请重新输入！"
        await handle_send(bot, event, msg)
        await give_stone.finish()

    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
            
    if give_qq:
        if str(give_qq) == str(user_id):
            msg = f"请不要送灵石给自己！"
            await handle_send(bot, event, msg)
            await give_stone.finish()
            
        give_user = sql_message.get_user_info_with_id(give_qq)
        if give_user:
            # 检查接收方每日接收上限（同样计算）
            receiver_rank = convert_rank(give_user['level'])[0]
            daily_receive_limit = 100000000 + (hujiang_rank - receiver_rank) * 20000000
            
            already_received = stone_limit.get_receive_limit(give_qq)
            remaining_receive = daily_receive_limit - already_received
            
            if give_stone_num > remaining_receive:
                msg = f"{give_user['user_name']}道友今日已收{number_to(already_received)}灵石，还可接收{number_to(remaining_receive)}灵石！"
                await handle_send(bot, event, msg)
                await give_stone.finish()
                
            # 执行赠送
            sql_message.update_ls(user_id, give_stone_num, 2)  # 减少用户灵石
            give_stone_num2 = int(give_stone_num * 0.1)
            num = int(give_stone_num) - give_stone_num2
            sql_message.update_ls(give_qq, num, 1)  # 增加用户灵石
            
            # 更新额度记录
            stone_limit.update_send_limit(user_id, give_stone_num)
            stone_limit.update_receive_limit(give_qq, num)
            
            msg = f"共赠送{number_to(give_stone_num)}枚灵石给{give_user['user_name']}道友！收取手续费{number_to(give_stone_num2)}枚"
            await handle_send(bot, event, msg)
            await give_stone.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await give_stone.finish()

    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            if give_message['user_name'] == user_info['user_name']:
                msg = f"请不要送灵石给自己！"
                await handle_send(bot, event, msg)
                await give_stone.finish()
                
            # 检查接收方每日接收上限
            receiver_rank = convert_rank(give_message['level'])[0]
            daily_receive_limit = 100000000 + (hujiang_rank - receiver_rank) * 20000000
            
            already_received = stone_limit.get_receive_limit(give_message['user_id'])
            remaining_receive = daily_receive_limit - already_received
            
            if give_stone_num > remaining_receive:
                msg = f"{give_message['user_name']}道友今日已收{number_to(already_received)}灵石，还可接收{number_to(remaining_receive)}灵石！"
                await handle_send(bot, event, msg)
                await give_stone.finish()
                
            # 执行赠送
            sql_message.update_ls(user_id, give_stone_num, 2)  # 减少用户灵石
            give_stone_num2 = int(give_stone_num * 0.1)
            num = int(give_stone_num) - give_stone_num2
            sql_message.update_ls(give_message['user_id'], num, 1)  # 增加用户灵石
            
            # 更新额度记录
            stone_limit.update_send_limit(user_id, give_stone_num)
            stone_limit.update_receive_limit(give_message['user_id'], num)
            
            msg = f"共赠送{number_to(give_stone_num)}枚灵石给{give_message['user_name']}道友！收取手续费{number_to(give_stone_num2)}枚"
            await handle_send(bot, event, msg)
            await give_stone.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await give_stone.finish()

    else:
        msg = f"未获到对方信息，请输入正确的道号！"
        await handle_send(bot, event, msg)
        await give_stone.finish()

# 偷灵石
@steal_stone.handle(parameterless=[Cooldown(stamina_cost = 10, at_sender=False)])
async def steal_stone_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await steal_stone.finish()
    user_id = user_info['user_id']
    steal_user = None
    steal_user_stone = None
    user_stone_num = user_info['stone']
    steal_qq = None  # 艾特的时候存到这里, 要偷的人
    coststone_num = XiuConfig().tou
    if int(coststone_num) > int(user_stone_num):
        msg = f"道友的偷窃准备(灵石)不足，请打工之后再切格瓦拉！"
        sql_message.update_user_stamina(user_id, 10, 1)
        await handle_send(bot, event, msg)
        await steal_stone.finish()
    for arg in args:
        if arg.type == "at":
            steal_qq = arg.data.get('qq', '')
        nick_name = args.extract_plain_text().split()[0]
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            steal_qq = give_message['user_id']
        else:
            steal_qq = "000000"
    if steal_qq:
        if steal_qq == user_id:
            msg = f"请不要偷自己刷成就！"
            sql_message.update_user_stamina(user_id, 10, 1)
            await handle_send(bot, event, msg)
            await steal_stone.finish()
        else:
            steal_user = sql_message.get_user_info_with_id(steal_qq)
            if steal_user:
                steal_user_stone = steal_user['stone']
                steal_user_stone = min(steal_user_stone, 10000000)
            else:
                steal_user is None
    if steal_user:
        steal_success = random.randint(0, 100)
        result = OtherSet().get_power_rate(user_info['power'], steal_user['power'])
        if isinstance(result, int):
            if int(steal_success) > result:
                sql_message.update_ls(user_id, coststone_num, 2)  # 减少手续费
                sql_message.update_ls(steal_qq, coststone_num, 1)  # 增加被偷的人的灵石
                msg = f"道友偷窃失手了，被对方发现并被派去华哥厕所义务劳工！赔款{number_to(coststone_num)}灵石"
                await handle_send(bot, event, msg)
                await steal_stone.finish()
            get_stone = random.randint(int(XiuConfig().tou_lower_limit * steal_user_stone),
                                       int(XiuConfig().tou_upper_limit * steal_user_stone))
            if int(get_stone) > int(steal_user_stone):
                sql_message.update_ls(user_id, steal_user_stone, 1)  # 增加偷到的灵石
                sql_message.update_ls(steal_qq, steal_user_stone, 2)  # 减少被偷的人的灵石
                msg = f"{steal_user['user_name']}道友已经被榨干了~"
                await handle_send(bot, event, msg)
                await steal_stone.finish()
            else:
                sql_message.update_ls(user_id, get_stone, 1)  # 增加偷到的灵石
                sql_message.update_ls(steal_qq, get_stone, 2)  # 减少被偷的人的灵石
                msg = f"共偷取{steal_user['user_name']}道友{number_to(get_stone)}枚灵石！"
                await handle_send(bot, event, msg)
                await steal_stone.finish()
        else:
            msg = result
            await handle_send(bot, event, msg)
            await steal_stone.finish()
    else:
        msg = f"对方未踏入修仙界，不要对杂修出手！"
        await handle_send(bot, event, msg)
        await steal_stone.finish()


# GM加灵石
@gm_command.handle(parameterless=[Cooldown(at_sender=False)])
async def gm_command_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：神秘力量 灵石数量\n：神秘力量 道号 灵石数量"
        await handle_send(bot, event, msg)
        await gm_command.finish()
        
    if len(arg_list) < 2:
        stone_num = str(arg_list[0])  # 灵石数
        nick_name = None
    else:
        stone_num = arg_list[1]  # 灵石数
        nick_name = arg_list[0]  # 道号

    give_stone_num = stone_num
    # 遍历Message对象，寻找艾特信息
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data["qq"]
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
        if give_user:
            sql_message.update_ls(give_qq, give_stone_num, 1)  # 增加用户灵石
            msg = f"共赠送{number_to(int(give_stone_num))}枚灵石给{give_user['user_name']}道友！"
            await handle_send(bot, event, msg)
            await gm_command.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await gm_command.finish()
    else:
        sql_message.update_ls_all(give_stone_num)
        msg = f"全服通告：赠送所有用户{number_to(int(give_stone_num))}灵石,请注意查收！"
        await handle_send(bot, event, msg)
        enabled_groups = JsonConfig().get_enabled_groups()
        for group_id in enabled_groups:
            bot = get_bot()
            if int(group_id) == event.group_id:
                continue
            try:
                if XiuConfig().img:
                    pic = await get_msg_pic(msg)
                    await bot.send_group_msg(group_id=int(group_id), message=MessageSegment.image(pic))
                else:
                    await bot.send_group_msg(group_id=int(group_id), message=msg)
            except ActionFailed:  # 发送群消息失败
                continue
    await gm_command.finish()

# GM加思恋结晶
@ccll_command.handle(parameterless=[Cooldown(at_sender=False)])
async def ccll_command_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：传承力量 思恋结晶数量\n：传承力量 道号 思恋结晶数量"
        await handle_send(bot, event, msg)
        await ccll_command.finish()
        
    if len(arg_list) < 2:
        stone_num = str(arg_list[0])  # 思恋结晶数
        nick_name = None
    else:
        stone_num = arg_list[1]  # 思恋结晶数
        nick_name = arg_list[0]  # 道号

    give_stone_num = stone_num
    # 遍历Message对象，寻找艾特信息
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data["qq"]
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
        if give_user:
            xiuxian_impart.update_stone_num(give_stone_num, give_qq, 1)  # 增加用户思恋结晶
            msg = f"共赠送{number_to(int(give_stone_num))}枚思恋结晶给{give_user['user_name']}道友！"
            await handle_send(bot, event, msg)
            await ccll_command.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await ccll_command.finish()
    else:
        xiuxian_impart.update_impart_stone_all(give_stone_num)
        msg = f"全服通告：赠送所有用户{number_to(int(give_stone_num))}思恋结晶,请注意查收！"
        await handle_send(bot, event, msg)
        enabled_groups = JsonConfig().get_enabled_groups()
        for group_id in enabled_groups:
            bot = get_bot()
            if int(group_id) == event.group_id:
                continue
            try:
                if XiuConfig().img:
                    pic = await get_msg_pic(msg)
                    await bot.send_group_msg(group_id=int(group_id), message=MessageSegment.image(pic))
                else:
                    await bot.send_group_msg(group_id=int(group_id), message=msg)
            except ActionFailed:  # 发送群消息失败
                continue
    await ccll_command.finish()
    
@zaohua_xiuxian.handle(parameterless=[Cooldown(at_sender=False)])
async def zaohua_xiuxian_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：造化力量 道号 境界名"
        await handle_send(bot, event, msg)
        await zaohua_xiuxian.finish()
    if len(arg_list) < 2:
        jj_name = arg_list[0]
    else:
        jj_name = arg_list[1]
        
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
    else:
        give_user = sql_message.get_user_info_with_name(arg_list[0])
        give_qq = give_user['user_id']
    if give_user:
        level = jj_name
        if len(jj_name) == 5:
            level = jj_name
        elif len(jj_name) == 3:
            level = (jj_name + '圆满')
        if convert_rank(level)[0] is None:
            msg = f"境界错误，请输入正确境界名！"
            await handle_send(bot, event, msg)
            await zaohua_xiuxian.finish()
        max_exp = int(jsondata.level_data()[level]["power"])
        exp = give_user['exp']
        now_exp = exp - 100
        sql_message.update_j_exp(give_qq, now_exp) #重置用户修为
        sql_message.update_exp(give_qq, max_exp)  # 更新修为
        sql_message.updata_level(give_qq, level)  # 更新境界
        sql_message.update_user_hp(give_qq)  # 重置用户状态
        sql_message.update_power2(give_qq)  # 更新战力
        msg = f"{give_user['user_name']}道友的境界已变更为{level}！"
        await handle_send(bot, event, msg)
        await zaohua_xiuxian.finish()
    else:
        msg = f"对方未踏入修仙界，不可修改！"
        await handle_send(bot, event, msg)
        await zaohua_xiuxian.finish()
        
        
@cz.handle(parameterless=[Cooldown(at_sender=False)])
async def cz_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """创造力量"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if len(arg_list) < 2:
        msg = f"请输入正确指令！例如：创造力量 物品 数量\n创造力量 道号 物品 数量"
        await handle_send(bot, event, msg)
        await cz.finish()
        
    if len(arg_list) < 3:
        
        goods_num = arg_list[1]
        if goods_num.isdigit():
            goods_num = int(arg_list[1])
            goods_name = arg_list[0]
            nick_name = None
        else:
            goods_num = 1
            goods_name = arg_list[1]
            nick_name = arg_list[0]
    else:
        goods_num = int(arg_list[2])
        goods_name = arg_list[1]
        nick_name = arg_list[0]
    goods_id = None
    goods_type = None

    if goods_name.isdigit():  # 如果是纯数字，视为ID
        goods_id = int(goods_name)
        item_info = items.get_data_by_item_id(goods_id)
        if not item_info:
            msg = f"ID {goods_id} 对应的物品不存在，请检查输入！"
            await handle_send(bot, event, msg)
            await cz.finish()
    else:  # 视为物品名称
        for k, v in items.items.items():
            if goods_name == v['name']:
                goods_id = k
                goods_type = v['type']
                break
        if goods_id is None:
            msg = f"物品 {goods_name} 不存在，请检查名称是否正确！"
            await handle_send(bot, event, msg)
            await cz.finish()
            
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
        if give_user:
            sql_message.send_back(give_qq, goods_id, goods_name, goods_type, goods_num, 1)
            msg = f"{give_user['user_name']}道友获得了系统赠送的{goods_num}个{goods_name}！"
            await handle_send(bot, event, msg)
            await cz.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await cz.finish()
    
    all_users = sql_message.get_all_user_id()
    for user_id in all_users:
        sql_message.send_back(user_id, goods_id, goods_name, goods_type, goods_num, 1)  # 给每个用户发送物品
    msg = f"全服通告：赠送所有用户{goods_num}个{goods_name},请注意查收！"
    await handle_send(bot, event, msg)
    enabled_groups = JsonConfig().get_enabled_groups()
    for group_id in enabled_groups:
        bot = get_bot()
        if int(group_id) == event.group_id:
                continue
        try:
            if XiuConfig().img:
                pic = await get_msg_pic(msg)
                await bot.send_group_msg(group_id=int(group_id), message=MessageSegment.image(pic))
            else:
                await bot.send_group_msg(group_id=int(group_id), message=msg)
        except ActionFailed:  # 发送群消息失败
            continue
    await cz.finish()


#GM改灵根
@gmm_command.handle(parameterless=[Cooldown(at_sender=False)])
async def gmm_command_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if not args:
        msg = f"请输入正确指令！例如：轮回力量 道号 8(1为混沌,2为融合,3为超,4为龙,5为天,6为千世,7为万世,8为永恒,9为命运)"
        await handle_send(bot, event, msg)
        await gmm_command.finish()
    if len(arg_list) < 2:
        root_name_list = arg_list[0]
    else:
        root_name_list = arg_list[1]
        
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if give_qq:
        give_user = sql_message.get_user_info_with_id(give_qq)
    else:
        give_user = sql_message.get_user_info_with_name(arg_list[0])
        give_qq = give_user['user_id']
    if give_user:
        root_name = sql_message.update_root(give_qq, root_name_list)
        sql_message.update_power2(give_qq)
        msg = f"{give_user['user_name']}道友的灵根已变更为{root_name}！"
        await handle_send(bot, event, msg)
        await gmm_command.finish()
    else:
        msg = f"对方未踏入修仙界，不可修改！"
        await handle_send(bot, event, msg)
        await gmm_command.finish()


@rob_stone.handle(parameterless=[Cooldown(stamina_cost = 15, at_sender=False)])
async def rob_stone_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """抢劫
            player1 = {
            "NAME": player,
            "HP": player,
            "ATK": ATK,
            "COMBO": COMBO
        }"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await rob_stone.finish()
    user_id = user_info["user_id"]
    user_mes = sql_message.get_user_info_with_id(user_id)
    give_qq = None  # 艾特的时候存到这里
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    nick_name = args.extract_plain_text().split()[0]
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    player1 = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '爆伤': None, '防御': 0}
    player2 = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '爆伤': None, '防御': 0}
    user_2 = sql_message.get_user_info_with_id(give_qq)
    if user_mes and user_2:
        if user_info['root'] == "凡人":
            msg = f"目前职业无法抢劫！"
            sql_message.update_user_stamina(user_id, 15, 1)
            await handle_send(bot, event, msg)
            await rob_stone.finish()
       
        if give_qq:
            if str(give_qq) == str(user_id):
                msg = f"请不要抢自己刷成就！"
                sql_message.update_user_stamina(user_id, 15, 1)
                await handle_send(bot, event, msg)
                await rob_stone.finish()

            if user_2['root'] == "凡人":
                msg = f"对方职业无法被抢劫！"
                sql_message.update_user_stamina(user_id, 15, 1)
                await handle_send(bot, event, msg)
                await rob_stone.finish()

            is_type, msg = check_user_type(user_id, 0)  # 需要在无状态的用户
            if not is_type:
                await handle_send(bot, event, msg)
                await rob_stone.finish()
            is_type, msg = check_user_type(give_qq, 0)  # 需要在无状态的用户
            if not is_type:
                msg = "对方现在在闭关呢，无法抢劫！"
                await handle_send(bot, event, msg)
                await rob_stone.finish()
            if user_2:
                if user_info['hp'] is None:
                    # 判断用户气血是否为None
                    sql_message.update_user_hp(user_id)
                    user_info = sql_message.get_user_info_with_id(user_id)
                if user_2['hp'] is None:
                    sql_message.update_user_hp(give_qq)
                    user_2 = sql_message.get_user_info_with_id(give_qq)

                if user_2['hp'] <= user_2['exp'] / 10:
                    time_2 = leave_harm_time(give_qq)
                    msg = f"对方重伤藏匿了，无法抢劫！距离对方脱离生命危险还需要{time_2}分钟！"
                    sql_message.update_user_stamina(user_id, 15, 1)
                    await handle_send(bot, event, msg)
                    await rob_stone.finish()

                if user_info['hp'] <= user_info['exp'] / 10:
                    time_msg = leave_harm_time(user_id)
                    msg = f"重伤未愈，动弹不得！距离脱离生命危险还需要{time_msg}分钟！"
                    msg += f"请道友进行闭关，或者使用药品恢复气血，不要干等，没有自动回血！！！"
                    sql_message.update_user_stamina(user_id, 15, 1)
                    await handle_send(bot, event, msg)
                    await rob_stone.finish()
                    
                impart_data_1 = xiuxian_impart.get_user_impart_info_with_id(user_id)
                player1['user_id'] = user_info['user_id']
                player1['道号'] = user_info['user_name']
                player1['气血'] = user_info['hp']
                player1['攻击'] = user_info['atk']
                player1['真元'] = user_info['mp']
                player1['会心'] = int(
                    (0.01 + impart_data_1['impart_know_per'] if impart_data_1 is not None else 0) * 100)
                player1['爆伤'] = int(
                    1.5 + impart_data_1['impart_burst_per'] if impart_data_1 is not None else 0)
                user_buff_data = UserBuffDate(user_id)
                user_armor_data = user_buff_data.get_user_armor_buff_data()
                if user_armor_data is not None:
                    def_buff = int(user_armor_data['def_buff'])
                else:
                    def_buff = 0
                player1['防御'] = def_buff

                impart_data_2 = xiuxian_impart.get_user_impart_info_with_id(user_2['user_id'])
                player2['user_id'] = user_2['user_id']
                player2['道号'] = user_2['user_name']
                player2['气血'] = user_2['hp']
                player2['攻击'] = user_2['atk']
                player2['真元'] = user_2['mp']
                player2['会心'] = int(
                    (0.01 + impart_data_2['impart_know_per'] if impart_data_2 is not None else 0) * 100)
                player2['爆伤'] = int(
                    1.5 + impart_data_2['impart_burst_per'] if impart_data_2 is not None else 0)
                user_buff_data = UserBuffDate(user_2['user_id'])
                user_armor_data = user_buff_data.get_user_armor_buff_data()
                if user_armor_data is not None:
                    def_buff = int(user_armor_data['def_buff'])
                else:
                    def_buff = 0
                player2['防御'] = def_buff

                result, victor = OtherSet().player_fight(player1, player2)
                await send_msg_handler(bot, event, '决斗场', bot.self_id, result)
                if victor == player1['道号']:
                    foe_stone = user_2['stone']
                    foe_stone = min(foe_stone, 10000000)
                    if foe_stone > 0:
                        sql_message.update_ls(user_id, int(foe_stone * 0.1), 1)
                        sql_message.update_ls(give_qq, int(foe_stone * 0.1), 2)
                        exps = int(user_2['exp'] * 0.005)
                        sql_message.update_exp(user_id, exps)
                        sql_message.update_j_exp(give_qq, exps / 2)
                        msg = f"大战一番，战胜对手，获取灵石{number_to(foe_stone * 0.1)}枚，修为增加{number_to(exps)}，对手修为减少{number_to(exps / 2)}"
                        await handle_send(bot, event, msg)
                        await rob_stone.finish()
                    else:
                        exps = int(user_2['exp'] * 0.005)
                        sql_message.update_exp(user_id, exps)
                        sql_message.update_j_exp(give_qq, exps / 2)
                        msg = f"大战一番，战胜对手，结果对方是个穷光蛋，修为增加{number_to(exps)}，对手修为减少{number_to(exps / 2)}"
                        await handle_send(bot, event, msg)
                        await rob_stone.finish()

                elif victor == player2['道号']:
                    mind_stone = user_info['stone']
                    mind_stone = min(mind_stone, 10000000)
                    if mind_stone > 0:
                        sql_message.update_ls(user_id, int(mind_stone * 0.1), 2)
                        sql_message.update_ls(give_qq, int(mind_stone * 0.1), 1)
                        exps = int(user_info['exp'] * 0.005)
                        sql_message.update_j_exp(user_id, exps)
                        sql_message.update_exp(give_qq, exps / 2)
                        msg = f"大战一番，被对手反杀，损失灵石{number_to(mind_stone * 0.1)}枚，修为减少{number_to(exps)}，对手获取灵石{number_to(mind_stone * 0.1)}枚，修为增加{number_to(exps / 2)}"
                        await handle_send(bot, event, msg)
                        await rob_stone.finish()
                    else:
                        exps = int(user_info['exp'] * 0.005)
                        sql_message.update_j_exp(user_id, exps)
                        sql_message.update_exp(give_qq, exps / 2)
                        msg = f"大战一番，被对手反杀，修为减少{number_to(exps)}，对手修为增加{number_to(exps / 2)}"
                        await handle_send(bot, event, msg)
                        await rob_stone.finish()

                else:
                    msg = f"发生错误，请检查后台！"
                    await handle_send(bot, event, msg)
                    await rob_stone.finish()

    else:
        msg = f"对方未踏入修仙界，不可抢劫！"
        await handle_send(bot, event, msg)
        await rob_stone.finish()


@restate.handle(parameterless=[Cooldown(at_sender=False)])
async def restate_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """重置用户状态。
    单用户：重置状态@xxx
    多用户：重置状态"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await restate.finish()
    give_qq = None  # 艾特的时候存到这里
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
    if not args:
        sql_message.restate()
        msg = f"所有用户信息重置成功！"
        await handle_send(bot, event, msg)
        await restate.finish()
    else:
        nick_name = args.extract_plain_text().split()[0]
    if nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            give_qq = give_message['user_id']
        else:
            give_qq = "000000"
    if give_qq:
        sql_message.restate(give_qq)
        msg = f"{give_qq}用户信息重置成功！"
        await handle_send(bot, event, msg)
        await restate.finish()
    else:
        msg = f"对方未踏入修仙界，不可抢劫！"
        await handle_send(bot, event, msg)
        await restate.finish()

@view_logs.handle(parameterless=[Cooldown(at_sender=False)])
async def view_logs_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """查看修仙日志"""
    args = args.extract_plain_text().split()
    date_str = None
    page = 1
    
    # 解析参数
    if len(args) >= 1:
        # 检查第一个参数是否是6位数字（日期格式yymmdd）
        if args[0].isdigit() and len(args[0]) == 6:
            date_str = args[0]
            # 如果有第二个参数且是数字，作为页码
            if len(args) >= 2 and args[1].isdigit():
                page = int(args[1])
        elif args[0].isdigit():
            # 如果只有一个数字参数，作为页码
            page = int(args[0])
    
    user_id = event.get_user_id()
    logs_data = get_logs(user_id, date_str=date_str, page=page)
    
    if not logs_data["logs"]:
        msg = "没有找到日志记录！"
        if "error" in logs_data:
            msg += f"\n错误：{logs_data['error']}"
        await handle_send(bot, event, msg)
        await view_logs.finish()
    
    # 构建日志消息
    date_display = date_str if date_str else datetime.now().strftime("%y%m%d")
    msg = [f"\n修仙日志 - {date_display}\n第{page}页/共{logs_data['total_pages']}页\n═════════════"]
    
    for log in logs_data["logs"]:
        msg.append(f"{log['timestamp']}\n{log['message']}\n═════════════")
    
    await send_msg_handler(bot, event, '修仙日志', bot.self_id, msg)
    await view_logs.finish()

@set_xiuxian.handle()
async def open_xiuxian_(bot: Bot, event: GroupMessageEvent):
    """群修仙开关配置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_msg = str(event.message)
    group_id = str(event.group_id)
    conf_data = JsonConfig().read_data()

    if "启用" in group_msg:
        if group_id not in conf_data["group"]:
            msg = "当前群聊修仙模组已启用，请勿重复操作！"
            await handle_send(bot, event, msg)
            await set_xiuxian.finish()
        JsonConfig().write_data(2, group_id)
        msg = "当前群聊修仙基础模组已启用，快发送 我要修仙 加入修仙世界吧！"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()

    elif "禁用" in group_msg:
        if group_id in conf_data["group"]:
            msg = "当前群聊修仙模组已禁用，请勿重复操作！"
            await handle_send(bot, event, msg)
            await set_xiuxian.finish()
        JsonConfig().write_data(1, group_id)
        msg = "当前群聊修仙基础模组已禁用！"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()
    else:
        msg = "指令错误，请输入：启用修仙功能/禁用修仙功能"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()
        

@set_private_chat.handle()
async def set_private_chat_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """私聊功能开关配置（管理员专用）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = str(event.message)
    conf_data = JsonConfig().read_data()

    if "启用" in msg:
        if conf_data["private_enabled"]:
            msg = "私聊修仙功能已启用，请勿重复操作！"
        else:
            JsonConfig().write_data(3)
            msg = "私聊修仙功能已启用，所有用户现在可以在私聊中使用修仙命令！"
    elif "禁用" in msg:
        if not conf_data["private_enabled"]:
            msg = "私聊修仙功能已禁用，请勿重复操作！"
        else:
            JsonConfig().write_data(4)
            msg = "私聊修仙功能已禁用，所有用户的私聊修仙功能已关闭！"
    else:
        msg = "指令错误，请输入：启用私聊功能/禁用私聊功能"

    await handle_send(bot, event, msg)
    await set_private_chat.finish()
    
@xiuxian_updata_level.handle(parameterless=[Cooldown(at_sender=False)])
async def xiuxian_updata_level_(bot: Bot, event: GroupMessageEvent):
    """将修仙1的境界适配到修仙2"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await xiuxian_updata_level.finish()
    level_dict={
        "搬血境":"感气境",
        "洞天境":"练气境",
        "化灵境":"筑基境",
        "铭纹境":"结丹境",
        "列阵境":"金丹境",
        "尊者境":"元神境",
        "神火境":"化神境",
        "真一境":"炼神境",
        "圣祭境":"返虚境",
        "天神境":"大乘境",
    }
    level = user_info['level']
    user_id = user_info['user_id']
    if level == "至高":
        level = "至高"
    else:
        try:
            level = level_dict.get(level[:3]) + level[-2:]
        except:
            level = level
    sql_message.updata_level(user_id=user_id,level_name=level)
    msg = '境界适配成功成功！'
    await handle_send(bot, event, msg)
    await xiuxian_updata_level.finish()

def generate_daohao():
    """生成严格控制在2-7实际汉字长度的道号系统（完整词库版）"""
    # 拼接符号库（不计入总字数）
    connectors = ['·', '-', '~', '丨', '※', '〓', '§']
    
    # 姓氏库（单姓、复姓和三字姓）
    family_names = {
        'single': [
            '李', '王', '张', '刘', '陈', '杨', '赵', '黄', '周', '吴',
            '玄', '玉', '清', '云', '风', '霜', '雪', '月', '星', '阳',
            '金', '木', '水', '火', '土', '阴', '阳', '乾', '坤', '艮',
            '神', '仙', '圣', '佛', '魔', '妖', '鬼', '邪', '煞', '冥',
            '天', '昊', '穹', '苍', '幽', '冥', '太', '上', '元', '始',
            '剑', '刀', '枪', '戟', '弓', '琴', '棋', '书', '画', '符'
        ],
        'double': [
            '轩辕', '上官', '欧阳', '诸葛', '司马', '皇甫', '司空', '东方', '南宫', '西门',
            '长孙', '宇文', '慕容', '司徒', '令狐', '澹台', '公冶', '申屠', '太史', '端木',
            '青松', '白石', '碧泉', '紫竹', '金枫', '玉梅', '寒潭', '幽兰', '流云', '飞雪',
            '惊雷', '暮雨', '晨露', '晚霞', '孤峰', '断崖', '古木', '残阳', '新月', '繁星',
            '九霄', '太虚', '凌霄', '玄天', '紫霄', '青冥', '碧落', '黄泉', '星河', '月华',
            '昆仑', '蓬莱', '方丈', '瀛洲', '岱舆', '员峤', '峨眉', '青城', '天山', '沧海'
        ],
        'triple': [
            '太乙玄', '九幽寒', '凌霄子', '紫阳君', '玄冥上', '青莲剑', '白虹贯', '金乌曜',
            '玉虚宫', '碧游仙', '黄泉路', '血煞魔', '噬魂妖', '夺魄鬼', '摄心怪', '炼尸精'
        ]
    }

    # 名字库（单字、双字和三字）
    given_names = {
        'single': [
            '子', '尘', '空', '灵', '虚', '真', '元', '阳', '明', '玄',
            '霄', '云', '风', '雨', '雪', '霜', '露', '霞', '雾', '虹',
            '剑', '刃', '锋', '芒', '光', '影', '气', '意', '心', '神',
            '丹', '药', '炉', '鼎', '火', '炎', '金', '玉', '玄', '灵',
            '佛', '禅', '法', '僧', '念', '定', '慧', '戒', '忍', '悟',
            '龙', '凤', '麟', '龟', '虎', '雀', '鹏', '蛟', '猿', '鹤'
        ],
        'double': [
            '太虚', '紫阳', '玄灵', '玉真', '无尘', '逍遥', '长生', '不老', '凌霄', '琼华',
            '妙法', '通玄', '悟真', '明心', '见性', '合道', '冲虚', '守一', '抱朴', '坐忘',
            '青锋', '寒光', '流影', '断水', '破岳', '斩龙', '诛邪', '戮仙', '天问', '无尘',
            '九转', '七返', '五气', '三花', '金丹', '玉液', '炉火', '鼎纹', '药王', '灵枢',
            '菩提', '明镜', '般若', '金刚', '罗汉', '菩萨', '佛陀', '禅心', '觉悟', '轮回',
            '青龙', '白虎', '朱雀', '玄武', '麒麟', '凤凰', '鲲鹏', '蛟龙', '仙鹤', '灵龟'
        ],
        'triple': [
            '太乙剑', '九幽火', '凌霄子', '紫阳君', '玄冥气', '青莲剑', '白虹贯', '金乌曜',
            '玉虚宫', '碧游仙', '黄泉路', '血煞魔', '噬魂妖', '夺魄鬼', '摄心怪', '炼尸精',
            '混元一', '两仪生', '三才立', '四象成', '五行转', '六合聚', '七星列', '八卦演',
            '九宫变', '十方界', '百炼钢', '千幻影', '万法归', '亿劫渡', '无量寿', '永恒道'
        ]
    }

    # 修饰词库（1-5字）
    modifiers = {
        'single': [
            '子', '君', '公', '仙', '圣', '尊', '王', '皇', '帝', '祖',
            '魔', '妖', '鬼', '怪', '精', '灵', '魅', '魍', '魉', '尸',
            '神', '佛', '道', '儒', '剑', '刀', '枪', '戟', '弓', '琴'
        ],
        'double': [
            '真人', '真君', '上仙', '金仙', '天君', '星君', '元君', '道君', '老祖', '天尊',
            '剑仙', '剑魔', '剑圣', '剑痴', '剑狂', '剑鬼', '剑妖', '剑神', '剑尊', '剑帝',
            '丹圣', '药尊', '炉仙', '鼎君', '火灵', '炎帝', '金仙', '玉女', '玄师', '灵童',
            '尊者', '罗汉', '菩萨', '佛陀', '禅师', '法师', '和尚', '头陀', '沙弥', '比丘',
            '妖王', '魔尊', '鬼帝', '怪皇', '精主', '灵母', '魅仙', '魍圣', '魉神', '尸祖'
        ],
        'triple': [
            '大罗仙', '混元子', '太乙尊', '玄天君', '紫霄神', '青冥主', '碧落仙', '黄泉使',
            '星河君', '月华主', '日曜神', '云海仙', '风雷尊', '霜雪神', '虹霓使', '霞光君',
            '昆仑仙', '蓬莱客', '方丈僧', '瀛洲使', '岱舆君', '员峤主', '峨眉仙', '青城道',
            '金刚身', '罗汉果', '菩提心', '般若智', '明王怒', '如来掌', '天魔舞', '血煞阵'
        ],
        'quad': [
            '太乙救苦', '混元无极', '玄天上帝', '紫霄雷帝', '青冥剑主', '碧落黄泉', '星河倒悬',
            '月华如水', '日曜中天', '云海翻腾', '风雷激荡', '霜雪漫天', '虹霓贯日', '霞光万道',
            '昆仑之巅', '蓬莱仙岛', '方丈神山', '瀛洲幻境', '金刚不坏', '罗汉金身', '菩提般若',
            '明王怒火', '如来神掌', '天魔乱舞', '血煞冲天', '幽冥鬼域', '黄泉路上', '九幽之主',
            '噬魂夺魄'
        ],
        'quint': [
            '太乙救苦天尊', '混元无极道君', '玄天荡魔大帝', '紫霄神雷真君', '青冥剑道至尊',
            '碧落黄泉主宰', '星河倒悬真仙', '月华如水仙子', '日曜中天神君', '云海翻腾老祖',
            '金刚不坏罗汉', '菩提般若菩萨', '明王怒火金刚', '如来神掌佛陀', '天魔乱舞魔尊',
            '血煞冲天妖王', '幽冥鬼域鬼帝', '黄泉路上阎君', '九幽之主冥王', '噬魂夺魄魔君'
        ]
    }

    # 选择修饰词类型（权重分配）
    mod_type = random.choices(
        ['single', 'double', 'triple', 'quad', 'quint'],
        weights=[20, 30, 25, 15, 10]
    )[0]
    modifier = random.choice(modifiers[mod_type])

    # 根据修饰词长度选择姓氏和名字
    if mod_type == 'quint':  # 5字修饰词特殊处理
        # 只能搭配单字姓或单字名
        if random.random() < 0.7:
            family_name = random.choice(family_names['single'])
            given_name = ""
        else:
            family_name = ""
            given_name = random.choice(given_names['single'])
    else:
        # 正常选择姓氏（单70%，复25%，三字5%）
        family_type = random.choices(
            ['single', 'double', 'triple'],
            weights=[70, 25, 5]
        )[0]
        family_name = random.choice(family_names[family_type])
        
        # 正常选择名字（单40%，双50%，三字10%）
        given_type = random.choices(
            ['single', 'double', 'triple'],
            weights=[40, 50, 10]
        )[0]
        given_name = random.choice(given_names[given_type])

    # 可选的拼接符号（30%概率添加）
    connector = random.choices(
        ['', random.choice(connectors)],
        weights=[70, 30]
    )[0]

    # 计算实际汉字长度（忽略连接符）
    def real_length(s):
        return len([c for c in s if c not in connectors])

    # 生成所有可能的结构选项（带权重）
    options = []

    # 1. 正向结构：姓[+连接符]+名[+连接符]+修饰词
    def add_option(parts, weight):
        s = connector.join(filter(None, parts))
        if 2 <= real_length(s) <= 7:
            options.append((s, weight))

    # 正向组合
    add_option([family_name, given_name, modifier], 25)  # 姓+名+修饰词
    add_option([family_name, modifier], 15)             # 姓+修饰词
    add_option([given_name, modifier], 15)              # 名+修饰词
    add_option([family_name, given_name], 10)          # 姓+名

    # 倒装组合（确保修饰词位置正确）
    add_option([modifier, given_name, family_name], 10)  # 修饰词+名+姓
    add_option([modifier, family_name], 8)               # 修饰词+姓
    add_option([modifier, given_name], 7)                # 修饰词+名

    # 单独使用（需长度2-7）
    if 2 <= len(modifier) <= 7:
        options.append((modifier, 5))  # 单独修饰词
    if family_name and given_name:
        add_option([family_name, given_name], 5)  # 姓+名（已添加，权重叠加）

    # 如果没有合适选项（理论上不会发生），返回保底结果
    if not options:
        return modifier[:7] if len(modifier) >= 2 else "道君"

    # 按权重随机选择
    daohao_list, weights = zip(*options)
    daohao = random.choices(daohao_list, weights=weights)[0]

    # 最终验证
    if not (2 <= real_length(daohao) <= 7):
        return generate_daohao()  # 重新生成
    
    return daohao

# 仙缘数据路径
XIANGYUAN_DATA_PATH = Path(__file__).parent / "xiangyuan_data"
XIANGYUAN_DATA_PATH.mkdir(parents=True, exist_ok=True)

def get_xiangyuan_data(group_id):
    """获取群仙缘数据"""
    file_path = XIANGYUAN_DATA_PATH / f"xiangyuan_{group_id}.json"
    try:
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
    except:
        pass
    return {"gifts": {}, "last_id": 1}

def save_xiangyuan_data(group_id, data):
    """保存群仙缘数据"""
    file_path = XIANGYUAN_DATA_PATH / f"xiangyuan_{group_id}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def parse_gift_items(items_str):
    """解析仙缘物品字符串（改进版，参考补偿系统）"""
    items_list = []
    for item_part in items_str.split(','):
        item_part = item_part.strip()
        if 'x' in item_part:
            item_id_or_name, quantity = item_part.split('x', 1)
            quantity = int(quantity)
        else:
            item_id_or_name = item_part
            quantity = 1
        
        # 处理灵石特殊物品
        if item_id_or_name == "灵石":
            items_list.append({
                "type": "stone",
                "id": "stone",
                "name": "灵石",
                "quantity": quantity if quantity > 0 else 1000000,
                "desc": f"获得 {number_to(quantity if quantity > 0 else 1000000)} 灵石"
            })
            continue
        
        # 尝试转换为物品ID
        goods_id = None
        if item_id_or_name.isdigit():  # 如果是数字，直接作为ID
            goods_id = int(item_id_or_name)
            item_info = items.get_data_by_item_id(goods_id)
            if not item_info:
                raise ValueError(f"物品ID {goods_id} 不存在")
        else:  # 否则作为物品名称查找
            for k, v in items.items.items():
                if item_id_or_name == v['name']:
                    goods_id = k
                    break
            if not goods_id:
                raise ValueError(f"物品 {item_id_or_name} 不存在")
        
        item_info = items.get_data_by_item_id(goods_id)
        items_list.append({
            "type": item_info['type'],
            "id": goods_id,
            "name": item_info['name'],
            "quantity": quantity,
            "desc": item_info['desc']
        })
    
    if not items_list:
        raise ValueError("未指定有效的仙缘物品")
    
    return items_list

def check_user_has_item(user_id, item_id, quantity):
    """检查用户是否有足够数量的物品（包含灵石检查）"""
    if item_id == "stone":  # 灵石特殊处理
        user_info = sql_message.get_user_info_with_id(user_id)
        return user_info['stone'] >= quantity
    
    back_msg = sql_message.get_back_msg(user_id)
    for item in back_msg:
        if str(item["goods_id"]) == str(item_id):
            available = item["goods_num"] - item["bind_num"]
            return available >= quantity
    return False

@give_xiangyuan.handle(parameterless=[Cooldown(at_sender=False)])
async def give_xiangyuan_(bot: Bot, event: GroupMessageEvent, args: Message = CommandArg()):
    """送仙缘"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await give_xiangyuan.finish()
    
    user_id = user_info["user_id"]
    group_id = str(event.group_id)
    args = args.extract_plain_text().split()
    
    if len(args) < 1:
        msg = "指令格式：送仙缘 物品1x数量,物品2x数量 [领取人数]\n示例：送仙缘 灵石x1000000,两仪心经x1 5"
        await handle_send(bot, event, msg)
        await give_xiangyuan.finish()
    
    # 解析物品和数量
    items_str = args[0]
    try:
        gift_items = parse_gift_items(items_str)
    except ValueError as e:
        msg = f"物品格式错误：{str(e)}"
        await handle_send(bot, event, msg)
        await give_xiangyuan.finish()
    
    # 解析领取人数
    receiver_count = None
    if len(args) > 1 and args[1].isdigit():
        receiver_count = int(args[1])
    
    # 检查物品有效性
    valid_items = []
    total_stone = 0
    
    hujiang_rank = convert_rank("江湖好手")[0]
    user_rank = convert_rank(user_info['level'])[0]
    
    # 计算发送方每日赠送上限（基础100000000 + 每境界20000000）
    daily_send_limit = 100000000 + (hujiang_rank - user_rank) * 20000000
    
    for item in gift_items:
        if item["type"] == "stone":  # 灵石特殊处理
            total_stone += item["quantity"]
            valid_items.append(item)
            continue
        
        # 检查背包是否有该物品
        if not check_user_has_item(user_id, item["id"], item["quantity"]):
            msg = f"背包中没有足够的 {item['name']}！"
            await handle_send(bot, event, msg)
            await give_xiangyuan.finish()
        
        valid_items.append(item)

        # 检查禁止交易的物品
        if str(item["id"]) in BANNED_ITEM_IDS:
            msg = f"物品 {item['name']} 禁止交易！"
            await handle_send(bot, event, msg)
            await give_xiangyuan.finish()

    # 检查灵石赠送额度
    if total_stone > 0:
        # 检查发送方今日已送额度
        already_sent = stone_limit.get_send_limit(user_id)
        remaining_send = daily_send_limit - already_sent
        
        if total_stone > remaining_send:
            msg = f"道友今日已送{number_to(already_sent)}灵石，还可赠送{number_to(remaining_send)}灵石！"
            await handle_send(bot, event, msg)
            await give_xiangyuan.finish()
        
        # 检查灵石是否足够
        if total_stone > int(user_info['stone']):
            msg = f"道友的灵石不够，请重新输入！"
            await handle_send(bot, event, msg)
            await give_xiangyuan.finish()
    
    # 确定领取人数
    if receiver_count is None:
        # 自动计算：最少为物品种类数，最多100
        receiver_count = max(len(valid_items), 2)
        receiver_count = min(receiver_count, 100)
    else:
        # 检查手动指定的人数是否合理
        receiver_count = max(receiver_count, max(len(valid_items), 2))
        receiver_count = min(receiver_count, 100)
    
    # 扣除物品
    for item in valid_items:
        if item["type"] != "stone":
            sql_message.update_back_j(user_id, item["id"], num=item["quantity"])
    
    # 扣除灵石并更新额度
    if total_stone > 0:
        sql_message.update_ls(user_id, total_stone, 2)
        stone_limit.update_send_limit(user_id, total_stone)
    
    # 创建仙缘记录
    xiangyuan_data = get_xiangyuan_data(group_id)
    xiangyuan_id = xiangyuan_data["last_id"]
    xiangyuan_data["last_id"] += 1
    
    xiangyuan_data["gifts"][str(xiangyuan_id)] = {
        "id": xiangyuan_id,
        "giver_id": user_id,
        "giver_name": user_info["user_name"],
        "items": valid_items,
        "receiver_count": receiver_count,
        "received": 0,
        "receivers": {},
        "create_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    save_xiangyuan_data(group_id, xiangyuan_data)
    
    # 构建消息
    msg = f"【{user_info['user_name']}】送出仙缘 #{xiangyuan_id}：\n"
    for item in valid_items:
        if item["type"] == "stone":
            msg += f"{item['name']} x{number_to(item['quantity'])}\n"
        else:
            msg += f"{item['name']} x{item['quantity']}\n"
    msg += f"可领取人数：{receiver_count}\n"
    msg += "同群道友可发送【抢仙缘】获取仙缘"
    
    await handle_send(bot, event, msg)
    await give_xiangyuan.finish()

@get_xiangyuan.handle(parameterless=[Cooldown(at_sender=False)])
async def get_xiangyuan_(bot: Bot, event: GroupMessageEvent):
    """抢仙缘"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await get_xiangyuan.finish()
    
    user_id = user_info["user_id"]
    group_id = str(event.group_id)
    
    # 获取仙缘数据
    xiangyuan_data = get_xiangyuan_data(group_id)
    if not xiangyuan_data["gifts"]:
        msg = "当前没有可领取的仙缘！"
        await handle_send(bot, event, msg)
        await get_xiangyuan.finish()
    
    # 过滤掉自己送的仙缘和已领完的仙缘
    available_gifts = []
    for gift_id, gift in xiangyuan_data["gifts"].items():
        if (gift["giver_id"] != user_id and 
            gift["received"] < gift["receiver_count"] and
            str(user_id) not in gift["receivers"]):
            available_gifts.append((gift_id, gift))
    
    if not available_gifts:
        msg = "没有可领取的仙缘了！"
        await handle_send(bot, event, msg)
        await get_xiangyuan.finish()
    
    # 随机选择一个仙缘
    selected_gift_id, selected_gift = random.choice(available_gifts)
    
    # 计算当前是第几个领取者
    current_receiver_num = selected_gift["received"] + 1
    total_receivers = selected_gift["receiver_count"]
    
    # 计算每个物品的剩余数量和权重
    items_to_distribute = []
    for item in selected_gift["items"]:
        # 计算已领取数量
        received = sum(
            v.get(item["name"], 0) for v in selected_gift["receivers"].values()
        )
        remaining = item["quantity"] - received
        
        if remaining > 0:
            # 设置基础权重（确保所有物品都有机会被选中）
            weight = 1
            
            # 根据物品类型调整权重（灵石物品权重更高）
            if item["name"] != "灵石":
                weight *= 2
            
            # 根据剩余比例调整权重（剩余比例越高权重越高）
            remaining_ratio = remaining / item["quantity"]
            weight *= (1 + remaining_ratio * 5)  # 放大剩余比例的影响
            
            items_to_distribute.append({
                "item": item,
                "remaining": remaining,
                "weight": weight
            })
    
    if not items_to_distribute:
        msg = "这个仙缘的物品已经被领完了！"
        await handle_send(bot, event, msg)
        await get_xiangyuan.finish()
    
    # 按权重随机选择要分配的物品
    weights = [item["weight"] for item in items_to_distribute]
    selected_item_data = random.choices(items_to_distribute, weights=weights, k=1)[0]
    selected_item = selected_item_data["item"]
    remaining = selected_item_data["remaining"]
    
    # 计算剩余领取人数
    remaining_receivers = total_receivers - selected_gift["received"]
    
    # 如果是最后一个领取者，分配所有剩余数量
    if current_receiver_num == total_receivers:
        amount = remaining
    else:
        # 计算基础分配数量（剩余数量除以剩余领取人数）
        base_amount = max(1, remaining // remaining_receivers)
        
        # 添加随机浮动（±20%的浮动范围）
        min_amount = max(1, int(base_amount * 0.8))
        max_amount = min(remaining, int(base_amount * 1.2))
        
        # 确保浮动后的数量不超过剩余数量
        max_amount = min(max_amount, remaining - (remaining_receivers - 1))
        
        # 随机生成实际分配数量
        amount = random.randint(min_amount, max_amount)
    
    # 发放奖励
    if selected_item["name"] == "灵石":
        sql_message.update_ls(user_id, amount, 1)
    else:
        sql_message.send_back(
            user_id,
            selected_item["id"],
            selected_item["name"],
            selected_item["type"],
            amount,
            1
        )
    
    # 记录领取信息
    if str(user_id) not in selected_gift["receivers"]:
        selected_gift["receivers"][str(user_id)] = {}
    selected_gift["receivers"][str(user_id)][selected_item["name"]] = (
        selected_gift["receivers"][str(user_id)].get(selected_item["name"], 0) + amount
    )
    selected_gift["received"] += 1
    
    # 更新数据
    xiangyuan_data["gifts"][selected_gift_id] = selected_gift
    save_xiangyuan_data(group_id, xiangyuan_data)
    
    # 构建消息
    if current_receiver_num == total_receivers:
        msg = f"恭喜【{user_info['user_name']}】获得大机缘：\n"
        msg += f"{selected_item['name']} x{amount}\n"
        msg += f"来自：{selected_gift['giver_name']}的仙缘 #{selected_gift['id']}\n"
        msg += "💫💫 最后一个有缘人，获得仙缘全部馈赠！"
    else:
        msg = f"恭喜【{user_info['user_name']}】抢到仙缘：\n"
        msg += f"{selected_item['name']} x{amount}\n"
        msg += f"来自：{selected_gift['giver_name']}的仙缘 #{selected_gift['id']}"
    
    await handle_send(bot, event, msg)
    await get_xiangyuan.finish()

@xiangyuan_list.handle(parameterless=[Cooldown(at_sender=False)])
async def xiangyuan_list_(bot: Bot, event: GroupMessageEvent):
    """仙缘列表"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    is_user, user_info, msg = check_user(event)
    if not is_user:
        await handle_send(bot, event, msg)
        await xiangyuan_list.finish()
    
    group_id = str(event.group_id)
    xiangyuan_data = get_xiangyuan_data(group_id)
    
    if not xiangyuan_data["gifts"]:
        msg = "当前没有仙缘可领取！"
        await handle_send(bot, event, msg)
        await xiangyuan_list.finish()
    
    # 构建消息
    msg = ["当前可领取的仙缘："]
    for gift_id, gift in xiangyuan_data["gifts"].items():
        if gift["received"] >= gift["receiver_count"]:
            continue
        
        item_list = []
        for item in gift["items"]:
            received = sum(
                v.get(item["name"], 0) for v in gift["receivers"].values()
            )
            remaining = item["quantity"] - received
            if remaining > 0:
                item_list.append(f"{item['name']} (剩{remaining}/{item['quantity']})")
        
        if item_list:
            msg.append(
                f"\n#{gift_id} 来自：{gift['giver_name']}\n"
                f"物品：{'、'.join(item_list)}\n"
                f"进度：{gift['received']}/{gift['receiver_count']}"
            )
    
    if len(msg) == 1:
        msg = ["所有仙缘都已被领取完毕！"]
    
    await handle_send(bot, event, "\n".join(msg))
    await xiangyuan_list.finish()

@clear_xiangyuan.handle(parameterless=[Cooldown(at_sender=False)])
async def clear_xiangyuan_(bot: Bot, event: GroupMessageEvent):
    """清空仙缘（管理员）"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_id = str(event.group_id)
    
    # 获取当前仙缘数据
    xiangyuan_data = get_xiangyuan_data(group_id)
    if not xiangyuan_data["gifts"]:
        msg = "当前没有仙缘可清空！"
        await handle_send(bot, event, msg)
        await clear_xiangyuan.finish()
    
    # 退还未领取的物品
    refund_count = 0
    for gift_id, gift in xiangyuan_data["gifts"].items():
        if gift["received"] < gift["receiver_count"]:
            # 计算剩余物品
            for item in gift["items"]:
                received = sum(
                    v.get(item["name"], 0) for v in gift["receivers"].values()
                )
                remaining = item["quantity"] - received
                
                if remaining > 0:
                    if item["name"] == "灵石":
                        # 退还灵石
                        sql_message.update_ls(gift["giver_id"], remaining, 1)
                    else:
                        # 退还物品
                        sql_message.send_back(
                            gift["giver_id"],
                            item["id"],
                            item["name"],
                            item["type"],
                            remaining,
                            1
                        )
                    refund_count += 1
    
    # 清空数据
    xiangyuan_data["gifts"] = {}
    save_xiangyuan_data(group_id, xiangyuan_data)
    
    msg = f"已清空所有仙缘，退还了{refund_count}件物品给原主人！"
    await handle_send(bot, event, msg)
    await clear_xiangyuan.finish()

@scheduler.scheduled_job("cron", hour=0, minute=0)
async def reset_xiangyuan_daily():
    """每日重置仙缘"""
    for file in XIANGYUAN_DATA_PATH.glob("xiangyuan_*.json"):
        group_id = file.stem.split("_")[1]
        xiangyuan_data = get_xiangyuan_data(group_id)
        
        if not xiangyuan_data["gifts"]:
            continue
        
        # 退还未领取的物品
        refund_count = 0
        for gift_id, gift in xiangyuan_data["gifts"].items():
            if gift["received"] < gift["receiver_count"]:
                # 计算剩余物品
                for item in gift["items"]:
                    received = sum(
                        v.get(item["name"], 0) for v in gift["receivers"].values()
                    )
                    remaining = item["quantity"] - received
                    
                    if remaining > 0:
                        if item["name"] == "灵石":
                            # 退还灵石
                            sql_message.update_ls(gift["giver_id"], remaining, 1)
                        else:
                            # 退还物品
                            sql_message.send_back(
                                gift["giver_id"],
                                item["id"],
                                item["name"],
                                item["type"],
                                remaining,
                                1
                            )
                        refund_count += 1
        
        # 清空数据
        xiangyuan_data["gifts"] = {}
        save_xiangyuan_data(group_id, xiangyuan_data)
        
        logger.info(f"仙缘系统：已为群{group_id}清空仙缘，退还了{refund_count}件物品")
