import re
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
from ..xiuxian_utils.xiuxian2_handle import (
    XiuxianDateManage, XiuxianJsonDate, OtherSet, 
    UserBuffDate, XIUXIAN_IMPART_BUFF, leave_harm_time
)
from ..xiuxian_config import XiuConfig, JsonConfig, convert_rank
from ..xiuxian_utils.utils import (
    check_user,
    get_msg_pic, number_to,
    CommandObjectID,
    Txt2Img, send_msg_handler, handle_send
)
from ..xiuxian_utils.item_json import Items
items = Items()

# 定时任务
scheduler = require("nonebot_plugin_apscheduler").scheduler
cache_help = {}
cache_level_help = {}
cache_level1_help = {}
cache_level2_help = {}
sql_message = XiuxianDateManage()  # sql类
xiuxian_impart = XIUXIAN_IMPART_BUFF()

run_xiuxian = on_command("我要修仙", priority=8, block=True)
restart = on_fullmatch("重入仙途", priority=7, block=True)
sign_in = on_command("修仙签到", priority=13, block=True)
help_in = on_command("修仙帮助", priority=12, block=True)
rank = on_command("排行榜", aliases={"修仙排行榜", "灵石排行榜", "战力排行榜", "境界排行榜", "宗门排行榜"},
                  priority=7, block=True)
remaname = on_command("修仙改名", priority=5, block=True)
level_up = on_fullmatch("突破", priority=6, block=True)
level_up_dr = on_fullmatch("渡厄突破", priority=7, block=True)
level_up_drjd = on_command("渡厄金丹突破", aliases={"金丹突破"}, priority=7, block=True)
level_up_zj = on_command("直接突破", aliases={"破"}, priority=7, block=True)
give_stone = on_command("送灵石", priority=5, permission=GROUP, block=True)
steal_stone = on_command("偷灵石", aliases={"飞龙探云手"}, priority=4, permission=GROUP, block=True)
gm_command = on_command("神秘力量", permission=SUPERUSER, priority=10, block=True)
gmm_command = on_command("轮回力量", permission=SUPERUSER, priority=10, block=True)
ccll_command = on_command("传承力量", permission=SUPERUSER, priority=10, block=True)
cz = on_command('创造力量', permission=SUPERUSER, priority=15,block=True)
rob_stone = on_command("抢劫", aliases={"拿来吧你"}, priority=5, permission=GROUP, block=True)
restate = on_command("重置状态", permission=SUPERUSER, priority=12, block=True)
set_xiuxian = on_command("启用修仙功能", aliases={'禁用修仙功能'}, permission=GROUP and (SUPERUSER | GROUP_ADMIN | GROUP_OWNER), priority=5, block=True)
set_private_chat = on_command("启用私聊功能", aliases={'禁用私聊功能'}, permission=SUPERUSER, priority=5, block=True)
user_leveluprate = on_command('我的突破概率', aliases={'突破概率'}, priority=5, block=True)
user_stamina = on_command('我的体力', aliases={'体力'}, priority=5, block=True)
xiuxian_updata_level = on_fullmatch('修仙适配', priority=15, permission=GROUP, block=True)
xiuxian_uodata_data = on_fullmatch('更新记录', priority=15, permission=GROUP, block=True)
level_help = on_fullmatch("灵根帮助", priority=15, block=True)
level1_help = on_fullmatch("品阶帮助", priority=15, block=True)
level2_help = on_fullmatch("境界帮助", priority=15, block=True)

__xiuxian_notes__ = f"""
【修仙指令】✨
===========
🌟 核心功能
→ 启程修仙:发送"我要修仙"🏃
→ 状态查询:发送"我的修仙信息"📊
→ 每日签到:发送"修仙签到"📅
→ 突破境界:发送"突破"🚀
*支持"连续突破"五次
→ 灵石交互:送/偷/抢灵石+数量+道号💰
===========
🌈 角色养成
→ 修炼方式:闭关/出关/灵石出关/灵石修炼/双修🧘
→ 灵根重置:发送"重入仙途"（需10万灵石）💎
→ 功法体系:发送"境界/品阶/灵根帮助"📚
→ 轮回重修:发送"轮回重修帮助"🌀
===========
🏯 系统功能
→ 宗门体系:发送"宗门帮助"
→ 灵庄系统:发送"灵庄帮助"
→ 秘境探索:发送"秘境帮助"
→ 炼丹指南:发送"炼丹帮助"
→ 灵田管理:发送"灵田帮助"
===========
🎮 特色玩法
→ 世界BOSS:发送"世界boss帮助"👾
→ 仙缘奇遇:发送"仙途奇缘帮助"🌈
→ 物品合成:发送"合成帮助"🔧
→ 批量祈愿:发送"传承祈愿 1000"🙏
===========
⚙️ 系统设置
→ 修改道号:发送"修仙改名+道号"✏️
→ 灵根优化:发送"开启/关闭自动选择灵根"🤖
→ 悬赏任务:发送"悬赏令帮助"📜
→ 状态查看:发送"我的状态"📝
===========
🏆 排行榜单
修仙/灵石/战力/宗门/排行榜
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
           永恒道果
    轮回道果——异界
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
上品法器——下品法器
上品符器——下品符器
""".strip()

__level2_help__ = f"""
详情:
            --境界帮助--            
                江湖人
                  ↓
搬血境 → 洞天境 → 化灵境
铭纹境 → 列阵境 → 尊者境 
神火境 → 真一境 → 圣祭境
天神境 → 虚道境 → 斩我境 
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


@xiuxian_uodata_data.handle(parameterless=[Cooldown(at_sender=False)])
async def mix_elixir_help_(bot: Bot, event: GroupMessageEvent):
    """更新记录"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    msg = __xiuxian_updata_data__
    await handle_send(bot, event, msg)
    await xiuxian_uodata_data.finish() 


@run_xiuxian.handle(parameterless=[Cooldown(at_sender=False)])
async def run_xiuxian_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """加入修仙"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    user_id = event.get_user_id()
    user_name = (
        event.sender.card if event.sender.card else event.sender.nickname
    )  # 获取为用户名
    root, root_type = XiuxianJsonDate().linggen_get()  # 获取灵根，灵根类型
    rate = sql_message.get_root_rate(root_type)  # 灵根倍率
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
            msg = "耳边响起一个神秘人的声音：不要忘记仙途奇缘！!\n不知道怎么玩的话可以发送 修仙帮助 喔！！"
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
    result = sql_message.get_sign(user_id)
    msg = result
    try:
        await handle_send(bot, event, msg)
        await sign_in.finish()
    except ActionFailed:
        await sign_in.finish("修仙界网络堵塞，发送失败!", reply_message=True)


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

    state["user_id"] = user_info['user_id']  # 将用户信息存储在状态中

    linggen_options = []
    for _ in range(10):
        name, root_type = XiuxianJsonDate().linggen_get()
        linggen_options.append((name, root_type))

    linggen_list_msg = "\n".join([f"{i+1}. {name} ({root_type})" for i, (name, root_type) in enumerate(linggen_options)])
    msg = f"请从以下灵根中选择一个:\n{linggen_list_msg}\n请输入对应的数字选择 (1-10):"
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
        msg = f"✨位面境界排行榜TOP100✨\n"
        num = 0
        for i in p_rank:
            num += 1
            msg += f"第{num}位 {i[0]} {i[1]},修为{number_to(i[2])}\n"
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message == "灵石排行榜":
        a_rank = sql_message.stone_top()
        msg = f"✨位面灵石排行榜TOP100✨\n"
        num = 0
        for i in a_rank:
            num += 1
            msg += f"第{num}位  {i[0]}  灵石：{number_to(i[1])}枚\n"
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message == "战力排行榜":
        c_rank = sql_message.power_top()
        msg = f"✨位面战力排行榜TOP100✨\n"
        num = 0
        for i in c_rank:
            num += 1
            msg += f"第{num}位  {i[0]}  战力：{number_to(i[1])}\n"
        await handle_send(bot, event, msg)
        await rank.finish()
    elif message in ["宗门排行榜", "宗门建设度排行榜"]:
        s_rank = sql_message.scale_top()
        msg = f"✨位面宗门建设排行榜TOP100✨\n"
        num = 0
        for i in s_rank:
            num += 1
            msg += f"第{num}位  {i[1]}  建设度：{number_to(i[2])}\n"
            if num == 100:
                break
        await handle_send(bot, event, msg)
        await rank.finish()


@remaname.handle(parameterless=[Cooldown(at_sender=False)])
async def remaname_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent, args: Message = CommandArg()):
    """修改道号"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    isUser, user_info, msg = check_user(event)
    if not isUser:
        await handle_send(bot, event, msg)
        await remaname.finish()
    user_id = user_info['user_id']
    user_name = args.extract_plain_text().strip()
    len_username = len(user_name.encode('gbk'))
    if len_username > 20:
        msg = "道号长度过长，请修改后重试！"
        await handle_send(bot, event, msg)
        await remaname.finish()
    elif len_username < 1:
        if XiuConfig().img:            
            msg = "道友确定要改名无名？还请三思。"
        await handle_send(bot, event, msg)            
        await remaname.finish()
    else:
        msg = sql_message.update_user_name(user_id, user_name)
        await handle_send(bot, event, msg)
        await remaname.finish()


@level_up.handle(parameterless=[Cooldown(stamina_cost=12, at_sender=False)])
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


@level_up_zj.handle(parameterless=[Cooldown(stamina_cost=3, at_sender=False)])
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
        msg = f"道友突破失败,境界受损,修为减少{now_exp}，下次突破成功率增加{update_rate}%，道友不要放弃！"
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
            msg = f"没有检测到{elixir_name}，道友突破失败,境界受损,修为减少{now_exp}，下次突破成功率增加{update_rate}%，道友不要放弃！"
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
            msg = f"没有检测到{elixir_name}，道友突破失败,境界受损,修为减少{now_exp}，下次突破成功率增加{update_rate}%，道友不要放弃！"
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
    give_qq = None  # 艾特的时候存到这里
    arg_list = args.extract_plain_text().split()
    if len(arg_list) < 2:
        msg = f"请输入正确的指令，例如：送灵石 少姜 600000"
        await handle_send(bot, event, msg)
        await give_stone.finish()
    stone_num = arg_list[1]  # 灵石数
    nick_name = arg_list[0]  # 道号
    if stone_num:
        pass
    else:
        msg = f"请输入正确的灵石数量！"
        await handle_send(bot, event, msg)
        await give_stone.finish()
    give_stone_num = stone_num
    if int(give_stone_num) > int(user_stone_num):
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
        else:
            give_user = sql_message.get_user_info_with_id(give_qq)
            if give_user:
                sql_message.update_ls(user_id, give_stone_num, 2)  # 减少用户灵石
                give_stone_num2 = int(give_stone_num) * 0.1
                num = int(give_stone_num) - int(give_stone_num2)
                sql_message.update_ls(give_qq, num, 1)  # 增加用户灵石
                msg = f"共赠送{number_to(int(give_stone_num))}枚灵石给{give_user['user_name']}道友！收取手续费{int(give_stone_num2)}枚"
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
            else:
                sql_message.update_ls(user_id, give_stone_num, 2)  # 减少用户灵石
                give_stone_num2 = int(give_stone_num) * 0.1
                num = int(give_stone_num) - int(give_stone_num2)
                sql_message.update_ls(give_message['user_id'], num, 1)  # 增加用户灵石
                msg = f"共赠送{number_to(int(give_stone_num))}枚灵石给{give_message['user_name']}道友！收取手续费{int(give_stone_num2)}枚"
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
            else:
                steal_user is None
    if steal_user:
        steal_success = random.randint(0, 100)
        result = OtherSet().get_power_rate(user_info['power'], steal_user['power'])
        if isinstance(result, int):
            if int(steal_success) > result:
                sql_message.update_ls(user_id, coststone_num, 2)  # 减少手续费
                sql_message.update_ls(steal_qq, coststone_num, 1)  # 增加被偷的人的灵石
                msg = f"道友偷窃失手了，被对方发现并被派去华哥厕所义务劳工！赔款{coststone_num}灵石"
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
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
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
    elif nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            sql_message.update_ls(give_message['user_id'], give_stone_num, 1)  # 增加用户灵石
            msg = f"共赠送{number_to(int(give_stone_num))}枚灵石给{give_message['user_name']}道友！"
            await handle_send(bot, event, msg)
            await gm_command.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await gm_command.finish()
    else:
        sql_message.update_ls_all(give_stone_num)
        msg = f"全服通告：赠送所有用户{give_stone_num}灵石,请注意查收！"
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
    for arg in args:
        if arg.type == "at":
            give_qq = arg.data.get("qq", "")
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
    elif nick_name:
        give_message = sql_message.get_user_info_with_name(nick_name)
        if give_message:
            xiuxian_impart.update_stone_num(give_stone_num, give_message['user_id'], 1)  # 增加用户思恋结晶
            msg = f"共赠送{number_to(int(give_stone_num))}枚思恋结晶给{give_message['user_name']}道友！"
            await handle_send(bot, event, msg)
            await ccll_command.finish()
        else:
            msg = f"对方未踏入修仙界，不可赠送！"
            await handle_send(bot, event, msg)
            await ccll_command.finish()
    else:
        xiuxian_impart.update_impart_stone_all(give_stone_num)
        msg = f"全服通告：赠送所有用户{give_stone_num}思恋结晶,请注意查收！"
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
    elif nick_name:
        give_user = sql_message.get_user_info_with_name(nick_name)
        if give_user:
            give_qq = give_user['user_id']
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
        msg = f"请输入正确指令！例如：轮回力量 道号 8(1为混沌,2为融合,3为超,4为龙,5为天,6为千世,7为万世,8为永恒)"
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
    player1 = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '爆伤': None, '防御': 0}
    player2 = {"user_id": None, "道号": None, "气血": None, "攻击": None, "真元": None, '会心': None, '爆伤': None, '防御': 0}
    user_2 = sql_message.get_user_info_with_id(give_qq)
    if user_mes and user_2:
        if user_info['root'] == "器师":
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

            if user_2['root'] == "器师":
                msg = f"对方职业无法被抢劫！"
                sql_message.update_user_stamina(user_id, 15, 1)
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
    if give_qq:
        sql_message.restate(give_qq)
        msg = f"{give_qq}用户信息重置成功！"
        await handle_send(bot, event, msg)
        await restate.finish()
    else:
        sql_message.restate()
        msg = f"所有用户信息重置成功！"
        await handle_send(bot, event, msg)
        await restate.finish()


@set_xiuxian.handle()
async def open_xiuxian_(bot: Bot, event: GroupMessageEvent):
    """群修仙开关配置"""
    bot, send_group_id = await assign_bot(bot=bot, event=event)
    group_msg = str(event.message)
    group_id = str(event.group_id)
    conf_data = JsonConfig().read_data()

    if "启用" in group_msg:
        if group_id in conf_data["group"]:
            msg = "当前群聊修仙模组已启用，请勿重复操作！"
            await handle_send(bot, event, msg)
            await set_xiuxian.finish()
        JsonConfig().write_data(1, group_id)
        msg = "当前群聊修仙基础模组已启用，快发送 我要修仙 加入修仙世界吧！"
        await handle_send(bot, event, msg)
        await set_xiuxian.finish()

    elif "禁用" in group_msg:
        if group_id not in conf_data["group"]:
            msg = "当前群聊修仙模组已禁用，请勿重复操作！"
            await handle_send(bot, event, msg)
            await set_xiuxian.finish()
        JsonConfig().write_data(2, group_id)
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
        "练气境":"搬血境",
        "筑基境":"洞天境",
        "结丹境":"化灵境",
        "元婴境":"铭纹境",
        "化神境":"列阵境",
        "炼虚境":"尊者境",
        "合体境":"神火境",
        "大乘境":"真一境",
        "渡劫境":"圣祭境",
        "半步真仙":"天神境中期",
        "真仙境":"虚道境",
        "金仙境":"斩我境",
        "太乙境":"遁一境"
    }
    level = user_info['level']
    user_id = user_info['user_id']
    if level == "半步真仙":
        level = "天神境中期"
    else:
        try:
            level = level_dict.get(level[:3]) + level[-2:]
        except:
            level = level
    sql_message.updata_level(user_id=user_id,level_name=level)
    msg = '境界适配成功成功！'
    await handle_send(bot, event, msg)
    await xiuxian_updata_level.finish()