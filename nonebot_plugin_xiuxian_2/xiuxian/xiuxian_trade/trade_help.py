import re

from ..adapter_compat import Bot, GroupMessageEvent, PrivateMessageEvent
from ..on_compat import on_command
from ..xiuxian_utils.lay_out import Cooldown, assign_bot
from ..xiuxian_utils.utils import number_to, send_help_message
from . import auction_config


trade_help = on_command("交易帮助", aliases={"仙肆帮助", "鬼市帮助", "拍卖帮助"}, priority=8, block=True)


@trade_help.handle(parameterless=[Cooldown(cd_time=0)])
async def trade_help_(bot: Bot, event: GroupMessageEvent | PrivateMessageEvent):
    """交易系统帮助"""
    bot, _ = await assign_bot(bot=bot, event=event)
    message = str(event.message)

    rank_msg = r"[\u4e00-\u9fa5]+"
    message_keywords = re.findall(rank_msg, message)

    help_sections = {
        "仙肆": """
**仙肆（全服坊市）**
---
**寻宝与购入**
- 仙肆查看 [类型] [页码]
> 浏览全服坊市货架
  支持类型
  > 技能|装备|丹药|药材
- 仙肆购买 编号 [数量]
> 购入指定货品
- 仙肆快速购买 物品
> 自动匹配最低价，可快速购买5种物品

**挂售与打理**
- 仙肆上架 物品 金额 [数量]
> 将囊中之物挂入仙肆
  最低金额60万灵石，手续费10-30%
- 仙肆快速上架 物品 [金额]
> 快速上架10个物品
  自动匹配最低价，数量固定10个（或全部）
- 仙肆自动上架 类型 品阶 [数量]
> 批量上架
  示例
  > 仙肆自动上架 装备 通天
- 仙肆下架 物品名 [数量]
> 下架自己的物品（默认1个；同名多档按单价从低到高）
- 我的仙肆 [页码]
> 查看自己上架的物品
""".strip(),
        "鬼市": """
**鬼市**
---
- 鬼市存灵石 数量
> 将灵石暂存鬼市
- 鬼市取灵石 数量
> 取回灵石（或收暂存之费）
- 鬼市信息
> 查阅鬼市账户与买卖
- 鬼市求购 物品 价格 [数量]
> 张贴求购之帖
- 鬼市摆摊 物品 价格 [数量]
> 设摊售物
- 鬼市收摊
> 收摊，货归囊中
""".strip(),
        "拍卖": f"""
**拍卖行**
---
**观拍**
- 拍卖查看 [ID]
> 翻阅拍卖珍品
  无参数查看当前拍卖列表；加ID查看指定拍卖品详情

**举牌**
- 拍卖竞拍 ID 价格
> 举价争宝
  每次加价不得少于{number_to(auction_config.get_auction_rules()['min_bid_increment'])}灵石
  示例
  > 拍卖竞拍 123456 5000000

**送拍**
- 拍卖上架 物品名 底价
> 将宝物送入拍卖
  最低底价
  > {number_to(auction_config.get_auction_rules()['min_price'])}灵石
  每人最多上架{auction_config.get_auction_rules()['max_user_items']}件（仅限非拍卖期间）
- 拍卖下架 物品名
> 撤回拍卖品（仅在非拍卖期间可操作）
- 我的拍卖
> 查看已上架物品（等待拍卖）

**场次**
- 拍卖活动
> 查看当前拍卖活动规则和限时交易状态
- 拍卖信息
> 查看开启时间、当前状态等信息

**规矩**
> 自动拍卖时间：每日{auction_config.get_auction_schedule()['start_hour']}点{auction_config.get_auction_schedule()['start_minute']}分
> 持续时间：{auction_config.get_auction_schedule()['duration_hours']}小时
> 手续费：{int(auction_config.get_auction_rules()['fee_rate'] * 100)}%
""".strip(),
        "交易": """
**交易总览**
---
**分门指引**
- 仙肆帮助
> 全服坊市
- 鬼市帮助
> 鬼市暗坊
- 拍卖帮助
> 拍卖珍品

**规矩**
> 仙肆手续费：500万以下10%，500-1000万15%，1000-2000万20%，2000万以上30%。
""".strip(),
    }

    if not message_keywords:
        msg = help_sections["交易"]
    else:
        keyword = message_keywords[0]

        if "仙肆" in keyword:
            msg = help_sections["仙肆"]
        elif "鬼市" in keyword:
            msg = help_sections["鬼市"]
        elif "拍卖" in keyword or "拍卖会" in keyword:
            msg = help_sections["拍卖"]
        elif "全部" in keyword:
            msg = (
                help_sections["仙肆"] + "\n\n" +
                help_sections["鬼市"] + "\n\n" +
                help_sections["拍卖"]
            )
        elif "交易" in keyword:
            msg = help_sections["交易"]
        else:
            msg = "可用分类：\n"
            msg += "仙肆帮助 | 鬼市帮助 | 拍卖帮助 | 交易帮助\n"
            msg += "完整列表：交易帮助全部"

    await send_help_message(
        bot,
        event,
        msg,
        k1="仙肆",
        v1="仙肆帮助",
        k2="鬼市",
        v2="鬼市帮助",
        k3="拍卖",
        v3="拍卖帮助",
    )
    await trade_help.finish()
