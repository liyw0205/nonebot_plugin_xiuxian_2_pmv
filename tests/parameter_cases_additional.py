"""Additional bounded semantic probes for ordinary non-asset commands."""

from tests.parameter_cases_assets import balances, backpack


def register(case):
    for command, query, marker in (
        ("排行榜", "战力排行榜", "战力"),
        ("通天塔商店", "通天塔商店 1", "通天塔商店"),
        ("历练商店", "历练商店 1", "历练商店"),
        ("我的体窍", "我的体窍 1", "体窍"),
    ):
        def build(command, query, marker):
            async def run(probe):
                before, stock = balances(probe), backpack(probe)
                text = await probe.command(command, query)
                assert marker in text and balances(probe) == before and backpack(probe) == stock, text
                probe.evidence.update(query=query, readonly=True)

            return run

        case(
            "additional-read-" + command,
            command,
            "真实查询参数",
            "注册玩家、真实配置和隔离迁移",
            "读取实际视图并保持资产不变",
            group="additional-player-parameters",
        )(build(command, query, marker))

    @case(
        "additional-read-修仙日志",
        "修仙日志",
        "yymmdd 日期与页码",
        "注册玩家并在隔离日志目录写入一条真实日志",
        "按日期和页码读取日志且资产不变",
        group="additional-player-parameters",
    )
    async def read_logs(probe):
        from datetime import datetime
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils.utils import log_message

        before, stock = balances(probe), backpack(probe)
        date = datetime.now().strftime("%y%m%d")
        log_message("900001", "参数路径诊断日志")
        query = f"修仙日志 {date} 1"
        text = await probe.command("修仙日志", query)
        assert "修仙日志" in text and "参数路径诊断日志" in text, text
        assert balances(probe) == before and backpack(probe) == stock
        probe.evidence.update(query=query, readonly=True, date_format="yymmdd")

    for command, query, markers in (
        ("拜师", "不存在的诊断对象", ("未找到", "不存在", "对象")),
        ("绑定道侣", "不存在的诊断对象", ("未找到", "不存在", "道侣")),
        ("师徒传功", "不存在的诊断对象", ("未找到", "不存在", "对象")),
        ("拜访道友", "不存在的诊断对象", ("未找到", "不存在", "洞府")),
        ("洞府种植", "青灵草种", ("尚未", "未识别", "种子")),
        ("洞府收获", "1", ("尚未", "灵田", "收获")),
        ("洞府施肥", "1", ("尚未", "灵田", "肥料")),
        ("洞府催熟", "1", ("尚未", "灵田", "催熟")),
        ("通天塔兑换", "999999 1", ("不存在", "物品ID", "商品", "兑换", "积分")),
        ("历练兑换", "999999 1", ("不存在", "商品", "兑换", "历练")),
        ("合成", "不存在的合成物 1", ("未找到", "合成")),
        ("领取任务奖励", "每日", ("没有", "未完成", "领取", "任务")),
        ("虚神界对决", "不存在的对手", ("不存在", "对手", "无法", "修士")),
        ("炼丹", "不存在的丹方 1", ("不存在", "配方", "丹方", "材料", "背包空空")),
    ):
        def build(command, query, markers):
            async def run(probe):
                before, stock = balances(probe), backpack(probe)
                text = await probe.command(command, command + " " + query)
                assert balances(probe) == before and backpack(probe) == stock, text
                assert any(marker in text for marker in markers), text
                probe.evidence.update(query=query, assets_unchanged=True)

            return run

        case(
            "additional-reject-" + command,
            command,
            "具体对象/编号参数",
            "注册玩家和隔离迁移，使用不存在或不满足前置的真实语法",
            "具体业务拒绝且资产不变",
            group="additional-player-parameters",
            outcome="rejection",
        )(build(command, query, markers))
