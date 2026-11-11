"""Activity parameters use shipped names and real migrated state, with a local open window."""
from contextlib import contextmanager
from copy import deepcopy
import json
from unittest.mock import patch

from tests.parameter_cases_assets import balances, backpack


@contextmanager
def active_config():
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_activity import activity_config, service
    cfg = json.loads(activity_config.DEFAULT_CONFIG_PATH.read_text(encoding="utf-8"))
    cfg.update(enabled=True, start_time="0", end_time="无限")
    for entry in cfg["gameplay_activities"]:
        entry.update(enabled=True, start_time="0", end_time="无限")
    with patch.object(activity_config, "load_config", return_value=cfg), patch.object(service, "load_config", return_value=cfg):
        yield cfg, service


def points(probe, cfg):
    activity = next(row for row in cfg["gameplay_activities"] if row["type"] == "event_points")
    probe.seed(probe.context.paths.game_db,
               "INSERT INTO activity_point_balance(activity_key,user_id,points,total_points) VALUES(?,'900001',10000,10000)",
               (activity["key"],))
    return activity, activity["shop"][0]


def register(case):
    @case("activity-stage-claim-invalid", "活动阶段领取", "不存在的阶段名", "活动配置开启、虚构注册玩家",
          "具体阶段不存在拒绝，资产和阶段领取记录不变", group="activity-extended", outcome="rejection")
    async def stage_invalid(probe):
        with active_config():
            before, stock = balances(probe), backpack(probe)
            text = await probe.command("活动阶段领取", "活动阶段领取 不存在的阶段")
            assert before == balances(probe) and stock == backpack(probe), text
            assert any(token in text for token in ("不存在", "阶段", "领取")), text
            probe.evidence.update(assets_unchanged=True, invalid_stage=True)

    @case("activity-rank-query-real-category", "活动排行", "实际榜单分类", "活动配置开启、真实积分模型",
          "读取指定榜单分类，玩家资产不变", group="activity-extended")
    async def rank_query(probe):
        with active_config():
            before = balances(probe)
            text = await probe.command("活动排行", "活动排行 签到榜")
            assert "排行" in text and balances(probe) == before, text
            probe.evidence.update(category="签到榜", readonly=True)

    @case("activity-boss-invalid-item", "活动讨伐", "不存在的攻击道具名", "活动首领入口已注册、虚构玩家",
          "具体道具拒绝，不扣资源或写首领进度", group="activity-extended", outcome="rejection")
    async def boss_invalid(probe):
        with active_config():
            before, stock = balances(probe), backpack(probe)
            text = await probe.command("活动讨伐", "活动讨伐 不存在的爆竹")
            assert before == balances(probe) and stock == backpack(probe), text
            assert any(token in text for token in ("不存在", "道具", "活动")), text
            probe.evidence.update(assets_unchanged=True, invalid_item=True)

    @case("activity-purchase-real-name-count", "活动购买", "真实商品名 数量2", "开启随包积分活动、10000积分、真实商品限购内",
          "买2份、积分扣准确成本、灵石奖励/次数/回执入库，重放不重复", group="activity-parameters")
    async def purchase(probe):
        with active_config() as (cfg, service):
            activity, item = points(probe, cfg)
            before = balances(probe)
            text = await probe.command("活动购买", f"活动购买 {item['name']} 2", number=980001)
            after = balances(probe)
            reward = service.parse_reward(item["reward"])[0]["quantity"]
            assert after[0][1] - before[0][1] == reward * 2, text
            saved = probe.rows(probe.context.paths.game_db, "SELECT points FROM activity_point_balance WHERE activity_key=? AND user_id='900001'", (activity["key"],))
            assert saved == [(10000 - item["cost"] * 2,)], (text, saved)
            await probe.command("活动购买", f"活动购买 {item['name']} 2", number=980001)
            assert after == balances(probe) and saved == probe.rows(probe.context.paths.game_db, "SELECT points FROM activity_point_balance WHERE activity_key=? AND user_id='900001'", (activity["key"],))
            probe.evidence.update(activity_key=activity["key"], item_key=item["item_key"], cost=item["cost"] * 2, reward=reward * 2, replay_unchanged=True)

    for label, suffix in (("zero", "0"), ("negative", "-2"), ("bad", "abc"), ("extra", "1 extra"), ("huge", "9" * 350)):
        def build(suffix):
            async def run(probe):
                with active_config() as (cfg, service):
                    activity, item = points(probe, cfg)
                    before, stock = balances(probe), backpack(probe)
                    text = await probe.command("活动购买", f"活动购买 {item['name']} {suffix}")
                    assert before == balances(probe) and stock == backpack(probe), text
                    assert probe.rows(probe.context.paths.game_db, "SELECT points FROM activity_point_balance WHERE activity_key=?", (activity["key"],)) == [(10000,)]
                    probe.evidence.update(points_unchanged=True, assets_unchanged=True)
            return run
        case("activity-purchase-reject-" + label, "活动购买", "真实商品 " + label, "合法积分与商品，排除入口资格遮挡",
             "参数或超限拒绝，不扣积分或发奖", group="activity-parameters", outcome="rejection")(build(suffix))

    @case("activity-collect-phrase-name", "活动兑换", "真实词组名", "活动字牌满足真实词组需求",
          "扣除对应字牌、按词组发奖、兑换次数和回执入库", group="activity-parameters")
    async def collect(probe):
        from collections import Counter
        with active_config() as (cfg, service):
            activity = next(row for row in cfg["gameplay_activities"] if row["type"] == "collect_words")
            phrase = activity["phrases"][0]
            needs = Counter(phrase["phrase"])
            for char, count in needs.items():
                probe.seed(probe.context.paths.game_db, "INSERT INTO activity_collect_inventory(activity_key,user_id,word_char,count) VALUES(?,'900001',?,?)", (activity["key"], char, count))
            before = balances(probe)
            text = await probe.command("活动兑换", "活动兑换 " + phrase["name"], number=980002)
            assert "成功" in text and balances(probe) != before, text
            assert all(row[0] == 0 for row in probe.rows(probe.context.paths.game_db, "SELECT count FROM activity_collect_inventory WHERE user_id='900001'"))
            saved = balances(probe)
            await probe.command("活动兑换", "活动兑换 " + phrase["name"], number=980002)
            assert saved == balances(probe)
            probe.evidence.update(activity_key=activity["key"], phrase=phrase["phrase"], consumed=dict(needs), replay_unchanged=True)

    @case("activity-task-claim-name", "活动任务领取", "真实日任务名称", "真实日任务进度已达到目标、尚未领取",
          "只领指定任务、奖励与claimed入库，事件重放不重复", group="activity-parameters")
    async def tasks(probe):
        with active_config() as (cfg, service):
            task = cfg["daily_tasks"][0]
            key, scope = service._activity_config_key(cfg), service._task_scope_key("daily")
            probe.seed(probe.context.paths.game_db, "INSERT INTO activity_task_progress(activity_key,user_id,scope_type,scope_key,task_key,progress,target) VALUES(?,'900001','daily',?,?,?,?)", (key, scope, task["key"], task["target"], task["target"]))
            before = balances(probe)
            text = await probe.command("活动任务领取", "活动任务领取 " + task["name"], number=980003)
            assert "领取成功" in text and balances(probe) != before, text
            assert probe.rows(probe.context.paths.game_db, "SELECT claimed FROM activity_task_progress WHERE user_id='900001'") == [(1,)]
            saved = balances(probe)
            await probe.command("活动任务领取", "活动任务领取 " + task["name"], number=980003)
            assert saved == balances(probe)
            probe.evidence.update(task_key=task["key"], scope=scope, replay_unchanged=True)

    for form in ("level", "name"):
        def build(form):
            async def run(probe):
                with active_config() as (cfg, service):
                    key = service._activity_config_key(cfg)
                    reward = cfg["extensions"]["activity_pass"]["level_rewards"][0]
                    probe.seed(probe.context.paths.game_db, "INSERT INTO activity_pass_balance(activity_key,user_id,exp,total_exp,level) VALUES(?,'900001',0,200,2)", (key,))
                    query = str(reward["level"]) if form == "level" else reward["name"]
                    before = balances(probe)
                    text = await probe.command("活动战令领取", "活动战令领取 " + query)
                    assert "领取成功" in text and balances(probe) != before, text
                    assert probe.rows(probe.context.paths.game_db, "SELECT level FROM activity_pass_reward_claim WHERE user_id='900001'") == [(reward["level"],)]
                    probe.evidence.update(level=reward["level"], form=form, wallet_increased=True)
            return run
        case("activity-pass-claim-" + form, "活动战令领取", "真实档位或名称", "2级战令，等级1奖励未领",
             "只领取指定档位且奖励/领取表一致", group="activity-parameters")(build(form))
