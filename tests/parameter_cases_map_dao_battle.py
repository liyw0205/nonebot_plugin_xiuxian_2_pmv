"""Exercise the registered dao battle matcher against isolated databases."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch


def _setup(probe):
    from nonebot_plugin_xiuxian_2.features.combat_settlement.migrations import (
        apply_dao_battle_operations,
        apply_dao_battle_record,
    )
    from nonebot_plugin_xiuxian_2.features.base.migrations import apply_base_stamina_operations
    from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
    from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema
    from nonebot_plugin_xiuxian_2.xiuxian import xiuxian_map

    game, player = probe.context.paths.game_db, probe.context.paths.player_db
    with DatabaseUnitOfWork(game) as uow:
        apply_platform_schema(uow)
        apply_base_stamina_operations(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_platform_schema(uow)
        apply_dao_battle_operations(uow)
        apply_dao_battle_record(uow)
    data = xiuxian_map._load_map_data()
    realm = xiuxian_map._all_realms(data)[0]
    heaven = xiuxian_map._heaven_names(data, realm)[0]
    nodes = xiuxian_map._nodes(data, realm, heaven)
    for uid, node in zip(("900001", "900002", "900003"), (nodes[0], nodes[0], nodes[1])):
        xiuxian_map.map_application.save_status(uid, realm, heaven, node["id"], [node["id"]])
    return xiuxian_map, player


async def _invoke(probe, module, number, *, roll=0.0, target="诊断对手", allow_exception=False):
    from types import SimpleNamespace

    probe.clear_cooldowns()
    random_source = SimpleNamespace(random=lambda: roll, randint=lambda low, high: low)
    with patch.object(module, "runtime_random", random_source):
        message = "论道切磋" if target is None else f"论道切磋 {target}"
        return await probe.command("论道切磋", message, number=number,
                                   allow_exception=allow_exception)


def _projection(probe, player):
    return (
        probe.rows(player, "SELECT user_id,total,win,lose FROM dao_record ORDER BY user_id"),
        probe.rows(player, "SELECT operation_id,payload FROM map_dao_battle_operations ORDER BY operation_id"),
    )


def register(case):
    @case("map-dao-receipt-ignore", "论道切磋", "合法道号", "在同节点且回执拒写",
          "失败无战绩和回执，解除故障后可恢复", group="map-dao-battle", outcome="recovery")
    async def receipt_ignore(probe):
        module, player = _setup(probe)
        probe.seed(player, "CREATE TRIGGER dao_fault BEFORE INSERT ON map_dao_battle_operations "
                           "BEGIN SELECT RAISE(IGNORE); END")
        reply = await _invoke(probe, module, 975101)
        projection = _projection(probe, player)
        assert projection == ([], []) and "胜者" not in reply, (reply, projection)
        probe.seed(player, "DROP TRIGGER dao_fault")
        retry = await _invoke(probe, module, 975101)
        assert "胜者：诊断道友" in retry, retry
        assert _projection(probe, player)[0] == [("900001", 1, 1, 0), ("900002", 1, 0, 1)]

    @case("map-dao-replayed-roll", "论道切磋", "同消息同道号", "第二次投递随机值反转",
          "重放返回原胜者且不再记录战绩或扣体力", group="map-dao-battle")
    async def replayed_roll(probe):
        module, player = _setup(probe)
        game = probe.context.paths.game_db
        first = await _invoke(probe, module, 975102)
        assert "胜者：诊断道友" in first, first
        projection = _projection(probe, player)
        user_stamina = probe.rows(game, "SELECT user_id,user_stamina FROM user_xiuxian ORDER BY user_id")
        second = await _invoke(probe, module, 975102, roll=0.999)
        assert "胜者：诊断道友" in second and _projection(probe, player) == projection, (first, second)
        assert probe.rows(game, "SELECT user_id,user_stamina FROM user_xiuxian ORDER BY user_id") == user_stamina
        conflict = await _invoke(probe, module, 975102, target="诊断管理")
        assert "操作号已用于不同内容" in conflict and _projection(probe, player) == projection, conflict

    @case("map-dao-record-ignore", "论道切磋", "合法道号", "发起方战绩 INSERT 被忽略",
          "具体原因回复且双人战绩/永久回执全回滚", group="map-dao-battle", outcome="recovery")
    async def challenger_record_ignore(probe):
        module, player = _setup(probe)
        probe.seed(player, "CREATE TRIGGER dao_record_fault BEFORE INSERT ON dao_record "
                           "WHEN NEW.user_id='900001' BEGIN SELECT RAISE(IGNORE); END")
        reply = await _invoke(probe, module, 975103)
        assert "发起方战绩" in reply and "论道战绩未能保存" in reply, reply
        assert _projection(probe, player) == ([], [])

    @case("map-dao-record-rewrite", "论道切磋", "合法道号", "对手战绩 AFTER INSERT 被改写",
          "实际投影不符时双方战绩/回执全回滚", group="map-dao-battle", outcome="recovery")
    async def target_record_rewrite(probe):
        module, player = _setup(probe)
        probe.seed(player, "CREATE TRIGGER dao_record_fault AFTER INSERT ON dao_record "
                           "WHEN NEW.user_id='900002' BEGIN UPDATE dao_record SET total=9 WHERE user_id=NEW.user_id; END")
        reply = await _invoke(probe, module, 975104)
        assert "对方" in reply and "论道战绩与结算结果不一致" in reply, reply
        assert _projection(probe, player) == ([], [])

    @case("map-dao-receipt-rewrite", "论道切磋", "合法道号", "永久回执 AFTER INSERT payload 被改写",
          "战绩与损坏回执一起回滚并保留原因", group="map-dao-battle", outcome="recovery")
    async def receipt_rewrite(probe):
        module, player = _setup(probe)
        probe.seed(player, "CREATE TRIGGER dao_receipt_fault AFTER INSERT ON map_dao_battle_operations "
                           "BEGIN UPDATE map_dao_battle_operations SET payload='{}' WHERE operation_id=NEW.operation_id; END")
        reply = await _invoke(probe, module, 975105)
        assert "论道回执" in reply and "论道回执与结算结果不一致" in reply, reply
        assert _projection(probe, player) == ([], [])

    @case("map-dao-abort-recovery", "论道切磋", "同消息重试", "战绩写入 ABORT 后解除触发器",
          "原 SQL 错误有定位回复，失败 ledger 可恢复且只结算一次", group="map-dao-battle", outcome="recovery")
    async def abort_recovery(probe):
        module, player = _setup(probe)
        probe.seed(player, "CREATE TRIGGER dao_record_fault BEFORE INSERT ON dao_record "
                           "BEGIN SELECT RAISE(ABORT,'forced dao record failure'); END")
        failed = await _invoke(probe, module, 975106, allow_exception=True)
        assert "定位号：" in failed and "内部异常" in failed, failed
        assert _projection(probe, player) == ([], [])
        probe.seed(player, "DROP TRIGGER dao_record_fault")
        recovered = await _invoke(probe, module, 975106)
        assert "胜者：诊断道友" in recovered, recovered
        assert _projection(probe, player)[0] == [("900001", 1, 1, 0), ("900002", 1, 0, 1)]

    @case("map-dao-random-and-position", "论道切磋", "无目标随机选择", "一个同节点目标和一个异节点玩家",
          "只记录实际同节点的一组双方结果", group="map-dao-battle")
    async def random_target(probe):
        module, player = _setup(probe)
        reply = await _invoke(probe, module, 975107, target=None)
        records = _projection(probe, player)[0]
        assert "胜者：" in reply and records == [("900001", 1, 1, 0), ("900002", 1, 0, 1)], (reply, records)
        assert _projection(probe, player)[1][0][0] == "map-dao:975107:900001"
        game = probe.context.paths.game_db
        stamina = probe.rows(game, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'")
        replay = await _invoke(probe, module, 975107, roll=0.999, target=None)
        assert "胜者：" in replay and _projection(probe, player)[0] == records
        assert probe.rows(game, "SELECT user_stamina FROM user_xiuxian WHERE user_id='900001'") == stamina
        assert probe.rows(player, "PRAGMA integrity_check") == [("ok",)]
        from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
        with DatabaseUnitOfWork(game, read_only=True) as uow:
            assert uow.query_one("PRAGMA integrity_check")["integrity_check"] == "ok"

    @case("map-dao-target-missing", "论道切磋", "不存在的道号", "同节点有其他道友但指定目标不存在",
          "返回目标未找到且不写双方战绩与永久回执", group="map-dao-battle", outcome="rejection")
    async def missing_named_target(probe):
        module, player = _setup(probe)
        reply = await _invoke(probe, module, 975108, target="不存在的道友")
        assert "附近未找到道友" in reply, reply
        assert _projection(probe, player) == ([], [])
