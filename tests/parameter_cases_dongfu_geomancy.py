"""Real matcher coverage for the read-only cave geomancy view."""

import sqlite3
from unittest.mock import patch

from tests.parameter_cases_dongfu_status_view import _seed, _snapshot


def _set_type(probe, node_type):
    probe.seed(
        probe.context.paths.player_db,
        "UPDATE dongfu_status SET node_type=? WHERE user_id='900001'",
        (node_type,),
    )


async def _view(probe, number, *, command="洞府地脉", actor="900001", allow_exception=False):
    return await probe.command(
        command, command, number=number, actor=actor, event_type="private",
        allow_exception=allow_exception, timeout=30,
    )


def register(case):
    @case(
        "dongfu-geomancy-configured-types",
        "洞府地脉",
        "依次读取全部配置地势并通过地脉查看别名",
        "隔离玩家有效洞府，数据库node_type依次使用五种受支持类型",
        "返回对应地势效果和值，命令不写任何数据库",
        group="dongfu-geomancy",
    )
    async def configured_types(probe):
        _seed(probe)
        expected = {
            "水域": (
                "水润灵脉", "灵田成熟时间小幅缩短，灵息露催熟效果增强。",
                "成熟时间-8%、催熟额外+20分钟",
            ),
            "灵林": (
                "草木灵脉", "药材类灵田收获时有概率额外产出。",
                "成熟时间-5%、额外产出概率+25%",
            ),
            "仙山": (
                "仙山灵脉", "神种收获时有概率额外产出，普通灵田也略微加速。",
                "成熟时间-4%、神种额外产出概率+18%",
            ),
            "矿脉": (
                "玄矿地脉", "洞府布阵灵石消耗降低，高阶布阵少量节省玄铁阵石。",
                "布阵灵石-10%、高阶布阵阵石-1",
            ),
            "险地": (
                "险煞灵脉", "灵田收获更易额外产出，但洞府更容易被潜入得手。",
                "额外产出概率+35%、被潜入成功率+8%",
            ),
        }
        checks = []
        for index, (node_type, (name, description, values)) in enumerate(expected.items()):
            _set_type(probe, node_type)
            before = _snapshot(probe)
            command = "地脉查看" if index % 2 else "洞府地脉"
            reply = await _view(probe, 974100 + index, command=command)
            after = _snapshot(probe)
            assert f"地脉：{name}（节点类型：{node_type}）" in reply, reply
            assert f"效果：{description}" in reply, reply
            assert f"数值：{values}" in reply, reply
            assert after == before, f"{node_type} view wrote to a database"
            checks.append({"node_type": node_type, "reply": reply, "snapshot": after})
        probe.evidence["types"] = checks

    @case(
        "dongfu-geomancy-unknown-and-unbuilt",
        "洞府地脉",
        "未知地势和未建设玩家真实指令查询",
        "有效玩家node_type为未知旧值，另一个已注册玩家没有洞府行",
        "未知类型使用普通地势合同，未建设玩家不串读且查询零写",
        group="dongfu-geomancy",
    )
    async def unknown_and_unbuilt(probe):
        _seed(probe)
        _set_type(probe, "旧版遗留地貌")
        before = _snapshot(probe)
        unknown = await _view(probe, 974110)
        after_unknown = _snapshot(probe)
        assert "地脉：普通地势（节点类型：旧版遗留地貌）" in unknown, unknown
        assert "效果：无额外加成" in unknown, unknown
        assert after_unknown == before, "unknown geomancy view wrote to a database"

        unbuilt = await _view(probe, 974111, actor="900002")
        after_unbuilt = _snapshot(probe)
        assert "你尚未建设洞府。" in unbuilt, unbuilt
        assert "云宫" not in unbuilt and "旧版遗留地貌" not in unbuilt, unbuilt
        assert after_unbuilt == before, "unbuilt cave view wrote to a database"
        probe.evidence.update(
            before=before, after_unknown=after_unknown,
            after_unbuilt=after_unbuilt, unknown=unknown, unbuilt=unbuilt,
        )

    @case(
        "dongfu-geomancy-read-denied-recovery",
        "洞府地脉",
        "真实SELECT通过SQLite authorizer拒绝并解除后重放相同消息",
        "有效洞府状态读取node_type时返回SQLITE_DENY",
        "失败有定位回复且不伪装未建设；解除故障后同消息恢复有效地脉",
        group="dongfu-geomancy",
        outcome="rejection",
    )
    async def denied_read_recovers(probe):
        from nonebot_plugin_xiuxian_2.features.dongfu import status_repository

        _seed(probe)
        player = probe.context.paths.player_db
        before = _snapshot(probe)
        original_uow = status_repository.DatabaseUnitOfWork

        class DenyGeomancyReadUnitOfWork:
            def __init__(self, *args, **kwargs):
                self.delegate = original_uow(*args, **kwargs)

            def __enter__(self):
                active = self.delegate.__enter__()

                def authorize(action, table, column, database, trigger):
                    if (
                        action == sqlite3.SQLITE_READ
                        and table == "dongfu_status"
                        and column == "node_type"
                    ):
                        return sqlite3.SQLITE_DENY
                    return sqlite3.SQLITE_OK

                active.connection.set_authorizer(authorize)
                return active

            def __exit__(self, exc_type, exc, traceback):
                return self.delegate.__exit__(exc_type, exc, traceback)

        with patch.object(
            status_repository, "DatabaseUnitOfWork", DenyGeomancyReadUnitOfWork
        ):
            reply = await _view(probe, 974120, allow_exception=True)
        failed_call = probe.calls[-1]
        after_failure = _snapshot(probe)
        recovered = await _view(probe, 974120)
        after_recovery = _snapshot(probe)
        probe.evidence.update(
            player_db=str(player), before=before, after_failure=after_failure,
            after_recovery=after_recovery, reply=reply, recovered=recovered,
            errors=failed_call.get("errors", []),
            caught_exceptions=failed_call.get("caught_exceptions", []),
        )
        errors = "\n".join(failed_call.get("errors", []))
        assert "洞府地脉处理提示" in reply and "定位号" in reply, reply
        assert "你尚未建设洞府" not in reply, reply
        assert "access to dongfu_status.node_type is prohibited" in errors, failed_call
        assert "水润灵脉" in recovered, recovered
        assert after_failure == after_recovery == before, "read failure or recovery wrote data"
