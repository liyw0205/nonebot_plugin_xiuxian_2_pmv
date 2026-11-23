"""Validate the title catalog refresh through its registered player command."""

import json

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def register(case):
    @case("title-refresh-cache-validation", "刷新称号", "有效/坏/缺失/替换配置下缓存原子刷新",
          "真实matcher/DI/handler，隔离称号JSON与game/player SQLite",
          "错误配置不伪报成功且保留旧缓存；恢复有效配置后原命令刷新生效",
          group="title-refresh-parameters")
    async def refresh_cache_validation(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_title import title_data
        from nonebot.adapters.onebot.v11 import PrivateMessageEvent
        from unittest.mock import patch
        from tests import command_probe

        path = title_data.TITLE_JSONPATH
        assert path.resolve().is_relative_to(probe.context.paths.data_root.resolve()), path
        original = path.read_bytes()
        before = database_projection(probe)
        first_catalog = {
            "990001": {"name": "缓存验收甲", "desc": "第一份配置", "condition": "修仙签到>=1"},
        }
        second_catalog = {
            "990002": {"name": "缓存验收乙", "desc": "替换配置", "condition": "境界突破>=1"},
        }
        try:
            path.write_text(json.dumps(first_catalog, ensure_ascii=False), encoding="utf-8")
            probe.clear_cooldowns()
            first = await probe.command("刷新称号", "刷新称号", actor="900001", number=999820)
            assert "当前共有1个称号" in first, first
            assert title_data.get_all_titles() == first_catalog
            assert title_data.get_title_condition_keys() == {"修仙签到"}

            path.write_text("{invalid json", encoding="utf-8")
            probe.clear_cooldowns()
            invalid = await probe.command(
                "刷新称号", "刷新称号", actor="900001", number=999821,
                allow_exception=True,
            )
            assert "称号配置JSON格式错误" in invalid and "当前共有0个称号" not in invalid, invalid
            assert any("ujson.JSONDecodeError" in error and "Expected object or value" in error
                       for error in probe.calls[-1]["errors"])
            assert title_data.get_all_titles() == first_catalog
            assert title_data.get_title_condition_keys() == {"修仙签到"}

            path.write_text(json.dumps({"990003": "not-an-object"}), encoding="utf-8")
            probe.clear_cooldowns()
            invalid_shape = await probe.command(
                "刷新称号", "刷新称号", actor="900001", number=999822,
                allow_exception=True,
            )
            assert "称号配置JSON格式错误" in invalid_shape and "当前共有0个称号" not in invalid_shape
            assert title_data.get_all_titles() == first_catalog
            assert title_data.get_title_condition_keys() == {"修仙签到"}

            path.unlink()
            probe.clear_cooldowns()
            missing = await probe.command(
                "刷新称号", "刷新称号", actor="900001", number=999823,
                allow_exception=True,
            )
            assert "称号配置文件缺失" in missing and "当前共有0个称号" not in missing, missing
            assert title_data.get_all_titles() == first_catalog
            assert title_data.get_title_condition_keys() == {"修仙签到"}

            path.write_text(json.dumps(second_catalog, ensure_ascii=False), encoding="utf-8")
            probe.clear_cooldowns()
            make_group = command_probe.make_event

            def make_private(text, actor, number):
                data = make_group(text, actor, number).model_dump()
                data.pop("group_id")
                data.update(message_type="private", sub_type="friend")
                return PrivateMessageEvent.model_validate(data)

            with patch.object(command_probe, "make_event", side_effect=make_private):
                recovered = await probe.command("刷新称号", "刷新称号", actor="900001", number=999824)
            assert "当前共有1个称号" in recovered, recovered
            assert title_data.get_all_titles() == second_catalog
            assert title_data.get_title_condition_keys() == {"境界突破"}
            assert database_projection(probe) == before
            integrity(probe)
            probe.evidence.update(
                valid_refresh=True, invalid_config_preserved_last_good=True,
                invalid_shape_preserved_last_good=True, missing_config_preserved_last_good=True,
                repaired_config_reloaded=True, invalid_reply=invalid, missing_reply=missing,
                private_message=True, condition_key_cache_replaced=True, database_zero_writes=True,
                integrity_check="ok",
            )
        finally:
            path.write_bytes(original)
            title_data.refresh_title_cache()
