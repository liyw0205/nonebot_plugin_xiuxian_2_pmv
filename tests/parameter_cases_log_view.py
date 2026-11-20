"""Probe log-view parameter handling through its registered commands."""
import json
from datetime import datetime, timedelta

from tests.parameter_cases_team_leave import integrity
from tests.parameter_cases_team_view import database_projection


def _seed_logs(user_id, count, marker):
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

    for index in range(count):
        utils.log_message(str(user_id), f"{marker}-{index:02d}")
    date = datetime.now().strftime("%y%m%d")
    path = utils.PLAYERSDATA / str(user_id) / "logs" / f"{date}.log"
    assert path.is_file(), path
    return date, path


def _assert_read_only(probe, database_before, files):
    assert database_projection(probe) == database_before
    for path, content in files.items():
        assert path.read_bytes() == content, path
    integrity(probe)


def register(case):
    @case("log-view-parameter-boundaries", "修仙日志", "日期、页码和 Unicode 边界",
          "真实注册 matcher/DI/handler 与隔离日志文件",
          "默认、指定日期、超页钳制与非法参数保持只读", group="log-view-parameters")
    async def parameter_boundaries(probe):
        database_before = database_projection(probe)
        date, log_file = _seed_logs("900001", 12, "分页日志")
        original = log_file.read_bytes()
        files = {log_file: original}

        default_reply = await probe.command("修仙日志", "修仙日志   ", number=999850, timeout=30)
        assert "页码：1/2" in default_reply and "分页日志-11" in default_reply, default_reply
        _assert_read_only(probe, database_before, files)

        blank_text_reply = await probe.command("修仙日志", "修仙日志", number=999851, timeout=30)
        assert "页码：1/2" in blank_text_reply and "分页日志-11" in blank_text_reply, blank_text_reply
        _assert_read_only(probe, database_before, files)

        for number, query in ((999852, "修仙日志 abc"), (999853, "修仙日志 ²")):
            reply = await probe.command("修仙日志", query, number=number, timeout=30)
            assert "页码：1/2" in reply and "分页日志-11" in reply, (query, reply)
            _assert_read_only(probe, database_before, files)

        page_two = await probe.command("修仙日志", f"修仙日志 {date} 2", number=999854, timeout=30)
        assert "页码：2/2" in page_two and "分页日志-01" in page_two and "分页日志-00" in page_two, page_two
        _assert_read_only(probe, database_before, files)

        unicode_page = await probe.command("修仙日志", f"修仙日志 {date} ٢", number=999855, timeout=30)
        assert "页码：2/2" in unicode_page and "分页日志-01" in unicode_page, unicode_page
        _assert_read_only(probe, database_before, files)

        non_decimal_page = await probe.command("修仙日志", f"修仙日志 {date} ²", number=999859, timeout=30)
        assert "页码：1/2" in non_decimal_page and "分页日志-11" in non_decimal_page, non_decimal_page
        _assert_read_only(probe, database_before, files)

        oversized_page = await probe.command("修仙日志", f"修仙日志 {date} {'9' * 5000}",
                                             number=999856, timeout=30)
        assert "页码：2/2" in oversized_page and "分页日志-00" in oversized_page, oversized_page
        _assert_read_only(probe, database_before, files)

        extra_args = await probe.command("修仙日志", f"修仙日志 {date} 1 忽略参数",
                                         number=999857, timeout=30)
        assert "页码：1/2" in extra_args and "分页日志-11" in extra_args, extra_args
        _assert_read_only(probe, database_before, files)

        probe.evidence.update(date=date, records=12, cases_checked=9, log_file_unchanged=True,
                              full_database_zero_writes=True, integrity_check="ok")

    @case("log-view-date-alias-isolation", "修仙日志", "缺失日期、别名与本人隔离",
          "两个玩家各自拥有隔离日志文件",
          "别名读取对应玩家内容，缺失日期沿用最近日志回退", group="log-view-parameters")
    async def date_alias_isolation(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        database_before = database_projection(probe)
        date, first_file = _seed_logs("900001", 1, "甲玩家日志")
        second_date, second_file = _seed_logs("900002", 1, "乙玩家日志")
        assert second_date == date
        older_date = (datetime.strptime(date, "%y%m%d") - timedelta(days=1)).strftime("%y%m%d")
        older_file = utils.PLAYERSDATA / "900001" / "logs" / f"{older_date}.log"
        older_file.write_text(
            json.dumps([{"timestamp": "旧日期", "message": "旧日期日志"}], ensure_ascii=False),
            encoding="utf-8",
        )
        files = {
            first_file: first_file.read_bytes(),
            second_file: second_file.read_bytes(),
            older_file: older_file.read_bytes(),
        }

        recent = await probe.command("修仙日志", "修仙日志", number=999858, timeout=30)
        assert "甲玩家日志-00" in recent and "旧日期日志" not in recent, recent
        _assert_read_only(probe, database_before, files)

        for index, alias in enumerate(("查看日志", "我的日志", "查日志", "日志记录")):
            actor = "900001" if index % 2 == 0 else "900002"
            reply = await probe.command(alias, f"{alias} {date} 1", actor=actor,
                                        number=999860 + index, timeout=30)
            expected = "甲玩家日志-00" if actor == "900001" else "乙玩家日志-00"
            forbidden = "乙玩家日志-00" if actor == "900001" else "甲玩家日志-00"
            assert expected in reply and forbidden not in reply, (alias, actor, reply)
            _assert_read_only(probe, database_before, files)

        missing_date = await probe.command("修仙日志", "修仙日志 991231 1", number=999864, timeout=30)
        assert "甲玩家日志-00" in missing_date and "乙玩家日志-00" not in missing_date, missing_date
        _assert_read_only(probe, database_before, files)
        probe.evidence.update(date=date, aliases=4, players_isolated=True, missing_date_fallback=True,
                              log_files_unchanged=True, full_database_zero_writes=True,
                              integrity_check="ok")

    @case("log-view-empty-and-corrupt", "修仙日志", "空日志与坏历史 JSON",
          "未注册隔离玩家目录可控，不写业务数据库",
          "空目录给出空态，坏历史保留具体读取失败回复且源文件不变",
          group="log-view-parameters", outcome="rejection")
    async def empty_and_corrupt(probe):
        from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_utils import utils

        database_before = database_projection(probe)
        actor = "900099"
        reply = await probe.command("修仙日志", "修仙日志", actor=actor, number=999870, timeout=30)
        assert "暂无任何日志记录" in reply, reply
        _assert_read_only(probe, database_before, {})

        date = datetime.now().strftime("%y%m%d")
        log_file = utils.PLAYERSDATA / actor / "logs" / f"{date}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text("{broken json", encoding="utf-8")
        original = log_file.read_bytes()
        corrupt_reply = await probe.command("修仙日志", "修仙日志", actor=actor, number=999871,
                                            allow_exception=True, timeout=30)
        assert "获取日志失败" in corrupt_reply and "Expecting property name" in corrupt_reply, corrupt_reply
        assert probe.calls[-1]["status"] == "exception"
        assert all("Traceback" not in error for error in probe.calls[-1]["errors"])
        _assert_read_only(probe, database_before, {log_file: original})
        probe.evidence.update(empty_message=True,
                              bad_json_classification="expected read rejection; logger error is not a traceback",
                              bad_json_reason="Expecting property name enclosed in double quotes",
                              source_unchanged=True, full_database_zero_writes=True, integrity_check="ok")
