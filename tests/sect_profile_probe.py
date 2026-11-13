"""Offline profile commands with real sect reads and isolated databases."""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import tests
from tests.command_probe import install_boundary, inventory, isolate_source, prepare, run_case


async def main():
    _, managers = await prepare()
    game = managers[0]
    from nonebot_plugin_xiuxian_2.core.errors import DomainError
    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_info import user_info

    entry, = [item for item in inventory() if "xiuxian_message_" in item["handlers"]]
    count = 0

    async def profile_commands(expected_sect):
        nonlocal count
        for command in ("我的存档", "我的修仙信息"):
            count += 1
            result = await run_case(entry, "sect-profile", 930000 + count, input_text=command)
            assert result["status"] == "pass", result
            assert expected_sect in "\n".join(result["replies"]), result

    # No sect must skip the sect reader altogether.
    game.conn.execute("UPDATE user_xiuxian SET sect_id=NULL,sect_position=NULL WHERE user_id='900001'")
    game._commit_write()
    with patch.object(user_info, "SectInfoSqlRepository",
                      side_effect=AssertionError("unexpected sect read")):
        await profile_commands("无宗门")

    game.create_sect("900001", "fixture-sect")
    sect_id = game.get_sect_name("fixture-sect")
    game.update_usr_sect("900001", sect_id, 0)
    game.conn.execute("UPDATE user_xiuxian SET sect_position=NULL WHERE user_id='900001'")
    game._commit_write()
    orphan_before = game.conn.execute(
        "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='900001'"
    ).fetchone()
    await profile_commands("fixture-sect")
    assert "宗门职位未登记" in (await run_case(
        entry, "sect-profile", 939001, input_text="我的修仙信息"
    ))["replies"][0]
    assert game.conn.execute(
        "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='900001'"
    ).fetchone() == orphan_before
    game.conn.execute(
        "UPDATE user_xiuxian SET sect_id=?,sect_position=NULL WHERE user_id='900001'",
        (sect_id + 999999,),
    )
    game._commit_write()
    orphan_before = game.conn.execute(
        "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='900001'"
    ).fetchone()
    await profile_commands("宗门记录不存在")
    orphan_reply = await run_case(entry, "sect-profile", 939002, input_text="我的修仙信息")
    assert "宗门职位未登记" in "\n".join(orphan_reply["replies"]), orphan_reply
    assert game.conn.execute(
        "SELECT sect_id,sect_position FROM user_xiuxian WHERE user_id='900001'"
    ).fetchone() == orphan_before

    game.conn.execute(
        "UPDATE user_xiuxian SET sect_id=?,sect_position=0 WHERE user_id='900001'",
        (sect_id,),
    )
    game._commit_write()
    for field, writer in (("sect_materials", game.update_sect_materials),
                          ("sect_used_stone", game.update_sect_used_stone)):
        for value, suffix in ((None, None), ("1e3", None),
                              (b"100000000000000000001", None),
                              (float(2**63), "precision_lost_real"),
                              (1.5, "invalid_numeric_value"),
                              ("invalid", "invalid_numeric_value")):
            game.conn.execute("UPDATE sects SET sect_materials=0,sect_used_stone=0 WHERE sect_id=?", (sect_id,))
            game.conn.execute(f"UPDATE sects SET {field}=? WHERE sect_id=?", (value, sect_id))
            game._commit_write()
            before = game.conn.execute("SELECT * FROM sects WHERE sect_id=?", (sect_id,)).fetchone()
            if suffix is not None:
                for action in (lambda: game.get_sect_info(sect_id), lambda: writer(sect_id, 1, 1)):
                    try:
                        action()
                    except DomainError as exc:
                        assert exc.code == "currency_precision_unavailable", exc
                        assert exc.details["field"] == field, exc.details
                        assert exc.details["reason"] == f"{field}_{suffix}", exc.details
                    else:
                        raise AssertionError(f"invalid {field} accepted")
                    finally:
                        game.conn.rollback()
            else:
                game.get_sect_info(sect_id)
            await profile_commands("fixture-sect")
            after = game.conn.execute("SELECT * FROM sects WHERE sect_id=?", (sect_id,)).fetchone()
            assert before == after, (field, before, after)
    print(f"Sect profile dispatch: {count} commands passed; strict financial refusals preserved")


if __name__ == "__main__":
    isolate_source()
    install_boundary(tests._data_dir)
    asyncio.run(main())
