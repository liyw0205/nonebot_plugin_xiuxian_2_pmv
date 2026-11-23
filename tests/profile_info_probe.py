"""Reproduce profile read failures through the registered text info command."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import tests
from tests.command_probe import install_boundary, isolate_source, inventory, prepare, run_case


def database_projection(database: Path, *, ignore_activity_time: bool = False):
    with sqlite3.connect(database) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        ).fetchall()
        result = []
        for (table,) in tables:
            columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')]
            selected = [
                name for name in columns
                if not (ignore_activity_time and table == "user_cd" and name == "last_check_info_time")
            ]
            sql = f'SELECT {", ".join(chr(34) + name.replace(chr(34), chr(34) * 2) + chr(34) for name in selected)} FROM "{table}"'
            rows = connection.execute(sql).fetchall() if selected else []
            result.append((table, tuple(sorted(rows, key=repr))))
        return tuple(result)


def changed_tables(before, after):
    before_by_table = dict(before)
    after_by_table = dict(after)
    return {
        table: {"before": before_by_table.get(table), "after": after_by_table.get(table)}
        for table in sorted(set(before_by_table) | set(after_by_table))
        if before_by_table.get(table) != after_by_table.get(table)
    }


async def main() -> None:
    isolate_source()
    install_boundary(tests._data_dir)
    context, managers = await prepare()

    from nonebot_plugin_xiuxian_2.features.info import profile_repository

    entry, = [item for item in inventory() if "xiuxian_message_" in item["handlers"]]
    game, player, impart = managers[0], managers[1], managers[3]
    game.conn.execute(
        "UPDATE user_xiuxian SET exp=4567,stone=7654,hp=1200,mp=1300,atk=321 "
        "WHERE user_id='900001'"
    )
    game.conn.execute(
        "UPDATE user_xiuxian SET user_name='隔离档案',exp=123,stone=456,hp=700,mp=800,atk=777 "
        "WHERE user_id='900002'"
    )
    game.get_user_cd("900001")
    game.conn.execute(
        "UPDATE user_cd SET last_check_info_time='2000-01-01 00:00:00' WHERE user_id='900001'"
    )
    game._commit_write()
    game._clear_read_cache()

    game_before = database_projection(context.paths.game_db, ignore_activity_time=True)
    player_before = database_projection(context.paths.player_db)
    impart_before = database_projection(context.paths.impart_db)
    activity_before = game.conn.execute(
        "SELECT last_check_info_time FROM user_cd WHERE user_id='900001'"
    ).fetchone()[0]
    for index, command in enumerate(("我的修仙信息", "我的存档", "存档", "修仙信息")):
        result = await run_case(entry, "profile-info", 972010 + index, input_text=command)
        reply = "\n".join(result["replies"])
        assert result["status"] == "pass", json.dumps(result, ensure_ascii=False, indent=2)
        assert "道号: 诊断道友" in reply and "攻击力: 321" in reply, reply
        assert "所在宗门: 无宗门" in reply and "关系：道侣：无；师父：无；徒弟：无" in reply, reply

    private = await run_case(
        entry,
        "profile-info-private",
        972017,
        input_text="存档",
        event_type="private",
    )
    private_reply = "\n".join(private["replies"])
    assert private["status"] == "pass", json.dumps(
        private, ensure_ascii=False, indent=2
    )
    assert "道号: 诊断道友" in private_reply and "攻击力: 321" in private_reply, private_reply

    player_after_aliases = database_projection(context.paths.player_db)
    impart_after_aliases = database_projection(context.paths.impart_db)
    assert player_after_aliases == player_before, json.dumps(
        {"player_read_side_effects": changed_tables(player_before, player_after_aliases)},
        ensure_ascii=False,
        indent=2,
    )
    assert impart_after_aliases == impart_before, json.dumps(
        {"impart_read_side_effects": changed_tables(impart_before, impart_after_aliases)},
        ensure_ascii=False,
        indent=2,
    )

    game.conn.execute(
        "INSERT INTO sects(sect_id,sect_name,sect_owner,sect_scale,sect_used_stone) "
        "VALUES(1,'隔离宗门','900001',1,0)"
    )
    game.conn.execute(
        "UPDATE user_xiuxian SET sect_id=1,sect_position=0 WHERE user_id='900001'"
    )
    buff = game.conn.execute(
        "SELECT 1 FROM BuffInfo WHERE user_id='900001'"
    ).fetchone()
    if buff:
        game.conn.execute(
            "UPDATE BuffInfo SET main_buff=9001,faqi_buff=7001,armor_buff=6001 "
            "WHERE user_id='900001'"
        )
    else:
        game.conn.execute(
            "INSERT INTO BuffInfo(user_id,main_buff,faqi_buff,armor_buff) "
            "VALUES('900001',9001,7001,6001)"
        )
    game._commit_write()
    game._clear_read_cache()
    player.conn.execute(
        "INSERT INTO title(user_id,unlocked,equipped) VALUES('900001','[\"30001\"]','30001') "
        "ON CONFLICT(user_id) DO UPDATE SET unlocked=excluded.unlocked,equipped=excluded.equipped"
    )
    player._commit_write()
    player.update_or_write_data(
        "900001", "partner", "partner_id", "900002", data_type="TEXT"
    )
    player.update_or_write_data(
        "900001", "partner", "affection", 5000, data_type="INTEGER"
    )
    player.update_or_write_data(
        "900001", "mentor", "mentor_id", "900002", data_type="TEXT"
    )
    player.update_or_write_data(
        "900001", "mentor", "apprentice_ids", ["900003"], data_type="TEXT"
    )
    player.update_or_write_data(
        "900003", "mentor", "mentor_id", "900001", data_type="TEXT"
    )

    game_before = database_projection(context.paths.game_db, ignore_activity_time=True)
    player_before = database_projection(context.paths.player_db)
    impart_before = database_projection(context.paths.impart_db)
    detailed = await run_case(
        entry, "profile-details", 972017, input_text="我的修仙信息"
    )
    detailed_reply = "\n".join(detailed["replies"])
    assert detailed["status"] == "pass", json.dumps(
        detailed, ensure_ascii=False, indent=2
    )
    for expected in (
        "境界: 江湖好手",
        "修为: 4567",
        "灵石: 7654",
        "称号: 初心不改",
        "修为排行: 第2位",
        "灵石排行: 第2位",
        "所在宗门: 隔离宗门",
        "宗门职位: 宗主",
        "主修功法: 吐纳功法(人阶下品)",
        "法器: 精铁符剑(下品符器)",
        "防具: 修士道袍(下品符器)",
        "道侣：隔离档案 (💕 心有灵犀)",
        "师父：隔离档案",
        "徒弟：诊断管理",
    ):
        assert expected in detailed_reply, (expected, detailed_reply)
    assert database_projection(context.paths.game_db, ignore_activity_time=True) == game_before
    assert database_projection(context.paths.player_db) == player_before
    assert database_projection(context.paths.impart_db) == impart_before

    with sqlite3.connect(context.paths.impart_db) as connection:
        connection.execute(
            "INSERT INTO xiuxian_impart(user_id,impart_atk_per) VALUES('900001',0.25)"
        )
    impart_with_bonus = database_projection(context.paths.impart_db)
    buffed = await run_case(
        entry, "profile-persisted-impart", 972018, input_text="我的修仙信息"
    )
    buffed_reply = "\n".join(buffed["replies"])
    assert buffed["status"] == "pass", json.dumps(
        buffed, ensure_ascii=False, indent=2
    )
    assert "攻击力: 446" in buffed_reply, buffed_reply
    assert database_projection(context.paths.impart_db) == impart_with_bonus

    player.conn.execute(
        "CREATE TABLE natal_treasure(user_id TEXT PRIMARY KEY,form INTEGER,name TEXT,level INTEGER)"
    )
    player.conn.execute(
        "INSERT INTO natal_treasure(user_id,form,name,level) VALUES('900001',1,'隔离法宝',3)"
    )
    player._commit_write()
    player_before = database_projection(context.paths.player_db)
    active_natal = await run_case(
        entry, "profile-existing-natal", 972019, input_text="我的修仙信息"
    )
    active_natal_reply = "\n".join(active_natal["replies"])
    assert active_natal["status"] == "pass", json.dumps(
        active_natal, ensure_ascii=False, indent=2
    )
    assert "本命法宝: 隔离法宝 (Lv.3)" in active_natal_reply, active_natal_reply

    other = await run_case(
        entry, "profile-isolation", 972020, actor="900002", input_text="我的修仙信息"
    )
    other_reply = "\n".join(other["replies"])
    assert "道号: 隔离档案" in other_reply and "攻击力: 777" in other_reply, other
    assert "道号: 诊断道友" not in other_reply, other
    natal_users = player.conn.execute(
        "SELECT user_id FROM natal_treasure ORDER BY user_id"
    ).fetchall()
    assert [row[0] for row in natal_users] == ["900001"], natal_users
    unregistered = await run_case(
        entry, "profile-unregistered", 972021, actor="900099", input_text="我的修仙信息"
    )
    assert "修仙界没有道友的信息" in "\n".join(unregistered["replies"]), unregistered
    activity_after = game.conn.execute(
        "SELECT last_check_info_time FROM user_cd WHERE user_id='900001'"
    ).fetchone()[0]
    assert activity_after and activity_after != activity_before, (activity_before, activity_after)
    game_after = database_projection(context.paths.game_db, ignore_activity_time=True)
    if game_after != game_before:
        before_by_table = dict(game_before)
        after_by_table = dict(game_after)
        changed = {
            table: {"before": before_by_table.get(table), "after": after_by_table.get(table)}
            for table in sorted(set(before_by_table) | set(after_by_table))
            if before_by_table.get(table) != after_by_table.get(table)
        }
        raise AssertionError(json.dumps({"unexpected_game_changes": changed}, ensure_ascii=False, indent=2))
    player_after = database_projection(context.paths.player_db)
    if player_after != player_before:
        before_by_table = dict(player_before)
        after_by_table = dict(player_after)
        changed = {
            table: {"before": before_by_table.get(table), "after": after_by_table.get(table)}
            for table in sorted(set(before_by_table) | set(after_by_table))
            if before_by_table.get(table) != after_by_table.get(table)
        }
        raise AssertionError(json.dumps({"unexpected_player_changes": changed}, ensure_ascii=False, indent=2))

    unit_of_work = profile_repository.DatabaseUnitOfWork

    class DenyProfileRead(unit_of_work):
        def __enter__(self):
            result = super().__enter__()
            if Path(self.database).resolve() == Path(context.paths.game_db).resolve():
                self.connection.set_authorizer(
                    lambda action, table, *_: sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and table == "user_xiuxian"
                    else sqlite3.SQLITE_OK
                )
            return result

    with patch.object(profile_repository, "DatabaseUnitOfWork", DenyProfileRead):
        result = await run_case(
            entry,
            "profile-read-failure",
            972001,
            input_text="我的修仙信息",
        )

    reply = "\n".join(result["replies"])
    assert result["status"] == "exception", json.dumps(result, ensure_ascii=False, indent=2)
    assert "玩家档案读取失败" in reply and "修仙界没有道友的信息" not in reply, json.dumps(
        {"result": result, "expected": "specific profile read failure, not unregistered"},
        ensure_ascii=False,
        indent=2,
    )
    assert result["logs"] and any(
        "玩家档案读取失败 subject=player_profile" in item["message"]
        and item["exception"]
        for item in result["logs"]
    ), result
    assert result["errors"] and any(
        "access to user_xiuxian.id is prohibited" in item for item in result["errors"]
    ), result
    assert database_projection(context.paths.game_db, ignore_activity_time=True) == game_before
    assert database_projection(context.paths.player_db) == player_before

    from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_natal_treasure import natal_data

    class DenyNatalRead(natal_data.DatabaseUnitOfWork):
        def __enter__(self):
            result = super().__enter__()
            if Path(self.database).resolve() == Path(context.paths.player_db).resolve():
                self.connection.set_authorizer(
                    lambda action, table, *_: sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and table == "natal_treasure"
                    else sqlite3.SQLITE_OK
                )
            return result

    with patch.object(natal_data, "DatabaseUnitOfWork", DenyNatalRead):
        result = await run_case(
            entry,
            "profile-natal-read-failure",
            972022,
            input_text="我的修仙信息",
        )
    reply = "\n".join(result["replies"])
    assert result["status"] == "exception", json.dumps(result, ensure_ascii=False, indent=2)
    assert (
        "本命法宝，本命法宝档案读取失败" in reply
        and "本命法宝: 无" not in reply
    ), reply
    assert result["logs"] and any(
        "本命法宝读取失败 subject=natal_treasure" in item["message"]
        and item["exception"]
        for item in result["logs"]
    ), result
    assert result["errors"] and any(
        "access to natal_treasure.form is prohibited" in item
        for item in result["errors"]
    ), result
    assert database_projection(context.paths.game_db, ignore_activity_time=True) == game_before
    assert database_projection(context.paths.player_db) == player_before

    from nonebot_plugin_xiuxian_2.features.impart import repository as impart_repository

    unit_of_work = impart_repository.DatabaseUnitOfWork

    class DenyImpartRead(unit_of_work):
        def __enter__(self):
            result = super().__enter__()
            if Path(self.database).resolve() == Path(context.paths.impart_db).resolve():
                self.connection.set_authorizer(
                    lambda action, table, *_: sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and table == "xiuxian_impart"
                    else sqlite3.SQLITE_OK
                )
            return result

    activity_before_impart_failure = game.conn.execute(
        "SELECT last_check_info_time FROM user_cd WHERE user_id='900001'"
    ).fetchone()[0]
    with patch.object(impart_repository, "DatabaseUnitOfWork", DenyImpartRead):
        result = await run_case(
            entry,
            "profile-impart-read-failure",
            972023,
            input_text="我的修仙信息",
        )
    reply = "\n".join(result["replies"])
    assert result["status"] == "exception", json.dumps(
        result, ensure_ascii=False, indent=2
    )
    assert "玩家传承属性读取失败" in reply, reply
    assert "攻击力:" not in reply, reply
    assert result["logs"] and any(
        "传承属性读取失败 subject=player_impart" in item["message"]
        and item["exception"]
        for item in result["logs"]
    ), result
    assert result["errors"] and any(
        "xiuxian_impart." in item and "prohibited" in item
        for item in result["errors"]
    ), result
    assert game.conn.execute(
        "SELECT last_check_info_time FROM user_cd WHERE user_id='900001'"
    ).fetchone()[0] == activity_before_impart_failure
    assert database_projection(context.paths.game_db, ignore_activity_time=True) == game_before
    assert database_projection(context.paths.player_db) == player_before
    assert database_projection(context.paths.impart_db) == impart_with_bonus
    print("Profile aliases, natal display/isolation, activity timestamp and read failures passed")


if __name__ == "__main__":
    asyncio.run(main())
