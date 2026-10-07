from __future__ import annotations

import __future__
import ast
import asyncio
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import tests  # Establish isolated paths before importing plugin modules.
import pytest

from nonebot_plugin_xiuxian_2.core.errors import ConflictError
from nonebot_plugin_xiuxian_2.features.arena.application import ArenaApplication
from nonebot_plugin_xiuxian_2.features.arena.migrations import (
    apply_arena_purchase,
    apply_arena_purchase_receipt,
    apply_arena_settlement,
    apply_arena_state,
)
from nonebot_plugin_xiuxian_2.features.arena.opponent_application import ArenaOpponentApplication
from nonebot_plugin_xiuxian_2.features.arena.opponent_repository import ArenaOpponentRepository
from nonebot_plugin_xiuxian_2.features.arena.repository import ArenaChallengePurchaseSqlRepository
from nonebot_plugin_xiuxian_2.features.arena.state_application import ArenaStateApplication
from nonebot_plugin_xiuxian_2.features.info.profile_application import PlayerProfileApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OperationLedger
from nonebot_plugin_xiuxian_2.plugin import build_migrations, migrations_for_database


PACKAGE = Path(__file__).resolve().parents[1] / "nonebot_plugin_xiuxian_2"
FACADE = PACKAGE / "xiuxian/xiuxian_arena/__init__.py"
RESULTS = PACKAGE / "compatibility/legacy_arena_transactions.py"
NOW = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
SUCCESS = "\u6210\u529f\u5151\u6362"
CONSOLATION = "\u5b89\u6170\u79ef\u5206"
VICTORY = "\u6311\u6218\u80dc\u5229"


def _load(path, namespace, names):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body
             if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
             and node.name in names]
    assert {node.name for node in nodes} == set(names)
    for node in nodes:
        if not isinstance(node, ast.ClassDef):
            node.decorator_list = []
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace


def _query(path, sql, parameters=()):
    with sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True) as connection:
        return connection.execute(sql, parameters).fetchall()


def _context(tmp_path, *, opponent=False, purchase_migrated=True):
    game, player = tmp_path / "game.db", tmp_path / "player.db"
    clock = SimpleNamespace(now=lambda: NOW)
    with DatabaseUnitOfWork(game) as uow:
        uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,hp INTEGER,mp INTEGER,user_stamina INTEGER)")
        uow.execute("INSERT INTO user_xiuxian VALUES('u','challenger',50,40,8)")
        if opponent:
            uow.execute("INSERT INTO user_xiuxian VALUES('v','opponent',60,45,9)")
        uow.execute(
            "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,goods_type TEXT,"
            "goods_num INTEGER,create_time TEXT,update_time TEXT,bind_num INTEGER,UNIQUE(user_id,goods_id))"
        )
        OperationLedger().ensure_schema(uow)
        apply_arena_purchase(uow)
        if purchase_migrated:
            apply_arena_purchase_receipt(uow)
        apply_arena_settlement(uow)
    with DatabaseUnitOfWork(player) as uow:
        apply_arena_state(uow)
    state = ArenaStateApplication(player, clock=clock)
    state.get("u")
    if opponent:
        state.get("v")
    with DatabaseUnitOfWork(player) as uow:
        uow.execute("UPDATE arena SET honor_points=100,weekly_purchases=? WHERE user_id='u'",
                    (json.dumps({"_last_reset": NOW.date().isoformat(), "1": 1}),))
        if opponent:
            uow.execute("UPDATE arena SET score=1100 WHERE user_id='v'")
    repository = ArenaChallengePurchaseSqlRepository(game, player, clock=clock)
    application = ArenaApplication(game, player, repository=repository, clock=clock)
    candidates = ArenaOpponentRepository(game, player, clock=clock)
    opponents = ArenaOpponentApplication(candidates, state)
    return SimpleNamespace(
        game=game, player=player, clock=clock, state=state, repository=repository,
        application=application, candidates=candidates, opponents=opponents,
        profiles=PlayerProfileApplication(game),
    )


class _Finished(Exception):
    pass


def _handlers(context):
    messages, battle_messages = [], []
    fight = Mock(return_value=(
        ["battle transcript"], 0,
        [{"challenger": {"user_id": "u", "hp": 11, "mp": 12}},
         {"opponent": {"user_id": "v", "hp": 21, "mp": 22}}],
    ))
    catalog = Mock()
    catalog.get_data_by_item_id.return_value = {"name": "test-item", "type": "test-type"}
    shop = SimpleNamespace(config={"\u5546\u5e97\u5546\u54c1": {
        "1": {"cost": 10, "weekly_limit": 5, "required_rank": "\u9752\u94dc"},
    }})

    async def assign_bot(**kwargs):
        return kwargs["bot"], None

    async def send(bot, event, message, **kwargs):
        messages.append(message)

    async def send_battle(bot, event, transcript):
        battle_messages.append((transcript, _query(context.player,
            "SELECT score,daily_challenges_used FROM arena WHERE user_id='u'")))

    async def finish():
        raise _Finished

    result_types = _load(RESULTS, {"__name__": __name__, "dataclass": dataclass}, {
        "ArenaPurchaseResult", "ArenaChallengeSettlementResult",
    })
    namespace = _load(FACADE, {
        **result_types, "re": re, "ConflictError": ConflictError, "logger": Mock(),
        "CommandArg": lambda: None, "assign_bot": assign_bot, "handle_send": send,
        "send_msg_handler": send_battle, "arena_application": context.application,
        "arena_opponent_application": context.opponents, "arena_profile_application": context.profiles,
        "arena_state_application": context.state, "runtime_clock": context.clock,
        "arena_ids": SimpleNamespace(new_id=Mock(side_effect=AssertionError("event identity required"))),
        "check_user": lambda event: (True, {"user_id": "u", "user_name": "challenger"}, ""),
        "arena_challenge": SimpleNamespace(finish=finish), "arena_buy": SimpleNamespace(finish=finish),
        "items": catalog, "arena_shop_data": shop, "XiuConfig": lambda: SimpleNamespace(max_goods_num=99),
        "arena_limit": SimpleNamespace(daily_challenges=10, win_points=20, lose_points=10, no_match_points=10),
        "ARENA_CHALLENGE_STAMINA_COST": 0, "_arena_fight": fight,
        "_sql_message": Mock(side_effect=AssertionError("legacy profile reader")),
        "_player_data_manager": Mock(side_effect=AssertionError("legacy state reader")),
    }, {
        "arena_buy_", "arena_challenge_", "_arena_purchase_result", "_arena_purchase_message",
        "_arena_settlement_result", "_arena_challenge_result_message", "_arena_final_vitals",
        "check_rank_requirement", "find_arena_opponent", "set_arena_opponent_cache",
        "get_arena_opponent_cache", "clear_arena_opponent_cache",
    })

    def run(handler, text="", *, event_id="event"):
        messages.clear()
        event = SimpleNamespace(message_id=event_id, get_user_id=lambda: "u")
        args = SimpleNamespace(extract_plain_text=lambda: text)
        try:
            asyncio.run(namespace[handler](SimpleNamespace(self_id="bot"), event, args))
        except _Finished:
            pass
        assert len(messages) == 1
        return messages[0]

    return SimpleNamespace(run=run, namespace=namespace, fight=fight, catalog=catalog,
                           shop=shop, battle_messages=battle_messages)


def _purchase_state(context):
    honor, weekly = _query(context.player, "SELECT honor_points,weekly_purchases FROM arena WHERE user_id='u'")[0]
    return honor, json.loads(weekly), _query(context.game, "SELECT goods_id,goods_num,bind_num FROM back ORDER BY goods_id")


def _battle_state(context):
    return (
        _query(context.player, "SELECT user_id,score,total_wins,total_losses,daily_challenges_used FROM arena ORDER BY user_id"),
        _query(context.game, "SELECT user_id,hp,mp,user_stamina FROM user_xiuxian ORDER BY user_id"),
    )


def test_real_purchase_handler_debits_honor_grants_bound_inventory_and_replays_once(tmp_path):
    context = _context(tmp_path)
    handlers = _handlers(context)
    first = handlers.run("arena_buy_", "1 2")
    assert SUCCESS in first
    assert _purchase_state(context) == (80, {"_last_reset": "2026-10-07", "1": 3}, [(1, 2, 2)])
    with patch.object(context.application, "purchase", side_effect=AssertionError("receipt must bypass writer")):
        assert handlers.run("arena_buy_", "1 2") == first
    assert _purchase_state(context)[0] == 80
    assert _query(context.game, "SELECT COUNT(*) FROM arena_purchase_operations") == [(1,)]


def test_original_quantity_clamps_once_and_successful_receipt_precedes_current_rank_limit_and_catalog(tmp_path):
    context = _context(tmp_path)
    handlers = _handlers(context)
    with patch.object(context.application, "purchase", wraps=context.application.purchase) as purchase:
        first = handlers.run("arena_buy_", "1 99", event_id="clamped")
    assert SUCCESS in first and "\u00d74" in first
    assert purchase.call_args.kwargs["quantity"] == 99
    assert purchase.call_args.kwargs["clamp_quantity"] is True
    assert _purchase_state(context) == (60, {"_last_reset": "2026-10-07", "1": 5}, [(1, 4, 4)])
    with DatabaseUnitOfWork(context.player) as uow:
        uow.execute("UPDATE arena SET honor_points=0,rank='invalid-current-rank' WHERE user_id='u'")
    handlers.shop.config = None
    handlers.catalog.get_data_by_item_id.side_effect = AssertionError("receipt precedes catalog")
    with patch.object(context.opponents, "state", side_effect=AssertionError("receipt precedes live rank and limit")), \
            patch.object(context.application, "purchase", side_effect=AssertionError("receipt must not write")):
        assert handlers.run("arena_buy_", "1 99", event_id="clamped") == first
    assert _purchase_state(context) == (0, {"_last_reset": "2026-10-07", "1": 5}, [(1, 4, 4)])


@pytest.mark.parametrize("changed", ["1 4", "2 99"])
def test_purchase_reused_event_with_changed_original_quantity_or_item_is_conflict(tmp_path, changed):
    context = _context(tmp_path)
    handlers = _handlers(context)
    assert SUCCESS in handlers.run("arena_buy_", "1 99", event_id="conflict")
    before = _purchase_state(context)
    with patch.object(context.opponents, "state", side_effect=AssertionError("conflict must precede state")):
        message = handlers.run("arena_buy_", changed, event_id="conflict")
    assert SUCCESS not in message
    assert "\u51b2\u7a81" in message or "\u672a\u7ed3\u7b97" in message or "\u672a\u5b8c\u6210" in message
    assert _purchase_state(context) == before
    assert _query(context.game, "SELECT COUNT(*) FROM arena_purchase_operations") == [(1,)]


@pytest.mark.parametrize("raw", ["1 0", "1 -1", "1 " + "9" * 5000, "9" * 5000 + " 1", "1 2 extra"],
                         ids=["zero", "negative", "long-quantity", "long-id", "extra-argument"])
def test_purchase_invalid_quantity_or_overlong_input_never_reaches_receipt_or_writer(tmp_path, raw):
    context = _context(tmp_path)
    handlers = _handlers(context)
    before = _purchase_state(context)
    with patch.object(context.application, "purchase_result", side_effect=AssertionError("invalid receipt read")) as read, \
            patch.object(context.application, "purchase", side_effect=AssertionError("invalid purchase")) as write:
        assert SUCCESS not in handlers.run("arena_buy_", raw)
        read.assert_not_called()
        write.assert_not_called()
    assert _purchase_state(context) == before
    assert _query(context.game, "SELECT COUNT(*) FROM arena_purchase_operations") == [(0,)]


def test_purchase_rejected_receipt_stays_rejected_after_honor_is_replenished(tmp_path):
    context = _context(tmp_path)
    with DatabaseUnitOfWork(context.player) as uow:
        uow.execute("UPDATE arena SET honor_points=0 WHERE user_id='u'")
    handlers = _handlers(context)
    assert SUCCESS not in handlers.run("arena_buy_", "1 2", event_id="poor")
    with DatabaseUnitOfWork(context.player) as uow:
        uow.execute("UPDATE arena SET honor_points=100 WHERE user_id='u'")
    with patch.object(context.application, "purchase", side_effect=AssertionError("rejected replay write")):
        message = handlers.run("arena_buy_", "1 2", event_id="poor")
    assert SUCCESS not in message
    assert _purchase_state(context)[0] == 100 and _purchase_state(context)[2] == []
    receipt = context.application.purchase_result(
        operation_id="arena-purchase:poor:u", user_id="u", item_id=1, quantity=2,
    )
    assert receipt["status"] == "honor_insufficient"


def test_real_no_match_handler_grants_consolation_once_and_replay_bypasses_matching(tmp_path):
    context = _context(tmp_path)
    handlers = _handlers(context)
    first = handlers.run("arena_challenge_", event_id="no-match")
    assert CONSOLATION in first
    assert _battle_state(context) == ([("u", 1010, 0, 1, 1)], [("u", 50, 40, 8)])
    handlers.fight.assert_not_called()
    with patch.object(context.opponents, "find", side_effect=AssertionError("no-match replay match")), \
            patch.object(context.profiles, "get_user_profile", side_effect=AssertionError("no-match replay profile")), \
            patch.object(context.application, "settle", side_effect=AssertionError("no-match replay settle")):
        assert handlers.run("arena_challenge_", event_id="no-match") == first
    assert _query(context.game, "SELECT COUNT(*) FROM arena_challenge_settlement_operations") == [(1,)]
    assert _battle_state(context)[0] == [("u", 1010, 0, 1, 1)]


def test_real_match_handler_settles_both_players_before_sending_and_never_refights_replay(tmp_path):
    context = _context(tmp_path, opponent=True)
    handlers = _handlers(context)
    context.opponents.set_cache("u", [{"user_id": "v", "score": 1100}])
    first = handlers.run("arena_challenge_", "1", event_id="battle")
    assert VICTORY in first
    handlers.fight.assert_called_once_with("u", "v", "bot", "arena-challenge:battle:u")
    assert _battle_state(context) == (
        [("u", 1020, 1, 0, 1), ("v", 1090, 0, 1, 0)],
        [("u", 11, 12, 8), ("v", 21, 22, 9)],
    )
    assert context.opponents.get_cache("u") is None
    assert handlers.battle_messages == [(["battle transcript"], [(1020, 1)])]
    handlers.fight.side_effect = AssertionError("battle replay must not refight")
    with patch.object(context.profiles, "get_user_profile", side_effect=AssertionError("battle replay profile")), \
            patch.object(context.opponents, "get_cache", side_effect=AssertionError("battle replay cache")), \
            patch.object(context.application, "settle", side_effect=AssertionError("battle replay settle")):
        assert handlers.run("arena_challenge_", "1", event_id="battle") == first
    assert handlers.fight.call_count == 1 and len(handlers.battle_messages) == 1
    assert _query(context.game, "SELECT COUNT(*) FROM arena_challenge_settlement_operations") == [(1,)]


def test_real_rejected_settlement_receipt_never_becomes_duplicate_success_in_handler(tmp_path):
    context = _context(tmp_path)
    before = _battle_state(context)
    player = context.profiles.get_user_profile("u")
    outcome = context.application.settle(
        operation_id="arena-challenge:rejected:u", challenger_id="u", opponent_id=None,
        outcome="no_match", challenge_cap=0, stamina_cost=0, challenged_at="rejected-time",
        expected_challenger_arena=context.opponents.state("u"), expected_opponent_arena=None,
        expected_challenger_player=player, expected_opponent_player=None,
        final_challenger_hp=50, final_challenger_mp=40, final_opponent_hp=None, final_opponent_mp=None,
        win_points=20, lose_points=10, no_match_points=10,
    )
    assert not outcome.ok and outcome.code == "limit_reached"
    handlers = _handlers(context)
    with patch.object(context.opponents, "state", side_effect=AssertionError("rejected replay state")), \
            patch.object(context.application, "settle", side_effect=AssertionError("rejected replay settle")):
        message = handlers.run("arena_challenge_", event_id="rejected")
    assert CONSOLATION not in message and VICTORY not in message
    assert "\u62d2\u7edd" in message or "\u672a\u7ed3\u7b97" in message
    assert context.application.settlement_result(operation_id="arena-challenge:rejected:u", challenger_id="u")["status"] == "limit_reached"
    assert _battle_state(context) == before
    handlers.fight.assert_not_called()


@pytest.mark.parametrize("fault", ["candidate_read", "profile_missing", "settlement_storage"])
def test_challenge_read_or_storage_failure_does_not_award_consolation_or_claim_success(tmp_path, fault):
    context = _context(tmp_path, opponent=fault == "profile_missing")
    handlers = _handlers(context)
    before = _battle_state(context)
    if fault == "candidate_read":
        failure = patch.object(context.candidates, "candidates", side_effect=sqlite3.OperationalError("unavailable"))
    elif fault == "profile_missing":
        real_profile = context.profiles.get_user_profile
        failure = patch.object(context.profiles, "get_user_profile",
                               side_effect=lambda user: None if user == "v" else real_profile(user))
    else:
        with DatabaseUnitOfWork(context.game) as uow:
            uow.execute(
                "CREATE TRIGGER fail_arena_settlement BEFORE INSERT ON arena_challenge_settlement_operations "
                "BEGIN SELECT RAISE(ABORT,'injected settlement storage failure'); END"
            )
        failure = patch.object(context.candidates, "candidates", wraps=context.candidates.candidates)
    with failure:
        message = handlers.run("arena_challenge_", event_id=fault)
    assert CONSOLATION not in message and VICTORY not in message
    assert _battle_state(context) == before
    assert _query(context.game, "SELECT COUNT(*) FROM arena_challenge_settlement_operations") == [(0,)]
    assert handlers.battle_messages == []
    handlers.fight.assert_not_called()


@pytest.mark.parametrize("handler", ["arena_buy_", "arena_challenge_"])
def test_missing_receipt_schema_is_reported_without_request_time_ddl(tmp_path, handler):
    context = _context(tmp_path, purchase_migrated=False)
    if handler == "arena_challenge_":
        with DatabaseUnitOfWork(context.game) as uow:
            uow.execute("DROP TABLE arena_challenge_settlement_operations")
    schemas = {path: _query(path, "SELECT name,sql FROM sqlite_master ORDER BY name")
               for path in (context.game, context.player)}
    purchase_before, battle_before = _purchase_state(context), _battle_state(context)
    message = _handlers(context).run(handler, "1 2" if handler == "arena_buy_" else "")
    assert SUCCESS not in message and CONSOLATION not in message and VICTORY not in message
    assert any(text in message for text in ("\u5f02\u5e38", "\u672a\u5c31\u7eea", "\u672a\u7ed3\u7b97"))
    assert {path: _query(path, "SELECT name,sql FROM sqlite_master ORDER BY name")
            for path in (context.game, context.player)} == schemas
    assert _purchase_state(context) == purchase_before and _battle_state(context) == battle_before


def test_purchase_receipt_migration_is_additive_and_game_only():
    migrations = build_migrations()
    registered = [migration for migration in migrations if migration.version == "arena.010"]
    assert len(registered) == 1
    for key in ("game_db", "player_db", "impart_db", "trade_db"):
        versions = {migration.version for migration in migrations_for_database(migrations, key)}
        assert ("arena.010" in versions) is (key == "game_db")
