"""Effects outbox contracts for ordinary closing settlement."""

from __future__ import annotations

import json

from nonebot_plugin_xiuxian_2.core.result import OperationOutcome
from nonebot_plugin_xiuxian_2.features.buff.application import BuffApplication
from nonebot_plugin_xiuxian_2.features.buff.closing_effects_application import (
    ClosingEffectsApplication,
)
from nonebot_plugin_xiuxian_2.features.buff.closing_repository import (
    ClosingSettlementSqlRepository,
)
from nonebot_plugin_xiuxian_2.features.buff.migrations import (
    apply_closing_settlement_game,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import (
    DatabaseUnitOfWork,
    OperationLedger,
    OutboxStore,
)


class RecordingEffects:
    def __init__(self, *, fail_once: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.fail_once = fail_once

    def on_closing_settled(self, *, payload: dict, event_id: str) -> None:
        self.calls.append((str(event_id), dict(payload)))
        if self.fail_once and len(self.calls) == 1:
            raise RuntimeError("projection interrupted")


def _prepare_game(tmp_path):
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        OperationLedger().ensure_schema(uow)
        OutboxStore().ensure_schema(uow)
        apply_closing_settlement_game(uow)
        uow.execute(
            "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,exp INTEGER,stone INTEGER,hp INTEGER,mp INTEGER,atk INTEGER,power INTEGER)"
        )
        uow.execute(
            "CREATE TABLE user_cd(user_id TEXT PRIMARY KEY,type INTEGER,create_time TEXT,scheduled_time TEXT)"
        )
        uow.execute("INSERT INTO user_xiuxian VALUES('u',100,50,1,2,3,4)")
        uow.execute("INSERT INTO user_cd VALUES('u',1,'start',NULL)")
    return game, player


def _request(operation_id: str = "closing-effects") -> dict:
    return {
        "operation_id": operation_id,
        "user_id": "u",
        "expected_create_time": "start",
        "exp_gain": 20,
        "stone_cost": 10,
        "new_hp": 30,
        "new_mp": 40,
        "new_atk": 5,
        "new_power": 999,
        "exp_time": 45,
    }


def test_closing_effect_event_id_is_stable_across_repository_replay(tmp_path) -> None:
    game, _ = _prepare_game(tmp_path)
    repository = ClosingSettlementSqlRepository(game)

    first = repository.settle(
        "stable-event",
        "u",
        "start",
        20,
        10,
        30,
        40,
        5,
        999,
        45,
    )
    duplicate = repository.settle(
        "stable-event",
        "u",
        "start",
        20,
        10,
        30,
        40,
        5,
        999,
        45,
    )

    assert first.status == "applied"
    assert duplicate.status == "duplicate"
    assert first.effects_event_id == duplicate.effects_event_id == "buff.closing.effects:stable-event"
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        row = uow.query_one(
            "SELECT event_type,payload_json,status FROM domain_outbox WHERE event_id=?",
            (first.effects_event_id,),
        )
    assert row["event_type"] == "buff.closing.effects"
    assert row["status"] == "pending"
    assert json.loads(row["payload_json"])["operation_id"] == "stable-event"


def test_failed_effect_retries_to_sent_without_reapplying_game_state(tmp_path) -> None:
    game, player = _prepare_game(tmp_path)
    effects = RecordingEffects(fail_once=True)
    application = BuffApplication(game, player, closing_effects=effects)
    request = _request("closing-retry")

    first = application.closing_settle(**request)
    assert first.ok
    assert "补偿" in first.message
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        pending = uow.query_one(
            "SELECT status,attempts FROM domain_outbox WHERE event_id=?",
            ("buff.closing.effects:closing-retry",),
        )
        exp = uow.query_one("SELECT exp FROM user_xiuxian WHERE user_id='u'")["exp"]
    assert (pending["status"], pending["attempts"], exp) == ("pending", 1, 120)

    replay = application.closing_replay("closing-retry")
    assert replay.ok
    assert replay.replayed
    assert len(effects.calls) == 2
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        sent = uow.query_one(
            "SELECT status,attempts FROM domain_outbox WHERE event_id=?",
            ("buff.closing.effects:closing-retry",),
        )
    assert (sent["status"], sent["attempts"]) == ("sent", 1)


def test_legacy_ledger_receipt_without_effects_does_not_invent_projection(tmp_path) -> None:
    game = tmp_path / "game.db"
    player = tmp_path / "player.db"
    effects = RecordingEffects()
    application = BuffApplication(game, player, closing_effects=effects)
    outcome = OperationOutcome.applied(
        "legacy-closing",
        "buff.closing_settle",
        data={"status": "applied", "exp_gain": 20},
        audit_category="buff",
        occurred_at="2026-10-09T10:00:00+08:00",
    )
    with DatabaseUnitOfWork(game, immediate=True) as uow:
        application.ledger.ensure_schema(uow)
        application.ledger.begin(
            uow,
            "legacy-closing",
            "buff.closing_settle",
            {"user_id": "u", "legacy": True},
        )
        application.ledger.finish(uow, outcome)

    replay = application.closing_replay("legacy-closing")

    assert replay.ok
    assert replay.replayed
    assert effects.calls == []


def test_successful_replay_dispatches_effects_exactly_once(tmp_path) -> None:
    game, player = _prepare_game(tmp_path)
    effects = RecordingEffects()
    application = BuffApplication(game, player, closing_effects=effects)
    request = _request("closing-once")

    first = application.closing_settle(**request)
    replay = application.closing_settle(**request)

    assert first.ok
    assert replay.ok
    assert replay.replayed
    assert len(effects.calls) == 1
    assert effects.calls[0][0] == "buff.closing.effects:closing-once"
    with DatabaseUnitOfWork(game, read_only=True) as uow:
        assert uow.query_one(
            "SELECT COUNT(*) AS n FROM domain_outbox WHERE event_id=? AND status='sent'",
            ("buff.closing.effects:closing-once",),
        )["n"] == 1


def test_feature_effects_application_forwards_stable_projection_ids(tmp_path) -> None:
    calls: dict[str, list] = {"statistics": [], "logs": [], "tasks": [], "activity": []}

    class Statistics:
        def record(self, **kwargs):
            calls["statistics"].append(kwargs)
            return True

    application = ClosingEffectsApplication(
        tmp_path / "player.db",
        players_dir=tmp_path / "players",
        statistics=Statistics(),
        invalidate_cache=lambda: calls["statistics"].append({"invalidated": True}),
        log_event=lambda **kwargs: calls["logs"].append(kwargs),
        task_progress=lambda *args, **kwargs: calls["tasks"].append((args, kwargs)),
        activity_event=lambda *args, **kwargs: calls["activity"].append((args, kwargs)),
    )

    application.on_closing_settled(
        payload={
            "operation_id": "closing-feature",
            "user_id": "u",
            "exp_time": 45,
            "exp_gain": 20,
            "stone_cost": 10,
            "occurred_at": "2026-10-09T10:00:00+08:00",
        },
        event_id="buff.closing.effects:closing-feature",
    )

    assert calls["statistics"][0]["event_id"] == "buff.closing.effects:closing-feature"
    assert calls["logs"][0]["event_id"] == "buff.closing.effects:closing-feature:log"
    assert calls["tasks"][0][1]["operation_id"] == "task-progress:closing-feature"
    assert calls["activity"][0][1]["event_id"] == "buff.closing.effects:closing-feature:activity:out_closing"
