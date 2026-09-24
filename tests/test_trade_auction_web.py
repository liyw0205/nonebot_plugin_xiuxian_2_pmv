from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, request

from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_player_queue,
    apply_auction_queue_operations,
    apply_auction_settlement,
)
from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
from nonebot_plugin_xiuxian_2.features.trade.application import TradeApplication
from nonebot_plugin_xiuxian_2.features.trade.manifest import FEATURE
from nonebot_plugin_xiuxian_2.features.trade.web import blueprint as trade_blueprint
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class FixedClock:
    def now(self):
        return datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)


class SettlementEffects:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def on_settlement(self, *, event_id, event_key, **kwargs) -> None:
        self.events.append((event_id, event_key))


class TestTradeAuctionWeb:
    def setup_method(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game = root / "game.sqlite3"
        self.trade = root / "trade.sqlite3"
        with DatabaseUnitOfWork(self.game) as uow:
            apply_platform_schema(uow)
            apply_auction_settlement(uow)
            apply_auction_queue_operations(uow)
            uow.execute(
                "CREATE TABLE back(user_id TEXT NOT NULL,goods_id INTEGER NOT NULL,"
                "goods_name TEXT NOT NULL,goods_type TEXT NOT NULL,goods_num INTEGER NOT NULL,"
                "create_time TEXT,update_time TEXT,bind_num INTEGER NOT NULL DEFAULT 0,"
                "state INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(user_id,goods_id))"
            )
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,user_name TEXT,stone INTEGER)"
            )
            uow.execute(
                "INSERT INTO back(user_id,goods_id,goods_name,goods_type,goods_num,bind_num,state) "
                "VALUES('seller',1001,'玩家法器','装备',1,0,0)"
            )
            uow.execute(
                "INSERT INTO user_xiuxian(user_id,user_name,stone) VALUES('bidder','竞拍者',1000)"
            )
        with DatabaseUnitOfWork(self.trade) as uow:
            apply_auction_player_queue(uow)

        self.effects = SettlementEffects()
        settlement = AuctionSettlementApplication(self.game, effects=self.effects)
        self.application = TradeApplication(
            self.game,
            self.trade,
            clock=FixedClock(),
            auction_settlement=settlement,
            auction_max_goods_num=1,
            auction_max_user_items=1,
        )
        app = Flask(__name__)
        app.secret_key = "test"
        app.register_blueprint(
            trade_blueprint(
                self.application,
                permission=lambda required: required == "user"
                or request.headers.get("X-Role") == "admin",
            )
        )
        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["_csrf_token"] = "csrf"

    def teardown_method(self) -> None:
        self.temp_dir.cleanup()

    def post(self, path: str, operation_id: str, payload: dict, *, admin=False):
        headers = {
            "Idempotency-Key": operation_id,
            "X-CSRF-Token": "csrf",
        }
        if admin:
            headers["X-Role"] = "admin"
        return self.client.post(path, headers=headers, json={"user_id": "seller", **payload})

    def test_queue_routes_use_feature_applications_and_replay(self) -> None:
        invalid = self.post("/api/v1/trade/dequeue", "queue-invalid", {})
        assert invalid.status_code == 400

        first = self.post(
            "/api/v1/trade/enqueue",
            "queue-add",
            {
                "item_id": 1001,
                "item_name": "玩家法器",
                "start_price": 600000,
                "user_name": "卖家",
            },
        )
        replay = self.post(
            "/api/v1/trade/enqueue",
            "queue-add",
            {
                "item_id": 1001,
                "item_name": "玩家法器",
                "start_price": 600000,
                "user_name": "卖家",
            },
        )
        removed = self.post(
            "/api/v1/trade/dequeue",
            "queue-remove",
            {"item_id": 1001, "item_type": "装备"},
        )

        assert (first.status_code, replay.status_code, removed.status_code) == (200, 200, 200)
        assert first.get_json()["data"]["data"]["status"] == "completed"
        assert replay.get_json()["data"]["status"] == "replayed"
        assert removed.get_json()["data"]["data"]["status"] == "completed"
        with DatabaseUnitOfWork(self.game) as uow:
            inventory = uow.query_one(
                "SELECT goods_num,bind_num FROM back WHERE user_id='seller' AND goods_id=1001"
            )
        with DatabaseUnitOfWork(self.trade) as uow:
            queued = uow.query_one(
                "SELECT 1 AS present FROM auction_player_upload WHERE user_id='seller'"
            )
        assert (inventory["goods_num"], inventory["bind_num"], queued) == (1, 1, None)

    def test_session_routes_are_admin_only_and_settlement_effects_replay_once(self) -> None:
        denied = self.post(
            "/api/v1/trade/session_start",
            "session-denied",
            {"system_items_config": {}, "duration_hours": 2},
        )
        assert denied.status_code == 403

        payload = {
            "system_items_config": {"系統丹藥": {"id": 2001, "start_price": 100}},
            "duration_hours": 2,
            "system_item_count": 1,
        }
        started = self.post(
            "/api/v1/trade/session_start", "session-start", payload, admin=True
        )
        start_replay = self.post(
            "/api/v1/trade/session_start", "session-start", payload, admin=True
        )
        assert (started.status_code, start_replay.status_code) == (200, 200)
        assert started.get_json()["data"]["data"]["status"] == "started"
        assert start_replay.get_json()["data"]["status"] == "replayed"

        with DatabaseUnitOfWork(self.game) as uow:
            auction = uow.query_one("SELECT id FROM auction_current")
            uow.execute(
                "UPDATE auction_current SET bids=? WHERE id=?",
                (json.dumps({"bidder": 120}), auction["id"]),
            )
        finish_payload = {
            "end_time": 1790253000,
            "fee_rate": 0.2,
            "item_types": {"2001": "丹药"},
        }
        settled = self.post(
            "/api/v1/trade/session_finish", "session-finish", finish_payload, admin=True
        )
        finish_replay = self.post(
            "/api/v1/trade/session_finish", "session-finish", finish_payload, admin=True
        )

        assert (settled.status_code, finish_replay.status_code) == (200, 200)
        assert settled.get_json()["data"]["action"] == "auction.settle"
        assert settled.get_json()["data"]["data"]["status"] == "settled"
        assert finish_replay.get_json()["data"]["status"] == "replayed"
        assert len(self.effects.events) == 1
        with DatabaseUnitOfWork(self.game) as uow:
            event = uow.query_one("SELECT status FROM domain_outbox")
            winner_item = uow.query_one(
                "SELECT goods_num FROM back WHERE user_id='bidder' AND goods_id=2001"
            )
        assert (event["status"], winner_item["goods_num"]) == ("sent", 1)

    def test_manifest_marks_only_session_management_as_admin(self) -> None:
        permissions = {route.path.rsplit("/", 1)[-1]: route.permission for route in FEATURE.routes}

        assert permissions["enqueue"] == "user"
        assert permissions["dequeue"] == "user"
        assert permissions["session_start"] == "admin"
        assert permissions["session_finish"] == "admin"
