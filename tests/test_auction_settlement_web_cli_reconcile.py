from __future__ import annotations

import json
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from nonebot_plugin_xiuxian_2.adapters.web.app import create_app
from nonebot_plugin_xiuxian_2.bootstrap import build_runtime_context
from nonebot_plugin_xiuxian_2.cli import main as cli_main
from nonebot_plugin_xiuxian_2.features.auction.migrations import (
    apply_auction_settlement_game_effects,
    apply_auction_settlement_statistics,
)
from nonebot_plugin_xiuxian_2.features.auction.settlement import AuctionSettlementApplication
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork, OutboxStore
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema


class AuctionSettlementWebCliReconcileTests(unittest.TestCase):
    def _prepare(self, directory: str, event_id: str) -> None:
        context = build_runtime_context(data_dir=directory, legacy_startup=False)
        with DatabaseUnitOfWork(context.database.path("game_db")) as uow:
            apply_platform_schema(uow)
            apply_auction_settlement_game_effects(uow)
            OutboxStore().append(
                uow,
                event_id=event_id,
                aggregate_type="auction",
                aggregate_id="a-1",
                event_type=AuctionSettlementApplication.effects_event,
                payload={
                    "event_id": event_id,
                    "operation_id": "settle-1",
                    "event_key": "seller_miss",
                    "settlement": {
                        "auction_id": "a-1", "item_id": 10, "item_name": "灵剑",
                        "seller_id": "seller", "final_price": None, "status": "流拍",
                    },
                    "occurred_at": "2026-09-24T10:00:00+00:00",
                },
            )
        with DatabaseUnitOfWork(context.database.path("player_db")) as uow:
            apply_auction_settlement_statistics(uow)

    def _assert_projection(self, directory: str, event_id: str) -> None:
        data = Path(directory)
        with DatabaseUnitOfWork(data / "player.db") as uow:
            row = uow.query_one(
                'SELECT "拍卖流拍次数" FROM statistics WHERE user_id=?', ("seller",)
            )
        log_file = data / "players" / "seller" / "logs" / "260924.log"
        logs = json.loads(log_file.read_text(encoding="utf-8"))
        self.assertEqual(row["拍卖流拍次数"], 1)
        self.assertEqual(sum(item.get("event_id") == event_id for item in logs), 1)

    def test_web_reconcile_installs_and_dispatches_settlement_handler(self):
        with tempfile.TemporaryDirectory() as directory:
            event_id = "settle-1:auction.settlement:a-1:seller_miss"
            self._prepare(directory, event_id)
            context = build_runtime_context(data_dir=directory, legacy_startup=False)
            client = create_app(context=context).test_client()
            csrf = client.get("/api/v1/csrf").get_json()["data"]["token"]

            response = client.post(
                "/api/v1/reconcile",
                headers={"X-Role": "admin", "X-CSRF-Token": csrf},
            )

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.get_json()["data"]["clean"])
            self.assertIn(AuctionSettlementApplication.effects_event, context.outbox_handlers)
            self._assert_projection(directory, event_id)

    def test_cli_apply_reconciles_settlement_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            event_id = "settle-1:auction.settlement:a-1:seller_miss"
            self._prepare(directory, event_id)

            with redirect_stdout(StringIO()):
                result = cli_main(["reconcile", "--apply", "--data-dir", directory])

            self.assertEqual(result, 0)
            self._assert_projection(directory, event_id)
            with DatabaseUnitOfWork(Path(directory) / "xiuxian.db") as uow:
                row = uow.query_one(
                    "SELECT status FROM domain_outbox WHERE event_id=?", (event_id,)
                )
            self.assertEqual(row["status"], "sent")


if __name__ == "__main__":
    unittest.main()
