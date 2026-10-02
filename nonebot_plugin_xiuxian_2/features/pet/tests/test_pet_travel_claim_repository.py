import json
import tempfile
import unittest
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger, OutboxStore, ReconcileService
from ..application import PetApplication
from ..migrations import apply_pet_travel_claim
from ..repository import PetTravelClaimSqlRepository


class RecordingEffects:
    def __init__(self) -> None:
        self.event_ids: list[str] = []

    def dispatch(self, event_id: str) -> bool:
        self.event_ids.append(event_id)
        return True


class PetTravelClaimSqlRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.game = root / "game.db"
        self.player = root / "player.db"
        self.travel = {"pet_uid": "p", "start_at": 1, "end_at": 2, "duration_hours": 8}
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER, exp INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u',100,200)")
            uow.execute("CREATE TABLE back(user_id TEXT, goods_id INTEGER, goods_name TEXT, goods_type TEXT, goods_num INTEGER, bind_num INTEGER, PRIMARY KEY(user_id,goods_id))")
            apply_pet_travel_claim(uow)
            OperationLedger().ensure_schema(uow)
            OutboxStore().ensure_schema(uow)
        with DatabaseUnitOfWork(self.player) as uow:
            uow.execute("CREATE TABLE player_pet_item(user_id TEXT, uid TEXT, total_exp INTEGER)")
            uow.execute("INSERT INTO player_pet_item VALUES('u','p',0)")
            uow.execute("CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, travel TEXT)")
            uow.execute("INSERT INTO player_pet VALUES('u',?)", (json.dumps(self.travel, ensure_ascii=False),))

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_claim_and_replay(self) -> None:
        repository = PetTravelClaimSqlRepository(self.game, self.player)
        rewards = [{"id": 1, "name": "item", "type": "type", "amount": 2}]
        first = repository.claim("op", "u", self.travel, 10, 20, rewards, 99)
        replay = repository.claim("op", "u", self.travel, 10, 20, rewards, 99)
        self.assertEqual((first.status, replay.status, first.items), ("applied", "duplicate", ((1, 2),)))
        self.assertEqual(first.effects_event_id, replay.effects_event_id)
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone,exp FROM user_xiuxian WHERE user_id='u'")["stone"], 110)
            event = uow.query_one("SELECT event_type,payload_json FROM domain_outbox WHERE event_id=?", (first.effects_event_id,))
            self.assertEqual("game_event.projection", event["event_type"])
            payload = json.loads(event["payload_json"])
            self.assertEqual("pet_travel_claim", payload["event_key"])
            self.assertEqual({"宠物游历次数": 1, "宠物游历时长": 8}, payload["stat_increments"])

    def test_application_replay_dispatches_same_event_without_regranting(self) -> None:
        effects = RecordingEffects()
        application = PetApplication(self.game, self.player, game_event_effects=effects)
        rewards = [{"id": 1, "name": "item", "type": "type", "amount": 2}]
        first = application.claim_travel(
            operation_id="app-op", user_id="u", expected_travel=self.travel,
            stone=10, exp=20, items=rewards, max_goods_num=99,
        )
        replay = application.claim_travel(
            operation_id="app-op", user_id="u", expected_travel=self.travel,
            stone=10, exp=20, items=rewards, max_goods_num=99,
        )
        self.assertEqual(first.data["effects_event_id"], replay.data["effects_event_id"])
        self.assertEqual([first.data["effects_event_id"]] * 2, effects.event_ids)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(110, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])

    def test_reconcile_recovers_started_application_after_claim_commit(self) -> None:
        operation_id = "interrupted-op"
        rewards = [{"id": 1, "name": "item", "type": "type", "amount": 2}]
        payload = {
            "user_id": "u",
            "expected_travel": self.travel,
            "stone": 10,
            "exp": 20,
            "items": rewards,
            "max_goods_num": 99,
        }
        with DatabaseUnitOfWork(self.game) as uow:
            OperationLedger().begin(uow, operation_id, "pet.travel_claim", payload)
        PetTravelClaimSqlRepository(self.game, self.player).claim(
            operation_id, "u", self.travel, 10, 20, rewards, 99
        )

        effects = RecordingEffects()
        application = PetApplication(self.game, self.player, game_event_effects=effects)
        with DatabaseUnitOfWork(self.game, immediate=True) as uow:
            report = ReconcileService().run(
                uow,
                operation_handlers={
                    "pet.travel_claim": application.reconcile_travel_claim_operation,
                },
            )
        replay = application.claim_travel(
            operation_id=operation_id,
            user_id="u",
            expected_travel=self.travel,
            stone=10,
            exp=20,
            items=rewards,
            max_goods_num=99,
        )

        self.assertEqual((0, 1), (report.operations, report.outbox_events))
        self.assertTrue(replay.replayed)
        self.assertEqual(["pet.travel.effects:interrupted-op"], effects.event_ids)
        with DatabaseUnitOfWork(self.game, read_only=True) as uow:
            self.assertEqual(110, uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"])

    def test_old_receipt_without_outbox_does_not_offer_dispatch(self) -> None:
        repository = PetTravelClaimSqlRepository(self.game, self.player)
        repository.claim("legacy", "u", self.travel, 10, 20, [], 99)
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("DELETE FROM domain_outbox WHERE event_id=?", ("pet.travel.effects:legacy",))
        replay = repository.claim("legacy", "u", self.travel, 10, 20, [], 99)
        self.assertEqual("duplicate", replay.status)
        self.assertIsNone(replay.effects_event_id)

    def test_stale_and_inventory_full_preserve_state(self) -> None:
        repository = PetTravelClaimSqlRepository(self.game, self.player)
        rewards = [{"id": 1, "name": "item", "type": "type", "amount": 2}]
        self.assertEqual(repository.claim("stale", "u", {**self.travel, "end_at": 3}, 10, 20, rewards, 99).status, "state_changed")
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("INSERT INTO back VALUES('u',1,'item','type',99,99)")
        self.assertEqual(repository.claim("full", "u", self.travel, 10, 20, rewards, 99).status, "inventory_full")

    def test_rollback_trigger_preserves_assets(self) -> None:
        with DatabaseUnitOfWork(self.game) as uow:
            uow.execute("CREATE TRIGGER fail_event BEFORE INSERT ON domain_outbox BEGIN SELECT RAISE(ABORT, 'failed'); END")
        with self.assertRaises(Exception):
            PetTravelClaimSqlRepository(self.game, self.player).claim("rollback", "u", self.travel, 10, 20, [], 99)
        with DatabaseUnitOfWork(self.game) as uow:
            self.assertEqual(uow.query_one("SELECT stone,exp FROM user_xiuxian WHERE user_id='u'")["stone"], 100)
            self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM pet_travel_claim_operations")["n"])
            self.assertEqual(0, uow.query_one("SELECT COUNT(*) AS n FROM domain_outbox")["n"])
        with DatabaseUnitOfWork(self.player, read_only=True) as uow:
            self.assertEqual(self.travel, json.loads(uow.query_one("SELECT travel FROM player_pet WHERE user_id='u'")["travel"]))
