import asyncio
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from ....infrastructure.database import DatabaseUnitOfWork, OperationLedger, ReconcileService
from ....plugin import apply_platform_schema
from ..application import MapApplication
from ..migrations import apply_map_mission_claim
from ..repository import MapMissionClaimSqlRepository


class Repo:
    def invoke(self, action, *args, **kwargs): return {"status": "applied", "action": action}


class CombatApplication:
    def __init__(self):
        self.kwargs = None

    def settle(self, **kwargs):
        self.kwargs = kwargs
        return "combat-settlement"


class CombatRunner:
    def __init__(self):
        self.arguments = None

    async def __call__(self, user_id, enemy, *, bot_id):
        self.arguments = (user_id, enemy, bot_id)
        return ["battle"], "群友赢了", {"群友": {"剩余气血": 100}}


class Clock:
    def now(self):
        return datetime(2026, 9, 15, 12, tzinfo=timezone.utc)


class RecordingEffects:
    def __init__(self):
        self.event_ids = []

    def dispatch(self, event_id):
        self.event_ids.append(event_id)
        return True


class MapApplicationTest(unittest.TestCase):
    def test_mission_claim_replay_dispatches_frozen_event(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
                apply_map_mission_claim(uow)
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u',10)")
                uow.execute(
                    "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                    "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                    "bind_num INTEGER,UNIQUE(user_id,goods_id))"
                )
            with DatabaseUnitOfWork(player) as uow:
                uow.execute(
                    "CREATE TABLE map_mission(user_id TEXT PRIMARY KEY,date TEXT,"
                    "mission_type TEXT,target INTEGER,claimed INTEGER,settlement TEXT)"
                )
                uow.execute("INSERT INTO map_mission VALUES('u','2026-09-15','gather',5,0,'snap')")
                uow.execute(
                    "CREATE TABLE map_daily_limit(user_id TEXT PRIMARY KEY,date TEXT,gather_count INTEGER)"
                )
                uow.execute("INSERT INTO map_daily_limit VALUES('u','2026-09-15',5)")

            effects = RecordingEffects()
            application = MapApplication(game, player, game_event_effects=effects)
            request = {
                "operation_id": "mission-claim",
                "user_id": "u",
                "expected_mission": {
                    "date": "2026-09-15",
                    "mission_type": "gather",
                    "target": 5,
                    "claimed": 0,
                    "settlement": "snap",
                },
                "expected_daily": {"date": "2026-09-15", "gather_count": 5},
                "progress_key": "gather_count",
                "stone": 7,
                "items": [],
                "max_goods_num": 99,
            }
            request_payload = {
                "user_id": "u",
                **{key: value for key, value in request.items() if key != "operation_id"},
            }
            with DatabaseUnitOfWork(game) as uow:
                OperationLedger().begin(
                    uow, "mission-claim", "map.mission_claim", request_payload
                )
            MapMissionClaimSqlRepository(game, player, clock=Clock()).claim(
                "mission-claim",
                "u",
                request["expected_mission"],
                request["expected_daily"],
                request["progress_key"],
                request["stone"],
                request["items"],
                request["max_goods_num"],
                {"detail": {"reward_source": "frozen"}},
            )
            with DatabaseUnitOfWork(game, immediate=True) as uow:
                report = ReconcileService().run(
                    uow,
                    operation_handlers={
                        "map.mission_claim": application.reconcile_mission_claim_operation,
                    },
                )
            self.assertEqual((0, 1), (report.operations, report.outbox_events))
            replay = application.mission_claim(
                **request,
                clock=Clock(),
                event_meta={"detail": {"reward_source": "changed"}},
            )

            self.assertTrue(replay.replayed)
            self.assertEqual(["map.mission.effects:mission-claim"], effects.event_ids)
            with DatabaseUnitOfWork(game, read_only=True) as uow:
                payload = json.loads(
                    uow.query_one(
                        "SELECT payload_json FROM domain_outbox WHERE event_id=?",
                        (effects.event_ids[0],),
                    )["payload_json"]
                )
                self.assertEqual("frozen", payload["meta"]["detail"]["reward_source"])

    def test_move_uses_operation_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
            app = MapApplication(game, player, repository=Repo())
            self.assertTrue(app.move(operation_id="map-1", user_id="u").ok)

    def test_default_application_does_not_construct_legacy_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
                uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, user_stamina INTEGER)")
                uow.execute("INSERT INTO user_xiuxian VALUES('u', 20)")
                uow.execute("CREATE TABLE map_movement_operations(operation_id TEXT PRIMARY KEY, payload TEXT, stamina INTEGER)")
            with DatabaseUnitOfWork(player) as uow:
                uow.execute("CREATE TABLE map_status(user_id TEXT PRIMARY KEY, realm TEXT, heaven TEXT, node_id TEXT, visited_nodes TEXT)")
                uow.execute("INSERT INTO map_status VALUES('u', '凡界', '一重天', 'n1', '[\"n1\"]')")

            application = MapApplication(game, player)
            result = application.move(
                operation_id="move-default",
                user_id="u",
                expected_position={"realm": "凡界", "heaven": "一重天", "node_id": "n1"},
                target_position={"realm": "凡界", "heaven": "一重天", "node_id": "n2"},
                expected_stamina=20,
                cost=5,
            )

            self.assertTrue(result.ok)

    def test_interactive_finish_uses_feature_settlement_repository(self):
        with tempfile.TemporaryDirectory() as directory:
            game = Path(directory) / "game.db"
            player = Path(directory) / "player.db"
            with DatabaseUnitOfWork(game) as uow:
                apply_platform_schema(uow)
            with DatabaseUnitOfWork(player) as uow:
                uow.execute(
                    "CREATE TABLE map_interactive_actions("
                    "user_id TEXT PRIMARY KEY, action_id TEXT, status TEXT, "
                    "state_json TEXT, settlement_json TEXT, ready_at TEXT, "
                    "expires_at TEXT, cooldown_seconds INTEGER, updated_at TEXT)"
                )
                uow.execute(
                    "INSERT INTO map_interactive_actions VALUES (?, ?, 'active', ?, '', ?, ?, ?, ?)",
                    (
                        "u",
                        "action-1",
                        json.dumps({"action_id": "action-1", "action": "采集"}),
                        "2026-09-21 12:00:00",
                        "2026-09-21 12:01:00",
                        20,
                        "2026-09-21 11:59:00",
                    ),
                )

            application = MapApplication(game, player)
            settlement = {"daily": {"date": "2026-09-21", "gather_count": 1}}
            first = application.interactive_finish(
                operation_id="finish-1",
                user_id="u",
                action_id="action-1",
                settlement=settlement,
            )
            replay = application.interactive_finish(
                operation_id="finish-1",
                user_id="u",
                action_id="action-1",
                settlement=settlement,
            )

            self.assertTrue(first.ok)
            self.assertEqual(replay.status, "replayed")
            with DatabaseUnitOfWork(player) as uow:
                row = uow.query_one(
                    "SELECT settlement_json FROM map_interactive_actions WHERE action_id = ?",
                    ("action-1",),
                )
            self.assertEqual(json.loads(row["settlement_json"]), settlement)

    def test_combat_settle_delegates_to_combat_feature_application(self):
        with tempfile.TemporaryDirectory() as directory:
            application = MapApplication(Path(directory) / "game.db", Path(directory) / "player.db")
            combat = CombatApplication()
            application._combat_settlement_application = combat
            values = {
                "operation_id": "combat-1",
                "user_id": "u",
                "expected_daily": {"date": "2026-09-21", "combat_count": 1},
                "snapshot": '{"task_id":"combat-1"}',
                "daily_limit": 4,
                "stone": 10,
                "items": ({"id": 1, "name": "材料", "type": "材料", "amount": 2},),
                "max_goods_num": 99,
            }

            self.assertEqual(application.combat_settle(**values), "combat-settlement")
            self.assertEqual(combat.kwargs, values)

    def test_combat_battle_uses_injected_runner_and_preserves_result(self):
        runner = CombatRunner()
        application = MapApplication(
            "game.db",
            "player.db",
            combat_runner=runner,
        )
        enemy = {"name": "守关石傀", "气血": 1000, "攻击": 200}

        result = asyncio.run(
            application.combat_battle(
                user_id="u",
                enemy=enemy,
                bot_id="bot-1",
            )
        )

        self.assertEqual(("u", enemy, "bot-1"), runner.arguments)
        self.assertEqual(
            (["battle"], "群友赢了", {"群友": {"剩余气血": 100}}),
            result,
        )

    def test_combat_battle_propagates_runner_failure(self):
        async def fail(*args, **kwargs):
            raise LookupError("battle engine unavailable")

        application = MapApplication("game.db", "player.db", combat_runner=fail)

        with self.assertRaisesRegex(LookupError, "battle engine unavailable"):
            asyncio.run(
                application.combat_battle(
                    user_id="u",
                    enemy={"name": "守关石傀"},
                    bot_id="bot-1",
                )
            )


if __name__ == "__main__": unittest.main()
