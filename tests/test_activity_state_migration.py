from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from nonebot_plugin_xiuxian_2.features.activity.migrations import (
    apply_activity_state_legacy,
    apply_activity_state_schema,
    apply_activity_event_receipts,
)
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.features.activity.point_shop_purchase_repository import (
    ActivityPointShopPurchaseSqlRepository,
)


class ActivityStateMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.database = self.root / "game.db"
        self.legacy_database = self.root / "activity" / "activity.db"
        self.legacy_database.parent.mkdir()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def create_legacy_state(self) -> None:
        with sqlite3.connect(self.legacy_database) as conn:
            conn.executescript(
                """
                CREATE TABLE activity_user(
                    user_id TEXT PRIMARY KEY, sign_days INTEGER NOT NULL,
                    last_sign_date TEXT DEFAULT '', create_time TEXT DEFAULT '',
                    update_time TEXT DEFAULT ''
                );
                CREATE TABLE activity_sign_log(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL,
                    sign_date TEXT NOT NULL, day_index INTEGER NOT NULL DEFAULT 0,
                    reward TEXT DEFAULT '', milestone_reward TEXT DEFAULT '',
                    reward_status TEXT DEFAULT '', reward_message TEXT DEFAULT '',
                    create_time TEXT DEFAULT '', finish_time TEXT DEFAULT '',
                    UNIQUE(user_id, sign_date)
                );
                CREATE TABLE activity_point_balance(
                    activity_key TEXT NOT NULL, user_id TEXT NOT NULL,
                    points INTEGER NOT NULL DEFAULT 0, update_time TEXT DEFAULT '',
                    PRIMARY KEY(activity_key, user_id)
                );
                CREATE TABLE activity_point_purchase_operations(
                    operation_id TEXT PRIMARY KEY, payload TEXT NOT NULL,
                    quantity INTEGER NOT NULL, cost INTEGER NOT NULL,
                    points INTEGER NOT NULL, personal_count INTEGER NOT NULL,
                    total_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            conn.executemany(
                "INSERT INTO activity_user(user_id,sign_days,last_sign_date) VALUES(?,?,?)",
                ((f"u{index}", index % 8, "2026-09-01") for index in range(205)),
            )
            conn.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,day_index,reward_status) "
                "VALUES('u0','2026-09-01',4,'success')"
            )
            conn.execute(
                "INSERT INTO activity_point_balance(activity_key,user_id,points) VALUES('fall','u0',17)"
            )
            conn.execute(
                "INSERT INTO activity_point_purchase_operations"
                "(operation_id,payload,quantity,cost,points,personal_count,total_count) "
                "VALUES('op1','payload',2,100,17,2,5)"
            )

    def migrate(self) -> None:
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
            apply_activity_state_legacy(uow)
            apply_activity_event_receipts(uow)

    def test_backfills_in_bounded_batches_with_defaults_and_stable_audit(self) -> None:
        self.create_legacy_state()

        self.migrate()
        self.migrate()

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(205, uow.query_one("SELECT COUNT(*) AS count FROM activity_user")["count"])
            self.assertEqual(
                (4, 4),
                tuple(uow.execute(
                    "SELECT sign_days,total_sign_days FROM activity_user WHERE user_id='u4'"
                ).fetchone()),
            )
            self.assertEqual(
                (17, 0),
                tuple(uow.execute(
                    "SELECT points,total_points FROM activity_point_balance WHERE user_id='u0'"
                ).fetchone()),
            )
            self.assertEqual(
                205,
                uow.query_one(
                    "SELECT source_rows AS count FROM activity_state_migration_audit "
                    "WHERE table_name='activity_user'"
                )["count"],
            )
            self.assertEqual(
                "success",
                uow.query_one("SELECT reward_status FROM activity_sign_log WHERE user_id='u0'")[
                    "reward_status"
                ],
            )
            self.assertEqual(
                "payload",
                uow.query_one(
                    "SELECT payload FROM activity_point_purchase_operations WHERE operation_id='op1'"
                )["payload"],
            )

        with sqlite3.connect(self.legacy_database) as conn:
            self.assertEqual(205, conn.execute("SELECT COUNT(*) FROM activity_user").fetchone()[0])
            self.assertNotIn(
                "total_sign_days",
                {row[1] for row in conn.execute("PRAGMA table_info(activity_user)")},
            )

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertIsNotNone(uow.query_one("SELECT 1 FROM sqlite_master WHERE name='activity_event_operations'"))

    def test_backfilled_purchase_receipt_replays_without_granting_assets_twice(self) -> None:
        payload = json.dumps(
            ["u0", "fall", "pack", 2, 100, 3, 4, 50, [[101, "活动令", "道具", 2]], 100],
            ensure_ascii=True,
            separators=(",", ":"),
        )
        with sqlite3.connect(self.legacy_database) as conn:
            conn.executescript(
                """
                CREATE TABLE activity_point_balance(
                    activity_key TEXT NOT NULL,user_id TEXT NOT NULL,
                    points INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',
                    PRIMARY KEY(activity_key,user_id)
                );
                CREATE TABLE activity_point_purchase(
                    activity_key TEXT NOT NULL,user_id TEXT NOT NULL,item_key TEXT NOT NULL,
                    count INTEGER NOT NULL DEFAULT 0,update_time TEXT DEFAULT '',
                    PRIMARY KEY(activity_key,user_id,item_key)
                );
                CREATE TABLE activity_point_purchase_operations(
                    operation_id TEXT PRIMARY KEY,payload TEXT NOT NULL,quantity INTEGER NOT NULL,
                    cost INTEGER NOT NULL,points INTEGER NOT NULL,personal_count INTEGER NOT NULL,
                    total_count INTEGER NOT NULL,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            conn.execute(
                "INSERT INTO activity_point_balance VALUES('fall','u0',800,'')"
            )
            conn.execute(
                "INSERT INTO activity_point_purchase VALUES('fall','u0','pack',2,'')"
            )
            conn.execute(
                "INSERT INTO activity_point_purchase_operations"
                "(operation_id,payload,quantity,cost,points,personal_count,total_count) "
                "VALUES('old-purchase',?,2,200,800,2,2)",
                (payload,),
            )

        self.migrate()
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute("CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY,stone INTEGER)")
            uow.execute("INSERT INTO user_xiuxian VALUES('u0',60)")
            uow.execute(
                "CREATE TABLE back(user_id TEXT,goods_id INTEGER,goods_name TEXT,"
                "goods_type TEXT,goods_num INTEGER,create_time TEXT,update_time TEXT,"
                "bind_num INTEGER,UNIQUE(user_id,goods_id))"
            )
            uow.execute("INSERT INTO back VALUES('u0',101,'活动令','道具',2,'','',2)")

        result = ActivityPointShopPurchaseSqlRepository(self.database).purchase(
            operation_id="old-purchase",
            user_id="u0",
            activity_key="fall",
            item_key="pack",
            quantity=2,
            unit_cost=100,
            personal_limit=3,
            stock_limit=4,
            rewards=(
                {"type": "stone", "quantity": 50},
                {"id": 101, "name": "活动令", "type": "道具", "quantity": 2},
            ),
            max_goods_num=100,
        )

        self.assertEqual("duplicate", result.status)
        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertEqual(800, uow.query_one(
                "SELECT points FROM activity_point_balance WHERE user_id='u0'"
            )["points"])
            self.assertEqual(2, uow.query_one(
                "SELECT count FROM activity_point_purchase WHERE user_id='u0'"
            )["count"])
            self.assertEqual(60, uow.query_one(
                "SELECT stone FROM user_xiuxian WHERE user_id='u0'"
            )["stone"])
            self.assertEqual(2, uow.query_one(
                "SELECT goods_num FROM back WHERE user_id='u0'"
            )["goods_num"])

    def test_conflicting_identity_fails_and_rolls_back_backfill(self) -> None:
        self.create_legacy_state()
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)
        with DatabaseUnitOfWork(self.database) as uow:
            uow.execute(
                "INSERT INTO activity_sign_log(user_id,sign_date,day_index) VALUES('u0','2026-09-01',99)"
            )

        with self.assertRaisesRegex(RuntimeError, "activity state migration conflict: activity_sign_log"):
            with DatabaseUnitOfWork(self.database) as uow:
                apply_activity_state_legacy(uow)

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertIsNone(uow.query_one("SELECT 1 FROM activity_user WHERE user_id='u0'"))
            self.assertEqual(
                99,
                uow.query_one("SELECT day_index FROM activity_sign_log WHERE user_id='u0'")[
                    "day_index"
                ],
            )
            self.assertIsNone(
                uow.query_one(
                    "SELECT 1 FROM sqlite_master WHERE name='activity_state_migration_audit'"
                )
            )

    def test_incomplete_identity_schema_fails_closed(self) -> None:
        with sqlite3.connect(self.legacy_database) as conn:
            conn.execute("CREATE TABLE activity_user(sign_days INTEGER)")
        with DatabaseUnitOfWork(self.database) as uow:
            apply_activity_state_schema(uow)

        with self.assertRaisesRegex(RuntimeError, "activity_user missing user_id"):
            with DatabaseUnitOfWork(self.database) as uow:
                apply_activity_state_legacy(uow)

    def test_disk_preflight_rejects_before_creating_target_schema(self) -> None:
        self.create_legacy_state()
        with patch(
            "nonebot_plugin_xiuxian_2.features.activity.migrations.shutil.disk_usage",
            return_value=SimpleNamespace(free=0),
        ):
            with self.assertRaisesRegex(RuntimeError, "activity_state migration needs"):
                with DatabaseUnitOfWork(self.database) as uow:
                    apply_activity_state_schema(uow)

        with DatabaseUnitOfWork(self.database, read_only=True) as uow:
            self.assertIsNone(
                uow.query_one("SELECT 1 FROM sqlite_master WHERE name='activity_user'")
            )


if __name__ == "__main__":
    unittest.main()
