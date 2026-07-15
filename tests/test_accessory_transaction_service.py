from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_back.transaction_service import (
    AccessoryTransactionService,
)
from tests.test_db_backend import db_backend


class AccessoryTransactionServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.game_database = root / "xiuxian.sqlite3"
        self.player_database = root / "player.sqlite3"
        self.service = AccessoryTransactionService(
            self.game_database, self.player_database
        )
        self.accessory = {
            "uid": "acc-1",
            "name": "测试戒指",
            "quality": 4,
            "affixes": [
                {"type": "攻击", "value": 0.1},
                {"type": "速度", "value": 22},
                {"type": "气血", "value": 0.08},
            ],
            "locked_affixes": [0],
            "wash_count": 3,
        }
        self.second = {
            "uid": "acc-2",
            "name": "测试项链",
            "quality": 2,
            "affixes": [],
            "wash_count": 0,
        }
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TABLE back (user_id TEXT NOT NULL, goods_id INTEGER NOT NULL, "
                "goods_name TEXT, goods_type TEXT, goods_num INTEGER NOT NULL, "
                "bind_num INTEGER DEFAULT 0, UNIQUE (user_id, goods_id))"
            )
            conn.execute(
                "INSERT INTO back VALUES (%s, %s, %s, %s, %s, %s)",
                ("user", 20023, "洗练石", "特殊道具", 20, 20),
            )
        self.write_bag([self.accessory, self.second])

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def write_bag(self, bag) -> None:
        self.write_accessories({}, bag)

    def write_accessories(self, equipped, bag, presets=None) -> None:
        presets = presets or {}
        with db_backend.transaction(self.player_database) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS player_accessory "
                "(user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT, "
                "preset_1 TEXT, preset_2 TEXT, preset_3 TEXT)"
            )
            conn.execute(
                "INSERT INTO player_accessory "
                "(user_id, equipped, bag, preset_1, preset_2, preset_3) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (user_id) DO UPDATE SET "
                "equipped=EXCLUDED.equipped, bag=EXCLUDED.bag, "
                "preset_1=EXCLUDED.preset_1, preset_2=EXCLUDED.preset_2, "
                "preset_3=EXCLUDED.preset_3",
                (
                    "user",
                    json.dumps(equipped),
                    json.dumps(bag),
                    json.dumps(presets.get(1)) if presets.get(1) is not None else None,
                    json.dumps(presets.get(2)) if presets.get(2) is not None else None,
                    json.dumps(presets.get(3)) if presets.get(3) is not None else None,
                ),
            )

    def accessory_state(self):
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute(
                "SELECT equipped,bag FROM player_accessory WHERE user_id=%s",
                ("user",),
            ).fetchone()
        return json.loads(row[0]), json.loads(row[1])

    def preset_state(self, preset_idx):
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute(
                f"SELECT preset_{int(preset_idx)} FROM player_accessory "
                "WHERE user_id=%s",
                ("user",),
            ).fetchone()
        return json.loads(row[0]) if row and row[0] else None

    def state(self):
        with db_backend.connection(self.game_database) as conn:
            row = conn.execute(
                "SELECT goods_num FROM back WHERE user_id=%s AND goods_id=%s",
                ("user", 20023),
            ).fetchone()
            stones = int(row[0]) if row else 0
        with db_backend.connection(self.player_database) as conn:
            row = conn.execute(
                "SELECT bag FROM player_accessory WHERE user_id=%s", ("user",)
            ).fetchone()
            bag = json.loads(row[0]) if row else []
        return stones, bag

    @staticmethod
    def reroll(accessory):
        accessory["affixes"] = [
            accessory["affixes"][0],
            {"type": "会心", "value": 0.09},
            {"type": "防御", "value": 0.07},
        ]
        accessory["wash_count"] += 1
        return accessory

    def wash(self, operation_id="wash-1", expected=None, stones=20):
        return self.service.wash(
            operation_id,
            "user",
            "acc-1",
            expected or self.accessory,
            stones,
            20023,
            16,
            self.reroll,
        )

    def test_wash_preserves_locked_affix_and_is_idempotent(self) -> None:
        first = self.wash()
        duplicate = self.wash()

        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(first.accessory, duplicate.accessory)
        stones, bag = self.state()
        self.assertEqual(stones, 4)
        self.assertEqual(bag[0]["affixes"][0], self.accessory["affixes"][0])
        self.assertEqual(bag[0]["locked_affixes"], [0])
        self.assertEqual(bag[0]["wash_count"], 4)

    def test_wash_rejects_stale_accessory_or_stone_snapshot(self) -> None:
        changed = dict(self.accessory, wash_count=2)
        self.assertEqual(self.wash("wash-stale-item", changed).status, "state_changed")
        self.assertEqual(self.wash("wash-stale-stone", stones=19).status, "state_changed")
        self.assertEqual(self.state()[0], 20)
        self.assertEqual(self.state()[1][0], self.accessory)

    def test_lock_and_unlock_are_idempotent_with_expected_snapshot(self) -> None:
        unlocked = dict(self.accessory)
        unlocked.pop("locked_affixes")
        self.write_bag([unlocked, self.second])

        locked = self.service.set_affix_locks(
            "lock-1", "lock", "user", "acc-1", unlocked, [0, 1]
        )
        duplicate = self.service.set_affix_locks(
            "lock-1", "lock", "user", "acc-1", unlocked, [0, 1]
        )
        self.assertEqual((locked.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual(duplicate.accessory["locked_affixes"], [0, 1])

        unlocked_result = self.service.set_affix_locks(
            "unlock-1", "unlock", "user", "acc-1", locked.accessory, [1]
        )
        unlock_duplicate = self.service.set_affix_locks(
            "unlock-1", "unlock", "user", "acc-1", locked.accessory, [1]
        )
        self.assertEqual(
            (unlocked_result.status, unlock_duplicate.status),
            ("applied", "duplicate"),
        )
        self.assertEqual(self.state()[1][0]["locked_affixes"], [1])

    def test_lock_snapshot_and_operation_payload_conflicts_change_nothing(self) -> None:
        stale = dict(self.accessory, wash_count=2)
        before = self.state()
        stale_result = self.service.set_affix_locks(
            "lock-stale", "lock", "user", "acc-1", stale, [0, 1]
        )
        self.assertEqual(stale_result.status, "state_changed")
        self.assertEqual(self.state(), before)

        applied = self.service.set_affix_locks(
            "lock-conflict", "lock", "user", "acc-1", self.accessory, [0, 1]
        )
        conflict = self.service.set_affix_locks(
            "lock-conflict", "lock", "user", "acc-1", self.accessory, [0]
        )
        self.assertEqual((applied.status, conflict.status), ("applied", "state_changed"))
        self.assertEqual(self.state()[1][0]["locked_affixes"], [0, 1])

    def test_single_decompose_returns_stones_once(self) -> None:
        first = self.service.decompose(
            "decompose-1", "user", "acc-2", self.second,
            20023, "洗练石", 3, 1000,
        )
        duplicate = self.service.decompose(
            "decompose-1", "user", "acc-2", self.second,
            20023, "洗练石", 3, 1000,
        )

        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        stones, bag = self.state()
        self.assertEqual(stones, 23)
        self.assertEqual([item["uid"] for item in bag], ["acc-1"])

    def test_batch_decompose_uses_whole_bag_snapshot(self) -> None:
        expected = [self.accessory, self.second]
        self.write_bag([self.second, self.accessory])
        result = self.service.batch_decompose(
            "batch-stale", "user", expected, ["acc-1", "acc-2"],
            20023, "洗练石", 23, 1000,
        )

        self.assertEqual(result.status, "state_changed")
        self.assertEqual(self.state()[0], 20)
        self.assertEqual(len(self.state()[1]), 2)

    def test_batch_decompose_removes_all_and_returns_total_once(self) -> None:
        first = self.service.batch_decompose(
            "batch-1", "user", [self.accessory, self.second], ["acc-1", "acc-2"],
            20023, "洗练石", 23, 1000,
        )
        duplicate = self.service.batch_decompose(
            "batch-1", "user", [self.accessory, self.second], ["acc-1", "acc-2"],
            20023, "洗练石", 23, 1000,
        )

        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((first.affected, duplicate.affected), (2, 2))
        self.assertEqual(self.state(), (43, []))

    def test_upgrade_consumes_matching_materials_and_replays_result(self) -> None:
        main = {
            "uid": "main",
            "item_id": 101,
            "name": "烈阳项链",
            "part": "项链",
            "set_type": "烈阳",
            "quality": 3,
            "wash_count": 7,
            "affixes": [{"type": "攻击", "value": 0.1}],
            "locked_affixes": [0],
        }
        material_one = dict(main, uid="material-1", wash_count=0)
        material_two = dict(main, uid="material-2", wash_count=0)
        unrelated = dict(main, uid="other", item_id=102)
        equipped = {"项链": main}
        bag = [material_one, unrelated, material_two]
        upgraded = dict(main)
        upgraded.update(
            {
                "quality": 4,
                "wash_count": 0,
                "affixes": [
                    {"type": "攻击", "value": 0.1},
                    {"type": "速度", "value": 20},
                    {"type": "气血", "value": 0.08},
                ],
            }
        )
        self.write_accessories(equipped, bag)

        first = self.service.upgrade(
            "upgrade-1",
            "user",
            "项链",
            equipped,
            bag,
            ["material-1", "material-2"],
            upgraded,
        )
        duplicate = self.service.upgrade(
            "upgrade-1",
            "user",
            "项链",
            equipped,
            bag,
            ["material-1", "material-2"],
            upgraded,
        )
        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((first.affected, duplicate.affected), (2, 2))
        self.assertEqual(first.accessory, duplicate.accessory)
        saved_equipped, saved_bag = self.accessory_state()
        self.assertEqual(4, saved_equipped["项链"]["quality"])
        self.assertEqual(0, saved_equipped["项链"]["wash_count"])
        self.assertEqual(["other"], [item["uid"] for item in saved_bag])

    def test_upgrade_rejects_stale_snapshot_and_invalid_material(self) -> None:
        main = {
            "uid": "main",
            "item_id": 101,
            "name": "烈阳戒指",
            "part": "戒指",
            "set_type": "烈阳",
            "quality": 2,
            "wash_count": 4,
            "affixes": [],
        }
        material = dict(main, uid="material")
        equipped = {"戒指": main}
        bag = [material]
        upgraded = dict(main, quality=3, wash_count=0)
        self.write_accessories(equipped, bag)

        stale = self.service.upgrade(
            "upgrade-stale",
            "user",
            "戒指",
            equipped,
            [],
            ["material"],
            upgraded,
        )
        self.assertEqual("state_changed", stale.status)

        mismatch = dict(material, item_id=999)
        self.write_accessories(equipped, [mismatch])
        invalid = self.service.upgrade(
            "upgrade-mismatch",
            "user",
            "戒指",
            equipped,
            [mismatch],
            ["material"],
            upgraded,
        )
        self.assertEqual("material_mismatch", invalid.status)
        self.assertEqual((equipped, [mismatch]), self.accessory_state())

    def test_save_preset_uses_equipped_snapshot_and_replays(self) -> None:
        bracelet = {"uid": "bracelet", "name": "烈阳手镯", "part": "手镯"}
        ring = {"uid": "ring", "name": "玄渊戒指", "part": "戒指"}
        equipped = {"手镯": bracelet, "戒指": ring, "手环": None, "项链": None}
        old_preset = {
            "手镯": "old", "戒指": None, "手环": None, "项链": None
        }
        self.write_accessories(equipped, [], {1: old_preset})

        first = self.service.save_preset(
            "preset-save-1", "user", 1, equipped, old_preset
        )
        duplicate = self.service.save_preset(
            "preset-save-1", "user", 1, equipped, old_preset
        )
        conflict = self.service.save_preset(
            "preset-save-1", "user", 1, equipped, duplicate.details["preset"]
        )

        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual("state_changed", conflict.status)
        self.assertTrue(duplicate.details["had_old"])
        self.assertEqual(
            {
                "手镯": "bracelet",
                "戒指": "ring",
                "手环": None,
                "项链": None,
            },
            self.preset_state(1),
        )

    def test_save_preset_rejects_stale_equipment_or_preset(self) -> None:
        equipped = {
            "手镯": {"uid": "bracelet", "name": "手镯", "part": "手镯"},
            "戒指": None,
            "手环": None,
            "项链": None,
        }
        empty = {"手镯": None, "戒指": None, "手环": None, "项链": None}
        self.write_accessories(equipped, [], {1: empty})

        stale_equipped = self.service.save_preset(
            "preset-save-stale-equipment", "user", 1, {}, empty
        )
        stale_preset = self.service.save_preset(
            "preset-save-stale-preset",
            "user",
            1,
            equipped,
            dict(empty, 手镯="missing"),
        )

        self.assertEqual("state_changed", stale_equipped.status)
        self.assertEqual("state_changed", stale_preset.status)
        self.assertEqual(empty, self.preset_state(1))

    def test_preset_service_migrates_legacy_accessory_table(self) -> None:
        equipped = {"手镯": None, "戒指": None, "手环": None, "项链": None}
        empty = {"手镯": None, "戒指": None, "手环": None, "项链": None}
        with db_backend.transaction(self.player_database) as conn:
            conn.execute("DROP TABLE player_accessory")
            conn.execute(
                "CREATE TABLE player_accessory "
                "(user_id TEXT PRIMARY KEY, equipped TEXT, bag TEXT)"
            )
            conn.execute(
                "INSERT INTO player_accessory VALUES (%s, %s, %s)",
                ("user", json.dumps(equipped), json.dumps([])),
            )

        result = self.service.save_preset(
            "preset-migrate", "user", 1, equipped, empty
        )

        self.assertEqual("applied", result.status)
        with db_backend.connection(self.player_database) as conn:
            columns = set(conn.column_names("player_accessory"))
        self.assertTrue({"preset_1", "preset_2", "preset_3"}.issubset(columns))
        self.assertEqual(empty, self.preset_state(1))

    def test_quick_equip_is_atomic_idempotent_and_cleans_missing_uid(self) -> None:
        old_bracelet = {
            "uid": "old-bracelet", "name": "旧手镯", "part": "手镯"
        }
        target_bracelet = {
            "uid": "target-bracelet", "name": "新手镯", "part": "手镯"
        }
        current_ring = {"uid": "ring", "name": "戒指", "part": "戒指"}
        equipped = {
            "手镯": old_bracelet,
            "戒指": current_ring,
            "手环": None,
            "项链": None,
        }
        bag = [target_bracelet]
        preset = {
            "手镯": "target-bracelet",
            "戒指": "ring",
            "手环": None,
            "项链": "missing-necklace",
        }
        self.write_accessories(equipped, bag, {2: preset})

        first = self.service.quick_equip_preset(
            "preset-equip-1", "user", 2, equipped, bag, preset
        )
        duplicate = self.service.quick_equip_preset(
            "preset-equip-1", "user", 2, equipped, bag, preset
        )

        self.assertEqual((first.status, duplicate.status), ("applied", "duplicate"))
        self.assertEqual((first.affected, duplicate.affected), (1, 1))
        self.assertEqual(first.details, duplicate.details)
        saved_equipped, saved_bag = self.accessory_state()
        self.assertEqual("target-bracelet", saved_equipped["手镯"]["uid"])
        self.assertEqual(["old-bracelet"], [item["uid"] for item in saved_bag])
        self.assertIsNone(self.preset_state(2)["项链"])
        self.assertEqual(
            [{"slot": "戒指", "reason": "already_equipped"}],
            first.details["skipped"],
        )

    def test_quick_equip_part_mismatch_preserves_accessory(self) -> None:
        wrong_part = {
            "uid": "ring-in-bracelet-slot", "name": "错位戒指", "part": "戒指"
        }
        equipped = {"手镯": None, "戒指": None, "手环": None, "项链": None}
        bag = [wrong_part]
        preset = {
            "手镯": "ring-in-bracelet-slot",
            "戒指": None,
            "手环": None,
            "项链": None,
        }
        self.write_accessories(equipped, bag, {3: preset})

        result = self.service.quick_equip_preset(
            "preset-equip-mismatch", "user", 3, equipped, bag, preset
        )

        self.assertEqual("applied", result.status)
        self.assertEqual(0, result.affected)
        self.assertEqual((equipped, bag), self.accessory_state())
        self.assertEqual(preset, self.preset_state(3))
        self.assertEqual("part_mismatch", result.details["skipped"][0]["reason"])

    def test_operation_write_failure_rolls_back_preset_save_and_equip(self) -> None:
        equipped = {"手镯": None, "戒指": None, "手环": None, "项链": None}
        target = {"uid": "target", "name": "目标手镯", "part": "手镯"}
        bag = [target]
        preset = {"手镯": "target", "戒指": None, "手环": None, "项链": None}
        self.write_accessories(equipped, bag, {1: preset})
        with db_backend.connection(self.game_database) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS player_data", (str(self.player_database),)
            )
            self.service._ensure_schema(conn)
            conn.commit()
            conn.execute("DETACH DATABASE player_data")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_accessory_preset_operation "
                "BEFORE INSERT ON accessory_transaction_operations "
                "WHEN NEW.action IN ('save_preset', 'quick_equip_preset') "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        before = self.accessory_state(), self.preset_state(1)
        with self.assertRaises(db_backend.IntegrityError):
            self.service.save_preset(
                "preset-save-fail", "user", 1, equipped, preset
            )
        self.assertEqual(before, (self.accessory_state(), self.preset_state(1)))
        with self.assertRaises(db_backend.IntegrityError):
            self.service.quick_equip_preset(
                "preset-equip-fail", "user", 1, equipped, bag, preset
            )
        self.assertEqual(before, (self.accessory_state(), self.preset_state(1)))

    def test_operation_write_failure_rolls_back_upgrade(self) -> None:
        main = {
            "uid": "main",
            "item_id": 101,
            "name": "烈阳手镯",
            "part": "手镯",
            "set_type": "烈阳",
            "quality": 1,
            "wash_count": 2,
            "affixes": [],
        }
        material = dict(main, uid="material")
        equipped = {"手镯": main}
        bag = [material]
        upgraded = dict(main, quality=2, wash_count=0)
        self.write_accessories(equipped, bag)
        with db_backend.connection(self.game_database) as conn:
            conn.execute(
                "ATTACH DATABASE %s AS player_data", (str(self.player_database),)
            )
            self.service._ensure_schema(conn)
            conn.commit()
            conn.execute("DETACH DATABASE player_data")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_accessory_upgrade_operation "
                "BEFORE INSERT ON accessory_transaction_operations "
                "WHEN NEW.action='upgrade' "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        with self.assertRaises(db_backend.IntegrityError):
            self.service.upgrade(
                "upgrade-fail",
                "user",
                "手镯",
                equipped,
                bag,
                ["material"],
                upgraded,
            )
        self.assertEqual((equipped, bag), self.accessory_state())

    def test_operation_write_failure_rolls_back_wash_and_decompose(self) -> None:
        with db_backend.connection(self.game_database) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_database),))
            self.service._ensure_schema(conn)
            conn.commit()
            conn.execute("DETACH DATABASE player_data")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_accessory_operation "
                "BEFORE INSERT ON accessory_transaction_operations "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        before = self.state()
        with self.assertRaises(db_backend.IntegrityError):
            self.wash("wash-fail")
        self.assertEqual(self.state(), before)
        with self.assertRaises(db_backend.IntegrityError):
            self.service.decompose(
                "decompose-fail", "user", "acc-2", self.second,
                20023, "洗练石", 3, 1000,
            )
        self.assertEqual(self.state(), before)

    def test_operation_write_failure_rolls_back_lock_and_unlock(self) -> None:
        with db_backend.connection(self.game_database) as conn:
            conn.execute("ATTACH DATABASE %s AS player_data", (str(self.player_database),))
            self.service._ensure_schema(conn)
            conn.commit()
            conn.execute("DETACH DATABASE player_data")
        with db_backend.transaction(self.game_database) as conn:
            conn.execute(
                "CREATE TRIGGER fail_accessory_lock_operation "
                "BEFORE INSERT ON accessory_transaction_operations "
                "WHEN NEW.action IN ('lock', 'unlock') "
                "BEGIN SELECT RAISE(ABORT, 'operation failed'); END"
            )

        before = self.state()
        with self.assertRaises(db_backend.IntegrityError):
            self.service.set_affix_locks(
                "lock-fail", "lock", "user", "acc-1", self.accessory, [0, 1]
            )
        self.assertEqual(self.state(), before)
        with self.assertRaises(db_backend.IntegrityError):
            self.service.set_affix_locks(
                "unlock-fail", "unlock", "user", "acc-1", self.accessory, []
            )
        self.assertEqual(self.state(), before)

    def test_real_lock_and_unlock_handlers_use_transaction_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back/accessory.py"
        ).read_text(encoding="utf-8")
        lock_handler = source.split("@lock_accessory_affix.handle", 1)[1].split(
            "@unlock_accessory_affix.handle", 1
        )[0]
        unlock_handler = source.split("@unlock_accessory_affix.handle", 1)[1].split(
            "@wash_accessory.handle", 1
        )[0]

        self.assertIn("accessory_transaction_service.set_affix_locks", lock_handler)
        self.assertIn("accessory_transaction_service.set_affix_locks", unlock_handler)
        self.assertNotIn("player_data_manager.patch_doc", lock_handler)
        self.assertNotIn("player_data_manager.patch_doc", unlock_handler)

    def test_real_upgrade_handler_uses_transaction_service(self) -> None:
        source = (
            Path(__file__).parents[1]
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back/accessory.py"
        ).read_text(encoding="utf-8")
        handler = source.split("@upgrade_accessory.handle", 1)[1].split(
            "@accessory_preset.handle", 1
        )[0]
        self.assertIn("accessory_transaction_service.replay(", handler)
        self.assertIn("accessory_transaction_service.upgrade(", handler)
        self.assertNotIn("_save_data(", handler)
        self.assertNotIn("del bag[", handler)

    def test_real_preset_handlers_use_transaction_service(self) -> None:
        root = Path(__file__).parents[1]
        source = (
            root / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back/accessory.py"
        ).read_text(encoding="utf-8")
        save_handler = source.split("@accessory_preset.handle", 1)[1].split(
            "@quick_equip_accessory.handle", 1
        )[0]
        equip_handler = source.split("@quick_equip_accessory.handle", 1)[1]
        helpers = (
            root
            / "nonebot_plugin_xiuxian_2/xiuxian/xiuxian_back/accessory_helpers.py"
        ).read_text(encoding="utf-8")

        self.assertIn("accessory_transaction_service.save_preset(", save_handler)
        self.assertNotIn("_save_accessory_preset(", save_handler)
        self.assertIn(
            "accessory_transaction_service.quick_equip_preset(", equip_handler
        )
        self.assertNotIn("player_data_manager.patch_doc(", equip_handler)
        self.assertNotIn("def _save_accessory_preset(", helpers)


if __name__ == "__main__":
    unittest.main()
