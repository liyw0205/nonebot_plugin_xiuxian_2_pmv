"""Receipt storage contract for pet hatch replays.

The live deployment's ``pet_hatch_operations`` was the single largest object in
``xiuxian.db`` (94.2 MB of 290 MB, 4,600 rows, 18.4 KB per row) because
``_normalize_pet`` sets ``pet["skill"] = pet["skills"][0]`` and the receipt
serialised the fully materialised roster, writing the same 33-key skill
definition twice per pet.  These tests pin the compact v2 receipt:

* every pet key survives a round trip -- including multiple skills, a ``skill``
  that is not ``skills[0]``, non-mapping skill entries and unknown instance
  fields -- while skill *definitions* are deduplicated per receipt;
* legacy v1 rows keep decoding, a catalog rename cannot rewrite stored display
  text, and a corrupt or unknown-version row raises instead of pretending the
  operation returned no pets;
* the byte reduction is measured on realistic ten-pet batches, and the replay
  path keeps its behaviour: one stone charge, same pets, same result.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.compatibility.legacy_pet_transactions import PetHatchService
from nonebot_plugin_xiuxian_2.features.pet.migrations import apply_pet_hatch
from nonebot_plugin_xiuxian_2.features.pet.receipt_codec import (
    RECEIPT_VERSION,
    ReceiptDecodeError,
    decode_receipt,
    encode_receipt,
)
from nonebot_plugin_xiuxian_2.features.pet.repository import PetHatchSqlRepository
from nonebot_plugin_xiuxian_2.infrastructure.database import DatabaseUnitOfWork
from nonebot_plugin_xiuxian_2.plugin import apply_platform_schema

# The 33 keys a real skill definition carries in data/xiuxian/宠物/*.json.
_CATALOG_SKILL = {
    "skill_id": "skill_0021",
    "name": "青木回春",
    "type": "增益",
    "scope": "自身",
    "category": "木系",
    "min_rarity": "常见",
    "min_form": 0,
    "min_stars": 1,
    "base_power": 120,
    "star_bonus": 0.15,
    "form_bonus": 0.05,
    "rarity_bonus": 0.1,
    "weight": 30,
    "exclusive_pet_ids": [],
    "desc": "回复自身气血",
    "effect": "heal",
    "target_scope": "self",
    "target_count": 1,
    "hit_count": 1,
    "duration": 0,
    "success": 0.9,
    "min_power": 60,
    "max_power": 180,
    "hp_percent": 0.12,
    "shield_penetration": 0,
    "dot_power": 0,
    "buff_scale": 0,
    "shield_scale": 0,
    "reflect_scale": 0,
    "buff_type": "",
    "debuff_type": "",
    "control_type": "",
    "dot_type": "",
}

_DISPLAY_FIELDS = ("uid", "pet_id", "name", "rarity", "race", "type", "forms", "stars",
                   "exp", "total_exp", "max_stars", "form_index", "form_name")


def _pet(index: int, *, skill: dict | None = None, active: bool = False) -> dict:
    """A pet shaped exactly like ``_normalize_pet`` output."""

    skill = dict(_CATALOG_SKILL if skill is None else skill)
    return {
        "uid": f"pet_1785886393_{2000 + index}",
        "pet_id": f"pet_{index:03d}",
        "name": "青羽雀",
        "rarity": "常见",
        "race": "凡兽",
        "type": "增益",
        "forms": ["青羽雏雀", "青羽雀", "青羽灵雀", "青羽王雀", "青羽神雀"],
        "stars": 1,
        "exp": 0,
        "total_exp": 0,
        "max_stars": 5,
        "form_index": 0,
        "form_name": "青羽雏雀",
        "skills": [skill],
        "skill": skill,
        "active": active,
    }


def _legacy_document(pets) -> str:
    """The v1 writer exactly as it was before the codec existed."""

    return json.dumps(
        {
            "pets": [[pet, bool(active)] for pet, active in pets],
            "updated_meta": ["pet_1785886393_2000", 1, 0],
            "bag_limit": 1000,
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )


class ReceiptCodecTests(unittest.TestCase):
    def test_round_trip_preserves_every_pet_key(self) -> None:
        pets = [(_pet(1), False), (_pet(2, active=True), True)]
        decoded, meta, limit = decode_receipt(encode_receipt(pets, ["u", 2, 1], 1000))
        self.assertEqual(limit, 1000)
        self.assertEqual(list(meta), ["u", 2, 1])
        self.assertEqual(len(decoded), 2)
        for (source, source_active), (restored, restored_active) in zip(pets, decoded):
            self.assertEqual(set(restored), set(source))
            self.assertEqual(restored_active, source_active)
            for key in _DISPLAY_FIELDS:
                self.assertEqual(restored[key], source[key], msg=key)
            self.assertEqual(restored["skill"]["skill_id"], "skill_0021")
            self.assertEqual(restored["skill"]["name"], "青木回春")
            self.assertEqual(restored["skills"], [restored["skill"]])

    def test_multiple_skills_and_divergent_primary_are_preserved(self) -> None:
        """``skill`` need not equal ``skills[0]``; both lists must survive."""

        second = dict(_CATALOG_SKILL, skill_id="skill_0100", name="烈焰焚天", effect="burn")
        pet = _pet(1)
        pet["skill"] = dict(_CATALOG_SKILL, skill_id="skill_0777", name="独立主技能")
        pet["skills"] = [dict(_CATALOG_SKILL), second]
        decoded, _, _ = decode_receipt(encode_receipt([(pet, False)]))
        stored = decoded[0][0]
        self.assertEqual(len(stored["skills"]), 2)
        self.assertEqual([s["skill_id"] for s in stored["skills"]], ["skill_0021", "skill_0100"])
        self.assertEqual([s["name"] for s in stored["skills"]], ["青木回春", "烈焰焚天"])
        self.assertEqual(stored["skill"]["skill_id"], "skill_0777")
        self.assertEqual(stored["skill"]["name"], "独立主技能")

    def test_duplicate_skills_inside_one_pet_are_deduplicated_but_kept_twice(self) -> None:
        pet = _pet(1)
        pet["skills"] = [dict(_CATALOG_SKILL), dict(_CATALOG_SKILL)]
        document = json.loads(encode_receipt([(pet, False)]))
        self.assertEqual(len(document["skills"]), 1)
        stored = decode_receipt(document)[0][0][0]
        self.assertEqual(len(stored["skills"]), 2)
        self.assertEqual(stored["skills"][0], stored["skills"][1])

    def test_non_mapping_skill_entries_survive_verbatim(self) -> None:
        pet = _pet(1)
        pet["skills"] = [_CATALOG_SKILL, "legacy-skill-token", 17]
        pet["skill"] = "legacy-skill-token"
        stored = decode_receipt(encode_receipt([(pet, False)]))[0][0][0]
        self.assertEqual(stored["skills"][1], "legacy-skill-token")
        self.assertEqual(stored["skills"][2], 17)
        self.assertEqual(stored["skill"], "legacy-skill-token")

    def test_unknown_instance_fields_are_preserved(self) -> None:
        pet = _pet(1)
        pet["bond"] = {"mate": "pet_9", "since": 1785886393}
        pet["affix_rolls"] = [{"id": "a1", "value": 3}]
        pet["custom_name"] = "小翠"
        stored = decode_receipt(encode_receipt([(pet, False)]))[0][0][0]
        self.assertEqual(stored["bond"], pet["bond"])
        self.assertEqual(stored["affix_rolls"], pet["affix_rolls"])
        self.assertEqual(stored["custom_name"], "小翠")

    def test_catalog_attributes_are_not_duplicated_into_the_receipt(self) -> None:
        payload = encode_receipt([(_pet(1), False)])
        self.assertNotIn("base_power", payload)
        self.assertNotIn("hp_percent", payload)
        for frozen in ("skill_0021", "青木回春", "增益", "自身", "木系", "回复自身气血"):
            self.assertIn(frozen, payload)

    def test_skill_definition_is_stored_once_per_receipt(self) -> None:
        pets = [(_pet(i), False) for i in range(10)]
        document = json.loads(encode_receipt(pets))
        self.assertEqual(document["v"], RECEIPT_VERSION)
        self.assertEqual(len(document["skills"]), 1)
        self.assertEqual([row[2] for row in document["pets"]], [0] * 10)

    def test_distinct_skills_get_distinct_slots(self) -> None:
        other = dict(_CATALOG_SKILL, skill_id="skill_0099", name="烈焰焚天")
        document = json.loads(encode_receipt([(_pet(1), False), (_pet(2, skill=other), False)]))
        self.assertEqual(len(document["skills"]), 2)
        self.assertEqual([row[2] for row in document["pets"]], [0, 1])

    def test_ten_pet_batch_shrinks_below_a_third_of_the_legacy_receipt(self) -> None:
        pets = [(_pet(i), False) for i in range(10)]
        legacy = len(_legacy_document(pets).encode("utf-8"))
        compact = len(encode_receipt(pets).encode("utf-8"))
        self.assertLess(compact, legacy * 0.35, msg=f"legacy={legacy} compact={compact}")
        # The removed mass is the duplicated skill definition, not instance state.
        self.assertGreater(legacy - compact, 10 * 600)

    def test_legacy_document_still_decodes(self) -> None:
        pets = [(_pet(1), False), (_pet(2, active=True), True)]
        decoded, meta, limit = decode_receipt(_legacy_document(pets))
        self.assertEqual(decoded, tuple((dict(pet), active) for pet, active in pets))
        self.assertEqual(list(meta), ["pet_1785886393_2000", 1, 0])
        self.assertEqual(limit, 1000)

    def test_empty_legacy_document_is_an_empty_result_not_an_error(self) -> None:
        self.assertEqual(decode_receipt('{"pets": [], "updated_meta": [], "bag_limit": 5}'),
                         ((), (), 5))

    def test_corrupt_or_unknown_receipts_raise_instead_of_inventing_success(self) -> None:
        corrupt = [
            None,
            "",
            "   ",
            "not json",
            "[]",
            '"text"',
            "not-an-object",
            {"v": RECEIPT_VERSION},
            {"v": RECEIPT_VERSION, "pets": [], "skills": "not-a-list"},
            {"v": RECEIPT_VERSION, "pets": [["short"]], "skills": []},
            {"v": RECEIPT_VERSION, "pets": [[{"a": 1}, False, 0, None, 9]], "skills": []},
            {"v": RECEIPT_VERSION, "pets": [[{"a": 1}, False, 5, None, 1]], "skills": []},
            {"v": 99, "pets": [], "skills": []},
            {"pets": "nope"},
            {"pets": [["not-a-pet", True]]},
        ]
        for value in corrupt:
            with self.subTest(value=value):
                with self.assertRaises(ReceiptDecodeError):
                    decode_receipt(value)

    def test_skill_slot_out_of_range_raises(self) -> None:
        document = json.loads(encode_receipt([(_pet(1), False)]))
        document["pets"][0][2] = 7
        with self.assertRaises(ReceiptDecodeError):
            decode_receipt(document)

    def test_pets_without_skill_keys_do_not_gain_them(self) -> None:
        decoded, _, _ = decode_receipt(encode_receipt([({"uid": "p1", "name": "灵狐"}, True)]))
        self.assertEqual(decoded, (({"uid": "p1", "name": "灵狐"}, True),))

    def test_explicit_none_skill_values_are_preserved(self) -> None:
        pet = {"uid": "p1", "skill": None, "skills": None}
        stored = decode_receipt(encode_receipt([(pet, False)]))[0][0][0]
        self.assertEqual(stored, {"uid": "p1", "skill": None, "skills": None})

    def test_single_skill_key_is_restored_without_a_skills_list(self) -> None:
        pet = _pet(1)
        pet.pop("skills")
        decoded, _, _ = decode_receipt(encode_receipt([(pet, False)]))
        self.assertIn("skill", decoded[0][0])
        self.assertNotIn("skills", decoded[0][0])

    def test_renamed_or_deleted_catalog_skill_keeps_frozen_display_text(self) -> None:
        stored = encode_receipt([(_pet(1), False)])
        # The catalog later renames and reshapes the skill; the receipt is history.
        renamed = dict(_CATALOG_SKILL, name="青木回春·改", base_power=999)
        self.assertNotEqual(_CATALOG_SKILL["name"], renamed["name"])
        decoded, _, _ = decode_receipt(stored)
        self.assertEqual(decoded[0][0]["skill"]["name"], "青木回春")
        self.assertEqual(decoded[0][0]["skill"]["skill_id"], "skill_0021")
        # An unknown id is equally safe: the frozen name is all replay needs.
        unknown = dict(_CATALOG_SKILL, skill_id="skill_deleted")
        decoded_unknown, _, _ = decode_receipt(encode_receipt([(_pet(2, skill=unknown), False)]))
        self.assertEqual(decoded_unknown[0][0]["skill"]["name"], "青木回春")


class HatchRepositoryReceiptTests(unittest.TestCase):
    def build(self, temp: str, *, stone: int = 5000) -> tuple[Path, Path]:
        game = Path(temp) / "game.db"
        player = Path(temp) / "player.db"
        with DatabaseUnitOfWork(game) as uow:
            apply_platform_schema(uow)
            apply_pet_hatch(uow)
            uow.execute(
                "CREATE TABLE user_xiuxian(user_id TEXT PRIMARY KEY, stone INTEGER, "
                "exp INTEGER, hp INTEGER, mp INTEGER)"
            )
            uow.execute(
                "INSERT INTO user_xiuxian(user_id,stone,exp,hp,mp) VALUES('u',?,0,100,100)",
                (stone,),
            )
        with DatabaseUnitOfWork(player) as uow:
            uow.execute(
                "CREATE TABLE player_pet(user_id TEXT PRIMARY KEY, active_uid TEXT, "
                "egg_pity_count INTEGER DEFAULT 0, egg_pity_no_mythic_count INTEGER DEFAULT 0, "
                "travel TEXT)"
            )
            uow.execute(
                "CREATE TABLE player_pet_item(id TEXT PRIMARY KEY, user_id TEXT, uid TEXT, "
                "is_active INTEGER, pet_id TEXT, stars INTEGER, exp INTEGER, total_exp INTEGER, "
                "skill_id TEXT, created_at INTEGER, updated_at INTEGER)"
            )
        return game, player

    def _hatch(self, game: Path, player: Path, operation_id: str = "pet-hatch:1:u"):
        repository = PetHatchSqlRepository(game, player)
        pets = [(_pet(index), index == 0) for index in range(10)]
        return repository, pets, repository.hatch(
            operation_id,
            "u",
            5000,
            1000,
            ["", 0, 0, None],
            pets,
            ["pet_1785886393_2000", 10, 0],
            1000,
        )

    def test_new_receipt_is_compact_and_far_smaller_than_legacy(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            repository, pets, result = self._hatch(game, player)
            self.assertEqual(result.status, "applied")
            with DatabaseUnitOfWork(game) as uow:
                row = uow.query_one(
                    "SELECT payload,result_json FROM pet_hatch_operations WHERE operation_id=?",
                    ("pet-hatch:1:u",),
                )
            stored = row["result_json"]
            self.assertEqual(json.loads(stored)["v"], RECEIPT_VERSION)
            legacy = len(_legacy_document(pets).encode("utf-8"))
            self.assertLess(len(stored.encode("utf-8")), legacy * 0.35)

    def test_replay_charges_once_and_returns_the_same_pets(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            repository, pets, first = self._hatch(game, player)
            replay = repository.hatch(
                "pet-hatch:1:u", "u", 4000, 1000, ["", 0, 0, None], pets,
                ["pet_1785886393_2000", 10, 0], 1000,
            )
            self.assertEqual(replay.status, "duplicate")
            # A receipt freezes what the player saw plus the instance numbers; it
            # deliberately does not re-materialise catalog-owned skill attributes.
            self.assertEqual(len(replay.pets), len(first.pets))
            for (fresh, fresh_active), (stored, stored_active) in zip(first.pets, replay.pets):
                self.assertEqual(stored_active, fresh_active)
                for key in _DISPLAY_FIELDS:
                    self.assertEqual(stored[key], fresh[key], msg=key)
                self.assertEqual(stored["skill"]["skill_id"], fresh["skill"]["skill_id"])
                self.assertEqual(stored["skill"]["name"], fresh["skill"]["name"])
            self.assertEqual(list(replay.updated_meta), list(first.updated_meta))
            with DatabaseUnitOfWork(game) as uow:
                stone = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
                count = uow.query_one("SELECT COUNT(*) AS n FROM pet_hatch_operations")["n"]
            self.assertEqual(stone, 4000)  # one charge only
            self.assertEqual(count, 1)
            prior = repository.get_result("pet-hatch:1:u")
            self.assertEqual(prior.status, "duplicate")
            self.assertEqual(prior.pets, replay.pets)

    def test_changed_payload_still_reports_state_changed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            repository, _, _ = self._hatch(game, player)
            changed = repository.hatch(
                "pet-hatch:1:u", "u", 4000, 1000, ["", 0, 0, None],
                [(_pet(99), False)], ["pet_1785886393_2000", 10, 0], 1000,
            )
            self.assertEqual(changed.status, "state_changed")

    def test_corrupt_receipt_fails_closed_without_a_second_charge(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp, stone=5000)
            payload = json.dumps(["u", 1000, 1], separators=(",", ":"))
            with DatabaseUnitOfWork(game) as uow:
                uow.execute(
                    "INSERT INTO pet_hatch_operations(operation_id,payload,result_json) VALUES(?,?,?)",
                    ("pet-hatch:broken:u", payload, "corrupt-json"),
                )
            repository = PetHatchSqlRepository(game, player)
            prior = repository.get_result("pet-hatch:broken:u")
            self.assertEqual(prior.status, "receipt_unreadable")
            self.assertFalse(prior.succeeded)
            result = repository.hatch(
                "pet-hatch:broken:u", "u", 5000, 1000, ["", 0, 0, None],
                [(_pet(1), False)], ["pet_1785886393_2000", 10, 0], 1000,
            )
            self.assertEqual(result.status, "receipt_unreadable")
            self.assertFalse(result.succeeded)
            self.assertEqual(result.pets, ())
            with DatabaseUnitOfWork(game) as uow:
                uow.attach_database(player, "player_data")
                stone = uow.query_one("SELECT stone FROM user_xiuxian WHERE user_id='u'")["stone"]
                owned = uow.query_one("SELECT COUNT(*) AS n FROM player_data.player_pet_item")["n"]
                receipts = uow.query_one("SELECT COUNT(*) AS n FROM pet_hatch_operations")["n"]
            self.assertEqual(stone, 5000, "a corrupt receipt must never charge again")
            self.assertEqual(owned, 0)
            self.assertEqual(receipts, 1)

    def test_unknown_version_receipt_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            future = json.dumps({"v": 99, "pets": [], "skills": []}, separators=(",", ":"))
            with DatabaseUnitOfWork(game) as uow:
                uow.execute(
                    "INSERT INTO pet_hatch_operations(operation_id,payload,result_json) VALUES(?,?,?)",
                    ("pet-hatch:future:u", json.dumps(["u", 1000, 1], separators=(",", ":")), future),
                )
            repository = PetHatchSqlRepository(game, player)
            self.assertEqual(repository.get_result("pet-hatch:future:u").status, "receipt_unreadable")

    def test_legacy_row_planted_by_the_old_writer_is_readable(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            game, player = self.build(temp)
            pets = [(_pet(1), False), (_pet(2, active=True), True)]
            legacy = _legacy_document(pets)
            with DatabaseUnitOfWork(game) as uow:
                uow.execute(
                    "INSERT INTO pet_hatch_operations(operation_id,payload,result_json) VALUES(?,?,?)",
                    ("pet-hatch:legacy:u", json.dumps(["u", 1000, 2], separators=(",", ":")), legacy),
                )
            repository = PetHatchSqlRepository(game, player)
            prior = repository.get_result("pet-hatch:legacy:u")
            self.assertEqual(prior.status, "duplicate")
            self.assertEqual(prior.pets, tuple((dict(pet), active) for pet, active in pets))
            self.assertEqual(prior.bag_limit, 1000)

    def test_compatibility_shim_reads_both_shapes_and_rejects_corruption(self) -> None:
        pets = [(_pet(1), False)]
        compact = encode_receipt(pets, ["u", 3, 0], 1000)
        for payload in (_legacy_document(pets), compact):
            with self.subTest(payload=payload[:16]):
                result = PetHatchService._result_from_row(
                    "duplicate", json.dumps(["u", 1000, 1], separators=(",", ":")), payload
                )
                self.assertEqual(result.cost, 1000)
                self.assertEqual(result.bag_limit, 1000)
                self.assertEqual(result.pets[0][0]["name"], "青羽雀")
                self.assertEqual(result.pets[0][0]["skill"]["name"], "青木回春")
        broken = PetHatchService._result_from_row(
            "duplicate", json.dumps(["u", 1000, 1], separators=(",", ":")), "not-json"
        )
        self.assertEqual(broken.status, "receipt_unreadable")
        self.assertFalse(broken.succeeded)


if __name__ == "__main__":
    unittest.main()
