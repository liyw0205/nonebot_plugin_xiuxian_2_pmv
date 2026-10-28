from __future__ import annotations

import unittest
from pathlib import Path

from nonebot_plugin_xiuxian_2.features.world_events.contribution_view import (
    find_current_record,
    latest_wave,
    render_personal_contribution,
)


def _record(
    user_id: str,
    damage: int,
    *,
    wave: int = 1,
    realm: str = "练气境",
    attacks: int | None = None,
    **extra,
) -> dict:
    record = {
        "user_id": user_id,
        "realm": realm,
        "wave": wave,
        "damage": damage,
        "attacks": attacks if attacks is not None else 1,
        "name": user_id,
    }
    record.update(extra)
    return record


def _participants(*records: dict) -> dict:
    return {
        f"{record['realm']}:{record.get('wave', 1)}:{record['user_id']}": record
        for record in records
    }


class CurrentRecordLookupTests(unittest.TestCase):
    def test_finds_the_exact_current_record(self) -> None:
        participants = _participants(_record("u1", 10), _record("u2", 20))

        key, record = find_current_record(participants, "u1", "练气境", 1)

        self.assertEqual("练气境:1:u1", key)
        self.assertEqual(10, record["damage"])

    def test_finds_a_legacy_realm_user_key(self) -> None:
        participants = {"练气境:u1": _record("u1", 10, wave=1)}

        key, record = find_current_record(participants, "u1", "练气境", 1)

        self.assertEqual("练气境:u1", key)
        self.assertEqual(10, record["damage"])

    def test_missing_record_returns_empty_pair(self) -> None:
        self.assertEqual(("", {}), find_current_record({}, "u1", "练气境", 1))
        self.assertEqual(
            ("", {}),
            find_current_record(_participants(_record("u2", 10)), "u1", "练气境", 1),
        )

    def test_latest_wave_uses_the_players_highest_recorded_wave(self) -> None:
        participants = _participants(
            _record("u1", 10, wave=1), _record("u1", 20, wave=3), _record("u2", 99, wave=9)
        )

        self.assertEqual(3, latest_wave(participants, "u1", "练气境"))
        self.assertEqual(1, latest_wave(participants, "u3", "练气境"))
        self.assertEqual(5, latest_wave({}, "u1", "练气境", default=5))


class PersonalContributionTests(unittest.TestCase):
    def test_rank_is_visible_outside_the_top_five(self) -> None:
        participants = _participants(
            _record("u1", 100),
            _record("u2", 80),
            _record("u3", 60),
            _record("u4", 40),
            _record("u5", 20),
            _record("u6", 10),
        )

        lines = render_personal_contribution(
            participants, {}, "u6", "练气境", 1, active=True
        )

        self.assertIn("本期出手：1 次，真实伤害 10", lines[1])
        self.assertIn("第 6/6 名", lines[2])
        self.assertIn("本波伤害 10（占 3.2%）", lines[2])
        self.assertIn("本期结束后按贡献结算", lines[3])

    def test_totals_span_waves_but_rank_uses_the_current_wave(self) -> None:
        participants = _participants(
            _record("u1", 10, attacks=2),
            _record("u2", 90),
            _record("u1", 50, wave=2, attacks=3),
        )

        lines = render_personal_contribution(
            participants, {}, "u1", "练气境", 1, active=True
        )

        self.assertIn("本期出手：5 次，真实伤害 60", lines[1])
        self.assertIn("第 2/2 名", lines[2])
        self.assertIn("本波伤害 10（占 10.0%）", lines[2])

    def test_reward_ready_is_claimable(self) -> None:
        participants = _participants(_record("u1", 10, reward_ready=1))

        lines = render_personal_contribution(
            participants, {}, "u1", "练气境", 1, active=True
        )

        self.assertIn("可领取，发送【领取魔修奖励】", lines[-1])

    def test_finished_event_without_claim_points_at_the_claim_command(self) -> None:
        participants = _participants(_record("u1", 10))

        lines = render_personal_contribution(
            participants, {}, "u1", "练气境", 1, active=False
        )

        self.assertIn("本期未领取，发送【领取魔修奖励】", lines[-1])

    def test_claimed_supports_user_record_and_suffix_keys(self) -> None:
        participants = _participants(_record("u1", 10))
        for claimed in (
            {"u1": True},
            {"练气境:1:u1": True},
            {"older-wave:u1": True},
        ):
            with self.subTest(claimed=claimed):
                lines = render_personal_contribution(
                    participants, claimed, "u1", "练气境", 1, active=False
                )
                self.assertIn("本期已领取", lines[-1])

    def test_unrelated_claim_does_not_mark_the_player_claimed(self) -> None:
        participants = _participants(_record("u1", 10))

        lines = render_personal_contribution(
            participants, {"u2": True}, "u1", "练气境", 1, active=False
        )

        self.assertNotIn("已领取", lines[-1])

    def test_no_record_reports_no_participation(self) -> None:
        lines = render_personal_contribution({}, {}, "u1", "练气境", 1, active=True)

        self.assertEqual(("【你的战功】", "本期尚未参与讨伐。"), lines)

    def test_bad_fields_do_not_raise(self) -> None:
        participants = _participants({"user_id": "u1", "realm": "练气境", "damage": "bad", "attacks": None})

        lines = render_personal_contribution(
            participants, {}, "u1", "练气境", 1, active=True
        )

        self.assertIn("真实伤害 0", lines[1])
        self.assertIn("第 1/1 名", lines[2])


class PersonalContributionWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.source = (
            root / "nonebot_plugin_xiuxian_2" / "xiuxian" / "xiuxian_world_events" / "__init__.py"
        ).read_text(encoding="utf-8")

    def test_status_message_appends_the_personal_panel(self) -> None:
        start = self.source.index("def _build_state_message(")
        block = self.source[start : self.source.index("def _extract_total_damage", start)]
        helper_start = self.source.index("def _personal_panel_lines(")
        helper = self.source[helper_start:]

        self.assertIn("_personal_panel_lines(state, user_info, status)", block)
        self.assertIn("本境界本波贡献排行", block)
        self.assertIn("render_personal_contribution(", helper)
        self.assertIn("state.get(\"participants\", {})", helper)
        self.assertIn("state.get(\"claimed\", {})", helper)


if __name__ == "__main__":
    unittest.main()
