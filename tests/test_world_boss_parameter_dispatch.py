import unittest
from tests import test_guishi_parameter_dispatch as dispatch


class WorldBossParameterDispatchTests(unittest.TestCase):
    check_group = dispatch.GuishiParameterDispatchTests.check_group

    def test_purchase_player_lifecycle_and_write_recovery(self):
        self.check_group('world-boss-purchase', 3, {'success': 3})

    def test_refresh_player_and_write_recovery(self):
        self.check_group('world-boss-refresh', 3, {'success': 2, 'rejection': 1})

    def test_battle_write_rejection_and_ledger_recovery(self):
        self.check_group('world-boss-battle-recovery', 1, {'success': 1})

    def test_daily_reset_player_and_write_recovery(self):
        self.check_group('world-boss-daily-reset', 2, {'success': 2})

    def test_manual_spawn_session_parameters_and_write_recovery(self):
        self.check_group('world-boss-manual-spawn', 3, {'success': 3})
