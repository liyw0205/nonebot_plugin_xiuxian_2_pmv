import unittest
from tests import test_guishi_parameter_dispatch as dispatch


class WorldBossParameterDispatchTests(unittest.TestCase):
    check_group = dispatch.GuishiParameterDispatchTests.check_group

    def test_refresh_player_and_write_recovery(self):
        self.check_group('world-boss-refresh', 3, {'success': 2, 'rejection': 1})
