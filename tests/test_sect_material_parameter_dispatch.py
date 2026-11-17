import unittest
from tests import test_guishi_parameter_dispatch as dispatch


class SectMaterialParameterDispatchTests(unittest.TestCase):
    check_group = dispatch.GuishiParameterDispatchTests.check_group

    def test_scheduled_grant_player_reads_and_write_recovery(self):
        self.check_group('sect-material-schedule', 2, {'success': 2})
