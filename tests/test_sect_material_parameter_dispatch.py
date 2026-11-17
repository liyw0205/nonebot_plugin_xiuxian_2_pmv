import unittest
from tests import test_guishi_parameter_dispatch as dispatch


class SectMaterialParameterDispatchTests(unittest.TestCase):
    check_group = dispatch.GuishiParameterDispatchTests.check_group

    def test_scheduled_grant_player_reads_and_write_recovery(self):
        self.check_group('sect-material-schedule', 2, {'success': 2})

    def test_daily_maintenance_player_claim_and_write_recovery(self):
        self.check_group('sect-daily-maintenance', 2, {'success': 2})

    def test_elixir_claim_write_recovery_and_multiple_rewards(self):
        self.check_group('sect-elixir-claim', 2, {'success': 2})

    def test_daily_task_handoff_replay_and_write_recovery(self):
        self.check_group('sect-task-daily', 2, {'success': 2})

    def test_daily_task_stamina_handoff_and_write_recovery(self):
        self.check_group('sect-task-stamina', 1, {'success': 1})

    def test_scheduled_stamina_recovery_and_task_handoff(self):
        self.check_group('stamina-recovery', 1, {'success': 1})
