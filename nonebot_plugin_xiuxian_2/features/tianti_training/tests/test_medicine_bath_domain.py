import unittest
from datetime import datetime, timedelta

from ..domain import decide_medicine_bath_activation


class MedicineBathDomainTests(unittest.TestCase):
    def test_active_bath_is_rejected(self):
        now = datetime(2026, 9, 14, 10)
        self.assertEqual(decide_medicine_bath_activation(current_end_time=now + timedelta(minutes=1), now=now, duration_minutes=60, effect=2.0), ("bath_active", None))

    def test_expired_bath_can_start_at_fixed_time(self):
        now = datetime(2026, 9, 14, 10, 0, 1)
        status, end = decide_medicine_bath_activation(current_end_time=datetime(2026, 9, 14, 10), now=now, duration_minutes=60, effect=2.0)
        self.assertEqual(status, "applied")
        self.assertEqual(end, datetime(2026, 9, 14, 11, 0, 1))

    def test_invalid_activation_is_rejected(self):
        with self.assertRaises(ValueError):
            decide_medicine_bath_activation(current_end_time=None, now=datetime(2026, 9, 14, 10), duration_minutes=0, effect=2.0)


if __name__ == "__main__":
    unittest.main()
