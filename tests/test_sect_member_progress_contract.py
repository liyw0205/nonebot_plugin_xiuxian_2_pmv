from __future__ import annotations

import unittest

from scripts.check_full_refactor_progress import _sect_shop_checked_profile_reuse, _slice_status


class SectMemberProgressContractTests(unittest.TestCase):
    def test_shop_profile_reuse_requires_both_named_handlers_and_checked_source(self):
        shop = """@sect_shop.handle()
async def shop(event):
    registered, user_info, message = check_user(event)
    if not registered:
        return
    sect_id = user_info['sect_id']
"""
        buy = shop.replace("sect_shop.handle", "sect_buy.handle").replace("def shop", "def buy")
        self.assertEqual(2, _sect_shop_checked_profile_reuse(shop + buy))
        self.assertEqual(1, _sect_shop_checked_profile_reuse(shop + shop))
        self.assertEqual(0, _sect_shop_checked_profile_reuse(shop.replace("check_user(event)", "other_profile(event)")))
        self.assertEqual(0, _sect_shop_checked_profile_reuse(shop.replace("sect_shop.handle", "other.handle")))
        self.assertEqual(0, _sect_shop_checked_profile_reuse(shop.replace("    sect_id =", "    user_info = stale\n    sect_id =")))
        self.assertEqual(0, _sect_shop_checked_profile_reuse(shop.replace("['sect_id']", "['user_name']")))

    def test_all_sect_facade_member_lists_are_feature_owned(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["sect_member_list_application_owned"])
        self.assertTrue(sect["sect_member_list_repository_owned"])
        self.assertTrue(sect["sect_member_count_repository_owned"])
        self.assertTrue(sect["sect_member_list_read_only"])
        self.assertTrue(sect["sect_member_utils_application_injected"])
        self.assertTrue(sect["sect_member_utils_info_reads_feature_owned"])
        self.assertTrue(sect["sect_member_utils_join_count_feature_owned"])

    def test_sect_user_profile_reads_are_feature_owned_by_default(self):
        sect = _slice_status()["sect"]
        self.assertTrue(sect["sect_user_profile_repository_owned"])
        self.assertTrue(sect["sect_user_profile_facade_owned"])
        self.assertTrue(sect["sect_user_profile_helper_default_owned"])
        self.assertTrue(sect["sect_user_name_profile_repository_owned"])
        self.assertTrue(sect["sect_user_name_profile_facade_owned"])
        self.assertTrue(sect["sect_weekly_progress_profile_application_owned"])


if __name__ == "__main__":
    unittest.main()
