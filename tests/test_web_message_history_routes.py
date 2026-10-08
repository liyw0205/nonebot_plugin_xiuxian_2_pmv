from __future__ import annotations

import unittest
from unittest.mock import patch

import tests  # Keep web-module imports on the isolated test data directory.
import nonebot

nonebot.init()

from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import core
from nonebot_plugin_xiuxian_2.xiuxian.xiuxian_web import messages as message_routes


class MessageHistoryRepositoryFake:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.responses = {
            "list_messages": {
                "success": True,
                "total": 17,
                "has_more": True,
                "page": 2,
                "page_size": 10,
                "rows": [{"id": 41, "content": "history row"}],
            },
            "dates": {
                "success": True,
                "rows": [{"date": "2026-10-02", "label": "10月02日", "count": 3}],
            },
            "sessions": {
                "success": True,
                "last_row_id": 84,
                "rows": [{"target_id": "group-42", "last_row_id": 84}],
            },
            "sessions_since": {
                "success": True,
                "last_row_id": 93,
                "rows": [{"target_id": "user-9", "last_row_id": 93}],
            },
            "list_since": {
                "success": True,
                "rows": [{"id": 105, "content": "new row"}],
                "last_row_id": 105,
            },
            "list_before": {
                "success": True,
                "rows": [{"id": 91, "content": "older row"}],
                "has_more": True,
                "oldest_row_id": 91,
            },
        }

    def _result(self, method: str, kwargs: dict) -> dict:
        self.calls.append((method, kwargs))
        return self.responses[method]

    def list_messages(self, **kwargs):
        return self._result("list_messages", kwargs)

    def dates(self, **kwargs):
        return self._result("dates", kwargs)

    def sessions(self, **kwargs):
        return self._result("sessions", kwargs)

    def sessions_since(self, **kwargs):
        return self._result("sessions_since", kwargs)

    def list_since(self, **kwargs):
        return self._result("list_since", kwargs)

    def list_before(self, **kwargs):
        return self._result("list_before", kwargs)


class MessageHistoryRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        core.app.config.update(TESTING=True, SECRET_KEY="message-history-routes")
        self.client = core.app.test_client()
        self.repository = MessageHistoryRepositoryFake()
        self.factory_patch = patch.object(
            message_routes, "_message_history_repository", return_value=self.repository
        )
        self.factory_patch.start()
        self.addCleanup(self.factory_patch.stop)

    def _login(self) -> None:
        with self.client.session_transaction() as session:
            session["admin_id"] = "admin-1"

    def _assert_repository_call(self, method: str, expected: dict, presenter) -> None:
        self.assertEqual(len(self.repository.calls), 1)
        called_method, kwargs = self.repository.calls[0]
        self.assertEqual(called_method, method)
        self.assertEqual({key: value for key, value in kwargs.items() if key != "presenter"}, expected)
        self.assertIs(kwargs.get("presenter"), presenter)

    def test_history_routes_keep_anonymous_error_envelope(self) -> None:
        paths = (
            "/api/messages/list",
            "/api/messages/dates",
            "/api/messages/sessions",
            "/api/messages/sessions_since",
            "/api/messages/list_since",
            "/api/messages/list_before",
        )
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
            message_routes,
            "get_message_db_connection",
            side_effect=AssertionError("history route must not open the legacy connection"),
        ) as legacy_connection:
            for path in paths:
                with self.subTest(path=path):
                    response = self.client.get(path)
                    self.assertEqual(response.status_code, 401)
                    self.assertEqual(response.get_json(), {"success": False, "error": "未登录"})

        self.assertEqual(self.repository.calls, [])
        legacy_connection.assert_not_called()

    def test_list_route_preserves_response_and_maps_filters_to_repository(self) -> None:
        self._login()
        expected_response = self.repository.responses["list_messages"]
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
            message_routes,
            "get_message_db_connection",
            side_effect=AssertionError("history route must not open the legacy connection"),
        ) as legacy_connection:
            response = self.client.get(
                "/api/messages/list?scene=private&direction=recv&keyword=needle"
                "&group_id=group-7&user_id=user-8&adapter=QQ"
                "&start=2026-10-01T10:00&end=2026-10-02T11:00&date=2026-10-02"
                "&page=2&page_size=10&include_total=false"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), expected_response)
        self._assert_repository_call(
            "list_messages",
            {
                "scene": "private",
                "direction": "recv",
                "keyword": "needle",
                "group_id": "group-7",
                "user_id": "user-8",
                "adapter": "QQ",
                "start": "2026-10-01T10:00",
                "end": "2026-10-02T11:00",
                "date": "2026-10-02",
                "page": 2,
                "page_size": 10,
                "include_total": False,
            },
            message_routes._prepare_message_rows,
        )
        legacy_connection.assert_not_called()

    def test_dates_route_preserves_response_and_maps_count_option(self) -> None:
        self._login()
        expected_response = self.repository.responses["dates"]
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
            message_routes,
            "get_message_db_connection",
            side_effect=AssertionError("history route must not open the legacy connection"),
        ) as legacy_connection:
            response = self.client.get(
                "/api/messages/dates?scene=channel_group&target_id=group-42"
                "&adapter=QQ&include_counts=no"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), expected_response)
        self._assert_repository_call(
            "dates",
            {
                "scene": "channel_group",
                "target_id": "group-42",
                "adapter": "QQ",
                "include_counts": False,
            },
            None,
        )
        legacy_connection.assert_not_called()

    def test_sessions_route_passes_scene_adapter_and_presenter(self) -> None:
        self._login()
        expected_response = self.repository.responses["sessions"]
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
            message_routes,
            "get_message_db_connection",
            side_effect=AssertionError("history route must not open the legacy connection"),
        ) as legacy_connection:
            response = self.client.get("/api/messages/sessions?scene=channel_group&adapter=QQ")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), expected_response)
        self._assert_repository_call(
            "sessions",
            {"scene": "channel_group", "adapter": "QQ"},
            message_routes._prepare_session_rows,
        )
        legacy_connection.assert_not_called()

    def test_sessions_since_route_clamps_after_id_and_passes_presenter(self) -> None:
        self._login()
        expected_response = self.repository.responses["sessions_since"]
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
            message_routes,
            "get_message_db_connection",
            side_effect=AssertionError("history route must not open the legacy connection"),
        ) as legacy_connection:
            response = self.client.get(
                "/api/messages/sessions_since?scene=private&adapter=QQ&after_id=17"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), expected_response)
        self._assert_repository_call(
            "sessions_since",
            {"scene": "private", "adapter": "QQ", "after_id": 17},
            message_routes._prepare_session_rows,
        )
        legacy_connection.assert_not_called()

    def test_list_since_route_preserves_response_and_maps_target_filters(self) -> None:
        self._login()
        expected_response = self.repository.responses["list_since"]
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
            message_routes,
            "get_message_db_connection",
            side_effect=AssertionError("history route must not open the legacy connection"),
        ) as legacy_connection:
            response = self.client.get(
                "/api/messages/list_since?scene=private&target_id=user-9"
                "&adapter=QQ&date=2026-10-02&last_row_id=104"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), expected_response)
        self._assert_repository_call(
            "list_since",
            {
                "scene": "private",
                "target_id": "user-9",
                "adapter": "QQ",
                "date": "2026-10-02",
                "last_row_id": 104,
            },
            message_routes._prepare_message_rows,
        )
        legacy_connection.assert_not_called()

    def test_list_before_route_clamps_page_size_and_passes_presenter(self) -> None:
        self._login()
        expected_response = self.repository.responses["list_before"]
        with patch.object(core, "ADMIN_IDS", {"admin-1"}), patch.object(
            message_routes,
            "get_message_db_connection",
            side_effect=AssertionError("history route must not open the legacy connection"),
        ) as legacy_connection:
            response = self.client.get(
                "/api/messages/list_before?scene=group&target_id=group-42"
                "&adapter=QQ&keyword=needle&date=2026-10-02"
                "&before_row_id=92&page_size=20"
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), expected_response)
        self._assert_repository_call(
            "list_before",
            {
                "scene": "group",
                "target_id": "group-42",
                "adapter": "QQ",
                "keyword": "needle",
                "date": "2026-10-02",
                "before_row_id": 92,
                "page_size": 50,
            },
            message_routes._prepare_message_rows,
        )
        legacy_connection.assert_not_called()


if __name__ == "__main__":
    unittest.main()
