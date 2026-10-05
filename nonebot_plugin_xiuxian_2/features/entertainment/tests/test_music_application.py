from __future__ import annotations

import unittest

from ..external_query import EntertainmentExternalQueryProvider
from ..music_application import (
    MUSIC_SEARCH_RESPONSE_MAX_BYTES,
    MUSIC_SEARCH_TOTAL_BUDGET_SECONDS,
    EntertainmentMusicApplication,
)


def _songs(start: int, count: int) -> list[dict[str, object]]:
    return [
        {
            "songid": str(index),
            "title": f"Song {index}",
            "author": f"Artist {index}",
            "url": f"https://cdn.invalid/{index}.mp3",
            "pic": f"http://img.invalid/{index}.jpg",
            "link": f"https://music.invalid/{index}",
            "type": "netease",
            "lrc": f"[00:01.00]line {index}",
        }
        for index in range(start, start + count)
    ]


class _ExternalQueries:
    def __init__(self, responses, *, now=None, advance=0.0):
        self.responses = list(responses)
        self.now = now
        self.advance = advance
        self.calls: list[tuple[str, dict, dict]] = []

    def post_form_json(self, url, data, **kwargs):
        self.calls.append((url, data, kwargs))
        if self.now is not None:
            self.now[0] += self.advance
        return self.responses.pop(0)


class EntertainmentMusicApplicationTests(unittest.TestCase):
    def test_search_is_paged_deduplicated_and_returns_bounded_song_dtos(self):
        queries = _ExternalQueries(
            [
                {"data": _songs(1, 10)},
                {"data": _songs(11, 10)},
                {"data": _songs(21, 2)},
            ]
        )
        app = EntertainmentMusicApplication(queries)  # type: ignore[arg-type]
        app.set_config("song_limit", "21")

        songs = app.search("track", "netease")

        self.assertEqual(len(songs), 21)
        self.assertEqual([call[1]["page"] for call in queries.calls], [1, 2, 3])
        self.assertTrue(
            all(call[2]["max_bytes"] == MUSIC_SEARCH_RESPONSE_MAX_BYTES for call in queries.calls)
        )
        self.assertTrue(all(call[2]["timeout"] <= 5 for call in queries.calls))
        self.assertTrue(
            all(call[2]["total_timeout"] <= MUSIC_SEARCH_TOTAL_BUDGET_SECONDS for call in queries.calls)
        )
        self.assertEqual(songs[0]["cover_url"], "https://img.invalid/1.jpg")
        self.assertNotIn("raw", songs[0])

    def test_search_stops_starting_pages_after_total_budget(self):
        now = [0.0]
        queries = _ExternalQueries(
            [{"data": _songs(1 + page * 10, 10)} for page in range(5)],
            now=now,
            advance=11.0,
        )
        app = EntertainmentMusicApplication(queries, clock=lambda: now[0])  # type: ignore[arg-type]

        songs = app.search("track")

        self.assertEqual(len(songs), 20)
        self.assertEqual(len(queries.calls), 2)
        self.assertEqual(queries.calls[-1][2]["timeout"], 5.0)
        self.assertEqual(queries.calls[-1][2]["total_timeout"], 9.0)
        self.assertLessEqual(len(queries.calls) * 11, MUSIC_SEARCH_TOTAL_BUDGET_SECONDS + 2)

    def test_config_is_bounded_and_platform_aliases_are_preserved(self):
        app = EntertainmentMusicApplication(_ExternalQueries([]))  # type: ignore[arg-type]

        self.assertEqual(app.detect_platform("网易云点歌"), "netease")
        self.assertEqual(app.detect_platform("QQ点歌"), "qq")
        self.assertEqual(app.detect_platform("全民K歌"), "kg")
        self.assertEqual(app.detect_platform("点歌", fallback="qq"), "qq")
        self.assertEqual(app.set_config("song_limit", "51")[0], False)
        self.assertEqual(app.set_config("page_size", "0")[0], False)
        self.assertEqual(app.set_config("default_platform", "unknown")[0], False)
        self.assertEqual(app.set_config("api_base", "file:///tmp/music")[0], False)
        self.assertEqual(app.set_config("api_base", "https://example.invalid/api/")[0], True)
        self.assertEqual(app.load_config()["api_base"], "https://example.invalid/api/")

    def test_selection_cache_is_bounded_and_pages_are_clamped(self):
        now = [100.0]
        app = EntertainmentMusicApplication(
            _ExternalQueries([]),  # type: ignore[arg-type]
            clock=lambda: now[0],
            max_sessions=2,
        )
        app.set_config("page_size", "1")
        songs = _songs(1, 2)

        app.save_selection("u1", songs, "netease")
        app.save_selection("u2", songs, "netease")
        self.assertIsNotNone(app.get_selection("u1"))
        app.save_selection("u3", songs, "netease")

        self.assertIsNone(app.get_selection("u2"))
        page = app.set_selection_page("u1", 99)
        self.assertEqual(page["page"], 2)
        page["songs"].clear()
        self.assertEqual(len(app.get_selection("u1")["songs"]), 2)

    def test_selection_expires_lazily_and_selecting_refreshes_ttl(self):
        now = [100.0]
        app = EntertainmentMusicApplication(
            _ExternalQueries([]),  # type: ignore[arg-type]
            clock=lambda: now[0],
        )
        app.save_selection("u1", _songs(1, 2), "netease")
        now[0] = 200.0

        selected = app.select_song("u1", 1)
        self.assertEqual(selected["name"], "Song 1")
        now[0] = 300.0
        self.assertIsNotNone(app.get_selection("u1"))
        now[0] = 321.0
        self.assertIsNone(app.get_selection("u1"))


if __name__ == "__main__":
    unittest.main()
