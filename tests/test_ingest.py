"""Ingest error handling (ingest/tmdb.py, ingest/musicbrainz.py) with fake HTTP sessions and storage."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    import requests
    from ingest import musicbrainz, tmdb
except ImportError:  # requests is not installed in a bare environment
    requests = None

from common import config  # noqa: E402
from common.errors import MissingCredentials, SourceError  # noqa: E402

KEY = "abcdef0123456789abcdef0123456789"  # 32 chars: a v3 API key, sent as ?api_key=
ADLS = "abfss://bronze@acct.dfs.core.windows.net"


def response(status=200, payload=None):
    return mock.Mock(status_code=status, json=mock.Mock(return_value=payload or {}))


class FakeSession:
    def __init__(self, handler):
        self.handler, self.headers, self.calls = handler, {}, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return self.handler(url, params or {})


@unittest.skipIf(requests is None, "requests not installed")
class TmdbTest(unittest.TestCase):
    def run_with(self, handler, key=KEY):
        sess = FakeSession(handler)
        with mock.patch.object(config, "TMDB_API_KEY", key), mock.patch.object(config, "TMDB_PAGES", 1), \
                mock.patch.object(config, "TMDB_NEW_PAGES", 1), mock.patch.object(config, "BRONZE_ROOT", ADLS), \
                mock.patch.object(tmdb, "session", return_value=sess), mock.patch.object(tmdb.time, "sleep"), \
                mock.patch("common.storage.put_jsonl", side_effect=lambda k, recs: len(list(recs))) as put:
            return tmdb.run("2026-11-24", "B1"), put, sess

    def test_no_key_is_missing_credentials(self):
        with self.assertRaises(MissingCredentials):
            self.run_with(lambda url, p: response(), key="")

    def test_writes_movies_with_genre_names(self):
        def handler(url, params):
            if url.endswith("genre/movie/list"):
                return response(payload={"genres": [{"id": 28, "name": "Action"}]})
            return response(payload={"results": [{"id": 1, "title": "A", "genre_ids": [28]}]})
        n, put, _ = self.run_with(handler)
        self.assertEqual(n, 4)  # popular, top_rated, now_playing and upcoming, 1 page each
        key, recs = put.call_args.args
        self.assertEqual(key, "bronze/tmdb/movies/dt=2026-11-24/batch_id=B1/movies.jsonl")

    def test_new_movies_from_now_playing_and_upcoming(self):
        def handler(url, params):
            if url.endswith("genre/movie/list"):
                return response(payload={"genres": []})
            return response(payload={"results": [{"id": 1}], "total_pages": 9})
        _, put, sess = self.run_with(handler)
        self.assertEqual([r["_endpoint"] for r in put.call_args.args[1]],
                         ["movie/popular", "movie/top_rated", "movie/now_playing", "movie/upcoming"])
        self.assertEqual(tmdb.NEW_MOVIES, {"movie/now_playing", "movie/upcoming"})

    def test_paging_stops_at_the_last_page(self):
        def handler(url, params):
            if url.endswith("genre/movie/list"):
                return response(payload={"genres": []})
            return response(payload={"results": [{"id": params["page"]}], "total_pages": 2})
        sess = FakeSession(handler)
        with mock.patch.object(config, "TMDB_API_KEY", KEY), mock.patch.object(config, "TMDB_PAGES", 5), \
                mock.patch.object(config, "TMDB_NEW_PAGES", 5), mock.patch.object(config, "BRONZE_ROOT", ADLS), \
                mock.patch.object(tmdb, "session", return_value=sess), mock.patch.object(tmdb.time, "sleep"), \
                mock.patch("common.storage.put_jsonl", side_effect=lambda k, recs: len(list(recs))):
            self.assertEqual(tmdb.run("2026-11-24", "B1"), 4 * 2)
        self.assertEqual(len(sess.calls), 1 + 4 * 2)  # genre list + 2 pages of each endpoint, not 5

    def test_pauses_after_every_request(self):
        """At most 4 requests/s: under TMDB's ceiling (around 40/s) and within its old limit of 40 per 10 s."""
        def handler(url, params):
            return response(payload={"genres": []} if url.endswith("list") else {"results": [{"id": 1}]})
        sess = FakeSession(handler)
        with mock.patch.object(config, "TMDB_API_KEY", KEY), mock.patch.object(config, "TMDB_PAGES", 3), \
                mock.patch.object(config, "TMDB_NEW_PAGES", 2), mock.patch.object(config, "BRONZE_ROOT", ADLS), \
                mock.patch.object(tmdb, "session", return_value=sess), \
                mock.patch.object(tmdb.time, "sleep") as sleep, mock.patch("common.storage.put_jsonl", return_value=6):
            tmdb.run("2026-11-24", "B1")
        self.assertEqual(len(sess.calls), 1 + 2 * 3 + 2 * 2)  # genre list, 3 pages of the lists, 2 of the new movies
        self.assertEqual([c.args for c in sleep.call_args_list], [(0.25,)] * len(sess.calls))
        self.assertLessEqual(1 / tmdb.PAUSE_S, 4)

    def test_rejected_key_fails_without_leaking_it(self):
        with self.assertRaises(SourceError) as ctx:
            self.run_with(lambda url, p: response(status=401))
        self.assertEqual(ctx.exception.status, 401)
        self.assertNotIn(KEY, str(ctx.exception))

    def test_network_error_message_has_no_url(self):
        def handler(url, params):
            raise requests.ConnectionError(f"Max retries exceeded with url: /3/genre/movie/list?api_key={KEY}")
        with self.assertRaises(SourceError) as ctx:
            self.run_with(handler)
        self.assertNotIn(KEY, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertTrue(ctx.exception.__suppress_context__)

    def test_empty_result_fails(self):
        def handler(url, params):
            return response(payload={"genres": []} if url.endswith("list") else {"results": []})
        with self.assertRaisesRegex(SourceError, "no movies"):
            self.run_with(handler)


@unittest.skipIf(requests is None, "requests not installed")
class MusicBrainzTest(unittest.TestCase):
    def run_with(self, handler, sess=None):
        sess = sess or FakeSession(handler)
        with mock.patch.object(config, "MB_GENRES", ["jazz", "rock"]), mock.patch.object(config, "MB_PAGES_PER_GENRE", 1), \
                mock.patch.object(config, "MB_NEW_DAYS", 30), mock.patch.object(config, "BRONZE_ROOT", ADLS), \
                mock.patch.object(config, "MB_CONTACT", "team@example.org"), \
                mock.patch.object(musicbrainz, "session", return_value=sess), \
                mock.patch.object(musicbrainz.time, "sleep"), \
                mock.patch("common.storage.put_jsonl", side_effect=lambda k, recs: len(list(recs))) as put:
            n = musicbrainz.run("2026-11-24", "B1")
        self.assertEqual(put.call_args.args[0], "bronze/musicbrainz/release_groups/dt=2026-11-24/batch_id=B1/"
                                                "release_groups.jsonl")
        return n

    def test_one_failing_genre_does_not_stop_the_others(self):
        def handler(url, params):
            if "jazz" in params["query"]:
                raise requests.Timeout("timeout")
            return response(payload={"release-groups": [{"id": "x", "title": "Rock album"}]})
        self.assertEqual(self.run_with(handler), 2)  # rock: best matches + new albums

    def test_new_albums_of_each_genre(self):
        sess = FakeSession(lambda url, params: response(payload={"release-groups": [{"id": "x"}]}))
        self.run_with(None, sess)
        queries = [params["query"] for _, params in sess.calls]
        self.assertEqual(queries, [
            'tag:"jazz" AND primarytype:album',
            'tag:"jazz" AND primarytype:album AND firstreleasedate:[2026-10-25 TO 2026-11-24]',  # last 30 days
            'tag:"rock" AND primarytype:album',
            'tag:"rock" AND primarytype:album AND firstreleasedate:[2026-10-25 TO 2026-11-24]'])

    def test_new_albums_are_searched_even_when_the_best_matches_fail(self):
        def handler(url, params):
            if "firstreleasedate" not in params["query"]:
                return response(status=503)
            return response(payload={"release-groups": [{"id": "new"}]})
        self.assertEqual(self.run_with(handler), 2)  # one new album per genre

    def test_nothing_at_all_fails(self):
        with self.assertRaises(SourceError):
            self.run_with(lambda url, p: response(status=503))

    def test_no_contact_fails_instead_of_sending_a_placeholder(self):
        with mock.patch.object(config, "MB_CONTACT", ""), self.assertRaises(MissingCredentials):
            musicbrainz.run("2026-11-24", "B1")


if __name__ == "__main__":
    unittest.main()
