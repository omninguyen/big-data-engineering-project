"""ingest.sources: TMDB and MusicBrainz with fake HTTP sessions, and the choice of source module."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    import requests
    from ingest.sources import musicbrainz, tmdb
except ImportError:  # requests is not installed in a bare environment
    requests = None

from common import config  # noqa: E402
from common.errors import MissingCredentials, SourceError  # noqa: E402
from ingest import sources  # noqa: E402
from ingest.batch import Batch  # noqa: E402

KEY = "abcdef0123456789abcdef0123456789"  # 32 chars: a v3 API key, sent as ?api_key=


def response(status=200, payload=None):
    return mock.Mock(status_code=status, json=mock.Mock(return_value=payload or {}))


class FakeSession:
    def __init__(self, handler):
        self.handler, self.headers, self.calls = handler, {}, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return self.handler(url, params or {})


def tmdb_handler(url, params):
    if url.endswith("genre/movie/list"):
        return response(payload={"genres": [{"id": 28, "name": "Action"}]})
    return response(payload={"results": [{"id": 1, "title": "A", "genre_ids": [28]}]})


@unittest.skipIf(requests is None, "requests not installed")
class TmdbTest(unittest.TestCase):
    def extract(self, handler, key=KEY, pages=1):
        batch = Batch("tmdb", "2026-11-26")
        sess = FakeSession(handler)
        with mock.patch.object(config, "TMDB_API_KEY", key), mock.patch.object(config, "TMDB_PAGES", pages), \
                mock.patch.object(tmdb, "session", return_value=sess) as make, mock.patch.object(tmdb.time, "sleep"):
            pages_ = list(tmdb.extract(batch))
        make.assert_called_once_with(tmdb.USER_AGENT, proxy=config.TMDB_PROXY, on_response=batch.http.on_response)
        return pages_, batch, sess

    def test_no_key_is_missing_credentials(self):
        with self.assertRaises(MissingCredentials):
            self.extract(tmdb_handler, key="")

    def test_genres_then_movies_unchanged(self):
        pages, batch, _ = self.extract(tmdb_handler)
        self.assertEqual([(p.entity, p.request) for p in pages], [
            ("genres", {"endpoint": "genre/movie/list"}),
            ("movies", {"endpoint": "movie/popular", "page": 1}),
            ("movies", {"endpoint": "movie/top_rated", "page": 1})])
        self.assertEqual(pages[1].records, [{"id": 1, "title": "A", "genre_ids": [28]}])  # no field added
        self.assertEqual(batch.pages_failed, 0)

    def test_stops_an_endpoint_at_its_last_page(self):
        def handler(url, params):
            if url.endswith("genre/movie/list"):
                return response(payload={"genres": []})
            return response(payload={"results": [{"id": params["page"]}] if params["page"] < 3 else []})
        pages, batch, sess = self.extract(handler, pages=5)
        self.assertEqual(len([p for p in pages if p.entity == "movies"]), 4)  # 2 pages x 2 endpoints
        self.assertEqual(len(sess.calls), 1 + 2 * 3)
        self.assertEqual(batch.pages_failed, 0)

    def test_failing_page_is_counted_and_the_rest_goes_on(self):
        def handler(url, params):
            if url.endswith("movie/popular"):
                return response(status=500)
            return tmdb_handler(url, params)
        with self.assertLogs("ingest.batch", "WARNING"):
            pages, batch, _ = self.extract(handler)
        self.assertEqual([p.request for p in pages if p.entity == "movies"], [{"endpoint": "movie/top_rated", "page": 1}])
        self.assertEqual(batch.failures, [{"request": {"endpoint": "movie/popular", "page": 1}, "reason": "HTTP 500"}])

    def test_network_error_on_a_page_has_no_url(self):
        def handler(url, params):
            if url.endswith("movie/popular"):
                raise requests.ConnectionError(f"Max retries exceeded with url: /3/movie/popular?api_key={KEY}")
            return tmdb_handler(url, params)
        with self.assertLogs("ingest.batch", "WARNING"):
            _, batch, _ = self.extract(handler)
        self.assertEqual(batch.pages_failed, 1)
        self.assertNotIn(KEY, str(batch.failures))

    def test_rejected_key_fails_without_leaking_it(self):
        with self.assertRaises(SourceError) as ctx:
            self.extract(lambda url, p: response(status=401))
        self.assertEqual(ctx.exception.status, 401)
        self.assertNotIn(KEY, str(ctx.exception))

    def test_network_error_message_has_no_url(self):
        def handler(url, params):
            raise requests.ConnectionError(f"Max retries exceeded with url: /3/genre/movie/list?api_key={KEY}")
        with self.assertRaises(SourceError) as ctx:
            self.extract(handler)
        self.assertNotIn(KEY, str(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)
        self.assertTrue(ctx.exception.__suppress_context__)

    def test_no_movie_at_all_fails(self):
        def handler(url, params):
            return response(payload={"genres": []} if url.endswith("list") else {"results": []})
        with self.assertRaisesRegex(SourceError, "no movies"):
            self.extract(handler)


@unittest.skipIf(requests is None, "requests not installed")
class MusicBrainzTest(unittest.TestCase):
    def extract(self, handler):
        batch = Batch("musicbrainz", "2026-11-26")
        with mock.patch.object(config, "MB_GENRES", ["jazz", "rock"]), mock.patch.object(config, "MB_PAGES_PER_GENRE", 1), \
                mock.patch.object(musicbrainz, "session", return_value=FakeSession(handler)), \
                mock.patch.object(musicbrainz.time, "sleep"):
            return list(musicbrainz.extract(batch)), batch

    def test_one_failing_genre_does_not_stop_the_others(self):
        def handler(url, params):
            if "jazz" in params["query"]:
                raise requests.Timeout("timeout")
            return response(payload={"release-groups": [{"id": "x", "title": "Rock album"}]})
        with self.assertLogs("ingest.batch", "WARNING"):
            pages, batch = self.extract(handler)
        self.assertEqual([(p.entity, p.request, p.records) for p in pages],
                         [("release_groups", {"query_genre": "rock", "offset": 0}, [{"id": "x", "title": "Rock album"}])])
        self.assertEqual(batch.failures, [{"request": {"query_genre": "jazz", "offset": 0}, "reason": "Timeout"}])

    def test_nothing_at_all_fails(self):
        with self.assertRaises(SourceError), self.assertLogs("ingest.batch", "WARNING"):
            self.extract(lambda url, p: response(status=503))

    def test_contact_stays_out_of_the_manifest(self):
        with mock.patch.object(config, "MB_CONTACT", "someone@example.org"):
            self.assertNotIn("someone@example.org", str(musicbrainz.params()))


class ChoiceTest(unittest.TestCase):
    def test_modules(self):
        cases = {("musicbrainz", "tmdb"): "musicbrainz", ("tmdb", "tmdb"): "tmdb",
                 ("tmdb", "file"): "tmdb_file", ("tmdb", "sample"): "sample_movies"}
        for (name, movie_source), module in cases.items():
            if requests is None and module in ("tmdb", "musicbrainz"):
                continue
            with self.subTest(name=name, movie_source=movie_source), \
                    mock.patch.object(config, "MOVIE_SOURCE", movie_source):
                self.assertEqual(sources.get(name).__name__, f"ingest.sources.{module}")

    def test_unknown(self):
        with self.assertRaises(ValueError):
            sources.get("kafka")


if __name__ == "__main__":
    unittest.main()
