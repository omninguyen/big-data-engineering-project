"""ingest/wikipedia.py with a fake HTTP session, fake ids and fake storage."""
import os
import sys
import unittest
from datetime import date
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    import requests
    from ingest import wikipedia
except ImportError:  # requests is not installed in a bare environment
    requests = None

from common import config  # noqa: E402
from common.errors import MissingCredentials, SourceError  # noqa: E402

ADLS = "abfss://bronze@acct.dfs.core.windows.net/landing"
MBID = "0000000a-0000-4000-8000-000000000000"


def response(status=200, payload=None, headers=None):
    return mock.Mock(status_code=status, headers=headers or {}, json=mock.Mock(return_value=payload or {}))


def bindings(*rows):
    """A SPARQL answer: rows of (id, Wikidata item, article URL path)."""
    return response(payload={"results": {"bindings": [
        {"id": {"value": i}, "item": {"value": f"http://www.wikidata.org/entity/{q}"},
         "article": {"value": f"https://en.wikipedia.org/wiki/{path}"}} for i, q, path in rows]}})


def views(url):
    title = url.split("/user/", 1)[1].split("/", 1)[0]
    return response(payload={"items": [{"article": title, "timestamp": "2026112600", "views": 5}]})


class FakeSession:
    """POST: the next SPARQL answer (a response or an exception; empty bindings when none is left).
    GET: `get(url)` gives the pageviews answer."""
    def __init__(self, sparql=(), get=views):
        self.sparql, self.answer_get, self.headers, self.queries, self.urls = list(sparql), get, {}, [], []

    def post(self, url, data=None, headers=None, timeout=None):
        self.queries.append(data["query"])
        answer = self.sparql.pop(0) if self.sparql else bindings()
        if isinstance(answer, Exception):
            raise answer
        return answer

    def get(self, url, timeout=None):
        self.urls.append(url)
        answer = self.answer_get(url)
        if isinstance(answer, Exception):
            raise answer
        return answer


@unittest.skipIf(requests is None, "requests not installed")
class WikipediaTest(unittest.TestCase):
    def run_with(self, ids, sess, batch=200, contact="team@example.org"):
        with mock.patch.object(wikipedia, "media_ids", return_value=ids), \
                mock.patch.object(wikipedia, "session", return_value=sess), \
                mock.patch.object(config, "MB_CONTACT", contact), \
                mock.patch.object(config, "WIKI_SPARQL_BATCH", batch), mock.patch.object(config, "WIKI_DAYS", 30), \
                mock.patch.object(config, "WIKI_PROJECT", "en.wikipedia"), \
                mock.patch.object(config, "BRONZE_ROOT", ADLS), \
                mock.patch.object(wikipedia.time, "sleep") as sleep, \
                mock.patch("common.storage.put_jsonl", side_effect=lambda k, recs: len(list(recs))) as put:
            n = wikipedia.run("2026-11-27", "B1")
        return n, put, sleep

    def test_maps_ids_in_batches_then_writes_articles_and_pageviews(self):
        sess = FakeSession([bindings(("603", "Q83495", "The_Matrix"), ("550", "Q190050", "Fight_Club")),
                            bindings(),  # 13 has no article
                            bindings((MBID, "Q213", "OK_Computer"))])
        n, put, _ = self.run_with({"movie": ["603", "550", "13"], "music": [MBID]}, sess, batch=2)
        self.assertEqual(n, 3)  # one day of views for each of the 3 articles
        self.assertEqual(len(sess.queries), 3)  # movies in 2 batches of 2, albums in 1
        self.assertIn('VALUES ?id { "603" "550" }', sess.queries[0])
        self.assertIn("wdt:P4947 ?id", sess.queries[0])  # TMDB movie id
        self.assertIn("wdt:P436 ?id", sess.queries[2])  # MusicBrainz release group id
        self.assertIn("schema:isPartOf <https://en.wikipedia.org/>", sess.queries[0])
        (articles_key, mapped), (views_key, daily) = [c.args for c in put.call_args_list]
        self.assertEqual(articles_key, "bronze/wikidata/media_articles/dt=2026-11-27/batch_id=B1/media_articles.jsonl")
        self.assertEqual(views_key, "bronze/wikipedia/pageviews/dt=2026-11-27/batch_id=B1/pageviews.jsonl")
        self.assertEqual({k: v for k, v in mapped[0].items() if k != "_ingested_at"}, {
            "media_type": "movie", "source_id": "603", "wikidata_id": "Q83495", "article": "The_Matrix",
            "url": "https://en.wikipedia.org/wiki/The_Matrix"})
        self.assertEqual(sorted(r["article"] for r in daily), ["Fight_Club", "OK_Computer", "The_Matrix"])

    def test_only_valid_ids_go_into_the_query(self):
        sess = FakeSession([bindings(("603", "Q1", "A"))])
        self.run_with({"movie": ["603", 'x" } ?item ?p ?o {', ""], "music": ["not-an-mbid"]}, sess)
        self.assertEqual(len(sess.queries), 1)
        self.assertIn('VALUES ?id { "603" }', sess.queries[0])

    def test_window_and_article_urls(self):
        with mock.patch.object(config, "WIKI_DAYS", 30), mock.patch.object(config, "WIKI_PROJECT", "en.wikipedia"):
            first, last = wikipedia.window("2026-11-27")
            self.assertEqual((first, last), (date(2026, 10, 28), date(2026, 11, 26)))  # 30 days up to the day before
            self.assertTrue(wikipedia.pageviews_url("AC/DC (album)", first, last).endswith(
                "/en.wikipedia/all-access/user/AC%2FDC_%28album%29/daily/20261028/20261126"))  # people only
        sess = FakeSession([bindings(("1", "Q1", "Am%C3%A9lie"))])
        _, put, _ = self.run_with({"movie": ["1"], "music": []}, sess)
        self.assertEqual(put.call_args_list[0].args[1][0]["article"], "Amélie")  # decoded from the URL
        self.assertIn("/user/Am%C3%A9lie/", sess.urls[0])  # encoded again for the pageviews API

    def test_articles_without_views_or_failing_are_skipped(self):
        answers = {"A": response(status=404), "B": response(status=500), "C": requests.ConnectionError("down")}
        sess = FakeSession([bindings(("1", "Q1", "A"), ("2", "Q2", "B"), ("3", "Q3", "C"), ("4", "Q4", "D"))],
                           get=lambda url: answers.get(url.split("/user/")[1].split("/")[0]) or views(url))
        with self.assertLogs("ingest.wikipedia", "WARNING"):
            n, _, sleep = self.run_with({"movie": ["1", "2", "3", "4"], "music": []}, sess)
        self.assertEqual(n, 1)  # only D had views
        self.assertEqual(sum(1 for c in sleep.call_args_list if c.args == (wikipedia.PAUSE_S,)), 4)  # after each
        self.assertLessEqual(1 / wikipedia.PAUSE_S, 10)

    def test_refused_user_agent_fails(self):
        sess = FakeSession([bindings(("1", "Q1", "A"))], get=lambda url: response(status=403))
        with self.assertRaises(SourceError) as ctx:
            self.run_with({"movie": ["1"], "music": []}, sess)
        self.assertEqual(ctx.exception.status, 403)

    def test_each_media_type_has_its_wikidata_property(self):
        self.assertEqual(wikipedia.PROPERTY, {"movie": "P4947", "tv": "P4983", "music": "P436"})
        self.assertIn("?item wdt:P4983 ?id", wikipedia.sparql_query("tv", ["1399"]))  # TMDB TV series id
        self.assertTrue(wikipedia.VALID_ID["tv"].fullmatch("1399"))

    def test_sparql_is_retried_after_a_429_or_5xx(self):
        sess = FakeSession([response(status=429, headers={"Retry-After": "7"}), bindings(("1", "Q1", "A"))])
        with self.assertLogs("ingest.wikipedia", "WARNING"):
            n, _, sleep = self.run_with({"movie": ["1"], "music": []}, sess)
        self.assertEqual(n, 1)
        self.assertIn(mock.call(7.0), sleep.call_args_list)  # the Retry-After of the 429
        sess = FakeSession([response(status=503)] * wikipedia.MAX_ATTEMPTS)
        with self.assertLogs("ingest.wikipedia", "WARNING"), self.assertRaisesRegex(SourceError, "failed 4 times"):
            self.run_with({"movie": ["1"], "music": []}, sess)

    def test_rejected_sparql_query_fails_at_once(self):
        sess = FakeSession([response(status=400)])
        with self.assertRaises(SourceError) as ctx:
            self.run_with({"movie": ["1"], "music": []}, sess)
        self.assertEqual((ctx.exception.status, len(sess.queries)), (400, 1))

    def test_nothing_mapped_or_no_views_at_all_fails(self):
        with self.assertRaisesRegex(SourceError, "no movie or album"):
            self.run_with({"movie": ["1"], "music": [MBID]}, FakeSession())
        sess = FakeSession([bindings(("1", "Q1", "A"))], get=lambda url: response(status=404))
        with self.assertRaisesRegex(SourceError, "no pageviews"):
            self.run_with({"movie": ["1"], "music": []}, sess)

    def test_no_contact_fails_before_any_request(self):
        sess = FakeSession()
        with self.assertRaises(MissingCredentials):
            self.run_with({"movie": ["1"], "music": []}, sess, contact="")
        self.assertEqual((sess.queries, sess.urls), ([], []))


if __name__ == "__main__":
    unittest.main()
