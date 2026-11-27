"""ingest/listenbrainz.py with a fake HTTP session, fake album ids and fake storage."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    import requests
    from ingest import listenbrainz
except ImportError:  # requests is not installed in a bare environment
    requests = None

from common import config  # noqa: E402
from common.errors import SourceError  # noqa: E402

ADLS = "abfss://bronze@acct.dfs.core.windows.net/landing"


def mbid(n: int) -> str:
    return f"{n:08x}-0000-4000-8000-000000000000"


def response(status=200, payload=None, remaining="50", reset_in="10"):
    headers = {"X-RateLimit-Remaining": remaining, "X-RateLimit-Reset-In": reset_in}
    return mock.Mock(status_code=status, headers=headers, json=mock.Mock(return_value=payload))


def popularity(mbids, listens=7):
    return [{"release_group_mbid": m, "total_listen_count": listens, "total_user_count": 3} for m in mbids]


class FakeSession:
    """Answers each POST with the next item of `answers`: a response, an exception, or None for a 200 with counts."""
    def __init__(self, answers=()):
        self.answers, self.headers, self.bodies = list(answers), {}, []

    def post(self, url, json=None, timeout=None):
        self.url = url
        self.bodies.append(json)
        answer = self.answers.pop(0) if self.answers else None
        if isinstance(answer, Exception):
            raise answer
        return answer or response(payload=popularity(json["release_group_mbids"]))


@unittest.skipIf(requests is None, "requests not installed")
class ListenBrainzTest(unittest.TestCase):
    def run_with(self, ids, sess, batch_size=100):
        with mock.patch.object(listenbrainz, "album_ids", return_value=ids), \
                mock.patch.object(listenbrainz, "session", return_value=sess), \
                mock.patch.object(config, "LB_BATCH_SIZE", batch_size), \
                mock.patch.object(config, "MB_CONTACT", "team@example.org"), \
                mock.patch.object(config, "BRONZE_ROOT", ADLS), \
                mock.patch.object(listenbrainz.time, "sleep") as sleep, \
                mock.patch("common.storage.put_jsonl", side_effect=lambda k, recs: len(list(recs))) as put:
            n = listenbrainz.run("2026-11-27", "B1")
        return n, put, sleep

    def test_every_album_in_batches_of_lb_batch_size(self):
        sess = FakeSession()
        n, put, _ = self.run_with([mbid(i) for i in range(250)], sess)
        self.assertEqual(n, 250)
        self.assertEqual([len(b["release_group_mbids"]) for b in sess.bodies], [100, 100, 50])
        self.assertEqual(sess.url, "https://api.listenbrainz.org/1/popularity/release-group")
        key, records = put.call_args.args
        self.assertEqual(key, "bronze/listenbrainz/release_group_popularity/dt=2026-11-27/batch_id=B1/"
                              "release_group_popularity.jsonl")
        self.assertEqual(set(records[0]), {"release_group_mbid", "total_listen_count", "total_user_count",
                                           "_ingested_at"})

    def test_albums_without_data_are_kept_as_null(self):
        sess = FakeSession([response(payload=popularity([mbid(1)], listens=None))])
        _, put, _ = self.run_with([mbid(1)], sess)
        self.assertIsNone(put.call_args.args[1][0]["total_listen_count"])  # silver leaves them out

    def test_only_mbids_are_sent(self):
        sess = FakeSession()
        self.run_with([mbid(1), "not-an-id", None, mbid(2)], sess)
        self.assertEqual(sess.bodies, [{"release_group_mbids": [mbid(1), mbid(2)]}])

    def test_pauses_and_waits_for_the_rate_limit_reset(self):
        used_up = response(payload=popularity([mbid(1)]), remaining="0", reset_in="7")
        _, _, sleep = self.run_with([mbid(1), mbid(2)], FakeSession([used_up]), batch_size=1)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [8, listenbrainz.PAUSE_S])  # reset + 1 s

    def test_429_and_5xx_are_retried_after_the_wait(self):
        sess = FakeSession([response(429, reset_in="5"), response(503), requests.ConnectionError("reset")])
        with self.assertLogs("ingest.listenbrainz", "WARNING"):
            n, _, sleep = self.run_with([mbid(1)], sess)
        self.assertEqual(n, 1)
        self.assertEqual(len(sess.bodies), 4)
        waits = [c.args[0] for c in sleep.call_args_list]
        self.assertEqual(waits[:3], [6, 2, 4])  # 429: Reset-In + 1 s; then a growing back-off (0.5 s x 2^attempt)

    def test_gives_up_after_max_attempts(self):
        sess = FakeSession([response(503)] * listenbrainz.MAX_ATTEMPTS)
        with self.assertRaisesRegex(SourceError, "failed 4 times: HTTP 503"), \
                self.assertLogs("ingest.listenbrainz", "WARNING"):
            self.run_with([mbid(1)], sess)
        self.assertEqual(len(sess.bodies), listenbrainz.MAX_ATTEMPTS)

    def test_other_http_errors_fail_at_once(self):
        sess = FakeSession([response(400)])
        with self.assertRaises(SourceError) as ctx:
            self.run_with([mbid(1)], sess)
        self.assertEqual(ctx.exception.status, 400)
        self.assertEqual(len(sess.bodies), 1)

    def test_no_album_fails(self):
        with self.assertRaisesRegex(SourceError, "no album"):
            self.run_with([], FakeSession())

    def test_unexpected_answer_fails(self):
        with self.assertRaisesRegex(SourceError, "not a list"):
            self.run_with([mbid(1)], FakeSession([response(payload={"error": "x"})]))


if __name__ == "__main__":
    unittest.main()
