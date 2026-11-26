"""common.http.session: proxy per session, and the response hook that ingest.batch.HttpStats uses."""
import http.server
import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    from common.http import session
except ImportError:  # requests is not installed in the bare CI image
    session = None

from ingest.batch import HttpStats  # noqa: E402


@unittest.skipIf(session is None, "requests not installed")
class SessionProxyTest(unittest.TestCase):
    def test_no_proxy_by_default(self):
        self.assertEqual(session("ua").proxies, {})

    def test_proxy_applies_to_that_session_only(self):
        p = "http://host.docker.internal:7890"
        s = session("ua", proxy=p)
        self.assertEqual(s.proxies, {"http": p, "https": p})
        self.assertEqual(session("ua").proxies, {})


class TooManyRequestsOnce(http.server.BaseHTTPRequestHandler):
    hits = 0

    def do_GET(self):
        type(self).hits += 1
        self.send_response(429 if self.hits == 1 else 200)
        self.send_header("Retry-After", "0")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):
        pass


@unittest.skipIf(session is None, "requests not installed")
class HttpStatsTest(unittest.TestCase):
    def test_counts_the_retries_urllib3_made(self):
        server = http.server.HTTPServer(("127.0.0.1", 0), TooManyRequestsOnce)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        stats = HttpStats()
        s = session("ua", on_response=stats.on_response)
        self.addCleanup(s.close)
        s.trust_env = False  # no proxy from the environment for 127.0.0.1
        s.mount("http://", s.adapters["https://"])  # the retry policy, on plain http for this local server
        self.assertEqual(s.get(f"http://127.0.0.1:{server.server_port}/", timeout=10).status_code, 200)
        self.assertEqual(stats.as_dict(), {"requests": 2, "retries": 1, "status": {"200": 1, "429": 1}})

    def test_response_without_retry_history(self):
        stats = HttpStats()
        stats.on_response(type("R", (), {"status_code": 404, "raw": None})())
        self.assertEqual(stats.as_dict(), {"requests": 1, "retries": 0, "status": {"404": 1}})


if __name__ == "__main__":
    unittest.main()
