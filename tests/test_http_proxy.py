import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

try:
    from common.http import session
except ImportError:  # requests is not installed in the bare CI image
    session = None


@unittest.skipIf(session is None, "requests not installed")
class SessionProxyTest(unittest.TestCase):
    def test_no_proxy_by_default(self):
        self.assertEqual(session("ua").proxies, {})

    def test_proxy_applies_to_that_session_only(self):
        p = "http://host.docker.internal:7890"
        s = session("ua", proxy=p)
        self.assertEqual(s.proxies, {"http": p, "https": p})
        self.assertEqual(session("ua").proxies, {})


if __name__ == "__main__":
    unittest.main()
