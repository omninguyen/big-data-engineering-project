import os
import pickle
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common.errors import MissingCredentials, PipelineError, SourceError, redact  # noqa: E402


class RedactTest(unittest.TestCase):
    def test_query_secrets(self):
        self.assertEqual(redact("/3/movie/popular?api_key=abc123&page=1"), "/3/movie/popular?api_key=***&page=1")
        self.assertEqual(redact("x?TOKEN=t0k&password=p'"), "x?TOKEN=***&password=***'")

    def test_bearer(self):
        self.assertEqual(redact("Authorization: Bearer eyJhbGciOi.J9.x-y_z"), "Authorization: Bearer ***")

    def test_leaves_normal_text(self):
        text = "wrote 10 records to bronze/tmdb/movies/dt=2026-11-16/movies.jsonl (monkey=1)"
        self.assertEqual(redact(text), text)


class ErrorsTest(unittest.TestCase):
    def test_hierarchy(self):
        self.assertTrue(issubclass(MissingCredentials, PipelineError))
        self.assertTrue(issubclass(SourceError, PipelineError))
        self.assertFalse(issubclass(MissingCredentials, SourceError))  # skip and fail must stay separate

    def test_message_is_redacted(self):
        self.assertNotIn("abc123", str(MissingCredentials("bad url ?api_key=abc123")))
        e = SourceError("tmdb", "GET /genre/movie/list?api_key=abc123 failed", status=401)
        self.assertEqual(str(e), "[tmdb] GET /genre/movie/list?api_key=*** failed")
        self.assertEqual((e.source, e.status), ("tmdb", 401))

    def test_pickle_roundtrip(self):
        e = pickle.loads(pickle.dumps(SourceError("tmdb", "HTTP 401", status=401)))
        self.assertEqual((str(e), e.source, e.status), ("[tmdb] HTTP 401", "tmdb", 401))
        self.assertEqual(str(pickle.loads(pickle.dumps(MissingCredentials("no key")))), "no key")


if __name__ == "__main__":
    unittest.main()
