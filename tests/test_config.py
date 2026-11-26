"""common.config read in a fresh interpreter per case, so the module-level settings of other tests are untouched."""
import os
import subprocess
import sys
import unittest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
CLEAN = {k: v for k, v in os.environ.items()
         if k not in ("DATABRICKS_RUNTIME_VERSION", "UC_CATALOG", "UC_LANDING_ROOT", "MOVIE_SOURCE", "MOVIE_FILE",
                      "TMDB_API_KEY")}


def run(code: str, **env) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-c", f"from common import config; {code}"], cwd=SRC,
                          env={**CLEAN, "PYTHONPATH": SRC, **env}, capture_output=True, text=True, timeout=60)


class ConfigTest(unittest.TestCase):
    def out(self, code, **env):
        r = run(code, **env)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.strip()

    def test_defaults(self):
        self.assertEqual(self.out("print(config.ON_DATABRICKS, config.UC_LANDING_ROOT, config.MOVIE_FILE)"),
                         "False /Volumes/big_data_engineering/bronze/landing "
                         "/Volumes/big_data_engineering/bronze/landing/manual/tmdb_movies.json")

    def test_databricks_runtime_is_detected(self):
        self.assertEqual(self.out("print(config.ON_DATABRICKS)", DATABRICKS_RUNTIME_VERSION="16.4"), "True")

    def test_catalog_changes_the_volume(self):
        self.assertEqual(self.out("print(config.UC_LANDING_ROOT)", UC_CATALOG="thesis"), "/Volumes/thesis/bronze/landing")

    def test_secret_from_environment_without_databricks(self):
        self.assertEqual(self.out("print(config.TMDB_API_KEY)", TMDB_API_KEY=" k123 "), "k123")
        self.assertEqual(self.out("print(repr(config.TMDB_API_KEY))"), "''")

    def test_invalid_values_fail_fast(self):
        self.assertNotEqual(run("pass", UC_CATALOG="media; DROP").returncode, 0)
        self.assertNotEqual(run("pass", MOVIE_SOURCE="kafka").returncode, 0)


if __name__ == "__main__":
    unittest.main()
