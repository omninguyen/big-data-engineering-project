"""common.config read in a fresh interpreter per case, so the module-level settings of other tests are untouched."""
import os
import subprocess
import sys
import unittest

SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
CLEAN = {k: v for k, v in os.environ.items()
         if k not in ("DATABRICKS_RUNTIME_VERSION", "UC_CATALOG", "BRONZE_ROOT", "BRONZE_TABLES_ROOT", "SILVER_ROOT",
                      "GOLD_ROOT", "OPS_ROOT",
                      "MOVIE_SOURCE", "MOVIE_FILE", "TMDB_API_KEY")}


def run(code: str, **env) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, "-c", f"from common import config; {code}"], cwd=SRC,
                          env={**CLEAN, "PYTHONPATH": SRC, **env}, capture_output=True, text=True, timeout=60)


class ConfigTest(unittest.TestCase):
    def out(self, code, **env):
        r = run(code, **env)
        self.assertEqual(r.returncode, 0, r.stderr)
        return r.stdout.strip()

    def test_defaults(self):
        self.assertEqual(self.out("print(config.ON_DATABRICKS, repr(config.BRONZE_ROOT), repr(config.MOVIE_FILE))"),
                         "False '' ''")  # BRONZE_ROOT comes from the job (--set BRONZE_ROOT=abfss://...)

    def test_databricks_runtime_is_detected(self):
        self.assertEqual(self.out("print(config.ON_DATABRICKS)", DATABRICKS_RUNTIME_VERSION="16.4"), "True")

    def test_bronze_root_and_the_movie_file_under_it(self):
        self.assertEqual(self.out("print(config.BRONZE_ROOT, config.MOVIE_FILE)",
                                  BRONZE_ROOT="abfss://bronze@acct.dfs.core.windows.net/"),
                         "abfss://bronze@acct.dfs.core.windows.net "
                         "abfss://bronze@acct.dfs.core.windows.net/manual/tmdb_movies.json")

    def test_tables_next_to_landing(self):
        container = "abfss://bronze@acct.dfs.core.windows.net"
        self.assertEqual(self.out("print(config.BRONZE_TABLES_ROOT)", BRONZE_ROOT=f"{container}/landing/"),
                         f"{container}/tables")
        self.assertEqual(self.out("print(repr(config.BRONZE_TABLES_ROOT))", BRONZE_ROOT=container), "''")
        self.assertEqual(self.out("print(config.BRONZE_TABLES_ROOT)", BRONZE_ROOT=f"{container}/landing",
                                  BRONZE_TABLES_ROOT="abfss://silver@acct.dfs.core.windows.net/t/"),
                         "abfss://silver@acct.dfs.core.windows.net/t")

    def test_silver_root(self):
        self.assertEqual(self.out("print(repr(config.SILVER_ROOT))"), "''")  # from the job (--set SILVER_ROOT=...)
        self.assertEqual(self.out("print(config.SILVER_ROOT)", SILVER_ROOT="abfss://silver@acct.dfs.core.windows.net/"),
                         "abfss://silver@acct.dfs.core.windows.net")

    def test_gold_root(self):
        self.assertEqual(self.out("print(repr(config.GOLD_ROOT))"), "''")  # from the job (--set GOLD_ROOT=...)
        self.assertEqual(self.out("print(config.GOLD_ROOT)", GOLD_ROOT="abfss://gold@acct.dfs.core.windows.net/"),
                         "abfss://gold@acct.dfs.core.windows.net")

    def test_ops_root_is_a_folder_of_the_gold_container(self):
        self.assertEqual(self.out("print(repr(config.OPS_ROOT))"), "''")
        self.assertEqual(self.out("print(config.OPS_ROOT)", GOLD_ROOT="abfss://gold@acct.dfs.core.windows.net/"),
                         "abfss://gold@acct.dfs.core.windows.net/ops")
        ops = "abfss://ops@acct.dfs.core.windows.net"  # set explicitly
        self.assertEqual(self.out("print(config.OPS_ROOT)", GOLD_ROOT="abfss://gold@acct.dfs.core.windows.net",
                                  OPS_ROOT=f"{ops}/"), ops)

    def test_secret_from_environment_without_databricks(self):
        self.assertEqual(self.out("print(config.TMDB_API_KEY)", TMDB_API_KEY=" k123 "), "k123")
        self.assertEqual(self.out("print(repr(config.TMDB_API_KEY))"), "''")

    def test_invalid_values_fail_fast(self):
        self.assertNotEqual(run("pass", UC_CATALOG="media; DROP").returncode, 0)
        self.assertNotEqual(run("pass", MOVIE_SOURCE="kafka").returncode, 0)


if __name__ == "__main__":
    unittest.main()
