"""src/sql: the bronze CREATE TABLE and COPY INTO statements, and the runner with a fake Spark session."""
import os
import re
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import sql  # noqa: E402
from common import config  # noqa: E402

ROOT = "abfss://bronze@acct.dfs.core.windows.net/tables"
LANDING = "abfss://bronze@acct.dfs.core.windows.net/landing"
LINEAGE = {"dt", "batch_id", "_source_file", "_ingested_at", "_file_size", "_file_modified_at"}
SOURCES = {"tmdb_movies": "tmdb/movies", "musicbrainz_release_groups": "musicbrainz/release_groups"}


def columns(name: str) -> list:
    """Columns of the CREATE TABLE, in order."""
    text = sql.render("bronze", name, catalog="cat", tables_root=ROOT)
    return re.findall(r"^\s+(\w+)\s+[A-Z]", text, re.M)


def load(name: str) -> str:
    return sql.render("bronze/load", name, catalog="cat", landing_root=LANDING)


class FakeSpark:
    def __init__(self, locations=None):
        self.statements, self.locations = [], locations or {}

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("DESCRIBE DETAIL "):
            table = statement.split()[-1]
            row = {"location": self.locations.get(table, f"{ROOT}/{table.split('.')[-1]}/")}
        elif "COPY INTO" in statement:
            row = mock.Mock(asDict=mock.Mock(return_value={"num_inserted_rows": 3}))
        else:
            row = None
        return mock.Mock(first=mock.Mock(return_value=row))


class StatementTest(unittest.TestCase):
    def test_external_delta_tables_in_tables_root(self):
        for name in sql.BRONZE_TABLES:
            with self.subTest(table=name):
                text = sql.render("bronze", name, catalog="cat", tables_root=ROOT)
                self.assertNotIn("$", text)
                self.assertIn(f"CREATE TABLE IF NOT EXISTS cat.bronze.{name} (", text)
                self.assertIn("USING DELTA", text)
                self.assertIn(f"LOCATION '{ROOT}/{name}'", text)
        for folder in ("bronze", "bronze/load"):
            files = [f for f in os.listdir(os.path.join(sql.HERE, folder)) if f.endswith(".sql")]
            self.assertEqual(sorted(f[:-len(".sql")] for f in files), sorted(sql.BRONZE_TABLES), folder)

    def test_columns(self):
        for name in sql.BRONZE_TABLES:
            with self.subTest(table=name):
                self.assertLessEqual({"id", "title"} | LINEAGE, set(columns(name)))
        # fields written by the ingest that silver may need
        self.assertLessEqual({"endpoint", "genres", "genre_ids", "overview", "original_title", "poster_path"},
                             set(columns("tmdb_movies")))
        self.assertLessEqual({"query_genre", "artist_credit", "tags", "primary_type", "secondary_types"},
                             set(columns("musicbrainz_release_groups")))

    def test_missing_placeholder_fails(self):
        with self.assertRaises(KeyError):
            sql.render("bronze", "tmdb_movies", catalog="cat")


class LoadStatementTest(unittest.TestCase):
    def test_copy_into_from_the_landing_folder_of_the_ingest(self):
        from ingest.batch import bronze_key
        for name, source in SOURCES.items():
            with self.subTest(table=name):
                text = load(name)
                self.assertNotIn("$", text)
                self.assertIn(f"COPY INTO cat.bronze.{name}\n", text)
                self.assertIn(f"FROM '{LANDING}/{source}/'", text)
                self.assertIn("FILEFORMAT = TEXT", text)
                # the folder the ingest writes to (bronze/<source>/<entity>/dt=...)
                self.assertTrue(bronze_key(*source.split("/"), "d", "b", "f").startswith(f"bronze/{source}/dt="))

    def test_selects_every_column_of_the_table_in_order(self):
        for name in sql.BRONZE_TABLES:
            with self.subTest(table=name):
                aliases = re.findall(r"\bAS\s+(\w+),?\s*$", load(name), re.M)
                self.assertEqual(aliases, columns(name))

    def test_dt_and_batch_id_come_from_the_file_path(self):
        for name in sql.BRONZE_TABLES:
            text = load(name)
            self.assertIn("regexp_extract(_metadata.file_path, '/dt=([^/]+)/', 1)", text)
            self.assertIn("regexp_extract(_metadata.file_path, '/batch_id=([^/]+)/', 1)", text)


class CreateTest(unittest.TestCase):
    def create(self, spark, root=ROOT):
        with mock.patch.object(config, "BRONZE_TABLES_ROOT", root), mock.patch.object(config, "UC_CATALOG", "cat"):
            return sql.create_bronze_tables(spark)

    def test_creates_the_tables_and_checks_where_they_are(self):
        spark = FakeSpark()
        with self.assertLogs("sql", "INFO"):
            self.assertEqual(self.create(spark), ["cat.bronze.tmdb_movies", "cat.bronze.musicbrainz_release_groups"])
        self.assertEqual(len(spark.statements), 4)
        self.assertIn("CREATE TABLE IF NOT EXISTS cat.bronze.tmdb_movies", spark.statements[0])
        self.assertEqual(spark.statements[1], "DESCRIBE DETAIL cat.bronze.tmdb_movies")

    def test_a_table_of_the_same_name_elsewhere_fails(self):
        spark = FakeSpark({"cat.bronze.tmdb_movies": "abfss://uc@acct.dfs.core.windows.net/__unitystorage/t/1"})
        with self.assertRaisesRegex(RuntimeError, "drop it"):
            self.create(spark)

    def test_tables_root_must_be_an_abfss_path(self):
        for root in ("", "/tmp/tables", f"{ROOT}' OR '1"):
            with self.subTest(root=root), self.assertRaises(ValueError):
                self.create(FakeSpark(), root)


class LoadTest(unittest.TestCase):
    def load(self, spark, landing=LANDING):
        with mock.patch.object(config, "BRONZE_ROOT", landing), mock.patch.object(config, "UC_CATALOG", "cat"):
            return sql.load_bronze_tables(spark)

    def test_copy_into_every_table(self):
        spark = FakeSpark()
        with self.assertLogs("sql", "INFO"):
            loaded = self.load(spark)
        self.assertEqual(loaded, {"cat.bronze.tmdb_movies": {"num_inserted_rows": 3},
                                  "cat.bronze.musicbrainz_release_groups": {"num_inserted_rows": 3}})
        self.assertEqual([s.count("COPY INTO cat.bronze.") for s in spark.statements], [1, 1])

    def test_landing_must_be_an_abfss_path(self):
        for landing in ("", "/tmp/landing", f"{LANDING}' OR '1"):
            with self.subTest(landing=landing), self.assertRaises(ValueError):
                self.load(FakeSpark(), landing)


if __name__ == "__main__":
    unittest.main()
