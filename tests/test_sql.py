"""src/sql: the bronze table statements, and create_bronze_tables with a fake Spark session."""
import os
import re
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import sql  # noqa: E402
from common import config  # noqa: E402

ROOT = "abfss://bronze@acct.dfs.core.windows.net/tables"
LINEAGE = {"dt", "batch_id", "_source_file", "_ingested_at", "_file_size", "_file_modified_at"}


def columns(name: str) -> set:
    text = sql.render("bronze", name, catalog="cat", tables_root=ROOT)
    return set(re.findall(r"^\s+(\w+)\s+[A-Z]", text, re.M))


class FakeSpark:
    def __init__(self, locations=None):
        self.statements, self.locations = [], locations or {}

    def sql(self, statement):
        self.statements.append(statement)
        if statement.startswith("DESCRIBE DETAIL "):
            table = statement.split()[-1]
            row = {"location": self.locations.get(table, f"{ROOT}/{table.split('.')[-1]}/")}
            return mock.Mock(first=mock.Mock(return_value=row))
        return mock.Mock()


class StatementTest(unittest.TestCase):
    def test_external_delta_tables_in_tables_root(self):
        for name in sql.BRONZE_TABLES:
            with self.subTest(table=name):
                text = sql.render("bronze", name, catalog="cat", tables_root=ROOT)
                self.assertNotIn("$", text)
                self.assertIn(f"CREATE TABLE IF NOT EXISTS cat.bronze.{name} (", text)
                self.assertIn("USING DELTA", text)
                self.assertIn(f"LOCATION '{ROOT}/{name}'", text)
        files = os.listdir(os.path.join(sql.HERE, "bronze"))
        self.assertEqual(sorted(f[:-len(".sql")] for f in files), sorted(sql.BRONZE_TABLES))

    def test_columns(self):
        for name in sql.BRONZE_TABLES:
            with self.subTest(table=name):
                self.assertLessEqual({"id", "title"} | LINEAGE, columns(name))
        # fields written by the ingest that silver may need
        self.assertLessEqual({"endpoint", "genres", "genre_ids", "overview", "original_title", "poster_path"},
                             columns("tmdb_movies"))
        self.assertLessEqual({"query_genre", "artist_credit", "tags", "primary_type", "secondary_types"},
                             columns("musicbrainz_release_groups"))

    def test_missing_placeholder_fails(self):
        with self.assertRaises(KeyError):
            sql.render("bronze", "tmdb_movies", catalog="cat")


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


if __name__ == "__main__":
    unittest.main()
