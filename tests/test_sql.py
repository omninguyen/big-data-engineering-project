"""src/sql: the bronze and silver statements, and the runner with a fake Spark session."""
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
SILVER = "abfss://silver@acct.dfs.core.windows.net"
ROOTS = {"bronze": ROOT, "silver": SILVER}
SOURCES = {"tmdb_movies": "tmdb/movies", "musicbrainz_release_groups": "musicbrainz/release_groups"}


def create(layer: str, name: str) -> str:
    placeholder = {"bronze": "tables_root", "silver": "silver_root"}[layer]
    return sql.render(layer, name, catalog="cat", **{placeholder: ROOTS[layer]})


def columns(name: str, layer: str = "bronze") -> list:
    """Columns of the CREATE TABLE, in order."""
    return re.findall(r"^\s+(\w+)\s+[A-Z]", create(layer, name), re.M)


def column_list(statement: str) -> list:
    """The column list of the INSERT in a statement."""
    found = re.search(r"INSERT(?: (?:INTO|OVERWRITE) \S+)?\s*\(([^)]*)\)", statement)
    return [c.strip() for c in found.group(1).split(",")]


def load(name: str) -> str:
    return sql.render("bronze/load", name, catalog="cat", landing_root=LANDING)


def result(**counts):
    return mock.Mock(asDict=mock.Mock(return_value=counts))


class FakeSpark:
    """Records statements. Tables are where they should be; MERGE/INSERT insert 2 rows and update 1;
    `batches` are the new bronze batches of each silver table."""
    def __init__(self, locations=None, batches=None):
        self.statements, self.locations, self.batches = [], locations or {}, batches or {}

    def sql(self, statement):
        self.statements.append(statement)
        rows = []
        if statement.startswith("DESCRIBE DETAIL "):
            table = statement.split()[-1]
            _, layer, name = table.split(".")
            rows = [{"location": self.locations.get(table, f"{ROOTS[layer]}/{name}/")}]
        elif "COPY INTO" in statement:
            rows = [result(num_inserted_rows=3)]
        elif "EXCEPT" in statement:
            table = re.search(r"table_name = '(\w+)'", statement).group(1)
            rows = [{"batch_id": b} for b in self.batches.get(table, [])]
        elif re.search(r"^(MERGE INTO|INSERT INTO|INSERT OVERWRITE)", statement, re.M):
            rows = [result(num_affected_rows=3, num_inserted_rows=2, num_updated_rows=1)]
        return mock.Mock(first=mock.Mock(return_value=rows[0] if rows else None), collect=mock.Mock(return_value=rows))


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



class SplitTest(unittest.TestCase):
    def test_statements_end_with_a_semicolon_at_the_end_of_a_line(self):
        view, merge = "-- header\nCREATE TEMP VIEW v AS SELECT 'a;b' AS x", "-- step 2\nMERGE INTO t USING v ON 1 = 1"
        self.assertEqual(sql.statements(f"{view};\n\n{merge};\n-- end\n"), [view, merge])


class SilverStatementTest(unittest.TestCase):
    def test_external_delta_tables_in_the_silver_container_with_a_primary_key(self):
        for name in sql.SILVER_TABLES:
            with self.subTest(table=name):
                text = create("silver", name)
                self.assertNotIn("$", text)
                self.assertIn(f"CREATE TABLE IF NOT EXISTS cat.silver.{name} (", text)
                self.assertIn(f"LOCATION '{SILVER}/{name}'", text)
                self.assertIn("USING DELTA", text)
                key = re.search(r"PRIMARY KEY \(([^)]*)\)", text).group(1).split(", ")
                for column in key:  # Unity Catalog needs NOT NULL key columns
                    self.assertRegex(text, rf"\n  {column} +\w+ +NOT NULL")
        files = [f[:-len(".sql")] for f in os.listdir(os.path.join(sql.HERE, "silver", "load"))]
        self.assertEqual(sorted(files), sorted([*sql.SILVER_INCREMENTAL, *sql.SILVER_FULL]))
        for table in ("movie_genre", "release_group_genre"):  # foreign key to genre
            self.assertLess(sql.SILVER_TABLES.index("genre"), sql.SILVER_TABLES.index(table))

    def test_agreed_columns(self):
        self.assertEqual(columns("movie", "silver"), [
            "movie_id", "title", "original_title", "overview", "release_date", "original_language", "poster_path",
            "adult", "checksum", "valid_from", "valid_to", "is_current", "batch_id", "_loaded_at"])
        self.assertEqual(columns("movie_metric_daily", "silver"), [
            "movie_id", "snapshot_date", "popularity", "vote_average", "vote_count", "in_popular", "in_top_rated",
            "batch_id", "_loaded_at"])
        self.assertIn("PRIMARY KEY (movie_id, valid_from)", create("silver", "movie"))
        self.assertIn("PRIMARY KEY (movie_id, snapshot_date)", create("silver", "movie_metric_daily"))
        self.assertEqual(columns("genre", "silver"), ["genre_id", "name", "source", "_loaded_at"])
        self.assertEqual(columns("movie_genre", "silver"), ["movie_id", "genre_id", "position", "_loaded_at"])
        self.assertIn("PRIMARY KEY (movie_id, genre_id)", create("silver", "movie_genre"))
        self.assertIn("FOREIGN KEY (genre_id) REFERENCES cat.silver.genre", create("silver", "movie_genre"))
        self.assertEqual(columns("release_group", "silver"), [
            "release_group_id", "title", "artist_credit_name", "first_release_date", "release_date", "release_year",
            "primary_type", "secondary_types", "checksum", "valid_from", "valid_to", "is_current", "batch_id",
            "_loaded_at"])
        self.assertIn("PRIMARY KEY (release_group_id, valid_from)", create("silver", "release_group"))
        self.assertEqual(columns("release_group_genre", "silver"),
                         ["release_group_id", "genre_id", "position", "tag_count", "_loaded_at"])
        self.assertIn("PRIMARY KEY (release_group_id, genre_id)", create("silver", "release_group_genre"))
        self.assertIn("FOREIGN KEY (genre_id) REFERENCES cat.silver.genre", create("silver", "release_group_genre"))
        self.assertEqual(columns("artist", "silver"), [
            "artist_id", "name", "checksum", "valid_from", "valid_to", "is_current", "batch_id", "_loaded_at"])
        self.assertIn("PRIMARY KEY (artist_id, valid_from)", create("silver", "artist"))
        self.assertEqual(columns("release_group_artist", "silver"), [
            "release_group_id", "artist_id", "position", "credited_name", "join_phrase", "_loaded_at"])
        self.assertIn("PRIMARY KEY (release_group_id, artist_id)", create("silver", "release_group_artist"))

    def test_full_loads(self):
        for name in sql.SILVER_FULL:
            with self.subTest(table=name):
                steps = sql.statements(sql.render("silver/load", name, catalog="cat"))
                self.assertEqual(len(steps), 1)
                self.assertRegex(steps[0], rf"INSERT OVERWRITE cat\.silver\.{name}\s*\(")  # one write replaces it
                self.assertEqual(column_list(steps[0]), columns(name, "silver"))
        genre = sql.render("silver/load", "genre", catalog="cat")
        movie_genre = sql.render("silver/load", "movie_genre", catalog="cat")
        album_genre = sql.render("silver/load", "release_group_genre", catalog="cat")
        for text in (genre, movie_genre):  # the same genre_id in both tables, no sample movies
            self.assertIn("concat('tmdb:', lower(trim(name)))", text)
            self.assertIn("try_cast(id AS BIGINT) IS NOT NULL", text)
        self.assertIn("concat('musicbrainz:', lower(trim(query_genre)))", genre)
        self.assertIn("concat('musicbrainz:', genre)", album_genre)  # genre = lower(trim(query_genre or tag name))
        self.assertIn("ORDER BY batch_id DESC", movie_genre)  # the latest row of each movie
        self.assertIn("movie_id IN (SELECT movie_id FROM cat.silver.movie)", movie_genre)
        self.assertIn("WHERE batch_id = latest_batch_id", album_genre)  # the latest batch of each album
        self.assertIn("JOIN genres AS g ON f.genre = g.genre", album_genre)  # only the searched genres
        self.assertIn("release_group_id IN (SELECT release_group_id FROM cat.silver.release_group)", album_genre)
        album_artist = sql.render("silver/load", "release_group_artist", catalog="cat")
        self.assertIn("posexplode(artist_credit) AS (position, credit)", album_artist)  # credit order kept
        self.assertIn("WHERE rn = 1", album_artist)  # the latest row of each album
        self.assertIn("release_group_id IN (SELECT release_group_id FROM cat.silver.release_group)", album_artist)
        self.assertIn("artist_id IN (SELECT artist_id FROM cat.silver.artist)", album_artist)

    def test_movie_scd2_load(self):
        steps = sql.statements(sql.render("silver/load", "movie", catalog="cat", batch_id="B1"))
        self.assertEqual(len(steps), 3)
        view, close, insert = steps
        self.assertIn("WHERE batch_id = 'B1' AND try_cast(id AS BIGINT) IS NOT NULL", view)  # no sample movies
        self.assertIn("sha2(to_json(struct(title, original_title, overview, release_date, original_language, "
                      "poster_path, adult)), 256)", view)
        self.assertIn("WHEN MATCHED AND t.checksum <> s.checksum", close)
        self.assertIn("valid_to = s.valid_from, is_current = false", close)
        self.assertEqual(column_list(insert), columns("movie", "silver"))
        self.assertIn("LEFT ANTI JOIN cat.silver.movie AS t ON t.movie_id = s.movie_id AND t.is_current", insert)

    def test_release_group_scd2_load(self):
        steps = sql.statements(sql.render("silver/load", "release_group", catalog="cat", batch_id="B1"))
        self.assertEqual(len(steps), 3)
        view, close, insert = steps
        self.assertIn("WHERE batch_id = 'B1' AND id <> ''", view)
        self.assertIn("row_number() OVER (PARTITION BY id", view)  # one row per album, whatever the searches
        self.assertIn("sha2(to_json(struct(title, artist_credit_name, first_release_date, primary_type, "
                      "secondary_types)), 256)", view)  # no tags: their votes change daily
        self.assertIn("CASE WHEN length(first_release_date) = 10 THEN", view)  # 1997 is not 1997-01-01
        self.assertIn("WHEN MATCHED AND t.checksum <> s.checksum", close)
        self.assertEqual(column_list(insert), columns("release_group", "silver"))
        self.assertIn("LEFT ANTI JOIN cat.silver.release_group AS t", insert)

    def test_artist_scd2_load(self):
        steps = sql.statements(sql.render("silver/load", "artist", catalog="cat", batch_id="B1"))
        self.assertEqual(len(steps), 3)
        view, close, insert = steps
        self.assertIn("explode(artist_credit) AS credit", view)
        self.assertIn("WHERE batch_id = 'B1' AND id <> ''", view)
        self.assertIn("WHERE credit.artist.id <> ''", view)  # a credit without an artist cannot be linked
        self.assertIn("row_number() OVER (PARTITION BY credit.artist.id", view)  # one row per artist
        self.assertIn("sha2(to_json(struct(name)), 256)", view)
        self.assertIn("WHEN MATCHED AND t.checksum <> s.checksum", close)
        self.assertEqual(column_list(insert), columns("artist", "silver"))
        self.assertIn("LEFT ANTI JOIN cat.silver.artist AS t ON t.artist_id = s.artist_id AND t.is_current", insert)

    def test_metric_load_and_the_load_log(self):
        steps = sql.statements(sql.render("silver/load", "movie_metric_daily", catalog="cat", batch_id="B1"))
        self.assertEqual(len(steps), 1)
        self.assertIn("WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE", steps[0])  # a later batch of the day
        self.assertEqual(column_list(steps[0]), columns("movie_metric_daily", "silver"))
        logged = sql.render("silver", "log_batch", catalog="cat", table="movie", batch_id="B1",
                            inserted="2", updated="1")
        self.assertEqual(column_list(logged), columns("load_log", "silver"))
        self.assertIn("VALUES ('movie', 'B1', 2, 1, current_timestamp())", logged)


class SilverTest(unittest.TestCase):
    def run_silver(self, spark, root=SILVER):
        with mock.patch.object(config, "SILVER_ROOT", root), mock.patch.object(config, "UC_CATALOG", "cat"):
            sql.create_silver_tables(spark)
            return sql.load_silver_tables(spark)

    def test_each_new_batch_oldest_first_then_recorded(self):
        spark = FakeSpark(batches={"movie": ["B1", "B2"], "movie_metric_daily": ["B2"]})
        with self.assertLogs("sql", "INFO"):
            loaded = self.run_silver(spark)
        self.assertEqual(loaded, {
            "cat.silver.movie": {"batches": 2}, "cat.silver.movie_metric_daily": {"batches": 1},
            "cat.silver.release_group": {"batches": 0}, "cat.silver.artist": {"batches": 0},
            "cat.silver.genre": {"rows": 2}, "cat.silver.movie_genre": {"rows": 2},
            "cat.silver.release_group_genre": {"rows": 2}, "cat.silver.release_group_artist": {"rows": 2}})
        logged = [s for s in spark.statements if "INSERT INTO cat.silver.load_log" in s]
        self.assertEqual(len(logged), 3)
        # the row counts of the batch's MERGE and INSERT, summed (the fake gives 2 inserted, 1 updated for each)
        self.assertIn("VALUES ('movie', 'B1', 4, 2,", logged[0])
        self.assertIn("VALUES ('movie', 'B2', 4, 2,", logged[1])
        self.assertIn("VALUES ('movie_metric_daily', 'B2', 2, 1,", logged[2])
        b1 = next(i for i, s in enumerate(spark.statements) if "batch_id = 'B1'" in s)
        b2 = next(i for i, s in enumerate(spark.statements) if "batch_id = 'B2'" in s)
        self.assertLess(b1, b2)
        full = [i for i, s in enumerate(spark.statements) if "INSERT OVERWRITE" in s]
        self.assertEqual(len(full), len(sql.SILVER_FULL))
        self.assertGreater(min(full), max(i for i, s in enumerate(spark.statements) if "load_log" in s))  # last

    def test_nothing_new_still_rebuilds_the_full_load_tables(self):
        spark = FakeSpark()
        loaded = self.run_silver(spark)
        for name in sql.SILVER_INCREMENTAL:
            self.assertEqual(loaded[f"cat.silver.{name}"], {"batches": 0})
        self.assertFalse([s for s in spark.statements if re.search(r"^(MERGE|INSERT) INTO", s, re.M)])
        self.assertEqual(len([s for s in spark.statements if "INSERT OVERWRITE" in s]), len(sql.SILVER_FULL))

    def test_odd_batch_id_or_root_fails(self):
        with self.assertRaises(ValueError):
            self.run_silver(FakeSpark(batches={"movie": ["B1' OR '1"]}))
        for root in ("", "/tmp/silver", f"{SILVER}' OR '1"):
            with self.subTest(root=root), self.assertRaises(ValueError):
                self.run_silver(FakeSpark(), root)

    def test_a_managed_table_of_the_same_name_fails(self):
        spark = FakeSpark({"cat.silver.movie": "abfss://uc@acct.dfs.core.windows.net/__unitystorage/t/1"})
        with self.assertRaisesRegex(RuntimeError, "drop it"):
            self.run_silver(spark)



class OneTableTest(unittest.TestCase):
    """One job task per table: setup creates every table, then each task loads one."""
    def patched(self):
        return mock.patch.multiple(config, BRONZE_TABLES_ROOT=ROOT, BRONZE_ROOT=LANDING, SILVER_ROOT=SILVER,
                                   UC_CATALOG="cat")

    def test_setup_creates_bronze_then_silver(self):
        spark = FakeSpark()
        with self.patched(), self.assertLogs("sql", "INFO"):
            tables = sql.create_tables(spark)
        self.assertEqual(tables, [f"cat.bronze.{t}" for t in sql.BRONZE_TABLES] +
                         [f"cat.silver.{t}" for t in sql.SILVER_TABLES])

    def test_one_bronze_table(self):
        spark = FakeSpark()
        with self.patched(), self.assertLogs("sql", "INFO"):
            self.assertEqual(sql.load_bronze_table(spark, "tmdb_movies"), {"num_inserted_rows": 3})
        self.assertEqual(len(spark.statements), 1)
        self.assertIn("COPY INTO cat.bronze.tmdb_movies", spark.statements[0])

    def test_one_silver_table(self):
        spark = FakeSpark(batches={"movie": ["B1"], "release_group": ["B1"]})
        with self.patched(), self.assertLogs("sql", "INFO"):
            self.assertEqual(sql.load_silver_table(spark, "movie"), {"batches": 1})
        self.assertFalse([s for s in spark.statements if "release_group" in s])  # only its own batches
        spark = FakeSpark()
        with self.patched(), self.assertLogs("sql", "INFO"):
            self.assertEqual(sql.load_silver_table(spark, "genre"), {"rows": 2})
        self.assertEqual(len(spark.statements), 1)

    def test_unknown_table_fails(self):
        with self.patched():
            for load, name in ((sql.load_bronze_table, "movie"), (sql.load_silver_table, "load_log")):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    load(FakeSpark(), name)


if __name__ == "__main__":
    unittest.main()
