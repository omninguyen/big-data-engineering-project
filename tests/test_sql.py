"""src/sql: the bronze, silver, gold and ops statements, the marts views, and the runner with a fake Spark session."""
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
GOLD = "abfss://gold@acct.dfs.core.windows.net"
OPS = f"{GOLD}/ops"
ROOTS = {"bronze": ROOT, "silver": SILVER, "gold": GOLD, "ops": OPS}
SOURCES = {"tmdb_movies": "tmdb/movies", "tmdb_trending_movies": "tmdb/trending_movies",
           "tmdb_tv_shows": "tmdb/tv_shows",
           "musicbrainz_release_groups": "musicbrainz/release_groups",
           "listenbrainz_release_group_popularity": "listenbrainz/release_group_popularity",
           "wikidata_media_articles": "wikidata/media_articles", "wikipedia_pageviews": "wikipedia/pageviews"}


def create(layer: str, name: str) -> str:
    placeholder = {"bronze": "tables_root", "silver": "silver_root", "gold": "gold_root", "ops": "ops_root"}[layer]
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
                self.assertLessEqual(LINEAGE, set(columns(name)))
        for name in ("tmdb_movies", "musicbrainz_release_groups"):
            self.assertLessEqual({"id", "title"}, set(columns(name)))
        self.assertEqual(columns("listenbrainz_release_group_popularity")[:3],
                         ["release_group_mbid", "total_listen_count", "total_user_count"])
        self.assertEqual(columns("wikidata_media_articles")[:5],
                         ["media_type", "source_id", "wikidata_id", "article", "url"])
        self.assertEqual(columns("wikipedia_pageviews")[:5], ["project", "article", "view_date", "views", "agent"])
        # the API gives the day as yyyymmddhh
        self.assertIn("substr(value:['timestamp'], 1, 4), substr(value:['timestamp'], 5, 2)",
                      load("wikipedia_pageviews"))
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
            self.assertEqual(self.create(spark), [f"cat.bronze.{name}" for name in sql.BRONZE_TABLES])
        self.assertEqual(len(spark.statements), 2 * len(sql.BRONZE_TABLES))  # CREATE, then DESCRIBE DETAIL
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
        self.assertEqual(loaded, {f"cat.bronze.{name}": {"num_inserted_rows": 3} for name in sql.BRONZE_TABLES})
        self.assertEqual([s.count("COPY INTO cat.bronze.") for s in spark.statements], [1] * len(sql.BRONZE_TABLES))

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
        for table in ("movie_genre", "release_group_genre", "tv_show_genre"):  # foreign key to genre
            self.assertLess(sql.SILVER_TABLES.index("genre"), sql.SILVER_TABLES.index(table))
            self.assertLess(sql.SILVER_FULL.index("genre"), sql.SILVER_FULL.index(table))  # loaded after it

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
        self.assertEqual(columns("media_wiki_page", "silver"), [
            "media_type", "source_id", "wikidata_id", "article", "url", "batch_id", "_loaded_at"])
        self.assertIn("PRIMARY KEY (media_type, source_id)", create("silver", "media_wiki_page"))
        self.assertEqual(columns("wiki_pageview_daily", "silver"),
                         ["article", "view_date", "views", "batch_id", "_loaded_at"])
        self.assertIn("PRIMARY KEY (article, view_date)", create("silver", "wiki_pageview_daily"))
        self.assertEqual(columns("movie_trending_daily", "silver"),
                         ["movie_id", "snapshot_date", "rank", "batch_id", "_loaded_at"])
        self.assertIn("PRIMARY KEY (movie_id, snapshot_date)", create("silver", "movie_trending_daily"))
        self.assertEqual(columns("tv_show", "silver"), [
            "tv_id", "name", "original_name", "overview", "first_air_date", "original_language", "origin_country",
            "poster_path", "adult", "checksum", "valid_from", "valid_to", "is_current", "batch_id", "_loaded_at"])
        self.assertIn("PRIMARY KEY (tv_id, valid_from)", create("silver", "tv_show"))
        self.assertEqual(columns("tv_show_metric_daily", "silver"), [
            "tv_id", "snapshot_date", "popularity", "vote_average", "vote_count", "batch_id", "_loaded_at"])
        self.assertIn("PRIMARY KEY (tv_id, snapshot_date)", create("silver", "tv_show_metric_daily"))
        self.assertEqual(columns("tv_show_genre", "silver"), ["tv_id", "genre_id", "position", "_loaded_at"])
        self.assertIn("PRIMARY KEY (tv_id, genre_id)", create("silver", "tv_show_genre"))

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

    def test_listenbrainz_metric_load(self):
        self.assertEqual(columns("release_group_metric_daily", "silver"), [
            "release_group_id", "snapshot_date", "listen_count", "listener_count", "batch_id", "_loaded_at"])
        self.assertIn("PRIMARY KEY (release_group_id, snapshot_date)", create("silver", "release_group_metric_daily"))
        steps = sql.statements(sql.render("silver/load", "release_group_metric_daily", catalog="cat", batch_id="B1"))
        self.assertEqual(len(steps), 1)
        self.assertIn("FROM cat.bronze.listenbrainz_release_group_popularity", steps[0])
        self.assertIn("total_listen_count IS NOT NULL", steps[0])  # albums without ListenBrainz data
        self.assertIn("WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE", steps[0])
        self.assertEqual(column_list(steps[0]), columns("release_group_metric_daily", "silver"))

    def test_wikipedia_loads(self):
        for name, source in (("media_wiki_page", "wikidata_media_articles"),
                             ("wiki_pageview_daily", "wikipedia_pageviews")):
            with self.subTest(table=name):
                steps = sql.statements(sql.render("silver/load", name, catalog="cat", batch_id="B1"))
                self.assertEqual(len(steps), 1)
                self.assertIn(f"FROM cat.bronze.{source}", steps[0])
                self.assertIn("WHERE batch_id = 'B1'", steps[0])
                self.assertIn("WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE", steps[0])  # the later batch wins
                self.assertEqual(column_list(steps[0]), columns(name, "silver"))
        mapping = sql.render("silver/load", "media_wiki_page", catalog="cat", batch_id="B1")
        self.assertIn("GROUP BY media_type, source_id", mapping)  # one article per movie or album
        self.assertIn("min_by(url, article)", mapping)

    def test_tv_show_loads(self):
        steps = sql.statements(sql.render("silver/load", "tv_show", catalog="cat", batch_id="B1"))
        self.assertEqual(len(steps), 3)  # batch view, close changed versions, add new versions (as silver.movie)
        self.assertIn("FROM cat.bronze.tmdb_tv_shows", steps[0])
        self.assertIn("WHERE batch_id = 'B1'", steps[0])
        self.assertIn("array_join(origin_country, ',')               AS origin_country", steps[0])  # flat column
        self.assertIn("WHEN MATCHED AND t.checksum <> s.checksum AND s.valid_from > t.valid_from", steps[1])
        self.assertIn("LEFT ANTI JOIN cat.silver.tv_show AS t ON t.tv_id = s.tv_id AND t.is_current", steps[2])
        self.assertEqual(column_list(steps[2]), columns("tv_show", "silver"))
        metric = sql.statements(sql.render("silver/load", "tv_show_metric_daily", catalog="cat", batch_id="B1"))
        self.assertEqual(len(metric), 1)
        self.assertIn("WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE", metric[0])  # a later batch of the day
        self.assertEqual(column_list(metric[0]), columns("tv_show_metric_daily", "silver"))
        genres = sql.render("silver/load", "tv_show_genre", catalog="cat")
        self.assertIn("concat('tmdb_tv:', lower(trim(name))) AS genre_id", genres)  # TV genres are a list of their own
        self.assertIn("WHERE tv_id IN (SELECT tv_id FROM cat.silver.tv_show)", genres)
        genre = sql.render("silver/load", "genre", catalog="cat")
        self.assertIn("SELECT concat('tmdb_tv:', lower(trim(name))), trim(name), 'tmdb_tv'", genre)
        self.assertIn("FROM cat.bronze.tmdb_tv_shows", genre)

    def test_trending_load(self):
        steps = sql.statements(sql.render("silver/load", "movie_trending_daily", catalog="cat", batch_id="B1"))
        self.assertEqual(len(steps), 1)
        self.assertIn("FROM cat.bronze.tmdb_trending_movies", steps[0])
        self.assertIn("WHERE batch_id = 'B1'", steps[0])
        self.assertIn("min(rank)               AS rank", steps[0])  # a movie repeated on two pages keeps its best
        self.assertIn("WHEN MATCHED AND s.batch_id > t.batch_id THEN UPDATE", steps[0])  # a later batch of the day
        self.assertEqual(column_list(steps[0]), columns("movie_trending_daily", "silver"))

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
            "cat.silver.release_group_metric_daily": {"batches": 0},
            "cat.silver.media_wiki_page": {"batches": 0}, "cat.silver.wiki_pageview_daily": {"batches": 0},
            "cat.silver.movie_trending_daily": {"batches": 0},
            "cat.silver.tv_show": {"batches": 0}, "cat.silver.tv_show_metric_daily": {"batches": 0},
            "cat.silver.genre": {"rows": 2}, "cat.silver.movie_genre": {"rows": 2},
            "cat.silver.release_group_genre": {"rows": 2}, "cat.silver.release_group_artist": {"rows": 2},
            "cat.silver.tv_show_genre": {"rows": 2}})
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



class GoldStatementTest(unittest.TestCase):
    def test_external_delta_tables_in_the_gold_container_with_a_primary_key(self):
        for name in sql.GOLD_TABLES:
            with self.subTest(table=name):
                text = create("gold", name)
                self.assertNotIn("$", text)
                self.assertIn(f"CREATE TABLE IF NOT EXISTS cat.gold.{name} (", text)
                self.assertIn(f"LOCATION '{GOLD}/{name}'", text)
                self.assertIn("USING DELTA", text)
                key = re.search(r"PRIMARY KEY \(([^)]*)\)", text).group(1).split(", ")
                for column in key:  # Unity Catalog needs NOT NULL key columns
                    self.assertRegex(text, rf"\n  {column} +\w+ +NOT NULL")
        for folder in ("gold", "gold/load"):
            files = [f[:-len(".sql")] for f in os.listdir(os.path.join(sql.HERE, folder)) if f.endswith(".sql")]
            self.assertEqual(sorted(files), sorted(sql.GOLD_TABLES), folder)

    def test_agreed_columns(self):
        self.assertEqual(columns("dim_date", "gold"), [
            "date_key", "date", "year", "quarter", "month", "month_name", "day", "day_of_week", "day_name",
            "is_weekend"])
        self.assertIn("PRIMARY KEY (date_key)", create("gold", "dim_date"))
        self.assertEqual(columns("dim_media", "gold"), [
            "media_key", "media_id", "media_type", "title", "creator", "release_date", "release_year",
            "release_decade", "primary_genre", "genres", "language", "source"])
        self.assertIn("PRIMARY KEY (media_key)", create("gold", "dim_media"))
        self.assertEqual(columns("dim_genre", "gold"), ["genre_id", "genre_name", "media_type"])
        self.assertIn("PRIMARY KEY (genre_id)", create("gold", "dim_genre"))
        bridge = create("gold", "bridge_media_genre")
        self.assertEqual(columns("bridge_media_genre", "gold"), ["media_key", "genre_id", "is_primary"])
        self.assertIn("PRIMARY KEY (media_key, genre_id)", bridge)
        self.assertIn("FOREIGN KEY (media_key) REFERENCES cat.gold.dim_media", bridge)
        self.assertIn("FOREIGN KEY (genre_id) REFERENCES cat.gold.dim_genre", bridge)
        for dim in ("dim_media", "dim_genre"):  # a foreign key needs the table it refers to
            self.assertLess(sql.GOLD_TABLES.index(dim), sql.GOLD_TABLES.index("bridge_media_genre"))
        fact = create("gold", "fact_media_metric_daily")
        self.assertEqual(columns("fact_media_metric_daily", "gold"), [
            "date_key", "media_key", "popularity", "rating", "votes", "listen_count", "listener_count",
            "listen_count_change"])
        self.assertIn("PRIMARY KEY (date_key, media_key)", fact)
        self.assertIn("FOREIGN KEY (date_key) REFERENCES cat.gold.dim_date", fact)
        self.assertIn("FOREIGN KEY (media_key) REFERENCES cat.gold.dim_media", fact)
        for dim in ("dim_date", "dim_media"):
            self.assertLess(sql.GOLD_TABLES.index(dim), sql.GOLD_TABLES.index("fact_media_metric_daily"))
        change = create("gold", "fact_media_change")
        self.assertEqual(columns("fact_media_change", "gold"), ["date_key", "media_key", "change_type", "changed_at"])
        self.assertIn("PRIMARY KEY (media_key, changed_at)", change)
        self.assertIn("FOREIGN KEY (date_key) REFERENCES cat.gold.dim_date", change)
        self.assertIn("FOREIGN KEY (media_key) REFERENCES cat.gold.dim_media", change)
        for dim in ("dim_date", "dim_media"):
            self.assertLess(sql.GOLD_TABLES.index(dim), sql.GOLD_TABLES.index("fact_media_change"))
        page = create("gold", "dim_wiki_page")
        self.assertEqual(columns("dim_wiki_page", "gold"), ["media_key", "article", "wikidata_id", "url"])
        self.assertIn("PRIMARY KEY (media_key)", page)
        self.assertIn("FOREIGN KEY (media_key) REFERENCES cat.gold.dim_media", page)
        self.assertLess(sql.GOLD_TABLES.index("dim_media"), sql.GOLD_TABLES.index("dim_wiki_page"))
        views = create("gold", "fact_wiki_pageview_daily")
        self.assertEqual(columns("fact_wiki_pageview_daily", "gold"), ["date_key", "media_key", "views"])
        self.assertIn("PRIMARY KEY (date_key, media_key)", views)
        self.assertIn("FOREIGN KEY (date_key) REFERENCES cat.gold.dim_date", views)
        self.assertIn("FOREIGN KEY (media_key) REFERENCES cat.gold.dim_media", views)
        for table in ("dim_date", "dim_media", "dim_wiki_page"):  # its load reads dim_wiki_page
            self.assertLess(sql.GOLD_TABLES.index(table), sql.GOLD_TABLES.index("fact_wiki_pageview_daily"))
        trending = create("gold", "fact_media_trending_daily")
        self.assertEqual(columns("fact_media_trending_daily", "gold"), ["date_key", "media_key", "rank"])
        self.assertIn("PRIMARY KEY (date_key, media_key)", trending)
        self.assertIn("FOREIGN KEY (date_key) REFERENCES cat.gold.dim_date", trending)
        self.assertIn("FOREIGN KEY (media_key) REFERENCES cat.gold.dim_media", trending)
        for dim in ("dim_date", "dim_media"):
            self.assertLess(sql.GOLD_TABLES.index(dim), sql.GOLD_TABLES.index("fact_media_trending_daily"))

    def test_full_loads(self):
        for name in sql.GOLD_TABLES:
            with self.subTest(table=name):
                steps = sql.statements(sql.render("gold/load", name, catalog="cat"))
                self.assertEqual(len(steps), 1)
                self.assertRegex(steps[0], rf"INSERT OVERWRITE cat\.gold\.{name}\s*\(")  # one write replaces it
                self.assertEqual(column_list(steps[0]), columns(name, "gold"))

    def test_dim_date(self):
        text = sql.render("gold/load", "dim_date", catalog="cat")
        self.assertIn("CAST(date_format(d, 'yyyyMMdd') AS INT)  AS date_key", text)
        self.assertIn("weekday(d) + 1", text)  # 1 = Monday ... 7 = Sunday
        self.assertIn("sequence(DATE'1900-01-01', make_date(year(current_date()) + 1, 12, 31)", text)

    def test_dim_media(self):
        text = sql.render("gold/load", "dim_media", catalog="cat")
        for table in ("movie", "release_group"):  # the current version only
            self.assertIn(f"FROM cat.silver.{table}\n  WHERE is_current", text)
        # the same media_key in the media and in their genres
        self.assertIn("concat('movie:', CAST(movie_id AS STRING))  AS media_key", text)
        self.assertIn("concat('movie:', CAST(mg.movie_id AS STRING)) AS media_key", text)
        self.assertIn("concat('music:', release_group_id), release_group_id, 'music'", text)
        self.assertIn("concat('music:', rg.release_group_id), rg.position", text)
        self.assertIn("sort_array(collect_list(struct(position, name)))", text)  # primary genre first
        self.assertIn("LEFT JOIN genres AS g ON g.media_key = m.media_key", text)  # media without genres are kept
        # TV shows: tv:<TMDB tv_id>, the current version, their first air date as release date
        self.assertIn("concat('tv:', CAST(tv_id AS STRING)), CAST(tv_id AS STRING), 'tv', name, CAST(NULL AS STRING), "
                      "first_air_date,\n    year(first_air_date), original_language, 'tmdb'", text)
        self.assertIn("FROM cat.silver.tv_show\n  WHERE is_current", text)
        self.assertIn("concat('tv:', CAST(tg.tv_id AS STRING)), tg.position, g.name", text)

    def test_genre_and_bridge(self):
        genre = sql.render("gold/load", "dim_genre", catalog="cat")
        self.assertIn("FROM cat.silver.genre", genre)
        # every source of silver.genre: an unknown one fails the load (NOT NULL)
        self.assertIn("CASE source WHEN 'tmdb' THEN 'movie' WHEN 'tmdb_tv' THEN 'tv' WHEN 'musicbrainz' THEN 'music'",
                      genre)
        bridge = sql.render("gold/load", "bridge_media_genre", catalog="cat")
        # the same media_key as gold.dim_media
        self.assertIn("concat('movie:', CAST(movie_id AS STRING)) AS media_key, genre_id, position", bridge)
        self.assertIn("concat('music:', release_group_id), genre_id, position", bridge)
        self.assertIn("FROM cat.silver.movie_genre", bridge)
        self.assertIn("FROM cat.silver.release_group_genre", bridge)
        self.assertIn("concat('tv:', CAST(tv_id AS STRING)), genre_id, position\n  FROM cat.silver.tv_show_genre",
                      bridge)
        # one primary genre per media, the lowest position, as dim_media.primary_genre
        self.assertIn("row_number() OVER (PARTITION BY media_key ORDER BY position) = 1  AS is_primary", bridge)

    def test_change_fact(self):
        text = sql.render("gold/load", "fact_media_change", catalog="cat")
        self.assertIn("FROM cat.silver.movie\n", text)  # every version, not only the current one
        self.assertIn("FROM cat.silver.release_group\n", text)
        self.assertNotIn("is_current", text)
        # the same media_key as gold.dim_media
        self.assertIn("concat('movie:', CAST(movie_id AS STRING)) AS media_key", text)
        self.assertIn("concat('music:', release_group_id), valid_from", text)
        self.assertIn("concat('tv:', CAST(tv_id AS STRING)), valid_from\n  FROM cat.silver.tv_show\n", text)
        self.assertIn("CASE WHEN row_number() OVER (PARTITION BY media_key ORDER BY valid_from) = 1 THEN 'new'", text)
        self.assertIn("date_format(from_utc_timestamp(valid_from, 'Asia/Ho_Chi_Minh'), 'yyyyMMdd')", text)

    def test_fact(self):
        text = sql.render("gold/load", "fact_media_metric_daily", catalog="cat")
        self.assertIn("FROM cat.silver.movie_metric_daily", text)
        self.assertIn("FROM cat.silver.release_group_metric_daily", text)
        self.assertIn("FROM cat.silver.tv_show_metric_daily", text)
        self.assertIn("concat('tv:', CAST(tv_id AS STRING)),\n    popularity,\n    vote_average,\n    vote_count,",
                      text)  # TMDB metrics, as movies
        # the same keys as gold.dim_media and gold.dim_date
        self.assertIn("concat('movie:', CAST(movie_id AS STRING))  AS media_key", text)
        self.assertIn("concat('music:', release_group_id),", text)
        self.assertIn("CAST(date_format(snapshot_date, 'yyyyMMdd') AS INT)  AS date_key", text)
        self.assertIn("WHERE media_key IN (SELECT media_key FROM cat.gold.dim_media)", text)  # every row has its dim
        # listens of the day from the all-time totals
        self.assertIn("listen_count - lag(listen_count) OVER (PARTITION BY release_group_id ORDER BY snapshot_date)",
                      text)


    def test_wikipedia(self):
        page = sql.render("gold/load", "dim_wiki_page", catalog="cat")
        self.assertIn("FROM cat.silver.media_wiki_page", page)
        # the same media_key as gold.dim_media: movie:<TMDB id>, music:<MusicBrainz id>
        self.assertIn("concat(media_type, ':', source_id) AS media_key", page)
        self.assertIn("WHERE media_key IN (SELECT media_key FROM cat.gold.dim_media)", page)  # every row has its dim
        views = sql.render("gold/load", "fact_wiki_pageview_daily", catalog="cat")
        self.assertIn("FROM cat.silver.wiki_pageview_daily AS v", views)
        self.assertIn("JOIN cat.gold.dim_wiki_page AS p ON p.article = v.article", views)  # media of dim_media only
        self.assertIn("CAST(date_format(v.view_date, 'yyyyMMdd') AS INT)  AS date_key", views)


    def test_trending_fact(self):
        text = sql.render("gold/load", "fact_media_trending_daily", catalog="cat")
        self.assertIn("FROM cat.silver.movie_trending_daily", text)
        # the same keys as gold.dim_media and gold.dim_date
        self.assertIn("concat('movie:', CAST(movie_id AS STRING))           AS media_key", text)
        self.assertIn("CAST(date_format(snapshot_date, 'yyyyMMdd') AS INT)  AS date_key", text)
        # a day keeps the ranking of its latest batch only
        self.assertIn("max(batch_id) OVER (PARTITION BY snapshot_date)      AS day_batch_id", text)
        self.assertIn("WHERE batch_id = day_batch_id", text)
        self.assertIn("AND media_key IN (SELECT media_key FROM cat.gold.dim_media)", text)  # every row has its dim


class GoldTest(unittest.TestCase):
    def run_gold(self, spark, root=GOLD):
        with mock.patch.object(config, "GOLD_ROOT", root), mock.patch.object(config, "UC_CATALOG", "cat"):
            sql.create_gold_tables(spark)
            return sql.load_gold_tables(spark)

    def test_rebuilds_every_table(self):
        spark = FakeSpark()
        with self.assertLogs("sql", "INFO"):
            self.assertEqual(self.run_gold(spark), {f"cat.gold.{name}": {"rows": 2} for name in sql.GOLD_TABLES})
        self.assertEqual(len([s for s in spark.statements if "INSERT OVERWRITE cat.gold." in s]), len(sql.GOLD_TABLES))

    def test_odd_root_or_a_managed_table_fails(self):
        for root in ("", "/tmp/gold", f"{GOLD}' OR '1"):
            with self.subTest(root=root), self.assertRaises(ValueError):
                self.run_gold(FakeSpark(), root)
        spark = FakeSpark({"cat.gold.dim_media": "abfss://uc@acct.dfs.core.windows.net/__unitystorage/t/1"})
        with self.assertRaisesRegex(RuntimeError, "drop it"):
            self.run_gold(spark)


class OpsStatementTest(unittest.TestCase):
    # bronze table -> (main silver table, the filter of its silver load)
    FILTERS = {
        "tmdb_movies": ("movie", "try_cast(id AS BIGINT) IS NOT NULL"),
        "tmdb_trending_movies": (
            "movie_trending_daily", "try_cast(id AS BIGINT) IS NOT NULL AND rank IS NOT NULL AND dt IS NOT NULL"),
        "tmdb_tv_shows": ("tv_show", "try_cast(id AS BIGINT) IS NOT NULL"),
        "musicbrainz_release_groups": ("release_group", "id <> ''"),
        "listenbrainz_release_group_popularity": (
            "release_group_metric_daily",
            "release_group_mbid <> '' AND total_listen_count IS NOT NULL AND dt IS NOT NULL"),
        "wikidata_media_articles": (
            "media_wiki_page", "media_type IN ('movie', 'tv', 'music') AND source_id <> '' AND article <> ''"),
        "wikipedia_pageviews": (
            "wiki_pageview_daily", "article <> '' AND views IS NOT NULL AND view_date IS NOT NULL"),
    }

    def test_external_delta_tables_in_ops_with_a_primary_key(self):
        for name in sql.OPS_TABLES:
            with self.subTest(table=name):
                text = create("ops", name)
                self.assertNotIn("$", text)
                self.assertIn(f"CREATE TABLE IF NOT EXISTS cat.ops.{name} (", text)
                self.assertIn(f"LOCATION '{OPS}/{name}'", text)
                self.assertIn("USING DELTA", text)
                key = re.search(r"PRIMARY KEY \(([^)]*)\)", text).group(1).split(", ")
                for column in key:  # Unity Catalog needs NOT NULL key columns
                    self.assertRegex(text, rf"\n  {column} +\w+ +NOT NULL")
        for folder in ("ops", "ops/load"):
            files = [f[:-len(".sql")] for f in os.listdir(os.path.join(sql.HERE, folder)) if f.endswith(".sql")]
            self.assertEqual(sorted(files), sorted(sql.OPS_TABLES), folder)

    def test_agreed_columns(self):
        self.assertEqual(columns("pipeline_record_metrics", "ops"), [
            "dataset", "target", "batch_id", "dt", "rows_read", "rows_valid", "rows_rejected", "rows_duplicate",
            "files", "bytes", "last_file_modified_at", "last_ingested_at", "silver_loaded_at", "silver_lag_s"])
        self.assertIn("PRIMARY KEY (dataset, batch_id)", create("ops", "pipeline_record_metrics"))
        quality = create("ops", "data_quality_results")
        self.assertEqual(columns("data_quality_results", "ops"), [
            "dataset", "batch_id", "check_name", "severity", "value", "op", "threshold", "passed", "description"])
        self.assertIn("PRIMARY KEY (dataset, batch_id, check_name)", quality)
        self.assertIn("FOREIGN KEY (dataset, batch_id)\n    REFERENCES cat.ops.pipeline_record_metrics", quality)
        self.assertLess(sql.OPS_TABLES.index("pipeline_record_metrics"), sql.OPS_TABLES.index("data_quality_results"))

    def test_full_loads(self):
        for name in sql.OPS_TABLES:
            with self.subTest(table=name):
                steps = sql.statements(sql.render("ops/load", name, catalog="cat"))
                self.assertEqual(len(steps), 1)
                self.assertRegex(steps[0], rf"INSERT OVERWRITE cat\.ops\.{name}\s*\(")  # one write replaces it
                self.assertEqual(column_list(steps[0]), columns(name, "ops"))

    def test_record_metrics_use_the_filters_of_the_silver_loads(self):
        text = sql.render("ops/load", "pipeline_record_metrics", catalog="cat")
        self.assertEqual(set(self.FILTERS), set(sql.BRONZE_TABLES))  # every bronze table is measured
        for dataset, (target, predicate) in self.FILTERS.items():
            with self.subTest(dataset=dataset):
                self.assertIn(predicate, sql.render("silver/load", target, catalog="cat", batch_id="B1"))
                self.assertIn(f"CASE WHEN {predicate}", text)
                self.assertIn(f"'{dataset}', '{target}'" if dataset != "tmdb_movies" else
                              f"'{dataset}' AS dataset, '{target}' AS target", text)
                self.assertIn(f"FROM cat.bronze.{dataset}", text)
        self.assertIn("count(valid_key)           AS rows_valid", text)
        self.assertIn("b.rows_valid - b.keys_valid", text)  # duplicates: valid rows minus distinct keys
        self.assertIn("SELECT DISTINCT dataset, batch_id, _source_file, _file_size", text)  # each file counted once
        self.assertIn("LEFT JOIN cat.silver.load_log AS l ON l.table_name = b.target AND l.batch_id = b.batch_id",
                      text)  # a batch not in silver yet is kept
        self.assertIn("timestampdiff(SECOND, b.last_ingested_at, l.loaded_at)", text)

    def test_quality_checks(self):
        text = sql.render("ops/load", "data_quality_results", catalog="cat")
        self.assertEqual(text.count("FROM cat.ops.pipeline_record_metrics"), 3)
        for check in ("'rejected_rate'", "'duplicate_rate'", "'loaded_to_silver'"):
            self.assertIn(check, text)
        self.assertIn("try_divide(rows_rejected, rows_read)", text)
        self.assertIn("CASE op WHEN '<=' THEN value <= threshold ELSE value >= threshold END  AS passed", text)


class OpsTest(unittest.TestCase):
    def run_ops(self, spark, root=OPS):
        with mock.patch.object(config, "OPS_ROOT", root), mock.patch.object(config, "UC_CATALOG", "cat"):
            sql.create_ops_tables(spark)
            return sql.load_ops_tables(spark)

    def test_rebuilds_every_table_in_order(self):
        spark = FakeSpark()
        with self.assertLogs("sql", "INFO"):
            self.assertEqual(self.run_ops(spark), {f"cat.ops.{name}": {"rows": 2} for name in sql.OPS_TABLES})
        loads = [s for s in spark.statements if "INSERT OVERWRITE cat.ops." in s]
        self.assertEqual([re.search(r"INSERT OVERWRITE cat\.ops\.(\w+)", s).group(1) for s in loads], sql.OPS_TABLES)

    def test_odd_root_or_a_managed_table_fails(self):
        for root in ("", "/tmp/ops", f"{OPS}' OR '1"):
            with self.subTest(root=root), self.assertRaises(ValueError):
                self.run_ops(FakeSpark(), root)
        spark = FakeSpark({"cat.ops.pipeline_record_metrics": "abfss://uc@acct.dfs.core.windows.net/__unitystorage/1"})
        with self.assertRaisesRegex(RuntimeError, "drop it"):
            self.run_ops(spark)


def view(name: str) -> str:
    return sql.render("marts", name, catalog="cat")


def view_columns(name: str) -> list:
    """Columns of the view, from its column list."""
    found = re.search(rf"VIEW cat\.marts\.{name} \((.*?)\n\)\nCOMMENT", view(name), re.S)
    return re.findall(r"^\s+(\w+)\s+COMMENT", found.group(1), re.M)


class MartsStatementTest(unittest.TestCase):
    COLUMNS = {
        "catalog_items": ["media_type", "items", "items_with_genre", "genres", "first_release_year",
                          "last_release_year"],
        "items_per_genre": ["media_type", "genre_name", "items", "primary_items"],
        "items_per_release_year": ["media_type", "release_year", "release_decade", "items"],
        "movies_by_language": ["language", "movies", "rank"],
        "top_titles": ["media_type", "rank", "title", "creator", "primary_genre", "release_year", "popularity",
                       "rating", "votes", "listen_count", "listener_count", "snapshot_date"],
        "daily_listens": ["date", "albums_measured", "listens_gained", "total_listens"],
        "trending_7d": ["media_type", "rank", "title", "creator", "primary_genre", "listens_gained_7d",
                        "popularity_change_7d", "window_end"],
        "new_releases": ["media_type", "release_status", "rank", "title", "creator", "primary_genre",
                         "release_date", "days_from_release", "first_seen_date", "popularity", "listen_count",
                         "snapshot_date"],
        "catalog_changes": ["date", "media_type", "new_items", "changed_items"],
        "wiki_attention": ["media_type", "rank", "title", "creator", "primary_genre", "article", "url", "views_7d",
                           "views_prev_7d", "views_change_pct", "popularity", "listen_count", "window_end"],
        "daily_wiki_views": ["date", "media_type", "items", "views"],
        "wiki_coverage": ["media_type", "items", "items_with_article", "coverage_pct"],
        "tmdb_trending_today": ["rank", "rank_previous_day", "title", "primary_genre", "release_date", "popularity",
                                "days_trending_30d", "first_trending_date", "snapshot_date"],
        "freshness": ["dataset", "last_batch_id", "last_dt", "last_ingested_at", "hours_since_ingest",
                      "last_silver_loaded_at", "silver_lag_s", "status"],
        "latest_quality_checks": ["dataset", "batch_id", "dt", "check_name", "severity", "value", "op", "threshold",
                                  "passed", "description"],
        "quality_history": ["date", "dataset", "batches", "rows_read", "rows_valid", "rows_rejected",
                            "rows_duplicate", "files", "bytes", "silver_lag_s", "failed_critical", "failed_warning"],
        "data_catalog": ["layer", "table_name", "table_type", "description", "owner", "created", "last_altered"],
    }

    def test_one_view_per_file_over_gold_and_ops_only(self):
        files = [f[:-len(".sql")] for f in os.listdir(os.path.join(sql.HERE, "marts")) if f.endswith(".sql")]
        self.assertEqual(sorted(files), sorted(sql.MARTS_VIEWS))
        for name in sql.MARTS_VIEWS:
            with self.subTest(view=name):
                text = view(name)
                self.assertNotIn("$", text)
                self.assertEqual(len(sql.statements(text)), 1)
                self.assertIn(f"CREATE OR REPLACE VIEW cat.marts.{name} (", text)
                self.assertNotRegex(text, r"cat\.(bronze|silver)\.")  # the dashboard reads the modelled layers only
                self.assertNotIn("ARRAY", text.upper().replace("ARRAY_", ""))  # flat columns for Superset

    def test_agreed_columns(self):
        self.assertEqual(set(self.COLUMNS), set(sql.MARTS_VIEWS))
        for name, expected in self.COLUMNS.items():
            with self.subTest(view=name):
                self.assertEqual(view_columns(name), expected)

    def test_rankings_and_trends(self):
        for name in ("movies_by_language", "top_titles", "trending_7d"):  # a top 10 is a filter rank <= 10
            self.assertIn("CAST(row_number() OVER (", view(name))
        top = view("top_titles")
        self.assertIn("max(d.date) OVER (PARTITION BY m.media_type)  AS latest_date", top)
        self.assertIn("WHERE snapshot_date = latest_date", top)  # the latest snapshot of each media type
        # albums by listens, movies and TV shows by TMDB popularity
        self.assertIn("CASE WHEN media_type = 'music' THEN listen_count ELSE popularity END DESC NULLS LAST", top)
        self.assertIn("sum(f.listen_count_change)  AS listens_gained", view("daily_listens"))
        trending = view("trending_7d")
        self.assertIn("WHERE date > date_sub(latest_date, 7)", trending)  # the last 7 snapshot days
        self.assertIn("HAVING max(date) = max(latest_date)", trending)  # still in the latest snapshot
        self.assertIn("max_by(popularity, date) - min_by(popularity, date)", trending)
        self.assertIn("CASE WHEN r.media_type = 'music' THEN r.listens_gained_7d ELSE r.popularity_change_7d END",
                      trending)
        self.assertIn("CASE WHEN r.media_type = 'music' THEN l.listen_count ELSE l.popularity END",
                      view("new_releases"))
        for name in ("top_titles", "trending_7d", "new_releases", "catalog_items", "catalog_changes"):
            self.assertRegex(view(name), r"\n  media_type +COMMENT 'movie \| tv \| music'")

    def test_new_releases_and_catalog_changes(self):
        releases = view("new_releases")
        self.assertIn("to_date(from_utc_timestamp(current_timestamp(), 'Asia/Ho_Chi_Minh')) AS d", releases)
        self.assertIn("WHERE m.release_date BETWEEN date_sub(t.d, 30) AND date_add(t.d, 90)", releases)
        self.assertIn("CASE WHEN m.release_date > t.d THEN 'upcoming' ELSE 'released' END", releases)
        self.assertIn("WHERE c.change_type = 'new'", releases)  # first seen
        self.assertIn("LEFT JOIN latest_metric AS l ON l.media_key = r.media_key AND l.rn = 1", releases)
        changes = view("catalog_changes")
        self.assertIn("count(DISTINCT CASE WHEN c.change_type = 'new' THEN c.media_key END)", changes)
        self.assertIn("count(DISTINCT CASE WHEN c.change_type = 'changed' THEN c.media_key END)", changes)

    def test_wikipedia_views(self):
        attention = view("wiki_attention")
        self.assertIn("CAST(row_number() OVER (PARTITION BY m.media_type ORDER BY w.views_7d DESC, m.title) AS INT)",
                      attention)
        self.assertIn("max(d.date) OVER () AS latest_date", attention)  # the latest day with views
        self.assertIn("sum(CASE WHEN date > date_sub(latest_date, 7) THEN views ELSE 0 END)", attention)
        self.assertIn("sum(CASE WHEN date <= date_sub(latest_date, 7) THEN views ELSE 0 END)", attention)
        self.assertIn("WHERE date > date_sub(latest_date, 14)", attention)  # this week and the one before
        self.assertIn("try_divide(w.views_7d - w.views_prev_7d, w.views_prev_7d)", attention)  # NULL, not an error
        self.assertIn("LEFT JOIN latest_metric AS l ON l.media_key = w.media_key AND l.rn = 1", attention)
        self.assertIn("count(*) AS items, sum(f.views) AS views", view("daily_wiki_views"))
        coverage = view("wiki_coverage")
        self.assertIn("FROM cat.gold.dim_media AS m", coverage)  # every media, with an article or not
        self.assertNotIn("WHERE", coverage)  # every media type is looked up on Wikidata
        self.assertIn("LEFT JOIN cat.gold.dim_wiki_page AS p ON p.media_key = m.media_key", coverage)
        self.assertIn("try_divide(count(p.media_key), count(*))", coverage)

    def test_tmdb_trending_today(self):
        text = view("tmdb_trending_today")
        self.assertIn("max(d.date) OVER () AS latest_date", text)
        self.assertIn("WHERE t.date = t.latest_date", text)  # the latest trending day only
        self.assertIn("LEFT JOIN trending AS p ON p.media_key = t.media_key AND p.date = date_sub(t.latest_date, 1)",
                      text)  # NULL when the movie was not trending the day before
        self.assertIn("count_if(date > date_sub(latest_date, 30))  AS days_trending_30d", text)
        self.assertIn("f.media_key = t.media_key AND f.date_key = t.date_key", text)  # popularity of the same day

    def test_operations_views(self):
        for name in ("freshness", "latest_quality_checks"):  # batch ids start with their time: greatest = latest
            self.assertIn("row_number() OVER (PARTITION BY dataset ORDER BY batch_id DESC) AS rn", view(name))
            self.assertIn("WHERE rn = 1", view(name))
        freshness = view("freshness")
        self.assertIn("WHEN silver_loaded_at IS NULL THEN 'not_loaded'", freshness)
        self.assertIn("timestampdiff(MINUTE, last_ingested_at, current_timestamp()) > 26 * 60 THEN 'stale'", freshness)
        for name in ("freshness", "data_catalog"):  # times in Vietnam time, as dt and batch_id
            self.assertIn("from_utc_timestamp(", view(name))
            self.assertIn("'Asia/Ho_Chi_Minh')", view(name))
        history = view("quality_history")
        self.assertIn("LEFT JOIN checks AS c ON c.dataset = r.dataset AND c.batch_id = r.batch_id", history)
        self.assertIn("count_if(NOT passed AND severity = 'critical')", history)
        self.assertIn("GROUP BY r.dt, r.dataset", history)
        catalog = view("data_catalog")
        self.assertIn("FROM cat.information_schema.tables", catalog)
        self.assertIn("WHERE table_schema IN ('bronze', 'silver', 'gold', 'ops', 'marts')", catalog)


class MartsTest(unittest.TestCase):
    def test_creates_or_replaces_every_view(self):
        spark = FakeSpark()
        with mock.patch.object(config, "UC_CATALOG", "cat"), self.assertLogs("sql", "INFO"):
            views = sql.create_marts_views(spark)
        self.assertEqual(views, [f"cat.marts.{name}" for name in sql.MARTS_VIEWS])
        self.assertEqual([re.search(r"VIEW cat\.marts\.(\w+)", s).group(1) for s in spark.statements],
                         sql.MARTS_VIEWS)


class OneTableTest(unittest.TestCase):
    """One job task per table: setup creates every table, then each task loads one."""
    def patched(self):
        return mock.patch.multiple(config, BRONZE_TABLES_ROOT=ROOT, BRONZE_ROOT=LANDING, SILVER_ROOT=SILVER,
                                   GOLD_ROOT=GOLD, OPS_ROOT=OPS, UC_CATALOG="cat")

    def test_setup_creates_bronze_silver_gold_then_ops(self):
        spark = FakeSpark()
        with self.patched(), self.assertLogs("sql", "INFO"):
            tables = sql.create_tables(spark)
        self.assertEqual(tables, [f"cat.bronze.{t}" for t in sql.BRONZE_TABLES] +
                         [f"cat.silver.{t}" for t in sql.SILVER_TABLES] + [f"cat.gold.{t}" for t in sql.GOLD_TABLES] +
                         [f"cat.ops.{t}" for t in sql.OPS_TABLES])

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

    def test_one_gold_table(self):
        spark = FakeSpark()
        with self.patched(), self.assertLogs("sql", "INFO"):
            self.assertEqual(sql.load_gold_table(spark, "dim_media"), {"rows": 2})
        self.assertEqual(len(spark.statements), 1)
        self.assertIn("INSERT OVERWRITE cat.gold.dim_media", spark.statements[0])

    def test_one_ops_table(self):
        spark = FakeSpark()
        with self.patched(), self.assertLogs("sql", "INFO"):
            self.assertEqual(sql.load_ops_table(spark, "data_quality_results"), {"rows": 2})
        self.assertEqual(len(spark.statements), 1)
        self.assertIn("INSERT OVERWRITE cat.ops.data_quality_results", spark.statements[0])

    def test_unknown_table_fails(self):
        with self.patched():
            for load, name in ((sql.load_bronze_table, "movie"), (sql.load_silver_table, "load_log"),
                               (sql.load_gold_table, "movie"), (sql.load_ops_table, "dim_date")):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    load(FakeSpark(), name)


if __name__ == "__main__":
    unittest.main()
