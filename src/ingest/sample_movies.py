"""Offline demo movies, used when MOVIE_SOURCE=sample (e.g. TMDB is unreachable from your network).

Same bronze layout and field names as the TMDB ingest, so the Spark job needs no special path.
Only well-known public facts are included: title, original language, genres, release YEAR.
  - ratings, votes and popularity are left empty on purpose (no real numbers available offline);
  - release_date uses YYYY-01-01, so only the year is meaningful;
  - ids are "sample:<n>" and never look like real TMDB ids.
This is demo data: say so in any report, and switch back to MOVIE_SOURCE=tmdb for real data.
"""
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# (title, year, original_language, genres)
MOVIES = [
    ("The Shawshank Redemption", 1994, "en", "Drama,Crime"),
    ("The Godfather", 1972, "en", "Drama,Crime"),
    ("The Godfather Part II", 1974, "en", "Drama,Crime"),
    ("The Dark Knight", 2008, "en", "Drama,Action,Crime,Thriller"),
    ("Pulp Fiction", 1994, "en", "Thriller,Crime"),
    ("Schindler's List", 1993, "en", "Drama,History,War"),
    ("Forrest Gump", 1994, "en", "Comedy,Drama,Romance"),
    ("Fight Club", 1999, "en", "Drama,Thriller"),
    ("Inception", 2010, "en", "Action,Science Fiction,Adventure"),
    ("The Matrix", 1999, "en", "Action,Science Fiction"),
    ("The Matrix Reloaded", 2003, "en", "Adventure,Action,Science Fiction,Thriller"),
    ("Interstellar", 2014, "en", "Adventure,Drama,Science Fiction"),
    ("The Lord of the Rings: The Fellowship of the Ring", 2001, "en", "Adventure,Fantasy,Action"),
    ("The Lord of the Rings: The Two Towers", 2002, "en", "Adventure,Fantasy,Action"),
    ("The Lord of the Rings: The Return of the King", 2003, "en", "Adventure,Fantasy,Action"),
    ("Star Wars", 1977, "en", "Adventure,Action,Science Fiction"),
    ("The Empire Strikes Back", 1980, "en", "Adventure,Action,Science Fiction"),
    ("Return of the Jedi", 1983, "en", "Adventure,Action,Science Fiction"),
    ("Jurassic Park", 1993, "en", "Adventure,Science Fiction"),
    ("Titanic", 1997, "en", "Drama,Romance"),
    ("Gladiator", 2000, "en", "Action,Drama,Adventure"),
    ("Spirited Away", 2001, "ja", "Animation,Family,Fantasy"),
    ("My Neighbor Totoro", 1988, "ja", "Animation,Family,Fantasy"),
    ("Princess Mononoke", 1997, "ja", "Adventure,Fantasy,Animation"),
    ("Your Name.", 2016, "ja", "Romance,Animation,Drama"),
    ("Toy Story", 1995, "en", "Animation,Adventure,Family,Comedy"),
    ("Toy Story 2", 1999, "en", "Animation,Adventure,Family,Comedy"),
    ("Finding Nemo", 2003, "en", "Animation,Family"),
    ("The Lion King", 1994, "en", "Animation,Family,Drama"),
    ("Up", 2009, "en", "Animation,Comedy,Family,Adventure"),
    ("Ratatouille", 2007, "en", "Animation,Comedy,Family,Fantasy"),
    ("Coco", 2017, "en", "Family,Animation,Fantasy,Music,Comedy"),
    ("Shrek", 2001, "en", "Animation,Comedy,Fantasy,Family"),
    ("Spider-Man: Into the Spider-Verse", 2018, "en", "Action,Adventure,Animation,Science Fiction"),
    ("Parasite", 2019, "ko", "Comedy,Thriller,Drama"),
    ("Oldboy", 2003, "ko", "Drama,Thriller,Mystery,Action"),
    ("Amelie", 2001, "fr", "Comedy,Romance"),
    ("Se7en", 1995, "en", "Crime,Mystery,Thriller"),
    ("The Silence of the Lambs", 1991, "en", "Crime,Drama,Thriller,Horror"),
    ("Alien", 1979, "en", "Horror,Action,Science Fiction"),
    ("Aliens", 1986, "en", "Action,Thriller,Science Fiction"),
    ("Blade Runner", 1982, "en", "Science Fiction,Drama,Thriller"),
    ("Terminator 2: Judgment Day", 1991, "en", "Action,Thriller,Science Fiction"),
    ("Back to the Future", 1985, "en", "Adventure,Comedy,Science Fiction"),
    ("Back to the Future Part II", 1989, "en", "Adventure,Comedy,Science Fiction"),
    ("The Shining", 1980, "en", "Horror,Thriller"),
    ("Psycho", 1960, "en", "Horror,Mystery,Thriller"),
    ("Casablanca", 1942, "en", "Drama,Romance,War"),
    ("Citizen Kane", 1941, "en", "Drama,Mystery"),
    ("Saving Private Ryan", 1998, "en", "Drama,History,War,Action"),
    ("Braveheart", 1995, "en", "Action,Drama,History,War"),
    ("The Departed", 2006, "en", "Drama,Thriller,Crime"),
    ("Goodfellas", 1990, "en", "Drama,Crime"),
    ("Whiplash", 2014, "en", "Drama,Music"),
    ("La La Land", 2016, "en", "Comedy,Drama,Romance,Music"),
    ("Mad Max: Fury Road", 2015, "en", "Action,Adventure,Science Fiction"),
    ("The Avengers", 2012, "en", "Science Fiction,Action,Adventure"),
    ("Avengers: Endgame", 2019, "en", "Adventure,Science Fiction,Action"),
    ("Iron Man", 2008, "en", "Action,Science Fiction,Adventure"),
    ("Harry Potter and the Philosopher's Stone", 2001, "en", "Adventure,Fantasy"),
    ("Harry Potter and the Chamber of Secrets", 2002, "en", "Adventure,Fantasy"),
    ("The Grand Budapest Hotel", 2014, "en", "Comedy,Drama"),
    ("Pan's Labyrinth", 2006, "es", "Fantasy,Drama,War"),
    ("Cinema Paradiso", 1988, "it", "Drama,Romance"),
    ("Life Is Beautiful", 1997, "it", "Comedy,Drama,Romance"),
    ("Seven Samurai", 1954, "ja", "Action,Drama"),
    ("Home Alone", 1990, "en", "Comedy,Family"),
    ("The Hangover", 2009, "en", "Comedy"),
    ("Superbad", 2007, "en", "Comedy"),
    ("The Notebook", 2004, "en", "Romance,Drama"),
    ("Pride & Prejudice", 2005, "en", "Drama,Romance"),
    ("Get Out", 2017, "en", "Mystery,Thriller,Horror"),
    ("A Quiet Place", 2018, "en", "Horror,Drama,Science Fiction"),
    ("The Conjuring", 2013, "en", "Horror,Thriller"),
]


def records() -> list:
    now = datetime.now(timezone.utc).isoformat()
    return [{"id": f"sample:{i}", "title": title, "release_date": f"{year}-01-01",
             "original_language": lang, "genres": genres.split(","),
             "popularity": None, "vote_average": None, "vote_count": None,
             "_endpoint": "sample", "_ingested_at": now}
            for i, (title, year, lang, genres) in enumerate(MOVIES, start=1)]


def run(ds: str, batch_id: str) -> int:
    from common import storage  # imported here so records() stays importable on its own
    from ingest.batch import bronze_key
    key = bronze_key("tmdb", "movies", ds, batch_id, "movies.jsonl")
    n = storage.put_jsonl(key, records())
    log.warning("MOVIE_SOURCE=sample: wrote %s DEMO movies to %s (no ratings/popularity)", n,
                storage.volume_path(key))
    return n
