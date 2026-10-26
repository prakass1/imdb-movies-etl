WITH raw AS (
    -- Explode the comma-separated genre string into individual trimmed genre tags
    SELECT
        movieid,
        series_title,
        imdb_rating,
        votes_count AS no_of_votes,
        gross,
        TRIM(genre_tag) AS genre_tag
    FROM {catalog_db_table}
    LATERAL VIEW EXPLODE(SPLIT(genre, ',')) AS genre_tag
),

genre_bounds AS (
    -- Min/max per genre for min-max normalisation of each signal
    SELECT
        genre_tag,
        MIN(imdb_rating)  AS min_rating,  MAX(imdb_rating)  AS max_rating,
        MIN(no_of_votes)  AS min_votes,   MAX(no_of_votes)  AS max_votes,
        MIN(gross)        AS min_gross,   MAX(gross)        AS max_gross
    FROM raw
    GROUP BY genre_tag
),

scored AS (
    -- Weighted score: imdb_rating 40 %, no_of_votes 40 %, gross 20 %
    -- Each signal is min-max normalised within its genre so they are comparable.
    -- NULLIF guards against divisions by zero when all values in a genre are equal.
    SELECT
        r.movieid,
        r.series_title,
        r.genre_tag,
        r.imdb_rating,
        r.no_of_votes,
        r.gross,
        ROUND(
            0.4 * (r.imdb_rating - b.min_rating)  / NULLIF(b.max_rating - b.min_rating, 0)
          + 0.4 * (r.no_of_votes - b.min_votes)   / NULLIF(b.max_votes  - b.min_votes,  0)
          + 0.2 * (r.gross       - b.min_gross)    / NULLIF(b.max_gross  - b.min_gross,  0),
        4) AS score
    FROM raw r
    JOIN genre_bounds b ON r.genre_tag = b.genre_tag
),

ranked AS (
    SELECT
        movieid,
        series_title,
        genre_tag,
        imdb_rating,
        no_of_votes,
        gross,
        score,
        DENSE_RANK() OVER (PARTITION BY genre_tag ORDER BY score DESC) AS genre_rank
    FROM scored
)

SELECT
    genre_rank,
    genre_tag,
    movieid,
    series_title,
    imdb_rating,
    no_of_votes,
    gross,
    score
FROM ranked
ORDER BY
    genre_tag,
    genre_rank
