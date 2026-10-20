WITH raw AS (
    -- Explode the comma-separated genre string into individual trimmed tags
    SELECT
        movieid,
        series_title,
        imdb_rating,
        gross,
        no_of_votes,
        meta_score,
        TRIM(genre_tag) AS genre_tag
    FROM {catalog_db_table}
    LATERAL VIEW EXPLODE(SPLIT(genre, ',')) AS genre_tag
),

unique_genres AS (
    -- All 21 distinct genre tags (for documentation/reference)
    SELECT DISTINCT genre_tag
    FROM raw
    ORDER BY genre_tag
),

flagged AS (
    -- One row per (movie, genre_tag) with a 1 flag
    SELECT
        movieid,
        series_title,
        imdb_rating,
        gross,
        no_of_votes,
        meta_score,
        genre_tag,
        1 AS flag
    FROM raw
),

pivoted AS (
    -- Pivot genre tags into columns; NULL means the movie doesn't belong to that genre
    SELECT *
    FROM flagged
    PIVOT (
        MAX(flag)
        FOR genre_tag IN (
            'Action'    AS Action,
            'Adventure' AS Adventure,
            'Animation' AS Animation,
            'Biography' AS Biography,
            'Comedy'    AS Comedy,
            'Crime'     AS Crime,
            'Drama'     AS Drama,
            'Family'    AS Family,
            'Fantasy'   AS Fantasy,
            'Film-Noir' AS Film_Noir,
            'History'   AS History,
            'Horror'    AS Horror,
            'Music'     AS Music,
            'Musical'   AS Musical,
            'Mystery'   AS Mystery,
            'Romance'   AS Romance,
            'Sci-Fi'    AS Sci_Fi,
            'Sport'     AS Sport,
            'Thriller'  AS Thriller,
            'War'       AS War,
            'Western'   AS Western
        )
    )
)

SELECT
    movieid,
    series_title,
    imdb_rating,
    gross,
    no_of_votes,
    meta_score,
    COALESCE(Action,    0) AS Action,
    COALESCE(Adventure, 0) AS Adventure,
    COALESCE(Animation, 0) AS Animation,
    COALESCE(Biography, 0) AS Biography,
    COALESCE(Comedy,    0) AS Comedy,
    COALESCE(Crime,     0) AS Crime,
    COALESCE(Drama,     0) AS Drama,
    COALESCE(Family,    0) AS Family,
    COALESCE(Fantasy,   0) AS Fantasy,
    COALESCE(Film_Noir, 0) AS Film_Noir,
    COALESCE(History,   0) AS History,
    COALESCE(Horror,    0) AS Horror,
    COALESCE(Music,     0) AS Music,
    COALESCE(Musical,   0) AS Musical,
    COALESCE(Mystery,   0) AS Mystery,
    COALESCE(Romance,   0) AS Romance,
    COALESCE(Sci_Fi,    0) AS Sci_Fi,
    COALESCE(Sport,     0) AS Sport,
    COALESCE(Thriller,  0) AS Thriller,
    COALESCE(War,       0) AS War,
    COALESCE(Western,   0) AS Western
FROM pivoted