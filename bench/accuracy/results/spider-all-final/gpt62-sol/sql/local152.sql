WITH director_movies AS (
    SELECT
        dm.name_id AS director_id,
        m.id AS movie_id,
        julianday(m.date_published) AS release_day,
        m.duration,
        r.avg_rating,
        r.total_votes
    FROM director_mapping AS dm
    JOIN movies AS m ON m.id = dm.movie_id
    LEFT JOIN ratings AS r ON r.movie_id = m.id
), release_gaps AS (
    SELECT
        *,
        release_day - LAG(release_day) OVER (
            PARTITION BY director_id
            ORDER BY release_day, movie_id
        ) AS days_since_previous_movie
    FROM director_movies
)
SELECT
    g.director_id,
    n.name AS director_name,
    COUNT(*) AS number_of_movies,
    CAST(ROUND(AVG(g.days_since_previous_movie)) AS INTEGER) AS avg_inter_movie_days,
    ROUND(AVG(g.avg_rating), 2) AS avg_rating,
    SUM(g.total_votes) AS total_votes,
    MIN(g.avg_rating) AS min_rating,
    MAX(g.avg_rating) AS max_rating,
    SUM(g.duration) AS total_movie_duration
FROM release_gaps AS g
LEFT JOIN names AS n ON n.id = g.director_id
GROUP BY g.director_id, n.name
ORDER BY number_of_movies DESC, total_movie_duration DESC, g.director_id
LIMIT 9;
