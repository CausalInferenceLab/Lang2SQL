WITH director_movies AS (
    SELECT
        dm.director_id,
        n.primary_name AS director_name,
        m.movie_id,
        m.start_year,
        m.runtime_minutes
    FROM director_mapping dm
    JOIN names n ON dm.director_id = n.name_id
    JOIN movies m ON dm.movie_id = m.movie_id
),
movie_stats AS (
    SELECT
        d.director_id,
        d.director_name,
        COUNT(*) AS movie_count,
        SUM(d.runtime_minutes) AS total_movie_duration,
        MIN(d.start_year) AS first_year,
        MAX(d.start_year) AS last_year
    FROM director_movies d
    GROUP BY d.director_id, d.director_name
),
inter_movie_stats AS (
    SELECT
        d.director_id,
        CASE
            WHEN COUNT(*) > 1 THEN
                ROUND(1.0 * (MAX(d.start_year) - MIN(d.start_year)) / (COUNT(*) - 1))
            ELSE
                NULL
        END AS avg_inter_movie_duration
    FROM director_movies d
    GROUP BY d.director_id
),
rating_stats AS (
    SELECT
        dm.director_id,
        ROUND(AVG(r.average_rating), 2) AS avg_rating,
        SUM(r.num_votes) AS total_votes,
        MIN(r.average_rating) AS min_rating,
        MAX(r.average_rating) AS max_rating
    FROM director_mapping dm
    JOIN ratings r ON dm.movie_id = r.movie_id
    GROUP BY dm.director_id
)
SELECT
    ms.director_id,
    ms.director_name,
    ms.movie_count,
    ims.avg_inter_movie_duration,
    rs.avg_rating,
    rs.total_votes,
    rs.min_rating,
    rs.max_rating,
    ms.total_movie_duration
FROM movie_stats ms
LEFT JOIN inter_movie_stats ims ON ms.director_id = ims.director_id
LEFT JOIN rating_stats rs ON ms.director_id = rs.director_id
ORDER BY ms.movie_count DESC, ms.total_movie_duration DESC
LIMIT 9;
