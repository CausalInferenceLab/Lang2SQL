WITH highly_rated_movie_genres AS (
    SELECT g.movie_id, g.genre
    FROM genre AS g
    JOIN ratings AS r ON r.movie_id = g.movie_id
    WHERE r.avg_rating > 8
), top_genres AS (
    SELECT genre
    FROM highly_rated_movie_genres
    GROUP BY genre
    ORDER BY COUNT(DISTINCT movie_id) DESC, genre ASC
    LIMIT 3
)
SELECT n.name AS director_name,
       COUNT(DISTINCT dm.movie_id) AS movie_count
FROM highly_rated_movie_genres AS hg
JOIN top_genres AS tg ON tg.genre = hg.genre
JOIN director_mapping AS dm ON dm.movie_id = hg.movie_id
JOIN names AS n ON n.id = dm.name_id
GROUP BY n.id, n.name
ORDER BY movie_count DESC, director_name ASC, n.id ASC
LIMIT 4;
