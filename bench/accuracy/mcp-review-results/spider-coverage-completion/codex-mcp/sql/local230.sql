WITH rated_genres AS (
    SELECT DISTINCT g.movie_id, g.genre
    FROM main.genre AS g
    JOIN main.ratings AS r ON r.movie_id = g.movie_id
    WHERE r.avg_rating > 8
),
top_genres AS (
    SELECT genre
    FROM rated_genres
    GROUP BY genre
    ORDER BY COUNT(*) DESC, genre
    LIMIT 3
)
SELECT n.name AS director_name,
       COUNT(DISTINCT d.movie_id) AS movie_count
FROM main.director_mapping AS d
JOIN main.names AS n ON n.id = d.name_id
JOIN rated_genres AS g ON g.movie_id = d.movie_id
JOIN top_genres AS t ON t.genre = g.genre
GROUP BY n.id, n.name
ORDER BY movie_count DESC, n.name, n.id
LIMIT 4;
