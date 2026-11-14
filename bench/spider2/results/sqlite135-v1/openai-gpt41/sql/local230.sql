WITH top_genres AS (
  SELECT
    g.genre,
    COUNT(*) AS movies_count
  FROM genre g
  JOIN ratings r ON g.movie_id = r.movie_id
  WHERE r.avg_rating > 8
  GROUP BY g.genre
  ORDER BY movies_count DESC
  LIMIT 3
),
top_directors AS (
  SELECT
    n.name AS director,
    COUNT(*) AS high_rated_movie_count,
    g.genre
  FROM genre g
  JOIN ratings r ON g.movie_id = r.movie_id
  JOIN director_mapping d ON g.movie_id = d.movie_id
  JOIN names n ON d.name_id = n.id
  WHERE r.avg_rating > 8
    AND g.genre IN (SELECT genre FROM top_genres)
  GROUP BY n.name, g.genre
)
SELECT
  director,
  SUM(high_rated_movie_count) AS total_high_rated_movies
FROM top_directors
GROUP BY director
ORDER BY total_high_rated_movies DESC
LIMIT 4;
