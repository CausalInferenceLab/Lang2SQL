WITH director_movies AS (
  SELECT
    d.name_id AS director_id,
    m.duration,
    r.avg_rating,
    r.total_votes,
    julianday(m.date_published) - LAG(julianday(m.date_published)) OVER (
      PARTITION BY d.name_id ORDER BY m.date_published, m.id
    ) AS gap_days
  FROM main.director_mapping AS d
  JOIN main.movies AS m ON m.id = d.movie_id
  LEFT JOIN main.ratings AS r ON r.movie_id = m.id
),
director_stats AS (
  SELECT
    director_id,
    COUNT(*) AS movie_count,
    CAST(ROUND(AVG(gap_days)) AS INTEGER) AS avg_inter_movie_days,
    SUM(CAST(ROUND(avg_rating * 10) AS INTEGER)) * 10 AS rating_sum_hundredths,
    COUNT(avg_rating) AS rated_movies,
    SUM(total_votes) AS total_votes,
    MIN(avg_rating) AS min_rating,
    MAX(avg_rating) AS max_rating,
    SUM(duration) AS total_movie_duration
  FROM director_movies
  GROUP BY director_id
)
SELECT
  s.director_id,
  n.name AS director_name,
  s.movie_count,
  s.avg_inter_movie_days,
  ((s.rating_sum_hundredths + s.rated_movies / 2) / s.rated_movies) / 100.0 AS avg_rating,
  s.total_votes,
  s.min_rating,
  s.max_rating,
  s.total_movie_duration
FROM director_stats AS s
JOIN main.names AS n ON n.id = s.director_id
ORDER BY s.movie_count DESC, s.total_movie_duration DESC, s.director_id
LIMIT 9
