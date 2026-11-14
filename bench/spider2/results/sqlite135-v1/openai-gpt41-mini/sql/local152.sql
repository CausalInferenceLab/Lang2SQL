WITH director_movies AS (
  SELECT
    dm.director_id,
    COUNT(m.movie_id) AS movie_count,
    AVG(m.rating) AS avg_rating,
    SUM(m.votes) AS total_votes,
    MIN(m.rating) AS min_rating,
    MAX(m.rating) AS max_rating,
    SUM(m.duration) AS total_duration,
    GROUP_CONCAT(m.year, ',') AS years
  FROM director_mapping dm
  JOIN movies m ON dm.movie_id = m.movie_id
  GROUP BY dm.director_id
),
director_stats AS (
  SELECT
    dm.director_id,
    movie_count,
    ROUND(AVG(diff)) AS avg_inter_movie_duration,
    ROUND(avg_rating, 2) AS avg_rating,
    total_votes,
    min_rating,
    max_rating,
    total_duration
  FROM (
    SELECT
      director_id,
      movie_count,
      avg_rating,
      total_votes,
      min_rating,
      max_rating,
      total_duration,
      years,
      -- Calculate year differences between consecutive movies for each director
      (julianday(years_split[i+1] || '-01-01') - julianday(years_split[i] || '-01-01')) AS diff
    FROM director_movies,
    LATERAL (
      SELECT
        CAST(value AS INTEGER) AS year,
        ROW_NUMBER() OVER () as rn
      FROM (
        SELECT trim(value) AS value
        FROM STRING_SPLIT(years, ',')
      )
    ) years_split
  )
  GROUP BY director_id, movie_count, avg_rating, total_votes, min_rating, max_rating, total_duration
)
SELECT 
  ds.director_id,
  n.name,
  ds.movie_count,
  COALESCE(ds.avg_inter_movie_duration, 0) AS avg_inter_movie_duration,
  ds.avg_rating,
  ds.total_votes,
  ds.min_rating,
  ds.max_rating,
  ds.total_duration
FROM director_stats ds
JOIN names n ON ds.director_id = n.person_id
ORDER BY ds.movie_count DESC, ds.total_duration DESC
LIMIT 9;
