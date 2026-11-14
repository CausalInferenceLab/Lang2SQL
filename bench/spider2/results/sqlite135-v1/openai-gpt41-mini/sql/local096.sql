WITH MovieYear AS (
  SELECT 
    title, 
    CAST(SUBSTR(year, -4) AS INTEGER) AS yr
  FROM Movie
),
MovieActors AS (
  SELECT 
    m.title,
    mc.person_id,
    p.gender
  FROM MovieYear m
  JOIN M_Cast mc ON m.title = mc.movie_title
  JOIN Person p ON mc.person_id = p.id
),
MoviesWithActorsFlags AS (
  SELECT
    title,
    yr,
    MAX(CASE WHEN gender = 'Male' OR gender = 'None' THEN 1 ELSE 0 END) AS has_non_female,
    COUNT(*) AS actor_count
  FROM MovieActors
  GROUP BY title, yr
),
MoviesSummary AS (
  SELECT
    yr,
    COUNT(DISTINCT title) AS total_movies,
    SUM(CASE WHEN has_non_female = 0 THEN 1 ELSE 0 END) AS female_only_movies
  FROM MoviesWithActorsFlags
  GROUP BY yr
)
SELECT
  yr AS year,
  total_movies,
  ROUND(100.0 * female_only_movies / total_movies, 2) AS percentage_female_only
FROM MoviesSummary
ORDER BY year;
