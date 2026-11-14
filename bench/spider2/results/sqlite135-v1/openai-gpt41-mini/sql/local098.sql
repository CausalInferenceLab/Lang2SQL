WITH ActorFilms AS (
  SELECT 
    M_Cast.PID AS actor_id,
    CAST(Movie.year AS INTEGER) AS film_year
  FROM M_Cast
  JOIN Movie ON M_Cast.MID = Movie.MID
  GROUP BY M_Cast.PID, film_year
),
YearsRange AS (
  SELECT 
    actor_id,
    MIN(film_year) AS min_year,
    MAX(film_year) AS max_year
  FROM ActorFilms
  GROUP BY actor_id
),
YearSeries AS (
  SELECT actor_id, min_year + offs AS year
  FROM YearsRange
  JOIN (SELECT 0 AS offs UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20) AS seq
  WHERE min_year + offs <= max_year
),
ActorFilmYears AS (
  SELECT actor_id, film_year FROM ActorFilms
),
CheckedGaps AS (
  SELECT 
    y.actor_id, 
    y.year
  FROM YearSeries y
  LEFT JOIN ActorFilmYears afy ON y.actor_id = afy.actor_id AND y.year = afy.film_year
  GROUP BY y.actor_id, y.year
  HAVING SUM(CASE WHEN afy.film_year IS NOT NULL THEN 1 ELSE 0 END) = 0
    AND (
      NOT EXISTS(SELECT 1 FROM ActorFilmYears a1 WHERE a1.actor_id = y.actor_id AND a1.film_year BETWEEN y.year AND y.year + 3)
    )
)
SELECT 
  COUNT(*) AS actors_without_4_year_gap
FROM YearsRange
WHERE actor_id NOT IN (SELECT DISTINCT actor_id FROM CheckedGaps)
