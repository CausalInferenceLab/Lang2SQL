WITH movie_years AS (
  SELECT
    id AS movie_id,
    CAST(SUBSTR(year, -4) AS INTEGER) AS year
  FROM Movie
  WHERE year IS NOT NULL AND LENGTH(year) >= 4
),
non_female_cast_movies AS (
  SELECT
    mc.movie_id
  FROM M_Cast mc
  JOIN Person p ON mc.person_id = p.id
  WHERE p.gender IN ('Male', 'None')
  GROUP BY mc.movie_id
),
movies_with_exclusively_female_actors AS (
  SELECT
    my.movie_id,
    my.year
  FROM movie_years my
  LEFT JOIN non_female_cast_movies nfc ON my.movie_id = nfc.movie_id
  WHERE nfc.movie_id IS NULL
    AND EXISTS (
      SELECT 1 FROM M_Cast mc
      JOIN Person p ON mc.person_id = p.id
      WHERE mc.movie_id = my.movie_id
        AND p.gender = 'Female'
    )
)
SELECT
  my.year,
  COUNT(DISTINCT my.movie_id) AS total_movies,
  ROUND(COUNT(DISTINCT mwfa.movie_id) * 100.0 / COUNT(DISTINCT my.movie_id), 2) AS exclusively_female_actors_pct
FROM movie_years my
LEFT JOIN movies_with_exclusively_female_actors mwfa ON my.movie_id = mwfa.movie_id
GROUP BY my.year
ORDER BY my.year;
