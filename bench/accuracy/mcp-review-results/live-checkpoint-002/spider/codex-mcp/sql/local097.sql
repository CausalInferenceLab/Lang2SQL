WITH annual_counts AS (
    SELECT CAST(SUBSTR(year, -4) AS INTEGER) AS release_year,
           COUNT(*) AS film_count
    FROM main.Movie
    GROUP BY release_year
)
SELECT release_year AS start_year,
       SUM(film_count) OVER (
           ORDER BY release_year
           RANGE BETWEEN CURRENT ROW AND 9 FOLLOWING
       ) AS total_count
FROM annual_counts
ORDER BY total_count DESC, start_year ASC
LIMIT 1
