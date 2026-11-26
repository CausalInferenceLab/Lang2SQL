WITH yearly_counts AS (
    SELECT CAST(SUBSTR(TRIM(year), -4) AS INTEGER) AS release_year,
           COUNT(*) AS film_count
    FROM Movie
    GROUP BY release_year
)
SELECT s.release_year AS start_year,
       SUM(y.film_count) AS total_count
FROM yearly_counts AS s
JOIN yearly_counts AS y
  ON y.release_year >= s.release_year
 AND y.release_year < s.release_year + 10
GROUP BY s.release_year
ORDER BY total_count DESC, start_year ASC
LIMIT 1;
