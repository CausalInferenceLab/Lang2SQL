WITH category_counts AS (
    SELECT pcf_violation_category,
           COUNT(*) FILTER (WHERE collision_date >= '2011-01-01' AND collision_date < '2012-01-01') AS incidents_2011,
           COUNT(*) FILTER (WHERE collision_date >= '2021-01-01' AND collision_date < '2022-01-01') AS incidents_2021
    FROM collisions
    WHERE (collision_date >= '2011-01-01' AND collision_date < '2012-01-01')
       OR (collision_date >= '2021-01-01' AND collision_date < '2022-01-01')
    GROUP BY pcf_violation_category
), annual_totals AS (
    SELECT *,
           SUM(incidents_2011) OVER () AS total_2011,
           SUM(incidents_2021) OVER () AS total_2021
    FROM category_counts
)
SELECT 100.0 * incidents_2011 / NULLIF(total_2011, 0)
       - 100.0 * incidents_2021 / NULLIF(total_2021, 0) AS percentage_point_decrease
FROM annual_totals
WHERE pcf_violation_category IS NOT NULL
  AND incidents_2021 > 0
ORDER BY incidents_2021 DESC, pcf_violation_category
LIMIT 1;
