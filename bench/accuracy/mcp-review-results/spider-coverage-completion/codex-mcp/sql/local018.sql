WITH category_counts AS (
    SELECT pcf_violation_category,
           COUNT(*) FILTER (WHERE collision_date < '2012-01-01') AS incidents_2011,
           COUNT(*) FILTER (WHERE collision_date >= '2021-01-01') AS incidents_2021
    FROM main.collisions
    WHERE (collision_date >= '2011-01-01' AND collision_date < '2012-01-01')
       OR (collision_date >= '2021-01-01' AND collision_date < '2022-01-01')
    GROUP BY pcf_violation_category
),
annual_shares AS (
    SELECT pcf_violation_category,
           incidents_2021,
           100.0 * incidents_2011 / NULLIF(SUM(incidents_2011) OVER (), 0) AS share_2011,
           100.0 * incidents_2021 / NULLIF(SUM(incidents_2021) OVER (), 0) AS share_2021
    FROM category_counts
)
SELECT share_2011 - share_2021 AS percentage_point_decrease
FROM annual_shares
WHERE pcf_violation_category IS NOT NULL
ORDER BY incidents_2021 DESC
LIMIT 1
