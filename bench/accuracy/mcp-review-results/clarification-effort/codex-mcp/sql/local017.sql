WITH cause_counts AS (
    SELECT strftime('%Y', collision_date) AS year,
           pcf_violation_category AS cause,
           COUNT(*) AS collisions
    FROM main.collisions
    WHERE pcf_violation_category IS NOT NULL
      AND collision_date IS NOT NULL
    GROUP BY year, cause
),
ranked_causes AS (
    SELECT year, cause,
           RANK() OVER (PARTITION BY year ORDER BY collisions DESC) AS cause_rank
    FROM cause_counts
),
yearly_pairs AS (
    SELECT year, MIN(cause) AS cause_a, MAX(cause) AS cause_b
    FROM ranked_causes
    WHERE cause_rank <= 2
    GROUP BY year
    HAVING COUNT(*) = 2
)
SELECT MIN(year) AS year
FROM yearly_pairs
GROUP BY cause_a, cause_b
HAVING COUNT(*) = 1
ORDER BY year
LIMIT 1000
