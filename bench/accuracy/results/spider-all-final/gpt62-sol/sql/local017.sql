WITH cause_counts AS (
    SELECT CAST(strftime('%Y', collision_date) AS INTEGER) AS year,
           pcf_violation_category AS cause,
           COUNT(*) AS accident_count
    FROM collisions
    WHERE strftime('%Y', collision_date) IS NOT NULL
      AND pcf_violation_category IS NOT NULL
    GROUP BY year, cause
), ranked_causes AS (
    SELECT year, cause,
           ROW_NUMBER() OVER (
               PARTITION BY year ORDER BY accident_count DESC, cause
           ) AS cause_rank
    FROM cause_counts
), yearly_pairs AS (
    SELECT year, MIN(cause) AS cause_a, MAX(cause) AS cause_b
    FROM ranked_causes
    WHERE cause_rank <= 2
    GROUP BY year
    HAVING COUNT(*) = 2
), pair_frequency AS (
    SELECT year,
           COUNT(*) OVER (PARTITION BY cause_a, cause_b) AS years_with_pair
    FROM yearly_pairs
)
SELECT year
FROM pair_frequency
WHERE years_with_pair = 1;
