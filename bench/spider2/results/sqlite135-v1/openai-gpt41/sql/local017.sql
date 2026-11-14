WITH yearly_top_causes AS (
  SELECT
    strftime('%Y', collision_date) AS year,
    primary_collision_factor,
    COUNT(*) AS cnt,
    ROW_NUMBER() OVER (PARTITION BY strftime('%Y', collision_date) ORDER BY COUNT(*) DESC) AS rn
  FROM collisions
  GROUP BY year, primary_collision_factor
),
year_and_top_two AS (
  SELECT
    year,
    primary_collision_factor,
    rn
  FROM yearly_top_causes
  WHERE rn <= 2
),
cause_sets AS (
  SELECT
    year,
    GROUP_CONCAT(primary_collision_factor, ',') AS top2_causes
  FROM year_and_top_two
  GROUP BY year
)
SELECT year
FROM cause_sets
WHERE top2_causes NOT IN (
  SELECT top2_causes
  FROM cause_sets
  GROUP BY top2_causes
  HAVING COUNT(*) > 1
);
