WITH collision_factors_2011 AS (
  SELECT primary_collision_factor, COUNT(*) AS count_2011
  FROM collisions
  WHERE strftime('%Y', collision_date) = '2011'
  GROUP BY primary_collision_factor
),
collision_factors_2021 AS (
  SELECT primary_collision_factor, COUNT(*) AS count_2021
  FROM collisions
  WHERE strftime('%Y', collision_date) = '2021'
  GROUP BY primary_collision_factor
),
total_2011 AS (
  SELECT COUNT(*) AS total_2011
  FROM collisions
  WHERE strftime('%Y', collision_date) = '2011'
),
total_2021 AS (
  SELECT COUNT(*) AS total_2021
  FROM collisions
  WHERE strftime('%Y', collision_date) = '2021'
),
most_common_factor_2021 AS (
  SELECT primary_collision_factor
  FROM collision_factors_2021
  ORDER BY count_2021 DESC
  LIMIT 1
)

SELECT 
  (100.0 * cf2021.count_2021 / t2021.total_2021) - (100.0 * cf2011.count_2011 / t2011.total_2011) AS percentage_point_diff
FROM most_common_factor_2021 mcf
JOIN collision_factors_2021 cf2021 ON mcf.primary_collision_factor = cf2021.primary_collision_factor
JOIN collision_factors_2011 cf2011 ON mcf.primary_collision_factor = cf2011.primary_collision_factor
CROSS JOIN total_2011 t2011
CROSS JOIN total_2021 t2021
;
