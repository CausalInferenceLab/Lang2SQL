WITH driver_year_points AS (
  SELECT race_id, driver_id, SUM(points) AS total_points
  FROM results
  GROUP BY race_id, driver_id
),
constructor_year_points AS (
  SELECT race_id, constructor_id, SUM(points) AS total_points
  FROM results
  GROUP BY race_id, constructor_id
),
race_years AS (
  SELECT race_id, year
  FROM races
),
driver_year_points_with_year AS (
  SELECT r.year, d.driver_id, d.total_points
  FROM driver_year_points d
  JOIN race_years r ON d.race_id = r.race_id
),
constructor_year_points_with_year AS (
  SELECT r.year, c.constructor_id, c.total_points
  FROM constructor_year_points c
  JOIN race_years r ON c.race_id = r.race_id
),
max_driver_points AS (
  SELECT year, MAX(total_points) AS max_points
  FROM driver_year_points_with_year
  GROUP BY year
),
max_constructor_points AS (
  SELECT year, MAX(total_points) AS max_points
  FROM constructor_year_points_with_year
  GROUP BY year
)
SELECT d.year, (d.max_points + c.max_points) AS combined_points
FROM max_driver_points d
JOIN max_constructor_points c ON d.year = c.year
ORDER BY combined_points ASC
LIMIT 3;
