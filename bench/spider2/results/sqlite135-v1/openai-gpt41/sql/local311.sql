WITH best_driver_points AS (
  SELECT
    r.constructor_id,
    r.year,
    MAX(ds.points) AS best_driver_points
  FROM results r
  JOIN (
    SELECT race_id, driver_id, points
    FROM driver_standings
  ) ds ON r.race_id = ds.race_id AND r.driver_id = ds.driver_id
  JOIN races rc ON r.race_id = rc.race_id
  GROUP BY r.constructor_id, rc.year
),
team_points AS (
  SELECT
    constructor_id,
    rc.year,
    SUM(points) AS team_points
  FROM constructor_standings cs
  JOIN races rc ON cs.race_id = rc.race_id
  GROUP BY constructor_id, rc.year
),
combined_points AS (
  SELECT
    c.constructor_id,
    c.year,
    bdp.best_driver_points,
    tp.team_points,
    bdp.best_driver_points + tp.team_points AS combined_points
  FROM best_driver_points bdp
  JOIN team_points tp ON bdp.constructor_id = tp.constructor_id AND bdp.year = tp.year
  JOIN constructors c ON bdp.constructor_id = c.constructor_id
)
SELECT
  cn.name AS constructor_name,
  c.year,
  c.combined_points
FROM combined_points c
JOIN constructors cn ON c.constructor_id = cn.constructor_id
ORDER BY c.combined_points DESC
LIMIT 3;
