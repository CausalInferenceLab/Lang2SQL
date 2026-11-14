WITH driver_year_points AS (
  SELECT
    ds.year,
    ds.driverId,
    SUM(ds.points) AS total_points
  FROM driver_standings ds
  GROUP BY ds.year, ds.driverId
),
top_driver_per_year AS (
  SELECT
    dyp.year,
    dyp.driverId,
    dyp.total_points,
    RANK() OVER (PARTITION BY dyp.year ORDER BY dyp.total_points DESC) AS rnk
  FROM driver_year_points dyp
),
constructor_year_points AS (
  SELECT
    cs.year,
    cs.constructorId,
    SUM(cs.points) AS total_points
  FROM constructor_standings cs
  GROUP BY cs.year, cs.constructorId
),
top_constructor_per_year AS (
  SELECT
    cyp.year,
    cyp.constructorId,
    cyp.total_points,
    RANK() OVER (PARTITION BY cyp.year ORDER BY cyp.total_points DESC) AS rnk
  FROM constructor_year_points cyp
)
SELECT
  tdp.year,
  d.forename || ' ' || d.surname AS driver_full_name,
  tdp.total_points AS driver_points,
  c.name AS constructor_name,
  tcp.total_points AS constructor_points
FROM top_driver_per_year tdp
JOIN drivers d ON tdp.driverId = d.driverId
JOIN top_constructor_per_year tcp ON tdp.year = tcp.year AND tcp.rnk = 1
JOIN constructors c ON tcp.constructorId = c.constructorId
WHERE tdp.rnk = 1
ORDER BY tdp.year;
