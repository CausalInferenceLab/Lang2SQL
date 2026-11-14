WITH season_driver_points AS (
  SELECT
    ds.year,
    ds.driverId,
    ds.constructorId,
    SUM(ds.points) AS total_points
  FROM driver_standings ds
  WHERE ds.year >= 2001
  GROUP BY ds.year, ds.driverId, ds.constructorId
),
min_scorer_per_season AS (
  SELECT
    sdp.year,
    sdp.driverId,
    sdp.constructorId,
    sdp.total_points
  FROM season_driver_points sdp
  JOIN (
    SELECT year, MIN(total_points) AS min_points
    FROM season_driver_points
    WHERE total_points > 0
    GROUP BY year
  ) mp
    ON sdp.year = mp.year AND sdp.total_points = mp.min_points
  WHERE sdp.total_points > 0
),
constructor_least_scorer_seasons AS (
  SELECT 
    constructorId, 
    COUNT(DISTINCT year) AS seasons_with_least_point_scorer
  FROM min_scorer_per_season
  GROUP BY constructorId
)
SELECT
  c.name AS constructor_name,
  cls.seasons_with_least_point_scorer
FROM constructor_least_scorer_seasons cls
JOIN constructors c ON cls.constructorId = c.constructorId
ORDER BY cls.seasons_with_least_point_scorer DESC, c.name
LIMIT 5;
