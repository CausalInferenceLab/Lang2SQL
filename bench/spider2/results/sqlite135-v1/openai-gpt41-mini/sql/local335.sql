WITH season_driver_points AS (
    SELECT 
        ra.year AS season, 
        r.driverId, 
        r.constructorId,
        SUM(r.points) AS total_points
    FROM results r
    JOIN races ra ON r.raceId = ra.raceId
    WHERE ra.year >= 2001
    GROUP BY ra.year, r.driverId, r.constructorId
), season_min_driver_points AS (
    SELECT season, MIN(total_points) AS min_points
    FROM season_driver_points
    WHERE total_points > 0
    GROUP BY season
), min_point_drivers AS (
    SELECT sdp.season, sdp.driverId, sdp.constructorId
    FROM season_driver_points sdp
    JOIN season_min_driver_points smdp ON sdp.season = smdp.season AND sdp.total_points = smdp.min_points
    WHERE sdp.total_points > 0
), constructor_min_point_seasons AS (
    SELECT constructorId, COUNT(DISTINCT season) AS seasons_with_min_driver_points
    FROM min_point_drivers
    GROUP BY constructorId
)
SELECT c.name AS constructor_name, cmps.seasons_with_min_driver_points
FROM constructor_min_point_seasons cmps
JOIN constructors c ON cmps.constructorId = c.constructorId
ORDER BY cmps.seasons_with_min_driver_points DESC
LIMIT 5;
