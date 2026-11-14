WITH driver_points_year AS (
    SELECT 
        d.driverId, 
        d.forename || ' ' || d.surname AS driver_name, 
        r.year, 
        SUM(rs.points) AS total_points
    FROM results rs
    JOIN races r ON rs.raceId = r.raceId
    JOIN drivers d ON rs.driverId = d.driverId
    GROUP BY d.driverId, r.year
),
max_driver_points_year AS (
    SELECT 
        year, 
        MAX(total_points) AS max_points
    FROM driver_points_year
    GROUP BY year
),
top_drivers AS (
    SELECT dp.year, dp.driver_name, dp.total_points
    FROM driver_points_year dp
    JOIN max_driver_points_year md ON dp.year = md.year AND dp.total_points = md.max_points
),
constructor_points_year AS (
    SELECT 
        c.constructorId, 
        c.name AS constructor_name, 
        r.year, 
        SUM(rs.points) AS total_points
    FROM results rs
    JOIN races r ON rs.raceId = r.raceId
    JOIN constructors c ON rs.constructorId = c.constructorId
    GROUP BY c.constructorId, r.year
),
max_constructor_points_year AS (
    SELECT 
        year, 
        MAX(total_points) AS max_points
    FROM constructor_points_year
    GROUP BY year
),
top_constructors AS (
    SELECT cp.year, cp.constructor_name, cp.total_points
    FROM constructor_points_year cp
    JOIN max_constructor_points_year mc ON cp.year = mc.year AND cp.total_points = mc.max_points
)

SELECT 
    td.year, 
    td.driver_name, 
    td.total_points AS driver_points, 
    tc.constructor_name, 
    tc.total_points AS constructor_points
FROM top_drivers td
JOIN top_constructors tc ON td.year = tc.year
ORDER BY td.year;
