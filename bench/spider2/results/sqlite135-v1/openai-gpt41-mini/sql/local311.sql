WITH driver_best_points AS (
    SELECT 
        driver_standings.constructor_id,
        driver_standings.year,
        MAX(driver_standings.points) AS best_driver_points
    FROM driver_standings
    GROUP BY driver_standings.constructor_id, driver_standings.year
),
team_points AS (
    SELECT 
        constructor_standings.constructor_id,
        constructor_standings.year,
        constructor_standings.points AS team_points
    FROM constructor_standings
),
combined_points AS (
    SELECT 
        d.constructor_id,
        d.year,
        d.best_driver_points + t.team_points AS combined_points
    FROM driver_best_points d
    JOIN team_points t 
        ON d.constructor_id = t.constructor_id AND d.year = t.year
)
SELECT 
    c.name AS constructor_name,
    cp.year,
    cp.combined_points
FROM combined_points cp
JOIN constructors c ON cp.constructor_id = c.constructor_id
ORDER BY cp.combined_points DESC
LIMIT 3;
