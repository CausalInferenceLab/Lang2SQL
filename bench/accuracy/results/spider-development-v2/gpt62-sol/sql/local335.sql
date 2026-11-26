WITH point_results AS (
    SELECT race_id, driver_id, constructor_id, points FROM results
    UNION ALL
    SELECT race_id, driver_id, constructor_id, points FROM sprint_results
),
driver_constructor_points AS (
    SELECT r.year, p.driver_id, p.constructor_id, SUM(p.points) AS points
    FROM point_results AS p
    JOIN races AS r ON r.race_id = p.race_id
    WHERE r.year >= 2001
    GROUP BY r.year, p.driver_id, p.constructor_id
),
driver_season_points AS (
    SELECT year, driver_id, SUM(points) AS total_points
    FROM driver_constructor_points
    GROUP BY year, driver_id
    HAVING SUM(points) > 0
),
season_minimums AS (
    SELECT year, MIN(total_points) AS min_points
    FROM driver_season_points
    GROUP BY year
)
SELECT c.name AS constructor, COUNT(DISTINCT d.year) AS season_count
FROM driver_season_points AS d
JOIN season_minimums AS m ON m.year = d.year AND m.min_points = d.total_points
JOIN driver_constructor_points AS p ON p.year = d.year AND p.driver_id = d.driver_id
JOIN constructors AS c ON c.constructor_id = p.constructor_id
WHERE p.points > 0
GROUP BY c.constructor_id, c.name
ORDER BY season_count DESC, c.name ASC, c.constructor_id ASC
LIMIT 5;
