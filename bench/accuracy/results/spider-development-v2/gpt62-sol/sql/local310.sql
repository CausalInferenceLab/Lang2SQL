WITH season_results AS (
    SELECT r.year, res.driver_id, res.constructor_id, res.points
    FROM results AS res
    JOIN races AS r ON r.race_id = res.race_id
), driver_totals AS (
    SELECT year, driver_id, SUM(points) AS total_points
    FROM season_results
    GROUP BY year, driver_id
), constructor_totals AS (
    SELECT year, constructor_id, SUM(points) AS total_points
    FROM season_results
    GROUP BY year, constructor_id
), driver_maxima AS (
    SELECT year, MAX(total_points) AS top_driver_points
    FROM driver_totals
    GROUP BY year
), constructor_maxima AS (
    SELECT year, MAX(total_points) AS top_constructor_points
    FROM constructor_totals
    GROUP BY year
)
SELECT d.year
FROM driver_maxima AS d
JOIN constructor_maxima AS c ON c.year = d.year
ORDER BY d.top_driver_points + c.top_constructor_points ASC, d.year ASC
LIMIT 3;
