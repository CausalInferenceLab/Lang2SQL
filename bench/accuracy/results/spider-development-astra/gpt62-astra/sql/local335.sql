WITH scoring_results AS (
    SELECT race_id, driver_id, constructor_id, points FROM results
    UNION ALL
    SELECT race_id, driver_id, constructor_id, points FROM sprint_results
), constructor_points AS (
    SELECT r.year, s.driver_id, s.constructor_id, SUM(s.points) AS points
    FROM scoring_results AS s
    JOIN races AS r ON r.race_id = s.race_id
    WHERE r.year >= 2001
    GROUP BY r.year, s.driver_id, s.constructor_id
), driver_totals AS (
    SELECT year, driver_id, SUM(points) AS season_points
    FROM constructor_points
    GROUP BY year, driver_id
    HAVING SUM(points) > 0
), ranked_drivers AS (
    SELECT year, driver_id,
           RANK() OVER (PARTITION BY year ORDER BY season_points) AS points_rank
    FROM driver_totals
)
SELECT c.name AS constructor, COUNT(DISTINCT p.year) AS season_count
FROM ranked_drivers AS d
JOIN constructor_points AS p ON p.year = d.year AND p.driver_id = d.driver_id
JOIN constructors AS c ON c.constructor_id = p.constructor_id
WHERE d.points_rank = 1 AND p.points > 0
GROUP BY c.constructor_id, c.name
ORDER BY season_count DESC, c.name ASC
LIMIT 5;
