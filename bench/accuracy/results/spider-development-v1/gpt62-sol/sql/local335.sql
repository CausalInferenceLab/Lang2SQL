WITH season_finales AS (
    SELECT r.year, r.race_id,
           ROW_NUMBER() OVER (PARTITION BY r.year ORDER BY r.round DESC, r.race_id DESC) AS rn
    FROM races AS r
    WHERE r.year >= 2001
      AND EXISTS (SELECT 1 FROM driver_standings AS ds WHERE ds.race_id = r.race_id)
), ranked_drivers AS (
    SELECT f.year, ds.driver_id,
           DENSE_RANK() OVER (PARTITION BY f.year ORDER BY ds.points) AS points_rank
    FROM season_finales AS f
    JOIN driver_standings AS ds ON ds.race_id = f.race_id
    WHERE f.rn = 1 AND ds.points > 0
), scoring_constructors AS (
    SELECT r.year, res.driver_id, res.constructor_id
    FROM results AS res
    JOIN races AS r ON r.race_id = res.race_id
    WHERE r.year >= 2001 AND res.points > 0
    UNION
    SELECT r.year, sr.driver_id, sr.constructor_id
    FROM sprint_results AS sr
    JOIN races AS r ON r.race_id = sr.race_id
    WHERE r.year >= 2001 AND sr.points > 0
)
SELECT c.name AS constructor, COUNT(DISTINCT rd.year) AS season_count
FROM ranked_drivers AS rd
JOIN scoring_constructors AS sc ON sc.year = rd.year AND sc.driver_id = rd.driver_id
JOIN constructors AS c ON c.constructor_id = sc.constructor_id
WHERE rd.points_rank = 1
GROUP BY c.constructor_id, c.name
ORDER BY season_count DESC, c.name, c.constructor_id
LIMIT 5;
