WITH season_finales AS (
    SELECT year, MAX(round) AS final_round
    FROM races
    WHERE year >= 2001
    GROUP BY year
), ranked_scorers AS (
    SELECT r.year, ds.driver_id,
           DENSE_RANK() OVER (PARTITION BY r.year ORDER BY ds.points) AS points_rank
    FROM season_finales AS sf
    JOIN races AS r ON r.year = sf.year AND r.round = sf.final_round
    JOIN driver_standings AS ds ON ds.race_id = r.race_id
    WHERE ds.points > 0
), scoring_entries AS (
    SELECT race_id, driver_id, constructor_id FROM results WHERE points > 0
    UNION ALL
    SELECT race_id, driver_id, constructor_id FROM sprint_results WHERE points > 0
)
SELECT c.name AS constructor
FROM ranked_scorers AS rs
JOIN races AS r ON r.year = rs.year
JOIN scoring_entries AS se ON se.race_id = r.race_id AND se.driver_id = rs.driver_id
JOIN constructors AS c ON c.constructor_id = se.constructor_id
WHERE rs.points_rank = 1
GROUP BY c.constructor_id, c.name
ORDER BY COUNT(DISTINCT rs.year) DESC, c.name ASC, c.constructor_id ASC
LIMIT 5;
