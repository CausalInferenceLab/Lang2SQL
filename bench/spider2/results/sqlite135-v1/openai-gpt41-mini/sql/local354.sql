WITH races_50s AS (
    SELECT race_id, year, round
    FROM races
    WHERE year BETWEEN 1950 AND 1959
),
first_last_constructor AS (
    SELECT 
        driver_id,
        year,
        MIN(round) AS first_round,
        MAX(round) AS last_round
    FROM (
        SELECT r.year, r.round, r.race_id, res.driver_id
        FROM races_50s r
        JOIN results res ON r.race_id = res.race_id
    )
    GROUP BY driver_id, year
),
first_last_constructors AS (
    SELECT 
        f.driver_id,
        f.year,
        f.first_round,
        f.last_round,
        r1.constructor_id AS first_constructor_id,
        r2.constructor_id AS last_constructor_id,
        COUNT(DISTINCT r2_round.round) AS rounds_count
    FROM first_last_constructor f
    JOIN races_50s r_first ON r_first.year = f.year AND r_first.round = f.first_round
    JOIN results r1 ON r1.race_id = r_first.race_id AND r1.driver_id = f.driver_id
    JOIN races_50s r_last ON r_last.year = f.year AND r_last.round = f.last_round
    JOIN results r2 ON r2.race_id = r_last.race_id AND r2.driver_id = f.driver_id
    JOIN results r2_round ON r2_round.driver_id = f.driver_id
        AND r2_round.race_id IN (SELECT race_id FROM races_50s WHERE year = f.year)
    GROUP BY f.driver_id, f.year, f.first_round, f.last_round, r1.constructor_id, r2.constructor_id
    HAVING rounds_count >= 2
       AND r1.constructor_id = r2.constructor_id
)
SELECT driver_id, year AS season, first_constructor_id AS constructor_id
FROM first_last_constructors
ORDER BY driver_id, year
LIMIT 100;
