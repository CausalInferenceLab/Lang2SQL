WITH race_calendar AS (
    SELECT r.race_id, r.year, r.round,
           LAG(r.race_id) OVER (PARTITION BY r.year ORDER BY r.round) AS previous_race_id,
           LEAD(r.race_id) OVER (PARTITION BY r.year ORDER BY r.round) AS next_race_id
    FROM races AS r
    WHERE EXISTS (
        SELECT 1 FROM results AS x WHERE x.race_id = r.race_id
    )
), driver_years AS (
    SELECT DISTINCT c.year, x.driver_id
    FROM race_calendar AS c
    JOIN results AS x ON x.race_id = c.race_id
), missed_races AS (
    SELECT d.year, d.driver_id,
           MIN(c.round) AS first_missed_round,
           MAX(c.round) AS last_missed_round
    FROM driver_years AS d
    JOIN race_calendar AS c ON c.year = d.year
    LEFT JOIN results AS x ON x.race_id = c.race_id AND x.driver_id = d.driver_id
    WHERE x.driver_id IS NULL
    GROUP BY d.year, d.driver_id
    HAVING COUNT(*) BETWEEN 1 AND 2
)
SELECT AVG(m.first_missed_round) AS avg_first_missed_round,
       AVG(m.last_missed_round) AS avg_last_missed_round
FROM missed_races AS m
JOIN race_calendar AS first_missed
    ON first_missed.year = m.year AND first_missed.round = m.first_missed_round
JOIN race_calendar AS last_missed
    ON last_missed.year = m.year AND last_missed.round = m.last_missed_round
WHERE EXISTS (
    SELECT 1
    FROM results AS before_gap
    JOIN results AS after_gap ON after_gap.driver_id = before_gap.driver_id
    WHERE before_gap.driver_id = m.driver_id
      AND before_gap.race_id = first_missed.previous_race_id
      AND after_gap.race_id = last_missed.next_race_id
      AND before_gap.constructor_id <> after_gap.constructor_id
);
