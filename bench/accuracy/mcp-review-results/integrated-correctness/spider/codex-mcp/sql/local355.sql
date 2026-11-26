WITH recorded_races AS (
    SELECT r.race_id, r.year, r.round
    FROM main.races AS r
    WHERE r.race_id IN (SELECT race_id FROM main.results)
),
appearances AS (
    SELECT r.year, r.round, s.driver_id, s.constructor_id
    FROM recorded_races AS r
    JOIN main.results AS s ON s.race_id = r.race_id
),
driver_seasons AS (
    SELECT DISTINCT year, driver_id
    FROM appearances
),
missed_rounds AS (
    SELECT d.year, d.driver_id,
           MIN(r.round) AS first_missed_round,
           MAX(r.round) AS last_missed_round
    FROM driver_seasons AS d
    JOIN recorded_races AS r ON r.year = d.year
    LEFT JOIN appearances AS a
      ON a.year = d.year
     AND a.driver_id = d.driver_id
     AND a.round = r.round
    WHERE a.driver_id IS NULL
    GROUP BY d.year, d.driver_id
    HAVING COUNT(*) < 3
),
team_changes AS (
    SELECT DISTINCT m.year, m.driver_id, m.first_missed_round, m.last_missed_round
    FROM missed_rounds AS m
    JOIN appearances AS before_gap
      ON before_gap.year = m.year
     AND before_gap.driver_id = m.driver_id
     AND before_gap.round = m.first_missed_round - 1
    JOIN appearances AS after_gap
      ON after_gap.year = m.year
     AND after_gap.driver_id = m.driver_id
     AND after_gap.round = m.last_missed_round + 1
    WHERE before_gap.constructor_id <> after_gap.constructor_id
)
SELECT AVG(first_missed_round) AS avg_first_missed_round,
       AVG(last_missed_round) AS avg_last_missed_round
FROM team_changes
LIMIT 1
