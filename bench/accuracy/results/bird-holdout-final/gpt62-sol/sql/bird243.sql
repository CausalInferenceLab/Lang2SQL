WITH race_results AS (
    SELECT res.position, res.laps, res.milliseconds, res.statusId
    FROM races AS r
    JOIN results AS res ON res.raceId = r.raceId
    WHERE r.year = 1988 AND r.name = 'Australian Grand Prix'
), winner AS (
    SELECT laps, milliseconds
    FROM race_results
    WHERE position = 1
), last_finisher AS (
    SELECT rr.milliseconds
    FROM race_results AS rr
    JOIN winner AS w ON rr.laps = w.laps
    JOIN status AS s ON s.statusId = rr.statusId
    WHERE s.status = 'Finished'
    ORDER BY rr.position DESC
    LIMIT 1
)
SELECT ROUND(100.0 * (lf.milliseconds - w.milliseconds) / lf.milliseconds, 2) AS percentage_shorter
FROM winner AS w
CROSS JOIN last_finisher AS lf;
