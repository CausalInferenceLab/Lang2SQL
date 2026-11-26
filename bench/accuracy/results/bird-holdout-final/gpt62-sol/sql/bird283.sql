WITH q3_times AS (
    SELECT q.driverId,
           60000 * CAST(SUBSTR(q.q3, 1, INSTR(q.q3, ':') - 1) AS INTEGER)
           + CAST(REPLACE(SUBSTR(q.q3, INSTR(q.q3, ':') + 1), '.', '') AS INTEGER) AS time_ms
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND q.q3 GLOB '[0-9]*:[0-5][0-9].[0-9][0-9][0-9]'
)
SELECT d.number
FROM q3_times AS t
JOIN drivers AS d ON d.driverId = t.driverId
WHERE t.time_ms = (SELECT MIN(time_ms) FROM q3_times)
ORDER BY d.number;
