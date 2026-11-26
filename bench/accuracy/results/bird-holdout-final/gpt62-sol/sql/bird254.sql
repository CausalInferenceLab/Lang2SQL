WITH q1_times AS (
    SELECT q.driverId,
           60000 * CAST(substr(q.q1, 1, instr(q.q1, ':') - 1) AS INTEGER)
           + CAST(replace(substr(q.q1, instr(q.q1, ':') + 1), '.', '') AS INTEGER) AS q1_ms
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 1995
      AND r.name = 'Japanese Grand Prix'
      AND q.q1 GLOB '[0-9]*:[0-9][0-9].[0-9][0-9][0-9]'
)
SELECT d.nationality
FROM q1_times AS t
JOIN drivers AS d ON d.driverId = t.driverId
WHERE t.q1_ms = (SELECT MAX(q1_ms) FROM q1_times);
