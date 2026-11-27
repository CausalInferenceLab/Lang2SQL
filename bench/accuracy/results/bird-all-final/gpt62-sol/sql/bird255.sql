WITH q1_times AS (
    SELECT q.driverId, q.q1,
           ROUND(1000 * (
               CASE WHEN INSTR(q.q1, ':') > 0 THEN
                   60 * CAST(SUBSTR(q.q1, 1, INSTR(q.q1, ':') - 1) AS INTEGER)
                   + CAST(SUBSTR(q.q1, INSTR(q.q1, ':') + 1) AS REAL)
               ELSE CAST(q.q1 AS REAL)
               END
           )) AS q1_ms
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 1995
      AND r.name = 'Japanese Grand Prix'
      AND q.q1 GLOB '[0-9]*'
)
SELECT d.forename, d.surname, t.q1
FROM q1_times AS t
JOIN drivers AS d ON d.driverId = t.driverId
WHERE t.q1_ms = (SELECT MIN(q1_ms) FROM q1_times);
