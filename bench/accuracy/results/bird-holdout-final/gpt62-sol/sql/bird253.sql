WITH q1_times AS (
    SELECT q.driverId, q.q1,
           60 * CAST(SUBSTR(q.q1, 1, INSTR(q.q1, ':') - 1) AS INTEGER)
           + CAST(SUBSTR(q.q1, INSTR(q.q1, ':') + 1) AS REAL) AS elapsed_seconds
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 1995
      AND r.name = 'Japanese Grand Prix'
      AND INSTR(q.q1, ':') > 0
)
SELECT d.driverRef, d.nationality, d.dob, t.q1
FROM q1_times AS t
JOIN drivers AS d ON d.driverId = t.driverId
WHERE t.elapsed_seconds = (SELECT MAX(elapsed_seconds) FROM q1_times);
