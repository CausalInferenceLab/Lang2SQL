WITH ranked AS (
    SELECT q.driverId, q.q1,
           RANK() OVER (
               ORDER BY CAST(substr(q.q1, 1, instr(q.q1, ':') - 1) AS INTEGER) DESC,
                        CAST(substr(q.q1, instr(q.q1, ':') + 1) AS REAL) DESC
           ) AS time_rank
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 1995
      AND r.name = 'Japanese Grand Prix'
      AND q.q1 IS NOT NULL
)
SELECT d.driverRef, d.nationality, d.dob, ranked.q1
FROM ranked
JOIN drivers AS d ON d.driverId = ranked.driverId
WHERE ranked.time_rank = 1
ORDER BY d.driverRef
LIMIT 1000
