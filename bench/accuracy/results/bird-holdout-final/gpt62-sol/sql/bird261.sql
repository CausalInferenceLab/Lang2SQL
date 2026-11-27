WITH q2_times AS (
    SELECT q.driverId,
           60 * CAST(substr(q.q2, 1, instr(q.q2, ':') - 1) AS INTEGER)
              + CAST(substr(q.q2, instr(q.q2, ':') + 1) AS REAL) AS q2_seconds
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND instr(q.q2, ':') > 0
), ranked AS (
    SELECT driverId, DENSE_RANK() OVER (ORDER BY q2_seconds) AS time_rank
    FROM q2_times
)
SELECT d.nationality
FROM ranked AS q
JOIN drivers AS d ON d.driverId = q.driverId
WHERE q.time_rank = 2;
