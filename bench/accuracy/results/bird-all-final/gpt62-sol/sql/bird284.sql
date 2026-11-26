WITH ranked_q3 AS (
    SELECT d.code AS driver_code,
           q.q3 AS fastest_q3_time,
           RANK() OVER (
               ORDER BY 60 * CAST(substr(q.q3, 1, instr(q.q3, ':') - 1) AS INTEGER)
                        + CAST(substr(q.q3, instr(q.q3, ':') + 1) AS REAL)
           ) AS time_rank
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    JOIN drivers AS d ON d.driverId = q.driverId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND instr(q.q3, ':') > 0
)
SELECT driver_code, fastest_q3_time
FROM ranked_q3
WHERE time_rank = 1
ORDER BY driver_code;
