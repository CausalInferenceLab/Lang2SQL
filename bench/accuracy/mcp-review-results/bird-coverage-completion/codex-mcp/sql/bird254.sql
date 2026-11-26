WITH ranked AS (
    SELECT q.driverId,
           RANK() OVER (
               ORDER BY CAST(substr(q.q1, 1, instr(q.q1, ':') - 1) AS INTEGER) DESC,
                        CAST(substr(q.q1, instr(q.q1, ':') + 1) AS REAL) DESC
           ) AS time_rank
    FROM main.qualifying AS q
    JOIN main.races AS r ON r.raceId = q.raceId
    WHERE r.year = 1995
      AND r.name = 'Japanese Grand Prix'
      AND instr(q.q1, ':') > 1
)
SELECT d.nationality
FROM ranked
JOIN main.drivers AS d ON d.driverId = ranked.driverId
WHERE ranked.time_rank = 1
LIMIT 1000
