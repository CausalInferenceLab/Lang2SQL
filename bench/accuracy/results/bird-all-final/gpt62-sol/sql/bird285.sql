WITH q3_times AS (
    SELECT q.driverId,
           60 * CAST(SUBSTR(q.q3, 1, INSTR(q.q3, ':') - 1) AS INTEGER)
           + CAST(SUBSTR(q.q3, INSTR(q.q3, ':') + 1) AS REAL) AS seconds
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND INSTR(q.q3, ':') > 0
)
SELECT d.nationality
FROM q3_times AS t
JOIN drivers AS d ON d.driverId = t.driverId
WHERE t.seconds = (SELECT MIN(seconds) FROM q3_times);
