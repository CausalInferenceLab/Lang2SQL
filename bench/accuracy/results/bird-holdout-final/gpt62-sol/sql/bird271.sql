WITH q3_times AS (
    SELECT d.driverRef,
           60 * CAST(SUBSTR(q.q3, 1, INSTR(q.q3, ':') - 1) AS INTEGER)
           + CAST(SUBSTR(q.q3, INSTR(q.q3, ':') + 1) AS REAL) AS elapsed_seconds
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    JOIN drivers AS d ON d.driverId = q.driverId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND INSTR(q.q3, ':') > 0
)
SELECT driverRef
FROM q3_times
WHERE elapsed_seconds = (SELECT MIN(elapsed_seconds) FROM q3_times)
ORDER BY driverRef;
