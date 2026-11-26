WITH q3_times AS (
    SELECT q.driverId,
           60 * CAST(substr(q.q3, 1, instr(q.q3, ':') - 1) AS INTEGER)
              + CAST(substr(q.q3, instr(q.q3, ':') + 1) AS REAL) AS seconds
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    JOIN circuits AS c ON c.circuitId = r.circuitId
    WHERE r.year = 2008
      AND c.name = 'Marina Bay Street Circuit'
      AND instr(q.q3, ':') > 0
)
SELECT d.forename, d.surname
FROM q3_times AS t
JOIN drivers AS d ON d.driverId = t.driverId
WHERE t.seconds = (SELECT MIN(seconds) FROM q3_times);
