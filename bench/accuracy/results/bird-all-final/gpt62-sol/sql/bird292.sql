SELECT d.driverRef
FROM qualifying AS q
JOIN races AS r ON r.raceId = q.raceId
JOIN drivers AS d ON d.driverId = q.driverId
WHERE r.year = 2017
  AND r.name = 'Italian Grand Prix'
  AND (q.q2 IS NULL OR TRIM(q.q2) = '')
ORDER BY CAST(SUBSTR(q.q1, 1, INSTR(q.q1, ':') - 1) AS INTEGER) DESC,
         CAST(SUBSTR(q.q1, INSTR(q.q1, ':') + 1) AS REAL) DESC,
         q.driverId ASC;
