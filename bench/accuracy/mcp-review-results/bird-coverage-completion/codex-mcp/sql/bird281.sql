SELECT d.forename, d.surname, c.name AS constructor_name
FROM races AS r
JOIN qualifying AS q ON q.raceId = r.raceId
JOIN drivers AS d ON d.driverId = q.driverId
JOIN constructors AS c ON c.constructorId = q.constructorId
LEFT JOIN results AS s ON s.raceId = q.raceId AND s.driverId = q.driverId
WHERE r.year = 2017
  AND r.name = 'Italian Grand Prix'
  AND NULLIF(TRIM(q.q2), '') IS NOT NULL
ORDER BY CAST(SUBSTR(q.q2, 1, INSTR(q.q2, ':') - 1) AS INTEGER),
         CAST(SUBSTR(q.q2, INSTR(q.q2, ':') + 1) AS REAL),
         s.grid ASC NULLS LAST
LIMIT 1 OFFSET 1
