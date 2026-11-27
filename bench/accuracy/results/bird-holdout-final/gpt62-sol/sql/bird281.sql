SELECT d.forename, d.surname, c.name AS constructor_name
FROM qualifying AS q
JOIN races AS r ON r.raceId = q.raceId
JOIN drivers AS d ON d.driverId = q.driverId
JOIN constructors AS c ON c.constructorId = q.constructorId
LEFT JOIN results AS res ON res.raceId = q.raceId AND res.driverId = q.driverId
WHERE r.year = 2017
  AND r.name = 'Italian Grand Prix'
  AND INSTR(q.q2, ':') > 0
ORDER BY
  60000 * CAST(SUBSTR(q.q2, 1, INSTR(q.q2, ':') - 1) AS INTEGER)
  + CAST(REPLACE(SUBSTR(q.q2, INSTR(q.q2, ':') + 1), '.', '') AS INTEGER),
  CASE WHEN res.grid > 0 THEN 0 ELSE 1 END,
  res.grid
LIMIT 1 OFFSET 1;
