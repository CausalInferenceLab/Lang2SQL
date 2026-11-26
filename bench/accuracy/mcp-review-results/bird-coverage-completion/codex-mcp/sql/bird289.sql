SELECT d.number, d.surname
FROM qualifying AS q
JOIN drivers AS d ON d.driverId = q.driverId
JOIN races AS r ON r.raceId = q.raceId
JOIN circuits AS c ON c.circuitId = r.circuitId
WHERE r.year = 2017
  AND r.name = 'Italian Grand Prix'
  AND c.circuitRef = 'monza'
  AND q.q3 GLOB '[0-9]:[0-5][0-9].[0-9][0-9][0-9]'
  AND q.q3 < '1:38.000'
LIMIT -1
