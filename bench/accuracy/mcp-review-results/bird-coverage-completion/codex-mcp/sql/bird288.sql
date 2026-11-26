SELECT r.year, d.forename, d.surname, s.time AS finish_time
FROM races AS r
JOIN results AS s ON s.raceId = r.raceId
JOIN drivers AS d ON d.driverId = s.driverId
WHERE r.name = 'Canadian Grand Prix'
  AND r.year BETWEEN 2007 AND 2017
  AND r.time <= '17:00:00'
  AND s.position = 1
ORDER BY r.year
LIMIT 1000
