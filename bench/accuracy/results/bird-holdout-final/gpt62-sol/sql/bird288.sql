SELECT r.year, d.forename, d.surname, res.time AS finish_time
FROM races AS r
JOIN results AS res ON res.raceId = r.raceId
JOIN drivers AS d ON d.driverId = res.driverId
WHERE r.name = 'Canadian Grand Prix'
  AND r.year BETWEEN 2007 AND 2017
  AND time(r.time) <= '17:00:00'
  AND res.position = 1
ORDER BY r.year;
