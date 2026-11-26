SELECT COUNT(DISTINCT rival.driverId) AS drivers_outqualified
FROM drivers AS d
JOIN qualifying AS lucas ON lucas.driverId = d.driverId
JOIN races AS r ON r.raceId = lucas.raceId
JOIN qualifying AS rival
  ON rival.raceId = lucas.raceId
 AND rival.driverId <> lucas.driverId
 AND rival.position > lucas.position
WHERE d.forename = 'Lucas'
  AND d.surname = 'di Grassi'
  AND r.year = 2010
LIMIT 1
