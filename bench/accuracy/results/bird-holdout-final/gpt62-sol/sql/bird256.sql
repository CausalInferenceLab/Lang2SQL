SELECT COUNT(DISTINCT opponent.driverId) AS drivers_outqualified
FROM qualifying AS lucas
JOIN drivers AS d ON d.driverId = lucas.driverId
JOIN races AS r ON r.raceId = lucas.raceId
JOIN qualifying AS opponent
  ON opponent.raceId = lucas.raceId
 AND opponent.driverId <> lucas.driverId
 AND opponent.position > lucas.position
WHERE d.forename = 'Lucas' COLLATE NOCASE
  AND d.surname = 'di Grassi' COLLATE NOCASE
  AND r.date >= '2010-01-01'
  AND r.date < '2011-01-01';
