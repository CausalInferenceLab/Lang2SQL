SELECT COUNT(DISTINCT res.driverId)
FROM results AS res
JOIN races AS r ON r.raceId = res.raceId
JOIN status AS s ON s.statusId = res.statusId
WHERE r.year = 1962
  AND r.name = 'British Grand Prix'
  AND s.status = 'Withdrew';
