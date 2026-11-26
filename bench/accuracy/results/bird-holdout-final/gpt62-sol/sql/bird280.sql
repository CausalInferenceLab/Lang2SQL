SELECT COUNT(DISTINCT r.raceId) AS runnings
FROM races AS r
JOIN results AS res ON res.raceId = r.raceId
JOIN status AS s ON s.statusId = res.statusId
WHERE r.name = 'Canadian Grand Prix'
  AND s.status = 'Accident';
