SELECT ROUND(100.0 * (COUNT(*) - COUNT(CASE WHEN re.position = 1 THEN 1 END)) / COUNT(*), 2) AS non_victory_percentage
FROM results AS re
JOIN drivers AS d ON d.driverId = re.driverId
JOIN races AS ra ON ra.raceId = re.raceId
WHERE d.forename = 'Lewis'
  AND d.surname = 'Hamilton'
  AND ra.year >= 2010;
