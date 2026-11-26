SELECT ROUND(100.0 * AVG(CASE WHEN EXISTS (
    SELECT 1
    FROM results AS winner
    WHERE winner.raceId = entry.raceId
      AND winner.position = 1
      AND winner.laps = entry.laps
) THEN 1.0 ELSE 0.0 END), 2) AS percentage
FROM results AS entry
JOIN drivers AS driver ON driver.driverId = entry.driverId
JOIN races AS race ON race.raceId = entry.raceId
WHERE driver.nationality = 'Japanese'
  AND race.year BETWEEN 2007 AND 2009;
