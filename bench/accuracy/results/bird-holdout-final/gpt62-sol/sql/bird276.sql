SELECT s.year AS season_year,
       COUNT(ps.raceId) AS total_pit_stops
FROM seasons AS s
LEFT JOIN races AS r
  ON r.year = s.year
 AND EXISTS (
     SELECT 1
     FROM driverStandings AS ds
     JOIN drivers AS d ON d.driverId = ds.driverId
     WHERE ds.raceId = r.raceId
       AND d.nationality = 'Australian'
       AND ds.position BETWEEN 1 AND 19
 )
LEFT JOIN pitStops AS ps ON ps.raceId = r.raceId
WHERE s.year BETWEEN 2002 AND 2013
GROUP BY s.year
ORDER BY s.year;
