SELECT d.code
FROM drivers AS d
WHERE d.driverId IN (
    SELECT q.driverId
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND q.q3 <> ''
)
ORDER BY d.code ASC
LIMIT 1000
