WITH q3_times AS (
    SELECT q.driverId, q.q3
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND q.q3 <> ''
)
SELECT d.driverRef
FROM q3_times AS t
JOIN drivers AS d ON d.driverId = t.driverId
WHERE t.q3 = (SELECT MIN(q3) FROM q3_times)
ORDER BY d.driverRef
LIMIT 1000
