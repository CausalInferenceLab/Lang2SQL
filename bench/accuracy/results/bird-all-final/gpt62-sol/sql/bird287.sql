SELECT d.code
FROM drivers AS d
WHERE EXISTS (
    SELECT 1
    FROM qualifying AS q
    JOIN races AS r ON r.raceId = q.raceId
    WHERE q.driverId = d.driverId
      AND r.year = 2017
      AND r.name = 'Italian Grand Prix'
      AND TRIM(q.q3) NOT IN ('', '\N')
)
ORDER BY d.code;
