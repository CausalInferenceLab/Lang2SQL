SELECT
    s.year,
    COUNT(p.raceId) AS total_pit_stops
FROM seasons AS s
LEFT JOIN races AS r
    ON r.year = s.year
    AND r.raceId IN (
        SELECT ds.raceId
        FROM driverStandings AS ds
        JOIN drivers AS d ON d.driverId = ds.driverId
        WHERE d.nationality = 'Australian'
          AND ds.position BETWEEN 1 AND 19
    )
LEFT JOIN pitStops AS p ON p.raceId = r.raceId
WHERE s.year BETWEEN 2002 AND 2013
GROUP BY s.year
ORDER BY s.year
LIMIT 1000
