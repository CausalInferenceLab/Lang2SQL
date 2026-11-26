SELECT
    100.0 * COUNT(*) FILTER (WHERE d.dob < '1985-01-01' AND res.laps > 50)
        / NULLIF(COUNT(*), 0) AS percentage
FROM results AS res
JOIN races AS r ON r.raceId = res.raceId
JOIN drivers AS d ON d.driverId = res.driverId
WHERE r.year BETWEEN 2000 AND 2005
LIMIT 1
