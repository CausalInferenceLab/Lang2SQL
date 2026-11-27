SELECT 100.0 * COUNT(CASE WHEN d.dob < '1985-01-01' AND re.laps > 50 THEN 1 END)
       / NULLIF(COUNT(*), 0) AS percentage
FROM results AS re
JOIN races AS ra ON ra.raceId = re.raceId
LEFT JOIN drivers AS d ON d.driverId = re.driverId
WHERE ra.year BETWEEN 2000 AND 2005;
