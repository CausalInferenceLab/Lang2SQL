SELECT d.driverRef, d.nationality, d.dob
FROM qualifying AS q
JOIN races AS r ON r.raceId = q.raceId
JOIN drivers AS d ON d.driverId = q.driverId
WHERE r.year = 2017
  AND r.name = 'Italian Grand Prix'
  AND NULLIF(NULLIF(TRIM(q.q2), ''), '\N') IS NOT NULL
  AND NULLIF(NULLIF(TRIM(q.q3), ''), '\N') IS NULL;
