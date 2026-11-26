SELECT d.driverRef, d.nationality, d.dob
FROM races AS r
JOIN qualifying AS q ON q.raceId = r.raceId
JOIN drivers AS d ON d.driverId = q.driverId
WHERE r.year = 2017
  AND r.name = 'Italian Grand Prix'
  AND NULLIF(TRIM(q.q2), '') IS NOT NULL
  AND NULLIF(TRIM(q.q3), '') IS NULL;
